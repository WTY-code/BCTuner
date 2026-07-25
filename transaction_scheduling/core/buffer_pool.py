"""In-memory main pool for the scheduling pipeline.

Replaces the old JSONL-backed BufferPool. Producer threads (user simulator,
or a one-shot preloader for the legacy Caliper path) call ``submit(tx)``;
consumer thread (PoolController) drains via ``get_window(size, max_wait_ms)``.

Backwards-compatible with the legacy ``BufferPool(file_path)`` constructor:
if the first argument is a path to an existing JSONL, its lines are preloaded
into the queue at construction time. That keeps ``run_pipeline.py`` (the
Caliper parity path) working without changes.
"""

from __future__ import annotations

import json
import queue
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Union


class BufferPool:
    """Thread-safe in-memory FIFO of transaction dicts.

    - ``submit(tx)`` — bounded put. Mode ``block`` blocks the producer when
      full; ``drop_oldest`` evicts the head and retries.
    - ``get_window(size, max_wait_ms)`` — drain up to *size* items, waiting
      up to *max_wait_ms* for txs to appear.
    - ``preload_from_jsonl(path)`` — bulk-import at startup.

    Counters:
      ``enqueued_total``, ``dequeued_total``, ``dropped_total``,
      ``time_in_buffer_sum_ms``, ``time_in_buffer_count``.
    """

    def __init__(
        self,
        file_path: Optional[Union[str, Path]] = None,
        maxsize: int = 0,
        mode: str = "block",
    ) -> None:
        if mode not in ("block", "drop_oldest"):
            raise ValueError(f"unknown mode {mode!r}; want block|drop_oldest")
        self._q: "queue.Queue[Dict]" = queue.Queue(maxsize=maxsize)
        self.maxsize = maxsize
        self.mode = mode
        # Counters (read under _lock, but writes to `queue.Queue` are already
        # atomic and the counter increments here are cheap so a small lock is
        # fine).
        self._lock = threading.Lock()
        self.enqueued_total: int = 0
        self.dequeued_total: int = 0
        self.dropped_total: int = 0
        self.time_in_buffer_sum_ms: float = 0.0
        self.time_in_buffer_count: int = 0
        # Track per-tx entry timestamp via a monotonic-clock sidecar; keyed by
        # id(tx dict) so we don't mutate the tx.
        self._entry_ts: Dict[int, float] = {}

        if file_path is not None:
            self.preload_from_jsonl(file_path)

    # ---------------------------------------------------------- introspection

    def __len__(self) -> int:
        return self._q.qsize()

    def qsize(self) -> int:
        return self._q.qsize()

    def snapshot_counters(self) -> Dict[str, float]:
        with self._lock:
            avg_wait = (
                self.time_in_buffer_sum_ms / self.time_in_buffer_count
                if self.time_in_buffer_count
                else 0.0
            )
            return {
                "queued_depth": self._q.qsize(),
                "enqueued_total": self.enqueued_total,
                "dequeued_total": self.dequeued_total,
                "dropped_total": self.dropped_total,
                "avg_wait_ms": avg_wait,
            }

    # ---------------------------------------------------------------- producer

    def submit(self, tx: Dict) -> bool:
        """Enqueue one tx. Returns True on success, False if dropped.

        In ``block`` mode this call blocks until space is available (never
        returns False). In ``drop_oldest`` mode, if the queue is full the
        oldest tx is evicted and the new one is inserted.
        """
        tx.setdefault("t_entered_buffer", time.time())
        if self.mode == "block":
            self._q.put(tx)
            with self._lock:
                self.enqueued_total += 1
                self._entry_ts[id(tx)] = tx["t_entered_buffer"]
            return True

        # drop_oldest — try put_nowait; on full, drop one, retry.
        while True:
            try:
                self._q.put_nowait(tx)
                with self._lock:
                    self.enqueued_total += 1
                    self._entry_ts[id(tx)] = tx["t_entered_buffer"]
                return True
            except queue.Full:
                try:
                    dropped = self._q.get_nowait()
                    with self._lock:
                        self.dropped_total += 1
                        self._entry_ts.pop(id(dropped), None)
                except queue.Empty:
                    # Race: someone drained just now; retry the put.
                    continue

    # ---------------------------------------------------------------- consumer

    def get_window(self, size: int, max_wait_ms: int) -> List[Dict]:
        """Drain up to *size* items, waiting up to *max_wait_ms* for any to appear.

        Returns whatever has arrived by the deadline (possibly less than
        *size*, possibly empty).
        """
        if size <= 0:
            return []
        deadline = time.monotonic() + max_wait_ms / 1000.0
        out: List[Dict] = []
        # Block on the first item to honour the wait budget.
        try:
            wait = max(0.0, deadline - time.monotonic())
            first = self._q.get(timeout=wait) if wait > 0 else self._q.get_nowait()
            out.append(first)
        except queue.Empty:
            return out
        # Drain the rest non-blocking.
        while len(out) < size:
            try:
                out.append(self._q.get_nowait())
            except queue.Empty:
                # Optional: keep waiting until deadline for more.
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                # Small sleep to yield to producers.
                time.sleep(min(0.005, remaining))
                if time.monotonic() >= deadline:
                    break
        # Book-keeping.
        now = time.time()
        with self._lock:
            self.dequeued_total += len(out)
            for tx in out:
                ts = self._entry_ts.pop(id(tx), None)
                if ts is None:
                    ts = tx.get("t_entered_buffer", now)
                self.time_in_buffer_sum_ms += (now - ts) * 1000.0
                self.time_in_buffer_count += 1
        return out

    # ---------------------------------------------------------------- preload

    def preload_from_jsonl(self, path: Union[str, Path]) -> int:
        """Bulk-load a JSONL file into the queue (used by the Caliper path).

        Returns number of lines loaded. If the file does not exist, returns 0.
        """
        p = Path(path)
        if not p.is_file():
            return 0
        n = 0
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                tx = json.loads(line)
                self.submit(tx)
                n += 1
        return n

    # ---------------------------------------------------------------- legacy

    # Legacy alias kept for callers that used ``buf.get_window(size, wait_ms)``
    # with a file-loaded pool. Same signature, same semantics.
    def read_all(self) -> List[Dict]:  # pragma: no cover
        """Drain the entire queue (destructive). Legacy compat only."""
        out: List[Dict] = []
        while True:
            try:
                out.append(self._q.get_nowait())
            except queue.Empty:
                return out
