"""Main pool controller — paper's "main pool" component.

Owns the deque-backed ``main_queue`` (working set for the reorderer) and the
``recycle_bucket`` (txs that couldn't fit their group). Provides:

- ``load_window(window_size, max_wait_ms)`` — pull txs from ``BufferPool``
  into ``main_queue``.
- ``fetch_batch(size)`` — drain up to *size* txs from ``main_queue`` into a
  reorder window (called by the reorderer worker).
- ``push_back_main(tx)`` / ``send_to_recycle(tx)`` — scatter helpers used by
  ``_scatter_group``.
- ``smart_fill(initial_group, target_size, engine)`` — paper's SmartFill
  drafting from recycle_bucket then main_queue.
- ``needs_detox(block_size)`` / ``force_detox_batch(block_size)`` — recycle
  bucket eviction.

Thread safety: all mutations of ``main_queue`` and ``recycle_bucket`` go
through an internal ``threading.Lock``. Multiple reorderer threads may
therefore share one ``PoolController`` instance. The paper's mechanisms
(``smart_fill``, ``needs_detox``, ``force_detox_batch``, scatter helpers)
are all compound reads-then-writes and require this lock to be correct.
"""

from __future__ import annotations

import threading
from collections import deque
from typing import Deque, Dict, List, Optional, Union
from pathlib import Path

from core.data_model import Transaction
from core.conflict_engine import ConflictEngine, TransactionGroup
from core.buffer_pool import BufferPool


class PoolController:
    def __init__(
        self,
        buffer_path: Optional[Union[str, Path]] = None,
        max_age: int = 3,
        recycle_capacity: int = 1000,
        buffer_pool: Optional[BufferPool] = None,
    ) -> None:
        """Create the main pool.

        If ``buffer_pool`` is given, use it as the upstream I/O source (live
        mode — the user simulator writes into it directly). Otherwise
        instantiate a ``BufferPool`` from ``buffer_path`` (Caliper parity
        path — preloads a JSONL).
        """
        if buffer_pool is not None:
            self.io_pool = buffer_pool
        else:
            self.io_pool = BufferPool(buffer_path)
        self.main_queue: Deque[Transaction] = deque()
        self.recycle_bucket: List[Transaction] = []
        self.max_age = max_age
        self.recycle_capacity = recycle_capacity
        self._lock = threading.Lock()

    # -------------------------------------------------------------- loading

    def load_window(self, window_size: int, max_wait_ms: int) -> int:
        """Drain up to *window_size* txs from the buffer pool into main_queue."""
        raw_data = self.io_pool.get_window(window_size, max_wait_ms)
        if not raw_data:
            return 0
        with self._lock:
            for d in raw_data:
                self.main_queue.append(Transaction.from_dict(d))
        return len(raw_data)

    # ----------------------------------------------------- reorder-window fetch

    def fetch_batch(self, size: int) -> List[Transaction]:
        """Pop up to *size* txs from main_queue as a fresh reorder window."""
        batch: List[Transaction] = []
        with self._lock:
            while len(batch) < size and self.main_queue:
                batch.append(self.main_queue.popleft())
        return batch

    def push_back_main(self, tx: Transaction) -> None:
        with self._lock:
            self.main_queue.appendleft(tx)

    def send_to_recycle(self, tx: Transaction) -> None:
        with self._lock:
            self.recycle_bucket.append(tx)

    def has_pending_data(self) -> bool:
        with self._lock:
            return bool(self.main_queue) or bool(self.recycle_bucket)

    # -------------------------------------------------------- recycle bucket

    def needs_detox(self, block_size: int) -> bool:
        with self._lock:
            return len(self.recycle_bucket) > self.recycle_capacity

    def force_detox_batch(self, block_size: int) -> List[Transaction]:
        with self._lock:
            if not self.recycle_bucket:
                return []
            count = min(len(self.recycle_bucket), block_size)
            batch = self.recycle_bucket[:count]
            self.recycle_bucket = self.recycle_bucket[count:]
            return batch

    # ------------------------------------------------------------- smart fill

    def smart_fill(
        self,
        initial_group: List[Transaction],
        target_size: int,
        engine: ConflictEngine,
    ) -> List[Transaction]:
        """Draft txs from recycle_bucket then main_queue to top up *initial_group*.

        Preserves the paper's SmartFill semantics: (1) prefer recycle bucket
        (older txs that already missed a group), (2) scan main_queue with a
        scan-limit heuristic, (3) put skipped candidates back on the front
        of main_queue in original order.
        """
        wrapper = TransactionGroup()
        for tx in initial_group:
            wrapper.add(tx)

        need = target_size - len(initial_group)
        if need <= 0:
            return wrapper.to_list()

        with self._lock:
            # --- 1) draft from recycle bucket ---
            new_recycle: List[Transaction] = []
            for tx in self.recycle_bucket:
                if need > 0 and not wrapper.conflicts_with(tx):
                    wrapper.add(tx)
                    need -= 1
                else:
                    new_recycle.append(tx)
            self.recycle_bucket = new_recycle

            if need == 0:
                return wrapper.to_list()

            # --- 2) draft from main_queue with scan-limit ---
            skipped: List[Transaction] = []
            scan_limit = need * 10
            scanned = 0
            while need > 0 and self.main_queue and scanned < scan_limit:
                candidate = self.main_queue.popleft()
                scanned += 1
                # RW-set computation may be expensive; we run it while
                # holding the pool lock. In practice ``compute_rw_sets`` on
                # a single tx is O(a few ms) and this is the pattern the
                # existing code uses too.
                engine.compute_rw_sets([candidate])

                if not wrapper.conflicts_with(candidate):
                    wrapper.add(candidate)
                    need -= 1
                else:
                    skipped.append(candidate)

            # Restore skipped candidates at the head, preserving order.
            for tx in reversed(skipped):
                self.main_queue.appendleft(tx)

        return wrapper.to_list()

    # -------------------------------------------------------------- counters

    def snapshot_counters(self) -> Dict[str, int]:
        with self._lock:
            return {
                "main_queue_depth": len(self.main_queue),
                "recycle_bucket_depth": len(self.recycle_bucket),
            }
