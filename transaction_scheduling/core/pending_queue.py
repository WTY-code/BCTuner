"""Bounded FIFO of pre-grouped blocks between the reorderer and the submitter.

Reorderer threads ``put(block)`` after DSatur emits each group. The submitter
``get(timeout=...)`` blocks and pops one block at a time. When the queue is
full, put blocks — this is the intended back-pressure: submitter slow →
pending queue full → reorderer stalls → main pool depth rises.

A ``Block`` here is just a ``List[Transaction]``. Blocks in the queue are
assumed to be conflict-free by construction (produced by the reorderer's
BPC / DSatur output).
"""

from __future__ import annotations

import queue
import threading
import time
from typing import Dict, List, Optional

from core.data_model import Transaction

Block = List[Transaction]


class PendingQueue:
    """Bounded, block-on-full FIFO of Blocks.

    Counters exposed for the metric collector:
      ``enqueued_blocks``, ``dequeued_blocks``, ``queue_depth``,
      ``block_wait_ms_sum``, ``block_wait_ms_count`` (for avg derivation),
      ``enqueued_txs``, ``dequeued_txs``.
    """

    def __init__(self, maxsize: int = 16) -> None:
        if maxsize <= 0:
            raise ValueError("PendingQueue.maxsize must be > 0 (bounded, block-on-full)")
        self._q: "queue.Queue[Block]" = queue.Queue(maxsize=maxsize)
        self.maxsize = maxsize
        self._lock = threading.Lock()
        self._enqueued_blocks = 0
        self._dequeued_blocks = 0
        self._enqueued_txs = 0
        self._dequeued_txs = 0
        self._wait_ms_sum = 0.0
        self._wait_ms_count = 0
        self._entry_ts: Dict[int, float] = {}

    def __len__(self) -> int:
        return self._q.qsize()

    def qsize(self) -> int:
        return self._q.qsize()

    def put(self, block: Block, timeout: Optional[float] = None) -> None:
        """Enqueue one block. Blocks if the queue is full (unless *timeout*)."""
        if not block:
            return
        entry_ts = time.time()
        # `queue.Queue.put(block, timeout=None)` blocks indefinitely; that's
        # what we want for back-pressure.
        self._q.put(block, timeout=timeout)
        with self._lock:
            self._enqueued_blocks += 1
            self._enqueued_txs += len(block)
            self._entry_ts[id(block)] = entry_ts

    def get(self, timeout: Optional[float] = None) -> Optional[Block]:
        """Pop one block. Returns None if *timeout* elapsed without a block."""
        try:
            block = self._q.get(timeout=timeout)
        except queue.Empty:
            return None
        now = time.time()
        with self._lock:
            self._dequeued_blocks += 1
            self._dequeued_txs += len(block)
            ts = self._entry_ts.pop(id(block), None)
            if ts is not None:
                self._wait_ms_sum += (now - ts) * 1000.0
                self._wait_ms_count += 1
        return block

    def snapshot_counters(self) -> Dict[str, float]:
        with self._lock:
            avg_wait = (
                self._wait_ms_sum / self._wait_ms_count
                if self._wait_ms_count
                else 0.0
            )
            return {
                "queue_depth": self._q.qsize(),
                "enqueued_blocks": self._enqueued_blocks,
                "dequeued_blocks": self._dequeued_blocks,
                "enqueued_txs": self._enqueued_txs,
                "dequeued_txs": self._dequeued_txs,
                "block_wait_ms_avg": avg_wait,
            }
