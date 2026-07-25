"""FIFO block grouper — the "no-scheduler" baseline path.

Purpose: paired experiments (Auriga vs. unscheduled baseline) must run the
same workload through the same pipeline, differing only in whether Auriga's
BPC/DSatur reorderer is present. ``BaselineGrouper`` gives the pipeline a
drop-in stand-in for ``ConflictEngine`` that just chunks the window into
consecutive ``block_size``-sized FIFO groups, with no conflict avoidance.

Interface parity with ``ConflictEngine``:

- ``compute_rw_sets(txs)`` — same signature; populates each tx's
  ``reads`` / ``writes`` / ``valid`` fields via the predictor. Metrics
  downstream depend on those fields being populated even in the baseline
  path (that's how the collector reports how many MVCC conflicts the
  baseline actually suffered).
- ``build_schedule(txs, block_size)`` — same signature; returns a list of
  ``List[Transaction]``. Blocks are consecutive slices in arrival order.

The ``PipelineOrchestrator`` picks ``ConflictEngine`` or ``BaselineGrouper``
by ``--schedule auriga|none`` and treats the rest of the pipeline
identically.
"""

from __future__ import annotations

from typing import List

from core.data_model import ReadWriteSet, Transaction


class BaselineGrouper:
    def __init__(self, predictor) -> None:
        self.predictor = predictor

    def compute_rw_sets(self, txs: List[Transaction]) -> None:
        """Populate reads/writes/valid on each tx using the predictor.

        Baseline grouping doesn't need these to *decide* the schedule, but
        the downstream metric collector reports MVCC-conflict counts by
        looking at per-tx predicted RW sets vs actual commit status. So we
        run the same predictor as ``ConflictEngine``.
        """
        for tx in txs:
            if not tx.reads and not tx.writes:
                rw: ReadWriteSet = self.predictor.predict(tx.data)
                tx.reads = set(rw.reads)
                tx.writes = set(rw.writes)
                tx.valid = rw.valid

    def build_schedule(
        self, txs: List[Transaction], block_size: int
    ) -> List[List[Transaction]]:
        """Chunk *txs* into consecutive blocks of size *block_size* in FIFO order."""
        self.compute_rw_sets(txs)
        if block_size <= 0:
            raise ValueError("block_size must be > 0")
        return [txs[i : i + block_size] for i in range(0, len(txs), block_size)]
