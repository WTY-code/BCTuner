"""Record schemas for the metric collector.

Each dataclass corresponds to one line in one of the persisted JSONL files
or one aggregated stat in ``summary.json``. Everything is a plain dataclass
that ``dataclasses.asdict`` can convert directly to JSON.

Time convention:
- Time-since-epoch values (from the Node submitter/listener) arrive in
  **milliseconds** because JS ``Date.now()`` returns ms.
- Latencies here are stored in **milliseconds** for readability.
- Python-side times captured with ``time.time()`` (seconds) are converted
  to ms at the collector boundary.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class TxRecord:
    """One completed transaction (endorse + broadcast + commit merged)."""
    tx_id: str                                   # our synthetic id (u3-t42)
    fabric_txid: Optional[str] = None            # Fabric-assigned tx id
    user_id: Optional[int] = None
    function_name: Optional[str] = None
    arguments: List[str] = field(default_factory=list)
    block_id: Optional[int] = None               # scheduler-assigned block id
    # Timeline (all in ms since epoch; missing = None)
    t_generated: Optional[float] = None          # from workload file (seconds → ms handled by caller)
    t_entered_buffer: Optional[float] = None
    t_left_buffer: Optional[float] = None
    t_submit_start: Optional[float] = None
    t_endorse_done: Optional[float] = None
    t_broadcast_done: Optional[float] = None
    t_commit_seen: Optional[float] = None
    # Statuses
    endorse_err: Optional[str] = None
    broadcast_err: Optional[str] = None
    commit_validation_code: Optional[int] = None    # 0 == VALID
    commit_validation_name: Optional[str] = None    # e.g. MVCC_READ_CONFLICT
    # Fabric block placement
    fabric_block_num: Optional[int] = None
    fabric_block_index: Optional[int] = None
    # Predicted read/write sets (populated by scheduler side; useful for
    # bottleneck attribution — did we predict correctly?)
    predicted_reads: List[str] = field(default_factory=list)
    predicted_writes: List[str] = field(default_factory=list)

    # -------- derived latencies (ms) --------
    @property
    def endorse_latency_ms(self) -> Optional[float]:
        if self.t_endorse_done is None or self.t_submit_start is None:
            return None
        return self.t_endorse_done - self.t_submit_start

    @property
    def broadcast_latency_ms(self) -> Optional[float]:
        if self.t_broadcast_done is None or self.t_endorse_done is None:
            return None
        return self.t_broadcast_done - self.t_endorse_done

    @property
    def commit_latency_ms(self) -> Optional[float]:
        if self.t_commit_seen is None or self.t_broadcast_done is None:
            return None
        return self.t_commit_seen - self.t_broadcast_done

    @property
    def e2e_latency_ms(self) -> Optional[float]:
        if self.t_commit_seen is None or self.t_submit_start is None:
            return None
        return self.t_commit_seen - self.t_submit_start

    @property
    def queue_wait_ms(self) -> Optional[float]:
        if self.t_left_buffer is None or self.t_entered_buffer is None:
            return None
        return self.t_left_buffer - self.t_entered_buffer

    @property
    def is_valid_commit(self) -> bool:
        return self.commit_validation_code == 0


@dataclass
class BlockRecord:
    """One committed Fabric block (from the block listener)."""
    fabric_block_num: int
    t_seen_ms: float
    num_txs: int
    valid_count: int
    validation_code_counts: Dict[str, int] = field(default_factory=dict)
    tx_ids: List[str] = field(default_factory=list)      # fabric_txids in order

    @property
    def invalid_count(self) -> int:
        return self.num_txs - self.valid_count

    @property
    def mvcc_conflict_count(self) -> int:
        return self.validation_code_counts.get("MVCC_READ_CONFLICT", 0)

    @property
    def endorsement_policy_failure_count(self) -> int:
        return self.validation_code_counts.get("ENDORSEMENT_POLICY_FAILURE", 0)


@dataclass
class CounterSnapshot:
    """Periodic snapshot of scheduler-side counters (buffer, pool, pending queue)."""
    t_ms: float
    buffer_pool: Dict[str, float] = field(default_factory=dict)
    pool_controller: Dict[str, float] = field(default_factory=dict)
    pending_queue: Dict[str, float] = field(default_factory=dict)
