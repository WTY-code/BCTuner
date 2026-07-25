"""Central metric collector — merges events from every stage of the pipeline.

Event sources:

- **submitter** (Node ``submit.js`` via ``NodeSubmitter``): ``tx_submit``,
  ``tx_endorse_done``, ``tx_broadcast_done``, ``block_done``, and log lines.
- **block_listener** (Node ``block_listener.js`` via ``NodeSubmitter``):
  ``block_committed`` with per-tx ``validation_code``.
- **scheduler** (Python): pushes ``group_assigned`` events per tx (block_id
  assignment) and the reorderer's ``t_left_buffer`` timestamp; pushes
  ``CounterSnapshot`` events every ``snapshot_interval`` seconds.

The collector runs in its own thread. Callers push events via
``push(event)`` (thread-safe queue). Two indexes are maintained:
- ``by_our_id[tx_id]`` keyed by our synthetic tx id (u3-t42) — populated
  when the scheduler assigns a group and by ``tx_submit`` events.
- ``by_fabric_txid[fabric_txid]`` — cross-linked once ``tx_submit`` reports
  the Fabric-assigned txid. The block listener merges by this key.

When ``stop()`` is called, the collector drains its queue, flushes all
in-flight records (any tx that has at least an endorse event but no commit
by ``commit_grace_ms`` after stop) and writes:
- ``<out_dir>/tx.jsonl``
- ``<out_dir>/blocks.jsonl``
- ``<out_dir>/counters.jsonl``
- ``<out_dir>/summary.json``
"""

from __future__ import annotations

import dataclasses
import json
import queue
import statistics
import threading
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from core.metrics.schemas import BlockRecord, CounterSnapshot, TxRecord


# Mirror of the mapping in submitter/block_listener.js so tx_commit_done
# events (which carry a numeric code) can be enriched with a friendly name
# even before the block-listener event lands.
_VALIDATION_CODE_NAMES = {
    0: "VALID",
    10: "ENDORSEMENT_POLICY_FAILURE",
    11: "MVCC_READ_CONFLICT",
    12: "PHANTOM_READ_CONFLICT",
    22: "BAD_RWSET",
    254: "NOT_VALIDATED",
    255: "INVALID_OTHER_REASON",
}


class MetricCollector:
    def __init__(
        self,
        out_dir: Path,
        run_id: str = "run",
        commit_grace_ms: float = 5000.0,
    ) -> None:
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.run_id = run_id
        self.commit_grace_ms = commit_grace_ms

        self._q: "queue.Queue[dict]" = queue.Queue()
        self._by_our_id: Dict[str, TxRecord] = {}
        self._by_fabric_txid: Dict[str, TxRecord] = {}
        self._blocks: List[BlockRecord] = []
        self._counters: List[CounterSnapshot] = []
        self._log_lines: List[str] = []
        self._lock = threading.Lock()
        self._worker: Optional[threading.Thread] = None
        self._stop_flag = threading.Event()
        self.t_run_start_ms: float = time.time() * 1000.0
        self.t_run_end_ms: Optional[float] = None
        self._n_events = 0

    # --------------------------------------------------------------- lifecycle

    def start(self) -> None:
        if self._worker is not None:
            return
        self.t_run_start_ms = time.time() * 1000.0
        self._worker = threading.Thread(
            target=self._loop, name="metric-collector", daemon=True
        )
        self._worker.start()

    def stop(self) -> None:
        """Signal the worker to drain and exit. Call ``join()`` after."""
        self._stop_flag.set()
        # sentinel event to wake the worker
        self._q.put({"type": "__stop__"})

    def join(self, timeout: Optional[float] = None) -> None:
        if self._worker is not None:
            self._worker.join(timeout=timeout)
        self.t_run_end_ms = time.time() * 1000.0

    # ------------------------------------------------------------------ push

    def push(self, event: dict) -> None:
        """Enqueue one event dict. Thread-safe and non-blocking."""
        self._n_events += 1
        self._q.put(event)

    # ----------------------------------------------------------------- loop

    def _loop(self) -> None:
        while True:
            ev = self._q.get()
            et = ev.get("type", "")
            if et == "__stop__":
                break
            try:
                self._handle(ev)
            except Exception as exc:
                self._log_lines.append(f"[collector] error handling {et}: {exc}")

    # ---------------------------------------------------------------- merge

    def _rec_for_our_id(self, our_id: str) -> TxRecord:
        with self._lock:
            rec = self._by_our_id.get(our_id)
            if rec is None:
                rec = TxRecord(tx_id=our_id)
                self._by_our_id[our_id] = rec
            return rec

    def _link_fabric_txid(self, rec: TxRecord, fabric_txid: str) -> None:
        if not fabric_txid:
            return
        rec.fabric_txid = fabric_txid
        with self._lock:
            self._by_fabric_txid[fabric_txid] = rec

    # -------------------------------------------------------------- handlers

    def _handle(self, ev: dict) -> None:
        et = ev.get("type", "")

        if et == "log":
            self._log_lines.append(f"[{ev.get('source')}] {ev.get('line')}")
            return

        # ---- scheduler-side events ----
        if et == "tx_generated":  # push from user simulator (optional; may be from workload file)
            rec = self._rec_for_our_id(ev["tx_id"])
            rec.user_id = ev.get("user_id")
            rec.function_name = ev.get("function_name")
            rec.arguments = ev.get("arguments") or []
            rec.t_generated = ev.get("t_generated_ms")
            return

        if et == "tx_entered_buffer":
            rec = self._rec_for_our_id(ev["tx_id"])
            rec.t_entered_buffer = ev.get("t_ms")
            return

        if et == "tx_left_buffer":
            rec = self._rec_for_our_id(ev["tx_id"])
            rec.t_left_buffer = ev.get("t_ms")
            return

        if et == "group_assigned":
            rec = self._rec_for_our_id(ev["tx_id"])
            rec.block_id = ev.get("block_id")
            rec.predicted_reads = list(ev.get("predicted_reads") or [])
            rec.predicted_writes = list(ev.get("predicted_writes") or [])
            return

        # ---- Node submitter events ----
        if et == "tx_submit":
            rec = self._rec_for_our_id(ev["tx_id"])
            rec.t_submit_start = ev.get("t_submit_start")
            if ev.get("block_id") is not None and rec.block_id is None:
                rec.block_id = ev["block_id"]
            return

        if et == "tx_endorse_done":
            rec = self._rec_for_our_id(ev["tx_id"])
            rec.t_endorse_done = ev.get("t_endorse_done")
            rec.endorse_err = ev.get("err")
            if ev.get("fabric_txid"):
                self._link_fabric_txid(rec, ev["fabric_txid"])
            return

        if et == "tx_broadcast_done":
            rec = self._rec_for_our_id(ev["tx_id"])
            rec.t_broadcast_done = ev.get("t_broadcast_done")
            rec.broadcast_err = ev.get("err")
            if ev.get("fabric_txid"):
                self._link_fabric_txid(rec, ev["fabric_txid"])
            return

        if et == "tx_commit_done":
            # Submitter-side commit event from SubmittedTransaction.getStatus().
            # This is now our primary source of commit info because the
            # submitter gates on this before moving to the next block.
            rec = self._rec_for_our_id(ev["tx_id"])
            rec.t_commit_seen = ev.get("t_commit_done")
            code = ev.get("commit_code")
            if code is not None:
                rec.commit_validation_code = int(code)
                # Try to map the code — block_listener will overwrite with
                # its own richer name if it sees the same tx.
                rec.commit_validation_name = _VALIDATION_CODE_NAMES.get(
                    int(code), f"CODE_{code}"
                )
            if ev.get("commit_block_num") is not None:
                rec.fabric_block_num = ev["commit_block_num"]
            if ev.get("fabric_txid"):
                self._link_fabric_txid(rec, ev["fabric_txid"])
            return

        if et == "block_done":
            # Submitter side "block sent" summary; useful for debug.
            self._log_lines.append(
                f"[submitter] block_done id={ev.get('block_id')} "
                f"n={ev.get('n')} endorsed={ev.get('endorsed')} "
                f"broadcast={ev.get('broadcast')} committed={ev.get('committed')} "
                f"failed={ev.get('failed')}"
            )
            return

        # ---- Node block-listener events ----
        if et == "block_committed":
            fabric_block_num = ev.get("block_num")
            t_seen_ms = ev.get("t_seen")
            txs = ev.get("txs") or []
            valid_count = ev.get("valid_count", sum(1 for t in txs if t.get("valid")))
            code_counts: Dict[str, int] = defaultdict(int)
            for i, tx in enumerate(txs):
                code_counts[tx.get("validation_name", "UNKNOWN")] += 1
                fabric_txid = tx.get("tx_id")
                if not fabric_txid:
                    continue
                with self._lock:
                    rec = self._by_fabric_txid.get(fabric_txid)
                if rec is None:
                    # Config txs or txs we didn't submit — skip.
                    continue
                rec.t_commit_seen = t_seen_ms
                rec.commit_validation_code = tx.get("validation_code")
                rec.commit_validation_name = tx.get("validation_name")
                rec.fabric_block_num = fabric_block_num
                rec.fabric_block_index = i
            self._blocks.append(BlockRecord(
                fabric_block_num=fabric_block_num,
                t_seen_ms=t_seen_ms,
                num_txs=len(txs),
                valid_count=valid_count,
                validation_code_counts=dict(code_counts),
                tx_ids=[t.get("tx_id", "") for t in txs],
            ))
            return

        # ---- Counter snapshot ----
        if et == "counter_snapshot":
            snap = CounterSnapshot(
                t_ms=ev.get("t_ms", time.time() * 1000.0),
                buffer_pool=ev.get("buffer_pool") or {},
                pool_controller=ev.get("pool_controller") or {},
                pending_queue=ev.get("pending_queue") or {},
            )
            self._counters.append(snap)
            return

        # Unknown types are silently ignored (submitter_ready etc.).

    # ---------------------------------------------------------------- writers

    def write_outputs(self) -> Dict[str, Path]:
        """Flush all in-memory state to disk. Safe to call after ``stop() + join()``."""
        paths = {
            "tx": self.out_dir / "tx.jsonl",
            "blocks": self.out_dir / "blocks.jsonl",
            "counters": self.out_dir / "counters.jsonl",
            "summary": self.out_dir / "summary.json",
            "log": self.out_dir / "collector.log",
        }
        # Per-tx JSONL (only records that had at least a submit).
        with paths["tx"].open("w", encoding="utf-8") as fh:
            with self._lock:
                items = list(self._by_our_id.values())
            for rec in items:
                if rec.t_submit_start is None and rec.t_generated is None:
                    continue
                fh.write(json.dumps(_tx_to_dict(rec), ensure_ascii=False) + "\n")
        # Per-block JSONL.
        with paths["blocks"].open("w", encoding="utf-8") as fh:
            for b in self._blocks:
                fh.write(json.dumps(_block_to_dict(b), ensure_ascii=False) + "\n")
        # Per-counter JSONL.
        with paths["counters"].open("w", encoding="utf-8") as fh:
            for c in self._counters:
                fh.write(json.dumps(dataclasses.asdict(c), ensure_ascii=False) + "\n")
        # Log.
        paths["log"].write_text("\n".join(self._log_lines), encoding="utf-8")
        # Summary.
        with paths["summary"].open("w", encoding="utf-8") as fh:
            fh.write(json.dumps(self._summary(items), indent=2, ensure_ascii=False))
        return paths

    # ---------------------------------------------------------------- summary

    def _summary(self, tx_records: List[TxRecord]) -> Dict[str, Any]:
        wallclock_s = (
            (self.t_run_end_ms - self.t_run_start_ms) / 1000.0
            if self.t_run_end_ms is not None
            else None
        )

        submitted = [r for r in tx_records if r.t_submit_start is not None]
        endorsed = [r for r in submitted if r.t_endorse_done is not None and r.endorse_err is None]
        broadcast = [r for r in endorsed if r.t_broadcast_done is not None and r.broadcast_err is None]
        committed = [r for r in broadcast if r.t_commit_seen is not None]
        valid = [r for r in committed if r.is_valid_commit]

        def pct(latencies, p):
            latencies = sorted([x for x in latencies if x is not None])
            if not latencies:
                return None
            k = min(int(len(latencies) * p / 100.0), len(latencies) - 1)
            return latencies[k]

        def mkstats(latencies):
            xs = [x for x in latencies if x is not None]
            if not xs:
                return None
            return {
                "n": len(xs),
                "p50_ms": pct(xs, 50),
                "p90_ms": pct(xs, 90),
                "p95_ms": pct(xs, 95),
                "p99_ms": pct(xs, 99),
                "max_ms": max(xs),
                "mean_ms": statistics.fmean(xs),
            }

        # Aggregate block stats.
        num_blocks = len(self._blocks)
        avg_valid_per_block = (
            statistics.fmean([b.valid_count for b in self._blocks])
            if self._blocks else 0.0
        )
        avg_block_size = (
            statistics.fmean([b.num_txs for b in self._blocks])
            if self._blocks else 0.0
        )
        mvcc_total = sum(b.mvcc_conflict_count for b in self._blocks)
        endpol_total = sum(b.endorsement_policy_failure_count for b in self._blocks)

        counter_time_series = {}
        if self._counters:
            for field in ("buffer_pool", "pool_controller", "pending_queue"):
                series = []
                for c in self._counters:
                    series.append({"t_ms": c.t_ms, **getattr(c, field, {})})
                counter_time_series[field] = series

        summary: Dict[str, Any] = {
            "run_id": self.run_id,
            "wallclock_s": wallclock_s,
            "n_events_received": self._n_events,
            "counts": {
                "submitted": len(submitted),
                "endorsed": len(endorsed),
                "broadcast": len(broadcast),
                "commit_seen": len(committed),
                "valid": len(valid),
                "endorse_errors": len(submitted) - len(endorsed),
                "broadcast_errors": len(endorsed) - len(broadcast),
                "commit_missing": len(broadcast) - len(committed),
                "commit_invalid": len(committed) - len(valid),
                "blocks_committed": num_blocks,
                "mvcc_conflicts_total": mvcc_total,
                "endorsement_policy_failures_total": endpol_total,
            },
            "throughput": {
                "arrival_tps":
                    (len([r for r in tx_records if r.t_generated is not None]) / wallclock_s)
                    if wallclock_s else None,
                "send_tps": (len(submitted) / wallclock_s) if wallclock_s else None,
                "effective_tps": (len(valid) / wallclock_s) if wallclock_s else None,
                # Steady-state effective TPS uses only the submit-phase span
                # (first t_submit_start → last t_commit_seen), excluding
                # setup and the fixed commit_wait sleep at run end. This is
                # the number that maps to what the paper reports.
                "effective_tps_steady": _steady_eff_tps(tx_records),
                "send_tps_steady": _steady_send_tps(tx_records),
                # Raw TPS = all commits (valid + invalid) per second over the
                # submit-phase span. Measures Fabric's transaction-processing
                # capability independent of scheduling / MVCC outcomes.
                # eff = raw × success_rate.
                "raw_tps_steady": _steady_raw_tps(tx_records),
                "submit_span_s": _submit_span_s(tx_records),
                "success_rate":
                    (len(valid) / len(submitted)) if submitted else None,
                "endorse_success_rate":
                    (len(endorsed) / len(submitted)) if submitted else None,
                "mvcc_abort_rate":
                    (mvcc_total / len(committed)) if committed else None,
            },
            "latencies": {
                "endorse":   mkstats([r.endorse_latency_ms for r in tx_records]),
                "broadcast": mkstats([r.broadcast_latency_ms for r in tx_records]),
                "commit":    mkstats([r.commit_latency_ms for r in tx_records]),
                "e2e":       mkstats([r.e2e_latency_ms for r in tx_records]),
                "queue_wait": mkstats([r.queue_wait_ms for r in tx_records]),
            },
            "blocks": {
                "num_blocks": num_blocks,
                "avg_block_size": avg_block_size,
                "avg_valid_per_block": avg_valid_per_block,
                "validation_code_counts": _sum_dicts(
                    [b.validation_code_counts for b in self._blocks]
                ),
            },
            "counters_time_series": counter_time_series,
        }
        return summary


def _submit_span_s(tx_records: List[TxRecord]) -> Optional[float]:
    """Wall-clock span from the first submit to the last commit-seen event.

    Excludes seed / setup / commit_wait sleep — reflects the actual span
    during which the pipeline was doing useful work.
    """
    starts = [r.t_submit_start for r in tx_records if r.t_submit_start is not None]
    ends   = [r.t_commit_seen  for r in tx_records if r.t_commit_seen  is not None]
    if not starts or not ends:
        return None
    span_ms = max(ends) - min(starts)
    return span_ms / 1000.0 if span_ms > 0 else None


def _steady_eff_tps(tx_records: List[TxRecord]) -> Optional[float]:
    span_s = _submit_span_s(tx_records)
    if not span_s:
        return None
    n_valid = sum(1 for r in tx_records if r.is_valid_commit and r.t_commit_seen is not None)
    return n_valid / span_s


def _steady_send_tps(tx_records: List[TxRecord]) -> Optional[float]:
    span_s = _submit_span_s(tx_records)
    if not span_s:
        return None
    n_submitted = sum(1 for r in tx_records if r.t_submit_start is not None)
    return n_submitted / span_s


def _steady_raw_tps(tx_records: List[TxRecord]) -> Optional[float]:
    span_s = _submit_span_s(tx_records)
    if not span_s:
        return None
    n_commit = sum(1 for r in tx_records if r.t_commit_seen is not None)
    return n_commit / span_s


def _tx_to_dict(rec: TxRecord) -> Dict[str, Any]:
    d = dataclasses.asdict(rec)
    d["endorse_latency_ms"] = rec.endorse_latency_ms
    d["broadcast_latency_ms"] = rec.broadcast_latency_ms
    d["commit_latency_ms"] = rec.commit_latency_ms
    d["e2e_latency_ms"] = rec.e2e_latency_ms
    d["queue_wait_ms"] = rec.queue_wait_ms
    d["is_valid_commit"] = rec.is_valid_commit
    return d


def _block_to_dict(rec: BlockRecord) -> Dict[str, Any]:
    d = dataclasses.asdict(rec)
    d["invalid_count"] = rec.invalid_count
    d["mvcc_conflict_count"] = rec.mvcc_conflict_count
    d["endorsement_policy_failure_count"] = rec.endorsement_policy_failure_count
    return d


def _sum_dicts(dicts: List[Dict[str, int]]) -> Dict[str, int]:
    out: Dict[str, int] = defaultdict(int)
    for d in dicts:
        for k, v in d.items():
            out[k] += int(v)
    return dict(out)
