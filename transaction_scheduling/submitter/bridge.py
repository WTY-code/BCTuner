"""Python-side bridge to the Node.js Fabric submitter + block listener.

Spawns two persistent Node processes:

  submit.js         reads NDJSON block directives on stdin, submits via
                    fabric-gateway, emits per-tx events on stdout.
  block_listener.js subscribes to block events on the same channel, emits
                    per-block commit events on stdout (with per-tx
                    validation codes).

Usage::

    cfg = SubmitterConfig(
        ccp_path="/root/workspace2/auriga/infra/topology-manager/artifacts"
                 "/connection-profiles/connection-org1.yaml",
        msp_id="Org1MSP",
        user_msp_dir="/root/workspace2/auriga/infra/topology-manager/artifacts"
                     "/crypto-config/peerOrganizations/org1.example.com/users"
                     "/User1@org1.example.com/msp",
        channel="mychannel",
        chaincode="smallbank",
    )
    bridge = NodeSubmitter(cfg, event_sink=my_queue.put)
    bridge.start()
    bridge.submit_block(block_id=1, txs=[{"id":"tx-1", "functionName":"send_payment", "arguments":[...]}, ...])
    ...
    bridge.stop()

The ``event_sink`` callback is invoked (from the reader thread) for every
NDJSON event dict emitted by either Node process. The caller is responsible
for offloading heavy work off that callback.
"""

from __future__ import annotations

import json
import os
import queue
import re
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional


SUBMITTER_DIR = Path(__file__).resolve().parent
SUBMIT_JS = str(SUBMITTER_DIR / "submit.js")
BLOCK_LISTENER_JS = str(SUBMITTER_DIR / "block_listener.js")


@dataclass
class SubmitterConfig:
    ccp_path: str
    msp_id: str
    user_msp_dir: str
    channel: str = "mychannel"
    chaincode: str = "smallbank"
    peer_name: Optional[str] = None       # None → first peer in CCP
    start_block: Optional[int] = None     # for block listener; None = tip
    node_bin: str = "node"
    # Gate mode between blocks:
    #   "commit"    — submit.js awaits every tx's getStatus() before Promise.all
    #                 resolves for the block → next block starts only after
    #                 world-state is fully applied. 0 MVCC but low TPS.
    #   "broadcast" — submit.js returns after every tx's submit() (envelope
    #                 accepted by orderer). getStatus() fires in the background
    #                 so tx_commit_done events still land for metrics. Next
    #                 block starts immediately → high TPS, small MVCC risk.
    gate_mode: str = "commit"
    inter_block_pause_ms: int = 0
    # Rate control (P2.6). 0 = unlimited (backward-compat).
    target_tps: int = 0
    max_concurrency: int = 0


class NodeSubmitter:
    """Spawn + drive submit.js (persistent) and block_listener.js.

    All NDJSON events from either process are delivered to ``event_sink``
    on a single reader thread. Events look like::

        {"type": "tx_submit", "tx_id": "...", "fabric_txid": "...", "t_submit_start": 1234}
        {"type": "tx_endorse_done", ...}
        {"type": "tx_broadcast_done", ...}
        {"type": "block_done", ...}
        {"type": "block_committed", "block_num": N, "txs": [...]}

    The caller drives block submission by calling ``submit_block()``. The
    bridge is otherwise passive: it does not wait for or gate on any event
    itself. If the caller wants a "block A must broadcast_done before
    submitting block B" invariant, they enforce it at their end (e.g. by
    counting `tx_broadcast_done` events before the next put).
    """

    def __init__(
        self,
        cfg: SubmitterConfig,
        event_sink: Callable[[dict], None],
        start_listener: bool = True,
    ) -> None:
        self.cfg = cfg
        self.event_sink = event_sink
        self._start_listener = start_listener
        self._submit_proc: Optional[subprocess.Popen] = None
        self._listener_proc: Optional[subprocess.Popen] = None
        self._reader_threads: List[threading.Thread] = []
        self._stopped = threading.Event()
        self._submit_lock = threading.Lock()

    # ------------------------------------------------------------------ start

    def start(self) -> None:
        env = os.environ.copy()
        submit_args = [
            self.cfg.node_bin, SUBMIT_JS,
            "--ccp", self.cfg.ccp_path,
            "--msp-id", self.cfg.msp_id,
            "--user-msp-dir", self.cfg.user_msp_dir,
            "--channel", self.cfg.channel,
            "--chaincode", self.cfg.chaincode,
            "--gate-mode", self.cfg.gate_mode,
            "--inter-block-pause-ms", str(self.cfg.inter_block_pause_ms),
            "--target-tps", str(self.cfg.target_tps),
            "--max-concurrency", str(self.cfg.max_concurrency),
        ]
        if self.cfg.peer_name:
            submit_args += ["--peer", self.cfg.peer_name]

        self._submit_proc = subprocess.Popen(
            submit_args,
            cwd=str(SUBMITTER_DIR),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            bufsize=1,
            text=True,
        )
        self._start_reader("submit-stdout", self._submit_proc.stdout, is_events=True)
        self._start_reader("submit-stderr", self._submit_proc.stderr, is_events=False)

        # Wait for submitter_ready event via a short spin.
        self._wait_ready(self._submit_proc, "submitter_ready", timeout=15)

        if self._start_listener:
            listener_args = [
                self.cfg.node_bin, BLOCK_LISTENER_JS,
                "--ccp", self.cfg.ccp_path,
                "--msp-id", self.cfg.msp_id,
                "--user-msp-dir", self.cfg.user_msp_dir,
                "--channel", self.cfg.channel,
                "--chaincode", self.cfg.chaincode,
            ]
            if self.cfg.peer_name:
                listener_args += ["--peer", self.cfg.peer_name]
            if self.cfg.start_block is not None:
                listener_args += ["--start-block", str(self.cfg.start_block)]

            self._listener_proc = subprocess.Popen(
                listener_args,
                cwd=str(SUBMITTER_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                bufsize=1,
                text=True,
            )
            self._start_reader("listener-stdout", self._listener_proc.stdout, is_events=True)
            self._start_reader("listener-stderr", self._listener_proc.stderr, is_events=False)
            self._wait_ready(self._listener_proc, "listener_ready", timeout=15)

    def _start_reader(self, name: str, stream, is_events: bool) -> None:
        def _run():
            for line in stream:
                line = line.rstrip("\n")
                if not line:
                    continue
                if is_events:
                    try:
                        ev = json.loads(line)
                    except json.JSONDecodeError:
                        # Log-shaped output on the stdout channel; forward as a stderr-like event
                        self.event_sink({"type": "log", "source": name, "line": line})
                        continue
                    self.event_sink(ev)
                else:
                    # Node stderr — just tag and forward.
                    self.event_sink({"type": "log", "source": name, "line": line})
        t = threading.Thread(target=_run, name=name, daemon=True)
        t.start()
        self._reader_threads.append(t)

    def _wait_ready(self, proc: subprocess.Popen, ready_type: str, timeout: float) -> None:
        # We rely on the reader thread having captured a *_ready event.
        # We can't easily peek events without buffering, so use a shared
        # Event set by a sentinel wrapper on event_sink for now.
        deadline = time.time() + timeout
        seen = threading.Event()
        orig = self.event_sink

        def wrapped(ev: dict):
            if ev.get("type") == ready_type:
                seen.set()
            orig(ev)

        self.event_sink = wrapped
        try:
            while time.time() < deadline:
                if seen.wait(0.2):
                    return
                if proc.poll() is not None:
                    raise RuntimeError(
                        f"Node process exited before {ready_type} "
                        f"(rc={proc.returncode})"
                    )
            raise TimeoutError(f"did not see {ready_type} within {timeout}s")
        finally:
            self.event_sink = orig

    # ---------------------------------------------------------------- submit

    def submit_block(self, block_id: int, txs: List[dict]) -> None:
        """Send one block directive over stdin to submit.js.

        Thread-safe (holds an internal lock so multiple callers don't
        interleave partial lines on the pipe).
        """
        if self._submit_proc is None or self._submit_proc.stdin is None:
            raise RuntimeError("submitter not started")
        msg = json.dumps({"type": "block", "block_id": block_id, "txs": txs})
        with self._submit_lock:
            self._submit_proc.stdin.write(msg + "\n")
            self._submit_proc.stdin.flush()

    # ----------------------------------------------------------------- stop

    def stop(self, timeout: float = 10.0) -> None:
        if self._stopped.is_set():
            return
        self._stopped.set()
        try:
            if self._submit_proc and self._submit_proc.stdin:
                self._submit_proc.stdin.write(json.dumps({"type": "stop"}) + "\n")
                self._submit_proc.stdin.flush()
                self._submit_proc.stdin.close()
        except Exception:
            pass
        for proc, name in (
            (self._submit_proc, "submitter"),
            (self._listener_proc, "listener"),
        ):
            if proc is None:
                continue
            try:
                proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                proc.terminate()
                try:
                    proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    proc.kill()

    # ---------------------------------------------------------- context mgr

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()


# =============================================================================
# CCP peer-selection helper
# =============================================================================

def peers_from_ccp(ccp_path: str) -> List[dict]:
    """Read a Fabric connection profile YAML and return a list of peers.

    Each element is ``{"name": str, "url": str, "org": str, "machine": str}``.
    ``org`` is inferred from the peer name (``peerN.orgK...``); ``machine``
    is the host part of the URL (before the port), used to spread submitters
    across VMs.
    """
    import yaml
    with open(ccp_path, "r", encoding="utf-8") as fh:
        ccp = yaml.safe_load(fh)
    out: List[dict] = []
    for name, entry in (ccp.get("peers") or {}).items():
        url = entry.get("url") or ""
        # url = grpcs://192.168.0.153:7051
        host = url.split("://", 1)[-1].split(":", 1)[0] if "://" in url else url
        m = re.search(r"\.org(\d+)\.", name)
        org = f"Org{m.group(1)}" if m else "?"
        out.append({"name": name, "url": url, "org": org, "machine": host})
    return out


def select_balanced_peers(all_peers: List[dict], n: int) -> List[dict]:
    """Pick ``n`` peers rotating across (org, machine) to spread load.

    Deterministic: sort by (org, machine, name), then interleave orgs so
    consecutive workers hit different orgs and different machines.
    """
    if n <= 0:
        raise ValueError("n must be >= 1")
    if n > len(all_peers):
        raise ValueError(f"requested {n} peers but CCP only has {len(all_peers)}")
    # Group by org, sort each group by (machine, name).
    from collections import defaultdict
    buckets: Dict[str, List[dict]] = defaultdict(list)
    for p in all_peers:
        buckets[p["org"]].append(p)
    for org in buckets:
        buckets[org].sort(key=lambda p: (p["machine"], p["name"]))
    # Round-robin across orgs.
    orgs = sorted(buckets.keys())
    picked: List[dict] = []
    while len(picked) < n:
        made_progress = False
        for org in orgs:
            if buckets[org]:
                picked.append(buckets[org].pop(0))
                made_progress = True
                if len(picked) == n:
                    break
        if not made_progress:
            break
    return picked


# =============================================================================
# MultiPeerSubmitter — spawn N NodeSubmitter workers, per-tx round-robin
# =============================================================================

class MultiPeerSubmitter:
    """A pool of N NodeSubmitters (one per peer), dispatching txs with a
    per-tx round-robin shard: tx at global index ``i`` goes to
    ``workers[i % N]``. The counter is preserved across ``submit_block``
    calls so multiple sequential blocks continue the same shard stride —
    equivalent to Caliper's ``customWorkLoad.js`` sharding.

    Rationale: the earlier per-block dispatch (``block_id % N``) sent
    whole DSatur groups to different workers, so 15 workers each fired
    a *different* DSatur group in parallel and the orderer saw
    envelopes interleaved from 15 groups → cross-block MVCC. Per-tx
    sharding puts all 15 workers on the *same* global tx index at any
    wall-clock moment, so the orderer sees envelopes in near-schedule
    order → each Fabric 200 ms block cut lands inside a DSatur group.

      - ``start()``  starts all N workers in parallel (each takes ~10s).
      - ``submit_block(block_id, txs)``  → shards txs per-tx via ``i % N``.
        Each worker receives its share of this block as ONE sub-block
        with the same ``block_id`` (for metric traceability), so
        submit.js's ``enqueueBlock`` token bucket paces the worker's
        share at ``target_tps / N``. The global tx counter carries over
        across successive ``submit_block`` calls, preserving the shard
        stride across DSatur block boundaries.
      - ``stop()``   stops all workers.

    Only worker 0 also runs `block_listener.js`; the listener sees all
    committed blocks regardless of which peer submitted them, so running
    N listeners would just duplicate `block_committed` events.

    All worker stdout streams merge into a single ``event_sink``. Every
    event carries a globally unique ``tx_id`` (from our workload generator
    or seed function), so the `MetricCollector` can merge without knowing
    which worker produced which event.
    """

    def __init__(
        self,
        base_cfg: SubmitterConfig,
        peers: List[str],
        event_sink: Callable[[dict], None],
    ) -> None:
        if not peers:
            raise ValueError("MultiPeerSubmitter requires at least one peer")
        self.base_cfg = base_cfg
        self.peer_names: List[str] = list(peers)
        self.event_sink = event_sink
        self._workers: List[NodeSubmitter] = []
        # Build one NodeSubmitter per peer. Only worker 0 owns the listener.
        for i, peer in enumerate(self.peer_names):
            cfg = SubmitterConfig(
                ccp_path=base_cfg.ccp_path,
                msp_id=base_cfg.msp_id,
                user_msp_dir=base_cfg.user_msp_dir,
                channel=base_cfg.channel,
                chaincode=base_cfg.chaincode,
                peer_name=peer,
                start_block=base_cfg.start_block,
                node_bin=base_cfg.node_bin,
                gate_mode=base_cfg.gate_mode,
                inter_block_pause_ms=base_cfg.inter_block_pause_ms,
                # Split target_tps across workers so aggregate = base target.
                # 0 (unlimited) stays 0.
                target_tps=(base_cfg.target_tps // len(peers)
                            if base_cfg.target_tps > 0 else 0),
                max_concurrency=base_cfg.max_concurrency,
            )
            w = NodeSubmitter(cfg, event_sink=event_sink,
                              start_listener=(i == 0))
            self._workers.append(w)
        # Global tx-index counter for per-tx round-robin sharding. Persists
        # across submit_block calls so the shard stride is continuous
        # regardless of DSatur block boundaries.
        self._tx_counter: int = 0

    # ---------------------------------------------------------------- start

    def start(self) -> None:
        # Start workers in parallel — each takes ~10s to connect + wait for
        # submitter_ready. Sequential would multiply that by N.
        threads: List[threading.Thread] = []
        errors: List[BaseException] = []

        def _start(w: NodeSubmitter):
            try:
                w.start()
            except BaseException as e:
                errors.append(e)

        for w in self._workers:
            t = threading.Thread(target=_start, args=(w,), daemon=True)
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        if errors:
            # Roll back any workers that did come up.
            for w in self._workers:
                try:
                    w.stop(timeout=3)
                except Exception:
                    pass
            raise errors[0]

    # ---------------------------------------------------------------- submit

    def submit_block(self, block_id: int, txs: List[dict]) -> None:
        """Shard txs across all workers via per-tx global-index round-robin.

        For each tx, worker w = (global counter) % N. Per-worker shard is
        sent as one sub-block with the ORIGINAL ``block_id`` (metric
        traceability) — submit.js's ``enqueueBlock`` treats each as an
        independent unit paced by its own token bucket. Empty shards are
        skipped so a tiny block (fewer txs than workers) doesn't stall
        workers with 0 assignments.
        """
        n = len(self._workers)
        shards: List[List[dict]] = [[] for _ in range(n)]
        for tx in txs:
            w = self._tx_counter % n
            self._tx_counter += 1
            shards[w].append(tx)
        for w, shard in enumerate(shards):
            if shard:
                self._workers[w].submit_block(block_id, shard)

    # ---------------------------------------------------------------- stop

    def stop(self, timeout: float = 10.0) -> None:
        threads: List[threading.Thread] = []
        for w in self._workers:
            t = threading.Thread(target=lambda w=w: w.stop(timeout=timeout),
                                 daemon=True)
            t.start()
            threads.append(t)
        for t in threads:
            t.join(timeout=timeout + 2)

    # -------------------------------------------------------- context mgr

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()
