"""Online user simulator — reads a workload.jsonl and pushes into a BufferPool.

The generator (``workload/generator.py``) writes a JSONL where every line
has ``t_generated`` (seconds since workload t0) and ``user_id``. The user
simulator turns those lines into producer traffic:

- **realtime** (default): one dispatcher thread per ``user_id``; each thread
  submits its txs at wall-clock time ``t_sim_start + t_generated``.
  Arrival distribution is bit-identical to the recorded run.
- **asap**: all users fire as fast as the buffer accepts, ignoring
  ``t_generated``. Useful for stress tests.

The buffer pool is the only downstream boundary — anything that reads from
it (Auriga BPC, BaselineGrouper, or a stub) works unchanged. This makes the
simulator the *single* online workload driver for every experiment.

The simulator is idempotent w.r.t. shutdown: ``stop_signal.set()`` and
``join()`` fully drain any pending sleeps and let workers exit within a
loop-iteration bound.
"""

from __future__ import annotations

import json
import threading
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Union

from core.buffer_pool import BufferPool


class WorkloadUserSimulator:
    def __init__(
        self,
        workload_path: Union[str, Path],
        buffer_pool: BufferPool,
        time_mode: str = "realtime",
        stop_signal: Optional[threading.Event] = None,
        speedup: float = 1.0,
    ) -> None:
        if time_mode not in ("realtime", "asap"):
            raise ValueError(f"time_mode must be realtime|asap, got {time_mode!r}")
        self.workload_path = Path(workload_path)
        self.buffer_pool = buffer_pool
        self.time_mode = time_mode
        self.stop_signal = stop_signal or threading.Event()
        self.speedup = float(speedup)
        if self.speedup <= 0:
            raise ValueError("speedup must be > 0")

        # Populated by _load(); {user_id: [tx_dict, ...]} in t_generated order.
        self._per_user: Dict[int, List[dict]] = defaultdict(list)
        self._threads: List[threading.Thread] = []
        self._t_sim_start: Optional[float] = None
        self._n_loaded: int = 0

    # ---------------------------------------------------------------- load

    def _load(self) -> None:
        with self.workload_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                tx = json.loads(line)
                uid = int(tx.get("user_id", 0))
                self._per_user[uid].append(tx)
                self._n_loaded += 1
        # Ensure each user's txs are sorted by t_generated (they should be
        # already from the generator, but re-sort defensively).
        for uid in self._per_user:
            self._per_user[uid].sort(key=lambda r: r.get("t_generated", 0.0))

    # --------------------------------------------------------------- start

    def start(self) -> None:
        if self._threads:
            raise RuntimeError("simulator already started")
        self._load()
        self._t_sim_start = time.time()
        for uid, txs in self._per_user.items():
            t = threading.Thread(
                target=self._worker,
                args=(uid, txs),
                name=f"sim-user-{uid}",
                daemon=True,
            )
            t.start()
            self._threads.append(t)

    def join(self, timeout: Optional[float] = None) -> None:
        for t in self._threads:
            t.join(timeout=timeout)

    def stop(self) -> None:
        self.stop_signal.set()

    # -------------------------------------------------------------- worker

    def _worker(self, uid: int, txs: List[dict]) -> None:
        base = self._t_sim_start
        for tx in txs:
            if self.stop_signal.is_set():
                return
            if self.time_mode == "realtime":
                target = base + tx["t_generated"] / self.speedup
                now = time.time()
                wait = target - now
                if wait > 0:
                    # Sleep in small chunks so stop_signal is responsive.
                    end = now + wait
                    while True:
                        remaining = end - time.time()
                        if remaining <= 0:
                            break
                        if self.stop_signal.is_set():
                            return
                        time.sleep(min(0.05, remaining))
            # Copy so downstream mutations (t_entered_buffer) don't leak
            # back into our in-memory catalog.
            self.buffer_pool.submit(dict(tx))

    # -------------------------------------------------------------- status

    def stats(self) -> Dict[str, object]:
        return {
            "workload_path": str(self.workload_path),
            "time_mode": self.time_mode,
            "speedup": self.speedup,
            "n_users": len(self._per_user),
            "n_txs_loaded": self._n_loaded,
        }

    # -------------------------------------------------------- context mgr

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()
        self.join(timeout=5.0)
