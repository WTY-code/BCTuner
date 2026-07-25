"""Offline workload generator.

Deterministic given (profile_path, seed, num_users, per_user_tps, duration).
Emits a JSONL file where each line is::

    {"id": "u3-t42",
     "user_id": 3,
     "functionName": "send_payment",
     "arguments": ["50", "acc0007", "acc0134"],
     "t_generated": <seconds-from-t0, float>}

``t_generated`` is a *relative* delta from the start of the run (seconds
since t0), not a wall-clock timestamp. That makes the file portable across
machines and time-zones — the online user simulator replays these deltas
from its own t0.

Producer model: N virtual users, each firing at ``per_user_tps`` with
jitter drawn from an exponential distribution (Poisson arrival). Total
target TPS = N × per_user_tps.

CLI::

    python3 -m workload.generator \\
        --profile workload/profiles/smallbank.json \\
        --seed 42 --num-users 50 --per-user-tps 10 --duration 60 \\
        --out runs/smallbank-α1.1/workload.jsonl
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Iterable, Optional

# Make ``python3 -m workload.generator`` work when run from repo root.
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from workload import generators
from workload.profile import WorkloadProfile, load_profile


def generate(
    profile: WorkloadProfile,
    seed: int,
    num_users: int,
    per_user_tps: float,
    duration_s: float,
    out_path: Path,
    stream: Optional[Iterable] = None,  # for tests
) -> int:
    """Write a workload JSONL. Returns number of txs written."""
    if num_users <= 0 or per_user_tps <= 0 or duration_s <= 0:
        raise ValueError("num-users, per-user-tps, and duration must be > 0")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # One RNG per user, seeded deterministically from the master seed. That
    # gives us reproducibility even if we later parallelize generation.
    user_rngs = [random.Random(seed * 1_000_003 + uid) for uid in range(num_users)]

    # Per-user context copies so generator caches (e.g. batch counters,
    # zipf CDFs) don't leak between users.
    per_user_ctx = [dict(profile.context) for _ in range(num_users)]

    # Sample all arrival times upfront using Poisson processes. Each user
    # gets a stream of inter-arrival times ~ Exp(per_user_tps).
    # We schedule txs in a max-heap of (t_next, user_id) sorted by time.
    import heapq
    arrivals = []
    for uid in range(num_users):
        rng = user_rngs[uid]
        # First arrival exponentially distributed from t=0.
        t_next = rng.expovariate(per_user_tps)
        if t_next < duration_s:
            heapq.heappush(arrivals, (t_next, uid))

    per_user_seq = [0] * num_users
    n_written = 0
    with out_path.open("w", encoding="utf-8") as fh:
        while arrivals:
            t_generated, uid = heapq.heappop(arrivals)
            if t_generated >= duration_s:
                continue
            rng = user_rngs[uid]
            ctx = per_user_ctx[uid]
            fn = profile.sample_function(rng)
            args = [generators.get(g)(rng, ctx) for g in fn.args]
            seq = per_user_seq[uid]
            per_user_seq[uid] = seq + 1
            rec = {
                "id": f"u{uid}-t{seq}",
                "user_id": uid,
                "functionName": fn.name,
                "arguments": args,
                "t_generated": round(t_generated, 6),
            }
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            n_written += 1
            # Schedule this user's next arrival.
            delta = rng.expovariate(per_user_tps)
            t_next = t_generated + delta
            if t_next < duration_s:
                heapq.heappush(arrivals, (t_next, uid))

    return n_written


# ------------------------------------------------------------------------ CLI


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--profile", required=True, type=Path)
    ap.add_argument("--seed", required=True, type=int)
    ap.add_argument("--num-users", required=True, type=int)
    ap.add_argument("--per-user-tps", required=True, type=float)
    ap.add_argument("--duration", required=True, type=float,
                    help="workload duration in seconds")
    ap.add_argument("--out", required=True, type=Path)
    args = ap.parse_args(argv)

    profile = load_profile(args.profile)
    n = generate(
        profile=profile,
        seed=args.seed,
        num_users=args.num_users,
        per_user_tps=args.per_user_tps,
        duration_s=args.duration,
        out_path=args.out,
    )
    print(f"[generator] wrote {n} txs → {args.out}")
    print(f"           profile={args.profile.name} chaincode={profile.chaincode}")
    print(f"           num_users={args.num_users} per_user_tps={args.per_user_tps}"
          f" duration={args.duration}s seed={args.seed}")
    total_tps = n / args.duration
    print(f"           effective offered rate ≈ {total_tps:.1f} tx/s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
