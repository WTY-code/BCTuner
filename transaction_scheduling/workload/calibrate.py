"""Offline workload calibrator — measure the conflict rate of a workload.

Conflict rate here matches the paper's definition (Sec. V.C): for a stream
of transactions split into consecutive windows of ``block_size`` in arrival
order, how many transactions conflict with at least one earlier transaction
*within the same window*. Divide by total transactions.

This is the ``baseline conflict rate`` — what an unscheduled Fabric would
see as MVCC-abort candidates. Under Auriga's DSatur scheduler this collapses
to near-zero (verified 2026-07-03 integration test).

Usage::

    python3 -m workload.calibrate \\
        --workload experiments/mytest/workload.jsonl \\
        --template templates/smallbank_llm.json \\
        --block-size 100

Or run a sweep over profile parameters until we hit a target rate::

    python3 -m workload.calibrate --target-rate 0.20 --tolerance 0.03 \\
        --profile workload/profiles/smallbank.json --seed 42 \\
        --num-users 15 --per-user-tps 15 --duration 20 \\
        --block-size 100 --template templates/smallbank_llm.json
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Dict, List, Tuple

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from adapters.generic import GenericAdapter
from core.data_model import Transaction
from workload.generator import generate
from workload.profile import load_profile


def measure_conflict_rate(
    workload_path: Path,
    template_path: Path,
    block_size: int,
) -> Dict[str, float]:
    """Compute conflict rate for a workload JSONL.

    Returns dict with:
      - conflict_rate       — fraction of txs conflicting with a prior tx in
                              the same FIFO block (paper's Sec. V.C rate)
      - conflict_edge_rate  — fraction of tx-pairs that conflict, averaged
                              across all blocks (window-scoped)
      - num_txs, num_blocks, avg_conflicting_txs_per_block
      - hot_key_fraction    — |top-1 key| / total writes
    """
    adapter = GenericAdapter(str(template_path))

    txs: List[Transaction] = []
    with workload_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            txs.append(Transaction.from_dict(json.loads(line)))

    # Predict RW sets for all txs upfront.
    for tx in txs:
        rw = adapter.predict(tx.data)
        tx.reads = set(rw.reads)
        tx.writes = set(rw.writes)

    # Iterate FIFO blocks of block_size, count conflicting txs.
    total = len(txs)
    conflicting_txs = 0
    pairs_total = 0
    pairs_conflict = 0
    hot_key_writes: Dict[str, int] = {}
    per_block_conf: List[int] = []
    for i in range(0, total, block_size):
        block = txs[i : i + block_size]
        writes_seen: set = set()
        reads_seen: set = set()
        block_conf = 0
        for j, tx in enumerate(block):
            # count conflicts with THIS block's earlier txs
            if tx.writes & writes_seen or tx.writes & reads_seen or tx.reads & writes_seen:
                conflicting_txs += 1
                block_conf += 1
            writes_seen |= tx.writes
            reads_seen |= tx.reads
            for k in tx.writes:
                hot_key_writes[k] = hot_key_writes.get(k, 0) + 1
        # pair-wise conflict count within this block
        n = len(block)
        pairs_total += n * (n - 1) // 2
        # Approximate pairs count by scanning: for each key, C(k,2) pairs
        # But conflict is WW / WR / RW so let's compute exact.
        for a in range(n):
            for b in range(a + 1, n):
                ta, tb = block[a], block[b]
                if ta.writes & tb.writes or ta.writes & tb.reads or ta.reads & tb.writes:
                    pairs_conflict += 1
        per_block_conf.append(block_conf)

    total_writes = sum(hot_key_writes.values())
    top_key_frac = (
        max(hot_key_writes.values()) / total_writes
        if total_writes else 0.0
    )
    return {
        "conflict_rate": conflicting_txs / total if total else 0.0,
        "conflict_edge_rate": pairs_conflict / pairs_total if pairs_total else 0.0,
        "num_txs": total,
        "num_blocks": len(per_block_conf),
        "avg_conflicting_txs_per_block":
            sum(per_block_conf) / len(per_block_conf) if per_block_conf else 0.0,
        "hot_key_fraction": top_key_frac,
    }


# --- calibration sweep ---------------------------------------------------

def sweep_num_accounts(
    profile_path: Path,
    template_path: Path,
    seed: int,
    num_users: int,
    per_user_tps: float,
    duration_s: float,
    block_size: int,
    target_rate: float,
    tolerance: float,
    candidates: List[int],
    zipf_alpha: float = None,
) -> Tuple[int, Dict[str, float]]:
    """Try successive ``num_accounts`` values until conflict_rate matches target.

    Returns the first (num_accounts, stats) whose conflict_rate is within
    tolerance of target_rate. If ``zipf_alpha`` is given, override the
    profile's zipf_alpha for the sweep.
    """
    profile = load_profile(profile_path)
    if zipf_alpha is not None:
        profile.context["zipf_alpha"] = zipf_alpha
    best = None  # closest match seen
    for n_acct in candidates:
        profile.context["num_accounts"] = n_acct
        with tempfile.TemporaryDirectory() as td:
            wl = Path(td) / "wl.jsonl"
            generate(profile, seed=seed, num_users=num_users,
                     per_user_tps=per_user_tps, duration_s=duration_s,
                     out_path=wl)
            stats = measure_conflict_rate(wl, template_path, block_size)
        cr = stats["conflict_rate"]
        print(f"  num_accounts={n_acct:<5d} zipf_a={profile.context.get('zipf_alpha',1.1):.2f}  "
              f"→ conflict_rate={cr*100:5.1f}%  edges={stats['conflict_edge_rate']*100:4.1f}%  "
              f"top_key={stats['hot_key_fraction']*100:4.1f}%")
        if abs(cr - target_rate) <= tolerance:
            return n_acct, stats
        if best is None or abs(cr - target_rate) < abs(best[1]["conflict_rate"] - target_rate):
            best = (n_acct, stats)
    print(f"  no candidate within ±{tolerance*100:.1f}% of target; closest: "
          f"num_accounts={best[0]}, conflict_rate={best[1]['conflict_rate']*100:.1f}%")
    return best


def sweep_zipf_alpha(
    profile_path: Path,
    template_path: Path,
    seed: int,
    num_users: int,
    per_user_tps: float,
    duration_s: float,
    block_size: int,
    target_rate: float,
    tolerance: float,
    alphas: List[float],
    num_accounts: int,
) -> Tuple[float, Dict[str, float]]:
    """Try successive ``zipf_alpha`` values with fixed ``num_accounts``."""
    profile = load_profile(profile_path)
    profile.context["num_accounts"] = num_accounts
    best = None
    for alpha in alphas:
        profile.context["zipf_alpha"] = alpha
        with tempfile.TemporaryDirectory() as td:
            wl = Path(td) / "wl.jsonl"
            generate(profile, seed=seed, num_users=num_users,
                     per_user_tps=per_user_tps, duration_s=duration_s,
                     out_path=wl)
            stats = measure_conflict_rate(wl, template_path, block_size)
        cr = stats["conflict_rate"]
        print(f"  zipf_alpha={alpha:.2f} num_accounts={num_accounts:<5d}  "
              f"→ conflict_rate={cr*100:5.1f}%  edges={stats['conflict_edge_rate']*100:4.1f}%  "
              f"top_key={stats['hot_key_fraction']*100:4.1f}%")
        if abs(cr - target_rate) <= tolerance:
            return alpha, stats
        if best is None or abs(cr - target_rate) < abs(best[1]["conflict_rate"] - target_rate):
            best = (alpha, stats)
    print(f"  no candidate within ±{tolerance*100:.1f}% of target; closest: "
          f"zipf_alpha={best[0]}, conflict_rate={best[1]['conflict_rate']*100:.1f}%")
    return best


# --- CLI ---------------------------------------------------------------

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workload", type=Path,
                    help="Existing workload JSONL to measure (skip generation)")
    ap.add_argument("--template", type=Path, required=True,
                    help="LLM-extracted AST template (for RW prediction)")
    ap.add_argument("--block-size", type=int, default=100)
    # Sweep mode:
    ap.add_argument("--target-rate", type=float, default=None,
                    help="Target conflict rate for --sweep (0..1)")
    ap.add_argument("--tolerance", type=float, default=0.03)
    ap.add_argument("--profile", type=Path, default=None)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--num-users", type=int, default=10)
    ap.add_argument("--per-user-tps", type=float, default=10)
    ap.add_argument("--duration", type=float, default=15)
    ap.add_argument("--candidates", default="200,500,1000,2000,5000,10000",
                    help="Comma-separated num_accounts values to try in sweep")
    ap.add_argument("--sweep-alpha", default=None,
                    help="Comma-separated zipf_alpha values to sweep "
                         "(fixes num_accounts to --num-accounts)")
    ap.add_argument("--num-accounts", type=int, default=1000,
                    help="Fixed num_accounts for --sweep-alpha")
    ap.add_argument("--zipf-alpha", type=float, default=None,
                    help="Override zipf_alpha for --sweep-num-accounts")
    args = ap.parse_args(argv)

    if args.workload:
        stats = measure_conflict_rate(args.workload, args.template, args.block_size)
        print(json.dumps(stats, indent=2))
        return 0

    if args.target_rate is not None and args.profile:
        if args.sweep_alpha:
            alphas = [float(x) for x in args.sweep_alpha.split(",")]
            print(f"[calibrate] sweeping zipf_alpha={alphas} "
                  f"num_accounts={args.num_accounts} "
                  f"target={args.target_rate*100:.0f}% ±{args.tolerance*100:.0f}%")
            pick, stats = sweep_zipf_alpha(
                args.profile, args.template, args.seed, args.num_users,
                args.per_user_tps, args.duration, args.block_size,
                args.target_rate, args.tolerance, alphas, args.num_accounts)
            print()
            print(f"[calibrate] pick: zipf_alpha={pick}, stats:")
        else:
            candidates = [int(x) for x in args.candidates.split(",")]
            print(f"[calibrate] sweeping num_accounts={candidates} "
                  f"zipf_alpha={args.zipf_alpha or 'profile-default'} "
                  f"target={args.target_rate*100:.0f}% ±{args.tolerance*100:.0f}%")
            pick, stats = sweep_num_accounts(
                args.profile, args.template, args.seed, args.num_users,
                args.per_user_tps, args.duration, args.block_size,
                args.target_rate, args.tolerance, candidates,
                zipf_alpha=args.zipf_alpha)
            print()
            print(f"[calibrate] pick: num_accounts={pick}, stats:")
        print(json.dumps(stats, indent=2))
        return 0

    ap.error("Either --workload or (--profile + --target-rate) must be provided")
    return 2


if __name__ == "__main__":
    sys.exit(main())
