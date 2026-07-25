"""Argument generators for the workload generator.

Each generator is a callable ``Callable[[random.Random, dict], str]`` — it
receives the RNG (so the whole workload is deterministic given a seed) and
a small ``context`` dict of runtime facts (e.g. ``{"num_accounts": 200}``
from the profile). It returns a *string* — Fabric chaincode args are
always strings.

Register your generator via ``@register("name")`` and reference it from
profile JSON via the same name.
"""

from __future__ import annotations

import random
from typing import Callable, Dict

Generator = Callable[[random.Random, Dict], str]

_REGISTRY: Dict[str, Generator] = {}


def register(name: str):
    def deco(fn: Generator) -> Generator:
        _REGISTRY[name] = fn
        return fn
    return deco


def get(name: str) -> Generator:
    if name not in _REGISTRY:
        raise KeyError(f"unknown arg generator {name!r}; known: {sorted(_REGISTRY)}")
    return _REGISTRY[name]


# ------------------------------------------------------------------ built-ins

@register("acct_id_uniform")
def _acct_id_uniform(rng: random.Random, ctx: Dict) -> str:
    """Uniform random account id over ``ctx['num_accounts']``.

    Emits ``"acc0000"``..``"acc<N-1>"`` (4-digit zero-padded).
    """
    n = ctx.get("num_accounts", 100)
    return f"acc{rng.randrange(n):04d}"


@register("acct_id_zipf")
def _acct_id_zipf(rng: random.Random, ctx: Dict) -> str:
    """Zipf-distributed account id — models hot-key skew.

    Reads ``ctx['num_accounts']`` and ``ctx['zipf_alpha']`` (default 1.1).
    Uses inverse-CDF sampling with a truncated Zipf so the distribution is
    fully deterministic given the RNG state.
    """
    n = ctx.get("num_accounts", 100)
    alpha = float(ctx.get("zipf_alpha", 1.1))
    # Precompute per-run and cache on ctx.
    weights = ctx.get("_zipf_weights")
    if weights is None or ctx.get("_zipf_cached_n") != n or ctx.get("_zipf_cached_alpha") != alpha:
        weights = [1.0 / ((k + 1) ** alpha) for k in range(n)]
        total = sum(weights)
        # cumulative
        cum = []
        acc = 0.0
        for w in weights:
            acc += w / total
            cum.append(acc)
        ctx["_zipf_weights"] = cum
        ctx["_zipf_cached_n"] = n
        ctx["_zipf_cached_alpha"] = alpha
        weights = cum
    r = rng.random()
    # Binary search — lists are small (≤ a few thousand).
    lo, hi = 0, len(weights) - 1
    while lo < hi:
        mid = (lo + hi) // 2
        if weights[mid] < r:
            lo = mid + 1
        else:
            hi = mid
    return f"acc{lo:04d}"


@register("amount_int")
def _amount_int(rng: random.Random, ctx: Dict) -> str:
    lo = int(ctx.get("amount_lo", 1))
    hi = int(ctx.get("amount_hi", 1000))
    return str(rng.randint(lo, hi))


@register("small_int")
def _small_int(rng: random.Random, ctx: Dict) -> str:
    return str(rng.randint(1, 100))


@register("prefix_id")
def _prefix_id(rng: random.Random, ctx: Dict) -> str:
    n = ctx.get("num_prefixes", 10)
    return f"p{rng.randrange(n)}"


@register("batch_start")
def _batch_start(rng: random.Random, ctx: Dict) -> str:
    """Increasing 'start' for IOHeavy: rotates through a mod ring."""
    ring = int(ctx.get("start_ring", 10000))
    batch = int(ctx.get("batch_size", 50))
    counter = ctx.get("_batch_counter", 0)
    v = (counter * batch) % ring
    ctx["_batch_counter"] = counter + 1
    return str(v)


@register("batch_size")
def _batch_size(rng: random.Random, ctx: Dict) -> str:
    return str(int(ctx.get("batch_size", 50)))
