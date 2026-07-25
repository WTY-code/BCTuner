"""Workload profile loader + validator.

Profile JSON schema::

    {
      "chaincode": "smallbank",
      "context": {                     # forwarded to generators as-is
        "num_accounts": 200,
        "zipf_alpha": 1.1,
        "amount_lo": 1,
        "amount_hi": 1000
      },
      "functions": [
        {
          "name": "send_payment",
          "weight": 0.35,
          "args": ["amount_int", "acct_id_uniform", "acct_id_zipf"]
        },
        ...
      ]
    }

Weights must be positive; they're normalized at load time. Each ``args``
entry is the name of a generator registered in ``workload/generators.py``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Union

from workload import generators


@dataclass
class ProfileFunction:
    name: str          # chaincode function name
    weight: float      # relative sampling weight (normalized in load)
    args: List[str]    # ordered list of generator names


@dataclass
class WorkloadProfile:
    chaincode: str
    context: Dict[str, object]
    functions: List[ProfileFunction] = field(default_factory=list)
    _weights_cumulative: List[float] = field(default_factory=list)

    def sample_function(self, rng) -> ProfileFunction:
        r = rng.random()
        lo, hi = 0, len(self._weights_cumulative) - 1
        while lo < hi:
            mid = (lo + hi) // 2
            if self._weights_cumulative[mid] < r:
                lo = mid + 1
            else:
                hi = mid
        return self.functions[lo]


def load_profile(path: Union[str, Path]) -> WorkloadProfile:
    p = Path(path)
    raw = json.loads(p.read_text(encoding="utf-8"))
    _validate(raw, path=p)
    funcs = [
        ProfileFunction(
            name=fn["name"],
            weight=float(fn["weight"]),
            args=list(fn.get("args", [])),
        )
        for fn in raw["functions"]
    ]
    total = sum(f.weight for f in funcs)
    if total <= 0:
        raise ValueError(f"{p}: sum of function weights must be > 0")
    cum = []
    acc = 0.0
    for f in funcs:
        acc += f.weight / total
        cum.append(acc)
    return WorkloadProfile(
        chaincode=raw["chaincode"],
        context=dict(raw.get("context", {})),
        functions=funcs,
        _weights_cumulative=cum,
    )


def _validate(raw: dict, path: Path) -> None:
    if "chaincode" not in raw:
        raise ValueError(f"{path}: missing 'chaincode'")
    if "functions" not in raw or not isinstance(raw["functions"], list):
        raise ValueError(f"{path}: 'functions' must be a list")
    for i, fn in enumerate(raw["functions"]):
        for req in ("name", "weight"):
            if req not in fn:
                raise ValueError(f"{path}: functions[{i}] missing {req!r}")
        for j, g in enumerate(fn.get("args", [])):
            if not isinstance(g, str):
                raise ValueError(f"{path}: functions[{i}].args[{j}] not a string")
            try:
                generators.get(g)
            except KeyError as exc:
                raise ValueError(
                    f"{path}: functions[{i}].args[{j}] references unknown generator {g!r}"
                ) from exc
