#!/usr/bin/env python3
"""LLM-based chaincode path template extractor with preprocessing + retry loop.

Pipeline::

    Go source files → Preprocessor (strip boilerplate, find handlers)
                   → Prompt (code + schema + example)
                   → LLM generate
                   → Validator (JSON schema + RW-set equivalences)
                   → Retry with error feedback (up to max_retries)
                   → Output template JSON

Usage::

    python3 llm_extract.py \\
        --source infra/topology-manager/chaincode/token-erc-20/main.go \\
        --output templates/token_erc20_llm.json \\
        --validate --retries 3
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path
from textwrap import dedent
from typing import Any, Dict, List, Optional, Tuple
from urllib import error as urlerror, request as urlrequest

# ---- LLM API ----
# Credentials are read from environment variables.
# Copy .env.example to .env at the repo root and fill in your values,
# or set the variables directly in your shell before running this script.

BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
API_KEY  = os.getenv("DEEPSEEK_API_KEY",  "")
MODEL    = os.getenv("DEEPSEEK_MODEL",     "deepseek-chat")

# ---- Step 1: Preprocessor ----

RE_INVOKE_BODY = re.compile(
    r'func\s+\([^)]+\)\s+Invoke\([^)]+\)[^{]*\{([^}]+)\}',
    re.MULTILINE | re.DOTALL,
)


def preprocess(source: str) -> dict:
    """Extract handler functions from chaincode source.

    Returns::

        {"fn_names": [...], "condensed": "<relevant source subset>"}
    """
    # Find Invoke switch cases to identify handler functions
    invoke_match = RE_INVOKE_BODY.search(source)
    if not invoke_match:
        return {"fn_names": [], "condensed": source[:8000], "source_len": len(source)}
    invoke_body = invoke_match.group(1)
    fn_refs = set(re.findall(r'case\s+"([^"]+)"\s*:', invoke_body))
    fn_refs |= set(re.findall(r'return\s+c\.(\w+)\(', invoke_body))

    # Condense: remove license header, keep package + imports + all func bodies
    # Strip Apache license block (lines between /* and */)
    condensed = re.sub(r'/\*.*?\*/\s*', '', source, flags=re.DOTALL)
    condensed = re.sub(r'//\s*SPDX.*\n', '', condensed)
    # Remove empty lines
    condensed = re.sub(r'\n{3,}', '\n\n', condensed)

    return {
        "fn_names": sorted(fn_refs),
        "condensed": condensed[:12000],
        "source_len": len(source),
    }


# ---- Step 2: Prompt Builder ----

AST_SCHEMA = dedent("""\
{
  "chaincode": "<name>",
  "histograms": ["Field1", ...],
  "joint_groups": [["Field1", "Field2"], ...],
  "bypass_functions": ["readOnlyFn1", ...],
  "functions": {
    "<fn_name>": {
      "body": [
        {"type": "read",  "key": "prefix:${arg.N}"},
        {"type": "write", "key": "prefix:${arg.N}"},
        {"type": "iterate", "var": "i", "start": "N", "count": "${arg.N}",
         "body": [ ... ]},
        {"type": "branch",
         "condition": {"kind": "linear_1d", "field": "FieldName",
                       "op": ">=", "rhs": "${arg.N}"},
         "then_tag": "success",
         "else_tag": "abort",
         "then": [ ... ],
         "else": [ ... ]}
      ]
    }
  }
}
CONDITION KINDS: predicate | linear_1d | complex
BOTH then_tag AND else_tag are REQUIRED on every branch node.""")

AST_EXAMPLE = dedent("""\
{
  "chaincode": "token_erc20",
  "histograms": ["Balance"],
  "bypass_functions": ["balance_of", "total_supply"],
  "functions": {
    "mint": {
      "body": [
        {"type": "read",  "key": "balance:${arg.0}"},
        {"type": "write", "key": "balance:${arg.0}"}
      ]
    },
    "transfer": {
      "body": [
        {"type": "read", "key": "balance:${arg.0}"},
        {"type": "read", "key": "balance:${arg.1}"},
        {"type": "branch",
          "condition": {"kind": "linear_1d", "field": "Balance",
                        "op": ">=", "rhs": "${arg.2}"},
          "then_tag": "success",
          "then": [
            {"type": "write", "key": "balance:${arg.0}"},
            {"type": "write", "key": "balance:${arg.1}"}
          ],
          "else": [
            {"type": "read", "key": "balance:${arg.0}"},
            {"type": "read", "key": "balance:${arg.1}"}
          ]
        }
      ]
    },
    "batch_put": {
      "body": [
        {"type": "iterate", "var": "i", "start": "${arg.0}", "count": "${arg.1}",
         "body": [
           {"type": "write", "key": "k:${arg.2}_${arg.3}"}
         ]}
      ]
    },
    "balance_of": {
      "body": [
        {"type": "read", "key": "balance:${arg.0}"}
      ]
    }
  }
}

Explanation:
- "transfer": Branch on Balance >= amount (arg.2).  Success writes both balances.
- "batch_put": args = [start, count, prefix].  Inside the iterate body,
  the iteration index is appended as extra arg, so ${arg.3} = loop variable.
  NEVER use ${i} — always use ${arg.N} with the correct index.
- Key prefixes are semantic ("balance:", "k:"), derived from code.""")

SYSTEM_PROMPT = dedent("""\
You are an expert in analyzing Hyperledger Fabric chaincode written in Go.
Your ONLY task: extract execution paths and produce a valid AST template JSON.

DETECTING PROBABILITY BRANCHES:
- A PROBABILITY branch has a NUMERIC comparison (>=, <, >, <=) between a value
  parsed from state and a function argument.
  Example: `if fromBal < amount { return error }` → Branch on Balance >= amount.
- The field name MUST be semantic: CheckingBalance, SavingsBalance, Balance, etc.
  Infer it from the code context.  NEVER use generic names like "exists" or "val".
- ADD the field to the top-level "histograms" array.

DETECTING GUARD PATTERNS (NO branch needed):
- Nil checks (`if bytes == nil { return error }`) → omit branch.
- Argument validation (`if len(args) < N { return error }`) → omit branch.
- Error returns after a PutState call → omit branch (failure path has no writes).

DETECTING ITERATE:
- for loops `for i := 0; i < N; i++` using `stub.GetState`/`PutState` inside
  become iterate nodes.  The loop count references an argument: "${arg.N}".
- CRITICAL: inside the iterate body, the iteration variable is available as an
  extra argument appended to the original args.  If the function has M args
  (arg.0 through arg.M-1), then inside the iterate body, the iteration value
  is at ${arg.M}.  For nested iterates, the outer var is at ${arg.M} and the
  inner at ${arg.M+1}.  NEVER use literal `${i}` or `${var}` — always use
  the ${arg.N} form with the correct index.

DETECTING BYPASS:
- Functions that ONLY call GetState (no PutState) are read-only → add to
  "bypass_functions".  Include them in "functions" too.

KEY EXPRESSIONS:
- Use ${arg.N} (0-indexed).  Prefix should be semantic ("balance:", "stock:",
  "warehouse:", etc.), derived from the code's key construction pattern.
- For Fabric composite keys, join parts with ":" as separator.

OUTPUT ONLY VALID JSON.  Do NOT wrap in markdown fences.""")


def build_prompt(source: str, feedback: str = "") -> str:
    """Build user prompt with optional validation feedback for retries."""
    parts = []

    parts.append("## JSON Schema (MUST follow exactly)\n")
    parts.append(AST_SCHEMA)
    parts.append("\n## Complete Example\n")
    parts.append(AST_EXAMPLE)

    if feedback:
        parts.append("\n## PREVIOUS ATTEMPT FAILED — FIX THESE ERRORS:\n")
        parts.append(feedback)
        parts.append("\nGenerate a CORRECTED template JSON.\n")

    parts.append("## Chaincode Source\n")
    parts.append("```go")
    parts.append(source)
    parts.append("```")

    return "\n".join(parts)


# ---- Step 3: LLM Call ----

def call_llm(prompt: str) -> str:
    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.1,
        "max_tokens": 16384,
    }
    url = f"{BASE_URL.rstrip('/')}/chat/completions"
    data = json.dumps(payload).encode("utf-8")
    req = urlrequest.Request(url, data=data, headers={
        "Content-Type": "application/json",
        "Authorization": f"Bearer {API_KEY}",
    }, method="POST")
    try:
        with urlrequest.urlopen(req, timeout=300) as resp:
            body = resp.read().decode("utf-8")
    except urlerror.HTTPError as exc:
        err = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"LLM HTTP {exc.code}: {err}") from exc
    return json.loads(body)["choices"][0]["message"]["content"]


# ---- Step 4: Validator ----

def parse_json(text: str) -> dict:
    """Extract JSON from LLM response."""
    text = text.strip()
    if text.startswith("{"):
        return json.loads(text)
    for fence in (r"```json\s*(\{.*?\})\s*```", r"```\s*(\{.*?\})\s*```"):
        m = re.search(fence, text, flags=re.DOTALL)
        if m:
            return json.loads(m.group(1))
    raise ValueError(f"Cannot extract JSON. First 200 chars: {text[:200]}")


def validate(template: dict) -> List[str]:
    """Validate template structure. Empty list = valid."""
    errors = []
    if "chaincode" not in template:
        errors.append("missing 'chaincode'")
    if "functions" not in template:
        errors.append("missing 'functions'")
        return errors
    for fn, fd in template["functions"].items():
        if "body" not in fd:
            errors.append(f"{fn}: missing 'body'")
        elif not isinstance(fd["body"], list):
            errors.append(f"{fn}: 'body' is not a list")
        else:
            errors.extend(_validate_nodes(fd["body"], fn))
    return errors


def _validate_nodes(nodes: List[dict], path: str = "") -> List[str]:
    errors = []
    VALID_TYPES = {"read", "write", "iterate", "branch", "composite_key", "monte_carlo_fallback"}
    VALID_KINDS = {"predicate", "linear_1d", "complex"}
    for i, n in enumerate(nodes):
        p = f"{path}[{i}]"
        t = n.get("type", "")
        if t not in VALID_TYPES:
            errors.append(f"{p}: unknown type '{t}' (valid: {sorted(VALID_TYPES)})")
        if t in ("read", "write") and "key" not in n:
            errors.append(f"{p}: {t} missing 'key'")
        if t == "iterate":
            if "body" not in n:
                errors.append(f"{p}: iterate missing 'body'")
            else:
                errors.extend(_validate_nodes(n["body"], p))
        if t == "branch":
            if "condition" not in n:
                errors.append(f"{p}: branch missing 'condition'")
            else:
                kind = n["condition"].get("kind", "")
                if kind not in VALID_KINDS:
                    errors.append(
                        f"{p}: unknown condition kind {kind!r} (valid: {sorted(VALID_KINDS)})"
                    )
            if "then_tag" not in n:
                errors.append(f"{p}: branch missing 'then_tag'")
            if "else_tag" not in n:
                errors.append(f"{p}: branch missing 'else_tag'")
            for br in ("then", "else"):
                if br in n and isinstance(n[br], list):
                    errors.extend(_validate_nodes(n[br], f"{p}.{br}"))
    return errors


def validate_rwset(template: dict, source_path: str) -> List[str]:
    """Run RW-set evaluation with numeric test args to catch structural errors."""
    errors = []
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from ast_engine.evaluator import Evaluator

    eval_ = Evaluator(histogram_stores={}, theta=0.95)

    for fn_name, fn_def in template.get("functions", {}).items():
        try:
            # Use numeric test args so branch conditions can parse floats
            eval_.eval(fn_def, ["0", "1", "100", "5", "42", "7"])
        except Exception as e:
            errors.append(f"{fn_name}: eval failed — {e}")
    return errors


# ---- Step 5: Retry Loop ----

class ExtractionError(Exception):
    pass


def extract_with_retry(source: str, max_retries: int = 3) -> Tuple[dict, int, List[str]]:
    """Extract template with validation + retry feedback loop.

    Returns (template, attempt_count, final_errors).
    """
    prompt = build_prompt(source[:10000])
    last_errors = []

    for attempt in range(1, max_retries + 1):
        print(f"  [{attempt}/{max_retries}] Calling LLM...")
        try:
            raw = call_llm(prompt)
        except RuntimeError as e:
            print(f"  LLM call failed: {e}")
            if attempt < max_retries:
                continue
            raise ExtractionError(f"LLM failed after {max_retries} attempts") from e

        try:
            template = parse_json(raw)
        except (json.JSONDecodeError, ValueError) as e:
            err_msg = f"JSON parse error: {e}\nRaw output (first 500):\n{raw[:500]}"
            print(f"  Parse failed: {e}")
            prompt = build_prompt(source[:10000], feedback=err_msg)
            last_errors = [err_msg]
            continue

        # Validate structure
        struct_errors = validate(template)
        rwset_errors = validate_rwset(template, "")
        all_errors = struct_errors + rwset_errors

        if not all_errors:
            print(f"  [{attempt}/{max_retries}] Template valid!")
            return template, attempt, []

        print(f"  [{attempt}/{max_retries}] {len(all_errors)} errors found")
        for e in all_errors[:5]:
            print(f"    - {e}")

        if attempt < max_retries:
            feedback = "Validation errors:\n" + "\n".join(f"- {e}" for e in all_errors[:10])
            prompt = build_prompt(source[:10000], feedback=feedback)
        last_errors = all_errors

    raise ExtractionError(f"Template invalid after {max_retries} retries. Errors: {last_errors[:5]}")


# ---- CLI ----

def main():
    parser = argparse.ArgumentParser(description="LLM chaincode template extractor")
    parser.add_argument("--source", required=True, help="Chaincode .go source file(s), comma-separated")
    parser.add_argument("--output", help="Output JSON path")
    parser.add_argument("--retries", type=int, default=3, help="Max retry attempts (default: 3)")
    parser.add_argument("--validate-only", help="Validate an existing template file")
    args = parser.parse_args()

    if args.validate_only:
        with open(args.validate_only) as f:
            t = json.load(f)
        errs = validate(t) + validate_rwset(t, "")
        if errs:
            print(f"{len(errs)} errors:")
            for e in errs:
                print(f"  - {e}")
        else:
            print("Template is valid.")
        return

    # Step 1: Preprocess
    sources = [s.strip() for s in args.source.split(",")]
    all_fns = set()
    all_source = ""
    for src_path in sources:
        source = Path(src_path).read_text(encoding="utf-8")
        pre = preprocess(source)
        all_fns |= set(pre["fn_names"])
        all_source += pre["condensed"] + "\n"
    print(f"[1/4] Preprocessed {len(sources)} file(s): {len(all_fns)} handler functions: {sorted(all_fns)}")

    # Step 2-4: Extract with retry
    print(f"[2/4] Extracting template (max {args.retries} retries)...")
    try:
        template, attempts, errs = extract_with_retry(all_source, max_retries=args.retries)
    except ExtractionError as e:
        print(f"FAILED: {e}")
        sys.exit(1)

    print(f"[3/4] Template validated in {attempts} attempt(s)")
    print(f"  Functions: {list(template.get('functions', {}).keys())}")
    print(f"  Histograms: {template.get('histograms', [])}")
    print(f"  Bypass: {template.get('bypass_functions', [])}")

    # Step 5: Output
    out_path = args.output or "extracted_template.json"
    with open(out_path, "w") as f:
        json.dump(template, f, indent=2)
    print(f"[4/4] Saved to {out_path}")


if __name__ == "__main__":
    main()
