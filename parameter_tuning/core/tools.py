"""LangChain tools for the tuning agent.

- LLM Node1 tools: diagnose, retrieve_knowledge, reflect (main agent)
- LLM Node2 tool: make_test_tool (rate_explorer, closure-based)
"""

import json
import logging
import time
from typing import Any, Dict

from core.llm_config import LLM_API_KEY, LLM_BASE_URL, LLM_MODEL

from langchain_core.tools import tool

from utils.metrics import extract_throughput, extract_avg_latency, extract_success_rate
from utils.sdk import ConfigSDK, ConfigSDKError, reset_session
from utils.test_payload import TEST
from utils.utils import append_to_jsonl

logger = logging.getLogger("FabricAgent")


# =============================================================================
# LLM Node1 tools (main agent)
# =============================================================================


@tool
def diagnose() -> str:
    """Diagnose the latest Caliper benchmark report to identify performance bottlenecks.

    Reads the most recent caliper_result.log and returns a plain-text
    diagnostic analysis of error patterns, throughput, latency, and anomalies.
    Call this when test results are poor or unexpected.
    """
    from pathlib import Path

    from langchain_openai import ChatOpenAI

    from core.prompts import DIAGNOSE_SYSTEM_PROMPT

    # Read caliper_result.log
    temp_dir = Path(__file__).resolve().parent.parent / "temp"
    result_path = temp_dir / "caliper_result.log"
    if not result_path.exists():
        return "No caliper_result.log found. Run a benchmark first."

    try:
        raw = json.loads(result_path.read_text())
        log_text = raw.get("log", "")
    except (json.JSONDecodeError, KeyError):
        log_text = result_path.read_text()

    # Take last 200 lines — enough for error summary + result table
    lines = log_text.split("\n")
    excerpt = "\n".join(lines[-200:])

    logger.info("diagnose tool: analyzing log excerpt (%d lines, %d chars)",
                len(lines[-200:]), len(excerpt))

    prompt = DIAGNOSE_SYSTEM_PROMPT.format(log_excerpt=excerpt)

    diag_llm = ChatOpenAI(
        base_url=LLM_BASE_URL,
        api_key=LLM_API_KEY,
        model=LLM_MODEL,
        temperature=0.3,
        max_tokens=2048,
        timeout=120,
        max_retries=6,
    )

    try:
        response = diag_llm.invoke(prompt)
        result = response.content or "(empty response)"
        logger.info("diagnose tool: result (%d chars)", len(result))
        return result
    except Exception as e:
        logger.error("diagnose: LLM call failed: %s", e)
        return f"Diagnosis failed: {e}"


def _load_jsonl_as_dict(path) -> dict:
    """Read a JSONL file, keying each record by its 'name' or 'topology' field."""
    result = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            rec = json.loads(line)
            key = rec.get("name") or rec.get("topology")
            if key:
                result[key] = rec
    return result


@tool
def retrieve_knowledge(query: str) -> str:
    """Retrieve knowledge from the parameter KB and topology KB.

    Args:
        query: Comma-separated keys to look up. Accepts:
               - Fabric parameter names (e.g. "BatchTimeout,MaxMessageCount")
               - Topology keys (e.g. "8p", "16p") to get cliff_sharpness and
                 probe step sizes for the Send-Rate Prober

    Returns JSON with each entry's fields.
    Keys not found in either KB are marked with 'not found'.
    """
    import re
    from pathlib import Path

    kb_dir = Path(__file__).resolve().parent.parent / "knowledge"

    # Load parameter KB (para_explain.jsonl — Fabric tunable parameters)
    param_path = kb_dir / "para_explain.jsonl"
    if not param_path.exists():
        return json.dumps({"error": f"Parameter KB not found at {param_path}"})
    param_kb = _load_jsonl_as_dict(param_path)

    # Load topology KB (topology_kb.jsonl — cliff_sharpness, probe steps)
    topo_path = kb_dir / "topology_kb.jsonl"
    topo_kb = _load_jsonl_as_dict(topo_path) if topo_path.exists() else {}

    # Parse keys from query (split by comma / space / newline / semicolon)
    names = [n.strip() for n in re.split(r'[,;\s\n]+', query) if n.strip()]

    logger.info("retrieve_knowledge tool: looking up %d keys: %s", len(names), names)

    results = {}
    for name in names:
        if name in param_kb:
            entry = param_kb[name]
            results[name] = {
                "group": entry.get("group", ""),
                "type": entry.get("type", ""),
                "valid_range": entry.get("valid_range", []),
                "default": entry.get("default", ""),
                "effect": entry.get("effect", ""),
                "interacts_with": entry.get("interacts_with", []),
            }
        elif name in topo_kb:
            entry = topo_kb[name]
            results[name] = {
                "peer_count": entry.get("peer_count"),
                "cliff_sharpness": entry.get("cliff_sharpness"),
                "probe_step_coarse": entry.get("probe_step_coarse"),
                "probe_step_fine": entry.get("probe_step_fine"),
                "typical_tps_range": entry.get("typical_tps_range"),
                "note": entry.get("note", ""),
            }
        else:
            results[name] = {"note": "not found"}

    return json.dumps(results, ensure_ascii=False)


@tool
def reflect() -> str:
    """Reflect on the full tuning history to avoid local optima.

    Reads all experience entries from temp/experience.jsonl and returns
    a reflective analysis of what has been tried, what worked, and what
    unexplored directions remain.
    Call this after several iterations or when progress stalls.
    """
    from pathlib import Path

    from langchain_openai import ChatOpenAI

    from core.prompts import REFLECT_SYSTEM_PROMPT

    temp_dir = Path(__file__).resolve().parent.parent / "temp"
    exp_path = temp_dir / "experience.jsonl"
    if not exp_path.exists():
        return "No experience.jsonl found. Run some tuning iterations first."

    history_text = exp_path.read_text().strip()
    if not history_text:
        return "Experience file is empty. Run some tuning iterations first."

    logger.info("reflect tool: reviewing experience history (%d chars)",
                len(history_text))

    prompt = REFLECT_SYSTEM_PROMPT.format(experience_history=history_text)

    reflect_llm = ChatOpenAI(
        base_url=LLM_BASE_URL,
        api_key=LLM_API_KEY,
        model=LLM_MODEL,
        temperature=0.5,
        max_tokens=2048,
        timeout=120,
        max_retries=6,
    )

    try:
        response = reflect_llm.invoke(prompt)
        result = response.content or "(empty response)"
        logger.info("reflect tool: result (%d chars)", len(result))
        return result
    except Exception as e:
        logger.error("reflect: LLM call failed: %s", e)
        return f"Reflection failed: {e}"


# =============================================================================
# LLM Node2 tool (rate_explorer) — closure capturing state
# =============================================================================

def make_test_tool(state: Dict[str, Any]):
    """Return a `test` tool closure that captures the rate_explorer state dict.

    The tool reads session_id / config / config_server_url from *state*,
    runs a Caliper benchmark at the given send_rate, appends results to
    ``temp/explore_send_rate.jsonl``, and mutates *state* to track the best
    result seen so far.
    """

    @tool
    def test(send_rate: int) -> str:
        """Run Caliper benchmark with specified send_rate on the active session.

        Args:
            send_rate: Target TPS for the benchmark (positive integer).

        Returns JSON with: send_rate, tps, latency, success_rate, effective_tps.
        """
        from pathlib import Path

        session_id = state.get("session_id")
        config = state.get("config", {})
        config_server_url = state.get("config_server_url")

        if not session_id or not state.get("session_active"):
            return json.dumps({"error": "No active session"})

        tx_number = send_rate * 30
        test_payload = TEST.format(send_rate, tx_number)

        client = ConfigSDK(config_server_url)

        # Run test with retry on session failures
        try:
            result = client.session_test(session_id, test_payload)
        except ConfigSDKError as e:
            if e.status in (409, 500):
                logger.warning("test tool: session error %d, restarting...", e.status)
                reset_session(config_server_url)
                time.sleep(5)
                response = client.session_start(configs=config)
                new_session_id = response["session_id"]
                state["session_id"] = new_session_id
                result = client.session_test(new_session_id, test_payload)
            else:
                raise

        # Extract metrics
        tps_val = extract_throughput(result)
        latency = extract_avg_latency(result)
        success_rate = extract_success_rate(result, tx_number)
        effective_tps = tps_val * success_rate

        # Append to explore_send_rate.jsonl (include full Caliper output)
        temp_dir = Path(__file__).resolve().parent.parent / "temp"
        explore_path = str(temp_dir / "explore_send_rate.jsonl")
        append_to_jsonl(explore_path, {
            "config": config,
            "send_rate": send_rate,
            "tps": tps_val,
            "latency": latency,
            "success_rate": success_rate,
            "effective_tps": effective_tps,
            "caliper_output": json.dumps(result),
        })

        # Update best result in state if improved
        if effective_tps > state.get("best_effective_tps", 0):
            state["best_send_rate"] = send_rate
            state["best_effective_tps"] = effective_tps
            state["best_test_result"] = {
                "tps": tps_val,
                "latency": latency,
                "success_rate": success_rate,
                "effective_tps": effective_tps,
            }
            logger.info("test tool: new best! send_rate=%d effective_tps=%.1f",
                        send_rate, effective_tps)

        # Increment round counter
        state["round"] = state.get("round", 0) + 1

        logger.info("test tool: round=%d send_rate=%d tps=%.1f effective_tps=%.1f",
                    state["round"], send_rate, tps_val, effective_tps)

        # Return structured result (no caliper_output in message)
        return json.dumps({
            "send_rate": send_rate,
            "tps": tps_val,
            "latency": latency,
            "success_rate": success_rate,
            "effective_tps": effective_tps,
        })

    return test
