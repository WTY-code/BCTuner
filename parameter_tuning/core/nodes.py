"""LangGraph node functions for the Fabric tuning agent."""

import json
import logging
import os
import time
from typing import Any, Dict

from core.state import TuningState
from core.llm import make_llm
from utils.metrics import extract_avg_latency, extract_success_rate, extract_throughput
from core.prompts import build_system_prompt
from utils.sdk import ConfigSDK, ConfigSDKError, reset_session
from utils.test_payload import TEST
from utils.utils import (
    execute_tools,
    extract_json_block,
    make_history_entry,
    make_test_human_msg,
    manage_context,
    message_to_dict,
    tool_to_openai,
)

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

logger = logging.getLogger("FabricAgent")


# ---------------------------------------------------------------------------
# 1. init_node
# ---------------------------------------------------------------------------

def init_node(state: TuningState) -> Dict[str, Any]:
    url = state["config_server_url"]
    logger.info("init_node: resetting stale sessions on %s", url)
    reset_session(url)
    time.sleep(3)

    client = ConfigSDK(url)
    logger.info("init_node: fetching baseline config snapshot...")
    snapshot = client.info()
    core_keys = list(snapshot.get("core_cfg", {}).keys())
    orderer_keys = list(snapshot.get("orderer_cfg", {}).keys())
    tx_keys = list(snapshot.get("tx_cfg", {}).keys())
    logger.info(
        "init_node: snapshot ready (core=%d, orderer=%d, tx=%d keys)",
        len(core_keys), len(orderer_keys), len(tx_keys),
    )
    return {"baseline_snapshot": snapshot}


# ---------------------------------------------------------------------------
# 2. baseline_node
# ---------------------------------------------------------------------------

def baseline_node(state: TuningState) -> Dict[str, Any]:
    url = state["config_server_url"]
    target_tps = state["target_tps"]
    tx_number = target_tps * 100
    test_payload = TEST.format(target_tps, tx_number)

    logger.info("baseline_node: running baseline test (default config)...")
    client = ConfigSDK(url)
    with client.session(configs={}) as sess:
        result = sess.test(test_payload)

    tps = extract_throughput(result)
    latency = extract_avg_latency(result)
    success_rate = extract_success_rate(result, tx_number)
    effective_tps = tps * success_rate

    logger.info(
        "baseline_node: TPS=%.1f Latency=%.3fs SuccessRate=%.1f%% EffectiveTPS=%.1f",
        tps, latency, success_rate * 100, effective_tps,
    )

    # Write baseline as step 0 to iteration.jsonl (full default config)
    baseline_config = {}
    for section in ("core_cfg", "orderer_cfg", "tx_cfg"):
        baseline_config.update(state.get("baseline_snapshot", {}).get(section, {}))
    append_to_jsonl(_RATE_ITERATION_PATH, {
        "step": 0,
        "config": baseline_config,
        "best_send_rate": target_tps,
        "best_effective_tps": effective_tps,
        "tps": tps,
        "latency": latency,
        "success_rate": success_rate,
    })

    return {
        "current_config": {},
        "best_config": {},
        "best_effective_tps": effective_tps,
        "step": 1,
        "history": [
            make_history_entry("Initial baseline test with default configuration.",
                                {}, tps, latency, success_rate, effective_tps),
        ],
        "messages": [
            make_test_human_msg(tps, latency, success_rate, effective_tps),
        ],
    }


# ---------------------------------------------------------------------------
# 3. think_node
# ---------------------------------------------------------------------------

def think_node(state: TuningState) -> Dict[str, Any]:
    logger.info("think_node: building messages and calling LLM...")
    llm = make_llm()

    msgs = manage_context(list(state["messages"]))

    # Insert SystemMessage before the first LLM call
    if not any(isinstance(m, SystemMessage) for m in msgs):
        system_msg = SystemMessage(content=build_system_prompt(state["baseline_snapshot"]))
        msgs.insert(0, system_msg)

    # Preserve reasoning_content from previous AIMessages (DeepSeek requires it)
    payload_msgs = []
    for m in msgs:
        d = message_to_dict(m)
        if isinstance(m, AIMessage):
            reasoning = m.additional_kwargs.get("reasoning_content")
            if reasoning:
                d["reasoning_content"] = reasoning
        payload_msgs.append(d)

    # Build tool schemas in OpenAI format
    from core.tools import diagnose, reflect, retrieve_knowledge
    tools_schema = [tool_to_openai(t) for t in (diagnose, retrieve_knowledge, reflect)]

    response = llm.client.create(
        model=llm.model_name,
        messages=payload_msgs,
        temperature=llm.temperature,
        max_tokens=llm.max_tokens,
        tools=tools_schema,
        tool_choice="auto",
        timeout=600,
    )
    raw = response.choices[0].message.content or ""
    tool_calls = response.choices[0].message.tool_calls or []

    # Build AIMessage with DeepSeek-specific reasoning_content preserved
    ai_kwargs: Dict[str, Any] = {}
    reasoning = getattr(response.choices[0].message, "reasoning_content", None)
    if reasoning:
        ai_kwargs["reasoning_content"] = reasoning

    ai_msg = AIMessage(
        content=raw,
        additional_kwargs=ai_kwargs,
        tool_calls=[
            {
                "id": tc.id,
                "name": tc.function.name,
                "args": json.loads(tc.function.arguments),
                "type": "function",
            }
            for tc in tool_calls
        ] if tool_calls else [],
    )

    logger.info("think_node: LLM response (%d chars, %d tool_calls)",
                len(raw), len(tool_calls))
    if tool_calls:
        tc_info = [(tc.function.name, json.loads(tc.function.arguments)) for tc in tool_calls]
        logger.info("think_node tool_calls: %s", tc_info)
    else:
        logger.info("think_node thought:\n%s", raw[:2000])
    return {"messages": [ai_msg]}


# ---------------------------------------------------------------------------
# 4. parse_node
# ---------------------------------------------------------------------------

def parse_node(state: TuningState) -> Dict[str, Any]:
    last_msg = state["messages"][-1]
    if not isinstance(last_msg, AIMessage):
        logger.warning("parse_node: last message is not AIMessage (%s), retrying think",
                       type(last_msg).__name__)
        return {"next_action": "parse_error"}

    tool_calls = getattr(last_msg, "tool_calls", None) or []

    # --- branch 1: tool calls (execute all inline, return ToolMessages) ---
    # Must run before any routing that would leave the AIMessage's tool_calls
    # unanswered — the OpenAI-format API requires every tool_call_id to be
    # followed by a matching ToolMessage before the next assistant turn.
    if tool_calls:
        valid = []
        for tc in tool_calls:
            name = tc.get("name", "")
            tid = tc.get("id", "")
            args = tc.get("args", {})
            if name in ("diagnose", "retrieve_knowledge", "reflect"):
                valid.append((tid, name, args))
            else:
                logger.warning("parse_node: unknown tool '%s', skipping", name)

        if valid:
            tool_names = [n for _, n, _ in valid]
            logger.info("parse_node: executing %d tool(s): %s", len(valid), ", ".join(tool_names))
            for _, name, args in valid:
                if args:
                    logger.info("parse_node: tool '%s' args: %s", name, args)
            tool_msgs = execute_tools(valid, logger=logger)
            # Route directly back to think with all tool responses
            return {
                "next_action": "tool_done",
                "messages": tool_msgs,
            }
        logger.info("parse_node routing: next_action=parse_error")
        return {"next_action": "parse_error"}

    # --- branch 2: JSON config → invoke rate_explorer ---
    # Config proposals must be honored even after the step budget is
    # exhausted; otherwise the safety valve at guard_node (step > max_steps
    # + 3) is unreachable because step only advances on rate_explorer
    # completion. Guard is only invoked below when the LLM produces neither
    # a tool_call nor a valid config.
    raw = last_msg.content
    logger.info("parse_node: no tool_calls, trying JSON config extraction...")

    json_text = extract_json_block(raw)
    if json_text:
        try:
            parsed = json.loads(json_text)
            # Support both {"config": {...}} and direct config {...} formats
            if "config" in parsed and isinstance(parsed["config"], dict):
                config = {str(k): str(v) for k, v in parsed["config"].items()}
            else:
                # Filter out metadata keys, keep only string values as config
                config = {str(k): str(v) for k, v in parsed.items()
                          if k not in ("thought", "action") and v is not None}

            if not config:
                logger.warning("parse_node: empty config, falling through to finish")
                return {"next_action": "parse_error"}

            logger.info("parse_node: invoking rate_explorer with %d config keys", len(config))

            # Build rate_explorer initial state and invoke
            from core.graph import build_rate_explorer_graph

            rate_explorer_initial = {
                "config": config,
                "config_server_url": state["config_server_url"],
                "initial_send_rate": state["target_tps"],
                "max_rounds": 5,
                "step": state["step"],
                "topology": state.get("topology", ""),
            }

            try:
                rate_explorer = build_rate_explorer_graph()
                sg_result = rate_explorer.invoke(rate_explorer_initial)
            except Exception as exc:
                logger.error("parse_node: rate_explorer failed: %s", exc)
                reset_session(state["config_server_url"])
                return {"next_action": "parse_error"}

            best_send_rate = sg_result.get("best_send_rate", 0)
            best_effective_tps = sg_result.get("best_effective_tps", 0.0)
            best_test = sg_result.get("best_test_result") or {}

            new_step = state["step"] + 1
            is_best = best_effective_tps > state.get("best_effective_tps", 0)
            label = " (NEW BEST)" if is_best else ""

            logger.info(
                "parse_node: rate_explorer result send_rate=%d effective_tps=%.1f%s",
                best_send_rate, best_effective_tps, label,
            )
            logger.info("parse_node routing: next_action=summarize")

            return {
                "next_action": "summarize",
                "step": new_step,
                "current_config": config,
                "best_config": config if is_best else state.get("best_config"),
                "best_effective_tps": max(best_effective_tps, state.get("best_effective_tps", 0)),
                "messages": [
                    make_test_human_msg(
                        best_test.get("tps", 0),
                        best_test.get("latency", 0),
                        best_test.get("success_rate", 0),
                        best_effective_tps,
                        config=config,
                    ),
                ],
                "history": [
                    make_history_entry(
                        f"Tested config with optimal send_rate={best_send_rate}. "
                        f"EffectiveTPS={best_effective_tps:.1f}{label}",
                        config,
                        best_test.get("tps", 0),
                        best_test.get("latency", 0),
                        best_test.get("success_rate", 0),
                        best_effective_tps,
                    ),
                ],
            }
        except json.JSONDecodeError:
            logger.warning("parse_node: JSON decode failed, falling through to finish")

    # --- branch 3: neither tools nor config → finish ---
    logger.info("parse_node: no tool_calls and no valid config → finish")

    # Check if we've reached max_steps
    current_step = state.get("step", 1)
    max_steps = state.get("max_steps", 10)

    if current_step < max_steps:
        logger.info("parse_node: prompting agent to continue exploration (step %d/%d)",
                    current_step, max_steps)
        reminder_msg = HumanMessage(content=json.dumps({
            "reminder": f"You have completed {current_step} tuning steps out of {max_steps}. "
                       f"The parameter space is not fully explored yet. "
                       f"Consider trying different parameter directions or calling tools "
                       f"(diagnose/retrieve_knowledge/reflect) to guide further tuning."
        }))
        return {
            "next_action": "tool_done",
            "messages": [reminder_msg],
        }

    logger.info("parse_node routing: next_action=guard (harness termination check)")
    return {"next_action": "guard"}


# ---------------------------------------------------------------------------
# 5. guard_node — Harness termination gate
# ---------------------------------------------------------------------------

def guard_node(state: TuningState) -> Dict[str, Any]:
    """Harness termination guard: grants finish only when all three conditions pass.

    Conditions (per paper §III.B.1):
      1. Exploration depth    — enough distinct parameter groups covered
      2. Group diversification — trials led by sufficiently varied groups
      3. Trend stabilization  — effective TPS has plateaued

    Safety valve: if step > max_steps + 3, finish unconditionally to prevent
    an infinite loop when conditions are structurally hard to satisfy.
    """
    history = state.get("history", [])
    best_tps = state.get("best_effective_tps", 0.0)

    if state.get("step", 0) > state.get("max_steps", 10) + 3:
        logger.info("guard_node: safety valve — step=%d > max_steps+3=%d, granting finish",
                    state.get("step", 0), state.get("max_steps", 10) + 3)
        return {"next_action": "finish"}

    d_ok, d_msg = _check_exploration_depth(history)
    g_ok, g_msg = _check_group_diversification(history)
    t_ok, t_msg = _check_trend_stabilization(history, best_tps)

    if d_ok and g_ok and t_ok:
        logger.info("guard_node: all three conditions passed — granting finish")
        return {"next_action": "finish"}

    failed = [msg for ok, msg in [(d_ok, d_msg), (g_ok, g_msg), (t_ok, t_msg)] if not ok]
    logger.info("guard_node: termination denied — %d condition(s) failed", len(failed))
    for m in failed:
        logger.info("  • %s", m)

    feedback = {
        "harness_guard": "TERMINATION DENIED",
        "conditions_failed": failed,
        "instruction": (
            "The Harness has denied termination. Continue exploration to satisfy "
            "the conditions listed above, then you may request to stop again."
        ),
    }
    return {
        "next_action": "think",
        "messages": [HumanMessage(content=json.dumps(feedback, ensure_ascii=False))],
    }


# ---------------------------------------------------------------------------
# 6. finish_node
# ---------------------------------------------------------------------------

def finish_node(state: TuningState) -> Dict[str, Any]:
    temp_dir_s = os.path.join(os.path.dirname(__file__), "..", "temp")
    os.makedirs(temp_dir_s, exist_ok=True)

    # Backup iteration.jsonl and experience.jsonl
    backup_jsonl(os.path.join(temp_dir_s, "iteration.jsonl"))
    backup_jsonl(os.path.join(temp_dir_s, "experience.jsonl"))

    best_path = os.path.join(temp_dir_s, "best_config.json")
    best = state.get("best_config", {})
    with open(best_path, "w") as f:
        json.dump(best, f, indent=4)

    logger.info("=== Optimization Finished ===")
    logger.info("Best Effective TPS: %.1f", state.get("best_effective_tps", 0))
    logger.info("Best Config (%d keys) written to %s", len(best), best_path)
    logger.info("Steps executed: %d", state.get("step", 0))

    return {"done": True}


# =============================================================================
# rate_explorer nodes — explore optimal send_rate for a given config
# =============================================================================

from pathlib import Path

from core.state import RateExplorerState
from core.prompts import RATE_EXPLORER_SYSTEM_PROMPT, SUMMARIZER_SYSTEM_PROMPT
from core.tools import make_test_tool
from utils.utils import append_to_jsonl, backup_jsonl, read_jsonl

_rate_explorer_temp_dir = Path(__file__).resolve().parent.parent / "temp"
_RATE_EXPLORE_PATH = str(_rate_explorer_temp_dir / "explore_send_rate.jsonl")
_RATE_ITERATION_PATH = str(_rate_explorer_temp_dir / "iteration.jsonl")

_KB_DIR = Path(__file__).resolve().parent.parent / "knowledge"
_TOPOLOGY_KB_PATH = _KB_DIR / "topology_kb.jsonl"
_PARA_EXPLAIN_PATH = _KB_DIR / "para_explain.jsonl"

# ---------------------------------------------------------------------------
# Harness guard — module-level constants and helpers
# ---------------------------------------------------------------------------

_MIN_GROUPS = 4          # distinct parameter groups required for exploration depth
_MIN_DOMINANT = 3        # distinct dominant groups required for diversification
_TREND_WINDOW = 5        # recent entries examined for trend stabilization
_TREND_THRESHOLD = 0.05  # max relative spread (5% of best TPS) to declare plateau


def _load_param_group_map() -> dict:
    """Load {param_name: group_name} from para_explain.jsonl (called once at import)."""
    result = {}
    if _PARA_EXPLAIN_PATH.exists():
        for line in _PARA_EXPLAIN_PATH.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                rec = json.loads(line)
                if rec.get("name") and rec.get("group"):
                    result[rec["name"]] = rec["group"]
    return result


_PARAM_GROUP_MAP: dict = _load_param_group_map()


def _check_exploration_depth(history: list) -> "tuple[bool, str]":
    """Condition 1: at least _MIN_GROUPS distinct parameter groups touched across all trials."""
    groups_touched: set = set()
    for entry in history:
        for param in entry.get("config", {}):
            g = _PARAM_GROUP_MAP.get(param)
            if g:
                groups_touched.add(g)
    n = len(groups_touched)
    if n >= _MIN_GROUPS:
        return True, f"exploration depth OK ({n} groups)"
    return False, (
        f"Exploration depth insufficient: only {n}/{_MIN_GROUPS} required parameter groups "
        f"have been tested. Groups covered: {sorted(groups_touched)}. "
        f"Explore parameters from more categories (block_cutting, concurrency, gossip, "
        f"couchdb, orderer, keepalive, etc.) before stopping."
    )


def _dominant_group(config: dict) -> str:
    """Return the parameter group that appears most in this config dict."""
    counts: dict = {}
    for param in config:
        g = _PARAM_GROUP_MAP.get(param, "unknown")
        counts[g] = counts.get(g, 0) + 1
    if not counts:
        return "unknown"
    return max(counts, key=counts.__getitem__)


def _check_group_diversification(history: list) -> "tuple[bool, str]":
    """Condition 2: at least _MIN_DOMINANT distinct dominant groups across all trials."""
    dominant_groups: set = set()
    for entry in history:
        cfg = entry.get("config", {})
        if cfg:
            dominant_groups.add(_dominant_group(cfg))
    n = len(dominant_groups)
    if n >= _MIN_DOMINANT:
        return True, f"group diversification OK ({n} dominant groups)"
    return False, (
        f"Group diversification insufficient: only {n}/{_MIN_DOMINANT} required distinct "
        f"dominant groups across trials. Dominant groups so far: {sorted(dominant_groups)}. "
        f"Try at least one trial whose primary focus is a different parameter category."
    )


def _check_trend_stabilization(history: list, best_tps: float) -> "tuple[bool, str]":
    """Condition 3: effective_tps has plateaued over the last _TREND_WINDOW entries."""
    recent = [e.get("effective_tps", 0.0) for e in history[-_TREND_WINDOW:]]
    if len(recent) < _TREND_WINDOW:
        return False, (
            f"Trend stabilization: insufficient data ({len(recent)}/{_TREND_WINDOW} entries). "
            f"Run at least {_TREND_WINDOW} trials before claiming convergence."
        )
    if best_tps <= 0:
        return False, "Trend stabilization: best_effective_tps is 0, cannot assess plateau."
    spread = (max(recent) - min(recent)) / best_tps
    if spread < _TREND_THRESHOLD:
        return True, f"trend stabilization OK (spread={spread:.3f} < {_TREND_THRESHOLD})"
    return False, (
        f"Trend not yet stable: recent TPS spread is {spread:.1%} of best "
        f"(threshold {_TREND_THRESHOLD:.0%}). Recent values: "
        f"{[round(v,1) for v in recent]}. Keep exploring to confirm convergence."
    )


def _build_topology_guidance(topology: str) -> str:
    """Return a formatted topology guidance string for RATE_EXPLORER_SYSTEM_PROMPT.

    Loads the matching record from topology_kb.jsonl and formats cliff_sharpness,
    step sizes, and TPS ceiling into a compact advisory block.
    Returns an empty string if topology is unknown or the file is missing.
    """
    if not topology:
        return ""
    if not _TOPOLOGY_KB_PATH.exists():
        return ""
    for line in _TOPOLOGY_KB_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rec = json.loads(line)
        if rec.get("topology") == topology:
            sharpness = rec["cliff_sharpness"]
            coarse = rec["probe_step_coarse"]
            fine = rec["probe_step_fine"]
            lo, hi = rec["typical_tps_range"]
            direction = (
                "Do NOT overshoot — use fine steps near peak to avoid collapse."
                if sharpness == "sharp"
                else "Coarse bracketing is safe for the initial sweep."
            )
            return (
                f"\nTopology profile ({topology}):\n"
                f"  cliff_sharpness = {sharpness} — {direction}\n"
                f"  coarse sweep step = {coarse} tx/s   fine step = {fine} tx/s\n"
                f"  typical TPS ceiling ≈ {lo}–{hi} tx/s\n"
            )
    return ""


# ---------------------------------------------------------------------------
# rate_explorer: 1. init_rate_explorer_node
# ---------------------------------------------------------------------------

def init_rate_explorer_node(state: RateExplorerState) -> Dict[str, Any]:
    logger.info("rate_explorer init: initial_send_rate=%d max_rounds=%d",
                state.get("initial_send_rate", 0), state.get("max_rounds", 5))
    return {
        "best_send_rate": state["initial_send_rate"],
        "best_effective_tps": 0.0,
        "best_test_result": None,
        "round": 0,
        "session_id": None,
        "session_active": False,
        "next_action": "deploy",
        "done": False,
    }


# ---------------------------------------------------------------------------
# rate_explorer: 2. deploy_and_test_rate_explorer_node
# ---------------------------------------------------------------------------

def deploy_and_test_rate_explorer_node(state: RateExplorerState) -> Dict[str, Any]:
    config = state["config"]
    send_rate = state["initial_send_rate"]
    url = state["config_server_url"]

    logger.info("rate_explorer deploy_and_test: ROUND 1/%d, %d overrides, send_rate=%d",
                state.get("max_rounds", 2), len(config), send_rate)

    tx_number = send_rate * 30
    test_payload = TEST.format(send_rate, tx_number)

    # Deploy session
    client = ConfigSDK(url)
    try:
        response = client.session_start(configs=config)
        session_id = response["session_id"]
    except ConfigSDKError as e:
        if e.status in (409, 500):
            logger.warning("rate_explorer: session_start error %d, resetting...", e.status)
            reset_session(url)
            time.sleep(5)
            response = client.session_start(configs=config)
            session_id = response["session_id"]
        else:
            raise

    # Run first test
    try:
        result = client.session_test(session_id, test_payload)
    except ConfigSDKError as e:
        if e.status in (409, 500):
            logger.warning("rate_explorer: session_test error %d, restarting...", e.status)
            reset_session(url)
            time.sleep(5)
            response = client.session_start(configs=config)
            session_id = response["session_id"]
            result = client.session_test(session_id, test_payload)
        else:
            raise

    # Extract metrics
    tps_val = extract_throughput(result)
    latency = extract_avg_latency(result)
    success_rate = extract_success_rate(result, tx_number)
    effective_tps = tps_val * success_rate

    # Append to explore_send_rate.jsonl (include full Caliper output)
    append_to_jsonl(_RATE_EXPLORE_PATH, {
        "config": config,
        "send_rate": send_rate,
        "tps": tps_val,
        "latency": latency,
        "success_rate": success_rate,
        "effective_tps": effective_tps,
        "caliper_output": json.dumps(result),
    })

    logger.info("rate_explorer deploy_and_test: tps=%.1f effective_tps=%.1f",
                tps_val, effective_tps)

    return {
        "session_id": session_id,
        "session_active": True,
        "best_send_rate": send_rate,
        "best_effective_tps": effective_tps,
        "best_test_result": {
            "tps": tps_val,
            "latency": latency,
            "success_rate": success_rate,
            "effective_tps": effective_tps,
        },
        "round": 1,
        "messages": [
            HumanMessage(content=json.dumps({
                "send_rate": send_rate,
                "tps": tps_val,
                "latency": latency,
                "success_rate": success_rate,
                "effective_tps": effective_tps,
            })),
        ],
    }


# ---------------------------------------------------------------------------
# rate_explorer: 3. think_rate_explorer_node
# ---------------------------------------------------------------------------

def think_rate_explorer_node(state: RateExplorerState) -> Dict[str, Any]:
    next_round = state.get("round", 1) + 1
    logger.info("rate_explorer think: ROUND %d/%d",
                next_round, state.get("max_rounds", 2))

    llm = make_llm()

    # Build message list
    logger.debug("rate_explorer think: %d messages in state", len(state["messages"]))
    msgs: list = list(state["messages"])

    # Build test history summary for the system prompt
    history_entries = read_jsonl(_RATE_EXPLORE_PATH)
    history_lines = []
    for entry in history_entries[-5:]:
        marker = ""
        if entry.get("send_rate") == state.get("best_send_rate"):
            marker = " <-- BEST"
        history_lines.append(
            f"  send_rate={entry['send_rate']} "
            f"effective_tps={entry.get('effective_tps', 0):.1f}"
            f"{marker}"
        )
    history_text = "\n".join(history_lines) if history_lines else "  (none yet)"

    topology_guidance = _build_topology_guidance(state.get("topology", ""))

    system_msg = SystemMessage(content=RATE_EXPLORER_SYSTEM_PROMPT.format(
        round=next_round,
        max_rounds=state.get("max_rounds", 5),
        remaining=state.get("max_rounds", 5) - state.get("round", 1),
        test_history=history_text,
        topology_guidance=topology_guidance,
    ))
    msgs.insert(0, system_msg)

    # Convert messages to OpenAI format
    payload_msgs = []
    for m in msgs:
        d = message_to_dict(m)
        if isinstance(m, AIMessage):
            reasoning = m.additional_kwargs.get("reasoning_content")
            if reasoning:
                d["reasoning_content"] = reasoning
        payload_msgs.append(d)

    # Build tool schema with state closure
    test_tool = make_test_tool(state)
    tools_schema = [tool_to_openai(test_tool)]

    response = llm.client.create(
        model=llm.model_name,
        messages=payload_msgs,
        temperature=llm.temperature,
        max_tokens=llm.max_tokens,
        tools=tools_schema,
        tool_choice="auto",
        timeout=600,
    )
    raw = response.choices[0].message.content or ""
    tool_calls = response.choices[0].message.tool_calls or []

    ai_kwargs: Dict[str, Any] = {}
    reasoning = getattr(response.choices[0].message, "reasoning_content", None)
    if reasoning:
        ai_kwargs["reasoning_content"] = reasoning

    ai_msg = AIMessage(
        content=raw,
        additional_kwargs=ai_kwargs,
        tool_calls=[
            {
                "id": tc.id,
                "name": tc.function.name,
                "args": json.loads(tc.function.arguments),
                "type": "function",
            }
            for tc in tool_calls
        ] if tool_calls else [],
    )

    logger.info("rate_explorer think: %d chars, %d tool_calls", len(raw), len(tool_calls))
    if tool_calls:
        tc_info = [(tc.function.name, json.loads(tc.function.arguments)) for tc in tool_calls]
        logger.info("rate_explorer think tool_calls: %s", tc_info)
    else:
        logger.info("rate_explorer think thought:\n%s", raw[:1000])
    return {"messages": [ai_msg]}


# ---------------------------------------------------------------------------
# rate_explorer: 4. parse_rate_explorer_node
# ---------------------------------------------------------------------------

def parse_rate_explorer_node(state: RateExplorerState) -> Dict[str, Any]:
    last_msg = state["messages"][-1]
    if not isinstance(last_msg, AIMessage):
        logger.warning("rate_explorer parse: last msg is not AIMessage (%s), finishing",
                       type(last_msg).__name__)
        return {"next_action": "finish"}

    tool_calls = getattr(last_msg, "tool_calls", None) or []

    # --- branch 1: tool calls (execute test tool inline) ---
    if tool_calls:
        tool_messages = []
        for tc in tool_calls:
            name = tc.get("name", "")
            tid = tc.get("id", "")
            args = tc.get("args", {})

            if name == "test":
                send_rate = args.get("send_rate")
                if not isinstance(send_rate, int) or send_rate <= 0:
                    tool_messages.append(ToolMessage(
                        content=json.dumps({"error": f"Invalid send_rate: {send_rate}"}),
                        tool_call_id=tid,
                    ))
                    continue

                logger.info("rate_explorer parse: executing test tool send_rate=%d", send_rate)
                test_tool = make_test_tool(state)
                try:
                    result_str = test_tool.invoke({"send_rate": send_rate})
                except Exception as e:
                    logger.error("rate_explorer parse: test tool failed: %s", e)
                    result_str = json.dumps({"error": str(e)})

                tool_messages.append(ToolMessage(content=result_str, tool_call_id=tid))
            else:
                tool_messages.append(ToolMessage(
                    content=json.dumps({"error": f"Unknown tool: {name}"}),
                    tool_call_id=tid,
                ))

        logger.info("rate_explorer parse: executed %d tool(s)",
                    len(tool_messages))
        # Log tool result previews
        for idx, tm in enumerate(tool_messages):
            logger.info("rate_explorer parse: tool result #%d: %s", idx + 1,
                        (tm.content or "")[:500])

        # Return tool-mutated state fields so they propagate to the next node
        state_update = {
            "messages": tool_messages,
            "round": state.get("round", 0),
            "best_send_rate": state.get("best_send_rate", 0),
            "best_effective_tps": state.get("best_effective_tps", 0.0),
            "best_test_result": state.get("best_test_result"),
            "session_id": state.get("session_id"),
        }

        if state.get("round", 0) >= state.get("max_rounds", 5):
            logger.info("rate_explorer parse: max rounds reached, finishing")
            state_update["next_action"] = "finish"
            return state_update

        state_update["next_action"] = "think"
        return state_update

    # --- branch 2: no tool_calls, check for finish signal ---
    if state.get("round", 0) >= state.get("max_rounds", 5):
        logger.info("rate_explorer parse: max rounds reached, finishing")
        return {"next_action": "finish"}

    raw = last_msg.content
    json_text = extract_json_block(raw)
    if json_text:
        try:
            parsed = json.loads(json_text)
            if parsed.get("action") == "finish":
                logger.info("rate_explorer parse: finish signal received")
                return {"next_action": "finish"}
        except json.JSONDecodeError:
            pass

    logger.info("rate_explorer parse: no clear signal, finishing by default")
    return {"next_action": "finish"}


# ---------------------------------------------------------------------------
# rate_explorer: 5. finish_rate_explorer_node
# ---------------------------------------------------------------------------

def finish_rate_explorer_node(state: RateExplorerState) -> Dict[str, Any]:
    session_id = state.get("session_id")
    url = state.get("config_server_url")

    # Close session
    if session_id and state.get("session_active"):
        try:
            client = ConfigSDK(url)
            client.session_end(session_id)
            logger.info("rate_explorer finish: session %s closed", session_id)
        except Exception as e:
            logger.warning("rate_explorer finish: failed to close session %s: %s",
                          session_id, e)

    # Write best test's full Caliper report to caliper_result.log (overwrite)
    _write_caliper_result(state)

    # Backup explore_send_rate.jsonl to avoid file bloat across iterations
    _backup_explore_file()

    # Write to iteration.jsonl
    config = state.get("config", {})
    best_test = state.get("best_test_result") or {}
    iteration_entry = {
        "step": state.get("step", 0),
        "config": config,
        "best_send_rate": state.get("best_send_rate", 0),
        "best_effective_tps": state.get("best_effective_tps", 0.0),
        "tps": best_test.get("tps", 0),
        "latency": best_test.get("latency", 0),
        "success_rate": best_test.get("success_rate", 0),
    }
    append_to_jsonl(_RATE_ITERATION_PATH, iteration_entry)
    logger.info("rate_explorer finish: iteration.jsonl written (step=%d)",
                state.get("step", 0))

    logger.info("rate_explorer finish: best_send_rate=%d best_effective_tps=%.1f rounds=%d",
                state.get("best_send_rate", 0),
                state.get("best_effective_tps", 0),
                state.get("round", 0))

    return {
        "done": True,
        "session_active": False,
    }


def _backup_explore_file() -> None:
    backup_jsonl(_RATE_EXPLORE_PATH)
    logger.info("rate_explorer finish: backed up explore log")


def _write_caliper_result(state: RateExplorerState) -> None:
    """Write the best test's Caliper raw output to caliper_result.log (overwrite)."""
    best_send_rate = state.get("best_send_rate")
    if best_send_rate is None:
        return
    # Find matching entry in explore_send_rate.jsonl
    entries = read_jsonl(_RATE_EXPLORE_PATH)
    caliper_raw = ""
    for e in entries:
        if e.get("send_rate") == best_send_rate:
            caliper_raw = e.get("caliper_output", "")
    if not caliper_raw:
        # Fallback: use the last entry
        for e in reversed(entries):
            if e.get("caliper_output"):
                caliper_raw = e["caliper_output"]
                break

    if caliper_raw:
        path = _rate_explorer_temp_dir / "caliper_result.log"
        path.write_text(caliper_raw)
        logger.info("rate_explorer finish: caliper_result.log written (%d chars)",
                    len(caliper_raw))


# =============================================================================
# Summarizer node — summarize each tuning step after rate_explorer finishes
# =============================================================================

def summarizer_node(state: TuningState) -> Dict[str, Any]:
    # Read last 2 entries from iteration.jsonl
    entries = read_jsonl(_RATE_ITERATION_PATH)
    recent = entries[-2:] if len(entries) >= 2 else entries
    history_text = json.dumps(recent, indent=2)

    # Find the most recent AIMessage as agent analysis
    agent_analysis = ""
    for m in reversed(state["messages"]):
        if isinstance(m, AIMessage) and m.content:
            agent_analysis = m.content
            break

    prompt = SUMMARIZER_SYSTEM_PROMPT.format(
        history_context=history_text,
        agent_analysis=agent_analysis,
    )

    llm = make_llm()
    response = llm.client.create(
        model=llm.model_name,
        messages=[{"role": "user", "content": prompt}],
        temperature=llm.temperature,
        max_tokens=1024,
        timeout=120,
    )
    summary = response.choices[0].message.content or ""

    logger.info("summarizer: generated summary (%d chars):\n%s", len(summary), summary[:2000])

    # Write experience to experience.jsonl
    # step was already incremented by parse_node for the next round, so use step-1
    exp_path = str(_rate_explorer_temp_dir / "experience.jsonl")
    append_to_jsonl(exp_path, {
        "step": state.get("step", 1) - 1,
        "experience": summary,
    })

    return {
        "messages": [HumanMessage(content=json.dumps({"summary": summary}))],
    }
