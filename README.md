# Auriga

**Harness-Governed LLM Agents for Effective Throughput Optimization in
Execute-Order-Validate Permissioned Blockchains**

Effective throughput in Execute-Order-Validate (EOV) blockchains is jointly
constrained by *system capacity* and *transaction success rate*:

```
TPS_eff = TPS_raw × R_succ
```

Existing approaches tune one factor in isolation: parameter-tuning frameworks
maximize raw throughput while ignoring MVCC aborts; concurrency-aware scheduling
mitigates aborts but assumes a fixed configuration. **Auriga** unifies both
through a split-phase architecture — an LLM-driven multi-agent parameter tuner lifts the throughput ceiling, and an LLM-extracted predictive
scheduler  eliminates intra-block MVCC conflicts before they reach
validation.

***

## Repository Structure

```
auriga/
├── README.md                              (this file)
├── .env.example                           credentials template
├── .gitignore
│
├── parameter_tuning/                      paper §III — harness-governed agent
│   ├── main.py                            CLI entry point
│   ├── core/                              graph, nodes, prompts, llm, state, tools
│   │   └── prompts.py                     system prompts for all LLM roles (main agent,
│   │                                        rate_explorer, summarizer, diagnose, reflect,
│   │                                        context compression)
│   ├── utils/                             SDK, metrics, context compression
│   ├── knowledge/                         semantic memory (KBs)
│   │   ├── para_explain.jsonl             Fabric parameter semantics
│   │   └── topology_kb.jsonl              per-topology cliff profiles
│   └── temp/                              runtime outputs (generated on first run)
│       └── run.log                        per-step tuning trajectory: tool calls, LLM
│                                            thoughts, rate_explorer probes, and final
│                                            effective-TPS achieved at each iteration
│
├── transaction_scheduling/                paper §IV — predictive scheduler
│   ├── run_pipeline.py                    CLI driver — end-to-end scheduling demo
│   ├── llm_extract.py                     §IV.A.1 — LLM AST extraction
│   ├── ast_engine/                        selectivity estimation
│   ├── adapters/                          GenericAdapter — template-driven, chaincode-agnostic
│   ├── core/                              conflict engine, BPC scheduler, pipeline
│   └── templates/                         LLM-extracted AST templates
│       ├── smallbank_llm.json
│       ├── token_erc20_llm.json
│       └── ioheavy_llm.json
│
└── infra/                                 paper §V — experimental infrastructure
    ├── local_config_server/               runtime config endpoint for the agent
    ├── caliper-benchmarks/                Hyperledger Caliper workload definitions
    └── topology-manager/                  Ansible-based Fabric network deployment
```

***

## Quick Start

### Prerequisites

- Python 3.9+
- Node.js 18+ (only for live Caliper experiments)
- A DeepSeek API key — [platform.deepseek.com](https://platform.deepseek.com)
- Hyperledger Fabric 3.1.3&#x20;
- Hyperledger Caliper 0.6.0&#x20;

### 1. Set credentials

```bash
cp .env.example .env
# Edit .env and fill in DEEPSEEK_API_KEY
```

The code reads `DEEPSEEK_API_KEY`, `DEEPSEEK_BASE_URL`, `DEEPSEEK_MODEL` from
the environment. Use `python-dotenv` or `export` the variables before running.

### 2. Install Python dependencies

```bash
pip install langgraph langchain-openai openai
pip install numpy pyyaml
```

***

## Parameter Tuning (paper §III)

A LangGraph multi-agent system that navigates Fabric's 102-parameter
configuration space through a ReAct-style closed loop. The agent proposes
configuration changes, deploys them to a running Fabric network via a local
config server, measures throughput, and refines its proposals based on
structured reflection. A Harness mediates all agent–environment interaction:
it routes tool calls, and triggers
length-based context compression when the conversation history approaches the
LLM context window. Two semantic knowledge bases — Fabric parameter semantics
and per-topology cliff profiles — ground the agent's reasoning. A dedicated
Send-Rate Prober identifies the optimal `(block_size, send_rate)` pair for
each candidate configuration.

### Running the Parameter Tuning Experiment

Reproduces the main parameter-tuning evaluation described in paper §V. Requires
a running Fabric network (see [Infrastructure](#infrastructure-paper-v) below)
and the local config server.

**Step 1 — Start the config server** (one terminal):

```bash
python infra/local_config_server/server.py
```

The server exposes a REST API on `http://127.0.0.1:8080` that lets the agent
read and write Fabric parameters on the target network.

**Step 2 — Run the tuning agent** (another terminal):

```bash
cd parameter_tuning
  python main.py --url http://127.0.0.1:8080 --topology 8p --max-steps 10 --tps 2000
```

Key arguments:

| Flag          | Meaning                                         |
| ------------- | ----------------------------------------------- |
| `--topology`  | Fabric topology specifier (e.g. `8p` = 8 peers) |
| `--max-steps` | Maximum ReAct iterations                        |
| `--tps`       | Target raw throughput (transactions per second) |

The agent writes per-step state to `parameter_tuning/temp/`:

| File                      | Content                                        |
| ------------------------- | ---------------------------------------------- |
| `experience.jsonl`        | Per-trial structured deltas (episodic memory)  |
| `iteration.jsonl`         | Full per-step agent state log                  |
| `explore_send_rate.jsonl` | Send-Rate Prober trajectory                    |
| `best_config.json`        | Best-found Fabric configuration                |
| `best_send_rate.json`     | Optimal send\_rate paired with the best config |

***

## Transaction Scheduling (paper §IV)

A two-phase predictive scheduler. **Offline**: an LLM extracts symbolic AST
templates from chaincode source code; a background orchestrator samples the
world state to build histograms for single-variable predicates and joint
sample reservoirs for multi-variable correlated predicates. **Online**: each
incoming transaction is mapped to its template, branch probabilities are
evaluated against the cached statistics, paths are unioned under a confidence
threshold θ to predict the read/write set, a conflict graph is constructed
over those predicted sets, and a capacitated DSatur Bin-Packing-with-Conflicts
heuristic groups non-conflicting transactions into groups. A RecycleBucket /
Smart-Fill mechanism preserves block utilization under skewed workloads, and
the BPC step can run across multiple concurrent threads.

### LLM Template Extraction (one-shot, per chaincode)

```bash
cd transaction_scheduling
python llm_extract.py --source <path/to/chaincode.go> --output templates/<name>_llm.json
```

### AST Template Schema

Each template is a JSON document with this top-level structure:

```jsonc
{
  "chaincode": "<name>",
  "histograms":      ["Field1", ...],          // fields needing 1-D histograms
  "joint_groups":    [["F1", "F2"], ...],      // field groups needing joint reservoirs
  "bypass_functions": ["readOnlyFn1", ...],    // read-only functions skipped by scheduler
  "functions": {
    "<fn_name>": { "body": [ <node>, ... ] }
  }
}
```

**Node types:**

| Type      | Purpose                                        | Required fields                                     |
| --------- | ---------------------------------------------- | --------------------------------------------------- |
| `read`    | Records a read of a world-state key            | `key`                                               |
| `write`   | Records a write to a world-state key           | `key`                                               |
| `iterate` | Sequential loop with `${arg.N}` for loop index | `var`, `start`, `count`, `body`                     |
| `branch`  | Conditional execution path                     | `condition`, `then`, `else`, `then_tag`, `else_tag` |

**Condition kinds**:

| Kind        | Description                                    | Estimator                                         |
| ----------- | ---------------------------------------------- | ------------------------------------------------- |
| `predicate` | Argument-only check (e.g. `arg.0 < 0`)         | Deterministic from arguments                      |
| `linear_1d` | One state variable, linear comparison          | 1-D histogram cumulative lookup                   |
| `complex`   | ≥ 2 correlated fields or non-linear expression | Empirical satisfaction ratio over joint reservoir |


### Running the Transaction Scheduling Pipeline

The pipeline reads transactions from a JSONL buffer, predicts each
transaction's read/write set via the AST template, constructs a conflict
graph over the predicted sets, and groups non-conflicting transactions into
capacity-bounded batches.

```bash
cd transaction_scheduling
python run_pipeline.py \
    --buffer <path/to/txs.jsonl> \
    --template templates/smallbank_llm.json \
    --block-size 1000 --window-size 5000 \
    --scheduler-workers 5
```

## Infrastructure (paper §V)

| Path                                                       | Role                                                                          |
| ---------------------------------------------------------- | ----------------------------------------------------------------------------- |
| [`infra/local_config_server/`](infra/local_config_server/) | HTTP config server that exposes Fabric parameters to the tuning agent         |
| [`infra/caliper-benchmarks/`](infra/caliper-benchmarks/)   | Hyperledger Caliper workload definitions for Smallbank, Token-ERC-20, IOHeavy |
| [`infra/topology-manager/`](infra/topology-manager/)       | Ansible playbooks and scripts for deploying multi-peer Fabric networks        |

Reproducing the full evaluation (§V) requires:

- Hyperledger Fabric 3.1.3 with RAFT consensus
- 8–40 peer cloud VMs (8 vCPUs, 16 GiB RAM each)
- Hyperledger Caliper 0.6.0 driven by the configurations in `infra/caliper-benchmarks/`
- `npm install` inside `infra/caliper-benchmarks/` to restore Node dependencies
- Fabric network deployment via the numbered scripts under
  `infra/topology-manager/scripts/` (`0_init_resources.py` …
  `5_deploy_chaincode.py`); `run_experiment.py` drives Caliper benchmark runs
  on the deployed network

