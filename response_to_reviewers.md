# Response to Reviewers (no-revision rebuttal)

> Scope: This document answers every weak point and question raised by
> Reviewers #1, #2, and #3 strictly from content already present in the
> submitted manuscript. Where a concern genuinely cannot be answered from
> the existing text, it is flagged as an open limitation to be addressed
> in the camera-ready rather than defended with fabricated content.

---

## Reviewer #1

### W1 — "Performance analysis can be improved."

Our evaluation spans three orthogonal axes already reported in Sec. V:

- **Topology scale** (Fig. 3(a–c)): 8→40 peers under fixed 30% contention.
  Auriga leads across all scales; the gap over the strongest independent
  baseline is 105% at 8 peers and remains 105–130% at 40 peers.
- **Contention** (Fig. 3(d–f) and Fig. 6): conflict rate swept from 10% to
  70%.
- **Component ablation** (Table II): hybrid composites
  (Athena+Auriga-S, Auriga-P+FabricSharp) isolate the contribution of each
  subsystem; Fig. 4 further isolates Auriga-P against
  Athena/AgentTune/GPTuner, and Fig. 6 further isolates Auriga-S against
  Default/FabricSharp.
- **Scheduler cost**: Fig. 7(a) shows near-linear scheduler throughput
  scaling (2.0k → 12.7k tx/s, 1–8 threads) against the 1.2k tx/s Fabric
  commit ceiling — scheduling is not a bottleneck.

### W2 — Contention → TPS relationship in a common situation.

The relationship is quantified twice:

- **Table I** (motivation): at low contention Default achieves
  TPS_eff = 294 with R_succ = 91.4%; at high contention TPS_eff collapses
  to 205 with R_succ = 63.1%. Parameter-tuning-only (Athena [5]) raises
  capacity to 907 → 469 but its R_succ decays from 89.9% to 69.7%, showing
  that raw capacity gains are consumed by MVCC waste as contention rises.
  Auriga sustains R_succ = 97.7% → 90.2%, yielding TPS_eff = 1,348 → 1,226.
- **Fig. 3(d–f)** shows the full curve from 10% to 70% conflict for all
  three chaincodes; the monotone decay of every baseline as contention
  rises is the "common situation" curve requested. On Smallbank, Default
  drops from ~610 to ~200 eff-TPS across this range; Auriga drops from
  ~1,200 to ~290 — the slope is gentler because the scheduler removes
  intra-block conflicts before block cutting.

---

## Reviewer #2

### W1 — Missing AdaChain [R1] and BFTBrain [R2]; positioning relative to runtime-adaptive systems.

Acknowledged: neither reference is cited. What the paper *does* say about
this design axis is in Sec. II.B and Sec. III.B: Auriga's throughput-aware
evaluation is intentionally *offline* because "evaluating a single
configuration in a blockchain requires updating parameters across all
nodes and restarting the entire network" (Sec. I), i.e., the per-trial
cost that makes online adaptation of block-cutting/gossip parameters
expensive. Runtime adaptivity in AdaChain switches concurrency-control
*architecture* (XOV/OE/EOV#) and BFTBrain adapts the *consensus protocol*
— neither retunes Fabric's static block-cutting/gossip knobs online, which
is Auriga-P's scope. Auriga's runtime adaptivity lives in Auriga-S, whose
histograms are "periodically synchronized and updated via background Monte
Carlo re-sampling" (Sec. IV.A.1) — this is our answer to workload drift
for the *scheduling* subsystem. Static-config drift is a fair open
limitation; we can commit to citing [R1,R2] and delineating scope in the
camera-ready.

### W2 — Missing conflict-handling alternatives (HTFabric, XOX Fabric, Block-STM, Forerunner, Anthemius, Aria).

The paper's taxonomy (Sec. VI) distinguishes tuning approaches from
conflict-mitigation approaches, and within the latter names Fabric++ [13],
FabricSharp [7], and FabricTL [14] — all of which are intra-block
reordering / early-abort approaches. Auriga-S is deliberately compared
against FabricSharp because both operate pre-ordering; the design argument
in Sec. IV (introduction and B.3) is that Auriga-S is **prediction-based
avoidance at the gateway** so it "avoids MVCC aborts without modifying the
blockchain kernel" (Abstract), whereas re-execution approaches
(HTFabric/XOX/Block-STM/Forerunner) require kernel or execution-path
changes. This design point is on the reviewer's side: the reason we do not
compare to Block-STM/XOX is that those require modification to the EOV
core, which is the deployability constraint Sec. II.B invokes. We agree
HTFabric is closer (Fabric-native re-execution) and can be added to Fig. 6
in the camera-ready without altering the paper's central claim.

### W3 — Template validity, missing latency, gateway trust, missing tuning-cost accounting.

Partial answers exist in the paper; the rest is honestly an open
limitation.

- **Template safety asymmetry** (Sec. IV.B.2): the near-unity θ
  deliberately over-predicts read-write sets, so "the predicted read-write
  set covers the true accessed keys with high confidence, minimizing MVCC
  aborts. The attendant rise in false positives is harmless, only
  moderately curtailing parallel throughput." This is the mechanism-level
  guardrail against template omissions — a missing path costs parallelism,
  not correctness, provided the *retained* paths cover the true accesses.
- **End-to-end success rate is reported** as R_succ in Table I (90.2% at
  high contention) and throughout Fig. 3/6. This is the closest
  measurement to "false-negative rate of predicted RW-sets" the paper
  offers today: every MVCC abort observed on Auriga corresponds to a
  prediction miss. Per-chaincode ground-truth logging is not in the
  current draft.
- **Latency / recycle-bucket bound / gateway operator / tuning wall-clock
  and variance**: not currently reported. Acknowledged as open for
  camera-ready.
- **Fig. 7(b)** does trade window size against success rate (73% → 99% as
  W: 3k→7k) at a 6% throughput cost — this is the closest existing figure
  to the "delay-vs-success" trade-off.

### Q1 — Contribution vs AdaChain/BFTBrain; handling workload drift with a fixed config.

Auriga-P and those systems solve different problems: AdaChain switches
concurrency-control architectures, BFTBrain adapts the consensus protocol,
whereas Auriga-P tunes Fabric's static configuration space (block cutter,
gossip, timeouts — Sec. III.A) which those systems leave fixed. Drift is
handled *in the scheduler*: predicate selectivity is recomputed against
periodically refreshed world-state histograms and Monte-Carlo re-samples
(Sec. IV.A.1 and Sec. IV.B.1), so the read-write predictions track
workload changes even when the offline configuration is frozen. The design
bet is that block-cutting/gossip optima shift on infrastructure
time-scales (topology, hardware), while contention shifts on workload
time-scales — the two are separated by design.

### Q2 — Why prediction over re-execution; can HTFabric be added?

The deployability argument is in Sec. IV introduction and Sec. VI
(Summary): prediction-based avoidance runs at the gateway and needs no
kernel patch, whereas re-execution requires modifying the peer's
validation pipeline. In multi-organization Fabric this matters because
peers are operated by different orgs and version-locked; the gateway is a
client-side component. HTFabric is Fabric-native and can be added as a
Fig. 6 baseline in the camera-ready; it does not disturb the paper's
positioning because HTFabric mitigates conflicts *after* endorsement
waste, whereas Auriga-S groups *before* it.

### Q3 — False-negative rate vs executed ground truth; range queries, composite keys, cross-chaincode.

The paper does not currently report per-chaincode template-miss rates
against logged ground truth; the observable proxy is R_succ (Table I,
Figs. 3/6). Aggregating over the three chaincodes at 30% contention, the
residual MVCC rate is <10%, giving an upper bound on the template
false-negative rate under the workloads evaluated. The classification in
Sec. IV.A.1 (predicate / linear_1d / complex) explicitly handles complex
predicates via offline Monte Carlo; range queries and composite /
cross-chaincode keys fall into the "complex" bucket by design. What the
paper does not do is enumerate the specific Fabric APIs
(GetStateByRange, composite keys, cross-chaincode invocation, PDCs) or
quantify template coverage on chaincodes that exercise them — this is a
fair limitation. **See "Extensibility to hard Fabric APIs" below for
the mechanism-level answer to how each of these APIs fits Auriga-S.**

### Q4 — Latency, recycle-bucket bound, gateway operator.

Not reported in the current manuscript. What *is* discussed:

- Recycle bucket is described in Sec. IV.C. It has no explicit starvation
  bound in the current design — deferred fragments are absorbed
  opportunistically by SmartFill into subsequent under-filled groups, but
  no deadline is enforced. This is an honest limitation.
- The 5,000-tx default window (Fig. 7(b)) is the paper's disclosed
  latency-vs-success knee.
- Gateway operator/trust model: not discussed. The scheduler assumes all
  traffic passes through a single scheduling boundary; multi-org
  deployment is not evaluated.

---

## Reviewer #3

### W1 & W3 — Online-phase mechanics and where batching/reordering sits in Fabric's client–endorser–orderer flow.

The paper's placement is: Auriga-S is a **client-side gateway** that
reorders transactions *before endorsement submission* (Sec. II.B:
"grouping non-conflicting ones into batches before submission" and
Sec. IV.C: "The batches are injected at the calibrated send rate").
Concretely, the flow is:

1. Incoming client transactions accumulate in a *scheduling window*
   (default 5,000 tx — Fig. 7(b)) at the gateway.
2. The scheduler predicts each tx's RW-set using AST templates + cached
   world-state statistics (Sec. IV.A–B).
3. It builds the conflict graph (Sec. IV.B.3), applies capacitated DSatur
   BPC (H2 heuristic) to produce color groups of size ≤ B =
   MaxMessageCount, and calls `ScheduleTransmission(G_i)` (Sec. IV.C)
   which pushes the group into the submission queue.
4. Endorsements happen normally at endorser peers; endorsed transactions
   arrive at the orderer in the scheduler's near-global order because
   Auriga-P sets B = MaxMessageCount, so the orderer's own batch-cut
   boundary aligns with the scheduler's group boundary.

Each endorser still sees every transaction it is entitled to endorse (per
the endorsement policy) — Auriga-S does not shard traffic across
endorsers; it only orders it. This is the coupling between Auriga-P and
Auriga-S alluded to in Sec. II.B: temporal decoupling via `send_rate` and
`B`.

### W2 — Fig. 1 doesn't sufficiently show integration.

Acknowledged. What Fig. 1 (right panel) does show is the internal
offline→online pipeline of the scheduler. The Fabric-facing arrow
"submit → Fabric Network" is a single edge; the reviewer's request to
expand this into an architectural walkthrough (client → gateway →
endorsers → orderer → peer validation) is a legitimate figure-improvement
request for the camera-ready.

### W4 — Where the agents run and their failure model.

Every agent in the paper is **offline**. The Main Agent, Send-Rate Prober,
Experience Distiller (Sec. III.A) run only during the pre-deployment
tuning phase; likewise the LLM-based AST extractor runs one-shot per
chaincode (Sec. IV.A). No agent is on the runtime path. The Harness
Runtime (Sec. III.B) is the only long-lived process, and it is external to
Fabric peers/orderers. Consequently: an agent failure during tuning aborts
the tuning run and can be restarted from the append-only
`experience.jsonl` (Sec. III.B.3, episodic memory) — no runtime
correctness impact. Runtime scheduling relies only on cached AST templates
and histograms, not on LLM availability. Resource overhead of the agents
is offline-only and does not compete with peer resources at runtime. This
is explicit in the paper's split-phase framing but could be stated more
prominently.

### W5 — Missing experimental details (endorser/orderer counts, endorsement policy, geo-distribution).

The paper reports: 8- to 40-peer topologies "deployed on public cloud VMs
(8 vCPUs, 16 GiB RAM)", "One VM hosts every four nodes", each topology
has "a dedicated master node running the tuning agent and configuration
server, with worker nodes hosting Fabric peers, and orderers" (Sec. V.A).
Endorsement policy and orderer count per topology are not stated;
geo-distribution is not evaluated (single-datacenter). Reasonable to
acknowledge and to add the deployment table in the camera-ready.

### W6 — AdaChain comparison.

See response to Reviewer #2 W1/Q1: AdaChain adapts the concurrency-control
paradigm at runtime; Auriga tunes Fabric's static configuration space
offline and does predictive conflict avoidance at a gateway. These target
different mechanisms and are not head-to-head substitutes, but the
omission is fair; will be added.

### Detailed comment — replace Fig. 1 with an architecture+workflow diagram.

Noted for the camera-ready.

---

---

## Extensibility to hard Fabric APIs (Reviewer #2, W3 / Q3)

The reviewer's concern is that "the three chaincodes evaluated are
simple, and the Fabric APIs that prevent key-level prediction (range
queries, composite keys derived from state, cross-chaincode calls,
private data collections) are not discussed." The paper indeed does not
discuss these APIs. We answer at the **mechanism level**: Auriga-S's
prediction abstraction is *a covering set of key strings*, guarded by
the near-unity θ over-prediction rule (Sec. IV.B.2). Any API reducible
to a covering key set — even a probabilistic one — fits the existing
evaluator, conflict engine, and BPC pipeline **without changes to the
online scheduler**. Under this abstraction, each of the four APIs the
reviewer names decomposes as follows.

### 1. Range queries — `GetStateByRange`, `GetPrivateDataByRange`, `GetQueryResult`

*Where it fits.* A range read `[startKey, endKey)` is already a covering
key set; it just happens to be described by two endpoints rather than
enumerated point keys. The AST needs one interval-typed node
(`range_read` / `range_write`) that carries `(prefix, start_expr,
end_expr)`. At scheduling time the conflict engine's key-intersection
check (Sec. IV.B.3, `W_u ∩ R_v ≠ ∅` and `R_u ∩ W_v ≠ ∅`) extends
naturally: an interval read conflicts with a point write iff the point
falls in the interval, and with another interval iff the intervals
overlap. Both are O(log n) with a sorted key index — no algorithmic
change to BPC.

*Selectivity.* Where the range endpoints depend on state (e.g., "read
all accounts whose balance ≥ X"), the exact prior mechanism applies:
the range predicate is classified as `linear_1d` (single field, indexed
by histogram) or `complex` (multi-field, joint reservoir; see
`ast_engine/complex_estimator.py`), and the offline Monte Carlo
estimator returns a probabilistic covering interval. For CouchDB rich
queries (`GetQueryResult` with a Mango selector), the LLM extracts the
selector expression from the query string into the same
`complex`-condition schema.

*Safe default.* When the LLM cannot bound a range (opaque predicate or
missing index), the θ-guarded rule dictates the conservative fallback:
declare the read set to be the full collection prefix. This is
performance-degrading (that transaction serialises with all writers
within the prefix) but never unsafe.

### 2. Composite keys derived from state — `CreateCompositeKey(objectType, attrs)` where `attrs[i]` is read from state

*Where it fits.* The AST schema **already** has a `CompositeKey`
node with `(prefix, parts)` (`ast_engine/nodes.py:23–27`), and the
adapter validator (`adapters/generic.py`) accepts `composite_key` as a
first-class node type. What the current templates do not exercise is
the case where a part is state-derived rather than argument-derived.

*Handling.* A state-derived part is structurally identical to a
`complex`-kind branch condition: it is a function of one or more world-
state fields whose value distribution is captured by the same joint
reservoir mechanism (`joint_reservoir_builder.py`). Instead of
emitting a probability, the estimator emits the top-k most probable
values of the composite part, and the evaluator's variant tree expands
into k parallel variants (analogous to the branch expansion in
`evaluator.py:96–124`). The θ-cut then unions the top-probability
composite keys until cumulative probability ≥ θ, and the tail is
discarded exactly the way improbable branches are today.

*Safe default.* Missing top-k coverage collapses to the full composite
prefix — the same over-prediction fallback used for range queries.

### 3. Cross-chaincode calls — `InvokeChaincode(cc, args, channel)`

*Where it fits.* This is an **offline-only** change to the extractor
(`llm_extract.py`), not a runtime change. The current extractor
processes one chaincode at a time and produces one template registry.
To handle cross-chaincode calls, the extractor loads the invoked
chaincode's template, resolves the invoked function name, and inlines
its body at the call site — the same way a compiler inlines a
callee. The reads/writes emitted at runtime are already namespaced by
key prefix; extending prefixes to `"<cc_name>:<key>"` prevents
cross-chaincode false conflicts by construction (two chaincodes' key
"balance:alice" no longer collide). No change to the evaluator, the
conflict engine, or BPC.

*Safe default.* If the target chaincode's template is unavailable at
extraction time, the calling path is annotated as `complex` with an
opaque estimator — degenerating to "assume the callee touches its
entire keyspace", again the correct-but-conservative behavior.

### 4. Private data collections — `GetPrivateData / PutPrivateData / GetPrivateDataByRange`

*Where it fits.* PDCs sit in a **separate MVCC namespace** per
`(collection, key)`. The scheduler already treats reads/writes as
opaque strings, so extending the key prefix to
`"pdc:<collection>:<key>"` cleanly separates PDC namespaces from public
state and from each other. No cross-collection false conflicts appear
because their prefixes differ; per-collection conflicts are handled by
the exact mechanism used for public state today. Range and composite
variants (`GetPrivateDataByRange`,
`GetPrivateDataByPartialCompositeKey`) reduce to cases 1 and 2 with the
PDC prefix.

*Hash reads.* `GetPrivateDataHash` returns the hash of the value; it
touches the same `(collection, key)` slot and is a read against the
same PDC key. No new abstraction needed.

*Safe default.* If a chaincode gates a public write on a PDC read
(cross-namespace correlation), the LLM emits the two accesses as
independent nodes; the θ-guard ensures the public write is grouped
against its own conflict class, and the PDC read against the PDC
class. False positives arise only when a real correlation exists that
the LLM failed to encode — the same failure mode already discussed for
public-state correlations.

### Summary — why the mechanism does not break on harder chaincodes

All four APIs reduce to *covering key set* prediction, which is the
level of abstraction the online scheduler already operates at. Table
below cross-references each API against what the current codebase
supports and what would need to be added:

| Fabric API                              | Runtime pipeline change | AST schema change            | Failure mode when LLM misses                       |
| --------------------------------------- | ----------------------- | ---------------------------- | -------------------------------------------------- |
| `GetStateByRange`, rich queries         | none                    | add `range_read/write` node  | fall back to full collection prefix (safe)         |
| State-derived `CompositeKey`            | none                    | none (schema already exists) | fall back to full composite prefix (safe)          |
| `InvokeChaincode`                       | none                    | none (offline inlining)      | fall back to opaque callee estimator (safe)        |
| `GetPrivateData` / PDC range / PDC hash | none                    | key prefix `pdc:<coll>:`     | independent PDC/public conflict classes (safe)     |

The last column is the same guarantee already claimed in Sec. IV.B.2
for the evaluated chaincodes: over-prediction costs parallelism, not
correctness. This is the argument for why demonstrating the mechanism
on Smallbank / Token-ERC-20 / IOHeavy — which exercise argument-only,
`linear_1d`, and iterated APIs — establishes the *principle*; the
schema and estimator infrastructure are already the ones the extended
APIs would use. Empirical validation on richer chaincodes is a fair
camera-ready follow-up.

---

## Suggested framing for the letter

- **Cluster the concessions**: cite AdaChain / BFTBrain / HTFabric / XOX /
  Block-STM / Forerunner / Anthemius / Aria; add an HTFabric bar to
  Fig. 6; add a Fabric-flow walkthrough figure; add per-topology
  endorser/orderer/policy table.
- **Defend from existing content**: contention→TPS relationship
  (Table I + Fig. 3(d–f)); scheduling overhead (Fig. 7(a)); window-size
  vs success trade-off (Fig. 7(b)); θ-guarded over-prediction as the
  correctness argument for LLM-extracted templates; offline-only LLM
  footprint as the failure-model argument.
- **Honestly own three open items** rather than fabricating answers:
  (i) per-chaincode template-miss rate against logged ground truth,
  (ii) end-to-end latency distributions and a recycle-bucket starvation
  bound,
  (iii) multi-org gateway trust model. All three can be flagged as
  camera-ready additions without disturbing the paper's central claims.
