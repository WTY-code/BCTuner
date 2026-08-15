## Reviewer #1

### 2. Brief summary of contributions

- This paper presents a harness-governed LLM agents for effective throughput optimization in execute-order-validate permissioned blockchains. The authors also evaluate the performance via a series of experiments.

### 3. Strong points

- Well motivated, and interesting topic.
- A novel method by using harness-governed LLM agents.
- The experimental reports upon Fabric network topologies shows the good performance against baselines.

### 4. Weak points
  - In this paper, the authors mainly describe the architecture of Auriga system. But the performance analysis can be improved.
  - In Table I, contention rate is critical for the overall performance. Better to describe the relationship between contention rate to tps in a common situation.

### 5. Detailed comments

- See comments above.

---

## Reviewer #2

### 2. Brief summary of contributions


- Auriga combines two optimizers around an unmodified Fabric deployment to improve effective throughput, defined as raw throughput times MVCC success rate. The first is an offline configuration search. LLM agents propose Fabric parameter settings, and each candidate is evaluated at its saturation point, because throughput measured at an arbitrary send rate does not reflect a configuration's capacity and each evaluation requires a network restart. The search outputs a send rate and a block capacity B, which the scheduler uses as fixed parameters. The second is an online scheduler, motivated by the fact that Fabric's execute-order-validate pipeline detects MVCC conflicts only at validation, after endorsement and ordering have been performed. An LLM extracts abstract-syntax-tree templates of each chaincode's state-accessing paths offline, and cached world-state statistics estimate the probability of each branch, the same computation a query optimizer performs for predicate selectivity. The predicted read-write sets define a conflict graph per scheduling window, which is colored into conflict-free groups of size B, and transactions that do not fit are deferred to a recycle bucket. The grouping is correct only if the templates are complete and all traffic passes through the gateway. Configurations are not changed after the search ends. The paper reports up to 6.0x improvement over the default Fabric configuration and 2.0x over the strongest baselines.

### 3. Strong points

  - The evaluation isolates each subsystem's contribution through hybrid composites (Athena+Auriga-S, Auriga-P+FabricSharp) in addition to comparing complete systems.
  - The scheduler formulates read-write-set prediction as selectivity estimation, classifies predicates by how they can be estimated (argument-only, linear one-dimensional against a histogram, complex via Monte Carlo), and sets theta near one because over-prediction reduces parallelism while under-prediction causes aborts. Scheduler overhead is measured (12.7k tx/s at 8 threads against a 1.2k tx/s commit ceiling) and is not the bottleneck.

### 4. Weak points

  1. The claim that no existing system jointly addresses capacity and conflict-induced loss is incorrect. AdaChain [R1] (VLDB'23) optimizes the same objective, successfully committed transactions per second under contention, by switching the blockchain architecture at runtime using reinforcement learning, and BFTBrain [R2] (NSDI'25) performs online adaptation at the consensus layer. Neither is cited. Relative to this line of work, the remaining contribution is the use of LLM agents instead of RL and a gateway scheduler instead of architecture switching, and the choice to tune once offline and keep the configuration fixed requires justification against systems that adapt at runtime.
  2. The survey of conflict handling omits the closest alternatives. HTFabric [R3] (CIKM'24) reorders and re-executes conflicting transactions in parallel within Fabric and should be a baseline in Fig. 6, since re-execution is the direct alternative to deferring transactions to a recycle bucket. XOX Fabric [R4] (ICBC'20) adds a post-order re-execution phase in the kernel. Block-STM [R5] (PPoPP'23) handles contention by speculative execution with re-execution and is deployed in production at Aptos. Forerunner [R6] (SOSP'21) predicts transaction behavior ahead of execution, the same premise as Auriga's scheduler. Anthemius [R7] (FC'25) assembles blocks into batches optimized for concurrent execution in a modular layer between mempool and consensus; given it, the new part of Auriga is predicting read-write sets without execution, which Fabric's pipeline requires and an order-execute system can obtain from execution or hints. The paper does not compare prediction-based avoidance against re-execution-based approaches. The grouping step itself, deterministic intra-batch reordering to avoid aborts, is the mechanism of Aria [R8] applied at a blockchain gateway.
  3. The templates, on which the scheduler's correctness depends, are LLM-generated and not validated. The theta threshold covers only the paths a template contains, and the extraction step is designed to discard paths, so an omitted state-accessing path produces the false negatives the scheduler is meant to prevent. No experiment compares predicted read-write sets against executed ground truth. The three chaincodes evaluated are simple, and the Fabric APIs that prevent key-level prediction (range queries, composite keys derived from state, cross-chaincode calls, private data collections) are not discussed. Several costs are also not measured. Latency is not reported, although a 5,000-transaction window corresponds to several seconds of queueing at the reported commit rates and the recycle bucket has no starvation bound. The trust model of the single gateway and the effect of traffic bypassing it are not discussed. The tuning comparison does not state the trial, wall-clock, or token budget per method, or the variance across runs of a nondeterministic search.

### 5. Detailed comments

- The related-work problem is the main one. AdaChain optimizes effective throughput under contention and adapts as the workload changes, while Auriga's configurations are fixed after the offline search and only the scheduler's histograms are updated. The taxonomy in Sec. VI (intrusive reordering versus reactive mitigation) does not cover re-execution-based and assembly-based approaches (HTFabric, XOX, Block-STM, Forerunner, Anthemius). There is a reasonable argument for prediction-based avoidance in Fabric, namely that re-execution requires kernel changes while avoidance can be implemented at a gateway, but the paper does not make it. HTFabric is Fabric-native, recent, and optimizes the same metric, so it belongs in Fig. 6.

  Three experiments are needed. First, template validation: execute the workloads, log the actual read-write sets, and report the per-chaincode miss rate of the predicted sets. Without this, the 90.2% success rate is a property of the three evaluated contracts rather than of the mechanism. Second, deployment sensitivity: state who operates the gateway in a multi-organization network and measure the effect of a fraction of traffic bypassing the scheduler, since the grouping assumes the orderer receives only scheduled traffic. Third, latency: the window and the recycle bucket both trade delay for success rate, Fig. 7(b) reports this trade-off without a delay axis, and deferred transactions should be reported either as end-to-end latency distributions or as goodput under a deadline.

  The scheduler and the hybrid-composite evaluation are worth keeping. My recommendation is major revision, and the missing related work alone justifies it.

  Questions for the response:

  1. What is Auriga's contribution relative to AdaChain [R1] and BFTBrain [R2], and how does a configuration fixed at deployment handle workload drift?
  2. Why is prediction-based avoidance preferable in Fabric to re-execution as in HTFabric [R3], and can HTFabric be added as a baseline in Fig. 6?
  3. What is the measured false-negative rate of predicted read-write sets against executed ground truth, per chaincode, and how do templates handle range queries, state-derived composite keys, and cross-chaincode calls?
  4. What are the end-to-end latency distributions including recycled transactions, what bounds waiting time in the recycle bucket, and who operates the gateway in a multi-organization deployment?

  References not in the submission's bibliography:
  [R1] C. Wu, B. Mehta, M. J. Amiri, R. Marcus, B. T. Loo. AdaChain: A Learned Adaptive Blockchain. PVLDB 16(8), 2023.
  [R2] C. Wu, H. Qin, M. J. Amiri, B. T. Loo, D. Malkhi, R. Marcus. BFTBrain: Adaptive BFT Consensus with Reinforcement Learning. NSDI 2025.
  [R3] J. Song, J. Jeong, J. Lee, I. Na, M.-S. Kim. HTFabric: A Fast Re-ordering and Parallel Re-execution Method for a High-throughput Blockchain. CIKM 2024.
  [R4] C. Gorenflo, L. Golab, S. Keshav. XOX Fabric: A Hybrid Approach to Blockchain Transaction Execution. ICBC 2020.
  [R5] R. Gelashvili, A. Spiegelman, Z. Xiang, G. Danezis, Z. Li, D. Malkhi, Y. Xia, R. Zhou. Block-STM: Scaling Blockchain Execution by Turning Ordering Curse to a Performance Blessing. PPoPP 2023. arXiv:2203.06871.
  [R6] Y. Chen et al. Forerunner: Constraint-Based Speculative Transaction Execution for Ethereum. SOSP 2021.
  [R7] R. Neiheiser, L. Kokoris-Kogias. Anthemius: Efficient & Modular Block Assembly for Concurrent Execution. FC 2025. arXiv:2502.10074.
  [R8] Y. Lu, X. Yu, L. Cao, S. Madden. Aria: A Fast and Practical Deterministic OLTP Database. PVLDB 13(12), 2020.

### 7. Inclusive writing
- No repository, artifact statement, prompts, knowledge bases, templates, or tuning traces are provided. Both subsystems depend on LLM outputs, so the results cannot be reproduced without them: the tuning outcome depends on a single run of the agent search, and the scheduler's correctness depends on the extracted templates.

---

## Reviewer #3

### 1. Reviewer confidence

- Knowledgeable

### 2. Brief summary of contributions

- The paper proposes an LLM-agent-driven approach to improving throughput in Hyperledger Fabric by combining offline symbolic execution (to predict transactions' read-write sets and reduce conflicts) with LLM-based agents that navigate the system's configuration space. The direction of applying LLM agents to blockchain performance tuning is novel, but the paper's description of the system architecture and the integration between the agents and Fabric is unclear.

### 3. Strong points

  1. The paper explores the trade-off between processing capacity and workload contention.
  2. Using LLM agents to tune Fabric configuration parameters is an interesting direction.
  3. The combination of symbolic execution and world-state statistical modeling to predict transactions' read-write sets without executing them is a reasonable technique.

### 4. Weak points

  1. The description of the online phase appears inconsistent with Fabric's actual architecture. In Fabric, clients submit transactions to endorsers, which execute them and return endorsement results to the clients; clients then submit the endorsed transactions to the ordering service, where batching and ordering occur. Given this, it is unclear what "grouping non-conflicting transactions into batches before submission" refers to, and at what point in this flow it takes place. Does each endorser receive only a subset of transactions? A clearer, step-by-step description of the online phase and how it maps onto Fabric's client-endorser-orderer flow would help.
  2. The system architecture and the mechanics of how the agents integrate with the Fabric system are not clearly explained. Figure 1 does not sufficiently clarify this integration, and a more detailed diagram or walkthrough is needed.
  3. Following on point 1, it would help to clarify precisely when batching and reordering of non-conflicting transactions occur relative to the standard Fabric pipeline (endorsement, submission to ordering service, ordering/batching). As written, it is difficult to tell whether this happens before, during, or after the ordering service's normal batching process.
  4. It is unclear where the agents run and what their failure model is. If the agents are deployed separately from endorsers and orderers, this introduces additional resource overhead that should be quantified. Additionally, since each agent appears to handle a distinct role, the paper should discuss what happens to system correctness and performance if an individual agent fails.
  5. The experimental evaluation lacks important details, including the number of orderers and endorsers used, the endorsement policy, and any consideration of geo-distributed node deployments, which can materially affect performance and should be discussed or evaluated.
  6. AdaChain addresses a similar goal by allowing the system to switch between different concurrency control paradigms (e.g., XOV, OE, EOV#). A discussion of this related work and an experimental comparison are needed.

### 5. Detailed comments

- Replacing Figure 1 with an architecture and workflow diagram would make the contribution clearer.