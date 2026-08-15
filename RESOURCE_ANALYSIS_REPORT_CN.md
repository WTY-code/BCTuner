# Fabric 提交速率瓶颈分析报告（v2 · 修订版）

**项目**：Auriga (PerfTuner) — 面向 ICDE 投稿
**测试床**：Fabric 3.1.3，8 个 peer 跨两台虚拟机部署 —
`host153`（192.168.0.153，Org1 + orderer0/1）与 `host212`（192.168.0.212，Org2 + orderer2），LevelDB 后端，论文的 `best_config.json`（MMC=1000，BatchTimeout=200 ms，PreferredMaxBytes=7 MB）
**日期**：2026-08-01

> **本版相比 v1 的核心变化**：v1 使用 Caliper 作为压测工具，观察到 CPU 消耗在 4 个 peer 之间高度不均衡（peer0.org1 独占 3 核），据此推断"背书请求分发不均是根本原因"。
>
> v2 改用**项目自研的 `MultiPeerSubmitter`**（论文提出的**按 tx 粒度的 round-robin 分发**）复测，发现即使背书负载完全均分（每个 peer 约 20 %），主机 CPU 仍然被打满。**因此 v1 的"分发不均"结论被推翻**。真正的根因在**下一层**——Fabric peer 侧的**提交（commit）阶段**——本报告用**分阶段在飞数（in-flight per stage）**数据将其精确定位。

---

## 一、一句话结论

> **Fabric 在本部署下 ≈ 1406 TPS（零冲突场景）的提交上限，根因是 peer 侧的 commit 阶段被 CPU 卡住。**
> 具体表现：submit → endorse → broadcast 三阶段全都跑到 1494 tx/s，但 commit 阶段只能吃进 1406 tx/s，导致**平均 1113 条交易积压在 peer 侧等待提交**（endorse 阶段的积压只有 83 条）。**四个 peer 的 CPU 各占系统 20 %，加总即打满整机**——这个每 tx 的 CPU 成本无法通过分发均衡来消除。

---

## 二、实验方法（本版的关键改进）

### 2.1 用自研提交器代替 Caliper

- **提交器**：`transaction_scheduling/submitter/`（Node.js + Python）
- **分发策略**：15 个 worker，跨 8 个 peer 按**单条交易粒度**做 round-robin
- **好处**：每笔交易的 4 个阶段时间戳（`t_submit_start` / `t_endorse_done` / `t_broadcast_done` / `t_commit_seen`）全部记录到 `tx.jsonl`，可以做**分阶段在飞数分析**——这是 Caliper 无法提供的

### 2.2 零冲突工作负载

- 3 万条 `create_account` 交易，account 键完全独立（编号 100000-129999，与 seed 的 10k 帐户不冲突）
- 每条 tx 写入全新的键 → **零 MVCC 冲突**
- 目的：把 Fabric 的成功率固定在 100 %，让观测到的上限就是**纯 Fabric 处理能力**，不再夹杂重试损耗

### 2.3 资源采集（与 v1 一致）

- 100 ms 快循环（cgroup v2 + `/proc`）
- 1000 ms 慢循环（Fabric Prometheus 端点）
- 采集器自身 CPU 开销 < 0.1 % of one core

### 2.4 实验结果概览

```
[RESULT N=15 tps=1500]
  send=1439  raw=1415  eff=1415
  succ=1.000  sub_span=20.9s  com_span=21.2s
  codes={'VALID': 30000}
```

30000/30000 全部 VALID，零 MVCC 冲突；稳态 10 秒窗口下 **commit-TPS 均值 1416，p95 1650**。

---

## 三、关键数据

### 3.1 分阶段在飞数（**本版的新证据，直接定位瓶颈**）

稳态窗口内每 100 ms 采样一次，统计每一时刻**正处于该阶段但尚未推进到下一阶段**的交易数：

| 阶段 | 在飞数 均值 | 在飞数 p95 | 在飞数 最大 | 阶段完成速率 |
|:-|-:|-:|-:|-:|
| awaiting_endorse | **83** | 123 | 150 | endorse_done: **1494 tx/s** |
| awaiting_broadcast | **19** | 53 | 63 | broadcast_done: **1493 tx/s** |
| **awaiting_commit** | **1113** | **1580** | **1718** | commit_seen: **1406 tx/s** |

**读法**：

1. **endorse 与 broadcast 完全跟得上 submit**——三者速率都在 1493-1494 tx/s 之间，在飞数分别只有 83 和 19 条，几乎瞬时通过。
2. **commit 完成速率只有 1406 tx/s**，比前置阶段少 ~88 tx/s。这个差额每秒积累一次，20 秒下来就是 ~1700 条积压——与观测到的 awaiting_commit 最大值 1718 完全对得上。
3. **在飞数比 endorse : broadcast : commit ≈ 83 : 19 : 1113**——**commit 阶段的堆积是前置阶段的 13-58 倍**，一目了然。

**结论一（新）**：**瓶颈阶段就是 commit**。这是**分阶段在飞数**这一手段给出的直接答案，v1 只能通过 `diagnose_ceiling` 间接推断"validation + ledger write is the wall"，v2 用 in-flight backlog 把它变成了可视化的定量证据。

### 3.2 主机资源饱和度

| 指标 | host153（Org1 + orderers） | host212（Org2 + orderer2） |
|:-|-:|-:|
| **主机 CPU（均值 / p95 / 最大）** | **99.6 % / 100 % / 100 %** | **97.4 % / 100 % / 100 %** |
| 主机内存已用 | 22.6 % | 20.7 % |
| 数据盘 `%util`（最大） | 54 % (vda) | 56 % (vda1) |
| 网卡 eth0 峰值 | 408 Mbps（TX） | 357 Mbps（RX） |
| Fabric peer 的 `go_goroutines` 增长 | +0.42/s | +0.36-0.42/s |

**结论二**：**双机 CPU 均被打满**（p95=100 %）。内存、磁盘、网络仍有大量余量。goroutine 数以 0.4/s 缓慢增长，这是 commit 积压对 gRPC 处理线程池施加的反向压力，与 3.1 节的现象一致。

### 3.3 各 peer 容器 CPU 分解（本版：**分发已均衡，但 CPU 仍然打满**）

**host153**：

| 容器 | 均值 | p95 | 最大 | 折算物理核数（8 核） |
|:-|-:|-:|-:|-:|
| peer0.org1 | 19.7 % | 27.7 % | 36.1 % | ≈ 1.6 核 |
| peer1.org1 | 20.2 % | 28.6 % | 33.1 % | ≈ 1.6 核 |
| peer2.org1 | 19.2 % | 28.2 % | 30.7 % | ≈ 1.5 核 |
| peer3.org1 | 21.6 % | 30.4 % | 34.7 % | ≈ 1.7 核 |
| orderer0 | 4.6 % | 6.3 % | 7.0 % | ≈ 0.4 核 |
| orderer1 | 4.6 % | 6.2 % | 6.9 % | ≈ 0.4 核 |
| **合计** | **89.9 %** | — | — | **≈ 7.2 核 / 8 核** |

**对照——v1 使用 Caliper 时的 host153 分布**：

| 容器 | Caliper CPU 均值 | v2 (我们的提交器) CPU 均值 |
|:-|-:|-:|
| peer0.org1 | **38.9 %**（独占 3 核） | 19.7 % |
| peer1.org1 | 13.9 % | 20.2 % |
| peer2.org1 | 13.9 % | 19.2 % |
| peer3.org1 | 15.0 % | 21.6 % |

**结论三（新）**：**我们的提交器已经把负载完美地均分到 4 个 peer 上**（Caliper 分布相差 2.5 倍，v2 相差 < 1.13 倍）——然而**主机 CPU 依然打满**。这直接**证伪了 v1 的"分发不均是根本原因"猜想**。

---

## 四、瓶颈定性（修订）

综合 3.1、3.2、3.3 的证据：

> **Fabric 上限的物理根因是 peer 侧的整机 CPU 饱和；上限的阶段位置是 commit（验证 + 写账本）；分发均衡不能解决它。**

支撑：

1. **阶段位置**（3.1）：commit 阶段积压 1113 条，比 endorse/broadcast 高 1-2 个数量级；commit 完成速率比前置阶段低 ~88 tx/s。
2. **物理资源**（3.2）：整机 CPU p95 = 100 %；内存、磁盘、网络均有余量。
3. **分发无救**（3.3）：每个 peer 消耗 ~20 % 系统 CPU 是**固定成本**（因为每个 peer 都要独立执行完整的 MVCC 验证 + ledger 写 + gossip），4 peer × 20 % + orderer + 系统开销 = ~100 %——**这是 Fabric peer 处理路径的天然并行度极限**。

---

## 五、根因解释（修订）

Q：为什么每个 peer 的 CPU 消耗如此均衡地卡在 ~20 %？

A：因为 Fabric 的 commit 阶段有**天然的对称性开销**：

**背书阶段（endorsement）是可分发的**——客户端可以选择任意 peer 做模拟执行。v1 中 Caliper 把请求集中在 peer0，v2 中我们的 round-robin 均匀分发，两种情况下背书总工作量相同、但 CPU 分布不同——所以背书**不是瓶颈**（本身只占很少一部分）。

**提交阶段（commit）不能分发**——**每个 peer 都必须独立完成同一区块的验证 + 落盘**：

- **MVCC 读检查**：Fabric 3.x 仍在 channel 级别的一把大锁下顺序验证一个区块内所有 tx 的读写集
- **签名与背书策略校验**：每 tx 至少 1-2 次 ECDSA 校验
- **Ledger 追加**：blockfile 追加 + LevelDB stateDB 批量写 + hist DB 更新
- **Gossip 广播**：向其他 peer 转发已提交区块的元信息

**这四步在每个 peer 上都要跑一遍，各占 ~20 % CPU；4 peer 平摊即打满整机**。**这不是分发不均的问题，而是 Fabric 每 tx 的 peer 侧固定 CPU 成本乘以副本数超过了机器算力**。

---

## 六、v1 结论与 v2 结论对比

| 维度 | v1（Caliper） | v2（自研 submitter + 分阶段分析） |
|:-|:-|:-|
| 上限阶段 | 推断 "validation + ledger write"（间接） | **直接观测：commit 阶段 in-flight 高出 13-58 倍** |
| 上限资源 | CPU（正确） | CPU（一致确认） |
| CPU 分布 | 高度不均：peer0.org1 独占 3 核 | 均匀：每个 peer ~1.5-1.7 核 |
| 根因归属 | ~~Caliper 分发策略偏斜~~（**已推翻**） | Fabric peer 侧 commit 路径的每 tx 固定 CPU 成本 |
| 建议方向 | 修改 endorser 分发策略 | 减小每 tx 的 commit CPU 成本；或增加算力 |

---

## 七、下一步建议（修订）

基于 v2 定位，v1 的"分发均衡实验"**已经隐式做过**（我们的自研提交器就是均衡的），并且**没有提高上限**，因此该方向可以放弃。

建议按以下优先级推进：

### 7.1 立即可做

**（A）在 peer0.org1 上做 `pprof` CPU 火焰图**
- 目标：确认 commit 阶段的 CPU 消耗集中在哪个函数
- 常见热点：`ValidateAndCommit` / `hasWriteConflicts` / `verifyEndorsement` / LevelDB `Put/WriteBatch`
- 结果决定下一步的具体优化方向（例如：签名验证 → 启用硬件加速；MVCC 验证 → 并行化；LevelDB → 换 BadgerDB / Sled）

**（B）观察 goroutine 增长的稳态**
- 目前 60 秒内 goroutine 增长 25-30 个，若持续跑 10 分钟看是否稳定或继续增长
- 若继续增长 → gRPC handler pool 也在积累 backlog，需扩容或重构

### 7.2 中期实验

**（C）验证 "每 tx CPU 成本 × 副本数 = 硬顶" 假设**
- **加副本**：Org1 扩到 6 个 peer（虚拟机上再启 2 个），看 commit 上限是否**下降**（若下降，验证副本成本模型）
- **减副本**：只跑 2 个 peer per org，看上限是否**上升**（预期上升到 ~2 倍）
- 这一步可以从数量上定量验证 v2 的根因假设

**（D）测量真实 NIC 带宽**
- 目前 `eth0` 报告 speed=-1（virtio），无法判定 481 Mbps 是 48% 还是 5 % 利用率
- 在两台 VM 装 `iperf3` 后跑一次 5 秒基线，明确物理上限

### 7.3 长期方向

**（E）针对 Fabric commit 路径的并行化**
- Fabric 3.x 依然在 channel 级别串行提交，本质上限制了单 channel 的吞吐
- 可探索：多 channel 分片、或直接修改 committer 使 tx 验证在 block 内部并行
- 属论文之外的方向（是 Fabric 上游的改动），但是"结构性"提升的唯一出路

---

## 八、可复现产物

| 类别 | 路径 |
|:-|:-|
| 分析报告（英文完整表格） | `experiments/p49-submitjs-create/resmon/report.md` |
| 分阶段在飞数原始 tx 数据 | `experiments/p49-submitjs-create/tx.jsonl` （30 000 条） |
| 快采样（100 ms） | `experiments/p49-submitjs-create/resmon/host{153,212}/os_fast.jsonl` |
| 慢采样（1 s Prom） | `experiments/p49-submitjs-create/resmon/host{153,212}/prom_slow.jsonl` |
| 工作负载 | `experiments/p49-submitjs-create/workload-create-30k.jsonl` |
| 采集器 | `experiments/scripts/collect_resources.py` |
| 编排脚本 | `experiments/scripts/run_with_resources.sh` |
| 分析器 | `experiments/scripts/resource_analyze.py` |

复现命令：

```bash
./experiments/scripts/run_with_resources.sh \
    experiments/p49-submitjs-create -- \
    python3 experiments/scripts/run_submitjs_pertx.py \
        --workload experiments/p49-submitjs-create/workload-create-30k.jsonl \
        --num-submitters 15 --target-tps 1500 --max-concurrency 1000 \
        --commit-wait 60 \
        --out experiments/p49-submitjs-create
```

---

## 九、总结

- **上限阶段已经确定**：peer 侧 commit 阶段（1113 条积压 vs endorse 83 条、broadcast 19 条）。
- **上限物理资源已经确定**：peer 主机 CPU（p95=100 %）。
- **v1 的"分发不均"根因假设已经被 v2 的均衡分发实验证伪**。
- **v2 的新根因假设**：Fabric peer 侧每 tx 有约 20 % 系统 CPU 的固定 commit 成本，4 副本平摊即打满整机。可通过"改变副本数看上限是否反向变化"这一实验定量验证。
- **下一步最短路径**：peer0 上做一次 pprof CPU profile，定位 commit 阶段的 CPU 具体消耗在哪一个函数上。
