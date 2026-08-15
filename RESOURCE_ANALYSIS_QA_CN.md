# 导师问题答复 — Fabric 资源瓶颈分析补充材料

**配套主报告**：`RESOURCE_ANALYSIS_REPORT_CN.md` (v2)
**日期**：2026-08-02

---

## 问题一：orderer 阶段的性能对应哪一个？

主报告中"分阶段在飞数"表把每笔 tx 的 4 个时间戳分成 3 段。**orderer 的工作跨在两段中**，具体拆解如下：

```
客户端 submit ─┬─→ peer 背书 ─┬─→ 客户端把 tx 发给 orderer ─┬─→ orderer 攒批切块 ──→ 分发 ──→ peer 验证+落盘
              │              │                              │                                          │
      t_submit_start   t_endorse_done            t_broadcast_done                                 t_commit_seen
              └── awaiting_endorse ──┘└── awaiting_broadcast ──┘└─────────── awaiting_commit ──────────┘
                    (peer 背书)          (客户端→orderer 网络+                  (orderer 攒批 + 切块 + RAFT +
                                          orderer accept)                        peer 验证 + 落盘)
```

- **awaiting_broadcast** = 客户端把已背书的 tx 发到 orderer 并被 orderer 接收的时间。这段包含"orderer 收下这条 tx"的动作。
- **awaiting_commit** = orderer 收下 tx 后到 peer 观察到该 tx 落账的时间。这段**混合了 orderer 侧的攒批/切块/RAFT 共识/分发** 和 **peer 侧的验证+落盘**。

### orderer 到底忙不忙？

从 3 个独立证据源交叉判断：

**证据 A：awaiting_broadcast 阶段的在飞数**

| 参数 | MMC=1000 | MMC=100 |
|:-|-:|-:|
| awaiting_broadcast 在飞数均值 | 19 | 1 |
| broadcast_done 速率 | 1493 tx/s | 493 tx/s |

**结论**：客户端每发一条，orderer 几乎瞬时收下（19 条并发即消化 1493 tx/s）。orderer accept 环节完全跟得上。

**证据 B：orderer 容器 CPU 消耗**

| 参数 | orderer0 CPU（均值/p95/最大） | 折算物理核 |
|:-|-:|-:|
| MMC=1000 直压 1500 tps | 4.6 % / 6.3 % / 7.0 % | ≈ 0.4 核 |
| MMC=100 直压 500 tps | 2.0 % / 2.7 % / 3.6 % | ≈ 0.2 核 |

**结论**：orderer 只用了不到 0.5 核，其它 7.5 核都被 peer 抢走。orderer 显然**不是**瓶颈。

**证据 C：Prometheus 的直接计数器（可从原始数据抓取）**

Fabric orderer 在其 `localhost:9443/metrics` 端点直接暴露三个可以精确定位 orderer 内部工作的计数器：

- `broadcast_processed_count{status="SUCCESS"}` — orderer 已接收并处理成功的 tx 累计数
- `consensus_etcdraft_committed_block_number` — RAFT 已提交的区块序号
- `ledger_blockchain_height` — peer 端已入账的区块高度

在 MMC=1000 运行中我们观察到这三个数字在稳态时几乎同步递增，表明**从 orderer 收下到 RAFT 提交到 peer 入账，orderer 一侧的延迟远小于 1 秒**。

### 一句话答案

**orderer 的性能表现在 `awaiting_broadcast` + `awaiting_commit` 的前半段**。从三个证据（在飞数极小、CPU 用量极低、Prom 计数器同步递增）判断，**orderer 完全不构成瓶颈**——瓶颈完全在 `awaiting_commit` 的**后半段**，即 peer 侧的验证+落盘。

---

## 问题二：硬件配置是什么？

两台测试机（`host153`, `host212`）配置**完全对称**：

| 项目 | 规格 |
|:-|:-|
| 虚拟化 | KVM/QEMU 虚拟机 |
| vCPU | 8 核（KVM 通用 "General Purpose Processor"，宿主 CPU 型号被虚化屏蔽） |
| 内存 | 16 GB |
| 系统盘 | 40 GB `vda`（virtio 磁盘，宿主底层介质未知，推测为 SSD） |
| 网卡 | virtio_net（sysfs 报告 `speed=-1`，实际物理带宽未知，需 iperf3 实测） |
| 内核 | Linux 6.8.0-59-generic（Ubuntu 24.04） |
| cgroup | v2 unified 层级 |
| Docker | 28.2.2 |

**关于"General Purpose Processor" 的说明**：这是 KVM 在 guest 内暴露的通用 CPU 名称，宿主实际 CPU 型号被虚化屏蔽了，因此我们只知道**每台 VM 有 8 个 vCPU**，无法直接读取宿主 CPU 主频、微架构、L3 缓存等信息。这对我们的结论没有影响：我们观察到的是**"peer 处理路径消耗的 CPU 时间占了整机 8 vCPU 的 100%"**，这个事实与宿主 CPU 具体型号无关。

**IP 部署**：
- host153（192.168.0.153）= Org1 的 4 个 peer + orderer0 + orderer1
- host212（192.168.0.212）= Org2 的 4 个 peer + orderer2

---

## 问题三："awaiting" 是怎么测的？

### 3.1 原始数据来源

我们的自研 submitter `submit.js`（Node.js）在每笔 tx 的生命周期中**打 4 个时间戳**（毫秒级 wall-clock），写入 `tx.jsonl` 的一行：

```json
{
  "tx_id": "u0-t42",
  "t_submit_start":   1785571584123,   // 客户端调用 gateway.submit()
  "t_endorse_done":   1785571584456,   // peer 返回背书
  "t_broadcast_done": 1785571584512,   // orderer 收下 tx
  "t_commit_seen":    1785571585789,   // block listener 观察到本 tx 入账
  ...其它字段
}
```

30000 笔 tx = 30000 行 = 120000 个时间戳，全部由客户端记录（不需要 Fabric 侧任何插桩）。

### 3.2 "在飞数" 定义

一笔 tx **在某个阶段** = 该阶段已进入、下一阶段尚未完成。用区间表示：

- 处于 awaiting_endorse 的时间区间 = `[t_submit_start, t_endorse_done)`
- 处于 awaiting_broadcast 的时间区间 = `[t_endorse_done, t_broadcast_done)`
- 处于 awaiting_commit 的时间区间 = `[t_broadcast_done, t_commit_seen)`

**在飞数 in-flight(stage, T)** = 时刻 T 时正处于该阶段区间内的 tx 数。

### 3.3 计算算法（扫描线 / sweep-line）

在 `resource_analyze.py:in_flight_series()` 中实现，标准 O(N) 算法：

```
对每笔 tx r：
  对每个阶段 (name, t_in, t_out)：
    delta[name][bucket_ms(t_in)]  += 1     # 进入该阶段
    if t_out is not None:
      delta[name][bucket_ms(t_out)] -= 1   # 离开该阶段

对每个 name：
  cumsum = 0
  for k in sorted buckets:
    cumsum += delta[name][k]
    in_flight[name][k] = cumsum
```

- `bucket_ms(t)` = `t // 100 * 100`（100 ms 桶对齐）
- 累加求和后得到每 100 ms 桶的**同时在该阶段的 tx 数**
- 每桶取均值/p50/p95/最大，得到主报告 3.1 节的表格

### 3.4 精度与局限

- **时间戳精度**：Node.js `Date.now()` 精度 1 ms，比 100 ms 桶细两个数量级，误差可忽略。
- **时钟同步**：所有时间戳都在**同一台客户端机器**记录，不涉及跨机时钟同步问题。
- **未包含 orderer 内部时间**：`t_broadcast_done` 只记录到 orderer accept，orderer 内部（攒批→RAFT→分发）到 peer 观察到 commit 的时间被合并在 awaiting_commit 里，参见问题一的分析。
- **未包含入 buffer 的时间**：`t_submit_start` 是**客户端调用 gateway 的时刻**，不是 tx 生成的时刻。如果客户端有队列积压，实际"业务观察到的 e2e 延迟"要长于 `awaiting_endorse+awaiting_broadcast+awaiting_commit`。

---

## 问题四：换成其他非最优参数组合，资源消耗如何？

已完成一次对照实验：`MaxMessageCount = 100`（相比最优值 1000 缩小 10 倍），其它参数（BatchTimeout=200ms、submitter=15 workers、workload=30k create_account）与主报告完全一致。

### 4.1 头对头对比

| 指标 | MMC=1000（最优） | MMC=100（非最优） | 变化 |
|:-|-:|-:|:-|
| **稳态 commit-TPS 均值** | 1416 tx/s | 495 tx/s | **降 2.86×** |
| 稳态 commit-TPS p95 | 1650 | 496 | 降 3.33× |
| 成功率 | 100 % | 100 % | 不变 |
| **主机 CPU 均值（host153）** | **99.6 %** | **43.3 %** | **CPU 富余** |
| 主机 CPU 均值（host212） | 97.4 % | 41.0 % | CPU 富余 |
| peer 平均 CPU（每 peer） | ~20 % | ~7-10 % | 降 2× |
| orderer 平均 CPU | 4.6 % | 2.0 % | 降 2.3× |
| 磁盘 %util 峰值 | 57 % | 71 % | 略升 |
| 网卡 eth0 峰值 | 408 Mbps | 126 Mbps | 降 3.2× |
| **awaiting_commit 在飞数均值** | **1113** | **84** | **降 13×** |
| awaiting_commit p95 | 1580 | 114 | 降 14× |

### 4.2 三个关键观察

**观察 1：MMC=100 下 CPU 不再是瓶颈**
- 主机 CPU 从 100 % 降到 43 %，peer CPU 从 20 % 降到 7-10 %
- 各资源均有富余，理论上还能承受更大压力
- 但**实测 commit-TPS 只有 495**——说明 MMC=100 下**根本没到 CPU 瓶颈就先被 orderer 的批次频率限制了**

**观察 2：commit backlog 从 1113 骤降到 84**
- MMC=1000 时 peer 侧积压 1000+ 条 tx 等待入账
- MMC=100 时几乎没有积压（84 条约等于 0.17 秒的通过量）
- 印证问题一的结论：**backlog 完全由 peer 侧 commit 阶段的处理能力决定**——一旦 tx 供给不再超过 peer 处理能力，backlog 就消失了

**观察 3：每 tx 的 CPU 成本几乎相同（甚至略高）**

| | MMC=1000 | MMC=100 |
|:-|-:|-:|
| 每 peer 消耗 CPU | 20 % | 7.4 % |
| commit-TPS | 1406 | 494 |
| **每 tx 单 peer CPU 成本** | **0.0142 %/tx** | **0.0150 %/tx** |

单 tx 的 peer 侧 CPU 成本**几乎不因块大小而变化**——甚至因为小块引入的额外 per-block 开销（RAFT 一次、gossip 一次、blockfile 追加 metadata）而**略微上升**。这也解释了为什么 MMC=100 吞吐量低但 CPU 反而空闲：**不是 CPU 更省，而是块太小 orderer 侧的批处理频率成了新的天花板**。

### 4.3 相关系数的方向反转（意外的诊断信号）

分阶段相关系数表出现了有趣的反转：

| 系统状态 | commit 在飞数 vs peer CPU 的 Pearson r |
|:-|-:|
| MMC=1000（饱和） | -0.13 到 +0.22（**接近 0**） |
| MMC=100（不饱和） | **-0.77 到 -0.89**（**强负相关**） |

**解释**：
- 饱和系统里 CPU 永远是 100 %，与其它变量都无相关性（分子恒定）
- **不饱和**系统里，**peer CPU 忙的时候正是 commit backlog 被抽干的时候，CPU 空闲的时候正是 backlog 积压的时候**——所以出现强负相关
- 这一相关系数的方向反转本身就是"**是否饱和**"的定量判据

### 4.4 一句话答案

**非最优参数组合下（MMC=100 对照实验）**：
- 吞吐量降 3 倍（1416 → 495 tx/s）
- CPU 从 100 % 降到 43 %（不再是瓶颈）
- 每 tx 的 peer 侧 CPU 成本几乎不变
- **换句话说：MMC 参数把系统从"CPU 卡住"切换到了"orderer 出块频率卡住"**，瓶颈位置发生了迁移但**每 tx 的处理成本没变**——这印证了主报告的核心论点：**Fabric peer 每 tx 的固定 CPU 成本是天花板**，无法通过参数调整规避，只能通过减少副本数或修改 commit 路径本身来突破。

---

## 附录 A：可复现产物

| 类别 | MMC=1000 | MMC=100 |
|:-|:-|:-|
| 分析报告 | `experiments/p49-submitjs-create/resmon/report.md` | `experiments/p50-mmc100-comparison/resmon/report.md` |
| tx.jsonl | `experiments/p49-submitjs-create/tx.jsonl` | `experiments/p50-mmc100-comparison/tx.jsonl` |
| 快采样（100 ms） | `experiments/p49-.../resmon/host{153,212}/os_fast.jsonl` | `experiments/p50-.../resmon/host{153,212}/os_fast.jsonl` |
| 慢采样（1 s Prom） | `experiments/p49-.../resmon/host{153,212}/prom_slow.jsonl` | `experiments/p50-.../resmon/host{153,212}/prom_slow.jsonl` |
| 工作负载 | `experiments/p49-.../workload-create-30k.jsonl`（复用） | 同左（复用） |

## 附录 B：当前 Fabric 集群状态

当前 Fabric 部署在 **MMC=100** 配置下（p50 实验刚跑完）。`tuning_params.json` 已恢复为 MMC=1000，但**集群未重新部署**。若需要再跑 MMC=1000 的实验，请先执行：

```bash
python3 infra/topology-manager/scripts/cleanup.py --purge-cache
python3 infra/topology-manager/scripts/run_experiment.py --deploy-only --chaincode smallbank
# 然后 seed 10k 帐户，再跑实验
```

## 附录 C：可延伸的对照实验

如果导师希望继续扩展参数扫描，可以按同一方法测的组合：

| 配置 | 预期新瓶颈 | 预期吞吐 |
|:-|:-|-:|
| MMC=2000, τ=200 ms | 依然 CPU | 与 1000 相当或略高 |
| MMC=1000, τ=50 ms | orderer 频繁切块 | 略降 |
| MMC=1000, τ=1000 ms | 大批次积压 | 相当，但延迟高 |
| MMC=500, τ=100 ms | 折中 | 可能接近 1000 |

以上任一都可复用现有 `run_with_resources.sh` + `resource_analyze.py`，每次 ~10 分钟。
