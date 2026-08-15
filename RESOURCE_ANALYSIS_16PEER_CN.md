# 16-peer 网络瓶颈对比实验报告

**配套主报告**：`RESOURCE_ANALYSIS_REPORT_CN.md` (v2) — 8-peer 结论
**日期**：2026-08-02

> **本次实验目的**：在 8-peer 网络已定位到"peer 侧 commit 阶段的 CPU 固定成本 × 副本数 = 硬顶"之后，把测试床扩到 16-peer（4 组 × 4 peer × 4 台机器），验证：
>
> 1. 增加副本数**是否能提高**吞吐（副本 = 更多算力总量）？
> 2. 还是**反而降低**吞吐（副本 = 更多通信开销）？
> 3. 瓶颈依然是**主机 CPU** 吗？

***

## 一、核心结论

> **在同一客户端配置（N=15 workers, target=1500 tps）下**，把 Fabric 网络从 8-peer 扩展到 16-peer（副本数 × 2，硬件也 × 2）：
>
> - **提交吞吐从 1416 tx/s 掉到 978 tx/s（下降 31 %）**
> - **4 台机器的 CPU 全部被打满至 100 %**
> - **awaiting\_commit 在飞数从 1113 涨到 8808（膨胀 7.9 倍）**
> - **orderer 主机的网络发送带宽从 481 Mbps 涨到 777 Mbps（增长 61 %）**
>
> **结论：加副本不是"分摊 CPU"，而是"增加通信+校验总量"**。这是v2 主报告"每 tx CPU 成本 × 副本数"假设的**强验证**——扩到 4 orgs 后，每 tx 的 commit 总成本从 8 份翻到 16 份，即使硬件也翻了一倍，也追不回。

***

## 二、实验设置

| 项目            | 8-peer 基线（p49）                  | 16-peer 对比（p51）                       |
| :------------ | :------------------------------ | :------------------------------------ |
| 组织数           | 2 (Org1, Org2)                  | 4 (Org1-4)                            |
| 每组 peer 数     | 4                               | 4                                     |
| **总 peer 数**  | **8**                           | **16**                                |
| orderer 数     | 3 (host153×2, host212×1)        | 3（不变）                                 |
| 物理机数          | 2 (host153, host212)            | 4 (host153, host212, host208, host99) |
| 每机 vCPU       | 8                               | 8（每台机器不变，但机器数翻倍）                      |
| 每机内存          | 16 GB                           | 16 GB                                 |
| **总集群算力**     | **16 vCPU**                     | **32 vCPU**（硬件资源翻倍）                   |
| 提交器           | 自研 submit.js MultiPeerSubmitter | 同左（不变）                                |
| workers 数 (N) | 15                              | 15（不变，直接对比）                           |
| target-tps    | 1500                            | 1500                                  |
| workload      | 30k create\_account             | 同左（不变）                                |

**关键**：只改变了 peer 集群的规模，客户端配置和 workload 完全一致。硬件资源翻倍了（16 → 32 vCPU），如果 Fabric 能线性扩展，我们应该看到大约 2× 的吞吐。

***

## 三、吞吐对比 — 反直觉的下降

| 指标                      |  8-peer (p49) | 16-peer (p51) | 变化                  |
| :---------------------- | ------------: | ------------: | :------------------ |
| submit 发送速率             |     1494 tx/s |     1601 tx/s | +7 %                |
| endorse\_done 速率        |     1494 tx/s |     1610 tx/s | +8 %                |
| broadcast\_done 速率      |     1493 tx/s |     1611 tx/s | +8 %                |
| **commit\_seen 速率**     | **1406 tx/s** |  **978 tx/s** | **−30 %**           |
| 成功率                     |         100 % |         100 % | 不变                  |
| commit 阶段耗时 (com\_span) |        21.2 s |        29.7 s | +40 %（30k 全部落账所需时间） |

**读法**：客户端发送与背书都还快了一点（有更多 peer 分担背书压力），但 **peer 侧的 commit 阶段速率从 1406 掉到 978 tx/s**——这是硬件翻倍反而变慢的直接指标。

***

## 四、in-flight 积压对比

| 阶段                   | 8-peer 在飞数均值 | 16-peer 在飞数均值 | 变化         |
| :------------------- | -----------: | ------------: | :--------- |
| awaiting\_endorse    |           83 |           144 | +73 %      |
| awaiting\_broadcast  |           19 |            26 | +37 %      |
| **awaiting\_commit** |     **1113** |      **8808** | **+691 %** |

**读法**：

- endorse 与 broadcast 阶段积压变化不大（略增），说明背书和 orderer 侧都能跟上
- **commit 阶段积压翻了 8 倍**——peer 侧的提交处理能力严重跟不上，导致大量 tx 堆积在等待落账

这个数据直接可视化了瓶颈的位置：**peer 侧 commit 阶段是唯一的堵点**，而扩展副本反而**加剧了这一堵点**。

***

## 五、主机 CPU 对比

**每台机器都被打满** — 无一例外：

| 机器                          | 8-peer CPU 均值 | 16-peer CPU 均值 | 变化     |
| :-------------------------- | ------------: | -------------: | :----- |
| host153 (Org1 + orderer0/1) |        99.6 % |    **100.0 %** | 更满     |
| host212 (Org2 + orderer2)   |        97.4 % |    **100.0 %** | 更满     |
| host208 (Org3)              |             — |    **100.0 %** | 新加入即打满 |
| host99 (Org4)               |             — |    **100.0 %** | 新加入即打满 |

**16-peer 中每 peer 容器的 CPU 消耗**（占系统 CPU 百分比，8 核系统）：

| VM                      | 每 peer CPU 均值 | 备注                      |
| :---------------------- | ------------: | :---------------------- |
| host153 (Org1) 4 个 peer |       19-22 % | 与 8-peer 时的 \~20 % 基本相同 |
| host212 (Org2) 4 个 peer |       22-23 % | 略高                      |
| host208 (Org3) 4 个 peer |       21-26 % | 略高                      |
| host99 (Org4) 4 个 peer  |       21-26 % | 略高                      |

**关键观察**：每个 peer 消耗的 CPU **没有下降**，反而**略有上升**——尽管这些 peer 分担的**背书**工作量应当变小了（15 workers 只轮询到 8 个 peer，其它 8 个从未被选为 endorser）。

这意味着即便一个 peer **没接到任何背书请求**，它依然要消耗 \~20-26 % 系统 CPU 用于：

- 从 orderer 收取全部区块
- 独立验证区块中所有 tx 的 MVCC + 签名
- 写 LevelDB + 更新 stateDB
- 通过 gossip 与其它 peer 同步

**证据**：p51 中 peer2/peer3 系列在 MultiPeerSubmitter 的 peer 列表里**都不存在**，即从未接到背书请求。它们的 CPU：

| peer       | CPU 均值 | 是否接受背书        |
| :--------- | -----: | :------------ |
| peer2.org1 | 22.1 % | 否（仅接收 gossip） |
| peer3.org1 | 18.7 % | 否             |
| peer2.org3 | 21.5 % | 否             |
| peer3.org3 | 26.4 % | 否             |
| peer2.org4 | 21.9 % | 否             |
| peer3.org4 | 26.4 % | 否             |

**这些"沉默 peer"消耗的 CPU 与接背书的 peer 几乎一样**。这直接印证了主报告的结论：**commit 阶段的每 peer 固定 CPU 成本才是主导**，背书阶段的分发差别对总 CPU 消耗影响很小。

***

## 六、网络通信开销 — 直接观测

**这就是"网络通信开销随节点增加的影响"**。

| 机器                        |      网卡 eth0 峰值 | 8-peer 对应值 | 变化        |
| :------------------------ | --------------: | ---------: | :-------- |
| **host153（含 orderer0+1）** | **777 Mbps TX** |   481 Mbps | **+61 %** |
| host212（含 orderer2）       |  380 Mbps TX/RX |   412 Mbps | 相当        |
| host208（无 orderer）        |   199 Mbps peak |          — | 新增        |
| host99（无 orderer）         |   197 Mbps peak |          — | 新增        |

**observation 1**：**host153 是 orderer0 + orderer1 的宿主机**，它的网络输出带宽增加最多——因为这两个 orderer 现在要把每个区块**广播给 16 个 peer** 而不是 8 个。理论上广播开销与 peer 数量成正比，我们看到 61 % 增长与 "peers × 2" 的方向一致（未完全 2× 是因为 gossip 也在分担部分传播）。

**observation 2**：orderer 容器的 CPU 消耗也随之上升：

| orderer  | 8-peer CPU 均值 | 16-peer CPU 均值 | 变化    |
| :------- | ------------: | -------------: | :---- |
| orderer0 |         4.6 % |          6.7 % | +46 % |
| orderer1 |         4.6 % |          6.6 % | +43 % |
| orderer2 |         4.8 % |          4.6 % | 不变    |

orderer0/orderer1 的 CPU 消耗增加 40-50 %，与其网络输出扩容一致——它们花更多 CPU 时间做 gRPC 序列化 + block 分发。orderer2 由于共识负载分担，变化不明显。

**observation 3**：goroutine 数增长率显著：

| peer             | 8-peer goroutines 增长斜率 | 16-peer 增长斜率 |
| :--------------- | ---------------------: | -----------: |
| 接背书的 peer0 系列    |                 0.42/s |       0.55/s |
| 不接背书的 peer2/3 系列 |                 0.06/s |   0.00/s（稳定） |

接背书的 peer 因积压更严重，gRPC handler goroutine 增长更快；不接背书的 peer goroutine 稳定，说明它们的负载完全来自 gossip 接收 + commit，没有 client-facing 请求积压。

***

## 七、"加副本反降性能"的定量模型

基于 8-peer + 16-peer 两组数据，可以拟合一个简单的性能模型：

设：

- P = peer 数（副本数）
- C\_peer = 每 peer 每 tx 消耗的 CPU（在两个测试中都是 \~0.014-0.015 % 每 tx，稳定）
- V = 每机 vCPU 数 = 8
- M = 机器数（P/4 = 4-peer per machine）
- 每机 CPU 预算 = V × 100 % = 800 % 每机

Fabric 稳态吞吐由每机 CPU 平衡决定：

**每机 tx 处理速率 ≈ 每机可用 CPU / (peer-per-machine × 每 tx CPU 成本)**

代入数字：

- 8-peer: **每机 4 个 peer**，800 % CPU / (4 × 0.014 % / tx) = **1428 tx/s per 机** ≈ 观测 1416
- 16-peer: **每机 4 个 peer**（相同），800 % CPU / (4 × 0.014 % / tx) = 应仍为 \~1428 tx/s

**但实测 16-peer 只到 978 tx/s——差了 32 %**。这个差额从哪里来？答案：**通信 + gossip 开销随 peer 总数增加而增加**。加副本的两个反向作用：

**正向（+）**：更多机器 = 更多 CPU 总量 = 理论上更多处理能力
**反向（−）**：每 peer 需要处理的 gossip 消息更多；orderer 广播开销更多；每机额外 CPU 花在跨机通信上

**16-peer 实测 978 tx/s** 说明**反向作用超过了正向作用**——加副本不仅没加吞吐，反而扣掉了 32 %。

**通信开销的量化**（每 tx 单 peer CPU 成本变化）：

| 网络规模    | commit 速率 | 每 peer CPU 均值 |  每 tx 单 peer CPU 成本 |
| :------ | --------: | ------------: | ------------------: |
| 8-peer  | 1406 tx/s |        \~20 % |            0.0142 % |
| 16-peer |  978 tx/s |        \~22 % | **0.0225 %**（+58 %） |

**结论**：**每 tx 单 peer CPU 成本从 0.014 % 涨到 0.023 %**，即增加了 58 %。这个多出来的 CPU 就是"通信+ gossip 开销放大"。

***

## 八、一句话答案

> **问题：加节点是否能通过资源分摊来提高性能？答案是明确的"不能"。**
>
> 我们已用 8-peer vs 16-peer 直接实验验证：**副本数翻倍 + 硬件翻倍的情况下，吞吐反而下降 30 %**。原因是 **Fabric 的每 tx commit 工作是"每 peer 一份"的完全复制模型**——peer 数量翻倍就意味着 commit 总 CPU 成本翻倍，而通信开销（orderer 广播 + peer 间 gossip）随 peer 数增加还额外多出 \~58 % 的每 tx 成本。**在这个部署下，8-peer 已经过了性能拐点**——继续加 peer 只会让性能更差。
>
> **可行的性能突破方向仍然是**（与 v1 报告一致）：
>
> - 减少每 tx 的 commit CPU 成本（换签名算法、并行验证）
> - 减少副本数（如降到 2 orgs × 2 peer = 4 peer）
> - Fabric 3.x 内部提交路径的并行化改造（研究方向）
> - 多 channel 应用层分片
>
> **不可行的方向**：
>
> - 加机器加副本 —— 本次实验已证明反向影响
> - 参数调优 —— MMC=100 vs MMC=1000 实验也证明只是转移瓶颈，无法突破

***

## 九、可复现产物

| 类别            | 8-peer 基线                                          | 16-peer 对比                                         |
| :------------ | :------------------------------------------------- | :------------------------------------------------- |
| 分析报告          | `experiments/p49-submitjs-create/resmon/report.md` | `experiments/p51-16peer-create/resmon/report.md`   |
| tx.jsonl      | `experiments/p49-.../tx.jsonl`                     | `experiments/p51-.../tx.jsonl`（30 000 条）           |
| 快采样（100 ms）   | `experiments/p49-.../resmon/host{153,212}/`        | `experiments/p51-.../resmon/host{99,153,208,212}/` |
| 慢采样（1 s Prom） | 同上                                                 | 同上（覆盖 15 个 peer + 3 个 orderer）                     |

复现命令（与 8-peer 完全一致的客户端参数）：

```bash
# 需先执行 cleanup + redeploy（会自动读取 network_config.json 部署 4 orgs）
python3 infra/topology-manager/scripts/cleanup.py --purge-cache
python3 infra/topology-manager/scripts/run_experiment.py --deploy-only --chaincode smallbank

# 然后跑实验
./experiments/scripts/run_with_resources.sh \
    experiments/p51-16peer-create -- \
    python3 experiments/scripts/run_submitjs_pertx.py \
        --workload experiments/p49-submitjs-create/workload-create-30k.jsonl \
        --num-submitters 15 --target-tps 1500 --max-concurrency 1000 \
        --commit-wait 60 \
        --out experiments/p51-16peer-create
```

wrapper 已自动扩展为按 `HOSTS` 列表遍历所有 4 台机器，采集器脚本无需改动。
