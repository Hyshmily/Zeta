# 常见问题 FAQ

> 关于 HotKey 架构与设计选择的常见疑问。

---

首先声明:绝大多数情况可以通过修改参数得到彻底解决,以下问题仅针对默认参数下的极端边缘场景，且在默认参数设定下已经有合理的保护能力。

## Q1: 为什么既要本地 HeavyKeeper，又要中央 Worker？本地判断还不够吗？

**简答：** 本地检测用于 _自我保存_（纳秒级）；中央 Worker 用于 _集群协调_（百毫秒级）。两者服务于不同的时间尺度与目的。

| 维度         | 本地 HeavyKeeper               | 中央 Worker                          |
| ------------ | ------------------------------ | ------------------------------------ |
| **响应时间** | 纳秒级                         | ~100ms–300ms（上报间隔 + 滑动窗口累积） |
| **视野范围** | 单节点                         | 全集群                               |
| **目的**     | 立刻升级热 Key TTL，保护本节点 | 全局共识、跨实例预热、协调冷却       |
| **何时触发** | 每次 `get()` 调用              | 累积足够窗口数据后                   |

**源码中的协作流程：**

1. **突发流量打到实例 A：** `HotKeyCache.get()` L1 未命中 → `loadAndCache()` 调用 `hotKeyDetector.add(key, 1).isLocalHotKey()` → 若为热 Key，以 `hotHardTtlMs`（1 小时）缓存。实例 A 瞬间被保护。

2. **同时，** `get()` 也会调用 `hotKeyReporter.record(key)` → `HotKeyReporter` 在本地 Caffeine 累加 → 每 `reportIntervalMs`（100ms）批量刷到 RabbitMQ。

3. **Worker 收到上报** → `ReportConsumer.onReport()` 调用 `detector.addCount(key, count)` → `HotKeyStateMachine.evaluate()` 追踪连续热点窗口 → 持续 `confirmDurationMs`（300ms）后，向 **所有实例** 广播 HOT。

4. **实例 B、C、D** 通过 `WorkerListener` 收到 HOT → 从 Redis 加载到本地 Caffeine，流量到达前完成预热。

**没有中央 Worker：** 实例 A 保住了，下一秒流量漂到实例 B，B 缓存为空。这就是"热点漂移"。

---

## Q2: 中央 Worker 上报是不是太慢了？

**简答：** 不。首先得明确worker上报的存在意义,在当 Worker 确认一个热 Key 之前时，本地 HeavyKeeper 早已保护了接到初始流量的节点。Worker 的延迟只影响 _跨实例协调_，而这对持续数分钟到数小时的热 Key 来说微不足道。

**一个真实热 Key（如秒杀商品）的时间线：**

```
t=0ms    Key X 首次在节点 A 被访问
t=+1μs   本地 HeavyKeeper："看起来热" → 以 1h 热 TTL 缓存在节点 A
t=+100ms HotKeyReporter 刷新 → Worker 收到第一批计数，key 超过阈值 → tick 1
t=+300ms   连续 3 个热 tick（共 300ms，confirmCount=3）→ CONFIRMED_HOT
t=+300ms   广播发送到所有节点（同一 tick 内 AMQP 发出）
t=+360ms   节点 B、C、D 收到 HOT → 从 Redis 加载 → 以热 TTL 缓存
(将 confirm-duration-ms 设为 0 配合 report-interval-ms=1 和 sliding-window=100/100 可将全链路延迟降低到约 9ms)
```

**真正的热 Key 会持续热几分钟甚至几小时**，而不是几百毫秒。Worker 的约 300ms 延迟（默认 `confirmDurationMs=300`，每 100ms 评估一次 = 3 个 tick）与总热期相比可以忽略不计。而接到第一波冲击的节点，早在微秒级就已被保护。

**实测延迟**（来自 `PropagationDelayIT`，10 个阶段，4.5 万 ops，0 错误）：

| 场景                     | P50            | P95        | P99         |
| ------------------------ | -------------- | ---------- | ----------- |
| Worker 决策流水线        | **56.38 ms**   | 99.21 ms   | 103.56 ms   |
| SM 确认流水线（3 确认窗）| **246.46 ms**  | 295.00 ms† | 295.00 ms†  |
| 全链路（SM 3 确认）      | **298.19 ms**  | 351.50 ms† | 351.50 ms†  |

> † P95=P99=Max，因为仅 **10 个键**（10 个样本点）。N=10 时 P95=第 9.5 个→第 10 个=Max，P99=第 9.9 个→第 10 个=Max。百分位数等于最大值，非测量异常。

- **Worker 决策流水线**（Phase 7）：Worker 检测到热键后发起 AMQP 广播。P50=56.38ms 含 `warmupJitterMs=100ms` + AMQP + L1 轮询。
- **SM 确认流水线**（Phase 8）：约 246ms 由确认窗口主导（3 × 100ms = 300ms），确认后走相同 AMQP 路径（约 56ms 含 jitter）。参数可调。
- **全链路（SM 3 确认）**（Phase 9）：完整端到端路径——本地 Caffeine 未命中 → 上报聚合（100ms 批次）→ AMQP 上报投递 → SlidingWindowDetector → 3 确认窗状态机 → AMQP 决策广播 → L1 提升。总延迟约 298ms P50，距理论 300ms 底线仅 0.6%。

> **极限参数调优**（PropagationDelayExtremeIT，相同容器，但 report-interval-ms=1、warmup-jitter-ms=0、confirm-duration-ms=0、sliding-window=100/100）大幅压缩延迟：Worker 决策流水线 → **2.41ms P50**，SM 流水线（0 确认）→ **7.71ms P50**，全链路（SM 0 确认）→ **9.23ms P50**（均使用相同的 10 键数）。从默认配置（3 确认窗，298.19ms P50）到极限配置（0 确认窗，9.23ms P50）的降幅达 **96.9%**。权衡讨论详见 [README 极限调优章节](../README.md#极限参数调整)。

**如果是仅仅 100ms 的脉冲呢？** 那不算是集群级别的热 Key——只是一个本地毛刺。本地 HeavyKeeper 用稍长的 TTL 处理它，Worker 完全不需要广播。两者各司其职。

`SingleFlight.java` 提供了额外保障：同一 Key 的并发 L1 未命中共享一个 `CompletableFuture`，在本地 HeavyKeeper 激活前的短暂窗口内防止对后端造成雪崩。

---

## Q3: 如果本地 HeavyKeeper 已经预热了缓存，中央 Worker 再广播 HOT 还有什么意义？

**简答：** 本地预热保护 _一个节点_。中央广播在流量到达之前保护 _所有节点_。

**本地预热无法解决的缺口：**

```
时间 ──────────────────────────────────────────────────────────────→
      节点 A 被命中 → 本地 HeavyKeeper → 以热 TTL 缓存
                                                      ↓ 节点 A 安全
      负载均衡将流量切换到 → 节点 B ← 无缓存 → 打到 Redis！
```

节点 A 安全了，但节点 B 完全不知道 Key X 是热的。当流量转向 B 时，Caffeine 缓存为空。这就是**热点漂移**问题。

Worker 广播同步预热 **所有节点**：

```
Worker 检测到 Key X 是全局热 Key → 广播 HOT
  ├─ 节点 A：已经热了（更新 decisionVersion，延长 TTL）
  ├─ 节点 B：收到 HOT → 从 Redis 加载 → 以热 TTL 缓存 → 准备就绪
  ├─ 节点 C：同样
  └─ 节点 D：同样
```

**除了预热，中央 Worker 还提供：**

- **统一冷却：** 只有 Worker 能决定一个 Key 何时不再是热 Key。没有它，各节点会各自冷却，导致缓存状态不一致。
- **决策版本仲裁：** 每条 HOT/COOL 广播携带单调递增的 `decisionVersion`（`WorkerBroadcaster.java`）。接收端据此丢弃乱序消息，确保每个 Key 的全局有序。

- **TopK 慢热检测：** `TopKValidator`（`TopKValidator.java`）定期扫描全局频率排行榜。逐渐累积高总访问量（但从不脉冲）的 Key 会被预热——这是本地 HeavyKeeper 做不到的。

---

## Q4: RabbitMQ 吞吐有限（~1万–10万 msg/s），上报流量会不会压垮它？

**简答：** 不会。作者通过上报通道中的三项设计来尽可能控制流量：

### 1. 批量聚合大幅减少消息量

`HotKeyReporter`（`HotKeyReporter.java`）不是每次访问都发送一条消息。它在本地 Caffeine 计数器中累积，每 `reportIntervalMs`（默认 100ms）批量刷新。一条 `ReportMessage` 可以携带成百上千个 Key-Count 对。

**公式：** 每秒消息数 = `1000 / reportIntervalMs` = 每个分片 10 msg/s，与访问总量无关。

### 2. 一致性哈希分片分散负载

每个 Key 通过 `Math.abs(key.hashCode()) % shardCount`（`HotKeyReporter.java`）路由到唯一分片。若 `shardCount = N`，上报负载被分到 N 个队列/消费者上。每个 Worker 消费者只处理分配到的 Key。

### 3. 控制通道与数据通道分离

| 通道                      | 交换机                      | 流量模式          | 量级                   |
| ------------------------- | --------------------------- | ----------------- | ---------------------- |
| **上报**（应用 → Worker） | `hotkey.report.exchange`    | 每 100ms 批量发送 | 稳定，与应用数量成比例 |
| **广播**（Worker → 应用） | `hotkey.broadcast.exchange` | HOT/COOL 决策     | 极低——仅状态转换时     |

广播是 **控制信号**，不是数据流。一个 Key 在其生命周期内最多经历 COLD → CONFIRMED_HOT → PRE_COOLING → COLD 的完整状态转换。`HotKeyStateMachine`（`HotKeyStateMachine.java`）的 `confirmCount`/`coolCount` 滞回机制确保不会出现抖动/广播风暴。

最后是状态机的设计，从根本上消除了冗余广播导致的客户端 CPU 过载和自造拥塞。极限测试中，SM 0 确认路径仅产生 10 条广播，而无状态机路径产生了 86 条。

**如果集群大到需要担心 MQ 吞吐：** 增加 `shardCount`，这样会按比例减少每个队列的消息量。每个新增分片都会在 Worker 上增加一个消费者线程。

---

## Q5: TopKValidator 基于历史排行预热 Key——这个功能实际用得多吗？

**简答：** 它是滑动窗口无法覆盖的极少数情况的安全网。该功能使用频率较低，作者把他作为补充检测手段而非主要路径。

**它捕捉什么：** 有些 Key 从不"脉冲"，但会在数分钟到数小时内逐渐累积高总流量——例如慢慢吸引更多访客的活动落地页，或被许多用户以稳定速率查询的仪表盘组件。

这些 Key **永远无法**触发滑动窗口阈值（默认 1s 窗口内 1000 QPS），但它们每日的总访问量排在全局 TopK 里。`TopKValidator.validate()`（`TopKValidator.java`）每 `validateIntervalMs`（默认 60s）运行一次，检查：

1. 这个 Key 是否稳定出现在全局 TopK 中？
2. 它是否连续 `preWarmMinAppearances`（默认 2）次验证周期都在 TopK 里？
3. 如果是 → 广播 HOT → Key 获得完整热 Key 保护。

**为什么它不是主要检测路径：**

- 滑动窗口覆盖了 99%+ 的真实热 Key（脉冲流量模式）。
- TopK 预热为慢热 Key 增加了 1–3 分钟的延迟才能被提升。
- 没有它，这个 Key 可能永远无法被提升，无限期依赖短 TTL——这是滑动窗口无法填补的正确性缺口。

**可配置开关：** 整个机制由 `hotkey.worker.topk-validation.*` 属性控制。设置 `validate-interval-ms: 0` 即可完全禁用。

---

## Q6: 是否存在一种极端情况：一个全新的 Key 在所有实例上同时爆发，快到 Worker 根本来不及广播？

**简答：** 理论上存在这种可能性（即便在全链路延迟压缩至~9.23ms的极限配置下也是如此）。但是作者在框架设计上尽量减少此类风险，但无法保证在极端边缘场景下绝对可靠。

**默认配置下的实际过程：**

1. **t=0：** Key X，从未被访问过，在全部 10 个节点上同时遭遇并发请求。

2. **t=+1μs：** 每个节点的 `SingleFlight`（`SingleFlight.java`）激活。压力测试中 `singleFlight_extremeDedup` 实现 **99.0% 去重**（100 线程 → 1 次执行），`singleFlight_cacheStampede` 实现 **91.5% 去重**（50 key × 20 线程 → 85 次执行）。后端最多收到 ~N 个并发请求（每节点一个），而非 `N × 并发数`。

3. **t+~5ms 至 t+~20ms：** 各节点首个请求完成（取决于 L2 延迟）。数据返回给所有等待调用方。值以 **普通 TTL**（默认 5min，详见 `HotKeyProperties.java:87`）缓存在 L1——本地 HeavyKeeper 尚未触发。

4. **t+100ms：** `HotKeyReporter` 刷新第一批计数到 Worker（`reportIntervalMs=100`，详见 `HotKeyProperties.java:148`）。`reporter_highFrequency` 压力测试确认 **3M ops/s 吞吐，零数据丢失**。

5. **t+~298ms：** 全链路完成——连续 3 个热评估 tick（confirmCount=3 × 100ms/tick = 300ms）后状态机转为 CONFIRMED_HOT 并广播。所有节点收到 HOT（传播延迟基准实测 P50=56.38ms，P95=99.21ms，P99=103.56ms）→ 升级 Key 为热 TTL（1h，详见 `HotKeyProperties.java:91`）并开启软过期。实测全链路（SM 3 确认窗）P50=298.19ms。状态机保证了每个 Key 在整个生命周期内**只广播一次**——极限测试中，SM 0 确认路径仅产生 10 条广播，而无状态机路径产生了 86 条，SM 的广播压制能力显著降低了系统负载。这是框架作者最引以为傲的设计之一。

Worker 广播解决的是 _第二波及后续_ 请求——确保当流量持续时（真实热 Key 就是这样），所有节点都处于最优的热 Key 配置状态。默认的 300ms 确认窗口是框架主动选择的稳定性保障，在这 300ms 内，本地 HeavyKeeper、SingleFlight 和普通 TTL 已构成三层保护，用户请求没有经历任何阻塞。

对于追求极致速度的场景，可以通过修改特定参数将全链路延迟压缩至 ~9.23ms，但这需要牺牲确认窗口的保护能力和状态机带来的广播压缩优势。该极端场景在绝大多数实际业务中极为罕见，通常需要超大规模流量突发才能触发。
