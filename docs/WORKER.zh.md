[← 返回首页](README.zh.md)

# Worker 模式

Worker 模式是一种可选部署拓扑：一个专用节点聚合所有应用实例的访问报告，通过滑动窗口 + 状态机管道执行集群维度的热点检测，并将 HOT/COOL 决策广播回每个实例。

此方案解决了**单实例盲区问题**——应用本地的 HeavyKeeper 无法区分"被同一个 pod 访问 100 次"与"被 100 个 pod 各访问 1 次"。Worker 模式提供集群共识的热点检测，无需中心代理。

## 架构

```
┌────────────────────┐      RabbitMQ fanout      ┌──────────────────────┐
│  App 实例 1        │  ─── 报告（定时）────────→ │                      │
│  HotKeyReporter    │                           │   Worker 节点        │
├────────────────────┤                           │                      │
│  App 实例 2        │  ─── 报告（定时）────────→ │  ┌────────────────┐  │
│  HotKeyReporter    │                           │  │ ReportConsumer │  │
├────────────────────┤                           │  │ (AMQP 消费者)  │  │
│  App 实例 N        │                           │  └───────┬────────┘  │
│  HotKeyReporter    │  ─── 报告（定时）────────→│           │           │
└────────────────────┘                           │          ↓           │
                                                 │  ┌────────────────┐  │
                                                 │  │HotKeyStateMach │  │
                                                 │  │ (按 key 的 FSM)│  │
                                                 │  └───────┬────────┘  │
                                                 │          │           │
                                                 │  ┌────────────────┐  │
                                                 │  │WorkerBroadcaste│  │
                                                 │  │ (HOT/COOL 通过 │  │
                                                 │  │  RabbitMQ)     │  │
                                                 │  └────────┬───────┘  │
                                                 └───────────┼──────────┘
                                                             │
                         RabbitMQ (hotkey.broadcast.exchange)
                                                             │
                                                             ↓
                          ┌──────────────────────────────────────────┐
                          │     所有 App 实例 (WorkerListener)       │
                          │  ┌──────────┐  ┌──────────┐  ┌────────┐  │
                          │  │ 实例 1   │  │ 实例 2   │  │  ...   │  │
                          │  └──────────┘  └──────────┘  └────────┘  │
                          └──────────────────────────────────────────┘
```

## 报告流程

1. 每次 `get()` / `getWithSoftExpire()` 调用触发 `hotKeyReporter.record(key)`，包括 L1 命中或 L2 读取路径
2. `HotKeyReporter` 在本地累计 per-key 计数（Caffeine 30s 过期，最大 100k key），周期性发布 `ReportMessage` 到 `hotkey.report.exchange` RabbitMQ 交换机，按 `app-name` 和 `shard-index` 路由
3. Worker 节点的 `ReportConsumer` 接收报告，将计数投喂到 `workerTopK.add()`，丢弃超过 5s 的陈旧报告
4. Worker 异步处理报告——不阻塞 App 实例

### 分片

`shard-count > 1` 时，报告分布到多个 Worker 实例。每个 App 实例计算 `abs(hash(cacheKey)) % shard-count` 并发布到对应分片路由键。每个 Worker 绑定自己的队列（`hotkey.report.{appName}.{shardIndex}`），支持 Worker 层水平扩展。

## 滑动窗口检测器

`SlidingWindowDetector` 是一个无锁时间序列计数器，在可配置窗口内跟踪每个 key 的访问计数。

| 属性                                               | 默认值 | 说明                             |
| -------------------------------------------------- | ------ | -------------------------------- |
| `hotkey.worker.sliding-window.duration-ms`         | `1000` | 窗口总时长（1 秒）               |
| `hotkey.worker.sliding-window.slices`              | `10`   | 每个窗口的时间片数（每片 100ms） |

每个 key 维护一个按时间片索引的 `long` 计数器数组。每次报告滴答时，回收最旧的时间片，重新计算各 key 的有效计数（所有时间片之和）。这提供了准确的 QPS 估算，无需逐次访问加锁。

## 热点状态机

每个被跟踪的 key 由 `HotKeyStateMachine` 管理其生命周期：

```
          ┌─────────────────────────────────────────────────┐
          │                                                 │
          ↓                                                 │
     ┌─────────┐     超过阈值         ┌──────┐               │
     │ NORMAL  │ ──────────────────→  │ HOT  │              │
     └─────────┘                      └──┬───┘              │
          ↑                              │                  │
          │        低于阈值               │                  │
          │     ┌────────────────────────┘                  │
          │     ↓                                           │
          │  ┌──────────┐   宽限期结束     ┌──────────┐      │
          │  │ PRE_COOL │ ──────────────→ │   COOL   │      │
          │  └──────────┘                 └──────────┘      │
          │         ↑                                       │
          │         | (宽限期内重新升温，静默返回 HOT)        │
          └─────────┴───────────────────────────────────────┘
```

- **NORMAL**：key 存在但低于热阈值。跟踪访问但不发送广播。
- **HOT**：key 超过阈值持续 `confirm-duration-ms`。Worker 向所有实例广播 `TYPE_HOT`。保持 HOT 直到持续冷却。
- **PRE_COOL**：key 降至阈值以下，进入宽限期。期间若重新升温，静默返回 HOT 状态，避免广播抖动。
- **COOL**：key 低于阈值持续 `cool-duration-ms`。Worker 广播 `TYPE_COOL`，key 返回 NORMAL。

### 状态机配置

| 属性                                                      | 默认值  | 说明                                |
| --------------------------------------------------------- | ------- | ----------------------------------- |
| `hotkey.worker.threshold.hot-threshold`                   | `1000`  | 绝对热阈值（设为 -1 启用比率模式）  |
| `hotkey.worker.threshold.hot-threshold-ratio`             | `0.01`  | 热阈值占估计全局 QPS 的比例         |
| `hotkey.worker.state-machine.confirm-duration-ms`         | `2000`  | 高于阈值确认 HOT 的持续时间（2s）   |
| `hotkey.worker.state-machine.cool-duration-ms`            | `15000` | 低于阈值确认 COOL 的持续时间（15s） |
| `hotkey.worker.state-machine.pre-cool-grace-ms`           | `5000`  | 静默重新升温的宽限期（5s）          |

## 动态阈值（全局 QPS）

Worker 根据流量模式自适应调整热阈值：

```
hotThreshold = max(minCount, estimatedGlobalQPS * hotThresholdRatio)
```

| 属性                                                                       | 默认值    | 说明                                    |
| -------------------------------------------------------------------------- | --------- | --------------------------------------- |
| `hotkey.worker.global-qps-dynamic-threshold.recalculate-interval-ms`       | `60000`   | 重新计算间隔（60s）                     |
| `hotkey.worker.global-qps-dynamic-threshold.qps-change-tolerance`          | `0.5`     | 触发阈值更新所需的 QPS 变化幅度（±50%） |
| `hotkey.worker.global-qps-dynamic-threshold.learning-period-ms`            | `30000`   | QPS 估算的学习周期                      |
| `hotkey.worker.global-qps-dynamic-threshold.hot-threshold-ratio`           | `0.01`    | 热阈值占估计全局 QPS 的比例             |

`qps-change-tolerance` 防止正常流量波动导致阈值抖动——仅显著 QPS 变化触发重新计算。

## Top-K 交叉验证

Worker 定期将自身集群维度 Top-K 与应用端 HeavyKeeper Top-K 进行交叉验证，确保一致性并支持预预热。

| 属性                                                          | 默认值  | 说明                          |
| ------------------------------------------------------------- | ------- | ----------------------------- |
| `hotkey.worker.topk-validation.validate-interval-ms`          | `60000` | 交叉验证间隔（60s）           |
| `hotkey.worker.topk-validation.pre-warm-count`                | `5`     | 符合预预热条件的 Top-K 条目数 |
| `hotkey.worker.topk-validation.pre-warm-min-appearances`      | `2`     | 预预热所需的最小连续出现次数  |

连续多个验证周期出现在 Worker Top-K 中的条目将成为预预热候选。Worker 可主动将这些 key 推送给 App 实例，早于本地自然检测。

## 部署模式

两种部署模式：

| 模式        | `worker.enabled` | 激活的 Bean                                                                            |
| ----------- | ---------------- | -------------------------------------------------------------------------------------- |
| App-only    | `false`（默认）   | `HotKeyCache`、TopK 检测器、reporter、actuator、sync                                   |
| Worker-only | `true`           | 仅 Worker（SlidingWindow、StateMachine、ReportConsumer、Broadcaster）                  |

**Worker-only** 模式下，`HotKey.isLocalHotKey()` / `get()` / `putThrough()` 抛出 `UnsupportedOperationException`——这些操作需要应用端缓存。Worker-TopK 查询（`returnWorkerHotKeys()`）仍可用。

## 配置示例

```yaml
hotkey:
  worker:
    # 必须显式设为 true 才能启用 Worker 模式
    enabled: true

    routing:
      app-name: my-service
      shard-count: 1
      shard-index: 0

    sliding-window:
      duration-ms: 1000
      slices: 10

    threshold:
      hot-threshold-ratio: 0.01

    state-machine:
      confirm-duration-ms: 2000
      cool-duration-ms: 15000
      pre-cool-grace-ms: 5000

    global-qps-dynamic-threshold:
      recalculate-interval-ms: 60000
      qps-change-tolerance: 0.5
      learning-period-ms: 30000
      hot-threshold-ratio: 0.01

    topk-validation:
      validate-interval-ms: 60000
      pre-warm-count: 5
      pre-warm-min-appearances: 2

    heavy-keeper:
      top-k: 100
      width: 20000
      depth: 10
      decay: 0.9
      min-count: 10

spring:
  rabbitmq:
    host: localhost
    port: 5672
```

> [!SECURITY]
> 以上示例使用明文 AMQP 连接。生产环境中应通过 `spring.rabbitmq.ssl.*` 启用 TLS：
>
> ```yaml
> spring:
>   rabbitmq:
>     ssl:
>       enabled: true
>       key-store: classpath:client.p12
>       key-store-password: changeit
>       trust-store: classpath:truststore.jks
>       trust-store-password: changeit
> ```
>
> 详见 [Spring Boot RabbitMQ SSL 文档](https://docs.spring.io/spring-boot/reference/messaging/amqp.html#page-title)。

### Worker Listener（App 实例端配置）

```yaml
hotkey:
  worker-listener:
    enabled: true
```

每个 App 实例需启用 `worker-listener` 以接收 Worker 发出的 HOT/COOL 决策。监听器绑定到队列 `hotkey.worker:{instanceId}` 并处理收到的决策。

### 报告配置（App 实例端）

```yaml
hotkey:
  local:
    app-name: my-service
    report-interval-ms: 100
    shard-count: 1
```

## 故障表现

| 故障            | 影响                                            | 恢复方式                  |
| --------------- | ----------------------------------------------- | ------------------------- |
| Worker 崩溃     | App 实例继续使用本地 TopK；无集群 HOT/COOL 决策 | 重启 Worker；实例自动重连 |
| 报告通道故障    | App 报告排队/缓冲（RabbitMQ 持久化）            | RabbitMQ 恢复后自动恢复   |
| Worker 广播故障 | 无跨实例 HOT/COOL 同步；本地 TopK 仍正常        | 重启 Worker broadcaster   |
