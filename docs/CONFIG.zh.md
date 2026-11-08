[← 返回首页](../README.zh.md)

# 配置参考

Zeta 的配置都在 `zeta.*` 下。App 侧主要使用 `zeta.local.*`、`zeta.report.*`、`zeta.sync.*` 与 `zeta.worker-listener.*`；Worker 侧使用 `zeta.worker.*`；`zeta.app-name` 由两端共享。

## 1. 部署

`zeta.app-name` 是唯一的租户键。上报路由、决策的**路由键**（`send.<app-name>`，HOT/COOL 投递；消息头只是给接收端自己做校验）、缓存同步消息与 Worker 心跳都用它区分应用，因此 App 与 Worker 必须配置同一个值。`zeta.worker.enabled` 决定进程是 App 还是 Worker-only；`zeta.worker-listener.enabled` 决定 App 是否消费 Worker 的心跳与 HOT/COOL 决策。`instance-id` 与 `node-id` 则保证多实例部署时队列名和版本戳不冲突。

App 侧：

```yaml
zeta:
  app-name: order
  local:
    instance-id: ${HOSTNAME} # 为空时自动取 ${server.port:instance}-${HOSTNAME}
    node-id: 1 # 多实例各配唯一值；不配则取 instance-id 的 CRC32
  report:
    enabled: true
  worker-listener:
    enabled: true # 部署了 Worker 集群时必须为 true
```

Worker 侧：

```yaml
zeta:
  app-name: order
  worker:
    enabled: true
    epoch-file: /data/zeta/epoch # 必须跨重启保留
```

| 属性                                    | 默认值    | 说明                                                                                                                                                            |
| --------------------------------------- | --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.app-name`                         | `default` | 逻辑应用名，非空，必须匹配 `[A-Za-z0-9_-]+`。共享 broker 的多应用必须各自设置唯一值；默认值会在启动时打印 WARN。                                                |
| `zeta.local.instance-id`                | `""`      | 队列命名与节点 ID 派生的实例标识；为空时自动取 `${server.port:instance}-${HOSTNAME}`，其中 `server.port` 读取同名**系统属性**，无 `HOSTNAME` 时用 JVM 级 UUID。 |
| `zeta.local.node-id`                    | 未设置    | 10 位版本戳节点 ID（`0..1023`）；未设置时取 `instance-id` 的稳定 CRC32。多实例请显式设置，避免同毫秒等戳碰撞。                                                  |
| `zeta.local.clock-max-forward-drift-ms` | `60000`   | 允许对端版本戳领先本地物理时间的上限。超限的戳不会抬高本地逻辑时钟，但消息本身仍会应用；`0` 表示只接受不晚于本地当前时间的戳。                                  |
| `zeta.worker.enabled`                   | `false`   | `true` 进入 Worker-only 模式：不创建 L1 缓存，`get()`/`put()` 会抛 `ZetaModeException`。                                                                        |
| `zeta.worker-listener.enabled`          | `false`   | `true` 时 App 消费 Worker 心跳与 HOT/COOL 决策。**部署了 Worker 集群却不开启它，所有报告都会被静默丢弃。**                                                      |

## 2. 本地热点检测（TopK）

App 用 HeavyKeeper（Count-Min Sketch 变体）维护一个本地 TopK。它只做快速启发式判断：命中 TopK 的 key 在本地提升为 HOT，使用更长的 TTL，但不代表集群范围的权威结论。权威的 HOT/COOL 仍由 Worker 决定。

```yaml
zeta:
  local:
    top-k: 100
    width: 50000 # 创建时自动向上对齐到 2 的幂
    depth: 5
    decay: 0.92
    min-count: 10
    sketch-window-count: 4
    expelled-queue-capacity: 10000
```

| 属性                                 | 默认值  | 说明                                                                                                                       |
| ------------------------------------ | ------- | -------------------------------------------------------------------------------------------------------------------------- |
| `zeta.local.top-k`                   | `100`   | TopK 集合大小。                                                                                                            |
| `zeta.local.width`                   | `50000` | Sketch 宽度；创建 Bean 时向上对齐到最近的 2 的幂（默认实际为 65,536），使桶下标走位掩码快速路径。上限 200,000。            |
| `zeta.local.depth`                   | `5`     | Sketch 深度（行数）。App 只需要快速启发式，较浅即可；权威判断在 Worker。                                                   |
| `zeta.local.decay`                   | `0.92`  | 计数器衰减因子。                                                                                                           |
| `zeta.local.min-count`               | `10`    | 进入 TopK 的最低计数。                                                                                                     |
| `zeta.local.sketch-window-count`     | `4`     | 每个 sketch slot 的滑动窗口数（环形缓冲）。范围 2–10，创建 Bean 时向上对齐到 2 的幂；默认覆盖 4 个衰减周期，减少热点漂移。 |
| `zeta.local.expelled-queue-capacity` | `10000` | 被挤出 TopK 的 key 的暂存队列容量，防止扩容时无界增长。                                                                    |

## 3. L1 缓存与 TTL

所有缓存名共用一张 Caffeine 表，`cache.max-size` 是整张表的进程级上限。条目分普通与热点两套 TTL：硬 TTL 到期由 Caffeine 驱逐，软 TTL 到期后返回陈旧值并在后台刷新（stale-while-revalidate）。TTL 默认带 ±5% 随机抖动，避免同一时刻集中失效。

```yaml
zeta:
  local:
    cache:
      max-size: 100000
      tombstone-maximum-size: 10000
      tombstone-ttl-seconds: 600
    hard-ttl-ms: 300000 # 普通 key：5 分钟
    soft-ttl-ms: 30000 # 普通 key：30 秒
    hot-hard-ttl-ms: 3600000 # 热点 key：1 小时
    hot-soft-ttl-ms: 300000 # 热点 key：5 分钟
    ttl-jitter-ratio: 0.05
```

| 属性                                      | 默认值              | 说明                                                                  |
| ----------------------------------------- | ------------------- | --------------------------------------------------------------------- |
| `zeta.local.cache.max-size`               | `100000`            | 整张 L1 表的最大条目数，所有缓存名共享。                              |
| `zeta.local.cache.tombstone-maximum-size` | `10000`             | 按 key 的失效墓碑上限，约束大规模失效突发下的内存。                   |
| `zeta.local.cache.tombstone-ttl-seconds`  | `600`               | 墓碑保留时长。只需覆盖一次在途读/加载；过期后失效前的回填会重新放行。 |
| `zeta.local.hard-ttl-ms`                  | `300000`（5 分钟）  | 普通 key 的硬 TTL；`0` 表示永久（不做硬过期）。                       |
| `zeta.local.hot-hard-ttl-ms`              | `3600000`（1 小时） | 热点 key 的硬 TTL；`0` 表示永久。                                     |
| `zeta.local.soft-ttl-ms`                  | `30000`（30 秒）    | 普通 key 的软 TTL；到期后返回陈旧值并异步刷新；`0` 关闭软过期。       |
| `zeta.local.hot-soft-ttl-ms`              | `300000`（5 分钟）  | 热点 key 的软 TTL；`0` 关闭软过期。                                   |
| `zeta.local.ttl-jitter-ratio`             | `0.05`              | 施加到所有 TTL 的随机偏移比例，`0.05` = ±5%；设为 `0` 关闭。          |

## 4. 加载、并发与刷新

读未命中时，`SingleFlight` 会把同一 key 的并发加载合并成一次执行；`in-flight-timeout-seconds` 限制单次加载的执行时间（排队等待不计入）。加载与后台刷新共用同一个有界线程池，`refresh-max-pools` 再用信号量限制并发刷新数。

```yaml
zeta:
  local:
    in-flight-max-size: 50000
    in-flight-ttl-seconds: 5
    in-flight-timeout-seconds: 3
    executor-core-pool-size: 8
    executor-max-pool-size: 32
    executor-queue-capacity: 2000
    executor-rejection: abort
    refresh-max-pools: 0 # 0 = executor-max-pool-size / 2
```

| 属性                                   | 默认值      | 说明                                                                                                                                                                                                                                                              |
| -------------------------------------- | ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.local.in-flight-max-size`        | `50000`     | SingleFlight 去重条目的最大数量。                                                                                                                                                                                                                                 |
| `zeta.local.in-flight-ttl-seconds`     | `5`         | 去重条目 TTL，必须大于最慢的远端数据源响应。                                                                                                                                                                                                                      |
| `zeta.local.in-flight-timeout-seconds` | `3`         | 单次加载执行超时。超时按 `LoadStatus.SUPPRESSED_TIMEOUT` 抑制，读取抛 `ZetaSuppressedException`（design decisions: read path）。去重保留期为 `max(in-flight-ttl-seconds, timeout + 1)`。                                                                          |
| `zeta.local.executor-core-pool-size`   | `8`         | 加载线程池核心大小。                                                                                                                                                                                                                                              |
| `zeta.local.executor-max-pool-size`    | `32`        | 加载线程池最大大小；必须大于或等于 `executor-core-pool-size`。                                                                                                                                                                                                    |
| `zeta.local.executor-queue-capacity`   | `2000`      | 加载线程池队列容量。                                                                                                                                                                                                                                              |
| `zeta.local.executor-rejection`        | `abort`     | 队列与最大线程池都满时的策略：`abort` 抛拒绝异常，`caller-runs` 在提交线程执行。**仅刷新扇出池生效**——加载池与发送池始终 abort。加载池拒绝会转为 `LoadStatus.SUPPRESSED_EXECUTOR`，读路径抛出 `ZetaSuppressedException`（熔断器打开时除外），绝不会按 miss 处理。 |
| `zeta.local.refresh-max-pools`         | `0`（自动） | 软过期后台刷新的最大并发数。`0` 取 `executor-max-pool-size` 的一半（至少 1）；刷新与读 miss 共用线程池，因此大于池大小的值会被收敛到 `executor-max-pool-size`。                                                                                                   |

## 5. 上报节拍

App 的读路径把访问计数累积在共享计数器中，按“潮汐”周期合并后上报给 Worker。`zeta.local.report-interval-ms` 是基线间隔（默认 50ms，低于 50ms 会被 WaveCounter 的调度节拍钳制到 50ms），`zeta.local.report-interval-tuning` 决定这个间隔是固定还是按实例自适应：

| 模式             | 行为                                                                                     |
| ---------------- | ---------------------------------------------------------------------------------------- |
| `off`            | 无调谐器，间隔固定为 `report-interval-ms`；`min-ms`、`max-ms`、`target-batch` 均不生效。 |
| `shadow`（默认） | 调谐器照常计算，并通过 `zeta.reporter.feedloop.*` 指标暴露它将采用的间隔，但不施加。     |
| `on`             | 每次冲刷（包括空窗口）都施加计算出的间隔，并限制在 `[min-ms, max-ms]` 之内。             |

示例——100–800ms 的动态上报周期，且不超过 Worker 的 1s 窗口：

```yaml
zeta:
  report:
    enabled: true
  local:
    report-interval-ms: 200 # 种子；超出 [min, max] 时会被钳制
    report-interval-min-ms: 100
    report-interval-max-ms: 800 # 空闲最坏：2 x 800 = 1600ms，仍 < 2 x 1s Worker 窗口
    report-interval-target-batch: 512
    report-interval-tuning: on
```

`on` 模式下 `report-interval-ms` 只是种子。`report-interval-max-ms` 同时是新热点 key 的首报上界：有流量时至多 `max-ms`，空闲后至多 `2 x max-ms`。`shadow` 或 `on` 下只校验 `max-ms >= min-ms`，不满足会在启动时快速失败；种子值越界则被钳制，不会失败。`target-batch` 是调谐器每次冲刷的目标 key 数：批稀疏时拉长间隔，批稠密时缩短间隔。

没有装配 reporter（`zeta.report.enabled=false`、classpath 上没有 AMQP，或 Worker-only 部署）时，共享计数器按 `detect-interval-ms` 把计数直接送入本地 sketch，不经过上报。

| 属性                                      | 默认值                         | 说明                                                                                                                                                                   |
| ----------------------------------------- | ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.report.enabled`                     | `true`                         | 启用 App→Worker 上报聚合；需要 `RabbitTemplate` Bean。                                                                                                                 |
| `zeta.local.report-exchange`              | `zeta.reportToWorker.exchange` | App 向 Worker 发送报告的 DirectExchange，JSON 报文体。必须与 Worker 侧 `zeta.worker.messaging.report-exchange` 一致。                                                  |
| `zeta.local.report-interval-ms`           | `50`                           | 潮汐基准/调谐器种子；低于 50ms 实际仍按 50ms 调度。                                                                                                                    |
| `zeta.local.detect-interval-ms`           | `500`                          | 两次 sketch 摄入之间的最大间隔。到期（或超过 key 上限）的潮汐与累积器一起直接送入 sketch，不再先合并；只有更早的潮汐才合并进累积器。没有 reporter 时它同时是潮汐基准。 |
| `zeta.local.report-interval-tuning`       | `shadow`                       | `off` / `shadow` / `on`，见上表。                                                                                                                                      |
| `zeta.local.report-interval-target-batch` | `512`                          | 调谐器的每批目标 key 数；仅 `tuning != off` 时有意义。                                                                                                                 |
| `zeta.local.report-interval-min-ms`       | `50`                           | 调谐器间隔下界；低于 WaveCounter 的 50ms 节拍下限时实际等效 50ms。                                                                                                     |
| `zeta.local.report-interval-max-ms`       | `1000`                         | 调谐器间隔上界，也是新热点的首报延迟上界；必须 ≥ `min-ms`。空闲最坏情况为该值的 2 倍。                                                                                 |

详见 [design decisions: reporting](design-decisions.md#reporting) 与 [design decisions: access counting](design-decisions.md#access-counting)。

## 6. 上报队列

上报批次先进入有界分发队列，再由消费者线程同步发送。队列是唯一的背压：队列满时提交线程最多阻塞 `queue-offer-timeout-ms`（整次 flush 共用一个预算），随后丢弃剩余批次；在队列中等待超过 5 秒的批次会在出队时被丢弃。丢弃不重试——下一拍上报的是最新计数。

```yaml
zeta:
  local:
    queue-capacity: 1024
    queue-offer-timeout-ms: 100
    consumer-count: 0 # 0 = max(4, availableProcessors / 2)
```

| 属性                                | 默认值 | 说明                                                                                                                                                                                    |
| ----------------------------------- | ------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.local.queue-capacity`         | `1024` | 上报分发队列容量，单位为批次。消费者阻塞在 publish 上时无法出队，因此这里是 broker 挂起时积压的上界；1024 批约等于 200 批/秒下的一个 5 秒陈旧窗口。一拍扇出到很多 Worker 的部署应调大。 |
| `zeta.local.queue-offer-timeout-ms` | `100`  | 队列写入超时，整次 flush 共用一个预算；超时后丢弃剩余批次。                                                                                                                             |
| `zeta.local.consumer-count`         | `0`    | 上报消费者线程数；`0` 自动取 `max(4, availableProcessors / 2)`。                                                                                                                        |

## 7. Worker 监听、心跳与集群健康

App 通过 `worker-listener` 消费 Worker 的 HOT/COOL 决策，同时消费心跳来维护 `HealthView`。后者决定报告路由到哪个 Worker，也决定 Worker 集群是否仍握有 TTL 权威、还是本地 TopK 必须接管。心跳在 AMQP `appName` 头中携带发送方的 `zeta.app-name`；App 丢弃 `appName` 头与本地不一致的心跳，因此共享 broker 不会把其他应用的 Worker 放进本租户的哈希环。升级前 Worker 的心跳没有 `appName` 头，为滚动升级兼容仍会处理。

```yaml
zeta:
  local:
    consistent-hashing:
      enabled: true
      virtual-nodes: 500
    heartbeat:
      exchange-name: zeta.heartbeat.exchange
      timeout-ms: 10000
      adaptive-timeout: true
      adaptive-floor-ms: 3000
      min-alive-workers: 0
  worker-listener:
    enabled: true
    exchange-name: zeta.send.exchange
    queue-prefix: zeta.worker
    auto-startup: true
    broadcast-jitter-ms: 0
    concurrent-consumers: 2
    scheduler-pool-size: 4
    prefetch-count: 5
    max-pending-units: 200000
```

### 决策监听

| 属性                                        | 默认值               | 说明                                                                                                                                                                                        |
| ------------------------------------------- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.worker-listener.enabled`              | `false`              | 开启心跳消费、HOT/COOL 决策监听和 HealthView。部署 Worker 集群时必须为 `true`。                                                                                                             |
| `zeta.worker-listener.exchange-name`        | `zeta.send.exchange` | 接收 HOT/COOL 的 DirectExchange，实例队列按 `send.<app-name>` 绑定；必须与 Worker 的 `zeta.worker.messaging.broadcast-exchange` 一致。心跳使用单独的 `zeta.local.heartbeat.exchange-name`。 |
| `zeta.worker-listener.queue-prefix`         | `zeta.worker`        | 实例级监听队列前缀；最终队列名为 `{prefix}:{instanceId}`。                                                                                                                                  |
| `zeta.worker-listener.auto-startup`         | `true`               | 监听容器是否随应用启动。                                                                                                                                                                    |
| `zeta.worker-listener.broadcast-jitter-ms`  | `0`                  | 应用广播前的随机抖动；`0` 表示收到即应用。                                                                                                                                                  |
| `zeta.worker-listener.concurrent-consumers` | `2`                  | 决策队列的并发消费者数。                                                                                                                                                                    |
| `zeta.worker-listener.scheduler-pool-size`  | `4`                  | 执行抖动延迟任务的调度线程池最小值；实际为 `max(this, concurrent-consumers × 2)`。                                                                                                          |
| `zeta.worker-listener.prefetch-count`       | `5`                  | 每个消费者的 AMQP 预取数。                                                                                                                                                                  |
| `zeta.worker-listener.max-pending-units`    | `200000`             | 决策调度器的全局待处理上限（加权单位）；超限丢弃（fire-and-forget，条目最终靠硬 TTL 或下一次状态转换收敛）。                                                                                |

### 心跳与存活判定

| 属性                                     | 默认值                    | 说明                                                                                                                                                          |
| ---------------------------------------- | ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.local.heartbeat.exchange-name`     | `zeta.heartbeat.exchange` | Worker 结构化心跳的 TopicExchange，必须与 `zeta.worker.messaging.heartbeat-exchange` 一致。                                                                   |
| `zeta.local.heartbeat.timeout-ms`        | `10000`                   | 心跳超时。开启自适应后，它是热身窗口与上限，不再是一刀切阈值。                                                                                                |
| `zeta.local.heartbeat.adaptive-timeout`  | `true`                    | 按每个 Worker 的心跳到达间隔做 Jacobson/Karels 估计（`SRTT + 4×RTTVAR`），钳制在 `[adaptive-floor-ms, timeout-ms]`。`false` 恢复固定 `timeout-ms`。           |
| `zeta.local.heartbeat.adaptive-floor-ms` | `3000`                    | 自适应窗口下限；必须 `<= timeout-ms`。应高于几个心跳间隔（Worker 默认 1s 一拍），避免丢一拍或短暂 GC 就误判死亡。                                             |
| `zeta.local.heartbeat.min-alive-workers` | `0`                       | 集群健康所需的最小存活 Worker 数。`0` 表示推导值：观测 Worker 数的三分之一向上取整，下限 1（3 台→1 台即健康，5 台→2 台，9 台→3 台）。正数则用绝对存活数覆盖。 |

### 哈希环

| 属性                                          | 默认值 | 说明                                                                                                 |
| --------------------------------------------- | ------ | ---------------------------------------------------------------------------------------------------- |
| `zeta.local.consistent-hashing.enabled`       | `true` | 仅控制 `/actuator/hotkeyring` 巡检端点是否创建。报告路由始终使用一致性哈希环；`false` 不会改变路由。 |
| `zeta.local.consistent-hashing.virtual-nodes` | `500`  | 每个物理 Worker 的虚拟节点数。                                                                       |

## 8. 缓存同步

`zeta.sync.*` 控制实例之间的写传播。开启后，`put(..., PutPolicy.synced())`、`refresh`、`invalidate` 和 `clearAll` 会把 PUT / REFRESH / INVALIDATE 消息广播到同一 `app-name` 的其他实例。同步是最终一致的：丢消息时对端会一直保留旧值，直到条目硬 TTL 到期。

同步写入的**生效**硬 TTL 若为永久，会在进入缓存前被拒绝：对端只能通过硬 TTL 淘汰该条目，否则会永久保留陈旧值。这包括未做单次覆盖且 `zeta.local.hard-ttl-ms: 0` 的配置，以及默认的 `@ZetaPut(sync = true)`。`refreshAll` 始终同步，并对其默认 TTL 应用同一检查。

```yaml
zeta:
  sync:
    enabled: true
    flush-delay-ms: 50
    max-defer-ms: 500
    put-max-bytes: 32768
    put-allowed-packages:
      - com.example.order.dto
    exchange-name: zeta.sync.exchange
    queue-prefix: zeta.sync
    dedup-window-seconds: 10
    dedup-max-size: 10000
    warmup-jitter-ms: 50
    concurrent-consumers: 3
    scheduler-pool-size: 4
    prefetch-count: 5
    max-pending-units: 50000
    auto-startup: true
```

| 属性                             | 默认值               | 说明                                                                                                                                                                                                                      |
| -------------------------------- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.sync.enabled`              | `false`              | 启用跨实例缓存同步（需要 RabbitMQ）。                                                                                                                                                                                     |
| `zeta.sync.flush-delay-ms`       | `50`                 | 待发送同步记录的合并窗口；窗口内同 key 写入合并为一批。                                                                                                                                                                   |
| `zeta.sync.max-defer-ms`         | `500`                | 最大延迟上限；持续写入时也强制刷出，避免 debounce 饥饿。                                                                                                                                                                  |
| `zeta.sync.put-max-bytes`        | `32768`              | 单条带值 PUT 的载荷上限。超限或无法 Jackson 往返序列化时降级为无值 REFRESH；`0` 表示所有同步写都走 REFRESH。                                                                                                              |
| `zeta.sync.put-allowed-packages` | `[]`                 | 带值 PUT 反序列化白名单额外放行的包前缀。内置白名单只有 JDK 值类型与 `io.github.hyshmily.zeta.*`；业务 DTO 不加入会 fail-closed 降级为带版本 INVALIDATE。**列出的前缀可被任何能写 broker 的发送方实例化，请保持最小化。** |
| `zeta.sync.exchange-name`        | `zeta.sync.exchange` | 同步消息的 FanoutExchange。                                                                                                                                                                                               |
| `zeta.sync.queue-prefix`         | `zeta.sync`          | 实例队列前缀；完整名称 `{prefix}:{instanceId}`。                                                                                                                                                                          |
| `zeta.sync.dedup-window-seconds` | `10`                 | 接收侧去重窗口。                                                                                                                                                                                                          |
| `zeta.sync.dedup-max-size`       | `10000`              | 去重缓存最大条目数。                                                                                                                                                                                                      |
| `zeta.sync.warmup-jitter-ms`     | `50`                 | 广播后某个 key 首次同步派发前的随机停驻上限，把对端回源分散到不同实例，避免惊群。                                                                                                                                         |
| `zeta.sync.concurrent-consumers` | `3`                  | 同步队列消费者并发数。                                                                                                                                                                                                    |
| `zeta.sync.scheduler-pool-size`  | `4`                  | 同步 jitter 延迟调度线程池大小。                                                                                                                                                                                          |
| `zeta.sync.prefetch-count`       | `5`                  | 每个同步消费者的 AMQP 预取数。                                                                                                                                                                                            |
| `zeta.sync.max-pending-units`    | `50000`              | 同步调度器全局待处理上限（加权单位，约 1KB 载荷/单位）；超限直接丢弃，消息已 ack，下一次写入会重发。                                                                                                                      |
| `zeta.sync.auto-startup`         | `true`               | 同步监听容器是否随应用启动。                                                                                                                                                                                              |

## 9. 熔断器

熔断器包在 `SingleFlight.load()` 外面。某个 key 前缀对应的 shard 打开后，加载会被立即抑制；`HotKeyCache.get()` 会尝试返回硬 TTL 尚未过期的陈旧条目（软过期、硬有效）。硬过期条目绝不作为 stale 返回。它默认关闭，因为开启会改变所有用户的读路径。

```yaml
zeta:
  local:
    circuit-breaker:
      enabled: true
      window-time-ms: 10000
      window-buckets: 10
      fail-threshold: 0.5
      request-volume-threshold: 20
      single-test-interval-ms: 5000
      half-open-max-probes: 3
      consecutive-success-threshold: 3
      max-prefixes: 64
      exclude-exceptions:
        - java.lang.IllegalArgumentException
```

| 属性                                                       | 默认值  | 说明                                                                                                                                                                              |
| ---------------------------------------------------------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.local.circuit-breaker.enabled`                       | `false` | 启用滑动窗口熔断器。                                                                                                                                                              |
| `zeta.local.circuit-breaker.window-time-ms`                | `10000` | 滑动窗口时长；窗口由流量推进，没有后台轮转。                                                                                                                                      |
| `zeta.local.circuit-breaker.window-buckets`                | `10`    | 滑动窗口桶数。                                                                                                                                                                    |
| `zeta.local.circuit-breaker.fail-threshold`                | `0.5`   | 失败率阈值（0.0–1.0），超过时打开。                                                                                                                                               |
| `zeta.local.circuit-breaker.request-volume-threshold`      | `20`    | 评估失败率所需的最小请求数。                                                                                                                                                      |
| `zeta.local.circuit-breaker.single-test-interval-ms`       | `5000`  | 半开探针间隔；半开周期内探针长时间无结算时强制重置，防止挂死。                                                                                                                    |
| `zeta.local.circuit-breaker.half-open-max-probes`          | `3`     | 半开状态最大并发探针数，超出拒绝。                                                                                                                                                |
| `zeta.local.circuit-breaker.consecutive-success-threshold` | `3`     | 半开状态连续成功多少次才关闭，防止单次成功导致抖动。                                                                                                                              |
| `zeta.local.circuit-breaker.log-enabled`                   | `true`  | 是否记录 OPEN/CLOSE/HALF-OPEN 状态切换。                                                                                                                                          |
| `zeta.local.circuit-breaker.exclude-exceptions`            | `[]`    | 不触发熔断的异常全类名；匹配异常或其 `cause` 时按成功计。                                                                                                                         |
| `zeta.local.circuit-breaker.include-exceptions`            | `[]`    | 只有列出的异常才触发熔断；为空表示全部触发。exclude 先于 include 检查。                                                                                                           |
| `zeta.local.circuit-breaker.max-prefixes`                  | `64`    | 最大存活熔断器分片数。注解 key 按 U+0000 命名空间分隔符前的 cache 命名空间分片；编程式 key 按第一个 `:` 前的段分片；两者都没有的 key 共用一个桶。单个坏数据源只影响自己的 shard。 |

## 10. Worker 基础与消息入口

Worker-only 进程消费 App 上报，维护滑动窗口与规则判定，并把 HOT/COOL 决策广播回 App。`epoch-file` 用于重启检测，必须放在跨重启保留的存储上；`max-tracked-keys` 是防 OOM 的硬上限。默认窗口（1s/16 片）下每个被跟踪 key 约占 350–400 B（翻倍的 `long[32]` 环 + map/window/key 字符串开销），因此默认 1,000,000 上限仅窗口表就约 350–400 MB 堆内存，尚未计入 hot key、Caffeine、队列等。

`zeta.worker.enabled` 在两种部署形态中二选一：

| 模式        | `worker.enabled` | 激活的 Bean                                                   |
| ----------- | ---------------- | ------------------------------------------------------------- |
| App-only    | `false`（默认）  | `HotKeyCache`、TopK、reporter、actuator、sync                 |
| Worker-only | `true`           | 仅 Worker（无缓存——`get()`/`put()` 抛出 `ZetaModeException`） |

检测面在启动时一次性校验：阈值按生效窗口换算成计数，任何违规都会让启动失败。开启后运行时视图是只读的：
`/actuator/hotkey-worker`（见 [MONITOR.zh.md](MONITOR.zh.md) §4）。发行包默认只暴露 `health`；
只读检测视图需要显式开启：在 `management.endpoints.web.exposure.include` 加入 `hotkeyworker`，
并设置 `management.endpoints.web.path-mapping.hotkeyworker: hotkey-worker`。自建 Worker 同样按此方式开启
（Boot 3.5 要求端点 id 为字母数字）。

```yaml
zeta:
  app-name: order
  worker:
    enabled: true
    max-tracked-keys: 1000000
    epoch-file: /data/zeta/epoch
    messaging:
      report-exchange: zeta.reportToWorker.exchange
      broadcast-exchange: zeta.send.exchange
      heartbeat-exchange: zeta.heartbeat.exchange
    report-consumer:
      concurrent-consumers: 8
      prefetch-count: 50
      staleness-threshold-ms: 0
    report-queue:
      max-length: 10000
      message-ttl-ms: 60000
    broadcast:
      hot-queue-capacity: 10000
      cool-queue-capacity: 10000
      cool-burst: 32
      hot-burst: 8
      max-decision-age-ms: 30000
    heartbeat:
      ping-interval-ms: 1000
      ready-grace-ms: 3000
```

`report-exchange` 只接收 App 上报；Worker 必须把分片上报队列绑定到它，而不是 `broadcast-exchange`
（后者承载 Worker→App 决策）。两者都是 `DirectExchange`，配置时按名称区分。可用
`rabbitmqctl list_bindings` 核对实际绑定（README「验证」§1）。

| 属性                                                 | 默认值                                                   | 说明                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| ---------------------------------------------------- | -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.worker.enabled`                                | `false`                                                  | 启用 Worker 模式。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `zeta.worker.max-tracked-keys`                       | `1000000`                                                | Worker 同时跟踪的 distinct key 软上限；硬余量保证 OOM 边界有限。达到上限时先回收采样中所有“超过整窗未访问”的窗口，一次采样可为并发准入释放多个名额；执行采样的 key 自身仍可用硬余量准入，只有硬账本也满时才拒绝（计数 + DEBUG 日志）；采样锁被其他线程持有时直接准入而不是拒绝——持有者可能正在释放它需要的名额，且该上限本就是近似值。一次完成的采样若未找到可回收窗口，会开启 50 ms 饱和窗口；在此期间达到上限的其他新 key 不再重复采样而直接拒绝，避免抢不到采样锁的并发准入持续绕过上限；若采样结束时账本已低于硬上限（并发清扫释放了名额）则直接准入。已持有 HOT 名额的 Key 重建窗口时同时绕过采样门控与硬余量，因此最多超出 `max-active-hot-keys` 个窗口；软上限之上还有 `max(8, 1%)` 的硬余量，达到后新 key 不再采样直接拒绝，采样后仍处于软上限则继续置位 relief 扫描；`0` 表示不限制。内存：每个被跟踪 key 持有 `2 × alignedSlices` 个 long 的翻倍环，加上约 80 B 的对象/map/字符串开销——默认 16 片时约 350–400 B/key，`1000000` 仅窗口表就约 350–400 MB 堆内存；启动时若该估算超过最大堆的一半会打印 WARN。 |
| `zeta.worker.epoch-file`                             | `""`（自动 → `${java.io.tmpdir}/zeta-epoch-<workerId>`） | 重启检测的 epoch 文件。每次启动在文件锁保护下自增；必须跨重启保留。文件为单个二进制 `long`（8 字节）；大小不是 8 字节的文件视为不存在，并在下次启动时修复。临时目录被清空会使 epoch 归零，App 将拒绝新实例的决策，直到缓存条目过期。默认文件名会把 id 中的文件系统敏感字符替换为 `_`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `zeta.worker.messaging.report-exchange`              | `zeta.reportToWorker.exchange`                           | App 报告的 DirectExchange，必须与 App 侧 `zeta.local.report-exchange` 一致。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `zeta.worker.messaging.broadcast-exchange`           | `zeta.send.exchange`                                     | HOT/COOL 广播的 DirectExchange，按 `send.<app-name>` 发布；必须与 App 侧 `zeta.worker-listener.exchange-name` 一致。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `zeta.worker.messaging.heartbeat-exchange`           | `zeta.heartbeat.exchange`                                | 心跳 TopicExchange，必须与 App 侧 `zeta.local.heartbeat.exchange-name` 一致。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `zeta.worker.report-consumer.concurrent-consumers`   | `8`                                                      | 上报队列并发消费者数，最小 1。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `zeta.worker.report-consumer.prefetch-count`         | `50`                                                     | 每个消费者的预取数，平衡吞吐与内存。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `zeta.worker.report-consumer.staleness-threshold-ms` | `0`                                                      | 消费端陈旧过滤；`0` 表示关闭，改由队列 `x-message-ttl` 兜底。开启时只丢弃正向年龄超过阈值的报告，负年龄（上报端时钟超前）始终放行。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `zeta.worker.report-queue.max-length`                | `10000`                                                  | 每个分片上报队列的最大消息数；超出后 broker 丢弃最旧消息（`drop-head`），保留最新统计信号。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `zeta.worker.report-queue.message-ttl-ms`            | `60000`                                                  | 队列内单条消息的 TTL；超时由 broker 丢弃。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `zeta.worker.broadcast.hot-queue-capacity`           | `10000`                                                  | 待发送 HOT 队列容量；队满时该决策立即丢弃（计入 `zeta.worker.decisions.dropped{type="hot"}`），并撤销产生它的晋升，名额随之归还。HOT 过期、发送失败或停机清理时同样归还名额。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `zeta.worker.broadcast.cool-queue-capacity`          | `10000`                                                  | 待发送 COOL 队列容量。它是突发缓冲而非“每席位一条”的保证：名额在 COOL 入队前已释放并可被下一轮生命周期复用，因此更小的队列也合法；被丢弃的 COOL 由 App 硬 TTL 收敛。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `zeta.worker.broadcast.cool-burst`                   | `32`                                                     | 每轮 drain 最多<b>发送</b>的 COOL 条数，之后让位给 HOT。终态条目（elide/supersede/expire）由独立的每 lane 工作量预算限制，不会长期占住 drain 线程。令牌桶上限取 `max-cool-broadcasts-per-second` 与该 burst 的较大值，因此高于每秒速率的 burst 仍可达到。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `zeta.worker.broadcast.hot-burst`                    | `8`                                                      | 每轮 drain 最多<b>发送</b>的 HOT 条数；保证 COOL 洪峰不会饿死 HOT。终态条目使用独立的每 lane 工作量预算。令牌桶上限取 `max-hot-broadcasts-per-second` 与该 burst 的较大值，因此高于每秒速率的 burst 仍可达到。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `zeta.worker.broadcast.max-decision-age-ms`          | `30000`                                                  | 决策在队列里的最大驻留时间（1ms–1h）；超过即按过期丢弃（`zeta.worker.decisions.expired`，带 `type=hot or type=cool` 标签）而不是发送；过期的 HOT 同时归还名额。广播器对已冷的 accepted 记录按该值的两倍保留且不设容量上限，因此所有仍可能发送的决策都不会失去排序前驱；最后接受的决策为 HOT 的记录会一直保留到接受一条 COOL，因为它是集群仍认为该 Key 为热的唯一凭据。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `zeta.worker.heartbeat.ping-interval-ms`             | `1000`                                                   | 结构化心跳发送间隔。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `zeta.worker.heartbeat.ready-grace-ms`               | `3000`                                                   | 没有追踪 key 时，Worker 在多久后仍上报 `ready`。只门控入环准入，用于打破冷启动“无环槽→无上报→无 key”的循环。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |

## 11. 检测（窗口、规则、容量与冷却）

一个配置块承载整个检测面：一个共享滑动窗口、一条必填的默认规则、一组有序规则、冷却调度的节奏，以及本 Worker 的
名额与广播预算。阈值以 **QPS** 表达，启动时一次换算成窗口计数——按**生效窗口**（`alignedSlices × sliceMs`）而不是
配置时长。`1s / 16` 实际运行 16 片 × 62ms（992ms），`hot-qps: 100` 换算为 `ceil(100 × 992 / 1000)` = 100 次访问。

晋升是单观测语义：`windowSum >= hotCount` 立即晋升。冷却有两种策略：

- `COOL_FOR_LOW_QPS`——窗口和持续严格低于 `cool-qps` 达 `low-qps-for` 后冷却；滞回带内的 key 一直保留名额。
- `COOL_FOR_EXPIRE`——晋升后恰好 `cool-after` 即冷却，不看流量；仍够热的 key 在下一批报告里重新挣回名额。
  怕"HOT 丢了不自愈"就用它。

规则按优先级匹配：`priority` 降序，其次类型特异性（`exact` > `prefix` > `regex`），再次 pattern 长度降序，最后配置
顺序。key 晋升时绑定当时的规则，替换规则集不会改变已热 key 的冷却标尺。默认规则兜底一切未命中的 key。

```yaml
zeta:
  worker:
    detection:
      enabled: true
      window:
        duration: 1s
        slices: 16
        retention: 2m
      defaults:
        hot-qps: 1000
        cool:
          strategy: COOL_FOR_LOW_QPS
          cool-qps: 100
          low-qps-for: 30s
      rules:
        - id: user-prefix
          match:
            type: prefix
            key: "user:"
          hot-qps: 200
          cool:
            strategy: COOL_FOR_EXPIRE
            cool-after: 10m
      cooling:
        low-qps-check-interval: 1s
        sweep-interval: 30s
        threads: 2
      limits:
        max-active-hot-keys: 10000
        max-hot-broadcasts-per-second: 500
        max-cool-broadcasts-per-second: 500
```

| 属性                                                          | 默认                | 说明                                                                                                                                                                                                                     |
| ------------------------------------------------------------- | ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `zeta.worker.detection.enabled`                               | `true`              | `false` 时保留监听但跳过全部评估并计数（`zeta.worker.detection.disabled.dropped`）；关闭期间不要求 `detection.defaults`。                                                                                                |
| `zeta.worker.detection.window.duration`                       | `1s`                | 配置的窗口时长；不能被对齐后片数整除时启动告警。                                                                                                                                                                         |
| `zeta.worker.detection.window.slices`                         | `16`                | 片数；向上对齐到 2 的幂。                                                                                                                                                                                                |
| `zeta.worker.detection.window.retention`                      | `2m`                | 空闲窗口回收阈值（仅内存上限）；必须 ≥ 生效窗口（`alignedSlices × sliceMs`）；恰在边界上的窗口可能仍带着保护分片，容量回收扫描会多等一个分片再回收。                                                                     |
| `zeta.worker.detection.defaults.hot-qps`                      | —（必填）           | 未命中规则的 HOT 阈值；字段可空，检测开启时缺了就是启动失败，而不是默默用默认值。                                                                                                                                        |
| `zeta.worker.detection.defaults.cool.strategy`                | —（必填）           | `COOL_FOR_LOW_QPS` 或 `COOL_FOR_EXPIRE`。                                                                                                                                                                                |
| `zeta.worker.detection.defaults.cool.cool-qps`                | LOW_QPS 必填        | 冷却带上限；换算后的 `coolCount` 必须严格小于 `hotCount`（滞回带为空即启动失败）。                                                                                                                                       |
| `zeta.worker.detection.defaults.cool.low-qps-for`             | LOW_QPS 必填        | 低于冷却带多久后冷却；必须 ≥ 生效窗口（`alignedSlices × sliceMs`；默认 1s/16 片为 992ms）。                                                                                                                              |
| `zeta.worker.detection.defaults.cool.cool-after`              | EXPIRE 必填         | HOT 存续时长；至少 1s。LOW_QPS 规则配置它（或 EXPIRE 规则配置 `cool-qps`）都是启动失败。                                                                                                                                 |
| `zeta.worker.detection.rules[].id`                            | 自动 `rule-<index>` | 指标 tag 用的稳定 id；必须唯一，且不能使用保留的 `defaults`（默认规则占用）。                                                                                                                                            |
| `zeta.worker.detection.rules[].match.type`                    | —                   | `exact` / `prefix` / `regex`（全串匹配）。生效的 `exact` 规则按键索引并在全局晋升下界之前检查，由该 exact 规则自身的阈值决定是否进入解析，未达阈值的上报不会写入匹配缓存；单条低阈值 exact 规则不会拉低所有 Key 的下界。 |
| `zeta.worker.detection.rules[].match.key`                     | —                   | pattern；空串或编译不过的正则都会启动失败。                                                                                                                                                                              |
| `zeta.worker.detection.rules[].priority`                      | `0`                 | 越大越优先。                                                                                                                                                                                                             |
| `zeta.worker.detection.rules[].hot-qps` / `.cool.*`           | —                   | 每条必须完整，不从 `defaults` 继承。                                                                                                                                                                                     |
| `zeta.worker.detection.cooling.low-qps-check-interval`        | `1s`                | LOW_QPS 复核周期；下界是一片、上界是配置时长，它决定复核速率 `max-active-hot-keys × 1000 / interval` 次/秒。                                                                                                             |
| `zeta.worker.detection.cooling.sweep-interval`                | `30s`               | 回收空闲窗口并重估逾期的冷却计时器，最小 1s；丢失的计时器最多晚一个周期再加一秒宽限——刚刚到期、仍由冷却轮线程处理的检查不由扫描代劳。                                                                                    |
| `zeta.worker.detection.cooling.threads`                       | `2`                 | 分片冷却轮的数量，每个轮一个推进线程；Key 按哈希分区。                                                                                                                                                                   |
| `zeta.worker.detection.limits.max-active-hot-keys`            | `10000`             | HOT 名额。满员时新晋升被拒（`zeta.worker.hot.rejected`），已有 key 永不被淘汰。                                                                                                                                          |
| `zeta.worker.detection.limits.max-hot-broadcasts-per-second`  | `500`               | HOT 发送预算，在 drain 线程用令牌桶执行。                                                                                                                                                                                |
| `zeta.worker.detection.limits.max-cool-broadcasts-per-second` | `500`               | COOL 发送预算；COOL 优先且带突发上限，冷却洪峰不会饿死 HOT。                                                                                                                                                             |

启动校验把所有违规合并成一次失败。硬错误：QPS/几何边界、`coolCount < hotCount`、`low-qps-for < 生效窗口`、
复核间隔不低于一片、策略下无效果字段、`defaults` 或规则不完整（仅检测开启时要求）、重复或保留的规则 id、
sweep 间隔低于 1s。告警：名额超过 `max-tracked-keys`、每个冷却轮复核速率超 200,000/s（默认规则也计入）、
阈值少于 10 次访问（泊松噪声）、冷却带低于 `hot-qps` 不足 10%、EXPIRE 刷新量超出广播预算。

## 12. 调度与内置周期

`zeta.scheduling.enabled` 是 App 侧内部定时任务的总开关。关闭后，HeavyKeeper 衰减、挤出队列清空、TopK 准入提升排空和 L1 过期清理都会停止；事件驱动的 NORMAL→HOT 提升也会停止，但加载和写入时命中 TopK 仍会直接标记 HOT。

```yaml
zeta:
  scheduling:
    enabled: true
  local:
    scheduler-pool-size: 8 # 共享调度器线程池
```

| 属性                             | 默认值 | 说明                                                                                                                                           |
| -------------------------------- | ------ | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.scheduling.enabled`        | `true` | 内部定时任务总开关。                                                                                                                           |
| `zeta.local.scheduler-pool-size` | `8`    | Zeta 共享调度器线程池大小。任务不会重叠；异常经 `ZetaExceptionHandler` 上报后继续下一次调度。Worker 控制面使用独立单线程调度器，不受此项影响。 |
