[← 返回首页](README.zh.md)

## 配置参考

| 方法                                                   | 说明                                                                                                                                                                        |
| ------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `peek(key)`                                            | 仅查 L1，不做频率追踪，不读 L2，不上报                                                                                                                                      |
| `getLocalCache()`                                      | 暴露原始 Caffeine {@code Cache<String, Object>}，用于 Caffeine 特定操作（asMap、policy、cleanUp）。⚠️ 绕过 HotKey 编排层——版本追踪、广播和过期管理均被跳过。仅操作本地 L1。 |
| `estimatedSize()`                                      | L1 缓存当前条目的估算数量（最佳估算）                                                                                                                                       |
| `stats()`                                              | L1 缓存统计快照：命中数、未命中数、命中率、驱逐数、估算大小                                                                                                                |
| `get(key, reader)`                                     | 从 L1 或 L2 reader 读取；每次访问触发本地 TopK 追踪 + App→Worker 上报；热点 key 提升到 L1（使用热点 TTL），普通 key 使用普通 TTL                                            |
| `get(key, reader, hardTtlMs, softTtlMs)`               | 同上，带 per-entry 硬和软 TTL 覆盖（传入 0 使用配置默认值）                                                                                                                 |
| `getWithSoftExpire(key, reader)`                       | 软失效——返回过期旧值+触发异步刷新；每次访问触发本地 TopK 追踪 + App→Worker 上报；根据 key 状态使用全局默认 TTL                                                              |
| `getWithSoftExpire(key, reader, softTtlMs)`            | 同上，带 per-call 软 TTL 覆盖（毫秒）                                                                                                                                       |
| `getWithSoftExpire(key, reader, hardTtlMs, softTtlMs)` | 同上，同时带 per-entry 硬 TTL 和 per-call 软 TTL 覆盖（毫秒）                                                                                                               |
| `putThrough(key, value, writer)`                       | 写穿透：writer.run()、nextVersion()、L1 更新（根据 key 状态使用有效 TTL）、可选同步                                                                                         |
| `putThrough(key, value, writer, hardTtlMs, softTtlMs)` | 同上，带 per-entry 硬和软 TTL 覆盖（传入 0 使用配置默认值）                                                                                                                 |
| `putBeforeInvalidate(key, mutation)`                   | 先写后失效，用于集合增量操作（LPUSH、SADD、ZADD）                                                                                                                           |
| `isLocalHotKey(cacheKey)`                              | 检查 key 是否在 L1 中为 HOT 状态（O(1)）                                                                                                                                    |
| `isWorkerHotKey(cacheKey)`                             | 检查 key 是否在 Worker TopK 中为集群热点（O(n)）                                                                                                                            |
| `notifyLocalDetector(cacheKey)`                        | 触发本地 HotKeyDetector 追踪指定 key，无需执行完整缓存读取。被 `@Intercept` 用于在方法体被跳过时保持 TopK 准确。                                                            |
| `isBlacklisted(cacheKey)`                              | 快速检查 key 是否被黑名单规则封锁                                                                                                                                           |
| `isWhitelisted(cacheKey)`                              | 快速检查 key 是否在白名单中（跳过 Worker 上报）                                                                                                                             |
| `invalidate(cacheKey)`                                 | 使单个 key 在所有缓存层失效                                                                                                                                                 |
| `invalidateAll()`                                      | 紧急清空——无广播地失效所有 L1 条目                                                                                                                                         |
| `invalidateAll(cacheKeys...)`                          | 可变参数重载 — 批量失效多个 key                                                                                                                                             |
| `invalidateAll(Collection)`                            | Collection 重载                                                                                                                                                             |
| `returnLocalHotKeys()`                                 | 应用端 Top-K 快照（key + 计数）                                                                                                                                             |
| `returnLocalExpelledHotKeys()`                         | 获取应用端被挤出的热点 key 队列；由内部定时器周期性清空                                                                                                                     |
| `returnLocalTotalDataStreams()`                        | 经过应用端 HeavyKeeper 的累计读取数                                                                                                                                         |
| `returnWorkerHotKeys()`                                | Worker 端（集群维度）Top-K 快照                                                                                                                                             |
| `returnWorkerExpelledHotKeys()`                        | Worker 端被挤出的热点 key 队列                                                                                                                                              |
| `returnWorkerTotalDataStreams()`                       | Worker 端 HeavyKeeper 累计读取数                                                                                                                                            |

### 核心配置（`hotkey.local.*`）

| 属性                                    | 默认值                   | 说明                                                                                         |
| --------------------------------------- | ------------------------ | -------------------------------------------------------------------------------------------- |
| `hotkey.local.top-k`                    | `100`                    | Top-K 集合大小                                                                               |
| `hotkey.local.width`                    | `50000`                  | Count-Min Sketch 宽度                                                                        |
| `hotkey.local.depth`                    | `5`                      | Count-Min Sketch 深度（行数）                                                                |
| `hotkey.local.decay`                    | `0.92`                   | 冲突衰减因子                                                                                 |
| `hotkey.local.min-count`                | `10`                     | 热点 key 最低计数阈值                                                                        |
| `hotkey.local.local-cache-max-size`     | `1000`                   | Caffeine L1 最大条目数                                                                       |
| `hotkey.local.local-cache-ttl-minutes`  | `5`                      | Caffeine L1 写入 TTL（分钟）                                                                 |
| `hotkey.local.inflight-max-size`        | `50000`                  | Inflight 去重最大条目数                                                                      |
| `hotkey.local.inflight-ttl-seconds`     | `5`                      | Inflight 去重 TTL（必须超过最慢 L2 响应）                                                    |
| `hotkey.local.inflight-timeout-seconds` | `3`                      | Inflight 超时（必须 < inflight-ttl-seconds）。超时返回 `Optional.empty()`，调用方应回退到 DB |
| `hotkey.local.executor-core-pool-size`  | `8`                      | 线程池核心大小                                                                               |
| `hotkey.local.executor-max-pool-size`   | `32`                     | 线程池最大大小                                                                               |
| `hotkey.local.executor-queue-capacity`  | `2000`                   | 线程池队列容量                                                                               |
| `hotkey.local.expelled-queue-capacity`  | `50000`                  | 被驱逐热 key 暂存队列容量（防止 TopK 溢出）                                                  |
| `hotkey.local.default-hard-ttl-ms`      | `300000`（5分钟）        | 普通 key 默认硬 TTL（Caffeine 驱逐）                                                         |
| `hotkey.local.hard-ttl-ms`              | `0`                      | 普通 key 每次调用的硬 TTL 覆盖；0 = 使用 `default-hard-ttl-ms`                               |
| `hotkey.local.default-hot-hard-ttl-ms`  | `3600000`（1小时）       | 热点 key 默认硬 TTL                                                                          |
| `hotkey.local.hot-hard-ttl-ms`          | `0`                      | 热点 key 每次调用的硬 TTL 覆盖；0 = 使用 `default-hot-hard-ttl-ms`                           |
| `hotkey.local.default-soft-ttl-ms`      | `30000`（30秒）          | 普通 key 默认软 TTL（过期后异步刷新）                                                        |
| `hotkey.local.soft-ttl-ms`              | `0`                      | 普通 key 每次调用的软 TTL 覆盖；0 = 使用 `default-soft-ttl-ms`                               |
| `hotkey.local.default-hot-soft-ttl-ms`  | `300000`（5分钟）        | 热点 key 默认软 TTL                                                                          |
| `hotkey.local.hot-soft-ttl-ms`          | `0`                      | 热点 key 每次调用的软 TTL 覆盖；0 = 使用 `default-hot-soft-ttl-ms`                           |
| `hotkey.local.refresh-max-pools`        | `100`                    | 软过期最大并发异步刷新数（信号量）                                                           |
| `hotkey.local.version-key-ttl-minutes`  | `60`                     | Redis 版本 key TTL（分钟），最小值为 1                                                       |
| `hotkey.local.report-exchange`          | `hotkey.report.exchange` | App 向 Worker 发送报告消息的 RabbitMQ 交换机                                                 |
| `hotkey.local.report-interval-ms`       | `100`                    | App 实例批量发送 TopK 报告到 Worker 的时间间隔（毫秒）                                       |
| `hotkey.local.app-name`                 | `"default"`              | 逻辑应用名，用于 Worker 路由的租户区分                                                       |
| `hotkey.local.shard-count`              | `1`                      | 消费者线程数自动计算的除数（max(1, shardCount/2)）；路由默认使用一致性哈希                   |
| `hotkey.local.instance-id`              | `""`（自动检测）         | 用于队列命名的显式实例 ID；为空时自动检测为 `server.port-HOSTNAME`（或 `server.port-UUID`）  |
| `hotkey.local.queue-capacity`           | `10000`                  | 报告分发器队列容量（内部有界队列）                                                           |
| `hotkey.local.queue-offer-timeout-ms`   | `100`                    | 报告队列写入超时（毫秒）——阻塞此时长后丢弃                                                   |
| `hotkey.local.consumer-count`           | `0`                      | 报告消费者线程数；0 = 自动（max(1, shardCount / 2)）                                         |

### 心跳配置（`hotkey.local.heartbeat.*`）

| 属性                                            | 默认值                      | 说明                                                                   |
| ----------------------------------------------- | --------------------------- | ---------------------------------------------------------------------- |
| `hotkey.local.heartbeat.exchange-name`          | `hotkey.heartbeat.exchange` | 心跳 Topic 交换机名称，用于 Worker 的 epoch 驱动结构化心跳             |
| `hotkey.local.heartbeat.timeout-ms`             | `3000`                      | 超时（毫秒）——在此窗口内未收到心跳则判定 Worker 死亡                   |
| `hotkey.local.heartbeat.verify-interval-ms`     | `1500`                      | 验证间隔（毫秒）——对怀疑死亡的 Worker 发送 Direct reply-to PING 的间隔 |
| `hotkey.local.heartbeat.ping-timeout-ms`        | `2000`                      | PING/PONG 验证探针超时（毫秒）                                         |
| `hotkey.local.heartbeat.degrade-after-failures` | `2`                         | 连续 PING 失败次数超过此值后降级该 Worker（允许本地提升其条目）        |

### 上报配置（`hotkey.report.*`）

| 属性                    | 默认值 | 说明                                                        |
| ----------------------- | ------ | ----------------------------------------------------------- |
| `hotkey.report.enabled` | `true` | 启用 App 到 Worker 的报告聚合（需要 `RabbitTemplate` Bean） |

### Reporter 速率限制器（`hotkey.local.reporter.*`）

| 属性                                         | 默认值  | 说明                                                                                                                                     |
| -------------------------------------------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `hotkey.local.reporter.enabled`              | `true`  | 启用 BBR 自适应速率限制，作用于 Reporter 刷盘路径                                                                                        |
| `hotkey.local.reporter.cpu-threshold`        | `800`   | CPU 阈值（0–1000 刻度，800 = 80%）。低于此值时限制器宽松（并发≤预算 **或** 不在冷却期即准入）；达到或超过此值时严格（仅并发≤预算才准入） |
| `hotkey.local.reporter.cpu-poll-interval-ms` | `500`   | CPU 轮询间隔（ms）。守护线程以此频率调用 `com.sun.management.OperatingSystemMXBean.getCpuLoad()`                                         |
| `hotkey.local.reporter.cpu-decay`            | `0.95`  | CPU 负载 EMA 平滑衰减因子（0.0–1.0）。值越高越平滑，但响应越慢                                                                           |
| `hotkey.local.reporter.bbr-window-ms`        | `10000` | BBR 滑动窗口时长（ms），用于追踪最大通过率和最小往返时间                                                                                 |
| `hotkey.local.reporter.bbr-window-buckets`   | `100`   | BBR 滑动窗口的桶数                                                                                                                       |
| `hotkey.local.reporter.bbr-cooldown-ms`      | `1000`  | 批次丢弃后的冷却时间（ms）。冷却期内无论 CPU 状态如何，限制器拒绝所有准入                                                                |

### 调度配置（`hotkey.scheduling.*`，`hotkey.decay-period`）

| 属性                        | 默认值 | 说明                                                                             |
| --------------------------- | ------ | -------------------------------------------------------------------------------- |
| `hotkey.scheduling.enabled` | `true` | 启用内部定时器（HeavyKeeper 衰减 + 挤出队列清空）                                |
| `hotkey.decay-period`       | `20`   | HeavyKeeper 衰减周期（秒），通过 `@Scheduled` 直接解析，不在 `hotkey.local.*` 下 |

### 一致性哈希（`hotkey.local.consistent-hashing.*`）

| 属性                                            | 默认值 | 说明                                                      |
| ----------------------------------------------- | ------ | --------------------------------------------------------- |
| `hotkey.local.consistent-hashing.enabled`       | `true` | 启用一致性哈希动态 Worker 路由（默认）；设为 `false` 禁用 |
| `hotkey.local.consistent-hashing.virtual-nodes` | `500`  | 每个物理 Worker 节点的虚拟节点数，用于哈希空间分布        |

### Spring Cache 集成（`hotkey.spring-cache.*`）

| 属性                                 | 默认值  | 说明                                                                    |
| ------------------------------------ | ------- | ----------------------------------------------------------------------- |
| `hotkey.spring-cache.enabled`        | `false` | 启用 Spring Cache 集成（将 `HotKeyCacheManager` 暴露为 CacheManager）    |
| `hotkey.spring-cache.key-separator`  | `::`    | 缓存区名称与 key 之间的分隔符（例如 `"users::123"`）                   |

支持标准 `@Cacheable` / `@CachePut` / `@CacheEvict` 触发热键检测、软过期和跨实例广播。同伴注解 `@HotKeyCacheTTL`、`@Intercept`、`@Fallback` 和 `@NullCaching` 在 `@Cacheable` 上继续有效。

### 缓存同步（`hotkey.sync.*`）

| 属性                               | 默认值                 | 说明                                                                             |
| ---------------------------------- | ---------------------- | -------------------------------------------------------------------------------- |
| `hotkey.sync.enabled`              | `false`                | 启用跨实例缓存同步（RabbitMQ）                                                   |
| `hotkey.sync.exchange-name`        | `hotkey.sync.exchange` | 同步消息 Fanout 交换机名称（REFRESH / INVALIDATE / INVALIDATE_ALL / RULES_SYNC） |
| `hotkey.sync.queue-prefix`         | `hotkey.sync`          | 队列名前缀；完整名称 = `{prefix}:{instanceId}`                                   |
| `hotkey.sync.dedup-window-seconds` | `10`                   | 接收同步消息的去重窗口（秒）                                                     |
| `hotkey.sync.dedup-max-size`       | `10000`                | 去重缓存最大条目数                                                               |
| `hotkey.sync.warmup-jitter-ms`     | `100`                  | 处理同步消息前的随机 jitter（防止惊群效应）                                      |
| `hotkey.sync.concurrent-consumers` | `3`                    | 同步队列 RabbitMQ 消费者并发数                                                   |
| `hotkey.sync.scheduler-pool-size`  | `4`                    | 同步 jitter 延迟的线程池大小                                                     |
| `hotkey.sync.prefetch-count`       | `5`                    | 每个同步消费者的 AMQP 预取数量                                                   |
| `hotkey.sync.auto-startup`         | `true`                 | 同步监听器容器是否随应用自动启动                                                 |

### Worker 监听器（`hotkey.worker-listener.*`）

| 属性                                           | 默认值                      | 说明                                            |
| ---------------------------------------------- | --------------------------- | ----------------------------------------------- |
| `hotkey.worker-listener.enabled`               | `false`                     | 启用接收 Worker HOT/COOL 决策                   |
| `hotkey.worker-listener.exchange-name`         | `hotkey.broadcast.exchange` | Worker 广播 Fanout 交换机名称                   |
| `hotkey.worker-listener.queue-prefix`          | `hotkey.worker`             | 队列名前缀；完整名称 = `{prefix}:{instanceId}`  |
| `hotkey.worker-listener.warmup-jitter-ms`      | `100`                       | 处理 Worker 消息前的随机 jitter（防止惊群效应） |
| `hotkey.worker-listener.concurrent-consumers`  | `2`                         | Worker 监听队列 RabbitMQ 消费者并发数           |
| `hotkey.worker-listener.scheduler-pool-size`   | `2`                         | Worker 监听器延迟 Redis 读取的线程池大小        |
| `hotkey.worker-listener.prefetch-count`        | `5`                         | Worker 监听器每个消费者的 AMQP 预取数量         |
| `hotkey.worker-listener.auto-startup`          | `true`                      | Worker 监听器容器是否随应用自动启动             |
| **`hotkey.worker-listener.sre.*`**             |                             | **SRE 自适应速率限制器**                        |
| `hotkey.worker-listener.sre.enabled`           | `true`                      | 在 HOT 决策处理路径上启用 SRE 速率限制器        |
| `hotkey.worker-listener.sre.window-ms`         | `3000`                      | 速率计算的滑动窗口时长（毫秒）                  |
| `hotkey.worker-listener.sre.buckets`           | `10`                        | 滑动窗口的桶数                                  |
| `hotkey.worker-listener.sre.min-samples`       | `20`                        | 限流开始前的最小总样本数                        |
| `hotkey.worker-listener.sre.success-threshold` | `0.6`                       | 成功率阈值（0.0–1.0）；成功率低于此值时触发限流 |

### Worker 节点（`hotkey.worker.*`）

| 属性                                                                 | 默认值                      | 说明                                                                                             |
| -------------------------------------------------------------------- | --------------------------- | ------------------------------------------------------------------------------------------------ |
| `hotkey.worker.enabled`                                              | `false`                     | 启用 Worker 模式（必须显式设为 true）                                                            |
| **`hotkey.worker.routing.*`**                                        |                             | **路由**                                                                                         |
| `hotkey.worker.routing.app-name`                                     | `"default"`                 | 逻辑应用名（租户区分）                                                                           |
| **`hotkey.worker.messaging.*`**                                      |                             | **消息**                                                                                         |
| `hotkey.worker.messaging.report-exchange`                            | `hotkey.report.exchange`    | App 报告消息的直接交换机                                                                         |
| `hotkey.worker.messaging.broadcast-exchange`                         | `hotkey.broadcast.exchange` | HOT/COOL 广播的交换机（Worker 使用路由键发布；可能需要与 worker-listener.exchange-name 对齐）    |
| `hotkey.worker.messaging.heartbeat-exchange`                         | `hotkey.heartbeat.exchange` | epoch 驱动结构化心跳的 Topic 交换机（必须与 App 端 `hotkey.local.heartbeat.exchange-name` 一致） |
| **`hotkey.worker.sliding-window.*`**                                 |                             | **滑动窗口**                                                                                     |
| `hotkey.worker.sliding-window.duration-ms`                           | `1000`                      | 滑动窗口时长（毫秒）                                                                             |
| `hotkey.worker.sliding-window.slices`                                | `10`                        | 每个窗口的时间片数                                                                               |
| **`hotkey.worker.threshold.*`**                                      |                             | **热点阈值**                                                                                     |
| `hotkey.worker.threshold.hot-threshold`                              | `1000`                      | 绝对热点阈值；`-1` = 使用比例阈值                                                                |
| `hotkey.worker.threshold.hot-threshold-ratio`                        | `0.01`                      | 热点阈值占估计全局 QPS 的比例（1%）                                                              |
| **`hotkey.worker.state-machine.*`**                                  |                             | **状态机**                                                                                       |
| `hotkey.worker.state-machine.confirm-duration-ms`                    | `300`                       | key 持续超过阈值才确认 HOT 的时长                                                                |
| `hotkey.worker.state-machine.cool-duration-ms`                       | `15000`                     | key 持续低于阈值才确认 COLD 的时长                                                               |
| `hotkey.worker.state-machine.pre-cool-grace-ms`                      | `5000`                      | COOL 结束时的宽限期，允许静默恢复                                                                |
| `hotkey.worker.state-machine.evict-interval-ms`                      | `30000`                     | 过期状态擦除间隔（毫秒）；必须 >= cool-duration-ms \* 2                                          |
| **`hotkey.worker.global-qps-dynamic-threshold.*`**                   |                             | **动态阈值（全局 QPS）**                                                                         |
| `hotkey.worker.global-qps-dynamic-threshold.recalculate-interval-ms` | `60000`                     | 动态阈值重新计算的时间间隔                                                                       |
| `hotkey.worker.global-qps-dynamic-threshold.qps-change-tolerance`    | `0.5`                       | 触发阈值更新的 QPS 变化容忍度（±50%）                                                            |
| `hotkey.worker.global-qps-dynamic-threshold.learning-period-ms`      | `30000`                     | QPS 估算的学习周期                                                                               |
| `hotkey.worker.global-qps-dynamic-threshold.hot-threshold-ratio`     | `0.01`                      | 热阈值占估计全局 QPS 的比例                                                                      |
| **`hotkey.worker.topk-validation.*`**                                |                             | **TopK 验证**                                                                                    |
| `hotkey.worker.topk-validation.validate-interval-ms`                 | `60000`                     | Top-K 交叉验证的运行间隔                                                                         |
| `hotkey.worker.topk-validation.pre-warm-count`                       | `5`                         | 有资格预热的 Top-K 数量                                                                          |
| `hotkey.worker.topk-validation.pre-warm-min-appearances`             | `2`                         | 预热前所需的最小连续 Top-K 出现次数                                                              |
| **`hotkey.worker.heavy-keeper.*`**                                   |                             | **HeavyKeeper（Worker 端）**                                                                     |
| `hotkey.worker.heavy-keeper.top-k`                                   | `100`                       | Worker 端 HeavyKeeper Top-K 容量                                                                 |
| `hotkey.worker.heavy-keeper.width`                                   | `20000`                     | Worker 端 Count-Min Sketch 宽度                                                                  |
| `hotkey.worker.heavy-keeper.depth`                                   | `10`                        | Worker 端 Count-Min Sketch 深度                                                                  |
| `hotkey.worker.heavy-keeper.decay`                                   | `0.9`                       | Worker 端 HeavyKeeper 衰减因子                                                                   |
| `hotkey.worker.heavy-keeper.min-count`                               | `10`                        | Worker 端最低计数阈值                                                                            |
| **`hotkey.worker.heartbeat.*`**                                      |                             | **心跳**                                                                                         |
| `hotkey.worker.heartbeat.ping-interval-ms`                           | `1000`                      | 结构化心跳发送间隔（毫秒）                                                                       |
| **`hotkey.worker.persistence.*`**                                    |                             | **TopK 持久化（热启动）**                                                                        |
| `hotkey.worker.persistence.enabled`                                  | `false`                     | 启用周期性 TopK 快照到 Redis（需手动开启）                                                       |
| `hotkey.worker.persistence.persist-interval-ms`                      | `30000`                     | TopK 快照间隔（毫秒）                                                                            |
| `hotkey.worker.persistence.topk-count`                               | `100`                       | 每次快照保存的 top key 数量                                                                      |
| `hotkey.worker.persistence.redis-key-prefix`                         | `"hotkey:topk:worker:"`     | Redis key 前缀；最终 key = prefix + appName + ":" + nodeId                                       |
| `hotkey.worker.persistence.ttl-days`                                 | `3`                         | Redis 中 TopK 数据的过期时间（天）                                                               |

## 模块说明

| 模块                                                     | 依赖                                                                                   | 自动配置条件                                                                                                                                                     |
| -------------------------------------------------------- | -------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `facade`                                                 | 无                                                                                     | 始终启用                                                                                                                                                         |
| `hotkey`                                                 | 无                                                                                     | 始终启用                                                                                                                                                         |
| `report`                                                 | `spring-boot-starter-amqp`                                                             | `@ConditionalOnBean(RabbitTemplate.class)` + 属性（`hotkey.report.enabled`）                                                                                     |
| `spring-cache`                                            | `spring-boot-starter-cache`                                                         | `@ConditionalOnClass(AbstractValueAdaptingCache.class)` + `@ConditionalOnBean(HotKey.class)` + 属性（`hotkey.spring-cache.enabled`）                                  |
| `cache`（Redis）                                         | `spring-boot-starter-data-redis`                                                       | `@ConditionalOnClass(RedisTemplate.class)` + `@ConditionalOnBean(RedisTemplate.class)`                                                                           |
| `amqp`（RabbitMQ，合并到 `HotKeyAmqpAutoConfiguration`） | `spring-boot-starter-amqp`（+ `spring-boot-starter-data-redis`，worker-listener 需要） | `@ConditionalOnClass(RabbitTemplate.class)` + 内部 `@ConditionalOnClass(RedisTemplate.class)` + 属性（`hotkey.sync.enabled` / `hotkey.worker-listener.enabled`） |
| `worker`                                                 | `spring-boot-starter-amqp`（+ `spring-boot-starter-data-redis`）                       | `@ConditionalOnBean(RabbitTemplate.class)` + 属性（`hotkey.worker.enabled`）                                                                                     |
| `actuator`                                               | `spring-boot-starter-actuator`                                                         | `@ConditionalOnClass(Endpoint.class)`                                                                                                                            |
| `micrometer`                                             | `io.micrometer:micrometer-core`                                                        | `@ConditionalOnClass(MeterBinder.class)` — 自动注册 Caffeine 缓存指标（`hotkey.l1.*`）+ 自定义 HotKey 业务指标                                                   |
| `scheduling`                                             | 无                                                                                     | `@ConditionalOnProperty` + `@ConditionalOnBean(TopK.class)`                                                                                                      |

## 安全性

所有基于 RabbitMQ 的交换机（`sync`、`report`、`worker/broadcast`）默认使用明文 AMQP 连接。生产环境中应通过 Spring Boot 的 `spring.rabbitmq.ssl.*` 配置 TLS：

```yaml
spring:
  rabbitmq:
    ssl:
      enabled: true
      key-store: classpath:client.p12
      key-store-password: changeit
      trust-store: classpath:truststore.jks
      trust-store-password: changeit
```

详见 [Spring Boot RabbitMQ SSL 文档](https://docs.spring.io/spring-boot/reference/messaging/amqp.html#page-title)。
