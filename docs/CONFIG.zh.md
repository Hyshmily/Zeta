[← 返回首页](README.zh.md)

## 配置参考

| 方法                                                   | 说明                                                                                                                                                                        |
| ------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `peek(key)`                                            | 仅查 L1，不做频率追踪，不读 L2，不上报                                                                                                                                      |
| `peekAll(Collection)`                                  | 批量 peek——返回 `Map<String, Object>` 的存在的键值对；缺失的 key 静默忽略                                                                                                   |
| `getLocalCache()`                                      | 暴露原始 Caffeine {@code Cache<String, Object>}，用于 Caffeine 特定操作（asMap、policy、cleanUp）。⚠️ 绕过 Zeta 编排层——版本追踪、广播和过期管理均被跳过。仅操作本地 L1。 |
| `estimatedSize()`                                      | L1 缓存当前条目的估算数量（最佳估算）                                                                                                                                       |
| `stats()`                                              | L1 缓存统计快照：命中数、未命中数、命中率、驱逐数、估算大小                                                                                                                |
| `computeIfAbsent(key, reader)`                         | `get(key, reader).orElse(null)` 的便捷简写；缓存未命中时通过 supplier 加载，loader 返回 null 时返回 null                                                                    |
| `computeIfAbsent(key, reader, hardTtlMs)`              | 同上，带显式硬 TTL 覆盖                                                                                                                                                         |
| `computeIfAbsent(key, reader, hardTtlMs, softTtlMs)`   | 同上，带显式硬和软 TTL 覆盖                                                                                                                                                     |
| `computeIfAbsent(Collection, Function)`                | 批量重载——遍历 keys 并返回 `Map<String, V>` 的加载结果                                                                                                                          |
| `computeIfAbsentWithSoftExpire(key, reader)`           | `getWithSoftExpire(key, reader).orElse(null)` 的便捷简写                                                                                                                        |
| `computeIfAbsentWithSoftExpire(key, reader, softTtlMs)` | 同上，带显式软 TTL 覆盖                                                                                                                                                        |
| `computeIfAbsentWithSoftExpire(key, reader, hardTtlMs, softTtlMs)` | 同上，带显式硬和软 TTL 覆盖                                                                                                                                          |
| `computeIfAbsentWithSoftExpire(Collection, Function)`  | 批量软过期重载——返回 `Map<String, V>`                                                                                                                                           |
| `get(key, reader)`                                     | 从 L1 或 L2 reader 读取；每次访问触发本地 TopK 追踪 + App→Worker 上报；热点 key 提升到 L1（使用热点 TTL），普通 key 使用普通 TTL                                            |
| `get(key, reader, hardTtlMs, softTtlMs)`               | 同上，带 per-entry 硬和软 TTL 覆盖（传入 0 使用配置默认值）                                                                                                                 |
| `getWithSoftExpire(key, reader)`                       | 软失效——返回过期旧值+触发异步刷新；每次访问触发本地 TopK 追踪 + App→Worker 上报；根据 key 状态使用全局默认 TTL                                                              |
| `getWithSoftExpire(key, reader, softTtlMs)`            | 同上，带 per-call 软 TTL 覆盖（毫秒）                                                                                                                                       |
| `getWithSoftExpire(key, reader, hardTtlMs, softTtlMs)` | 同上，同时带 per-entry 硬 TTL 和 per-call 软 TTL 覆盖（毫秒）                                                                                                               |
| `read(key)` | 流式读查询构造器：`hotKey.read(key).withPrimary(...).thenExecute(...).withHardTtl(...).execute()` 返回 `Optional<T>`；`executeOrNull()` 直接返回 `T`。支持 fallback 链、广播开关、空值缓存开关 |
| `write(key)`                                           | 流式写命令构造器：`hotKey.write(key).withHardTtl(...).putThrough(value, writer)` / `.putBeforeInvalidate(mutation)` / `.invalidate()`                                         |
| `putLocal(key, value)`                                 | 仅本地写：将值存入 L1，不 bump 版本号、不广播、不触发热 key 检测、不上报；保留现有 entry 元数据                                                                            |
| `putLocal(key, value, hardTtlMs, softTtlMs)`           | 同上，带 per-entry 硬和软 TTL 覆盖（传入 0 使用配置默认值）                                                                                                                 |
| `putLocal(Map)`                                        | 批量仅本地写——存储所有条目，不 bump 版本号、不广播、不触发热 key 检测、不上报                                                                                               |
| `putThrough(key, value, writer)`                       | 写穿透：writer.run()、nextVersion()、L1 更新（根据 key 状态使用有效 TTL）、可选同步                                                                                         |
| `putThrough(key, value, writer, hardTtlMs, softTtlMs)` | 同上，带 per-entry 硬和软 TTL 覆盖（传入 0 使用配置默认值）                                                                                                                 |
| `putBeforeInvalidate(key, mutation)`                   | 先写后失效，用于集合增量操作（LPUSH、SADD、ZADD）                                                                                                                           |
| `putBeforeInvalidateAll(Map)`                          | 批量先写后失效——执行所有变异并失效对应 key，带广播                                                                                                                           |
| `isLocalHotKey(cacheKey)`                              | 检查 key 是否在 L1 中为 HOT 状态（O(1)）                                                                                                                                    |
| `areLocalHotKeys(Collection)`                          | 批量检查——返回 `Map<String, Boolean>` 的所有给定 key 的本地热点状态                                                                                                          |
| `isWorkerHotKey(cacheKey)`                             | 检查 key 是否在 Worker TopK 中为集群热点（O(n)）                                                                                                                            |
| `areWorkerHotKeys(Collection)`                         | 批量检查——返回 `Map<String, Boolean>` 的所有给定 key 的集群热点状态                                                                                                         |
| `notifyLocalDetector(cacheKey)`                        | 触发本地 HotKeyDetector 追踪指定 key，无需执行完整缓存读取。被 `@Intercept` 用于在方法体被跳过时保持 TopK 准确。                                                            |
| `notifyLocalDetector(cacheKey, count)`                 | 使用自定义增量通知本地探测器，通过缓冲计数器路由                                                                                                                             |
| `notifyLocalDetector(Map)`                             | 批量通知本地探测器，多个 key → 计数条目，通过缓冲计数器路由                                                                                                                 |
| `notifyLocalDetectorDirect(cacheKey, count)`           | 直接增加本地 TopK，绕过缓冲区和报告到 Worker 路径                                                                                                                            |
| `notifyLocalDetectorDirect(Map)`                       | 批量直接增加本地 TopK，绕过缓冲区和报告                                                                                                                                     |
| `isBlacklisted(cacheKey)`                              | 快速检查 key 是否被黑名单规则封锁                                                                                                                                           |
| `isBlacklisted(Collection)`                            | 批量黑名单检查——返回 `Map<String, Boolean>`                                                                                                                                 |
| `isWhitelisted(cacheKey)`                              | 快速检查 key 是否在白名单中（跳过 Worker 上报）                                                                                                                             |
| `isWhitelisted(Collection)`                            | 批量白名单检查——返回 `Map<String, Boolean>`                                                                                                                                 |
| `evaluateRule(cacheKey)`                               | 对所有规则评估给定 key 并返回首个匹配的动作（无匹配时返回 `ALLOW`）                                                                                                         |
| `evaluateRules(Collection)`                            | 批量规则评估——返回 `Map<String, RuleAction>`                                                                                                                                |
| `invalidate(cacheKey)`                                 | 使单个 key 在所有缓存层失效                                                                                                                                                 |
| `invalidateAllLocal()`                                 | 紧急清空——无广播地失效所有 L1 条目                                                                                                                                         |
| `invalidateAll(cacheKeys...)`                          | 可变参数重载 — 批量失效多个 key                                                                                                                                             |
| `invalidateAll(Collection)`                            | Collection 重载                                                                                                                                                             |
| `evictLocal(key)`                                      | 从本地缓存驱逐单个 key，不广播、不 bump 版本号                                                                                                                              |
| `evictLocal(Collection)`                               | 从本地缓存批量驱逐多个 key，不广播、不 bump 版本号                                                                                                                          |
| `refresh(key, reader)`                                 | 本地驱逐后通过 supplier 加载并缓存；使用默认 TTL                                                                                                                            |
| `refresh(key, reader, hardTtlMs, softTtlMs)`           | 本地驱逐后通过 supplier 加载并缓存，带显式 TTL 覆盖                                                                                                                         |
| `refreshAll(Map)`                                      | 批量刷新——本地驱逐所有 key 后通过提供的 suppliers 加载                                                                                                                      |
| `registerRefresh(key, supplier, hardTtlMs, softTtlMs)` | 注册定时后台刷新；按 `softTtlMs × 1.1` 间隔调度 `getWithSoftExpire`；替换同 key 已有注册                                                                                   |
| `updateRefresh(key, supplier, hardTtlMs, softTtlMs)`   | 运行时更新刷新配置；委托给 `registerRefresh`                                                                                                                                |
| `unregisterRefresh(key)`                               | 取消指定 key 的定时刷新                                                                                                                                                     |
| `returnLocalHotKeys()`                                 | 应用端 Top-K 快照（key + 计数）                                                                                                                                             |
| `returnLocalTopNHotKeys(n)`                            | 从本地探测器返回前 N 个热点 key，按频率排序                                                                                                                                 |
| `returnLocalExpelledHotKeys()`                         | 获取应用端被挤出的热点 key 队列；由内部定时器周期性清空                                                                                                                     |
| `returnLocalTotalDataStreams()`                        | 经过应用端 HeavyKeeper 的累计读取数                                                                                                                                         |
| `returnWorkerHotKeys()`                                | Worker 端（集群维度）Top-K 快照                                                                                                                                             |
| `returnWorkerExpelledHotKeys()`                        | Worker 端被挤出的热点 key 队列                                                                                                                                              |
| `returnWorkerTotalDataStreams()`                       | Worker 端 HeavyKeeper 累计读取数                                                                                                                                            |
| `addBlacklist(Collection)`                             | 批量添加多个 key 模式到黑名单                                                                                                                                               |
| `removeBlacklist(Collection)`                          | 批量从黑名单移除多个 key 模式                                                                                                                                               |
| `addWhitelist(Collection)`                             | 批量添加多个 key 模式到白名单                                                                                                                                               |
| `removeWhitelist(Collection)`                          | 批量从白名单移除多个 key 模式                                                                                                                                               |

### 核心配置（`zeta.local.*`）

| 属性                                    | 默认值                   | 说明                                                                                         |
| --------------------------------------- | ------------------------ | -------------------------------------------------------------------------------------------- |
| `zeta.local.top-k`                    | `100`                    | Top-K 集合大小                                                                               |
| `zeta.local.width`                    | `50000`                  | Count-Min Sketch 宽度                                                                        |
| `zeta.local.depth`                    | `5`                      | Count-Min Sketch 深度（行数）                                                                |
| `zeta.local.decay`                    | `0.92`                   | 冲突衰减因子                                                                                 |

> **设计说明：** 应用端 HeavyKeeper 使用更宽（50k）但更浅（depth 5）的 Sketch，衰减稍慢（0.92）。较宽的 Sketch 在单 key 插入时减少指纹冲突概率。较浅的深度足以满足应用端快速*启发式*本地升级判断的需要——它不做权威的 HOT/COOL 决策。参见下方 [Worker 端 HeavyKeeper](#zetaworkerheavy-keeper) 的对比配置。
| `zeta.local.min-count`                | `10`                     | 热点 key 最低计数阈值                                                                        |
| `zeta.local.sketch-window-count`      | `3`                      | 每 sketch slot 的滑动窗口数（环形缓冲区）。W=3 覆盖 3×衰减周期的数据，消除热点漂移。范围 1–10 |
| `zeta.local.cache.max-size`           | `100000`                 | Caffeine L1 最大条目数（`max-weight` 为 0 时生效）                                          |
| `zeta.local.cache.max-weight`         | `0`                      | 内存权重限制（字节）；0 = 禁用。当 >0 时替代 `max-size`，使用 `DefaultWeigher` 估算权重       |
| `zeta.local.cache.max-value-size`     | `0`                      | 单值字节大小限制；0 = 不限。超过此大小的值不会被缓存                                         |

| `zeta.local.local-cache-ttl-minutes`  | `5`                      | Caffeine L1 写入 TTL（分钟）                                                                 |
| `zeta.local.inflight-max-size`        | `50000`                  | Inflight 去重最大条目数                                                                      |
| `zeta.local.inflight-ttl-seconds`     | `5`                      | Inflight 去重 TTL（必须超过最慢 L2 响应）                                                    |
| `zeta.local.inflight-timeout-seconds` | `3`                      | Inflight 超时（必须 < inflight-ttl-seconds）。超时返回 `Optional.empty()`，调用方应回退到 DB |
| `zeta.local.executor-core-pool-size`  | `8`                      | 线程池核心大小                                                                               |
| `zeta.local.executor-max-pool-size`   | `32`                     | 线程池最大大小                                                                               |
| `zeta.local.executor-queue-capacity`  | `2000`                   | 线程池队列容量                                                                               |
| `zeta.local.expelled-queue-capacity`  | `50000`                  | 被驱逐热 key 暂存队列容量（防止 TopK 溢出）                                                  |
| `zeta.local.default-hard-ttl-ms`      | `300000`（5分钟）        | 普通 key 默认硬 TTL（Caffeine 驱逐）                                                         |
| `zeta.local.hard-ttl-ms`              | `0`                      | 普通 key 每次调用的硬 TTL 覆盖；0 = 使用 `default-hard-ttl-ms`                               |
| `zeta.local.default-hot-hard-ttl-ms`  | `3600000`（1小时）       | 热点 key 默认硬 TTL                                                                          |
| `zeta.local.hot-hard-ttl-ms`          | `0`                      | 热点 key 每次调用的硬 TTL 覆盖；0 = 使用 `default-hot-hard-ttl-ms`                           |
| `zeta.local.default-soft-ttl-ms`      | `30000`（30秒）          | 普通 key 默认软 TTL（过期后异步刷新）                                                        |
| `zeta.local.soft-ttl-ms`              | `0`                      | 普通 key 每次调用的软 TTL 覆盖；0 = 使用 `default-soft-ttl-ms`                               |
| `zeta.local.default-hot-soft-ttl-ms`  | `300000`（5分钟）        | 热点 key 默认软 TTL                                                                          |
| `zeta.local.hot-soft-ttl-ms`          | `0`                      | 热点 key 每次调用的软 TTL 覆盖；0 = 使用 `default-hot-soft-ttl-ms`                           |
| `zeta.local.null-value-ttl-seconds`   | `10`                     | null 缓存条目 TTL（秒）；避免长时间缓存负结果                                                |
| `zeta.local.ttl-jitter-ratio`         | `0.05`                   | 偏移比例（0.0–1.0）；例如 0.05 表示对 TTL 计算施加 ±5% 的随机偏移。始终启用。                 |
| `zeta.local.refresh-max-pools`        | `100`                    | 软过期最大并发异步刷新数（信号量）                                                           |
| `zeta.local.version-key-ttl-minutes`  | `60`                     | Redis 版本 key TTL（分钟），最小值为 1                                                       |
| `zeta.local.report-exchange`          | `zeta.report.exchange` | App 向 Worker 发送报告消息的 RabbitMQ 交换机                                                 |
| `zeta.local.report-interval-ms`       | `50`                     | App 实例批量发送 TopK 报告到 Worker 的时间间隔（毫秒）                                       |
| `zeta.local.app-name`                 | `"default"`              | 逻辑应用名，用于 Worker 路由的租户区分                                                       |
| `zeta.local.shard-count`              | `1`                      | 消费者线程数自动计算的除数（max(4, availableProcessors/2)；路由默认使用一致性哈希             |
| `zeta.local.instance-id`              | `""`（自动检测）         | 用于队列命名的显式实例 ID；为空时自动检测为 `server.port-HOSTNAME`（或 `server.port-UUID`）  |
| `zeta.local.queue-capacity`           | `10000`                  | 报告分发器队列容量（内部有界队列）                                                           |
| `zeta.local.queue-offer-timeout-ms`   | `100`                    | 报告队列写入超时（毫秒）——阻塞此时长后丢弃                                                   |
| `zeta.local.consumer-count`           | `0`                      | 报告消费者线程数；0 = 自动（max(4, availableProcessors / 2)）                                |
| `zeta.local.scheduler-pool-size`      | `8`                      | Zeta 共享调度器线程池大小（定时任务）                                                        |
| `zeta.local.expected-worker-count`    | `0`                      | 期望的 Worker 节点数，用于基于仲裁的健康检查；0 = 动态发现（收到首个心跳前始终不健康）          |

### 心跳配置（`zeta.local.heartbeat.*`）

| 属性                                            | 默认值                      | 说明                                                                   |
| ----------------------------------------------- | --------------------------- | ---------------------------------------------------------------------- |
| `zeta.local.heartbeat.exchange-name`          | `hotkey.heartbeat.exchange` | 心跳 Topic 交换机名称，用于 Worker 的 epoch 驱动结构化心跳             |
| `zeta.local.heartbeat.timeout-ms`             | `10000`                     | 超时（毫秒）——在此窗口内未收到心跳则判定 Worker 死亡                   |
| `zeta.local.heartbeat.verify-interval-ms`     | `5000`                      | 验证间隔（毫秒）——对怀疑死亡的 Worker 发送 Direct reply-to PING 的间隔 |
| `zeta.local.heartbeat.ping-timeout-ms`        | `3000`                      | PING/PONG 验证探针超时（毫秒）                                         |
| `zeta.local.heartbeat.degrade-after-failures` | `3`                         | 连续 PING 失败次数超过此值后降级该 Worker（按指数退避重试）            |
| `zeta.local.heartbeat.verify-max-backoff-ms`  | `600000`                    | 单 Worker 指数退避最大间隔（毫秒，10 分钟）                            |
| `zeta.local.heartbeat.min-alive-workers`      | `0`                         | 集群健康所需最小存活 Worker 数；0=使用多数派公式（knownWorkerCount/2+1）|

### 熔断器配置（`zeta.local.circuit-breaker.*`）

| 属性                                                        | 默认值   | 说明                                                                 |
| ----------------------------------------------------------- | -------- | -------------------------------------------------------------------- |
| `zeta.local.circuit-breaker.enabled`                      | `false`  | 启用滑动窗口熔断器保护远程调用（默认关闭）                           |
| `zeta.local.circuit-breaker.window-time-ms`               | `10000`  | 滑动窗口时长（毫秒）                                                 |
| `zeta.local.circuit-breaker.window-buckets`               | `10`     | 滑动窗口桶数                                                         |
| `zeta.local.circuit-breaker.fail-threshold`               | `0.5`    | 失败率阈值（0.0–1.0），超过时打开熔断器                              |
| `zeta.local.circuit-breaker.request-volume-threshold`     | `20`     | 评估失败率所需的最小请求总数                                         |
| `zeta.local.circuit-breaker.single-test-interval-ms`      | `5000`   | 半开探测间隔（毫秒）                                                 |
| `zeta.local.circuit-breaker.log-enabled`                  | `true`   | 是否记录状态切换日志（OPEN/CLOSE/HALF-OPEN）                         |

熔断器作用于 `SingleFlight.load()`——打开时 `load()` 立即返回 `Optional.empty()`，`HotKeyCache.get()` 会尝试返回过期缓存（如果 L1 中存在）。仅当缓存加载器（数据库查询、远程 API）容易出现级联故障时再启用。

### 上报配置（`zeta.report.*`）

| 属性                    | 默认值 | 说明                                                        |
| ----------------------- | ------ | ----------------------------------------------------------- |
| `zeta.report.enabled` | `true` | 启用 App 到 Worker 的报告聚合（需要 `RabbitTemplate` Bean） |

### Reporter 速率限制器（`zeta.local.reporter.*`）

| 属性                                         | 默认值  | 说明                                                                                                                                     |
| -------------------------------------------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.local.reporter.enabled`              | `true`  | 启用 BBR 自适应速率限制，作用于 Reporter 刷盘路径                                                                                        |
| `zeta.local.reporter.cpu-threshold`        | `800`   | CPU 阈值（0–1000 刻度，800 = 80%）。低于此值时限制器宽松（并发≤预算 **或** 不在冷却期即准入）；达到或超过此值时严格（仅并发≤预算才准入） |
| `zeta.local.reporter.cpu-poll-interval-ms` | `500`   | CPU 轮询间隔（ms）。守护线程以此频率调用 `com.sun.management.OperatingSystemMXBean.getCpuLoad()`                                         |
| `zeta.local.reporter.cpu-decay`            | `0.95`  | CPU 负载 EMA 平滑衰减因子（0.0–1.0）。值越高越平滑，但响应越慢                                                                           |
| `zeta.local.reporter.bbr-window-ms`        | `10000` | BBR 滑动窗口时长（ms），用于追踪最大通过率和最小往返时间                                                                                 |
| `zeta.local.reporter.bbr-window-buckets`   | `100`   | BBR 滑动窗口的桶数                                                                                                                       |
| `zeta.local.reporter.bbr-cooldown-ms`      | `1000`  | 批次丢弃后的冷却时间（ms）。冷却期内无论 CPU 状态如何，限制器拒绝所有准入                                                                |

### Worker 监听器（`zeta.worker-listener.*`）

| 属性                                              | 默认值                       | 说明                                                                                                                                                |
| ------------------------------------------------- | ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zeta.worker-listener.enabled`                  | `false`                      | **部署了 Worker 集群时必须设为 `true`**。开启心跳消费、Worker 热/冷决策监听，以及驱动 Reporter 一致性哈希路由的 ClusterHealthView                        |
| `zeta.worker-listener.exchange-name`            | `hotkey.broadcast.exchange`  | 接收 Worker HOT/COOL 决策和心跳的 FanoutExchange 名称；必须与 Worker 侧 `zeta.worker.messaging.broadcast-exchange` 一致                            |
| `zeta.worker-listener.queue-prefix`             | `hotkey.worker`              | 实例级 Worker 监听队列前缀；最终队列名 `{prefix}:{instanceId}`                                                                                      |
| `zeta.worker-listener.warmup-jitter-ms`         | `50`                         | 处理每个 Worker 决策前的随机延迟（毫秒）；分散各实例的 Redis 读取，避免惊群效应                                                                       |
| `zeta.worker-listener.concurrent-consumers`     | `2`                          | Worker 决策队列的并发消费者数                                                                                                                        |
| `zeta.worker-listener.prefetch-count`           | `5`                          | 每消费者的 AMQP 预取数                                                                                                                               |
| `zeta.worker-listener.sre.enabled`              | `true`                       | 启用 Google SRE 自适应速率限制器作用于 HOT 提升处理                                                                                                  |
| `zeta.worker-listener.sre.success-threshold`    | `0.6`                        | 成功率低于此值时概率性丢弃 HOT 提升                                                                                                                 |

> **⚠️ 重要：启动顺序** — `hotkey.heartbeat.exchange` 和 `hotkey.broadcast.exchange` 由 App（common 模块）创建。Worker 节点**必须在 App 之后启动**，否则心跳会因 `NOT_FOUND` 错误失败，集群健康环将始终为空。使用 Docker Compose 时，请为 Worker 服务添加 `depends_on: app-1: { condition: service_started }`。或者，Worker 的心跳生产者延迟首次发送 `pingIntervalMs`（默认 1000ms）以使 RabbitAdmin 有足够时间声明 exchange。

> **⚠️ 未启用时的影响：** 不设置 `worker-listener.enabled=true`，App 不消费 Worker 心跳 → `ClusterHealthView` 记录为空 → `getAliveWorkerIds()` 返回空集 → Reporter 的 `routeNode()` 返回 `null` → 所有 report 批次被**静默丢弃**。Worker 永远收不到任何数据，也永远不会广播 HOT/COOL 决策。这是部署了 Worker 集群时最常见的配置错误。

### 调度配置（`zeta.scheduling.*`，`zeta.decay-period`）

| 属性                        | 默认值 | 说明                                                                             |
| --------------------------- | ------ | -------------------------------------------------------------------------------- |
| `zeta.scheduling.enabled` | `true` | 启用内部定时器（HeavyKeeper 衰减 + 挤出队列清空）                                |
| `zeta.decay-period`       | `20`   | HeavyKeeper 衰减周期（秒），通过 `@Scheduled` 直接解析，不在 `zeta.local.*` 下 |

### 一致性哈希（`zeta.local.consistent-hashing.*`）

| 属性                                            | 默认值 | 说明                                                      |
| ----------------------------------------------- | ------ | --------------------------------------------------------- |
| `zeta.local.consistent-hashing.enabled`       | `true` | 启用一致性哈希动态 Worker 路由（默认）；设为 `false` 禁用 |
| `zeta.local.consistent-hashing.virtual-nodes` | `500`  | 每个物理 Worker 节点的虚拟节点数，用于哈希空间分布        |

### Spring Cache 集成（`zeta.spring-cache.*`）

| 属性                                 | 默认值  | 说明                                                                    |
| ------------------------------------ | ------- | ----------------------------------------------------------------------- |
| `zeta.spring-cache.enabled`        | `false` | 启用 Spring Cache 集成（将 `HotKeyCacheManager` 暴露为 CacheManager）    |
| `zeta.spring-cache.key-separator`  | `::`    | 缓存区名称与 key 之间的分隔符（例如 `"users::123"`）                   |

支持标准 `@Cacheable` / `@CachePut` / `@CacheEvict` 触发热键检测、软过期和跨实例广播。同伴注解 `@HotKeyCacheTTL`、`@Intercept`、`@Fallback` 和 `@NullCaching` 在 `@Cacheable` 上继续有效。

### 缓存同步（`zeta.sync.*`）

| 属性                               | 默认值                 | 说明                                                                             |
| ---------------------------------- | ---------------------- | -------------------------------------------------------------------------------- |
| `zeta.sync.enabled`              | `false`                | 启用跨实例缓存同步（RabbitMQ）                                                   |
| `zeta.sync.exchange-name`        | `zeta.sync.exchange` | 同步消息 Fanout 交换机名称（REFRESH / INVALIDATE / INVALIDATE_ALL / RULES_SYNC） |
| `zeta.sync.queue-prefix`         | `hotkey.sync`          | 队列名前缀；完整名称 = `{prefix}:{instanceId}`                                   |
| `zeta.sync.dedup-window-seconds` | `10`                   | 接收同步消息的去重窗口（秒）                                                     |
| `zeta.sync.dedup-max-size`       | `10000`                | 去重缓存最大条目数                                                               |
| `zeta.sync.warmup-jitter-ms`     | `50`                   | 处理同步消息前的随机 jitter（防止惊群效应）                                      |
| `zeta.sync.concurrent-consumers` | `3`                    | 同步队列 RabbitMQ 消费者并发数                                                   |
| `zeta.sync.scheduler-pool-size`  | `4`                    | 同步 jitter 延迟的线程池大小                                                     |
| `zeta.sync.prefetch-count`       | `5`                    | 每个同步消费者的 AMQP 预取数量                                                   |
| `zeta.sync.auto-startup`         | `true`                 | 同步监听器容器是否随应用自动启动                                                 |

### Worker 监听器（`zeta.worker-listener.*`）

| 属性                                           | 默认值                      | 说明                                            |
| ---------------------------------------------- | --------------------------- | ----------------------------------------------- |
| `zeta.worker-listener.enabled`               | `false`                     | 启用接收 Worker HOT/COOL 决策                   |
| `zeta.worker-listener.exchange-name`         | `hotkey.broadcast.exchange` | Worker 广播 Fanout 交换机名称                   |
| `zeta.worker-listener.queue-prefix`          | `hotkey.worker`             | 队列名前缀；完整名称 = `{prefix}:{instanceId}`  |
| `zeta.worker-listener.warmup-jitter-ms`      | `50`                        | 处理 Worker 消息前的随机 jitter（防止惊群效应） |
| `zeta.worker-listener.concurrent-consumers`  | `2`                         | Worker 监听队列 RabbitMQ 消费者并发数           |
| `zeta.worker-listener.scheduler-pool-size`   | `2`                         | Worker 监听器抖动缓存更新任务的线程池大小                                       |
| `zeta.worker-listener.prefetch-count`        | `5`                         | Worker 监听器每个消费者的 AMQP 预取数量         |
| `zeta.worker-listener.auto-startup`          | `true`                      | Worker 监听器容器是否随应用自动启动             |
| **`zeta.worker-listener.sre.*`**             |                             | **SRE 自适应速率限制器**                        |
| `zeta.worker-listener.sre.enabled`           | `true`                      | 在 HOT 决策处理路径上启用 SRE 速率限制器        |
| `zeta.worker-listener.sre.window-ms`         | `3000`                      | 速率计算的滑动窗口时长（毫秒）                  |
| `zeta.worker-listener.sre.buckets`           | `10`                        | 滑动窗口的桶数                                  |
| `zeta.worker-listener.sre.min-samples`       | `20`                        | 限流开始前的最小总样本数                        |
| `zeta.worker-listener.sre.success-threshold` | `0.6`                       | 成功率阈值（0.0–1.0）；成功率低于此值时触发限流 |

### Worker 节点（`zeta.worker.*`）

| 属性                                                                 | 默认值                      | 说明                                                                                             |
| -------------------------------------------------------------------- | --------------------------- | ------------------------------------------------------------------------------------------------ |
| `zeta.worker.enabled`                                              | `false`                     | 启用 Worker 模式（必须显式设为 true）                                                            |
| **`zeta.worker.routing.*`**                                        |                             | **路由**                                                                                         |
| `zeta.worker.routing.app-name`                                     | `"default"`                 | 逻辑应用名（租户区分）                                                                           |
| **`zeta.worker.messaging.*`**                                      |                             | **消息**                                                                                         |
| `zeta.worker.messaging.report-exchange`                            | `zeta.report.exchange`    | App 报告消息的直接交换机                                                                         |
| `zeta.worker.messaging.broadcast-exchange`                         | `hotkey.broadcast.exchange` | HOT/COOL 广播的交换机（Worker 使用路由键发布；可能需要与 worker-listener.exchange-name 对齐）    |
| `zeta.worker.messaging.heartbeat-exchange`                         | `hotkey.heartbeat.exchange` | epoch 驱动结构化心跳的 Topic 交换机（必须与 App 端 `zeta.local.heartbeat.exchange-name` 一致） |
| **`zeta.worker.report-consumer.*`**                                |                             | **上报消费者**                                                                                   |
| `zeta.worker.report-consumer.concurrent-consumers`                 | `8`                         | 上报队列的并发消费者数，最小为 1                                                                  |
| `zeta.worker.report-consumer.prefetch-count`                       | `50`                        | 每个消费者的预取数，平衡吞吐量与内存压力                                                          |
| **`zeta.worker.sliding-window.*`**                                 |                             | **滑动窗口**                                                                                     |
| `zeta.worker.sliding-window.duration-ms`                           | `500`                       | 滑动窗口时长（毫秒）                                                                                                         |
| `zeta.worker.sliding-window.slices`                                | `10`                        | 每个窗口的时间片数                                                                               |
| **`zeta.worker.threshold.*`**                                      |                             | **热点阈值**                                                                                     |
| `zeta.worker.threshold.hot-threshold`                              | `1000`                      | 绝对热点阈值；`≤0` = 使用比例阈值                                                                |
| `zeta.worker.threshold.hot-threshold-ratio`                        | `0.01`                      | 热点阈值占估计全局 QPS 的比例（1%）                                                              |
| **`zeta.worker.state-machine.*`**                                  |                             | **状态机**                                                                                       |
| `zeta.worker.state-machine.sm-duration-ms`                        | `500`                       | 状态机时间片窗口时长（毫秒），独立于滑动窗口。每片 = sm-duration-ms / sm-slices                   |
| `zeta.worker.state-machine.sm-slices`                             | `10`                        | 状态机窗口内的时间片数                                                                            |
| `zeta.worker.state-machine.confirm-duration-ms`                    | `100`                       | HOT 确认总时长。confirmCount = ceil(confirm-duration-ms / slice-ms)                              |
| `zeta.worker.state-machine.cool-duration-ms`                       | `600000`                    | key 持续低于阈值才确认 COLD 的时长                                                               |
| `zeta.worker.state-machine.pre-cool-grace-ms`                      | `60000`                     | COOL 结束时的宽限期，允许静默恢复                                                                |
| `zeta.worker.state-machine.evict-interval-ms`                      | `30000`                     | 过期状态擦除间隔（毫秒）；必须 >= cool-duration-ms \* 2                                          |
| **`zeta.worker.global-qps-dynamic-threshold.*`**                   |                             | **动态阈值（全局 QPS）**                                                                         |
| `zeta.worker.global-qps-dynamic-threshold.recalculate-interval-ms` | `60000`                     | 动态阈值重新计算的时间间隔                                                                       |
| `zeta.worker.global-qps-dynamic-threshold.qps-change-tolerance`    | `0.5`                       | 触发阈值更新的 QPS 变化容忍度（±50%）                                                            |
| `zeta.worker.global-qps-dynamic-threshold.learning-period-ms`      | `30000`                     | QPS 估算的学习周期                                                                               |
| `zeta.worker.global-qps-dynamic-threshold.hot-threshold-ratio`     | `0.01`                      | 热阈值占估计全局 QPS 的比例                                                                      |
| **`zeta.worker.topk-validation.*`**                                |                             | **TopK 验证**                                                                                    |
| `zeta.worker.topk-validation.validate-interval-ms`                 | `60000`                     | Top-K 交叉验证的运行间隔                                                                         |
| `zeta.worker.topk-validation.pre-warm-count`                       | `5`                         | 有资格预热的 Top-K 数量                                                                          |
| `zeta.worker.topk-validation.pre-warm-min-appearances`             | `2`                         | 预热前所需的最小连续 Top-K 出现次数                                                              |
| **`zeta.worker.heavy-keeper.*`**                                   |                             | **HeavyKeeper（Worker 端）**                                                                     |
| `zeta.worker.heavy-keeper.top-k`                                   | `100`                       | Worker 端 HeavyKeeper Top-K 容量                                                                 |
| `zeta.worker.heavy-keeper.width`                                   | `20000`                     | Worker 端 Count-Min Sketch 宽度                                                                  |
| `zeta.worker.heavy-keeper.depth`                                   | `10`                        | Worker 端 Count-Min Sketch 深度                                                                  |
| `zeta.worker.heavy-keeper.decay`                                   | `0.9`                       | Worker 端 HeavyKeeper 衰减因子                                                                   |
| `zeta.worker.heavy-keeper.min-count`                               | `10`                        | Worker 端最低计数阈值                                                                            |

> **设计说明：** Worker 端 HeavyKeeper 优先保证频率估计边界而不是插入速度——使用更窄（20k）但更深（depth 10）的 Sketch，衰减略快（0.9）。批量报表消费者每次喂入大量 key-count 映射（最多 10k keys），更大的深度能提供更精确的批量频率估计。更快的衰减使 Worker 能更快适应流量变化，从而做出权威的 HOT/COOL 决策。与 [应用端配置](#core-zetalocal-) 对比。
| **`zeta.worker.heartbeat.*`**                                      |                             | **心跳**                                                                                         |
| `zeta.worker.heartbeat.ping-interval-ms`                           | `1000`                      | 结构化心跳发送间隔（毫秒）                                                                       |
| **`zeta.worker.persistence.*`**                                    |                             | **TopK 持久化（热启动）**                                                                        |
| `zeta.worker.persistence.enabled`                                  | `false`                     | 启用周期性 TopK 快照到 Redis（需手动开启）                                                       |
| `zeta.worker.persistence.persist-interval-ms`                      | `30000`                     | TopK 快照间隔（毫秒）                                                                            |
| `zeta.worker.persistence.topk-count`                               | `100`                       | 每次快照保存的 top key 数量                                                                      |
| `zeta.worker.persistence.redis-key-prefix`                         | `"hotkey:topk:worker:"`     | Redis key 前缀；最终 key = prefix + appName + ":" + nodeId                                       |
| `zeta.worker.persistence.ttl-days`                                 | `3`                         | Redis 中 TopK 数据的过期时间（天）                                                               |

## 模块说明

| 模块                                                     | 依赖                                                                                   | 自动配置条件                                                                                                                                                     |
| -------------------------------------------------------- | -------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `facade`                                                 | 无                                                                                     | 始终启用                                                                                                                                                         |
| `zeta`                                                 | 无                                                                                     | 始终启用                                                                                                                                                         |
| `report`                                                 | `spring-boot-starter-amqp`                                                             | `@ConditionalOnBean(RabbitTemplate.class)` + 属性（`zeta.report.enabled`）                                                                                     |
| `spring-cache`                                            | `spring-boot-starter-cache`                                                         | `@ConditionalOnClass(AbstractValueAdaptingCache.class)` + `@ConditionalOnBean(HotKey.class)` + 属性（`zeta.spring-cache.enabled`）                                  |
| `cache`（Redis）                                         | `spring-boot-starter-data-redis`                                                       | `@ConditionalOnClass(RedisTemplate.class)` + `@ConditionalOnBean(RedisTemplate.class)`                                                                           |
| `amqp`（RabbitMQ，合并到 `HotKeyAmqpAutoConfiguration`） | `spring-boot-starter-amqp`（+ `spring-boot-starter-data-redis`，worker-listener 需要） | `@ConditionalOnClass(RabbitTemplate.class)` + 内部 `@ConditionalOnClass(RedisTemplate.class)` + 属性（`zeta.sync.enabled` / `zeta.worker-listener.enabled`） |
| `worker`                                                 | `spring-boot-starter-amqp`（+ `spring-boot-starter-data-redis`）                       | `@ConditionalOnBean(RabbitTemplate.class)` + 属性（`zeta.worker.enabled`）                                                                                     |
| `actuator`                                               | `spring-boot-starter-actuator`                                                         | `@ConditionalOnClass(Endpoint.class)`                                                                                                                            |
| `micrometer`                                             | `io.micrometer:micrometer-core`                                                        | `@ConditionalOnClass(MeterBinder.class)` — 自动注册 Caffeine 缓存指标（`zeta.l1.*`）+ 自定义 Zeta 业务指标                                                   |
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
