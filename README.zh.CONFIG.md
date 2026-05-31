[← 返回首页](README.zh.md)

## 配置参考

### 核心配置（`hotkey.*`）

| 属性                                        | 默认值            | 说明                                                                     |
| ------------------------------------------- | ----------------- | ------------------------------------------------------------------------ |
| `hotkey.top-k`                              | `100`             | Top-K 集合大小                                                           |
| `hotkey.width`                              | `50000`           | Count-Min Sketch 宽度                                                    |
| `hotkey.depth`                              | `5`               | Count-Min Sketch 深度（行数）                                            |
| `hotkey.decay`                              | `0.92`            | 冲突衰减因子                                                             |
| `hotkey.min-count`                          | `10`              | 热点 key 最低计数阈值                                                    |
| `hotkey.local-cache-max-size`               | `1000`            | Caffeine L1 最大条目数                                                   |
| `hotkey.local-cache-ttl-minutes`            | `5`               | Caffeine L1 写入 TTL（分钟）                                            |
| `hotkey.local-cache-access-ttl-minutes`     | `0`               | Caffeine L1 访问 TTL（分钟），0 = 禁用。补充写入 TTL                     |
| `hotkey.inflight-max-size`                  | `50000`           | Inflight 去重最大条目数                                                  |
| `hotkey.inflight-ttl-seconds`               | `5`               | Inflight 去重 TTL（必须超过最慢 L2 响应）                               |
| `hotkey.inflight-timeout-seconds`           | `3`               | Inflight 超时（必须 < inflight-ttl-seconds）。超时返回 `Optional.empty()`，调用方应回退到 DB |
| `hotkey.executor-core-pool-size`            | `8`               | 线程池核心大小                                                           |
| `hotkey.executor-max-pool-size`             | `32`              | 线程池最大大小                                                           |
| `hotkey.executor-queue-capacity`            | `2000`            | 线程池队列容量                                                           |
| `hotkey.decay-period`                       | `20`              | （已废弃）HeavyKeeper 衰减周期（秒），仅向后兼容                         |
| `hotkey.default-hard-ttl-ms`                | `300000`（5分钟） | 普通 key 默认硬 TTL（Caffeine 驱逐）                                    |
| `hotkey.hard-ttl-ms`                        | `0`               | 普通 key 每次调用的硬 TTL 覆盖；0 = 使用 `default-hard-ttl-ms`          |
| `hotkey.default-hot-hard-ttl-ms`            | `3600000`（1小时）| 热点 key 默认硬 TTL                                                      |
| `hotkey.hot-hard-ttl-ms`                    | `0`               | 热点 key 每次调用的硬 TTL 覆盖；0 = 使用 `default-hot-hard-ttl-ms`      |
| `hotkey.default-soft-ttl-ms`                | `30000`（30秒）   | 普通 key 默认软 TTL（过期后异步刷新）                                    |
| `hotkey.soft-ttl-ms`                        | `0`               | 普通 key 每次调用的软 TTL 覆盖；0 = 使用 `default-soft-ttl-ms`          |
| `hotkey.default-hot-soft-ttl-ms`            | `300000`（5分钟） | 热点 key 默认软 TTL                                                      |
| `hotkey.hot-soft-ttl-ms`                    | `0`               | 热点 key 每次调用的软 TTL 覆盖；0 = 使用 `default-hot-soft-ttl-ms`      |
| `hotkey.refresh-max-pools`                  | `100`             | 软过期最大并发异步刷新数（信号量）                                       |
| `hotkey.version-key-ttl-minutes`            | `60`              | Redis 版本 key TTL（分钟），0 = 永不过期                                |
| `hotkey.report-exchange`                    | `hotkey.report.exchange` | App 向 Worker 发送报告消息的 RabbitMQ 交换机                  |
| `hotkey.report-interval-ms`                 | `100`             | App 实例批量发送 TopK 报告到 Worker 的时间间隔（毫秒）                   |
| `hotkey.report.enabled`                     | `true`            | 启用 App 到 Worker 的报告聚合（需要 RabbitTemplate）                      |
| `hotkey.app-name`                           | `"default"`       | 逻辑应用名，用于 Worker 路由的租户区分                                   |
| `hotkey.shard-count`                        | `1`               | Worker 侧处理的分片总数                                                  |
| `hotkey.scheduling.enabled`                 | `true`            | 启用内部定时器（HeavyKeeper 衰减 + 挤出队列清空）                       |
| `hotkey.instance-id`                        | `""`（自动检测）    | 用于队列命名的显式实例 ID；为空时自动检测为 `server.port-HOSTNAME`（或 `server.port-UUID`） |

### 缓存同步（`hotkey.sync.*`）

| 属性                                           | 默认值                     | 说明                                               |
| ---------------------------------------------- | -------------------------- | -------------------------------------------------- |
| `hotkey.sync.enabled`                          | `false`                    | 启用跨实例缓存同步（RabbitMQ）                     |
| `hotkey.sync.exchange-name`                    | `hotkey.sync.exchange`     | 同步消息 Fanout 交换机名称（INVALIDATE / REFRESH） |
| `hotkey.sync.queue-prefix`                     | `hotkey.sync`              | 队列名前缀；完整名称 = `{prefix}:{instanceId}`     |
| `hotkey.sync.dedup-window-seconds`             | `10`                       | 接收同步消息的去重窗口（秒）                       |
| `hotkey.sync.dedup-max-size`                   | `10000`                    | 去重缓存最大条目数                                  |
| `hotkey.sync.warmup-jitter-ms`                 | `100`                      | 处理同步消息前的随机 jitter（防止惊群效应）        |
| `hotkey.sync.concurrent-consumers`             | `3`                        | 同步队列 RabbitMQ 消费者并发数                      |
| `hotkey.sync.scheduler-pool-size`              | `4`                        | 同步 jitter 延迟的线程池大小                        |

### Worker 监听器（`hotkey.worker-listener.*`）

| 属性                                                  | 默认值                     | 说明                                                   |
| ----------------------------------------------------- | -------------------------- | ------------------------------------------------------ |
| `hotkey.worker-listener.enabled`                      | `false`                    | 启用接收 Worker HOT/COOL 决策                          |
| `hotkey.worker-listener.exchange-name`                | `hotkey.worker.exchange`   | Worker 广播 Fanout 交换机名称                           |
| `hotkey.worker-listener.queue-prefix`                 | `hotkey.worker`            | 队列名前缀；完整名称 = `{prefix}:{instanceId}`         |
| `hotkey.worker-listener.warmup-jitter-ms`             | `100`                      | 处理 Worker 消息前的随机 jitter（防止惊群效应）         |
| `hotkey.worker-listener.concurrent-consumers`         | `2`                        | Worker 监听队列 RabbitMQ 消费者并发数                    |
| `hotkey.worker-listener.scheduler-pool-size`          | `2`                        | Worker 监听器延迟 Redis 读取的线程池大小                 |

### Worker 节点（`hotkey.worker.*`）

| 属性                                                | 默认值             | 说明                                                     |
| --------------------------------------------------- | ------------------ | -------------------------------------------------------- |
| `hotkey.worker.enabled`                             | `false`            | 启用 Worker 模式                                         |
| `hotkey.worker.exclusive-mode`                      | `true`             | 启用时排除 App 端 Bean；设为 `false` 可共存               |
| `hotkey.worker.app-name`                            | `"default"`        | 逻辑应用名（租户区分）                                   |
| `hotkey.worker.report-exchange`                     | `hotkey.report.exchange` | App 报告消息的直接交换机                         |
| `hotkey.worker.broadcast-exchange`                  | `hotkey.broadcast.exchange` | HOT/COOL 广播的 Fanout 交换机（可能需要与 worker-listener.exchange-name 对齐） |
| `hotkey.worker.shard-count`                         | `1`                | 分片总数                                               |
| `hotkey.worker.shard-index`                         | `0`                | 当前 Worker 消费的零基分片索引                         |
| `hotkey.worker.window-duration-ms`                  | `1000`             | 滑动窗口时长（毫秒）                                   |
| `hotkey.worker.window-slices`                       | `10`               | 每个窗口的时间片数                                     |
| `hotkey.worker.hot-threshold`                       | `-1`               | 绝对热点阈值；`-1` = 使用比例阈值                      |
| `hotkey.worker.hot-threshold-ratio`                 | `0.01`             | 热点阈值占估计全局 QPS 的比例（1%）                    |
| `hotkey.worker.confirm-duration-ms`                 | `2000`             | key 持续超过阈值才确认 HOT 的时长                      |
| `hotkey.worker.cool-duration-ms`                    | `15000`            | key 持续低于阈值才确认 COLD 的时长                     |
| `hotkey.worker.pre-cool-grace-ms`                   | `5000`             | COOL 结束时的宽限期，允许静默恢复                      |
| `hotkey.worker.recalculate-interval-ms`             | `60000`            | 动态阈值重新计算的时间间隔                             |
| `hotkey.worker.qps-change-tolerance`                | `0.5`              | 触发阈值更新的 QPS 变化容忍度（±50%）                  |
| `hotkey.worker.topk-validate-interval-ms`           | `60000`            | Top-K 交叉验证的运行间隔                               |
| `hotkey.worker.topk-pre-warm-count`                 | `5`                | 有资格预热的 Top-K 数量                               |
| `hotkey.worker.topk-pre-warm-min-appearances`       | `2`                | 预热前所需的最小连续 Top-K 出现次数                    |
| `hotkey.worker.worker-top-k`                        | `100`              | Worker 端 HeavyKeeper Top-K 容量                       |
| `hotkey.worker.worker-width`                        | `20000`            | Worker 端 Count-Min Sketch 宽度                        |
| `hotkey.worker.worker-depth`                        | `10`               | Worker 端 Count-Min Sketch 深度                        |
| `hotkey.worker.worker-decay`                        | `0.9`              | Worker 端 HeavyKeeper 衰减因子                         |
| `hotkey.worker.worker-min-count`                    | `10`               | Worker 端最低计数阈值                                  |

## 模块说明

| 模块                    | 依赖                                                           | 自动配置条件                                            |
| ----------------------- | -------------------------------------------------------------- | ------------------------------------------------------- |
| `algorithm`             | 无                                                             | 始终启用                                                |
| `cache`（Redis）        | `spring-boot-starter-data-redis`                               | `@ConditionalOnClass` + `@ConditionalOnBean(StringRedisTemplate.class)` |
| `sync`（RabbitMQ）      | `spring-boot-starter-amqp` + `spring-boot-starter-data-redis`  | `@ConditionalOnClass` + 属性（`hotkey.sync.enabled`）   |
| `worker-listener`       | `spring-boot-starter-amqp` + `spring-boot-starter-data-redis`  | `@ConditionalOnClass` + 属性（`hotkey.worker-listener.enabled`） |
| `worker`                | `spring-boot-starter-amqp`（+ `spring-boot-starter-data-redis`）| `@ConditionalOnClass` + 属性（`hotkey.worker.enabled`） |
| `actuator`              | `spring-boot-starter-actuator`                                 | `@ConditionalOnClass`                                   |
| `scheduling`            | 无                                                             | `@ConditionalOnProperty` + `@ConditionalOnBean(TopK.class)` |
