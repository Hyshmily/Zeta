[← 返回首页](README.zh.md)

## 配置参考

### 核心配置（`hotkey.local.*`）

| 属性                                              | 默认值            | 说明                                                                     |
| ------------------------------------------------- | ----------------- | ------------------------------------------------------------------------ |
| `hotkey.local.top-k`                              | `100`             | Top-K 集合大小                                                           |
| `hotkey.local.width`                              | `50000`           | Count-Min Sketch 宽度                                                    |
| `hotkey.local.depth`                              | `5`               | Count-Min Sketch 深度（行数）                                            |
| `hotkey.local.decay`                              | `0.92`            | 冲突衰减因子                                                             |
| `hotkey.local.min-count`                          | `10`              | 热点 key 最低计数阈值                                                    |
| `hotkey.local.local-cache-max-size`               | `1000`            | Caffeine L1 最大条目数                                                   |
| `hotkey.local.local-cache-ttl-minutes`            | `5`               | Caffeine L1 写入 TTL（分钟）                                            |
| `hotkey.local.local-cache-access-ttl-minutes`     | `0`               | Caffeine L1 访问 TTL（分钟），0 = 禁用。补充写入 TTL                     |
| `hotkey.local.inflight-max-size`                  | `50000`           | Inflight 去重最大条目数                                                  |
| `hotkey.local.inflight-ttl-seconds`               | `5`               | Inflight 去重 TTL（必须超过最慢 L2 响应）                               |
| `hotkey.local.inflight-timeout-seconds`           | `3`               | Inflight 超时（必须 < inflight-ttl-seconds）。超时返回 `Optional.empty()`，调用方应回退到 DB |
| `hotkey.local.executor-core-pool-size`            | `8`               | 线程池核心大小                                                           |
| `hotkey.local.executor-max-pool-size`             | `32`              | 线程池最大大小                                                           |
| `hotkey.local.executor-queue-capacity`            | `2000`            | 线程池队列容量                                                           |
| `hotkey.local.decay-period`                       | `20`              | （已废弃）HeavyKeeper 衰减周期（秒），仅向后兼容                         |
| `hotkey.local.default-hard-ttl-ms`                | `300000`（5分钟） | 普通 key 默认硬 TTL（Caffeine 驱逐）                                    |
| `hotkey.local.hard-ttl-ms`                        | `0`               | 普通 key 每次调用的硬 TTL 覆盖；0 = 使用 `default-hard-ttl-ms`          |
| `hotkey.local.default-hot-hard-ttl-ms`            | `3600000`（1小时）| 热点 key 默认硬 TTL                                                      |
| `hotkey.local.hot-hard-ttl-ms`                    | `0`               | 热点 key 每次调用的硬 TTL 覆盖；0 = 使用 `default-hot-hard-ttl-ms`      |
| `hotkey.local.default-soft-ttl-ms`                | `30000`（30秒）   | 普通 key 默认软 TTL（过期后异步刷新）                                    |
| `hotkey.local.soft-ttl-ms`                        | `0`               | 普通 key 每次调用的软 TTL 覆盖；0 = 使用 `default-soft-ttl-ms`          |
| `hotkey.local.default-hot-soft-ttl-ms`            | `300000`（5分钟） | 热点 key 默认软 TTL                                                      |
| `hotkey.local.hot-soft-ttl-ms`                    | `0`               | 热点 key 每次调用的软 TTL 覆盖；0 = 使用 `default-hot-soft-ttl-ms`      |
| `hotkey.local.refresh-max-pools`                  | `100`             | 软过期最大并发异步刷新数（信号量）                                       |
| `hotkey.local.version-key-ttl-minutes`            | `60`              | Redis 版本 key TTL（分钟），0 = 永不过期                                |
| `hotkey.local.report-exchange`                    | `hotkey.report.exchange` | App 向 Worker 发送报告消息的 RabbitMQ 交换机                  |
| `hotkey.local.report-interval-ms`                 | `100`             | App 实例批量发送 TopK 报告到 Worker 的时间间隔（毫秒）                   |
| `hotkey.local.app-name`                           | `"default"`       | 逻辑应用名，用于 Worker 路由的租户区分                                   |
| `hotkey.local.shard-count`                        | `1`               | Worker 侧处理的分片总数                                                  |
| `hotkey.local.instance-id`                        | `""`（自动检测）    | 用于队列命名的显式实例 ID；为空时自动检测为 `server.port-HOSTNAME`（或 `server.port-UUID`） |
| `hotkey.local.queue-capacity`                     | `10000`             | 报告分发器队列容量（每个分片内部有界队列） |
| `hotkey.local.queue-offer-timeout-ms`             | `100`               | 报告队列写入超时（毫秒）——阻塞此时长后丢弃 |
| `hotkey.local.consumer-count`                     | `0`                 | 每个分片的报告消费者线程数；0 = 自动（max(1, shardCount / 2)） |

### 上报配置（`hotkey.report.*`）

| 属性                         | 默认值    | 说明                                                |
| ---------------------------- | -------- | ---------------------------------------------------- |
| `hotkey.report.enabled`     | `true`    | 启用 App 到 Worker 的报告聚合（需要 `RabbitTemplate` Bean） |

### 调度配置（`hotkey.scheduling.*`）

| 属性                           | 默认值   | 说明                                                      |
| ------------------------------ | -------- | --------------------------------------------------------- |
| `hotkey.scheduling.enabled`   | `true`    | 启用内部定时器（HeavyKeeper 衰减 + 挤出队列清空）         |

### 注解配置（`hotkey.annotation.*`）

| 属性                           | 默认值   | 说明                                                  |
| ------------------------------ | -------- | ----------------------------------------------------- |
| `hotkey.annotation.enabled`   | `false`  | 启用 `@HotKey` 注解支持（需要 classpath 包含 `spring-boot-starter-aop`） |

### 缓存同步（`hotkey.sync.*`）

| 属性                                           | 默认值                     | 说明                                               |
| ---------------------------------------------- | -------------------------- | -------------------------------------------------- |
| `hotkey.sync.enabled`                          | `false`                    | 启用跨实例缓存同步（RabbitMQ）                     |
| `hotkey.sync.exchange-name`                    | `hotkey.sync.exchange`     | 同步消息 Fanout 交换机名称（REFRESH / INVALIDATE / INVALIDATE_ALL / RULES_SYNC） |
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

| 属性                                                                      | 默认值                      | 说明                                                       |
| ------------------------------------------------------------------------- | --------------------------- | ---------------------------------------------------------- |
| `hotkey.worker.enabled`                                                   | `false`                     | 启用 Worker 模式（必须显式设为 true）                       |
| **`hotkey.worker.routing.*`**                                             |                             | **路由**                                                   |
| `hotkey.worker.routing.app-name`                                          | `"default"`                 | 逻辑应用名（租户区分）                                     |
| `hotkey.worker.routing.shard-count`                                       | `1`                         | 分片总数                                                   |
| `hotkey.worker.routing.shard-index`                                       | `0`                         | 当前 Worker 消费的零基分片索引                             |
| **`hotkey.worker.messaging.*`**                                           |                             | **消息**                                                   |
| `hotkey.worker.messaging.report-exchange`                                 | `hotkey.report.exchange`    | App 报告消息的直接交换机                                   |
| `hotkey.worker.messaging.broadcast-exchange`                              | `hotkey.broadcast.exchange` | HOT/COOL 广播的交换机（Worker 使用路由键发布；可能需要与 worker-listener.exchange-name 对齐） |
| **`hotkey.worker.sliding-window.*`**                                      |                             | **滑动窗口**                                               |
| `hotkey.worker.sliding-window.duration-ms`                                | `1000`                      | 滑动窗口时长（毫秒）                                       |
| `hotkey.worker.sliding-window.slices`                                     | `10`                        | 每个窗口的时间片数                                         |
| **`hotkey.worker.threshold.*`**                                           |                             | **热点阈值**                                               |
| `hotkey.worker.threshold.hot-threshold`                                   | `1000`                      | 绝对热点阈值；`-1` = 使用比例阈值                          |
| `hotkey.worker.threshold.hot-threshold-ratio`                             | `0.01`                      | 热点阈值占估计全局 QPS 的比例（1%）                        |
| **`hotkey.worker.state-machine.*`**                                       |                             | **状态机**                                                 |
| `hotkey.worker.state-machine.confirm-duration-ms`                         | `2000`                      | key 持续超过阈值才确认 HOT 的时长                          |
| `hotkey.worker.state-machine.cool-duration-ms`                            | `15000`                     | key 持续低于阈值才确认 COLD 的时长                         |
| `hotkey.worker.state-machine.pre-cool-grace-ms`                           | `5000`                      | COOL 结束时的宽限期，允许静默恢复                          |
| **`hotkey.worker.global-qps-dynamic-threshold.*`**                        |                             | **动态阈值（全局 QPS）**                                   |
| `hotkey.worker.global-qps-dynamic-threshold.recalculate-interval-ms`      | `60000`                     | 动态阈值重新计算的时间间隔                                 |
| `hotkey.worker.global-qps-dynamic-threshold.qps-change-tolerance`         | `0.5`                       | 触发阈值更新的 QPS 变化容忍度（±50%）                      |
| `hotkey.worker.global-qps-dynamic-threshold.learning-period-ms`           | `30000`                     | QPS 估算的学习周期                                        |
| `hotkey.worker.global-qps-dynamic-threshold.hot-threshold-ratio`          | `0.01`                      | 热阈值占估计全局 QPS 的比例                               |
| **`hotkey.worker.topk-validation.*`**                                     |                             | **TopK 验证**                                              |
| `hotkey.worker.topk-validation.validate-interval-ms`                      | `60000`                     | Top-K 交叉验证的运行间隔                                   |
| `hotkey.worker.topk-validation.pre-warm-count`                            | `5`                         | 有资格预热的 Top-K 数量                                   |
| `hotkey.worker.topk-validation.pre-warm-min-appearances`                  | `2`                         | 预热前所需的最小连续 Top-K 出现次数                        |
| **`hotkey.worker.heavy-keeper.*`**                                        |                             | **HeavyKeeper（Worker 端）**                                |
| `hotkey.worker.heavy-keeper.top-k`                                        | `100`                       | Worker 端 HeavyKeeper Top-K 容量                           |
| `hotkey.worker.heavy-keeper.width`                                        | `20000`                     | Worker 端 Count-Min Sketch 宽度                            |
| `hotkey.worker.heavy-keeper.depth`                                        | `10`                        | Worker 端 Count-Min Sketch 深度                            |
| `hotkey.worker.heavy-keeper.decay`                                        | `0.9`                       | Worker 端 HeavyKeeper 衰减因子                             |
| `hotkey.worker.heavy-keeper.min-count`                                    | `10`                        | Worker 端最低计数阈值                                      |

## 模块说明

| 模块                    | 依赖                                                           | 自动配置条件                                            |
| ----------------------- | -------------------------------------------------------------- | ------------------------------------------------------- |
| `facade`                | 无                                                             | 始终启用                                                |
| `algorithm`             | 无                                                             | 始终启用                                                |
| `report`                | `spring-boot-starter-amqp`                                     | `@ConditionalOnBean(RabbitTemplate.class)` + 属性（`hotkey.report.enabled`） |
| `annotation`            | `spring-boot-starter-aop`                                      | `@ConditionalOnClass(Aspect.class)` + `@ConditionalOnBean(HotKey.class)` + 属性（`hotkey.annotation.enabled`） |
| `cache`（Redis）        | `spring-boot-starter-data-redis`                               | `@ConditionalOnClass(RedisTemplate.class)` + `@ConditionalOnBean(RedisTemplate.class)` |
| `sync`（RabbitMQ）      | `spring-boot-starter-amqp` + `spring-boot-starter-data-redis`  | `@ConditionalOnClass({RabbitTemplate.class, RedisTemplate.class})` + 属性（`hotkey.sync.enabled`） |
| `worker-listener`       | `spring-boot-starter-amqp` + `spring-boot-starter-data-redis`  | `@ConditionalOnClass(RabbitTemplate.class)` + `@ConditionalOnBean(RedisTemplate.class)` + 属性（`hotkey.worker-listener.enabled`） |
| `worker`                | `spring-boot-starter-amqp`（+ `spring-boot-starter-data-redis`）| `@ConditionalOnBean(RabbitTemplate.class)` + 属性（`hotkey.worker.enabled`） |
| `actuator`             | `spring-boot-starter-actuator`                                 | `@ConditionalOnClass(Endpoint.class)`                                   |
| `scheduling`           | 无                                                             | `@ConditionalOnProperty` + `@ConditionalOnBean(TopK.class)` |

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
