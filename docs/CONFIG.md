[← Back to Home](README.md)

## Configuration Reference

### Core (`hotkey.local.*`)

| Property                                          | Default          | Description                                                                                |
| ------------------------------------------------- | ---------------- | ------------------------------------------------------------------------------------------ |
| `hotkey.local.top-k`                              | `100`            | Top-K set size                                                                             |
| `hotkey.local.width`                              | `50000`          | Count-Min Sketch width                                                                     |
| `hotkey.local.depth`                              | `5`              | Count-Min Sketch depth (rows)                                                              |
| `hotkey.local.decay`                              | `0.92`           | Conflict decay factor                                                                      |
| `hotkey.local.min-count`                          | `10`             | Minimum count threshold for hot key                                                        |
| `hotkey.local.local-cache-max-size`               | `1000`           | Caffeine L1 max entries                                                                    |
| `hotkey.local.local-cache-ttl-minutes`            | `5`              | Caffeine L1 write-based TTL (minutes)                                                      |
| `hotkey.local.local-cache-access-ttl-minutes`     | `0`              | Caffeine L1 access-based TTL (minutes), 0 = disabled. Supplements write-based TTL          |
| `hotkey.local.inflight-max-size`                  | `50000`          | In-flight dedup max entries                                                                |
| `hotkey.local.inflight-ttl-seconds`               | `5`              | In-flight dedup entry TTL (must exceed slowest L2 response)                                |
| `hotkey.local.inflight-timeout-seconds`           | `3`              | In-flight load timeout (must be < inflight-ttl-seconds). On timeout returns `Optional.empty()` — caller should fallback to DB |
| `hotkey.local.executor-core-pool-size`            | `8`              | Thread pool core size                                                                      |
| `hotkey.local.executor-max-pool-size`             | `32`             | Thread pool max size                                                                       |
| `hotkey.local.executor-queue-capacity`            | `2000`           | Thread pool queue capacity                                                                 |
| `hotkey.local.decay-period`                       | `20`             | (Deprecated) HeavyKeeper decay period in seconds — backward compatibility only             |
| `hotkey.local.default-hard-ttl-ms`                | `300000` (5min)  | Default hard TTL for normal keys (Caffeine eviction)                                       |
| `hotkey.local.hard-ttl-ms`                        | `0`              | Per-call hard TTL override for normal keys; 0 = use `default-hard-ttl-ms`                  |
| `hotkey.local.default-hot-hard-ttl-ms`            | `3600000` (1h)   | Default hard TTL for hot keys                                                              |
| `hotkey.local.hot-hard-ttl-ms`                    | `0`              | Per-call hard TTL override for hot keys; 0 = use `default-hot-hard-ttl-ms`                 |
| `hotkey.local.default-soft-ttl-ms`                | `30000` (30s)    | Default soft TTL for normal keys (stale-while-revalidate)                                  |
| `hotkey.local.soft-ttl-ms`                        | `0`              | Per-call soft TTL override for normal keys; 0 = use `default-soft-ttl-ms`                  |
| `hotkey.local.default-hot-soft-ttl-ms`            | `300000` (5min)  | Default soft TTL for hot keys                                                              |
| `hotkey.local.hot-soft-ttl-ms`                    | `0`              | Per-call soft TTL override for hot keys; 0 = use `default-hot-soft-ttl-ms`                 |
| `hotkey.local.refresh-max-pools`                  | `100`            | Max concurrent async refreshes for soft expire (Semaphore)                                  |
| `hotkey.local.version-key-ttl-minutes`            | `60`             | Redis version key TTL (minutes); 0 = no expire                                             |
| `hotkey.local.report-exchange`                    | `hotkey.report.exchange` | RabbitMQ exchange for app-to-Worker report messages                               |
| `hotkey.local.report-interval-ms`                 | `100`            | Interval at which app instances batch and send TopK reports to the Worker (ms)             |
| `hotkey.local.app-name`                           | `"default"`      | Logical application name used as tenant discriminator for Worker routing                   |
| `hotkey.local.shard-count`                        | `1`              | Total number of shards for Worker-side processing                                          |
| `hotkey.local.instance-id`                        | `""` (auto)      | Explicit instance ID for queue naming; auto-detected as `server.port-HOSTNAME` (or `server.port-UUID`) if empty |
| `hotkey.local.queue-capacity`                     | `10000`          | Report dispatcher queue capacity (internal bounded queue per shard) |
| `hotkey.local.queue-offer-timeout-ms`             | `100`            | Report queue offer timeout (ms) — blocks up to this duration before dropping |
| `hotkey.local.consumer-count`                     | `0`              | Report consumer thread count per shard; 0 = auto (max(1, shardCount / 2)) |

### Reporting (`hotkey.report.*`)

| Property                        | Default | Description                                                                  |
| ------------------------------- | ------- | ---------------------------------------------------------------------------- |
| `hotkey.report.enabled`        | `true`   | Enable app-to-Worker report aggregation (requires `RabbitTemplate` bean)      |

### Scheduling (`hotkey.scheduling.*`)

| Property                          | Default | Description                                                                  |
| --------------------------------- | ------- | ---------------------------------------------------------------------------- |
| `hotkey.scheduling.enabled`      | `true`   | Enable internal scheduler for HeavyKeeper decay and expelled queue drain      |

### Annotation (`hotkey.annotation.*`)

| Property                        | Default | Description                                                                              |
| ------------------------------- | ------- | ---------------------------------------------------------------------------------------- |
| `hotkey.annotation.enabled`    | `false` | Enable `@HotKey` annotation support (requires `spring-boot-starter-aop` on classpath)     |

### Cache Sync (`hotkey.sync.*`)

| Property                                    | Default                     | Description                                                         |
| ------------------------------------------- | --------------------------- | ------------------------------------------------------------------- |
| `hotkey.sync.enabled`                       | `false`                     | Enable cross-instance cache sync via RabbitMQ                       |
| `hotkey.sync.exchange-name`                 | `hotkey.sync.exchange`      | Fanout exchange name for sync messages (INVALIDATE / REFRESH)       |
| `hotkey.sync.queue-prefix`                  | `hotkey.sync`               | Queue name prefix; full name = `{prefix}:{instanceId}`              |
| `hotkey.sync.dedup-window-seconds`          | `10`                        | Dedup window for received sync messages (seconds)                   |
| `hotkey.sync.dedup-max-size`                | `10000`                     | Dedup cache max entries                                             |
| `hotkey.sync.warmup-jitter-ms`              | `100`                       | Random jitter before processing sync messages (prevents herd)       |
| `hotkey.sync.concurrent-consumers`          | `3`                         | Number of concurrent RabbitMQ consumers for sync queue               |
| `hotkey.sync.scheduler-pool-size`           | `4`                         | Thread pool size for async sync jitter delay scheduling              |

### Worker Listener (`hotkey.worker-listener.*`)

| Property                                           | Default                     | Description                                                            |
| -------------------------------------------------- | --------------------------- | ---------------------------------------------------------------------- |
| `hotkey.worker-listener.enabled`                   | `false`                     | Enable listening for Worker HOT/COOL decisions                         |
| `hotkey.worker-listener.exchange-name`             | `hotkey.worker.exchange`    | Fanout exchange name for Worker broadcasts                             |
| `hotkey.worker-listener.queue-prefix`              | `hotkey.worker`             | Queue name prefix; full name = `{prefix}:{instanceId}`                 |
| `hotkey.worker-listener.warmup-jitter-ms`          | `100`                       | Random jitter before processing Worker messages (prevents herd)        |
| `hotkey.worker-listener.concurrent-consumers`      | `2`                         | Number of concurrent RabbitMQ consumers for Worker listener queue      |
| `hotkey.worker-listener.scheduler-pool-size`       | `2`                         | Thread pool size for deferred Redis reads in Worker listener           |

### Worker Node (`hotkey.worker.*`)

| Property                                                                  | Default                    | Description                                                          |
| ------------------------------------------------------------------------- | -------------------------- | -------------------------------------------------------------------- |
| `hotkey.worker.enabled`                                                   | `false`                    | Enable Worker mode (must explicitly set to `true`)                   |
| **`hotkey.worker.routing.*`**                                             |                            | **Routing**                                                          |
| `hotkey.worker.routing.app-name`                                          | `"default"`                | Logical application name (tenant discriminator)                      |
| `hotkey.worker.routing.shard-count`                                       | `1`                        | Total number of shards                                               |
| `hotkey.worker.routing.shard-index`                                       | `0`                        | Zero-based shard index this Worker consumes                          |
| **`hotkey.worker.messaging.*`**                                           |                            | **Messaging**                                                        |
| `hotkey.worker.messaging.report-exchange`                                 | `hotkey.report.exchange`   | Direct exchange for app report messages                              |
| `hotkey.worker.messaging.broadcast-exchange`                              | `hotkey.broadcast.exchange`| Exchange for HOT/COOL broadcasts (Worker publishes with routing keys; may need alignment with worker-listener.exchange-name) |
| **`hotkey.worker.sliding-window.*`**                                      |                            | **Sliding Window**                                                   |
| `hotkey.worker.sliding-window.duration-ms`                                | `1000`                     | Sliding window duration (milliseconds)                               |
| `hotkey.worker.sliding-window.slices`                                     | `10`                       | Number of time slices within one window                              |
| **`hotkey.worker.threshold.*`**                                           |                            | **Hot Threshold**                                                    |
| `hotkey.worker.threshold.hot-threshold`                                   | `1000`                     | Absolute hot-key threshold; `-1` = use ratio-based                   |
| `hotkey.worker.threshold.hot-threshold-ratio`                             | `0.01`                     | Hot-key threshold as fraction of estimated global QPS (1%)           |
| **`hotkey.worker.state-machine.*`**                                       |                            | **State Machine**                                                    |
| `hotkey.worker.state-machine.confirm-duration-ms`                         | `2000`                     | Duration key must stay above threshold to be confirmed HOT           |
| `hotkey.worker.state-machine.cool-duration-ms`                            | `15000`                    | Duration key must stay below threshold to be considered COLD         |
| `hotkey.worker.state-machine.pre-cool-grace-ms`                           | `5000`                     | Grace period at end of cool-down for silent revival                  |
| **`hotkey.worker.global-qps-dynamic-threshold.*`**                        |                            | **Dynamic Threshold (Global QPS)**                                    |
| `hotkey.worker.global-qps-dynamic-threshold.recalculate-interval-ms`      | `60000`                    | Interval for dynamic threshold recalculation                         |
| `hotkey.worker.global-qps-dynamic-threshold.qps-change-tolerance`         | `0.5`                      | QPS change tolerance before threshold update (±50%)                  |
| `hotkey.worker.global-qps-dynamic-threshold.learning-period-ms`           | `30000`                    | Learning period for QPS estimation                                   |
| `hotkey.worker.global-qps-dynamic-threshold.hot-threshold-ratio`          | `0.01`                     | Hot threshold as fraction of estimated global QPS                    |
| **`hotkey.worker.topk-validation.*`**                                     |                            | **TopK Validation**                                                  |
| `hotkey.worker.topk-validation.validate-interval-ms`                      | `60000`                    | Interval between Top-K cross-validation runs                         |
| `hotkey.worker.topk-validation.pre-warm-count`                            | `5`                        | Number of top-ranked keys eligible for pre-warming                   |
| `hotkey.worker.topk-validation.pre-warm-min-appearances`                  | `2`                        | Min consecutive Top-K appearances required before pre-warming        |
| **`hotkey.worker.heavy-keeper.*`**                                        |                            | **HeavyKeeper (Worker-scoped)**                                      |
| `hotkey.worker.heavy-keeper.top-k`                                        | `100`                      | Worker-side HeavyKeeper Top-K capacity                               |
| `hotkey.worker.heavy-keeper.width`                                        | `20000`                    | Worker-side Count-Min Sketch width                                   |
| `hotkey.worker.heavy-keeper.depth`                                        | `10`                       | Worker-side Count-Min Sketch depth                                   |
| `hotkey.worker.heavy-keeper.decay`                                        | `0.9`                      | Worker-side HeavyKeeper decay factor                                 |
| `hotkey.worker.heavy-keeper.min-count`                                    | `10`                       | Worker-side minimum count threshold                                  |

## Modules

| Module                 | Dependency                                                    | Auto-Config                                            |
| ---------------------- | ------------------------------------------------------------- | ------------------------------------------------------- |
| `facade`               | none                                                          | always                                                 |
| `algorithm`            | none                                                          | always                                                 |
| `report`               | `spring-boot-starter-amqp`                                    | `@ConditionalOnBean(RabbitTemplate.class)` + property (`hotkey.report.enabled`) |
| `annotation`           | `spring-boot-starter-aop`                                     | `@ConditionalOnClass(Aspect.class)` + `@ConditionalOnBean(HotKey.class)` + property (`hotkey.annotation.enabled`) |
| `cache` (Redis)        | `spring-boot-starter-data-redis`                              | `@ConditionalOnClass(RedisTemplate.class)` + `@ConditionalOnBean(RedisTemplate.class)` |
| `sync` (RabbitMQ)      | `spring-boot-starter-amqp` + `spring-boot-starter-data-redis` | `@ConditionalOnClass({RabbitTemplate.class, RedisTemplate.class})` + property (`hotkey.sync.enabled`) |
| `worker-listener`      | `spring-boot-starter-amqp` + `spring-boot-starter-data-redis` | `@ConditionalOnClass(RabbitTemplate.class)` + `@ConditionalOnBean(RedisTemplate.class)` + property (`hotkey.worker-listener.enabled`) |
| `worker`               | `spring-boot-starter-amqp` (+ `spring-boot-starter-data-redis`) | `@ConditionalOnBean(RabbitTemplate.class)` + property (`hotkey.worker.enabled`) |
| `actuator`             | `spring-boot-starter-actuator`                                | `@ConditionalOnClass(Endpoint.class)`                                  |
| `scheduling`           | none                                                          | `@ConditionalOnProperty` + `@ConditionalOnBean(TopK.class)` |

## Security

All RabbitMQ-based exchanges (`sync`, `report`, `worker/broadcast`) use plain AMQP connections by default. In production, configure TLS via Spring Boot's `spring.rabbitmq.ssl.*`:

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

See [Spring Boot RabbitMQ SSL docs](https://docs.spring.io/spring-boot/reference/messaging/amqp.html#page-title) for details.
