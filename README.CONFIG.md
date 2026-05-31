[← Back to Home](README.md)

## Configuration Reference

### Core (`hotkey.*`)

| Property                                | Default          | Description                                                                                |
| --------------------------------------- | ---------------- | ------------------------------------------------------------------------------------------ |
| `hotkey.top-k`                          | `100`            | Top-K set size                                                                             |
| `hotkey.width`                          | `50000`          | Count-Min Sketch width                                                                     |
| `hotkey.depth`                          | `5`              | Count-Min Sketch depth (rows)                                                              |
| `hotkey.decay`                          | `0.92`           | Conflict decay factor                                                                      |
| `hotkey.min-count`                      | `10`             | Minimum count threshold for hot key                                                        |
| `hotkey.local-cache-max-size`           | `1000`           | Caffeine L1 max entries                                                                    |
| `hotkey.local-cache-ttl-minutes`        | `5`              | Caffeine L1 write-based TTL (minutes)                                                      |
| `hotkey.local-cache-access-ttl-minutes` | `0`              | Caffeine L1 access-based TTL (minutes), 0 = disabled. Supplements write-based TTL          |
| `hotkey.inflight-max-size`              | `50000`          | In-flight dedup max entries                                                                |
| `hotkey.inflight-ttl-seconds`           | `5`              | In-flight dedup entry TTL (must exceed slowest L2 response)                                |
| `hotkey.inflight-timeout-seconds`       | `3`              | In-flight load timeout (must be < inflight-ttl-seconds). On timeout returns `Optional.empty()` — caller should fallback to DB |
| `hotkey.executor-core-pool-size`        | `8`              | Thread pool core size                                                                      |
| `hotkey.executor-max-pool-size`         | `32`             | Thread pool max size                                                                       |
| `hotkey.executor-queue-capacity`        | `2000`           | Thread pool queue capacity                                                                 |
| `hotkey.decay-period`                   | `20`             | (Deprecated) HeavyKeeper decay period in seconds — backward compatibility only             |
| `hotkey.default-hard-ttl-ms`            | `300000` (5min)  | Default hard TTL for normal keys (Caffeine eviction)                                       |
| `hotkey.hard-ttl-ms`                    | `0`              | Per-call hard TTL override for normal keys; 0 = use `default-hard-ttl-ms`                  |
| `hotkey.default-hot-hard-ttl-ms`        | `3600000` (1h)   | Default hard TTL for hot keys                                                              |
| `hotkey.hot-hard-ttl-ms`                | `0`              | Per-call hard TTL override for hot keys; 0 = use `default-hot-hard-ttl-ms`                 |
| `hotkey.default-soft-ttl-ms`            | `30000` (30s)    | Default soft TTL for normal keys (stale-while-revalidate)                                  |
| `hotkey.soft-ttl-ms`                    | `0`              | Per-call soft TTL override for normal keys; 0 = use `default-soft-ttl-ms`                  |
| `hotkey.default-hot-soft-ttl-ms`        | `300000` (5min)  | Default soft TTL for hot keys                                                              |
| `hotkey.hot-soft-ttl-ms`                | `0`              | Per-call soft TTL override for hot keys; 0 = use `default-hot-soft-ttl-ms`                 |
| `hotkey.refresh-max-pools`              | `100`            | Max concurrent async refreshes for soft expire (Semaphore)                                  |
| `hotkey.version-key-ttl-minutes`        | `60`             | Redis version key TTL (minutes); 0 = no expire                                             |
| `hotkey.report-exchange`                | `hotkey.report.exchange` | RabbitMQ exchange for app-to-Worker report messages                               |
| `hotkey.report-interval-ms`             | `100`            | Interval at which app instances batch and send TopK reports to the Worker (ms)             |
| `hotkey.report.enabled`                 | `true`           | Enable app-to-Worker report aggregation (requires RabbitTemplate)                           |
| `hotkey.app-name`                       | `"default"`      | Logical application name used as tenant discriminator for Worker routing                   |
| `hotkey.shard-count`                    | `1`              | Total number of shards for Worker-side processing                                          |
| `hotkey.scheduling.enabled`             | `true`           | Enable internal scheduler for HeavyKeeper decay and expelled queue drain                   |
| `hotkey.instance-id`                    | `""` (auto)      | Explicit instance ID for queue naming; auto-detected as `server.port-HOSTNAME` (or `server.port-UUID`) if empty |

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

| Property                                        | Default         | Description                                                       |
| ----------------------------------------------- | --------------- | ----------------------------------------------------------------- |
| `hotkey.worker.enabled`                         | `false`         | Enable Worker mode                                                |
| `hotkey.worker.exclusive-mode`                  | `true`          | Exclude App-side beans when Worker is active; set `false` for coexistence |
| `hotkey.worker.app-name`                        | `"default"`     | Logical application name (tenant discriminator)                   |
| `hotkey.worker.report-exchange`                 | `hotkey.report.exchange` | Direct exchange for app report messages                 |
| `hotkey.worker.broadcast-exchange`              | `hotkey.broadcast.exchange` | Fanout exchange for HOT/COOL broadcasts (may need alignment with worker-listener.exchange-name) |
| `hotkey.worker.shard-count`                     | `1`             | Total number of shards                                           |
| `hotkey.worker.shard-index`                     | `0`             | Zero-based shard index this Worker consumes                      |
| `hotkey.worker.window-duration-ms`              | `1000`          | Sliding window duration (milliseconds)                           |
| `hotkey.worker.window-slices`                   | `10`            | Number of time slices within one window                          |
| `hotkey.worker.hot-threshold`                   | `-1`            | Absolute hot-key threshold; `-1` = use ratio-based               |
| `hotkey.worker.hot-threshold-ratio`             | `0.01`          | Hot-key threshold as fraction of estimated global QPS (1%)       |
| `hotkey.worker.confirm-duration-ms`             | `2000`          | Duration key must stay above threshold to be confirmed HOT       |
| `hotkey.worker.cool-duration-ms`                | `15000`         | Duration key must stay below threshold to be considered COLD     |
| `hotkey.worker.pre-cool-grace-ms`               | `5000`          | Grace period at end of cool-down for silent revival              |
| `hotkey.worker.recalculate-interval-ms`         | `60000`         | Interval for dynamic threshold recalculation                     |
| `hotkey.worker.qps-change-tolerance`            | `0.5`           | QPS change tolerance before threshold update (±50%)              |
| `hotkey.worker.topk-validate-interval-ms`       | `60000`         | Interval between Top-K cross-validation runs                     |
| `hotkey.worker.topk-pre-warm-count`             | `5`             | Number of top-ranked keys eligible for pre-warming               |
| `hotkey.worker.topk-pre-warm-min-appearances`   | `2`             | Min consecutive Top-K appearances required before pre-warming    |
| `hotkey.worker.worker-top-k`                    | `100`           | Worker-side HeavyKeeper Top-K capacity                           |
| `hotkey.worker.worker-width`                    | `20000`         | Worker-side Count-Min Sketch width                               |
| `hotkey.worker.worker-depth`                    | `10`            | Worker-side Count-Min Sketch depth                               |
| `hotkey.worker.worker-decay`                    | `0.9`           | Worker-side HeavyKeeper decay factor                             |
| `hotkey.worker.worker-min-count`                | `10`            | Worker-side minimum count threshold                              |

## Modules

| Module                 | Dependency                                                    | Auto-Config                                            |
| ---------------------- | ------------------------------------------------------------- | ------------------------------------------------------ |
| `algorithm`            | none                                                          | always                                                 |
| `cache` (Redis)        | `spring-boot-starter-data-redis`                              | `@ConditionalOnClass` + `@ConditionalOnBean(StringRedisTemplate.class)` |
| `sync` (RabbitMQ)      | `spring-boot-starter-amqp` + `spring-boot-starter-data-redis` | `@ConditionalOnClass` + property (`hotkey.sync.enabled`) |
| `worker-listener`      | `spring-boot-starter-amqp` + `spring-boot-starter-data-redis` | `@ConditionalOnClass` + property (`hotkey.worker-listener.enabled`) |
| `worker`               | `spring-boot-starter-amqp` (+ `spring-boot-starter-data-redis`) | `@ConditionalOnClass` + property (`hotkey.worker.enabled`) |
| `actuator`             | `spring-boot-starter-actuator`                                | `@ConditionalOnClass`                                  |
| `scheduling`           | none                                                          | `@ConditionalOnProperty` + `@ConditionalOnBean(TopK.class)` |
