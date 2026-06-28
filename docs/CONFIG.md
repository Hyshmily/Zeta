[← Back to Home](README.md)

## Configuration Reference

| Method                                                 | Description                                                                                                                                                                                                                                    |
| ------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `peek(key)`                                            | L1 lookup only, no frequency tracking, no L2 read, no reporting                                                                                                                                                                                |
| `peekAll(Collection)`                                  | Batch peek — returns `Map<String, Object>` of present key-value pairs; missing keys silently omitted                                                                                                                                           |
| `getLocalCache()`                                      | Exposes the raw Caffeine `Cache<String, Object>` for Caffeine-specific operations (asMap, policy, cleanUp). &#9888;&#65039; Bypasses HotKey orchestration — version tracking, broadcast, and expiry management are all skipped. Local L1 only. |
| `estimatedSize()`                                      | Estimated number of entries currently in the L1 cache (best-effort)                                                                                                                                                                            |
| `stats()`                                              | L1 cache statistics snapshot: hit count, miss count, hit rate, eviction count, estimated size                                                                                                                                                  |
| `computeIfAbsent(key, reader)`                         | Convenience shorthand for `get(key, reader).orElse(null)`; loads via supplier on cache miss, returns null when loader returns null                                              |
| `computeIfAbsent(key, reader, hardTtlMs)`              | Same with explicit hard TTL override                                                                                                                                               |
| `computeIfAbsent(key, reader, hardTtlMs, softTtlMs)`   | Same with explicit hard and soft TTL overrides                                                                                                                                     |
| `computeIfAbsent(Collection, Function)`                | Batch overload — iterates over keys and returns `Map<String, V>` of loaded values                                                                                                 |
| `computeIfAbsentWithSoftExpire(key, reader)`           | Convenience shorthand for `getWithSoftExpire(key, reader).orElse(null)`                                                                                                            |
| `computeIfAbsentWithSoftExpire(key, reader, softTtlMs)` | Same with explicit soft TTL override                                                                                                                                              |
| `computeIfAbsentWithSoftExpire(key, reader, hardTtlMs, softTtlMs)` | Same with explicit hard and soft TTL overrides                                                                                                                         |
| `computeIfAbsentWithSoftExpire(Collection, Function)`  | Batch soft-expire overload — returns `Map<String, V>`                                                                                                                              |
| `get(key, reader)`                                     | Read from L1 or L2 reader; every access triggers local TopK tracking + App&#8594;Worker reporting; hot keys promoted to L1 (with hot TTL), normal keys use normal TTL              |
| `get(key, reader, hardTtlMs, softTtlMs)`               | Same as above, with per-entry hard and soft TTL override (pass 0 to use default)                                                                                                                                                               |
| `getWithSoftExpire(key, reader)`                       | Soft expiry — returns stale data + triggers async refresh; every access triggers local TopK tracking + App&#8594;Worker reporting; uses global default TTLs based on key state                                                                 |
| `getWithSoftExpire(key, reader, softTtlMs)`            | Same as above, with per-call soft TTL override (ms)                                                                                                                                                                                            |
| `getWithSoftExpire(key, reader, hardTtlMs, softTtlMs)` | Same as above, with both per-entry hard TTL and per-call soft TTL override (ms)                                                                                                                                                                |
| `read(key)`                                            | Fluent read query builder: `hotKey.read(key).withPrimary(...).thenExecute(...).withHardTtl(...).execute()` returns `Optional<T>`; supports fallback chain, broadcast toggle, null-caching toggle                                                                       |
| `write(key)`                                           | Fluent write command builder: `hotKey.write(key).withHardTtl(...).putThrough(value, writer)` / `.putBeforeInvalidate(mutation)` / `.invalidate()`                                                                                              |
| `putLocal(key, value)`                                 | Local-only write: stores value in L1 without version bump, broadcast, hot-key detection, or reporting; preserves existing entry metadata                                                                                                      |
| `putLocal(key, value, hardTtlMs, softTtlMs)`           | Same as above, with per-entry hard and soft TTL override (pass 0 to use default)                                                                                                                                                               |
| `putLocal(Map)`                                        | Batch local-only write — stores all entries without version bump, broadcast, hot-key detection, or reporting                                                                                                                                   |
| `putThrough(key, value, writer)`                       | Write-through: writer.run(), nextVersion(), L1 update (with effective TTL based on key state), optional sync                                                                                                                                   |
| `putThrough(key, value, writer, hardTtlMs, softTtlMs)` | Same as above, with per-entry hard and soft TTL override (pass 0 to use default)                                                                                                                                                               |
| `putBeforeInvalidate(key, mutation)`                   | Write then invalidate, for incremental collection operations (LPUSH, SADD, ZADD)                                                                                                                                                               |
| `putBeforeInvalidateAll(Map)`                          | Batch write-then-invalidate — executes all mutations and invalidates corresponding keys with broadcast                                                                                                                                          |
| `isLocalHotKey(cacheKey)`                              | Check if key is HOT in L1 (O(1))                                                                                                                                                                                                               |
| `areLocalHotKeys(Collection)`                          | Batch check — returns `Map<String, Boolean>` of local hot key status for all given keys                                                                                                                                                        |
| `isWorkerHotKey(cacheKey)`                             | Check if key is a cluster hot key in Worker TopK (O(n))                                                                                                                                                                                        |
| `areWorkerHotKeys(Collection)`                         | Batch check — returns `Map<String, Boolean>` of cluster-wide hot key status for all given keys                                                                                                                                                 |
| `notifyLocalDetector(cacheKey)`                        | Triggers local HotKeyDetector tracking for a key without performing a full cache read. Used by `@Intercept` to keep TopK accurate when the method body is skipped.                                                                             |
| `notifyLocalDetector(cacheKey, count)`                 | Notify local detector with a custom delta, routing through the buffered counter                                                                                                                                                                |
| `notifyLocalDetector(Map)`                             | Batch-notify local detector with multiple key → count entries, routing through the buffered counter                                                                                                                                            |
| `notifyLocalDetectorDirect(cacheKey, count)`           | Increment the local TopK directly, bypassing buffer and report-to-Worker path                                                                                                                                                                  |
| `notifyLocalDetectorDirect(Map)`                       | Batch-increment the local TopK directly, bypassing buffer and reports                                                                                                                                                                          |
| `isBlacklisted(cacheKey)`                              | Quickly check whether a key is blocked by any blacklist rule                                                                                                                                                                                   |
| `isBlacklisted(Collection)`                            | Batch blacklist check — returns `Map<String, Boolean>`                                                                                                                                                                                         |
| `isWhitelisted(cacheKey)`                              | Quickly check whether a key is whitelisted (skips Worker reporting)                                                                                                                                                                            |
| `isWhitelisted(Collection)`                            | Batch whitelist check — returns `Map<String, Boolean>`                                                                                                                                                                                         |
| `evaluateRule(cacheKey)`                               | Evaluate all rules against the given key and return the first matching action (or `ALLOW` if no rule matches)                                                                                                                                  |
| `evaluateRules(Collection)`                            | Batch rule evaluation — returns `Map<String, RuleAction>`                                                                                                                                                                                      |
| `invalidate(cacheKey)`                                 | Invalidate a single key across all cache layers                                                                                                                                                                                                |
| `invalidateAllLocal()`                                 | Emergency flush — invalidate all L1 entries without broadcasting                                                                                                               |
| `invalidateAll(cacheKeys...)`                          | Varargs overload — batch invalidate multiple keys                                                                                                                                                                                              |
| `invalidateAll(Collection)`                            | Collection overload                                                                                                                                                                                                                            |
| `evictLocal(key)`                                      | Evict a single key from local cache without broadcasting and without bumping version numbers                                                                                                                                                   |
| `evictLocal(Collection)`                               | Evict multiple keys from local cache without broadcasting and without bumping version numbers                                                                                                                                                  |
| `refresh(key, reader)`                                 | Evict locally then load and cache via the supplier; uses default TTLs                                                                                                                                                                          |
| `refresh(key, reader, hardTtlMs, softTtlMs)`           | Evict locally then load and cache with explicit TTL overrides                                                                                                                                                                                  |
| `refreshAll(Map)`                                      | Batch refresh — evicts all keys locally then loads via provided suppliers                                                                                                                                                                      |
| `tryLock(key, expire, unit)`                           | Acquire a distributed lock with default retry counts; returns `AutoReleaseLock` or `null` if failed                                                                             |
| `tryLock(key, expire, unit, lockCount, inquiryCount, unlockCount)` | Same with explicit retry counts (negative values fall back to configured defaults)                                                                                |
| `tryLockAndRun(key, expire, unit, action)`              | Convenience — acquire lock, run action, release; returns `true` if lock acquired and action ran                                                                                 |
| `tryLockAndRun(key, expire, unit, action, lockCount, inquiryCount, unlockCount)` | Same with explicit retry counts                                                                                                                             |
| `returnLocalHotKeys()`                                 | App-side Top-K snapshot (key + count)                                                                                                                                                                                                          |
| `returnLocalTopNHotKeys(n)`                            | Return top N hot keys from the local detector, ordered by frequency                                                                                                                                                                            |
| `returnLocalExpelledHotKeys()`                         | Get app-side expelled hot key queue; periodically drained by internal timer                                                                                                                                                                    |
| `returnLocalTotalDataStreams()`                        | Cumulative reads through app-side HeavyKeeper                                                                                                                                                                                                  |
| `returnWorkerHotKeys()`                                | Worker-side (cluster-level) Top-K snapshot                                                                                                                                                                                                     |
| `returnWorkerExpelledHotKeys()`                        | Worker-side expelled hot key queue                                                                                                                                                                                                             |
| `returnWorkerTotalDataStreams()`                       | Worker-side HeavyKeeper cumulative reads                                                                                                                                                                                                       |
| `addBlacklist(Collection)`                             | Add multiple key patterns to the blacklist                                                                                                                                                                                                     |
| `removeBlacklist(Collection)`                          | Remove multiple key patterns from the blacklist                                                                                                                                                                                                |
| `addWhitelist(Collection)`                             | Add multiple key patterns to the whitelist                                                                                                                                                                                                     |
| `removeWhitelist(Collection)`                          | Remove multiple key patterns from the whitelist                                                                                                                                                                                                |

### Core (`hotkey.local.*`)

| Property                                | Default                  | Description                                                                                                                   |
| --------------------------------------- | ------------------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| `hotkey.local.top-k`                    | `100`                    | Top-K set size                                                                                                                |
| `hotkey.local.width`                    | `50000`                  | Count-Min Sketch width                                                                                                        |
| `hotkey.local.depth`                    | `5`                      | Count-Min Sketch depth (rows)                                                                                                 |
| `hotkey.local.decay`                    | `0.92`                   | Conflict decay factor                                                                                                         |
| `hotkey.local.min-count`                | `10`                     | Minimum count threshold for hot key                                                                                           |
| `hotkey.local.local-cache-max-size`     | `1000`                   | Caffeine L1 max entries                                                                                                       |
| `hotkey.local.local-cache-ttl-minutes`  | `5`                      | Caffeine L1 write-based TTL (minutes)                                                                                         |
| `hotkey.local.inflight-max-size`        | `50000`                  | In-flight dedup max entries                                                                                                   |
| `hotkey.local.inflight-ttl-seconds`     | `5`                      | In-flight dedup entry TTL (must exceed slowest L2 response)                                                                   |
| `hotkey.local.inflight-timeout-seconds` | `3`                      | In-flight load timeout (must be < inflight-ttl-seconds). On timeout returns `Optional.empty()` — caller should fallback to DB |
| `hotkey.local.executor-core-pool-size`  | `8`                      | Thread pool core size                                                                                                         |
| `hotkey.local.executor-max-pool-size`   | `32`                     | Thread pool max size                                                                                                          |
| `hotkey.local.executor-queue-capacity`  | `2000`                   | Thread pool queue capacity                                                                                                    |
| `hotkey.local.expelled-queue-capacity`  | `50000`                  | Capacity of the expelled hot key staging queue (prevents TopK overflow)                                                       |
| `hotkey.local.default-hard-ttl-ms`      | `300000` (5min)          | Default hard TTL for normal keys (Caffeine eviction)                                                                          |
| `hotkey.local.hard-ttl-ms`              | `0`                      | Per-call hard TTL override for normal keys; 0 = use `default-hard-ttl-ms`                                                     |
| `hotkey.local.default-hot-hard-ttl-ms`  | `3600000` (1h)           | Default hard TTL for hot keys                                                                                                 |
| `hotkey.local.hot-hard-ttl-ms`          | `0`                      | Per-call hard TTL override for hot keys; 0 = use `default-hot-hard-ttl-ms`                                                    |
| `hotkey.local.default-soft-ttl-ms`      | `30000` (30s)            | Default soft TTL for normal keys (stale-while-revalidate)                                                                     |
| `hotkey.local.soft-ttl-ms`              | `0`                      | Per-call soft TTL override for normal keys; 0 = use `default-soft-ttl-ms`                                                     |
| `hotkey.local.default-hot-soft-ttl-ms`  | `300000` (5min)          | Default soft TTL for hot keys                                                                                                 |
| `hotkey.local.hot-soft-ttl-ms`          | `0`                      | Per-call soft TTL override for hot keys; 0 = use `default-hot-soft-ttl-ms`                                                    |
| `hotkey.local.null-value-ttl-seconds`   | `10`                     | TTL (seconds) for null/cache-miss entries; avoids caching negative results too long                                           |
| `hotkey.local.ttl-jitter-ratio`         | `0.05`                   | Jitter ratio (0.0–1.0); e.g. 0.05 = ±5% random offset applied to all TTL calculations. Always enabled.                        |
| `hotkey.local.refresh-max-pools`        | `100`                    | Max concurrent async refreshes for soft expire (Semaphore)                                                                    |
| `hotkey.local.version-key-ttl-minutes`  | `60`                     | Redis version key TTL (minutes); minimum 1                                                                                    |
| `hotkey.local.report-exchange`          | `hotkey.report.exchange` | RabbitMQ exchange for app-to-Worker report messages                                                                           |
| `hotkey.local.report-interval-ms`       | `50`                     | Interval at which app instances batch and send TopK reports to the Worker (ms)                                                |
| `hotkey.local.app-name`                 | `"default"`              | Logical application name used as tenant discriminator for Worker routing                                                      |
| `hotkey.local.shard-count`              | `1`                      | Divisor for auto consumer count calculation (max(4, availableProcessors/2) when 0); routing uses CH by default               |
| `hotkey.local.instance-id`              | `""` (auto)              | Explicit instance ID for queue naming; auto-detected as `server.port-HOSTNAME` (or `server.port-UUID`) if empty               |
| `hotkey.local.queue-capacity`           | `10000`                  | Report dispatcher queue capacity (internal bounded queue)                                                                     |
| `hotkey.local.queue-offer-timeout-ms`   | `100`                    | Report queue offer timeout (ms) — blocks up to this duration before dropping                                                  |
| `hotkey.local.consumer-count`           | `0`                      | Report consumer thread count; 0 = auto (max(4, availableProcessors / 2))                                                      |
| `hotkey.local.scheduler-pool-size`      | `8`                      | Pool size for the shared HotKey scheduler (periodic tasks)                                                                    |
| `hotkey.local.expected-worker-count`    | `0`                      | Expected number of Worker nodes for quorum-based health checks; 0 = dynamic discovery (always unhealthy until first heartbeat) |

### Distributed Lock (`hotkey.local.*`)

| Property                                | Default | Description                                                                           |
| --------------------------------------- | ------- | ------------------------------------------------------------------------------------- |
| `hotkey.local.try-lock-lock-count`      | `2`     | Number of SET NX retries for distributed lock acquisition                             |
| `hotkey.local.try-lock-inquiry-count`   | `1`     | Number of GET inquiries after transient SET NX failure                                |
| `hotkey.local.try-lock-unlock-count`    | `2`     | Number of DEL retries for distributed lock release                                    |

### Heartbeat (`hotkey.local.heartbeat.*`)

| Property                                        | Default                     | Description                                                                                            |
| ----------------------------------------------- | --------------------------- | ------------------------------------------------------------------------------------------------------ |
| `hotkey.local.heartbeat.exchange-name`          | `hotkey.heartbeat.exchange` | Topic exchange name for epoch-driven structured heartbeats from Workers                                |
| `hotkey.local.heartbeat.timeout-ms`             | `30000`                     | Timeout (ms) — a Worker is considered dead if no heartbeat is received within this window              |
| `hotkey.local.heartbeat.verify-interval-ms`     | `5000`                      | Interval (ms) for verifying suspected dead Workers via Direct reply-to PING                            |
| `hotkey.local.heartbeat.ping-timeout-ms`        | `3000`                      | Timeout (ms) for a PING/PONG verification probe                                                        |
| `hotkey.local.heartbeat.degrade-after-failures` | `3`                         | Consecutive PING failures before degrading the Worker; uses exponential backoff per Worker             |
| `hotkey.local.heartbeat.verify-max-backoff-ms`  | `600000`                    | Max exponential backoff (ms) between verification probes for a repeatedly failing Worker (10min)       |
| `hotkey.local.heartbeat.min-alive-workers`      | `0`                         | Minimum alive Workers for cluster health; 0 = use majority formula (knownWorkerCount / 2 + 1)          |

### Circuit Breaker (`hotkey.local.circuit-breaker.*`)

| Property                                                  | Default  | Description                                                                   |
| --------------------------------------------------------- | -------- | ----------------------------------------------------------------------------- |
| `hotkey.local.circuit-breaker.enabled`                    | `false`  | Enable sliding-window circuit breaker for remote calls (disabled by default)  |
| `hotkey.local.circuit-breaker.window-time-ms`             | `10000`  | Sliding window duration (ms)                                                  |
| `hotkey.local.circuit-breaker.window-buckets`             | `10`     | Number of buckets dividing the sliding window                                 |
| `hotkey.local.circuit-breaker.fail-threshold`             | `0.5`    | Failure rate threshold (0.0–1.0); opens breaker when exceeded                 |
| `hotkey.local.circuit-breaker.request-volume-threshold`   | `20`     | Minimum total requests before evaluating failure rate                         |
| `hotkey.local.circuit-breaker.single-test-interval-ms`    | `5000`   | Interval (ms) between half-open probe requests                                |
| `hotkey.local.circuit-breaker.log-enabled`                | `true`   | Whether to log state transitions (OPEN/CLOSE/HALF-OPEN)                       |

The circuit breaker wraps `SingleFlight.load()` — when open, `load()` returns `Optional.empty()` immediately without executing the supplier. The calling `HotKeyCache.get()` then falls back to returning any stale L1 entry if available. Only enable when your cache-load suppliers (database queries, remote API calls) are prone to cascading failures.

### Reporting (`hotkey.report.*`)

| Property                | Default | Description                                                              |
| ----------------------- | ------- | ------------------------------------------------------------------------ |
| `hotkey.report.enabled` | `true`  | Enable app-to-Worker report aggregation (requires `RabbitTemplate` bean) |

### Reporter Rate Limiter (`hotkey.local.reporter.*`)

| Property                                     | Default | Description                                                                                                                                                                                                           |
| -------------------------------------------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `hotkey.local.reporter.enabled`              | `true`  | Enable BBR adaptive rate-limiting on the Reporter flush path                                                                                                                                                          |
| `hotkey.local.reporter.cpu-threshold`        | `800`   | CPU threshold on a 0–1000 scale (800 = 80%). Below this the limiter is permissive (admits if concurrency ≤ budget **or** not in cooldown); at or above this, strict enforcement (only admits if concurrency ≤ budget) |
| `hotkey.local.reporter.cpu-poll-interval-ms` | `500`   | CPU polling interval (ms). A daemon thread polls `com.sun.management.OperatingSystemMXBean.getCpuLoad()` at this rate                                                                                                 |
| `hotkey.local.reporter.cpu-decay`            | `0.95`  | EMA decay factor for CPU load smoothing (0.0–1.0). Higher = smoother but slower to react                                                                                                                              |
| `hotkey.local.reporter.bbr-window-ms`        | `10000` | BBR sliding window duration (ms) for tracking max pass rate and min round-trip time                                                                                                                                   |
| `hotkey.local.reporter.bbr-window-buckets`   | `100`   | Number of buckets dividing the BBR sliding window                                                                                                                                                                     |
| `hotkey.local.reporter.bbr-cooldown-ms`      | `1000`  | Cooldown period (ms) after a batch is dropped — the limiter refuses all admits during cooldown regardless of CPU state                                                                                                |

### Worker Listener (`hotkey.worker-listener.*`)

| Property                                       | Default                     | Description                                                                                                                    |
| ---------------------------------------------- | --------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `hotkey.worker-listener.enabled`               | `false`                     | **Must be `true` when a Worker cluster is deployed.** Enables heartbeat consumption, Worker hot/cool decision listening, and the ClusterHealthView that drives report routing via consistent-hash ring |
| `hotkey.worker-listener.exchange-name`         | `hotkey.broadcast.exchange` | FanoutExchange name for receiving Worker HOT/COOL decisions and heartbeats; must match Worker-side `hotkey.worker.messaging.broadcast-exchange` |
| `hotkey.worker-listener.queue-prefix`          | `hotkey.worker`             | Prefix for the per-instance Worker listener queue; final queue: `{prefix}:{instanceId}`                                        |
| `hotkey.worker-listener.warmup-jitter-ms`      | `50`                        | Random delay (ms) before processing each Worker decision; spreads Redis reads across instances to avoid thundering herd       |
| `hotkey.worker-listener.concurrent-consumers`  | `2`                         | Number of concurrent RabbitMQ consumers for the Worker decision queue                                                          |
| `hotkey.worker-listener.prefetch-count`        | `5`                         | AMQP prefetch count per consumer                                                                                               |
| `hotkey.worker-listener.sre.enabled`           | `true`                      | Enable Google SRE adaptive rate limiter on HOT promotion processing                                                            |
| `hotkey.worker-listener.sre.success-threshold` | `0.6`                       | Minimum success ratio below which HOT promotions are probabilistically dropped                                                 |

> **⚠️ IMPORTANT: Startup order** — The `hotkey.heartbeat.exchange` and `hotkey.broadcast.exchange` exchanges are created by the App (common module). Worker nodes must **start after the App**, otherwise heartbeats will fail with `NOT_FOUND` and the cluster health ring will remain empty. When using Docker Compose, add `depends_on: app-1: { condition: service_started }` to Worker services. Alternatively, the Worker's heartbeat producer delays its first send by `pingIntervalMs` (default 1000ms) to give RabbitAdmin time to declare the exchange.

> **⚠️ IMPACT when disabled:** Without `worker-listener.enabled=true`, the App does not consume Worker heartbeats → `ClusterHealthView` records stay empty → `getAliveWorkerIds()` returns empty → Reporter `routeNode()` returns `null` → all report batches are **silently dropped**. The Worker never receives any data and never broadcasts HOT/COOL decisions. This is the most common configuration error in deployments with a Worker cluster.

### Scheduling (`hotkey.scheduling.*`, `hotkey.decay-period`)

| Property                    | Default | Description                                                                                          |
| --------------------------- | ------- | ---------------------------------------------------------------------------------------------------- |
| `hotkey.scheduling.enabled` | `true`  | Enable internal scheduler for HeavyKeeper decay and expelled queue drain                             |
| `hotkey.decay-period`       | `20`    | HeavyKeeper decay period in seconds (resolved via `@Scheduled` directly, not under `hotkey.local.*`) |

### Consistent Hashing (`hotkey.local.consistent-hashing.*`)

| Property                                        | Default | Description                                                                               |
| ----------------------------------------------- | ------- | ----------------------------------------------------------------------------------------- |
| `hotkey.local.consistent-hashing.enabled`       | `true`  | Enable consistent hashing for dynamic Worker routing (default; set to `false` to disable) |
| `hotkey.local.consistent-hashing.virtual-nodes` | `500`   | Number of virtual nodes per physical Worker node for hash-space distribution              |

### Spring Cache Integration (`hotkey.spring-cache.*`)

| Property                          | Default | Description                                                                              |
| --------------------------------- | ------- | ---------------------------------------------------------------------------------------- |
| `hotkey.spring-cache.enabled`     | `false` | Enable Spring Cache integration (exposes `HotKeyCacheManager` as a `CacheManager` bean)  |
| `hotkey.spring-cache.key-separator` | `::` | Separator between cache name and key (e.g. `"users::123"`)                              |

Allows standard `@Cacheable` / `@CachePut` / `@CacheEvict` annotations to trigger HotKey hot-key detection, soft-expire, and cross-instance broadcast. Companion annotations `@HotKeyCacheTTL`, `@Intercept`, `@Fallback`, and `@NullCaching` remain functional for `@Cacheable` operations.

### Cache Sync (`hotkey.sync.*`)

| Property                           | Default                | Description                                                                                 |
| ---------------------------------- | ---------------------- | ------------------------------------------------------------------------------------------- |
| `hotkey.sync.enabled`              | `false`                | Enable cross-instance cache sync via RabbitMQ                                               |
| `hotkey.sync.exchange-name`        | `hotkey.sync.exchange` | Fanout exchange name for sync messages (REFRESH / INVALIDATE / INVALIDATE_ALL / RULES_SYNC) |
| `hotkey.sync.queue-prefix`         | `hotkey.sync`          | Queue name prefix; full name = `{prefix}:{instanceId}`                                      |
| `hotkey.sync.dedup-window-seconds` | `10`                   | Dedup window for received sync messages (seconds)                                           |
| `hotkey.sync.dedup-max-size`       | `10000`                | Dedup cache max entries                                                                     |
| `hotkey.sync.warmup-jitter-ms`     | `50`                   | Random jitter before processing sync messages (prevents herd)                               |
| `hotkey.sync.concurrent-consumers` | `3`                    | Number of concurrent RabbitMQ consumers for sync queue                                      |
| `hotkey.sync.scheduler-pool-size`  | `4`                    | Thread pool size for async sync jitter delay scheduling                                     |
| `hotkey.sync.prefetch-count`       | `5`                    | AMQP prefetch count per sync consumer                                                       |
| `hotkey.sync.auto-startup`         | `true`                 | Whether the sync listener container starts automatically with the application               |

### Worker Listener (`hotkey.worker-listener.*`)

| Property                                       | Default                     | Description                                                                     |
| ---------------------------------------------- | --------------------------- | ------------------------------------------------------------------------------- |
| `hotkey.worker-listener.enabled`               | `false`                     | Enable listening for Worker HOT/COOL decisions                                  |
| `hotkey.worker-listener.exchange-name`         | `hotkey.broadcast.exchange` | Fanout exchange name for Worker broadcasts                                      |
| `hotkey.worker-listener.queue-prefix`          | `hotkey.worker`             | Queue name prefix; full name = `{prefix}:{instanceId}`                          |
| `hotkey.worker-listener.warmup-jitter-ms`      | `50`                        | Random jitter before processing Worker messages (prevents herd)                 |
| `hotkey.worker-listener.concurrent-consumers`  | `2`                         | Number of concurrent RabbitMQ consumers for Worker listener queue               |
| `hotkey.worker-listener.scheduler-pool-size`   | `2`                         | Thread pool size for jittered Worker cache-update tasks                         |
| `hotkey.worker-listener.prefetch-count`        | `5`                         | AMQP prefetch count per worker-listener consumer                                |
| `hotkey.worker-listener.auto-startup`          | `true`                      | Whether the worker listener container starts automatically with the application |
| **`hotkey.worker-listener.sre.*`**             |                             | **SRE Adaptive Rate Limiter**                                                   |
| `hotkey.worker-listener.sre.enabled`           | `true`                      | Enable SRE rate limiter on HOT decision processing path                         |
| `hotkey.worker-listener.sre.window-ms`         | `3000`                      | Sliding window duration for rate calculation (ms)                               |
| `hotkey.worker-listener.sre.buckets`           | `10`                        | Number of buckets in the sliding window                                         |
| `hotkey.worker-listener.sre.min-samples`       | `20`                        | Minimum total samples before throttling starts                                  |
| `hotkey.worker-listener.sre.success-threshold` | `0.6`                       | Success ratio threshold (0.0–1.0); throttles when success rate drops below this |

### Worker Node (`hotkey.worker.*`)

| Property                                                             | Default                     | Description                                                                                                                  |
| -------------------------------------------------------------------- | --------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `hotkey.worker.enabled`                                              | `false`                     | Enable Worker mode (must explicitly set to `true`)                                                                           |
| **`hotkey.worker.routing.*`**                                        |                             | **Routing**                                                                                                                  |
| `hotkey.worker.routing.app-name`                                     | `"default"`                 | Logical application name (tenant discriminator)                                                                              |
| **`hotkey.worker.messaging.*`**                                      |                             | **Messaging**                                                                                                                |
| `hotkey.worker.messaging.report-exchange`                            | `hotkey.report.exchange`    | Direct exchange for app report messages                                                                                      |
| `hotkey.worker.messaging.broadcast-exchange`                         | `hotkey.broadcast.exchange` | Exchange for HOT/COOL broadcasts (Worker publishes with routing keys; may need alignment with worker-listener.exchange-name) |
| `hotkey.worker.messaging.heartbeat-exchange`                         | `hotkey.heartbeat.exchange` | Topic exchange for epoch-driven structured heartbeats (must match App-side `hotkey.local.heartbeat.exchange-name`)           |
| **`hotkey.worker.report-consumer.*`**                                |                             | **Report Consumer**                                                                                                          |
| `hotkey.worker.report-consumer.concurrent-consumers`                 | `8`                         | Number of concurrent consumers for the report queue. Minimum 1.                                                               |
| `hotkey.worker.report-consumer.prefetch-count`                       | `50`                        | Prefetch count per consumer; balances throughput vs memory pressure.                                                          |
| **`hotkey.worker.sliding-window.*`**                                 |                             | **Sliding Window**                                                                                                           |
| `hotkey.worker.sliding-window.duration-ms`                           | `1000`                      | Sliding window duration (milliseconds)                                                                                       |
| `hotkey.worker.sliding-window.slices`                                | `10`                        | Number of time slices within one window                                                                                      |
| **`hotkey.worker.threshold.*`**                                      |                             | **Hot Threshold**                                                                                                            |
| `hotkey.worker.threshold.hot-threshold`                              | `1000`                      | Absolute hot-key threshold; `≤0` = use ratio-based                                                                           |
| `hotkey.worker.threshold.hot-threshold-ratio`                        | `0.01`                      | Hot-key threshold as fraction of estimated global QPS (1%)                                                                   |
| **`hotkey.worker.state-machine.*`**                                  |                             | **State Machine**                                                                                                            |
| `hotkey.worker.state-machine.sm-duration-ms`                        | `500`                       | State-machine slice window duration (ms). Independent of sliding-window. Each slice = sm-duration-ms / sm-slices.             |
| `hotkey.worker.state-machine.sm-slices`                             | `10`                        | Number of slices within the state-machine window.                                                                            |
| `hotkey.worker.state-machine.confirm-duration-ms`                    | `100`                       | Total duration for HOT confirmation (confirmCount = ceil(confirm-duration-ms / slice-ms)). Must be ≥ slice-ms.                |
| `hotkey.worker.state-machine.cool-duration-ms`                       | `600000`                    | Duration key must stay below threshold to be considered COLD                                                                 |
| `hotkey.worker.state-machine.pre-cool-grace-ms`                      | `60000`                     | Grace period at end of cool-down for silent revival                                                                          |
| `hotkey.worker.state-machine.evict-interval-ms`                      | `30000`                     | Stale state eviction interval (ms); must be >= cool-duration-ms \* 2                                                         |
| **`hotkey.worker.global-qps-dynamic-threshold.*`**                   |                             | **Dynamic Threshold (Global QPS)**                                                                                           |
| `hotkey.worker.global-qps-dynamic-threshold.recalculate-interval-ms` | `60000`                     | Interval for dynamic threshold recalculation                                                                                 |
| `hotkey.worker.global-qps-dynamic-threshold.qps-change-tolerance`    | `0.5`                       | QPS change tolerance before threshold update (±50%)                                                                          |
| `hotkey.worker.global-qps-dynamic-threshold.learning-period-ms`      | `30000`                     | Learning period for QPS estimation                                                                                           |
| `hotkey.worker.global-qps-dynamic-threshold.hot-threshold-ratio`     | `0.01`                      | Hot threshold as fraction of estimated global QPS                                                                            |
| **`hotkey.worker.topk-validation.*`**                                |                             | **TopK Validation**                                                                                                          |
| `hotkey.worker.topk-validation.validate-interval-ms`                 | `60000`                     | Interval between Top-K cross-validation runs                                                                                 |
| `hotkey.worker.topk-validation.pre-warm-count`                       | `5`                         | Number of top-ranked keys eligible for pre-warming                                                                           |
| `hotkey.worker.topk-validation.pre-warm-min-appearances`             | `2`                         | Min consecutive Top-K appearances required before pre-warming                                                                |
| **`hotkey.worker.heavy-keeper.*`**                                   |                             | **HeavyKeeper (Worker-scoped)**                                                                                              |
| `hotkey.worker.heavy-keeper.top-k`                                   | `100`                       | Worker-side HeavyKeeper Top-K capacity                                                                                       |
| `hotkey.worker.heavy-keeper.width`                                   | `20000`                     | Worker-side Count-Min Sketch width                                                                                           |
| `hotkey.worker.heavy-keeper.depth`                                   | `10`                        | Worker-side Count-Min Sketch depth                                                                                           |
| `hotkey.worker.heavy-keeper.decay`                                   | `0.9`                       | Worker-side HeavyKeeper decay factor                                                                                         |
| `hotkey.worker.heavy-keeper.min-count`                               | `10`                        | Worker-side minimum count threshold                                                                                          |
| **`hotkey.worker.heartbeat.*`**                                      |                             | **Heartbeat**                                                                                                                |
| `hotkey.worker.heartbeat.ping-interval-ms`                           | `1000`                      | Interval (ms) between structured heartbeat sends                                                                             |
| **`hotkey.worker.persistence.*`**                                    |                             | **TopK Persistence (warm-start)**                                                                                            |
| `hotkey.worker.persistence.enabled`                                  | `false`                     | Enable periodic TopK snapshot to Redis (opt-in)                                                                              |
| `hotkey.worker.persistence.persist-interval-ms`                      | `30000`                     | Interval (ms) between TopK snapshots                                                                                         |
| `hotkey.worker.persistence.topk-count`                               | `100`                       | Number of top keys to persist per snapshot                                                                                   |
| `hotkey.worker.persistence.redis-key-prefix`                         | `"hotkey:topk:worker:"`     | Redis key prefix; final key = prefix + appName + ":" + nodeId                                                                |
| `hotkey.worker.persistence.ttl-days`                                 | `3`                         | TTL (days) for persisted TopK data in Redis                                                                                  |

## Modules

| Module                                                     | Dependency                                                                          | Auto-Config                                                                                                                                                            |
| ---------------------------------------------------------- | ----------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `facade`                                                   | none                                                                                | always                                                                                                                                                                 |
| `hotkey`                                                   | none                                                                                | always                                                                                                                                                                 |
| `report`                                                   | `spring-boot-starter-amqp`                                                          | `@ConditionalOnBean(RabbitTemplate.class)` + property (`hotkey.report.enabled`)                                                                                        |
| `spring-cache`                                              | `spring-boot-starter-cache`                                                        | `@ConditionalOnClass(AbstractValueAdaptingCache.class)` + `@ConditionalOnBean(HotKey.class)` + property (`hotkey.spring-cache.enabled`)                                  |
| `cache` (Redis)                                            | `spring-boot-starter-data-redis`                                                    | `@ConditionalOnClass(RedisTemplate.class)` + `@ConditionalOnBean(RedisTemplate.class)`                                                                                 |
| `amqp` (RabbitMQ, merged in `HotKeyAmqpAutoConfiguration`) | `spring-boot-starter-amqp` (+ `spring-boot-starter-data-redis` for worker-listener) | `@ConditionalOnClass(RabbitTemplate.class)` + inner `@ConditionalOnClass(RedisTemplate.class)` + properties (`hotkey.sync.enabled` / `hotkey.worker-listener.enabled`) |
| `worker`                                                   | `spring-boot-starter-amqp` (+ `spring-boot-starter-data-redis`)                     | `@ConditionalOnBean(RabbitTemplate.class)` + property (`hotkey.worker.enabled`)                                                                                        |
| `actuator`                                                 | `spring-boot-starter-actuator`                                                      | `@ConditionalOnClass(Endpoint.class)`                                                                                                                                  |
| `micrometer`                                               | `io.micrometer:micrometer-core`                                                     | `@ConditionalOnClass(MeterBinder.class)` — auto-registers Caffeine cache metrics (`hotkey.l1.*`) + custom HotKey business metrics                                      |
| `scheduling`                                               | none                                                                                | `@ConditionalOnProperty` + `@ConditionalOnBean(TopK.class)`                                                                                                            |

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
