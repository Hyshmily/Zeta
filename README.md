# HotKey

<p align="center">
  <img src="img/HotKey.png" alt="HotKey" width="300">
</p>

<p align="center">
  <a href="https://jitpack.io/#Hyshmily/HotKey"><img src="https://jitpack.io/v/Hyshmily/HotKey.svg" alt="JitPack"></a>
  <a href="https://www.apache.org/licenses/LICENSE-2.0"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
  <a href="https://openjdk.java.net/"><img src="https://img.shields.io/badge/Java-25-orange" alt="Java"></a>
  <a href="https://spring.io/projects/spring-boot"><img src="https://img.shields.io/badge/Spring%20Boot-3.5.3-brightgreen" alt="Spring Boot"></a>
</p>

[**中文版**](README.zh.md)

### Why HotKey?

In real-world development, the author frequently faced the challenge of managing large numbers of cache keys. Manual maintenance of multi-tier caches (Caffeine, Redis, database), logical expiration configuration, pre-computation and pre-warming of hot keys — each step was extremely tedious. In distributed cluster environments, correctly sharing hot keys across nodes and preventing cache stampedes under high concurrency became an evergreen problem.

The initial idea was simple: find an out-of-the-box solution, or at least wrap a convenient multi-level cache utility.

Unfortunately, existing solutions were either overly complex with high deployment and operational costs, or had been discontinued (such as JD's [hotkey](https://gitee.com/jd-platform-opensource/hotkey) project, which as of May 2026 is no longer maintained), making them unsuitable for lightweight, practical needs. While the author's design concepts align with many of theirs, the architectural complexity motivated a different path.

This is how HotKey was born: a lightweight, portable, out-of-the-box distributed hot key caching framework.

Past, present, and future — it will remain open source.

> [!TIP]
> **Before you start, have questions?** See [FAQ.md](docs/FAQ.md) for answers to common questions about local vs central detection, Worker delay, MQ throughput, and more.

### How It Works

HotKey is a [high-performance](docs/HotKey_Benchmark_Report.en.md), low-cost, lightweight, full-link hot key governance Spring Boot Starter, integrating **cache read/write (get/put), automatic hot key detection, multi-level cache pre-warming, cross-instance broadcast synchronization, AOP annotation interception, and allow/blocklist filtering**.

Most local caches store every entry uniformly in Caffeine. But under a massive key space:

- **Memory waste** — most keys are read once
- **Broadcast storm** — full cache = full invalidation, broadcasts grow linearly with key count

HotKey's strategy: **cache only truly hot data.**

Uses [HeavyKeeper](https://github.com/go-kratos/aegis) (Count-Min Sketch variant) for probabilistic frequency detection. **Read path**: all loaded keys go to L1, but with **differentiated TTLs** — hot keys last longer (1h), normal keys expire faster (5min). **Write path**: `putThrough` writes directly to L1 and broadcasts regardless — caller owns the write.

**Cluster-wide detection**: deploy a dedicated Worker node aggregating access reports from all instances — solves the "100 hits on one pod" vs "1 hit on 100 pods" blind spot.

### When to use

| Suitable                                            | Not suitable                                     |
| --------------------------------------------------- | ------------------------------------------------ |
| Read-heavy workloads (String / List / Set / ZSet)   | Write-heavy / atomic operations (seckill, Lua)   |
| Large key space with Pareto distribution            | Small key space (< 200), manual Caffeine is fine |
| Read-many-write-few, eventually consistent          | Strong read-after-write consistency required     |
| Spring Boot 3.x + Java 17+                          | Non-Spring-Boot projects                         |
| Optional Redis + optional RabbitMQ (multi-instance) |                                                  |
| Optional Worker node for cluster-wide detection     |                                                  |

## Features

- **HeavyKeeper Algorithm** — probabilistic top-k detection with Count-Min Sketch + exponential conflict decay
- **Multi-Level Cache** — Caffeine (L1) → optional reader callback (L2, e.g., Redis) + caller-side DB fallback via `Optional.orElseGet()`, with automatic hot-key promotion
- **Differentiated TTLs** — separate hard/soft TTLs for hot keys vs normal keys; hot keys cached longer (1h/5min) while normal keys expire faster (5min/30s)
- **In-Flight Dedup** — concurrent L1 miss requests share a single L2 read via a dedicated `SingleFlight` bean

  > [!NOTE]
  > Ensure `hotkey.local.inflight-ttl-seconds` exceeds the slowest L2 response time for your workload, or the cache entry may expire before the future completes, causing duplicate L2 reads.
  > Also ensure `hotkey.local.inflight-timeout-seconds` < `hotkey.local.inflight-ttl-seconds`. On timeout, `SingleFlight.load()` returns `Optional.empty()` — the caller should handle via DB fallback.

- **Soft Expire (Logical Expiration)** — return stale L1 value immediately while asynchronously refreshing in the background; lower p99 at the cost of short-lived staleness. **Fully replaces traditional Redis-side logical expiration** (`RedisData{data, expireTime}` wrapper pattern) — Redis stores raw values, HotKey manages staleness at the L1 Caffeine level
- **Redis Collections** — `putBeforeInvalidate` for List/Set/ZSet incremental writes; no `putThrough` needed
- **Hot Key Sync** — optional RabbitMQ fanout (via `hotkey.sync.*`) to synchronize cache invalidations across instances; separate worker-listener (via `hotkey.worker-listener.*`) for receiving Worker-originated HOT/COOL decisions
- **Worker Mode** — dedicated cluster-wide hot key detection node; sliding-window + state-machine pipeline for cross-instance consensus; see [Worker Mode](#worker-mode)
- **Report Aggregation** — every `get()` / `getWithSoftExpire()` call reports to local `HotKeyReporter`, which periodically batches access counts to Worker node via RabbitMQ for cluster-wide hot key detection
- **Configurable Thread Pool** — dedicated `TaskExecutor` with bounded queue
- **Spring Boot Auto-Configuration** — drop-in dependency, zero boilerplate

## Latency & Performance

<details>
<summary><b>Click to expand — full chain breakdown and extreme tuning notes</b></summary>

See the [benchmark report](docs/HotKey_Benchmark_Report.en.md) for details. Per-hop latencies over Redis + RabbitMQ containers (10 phases, 45k ops, 0 errors):

| Path                                       | Description                                                     | P50           | P95           | P99           |
| ------------------------------------------ | --------------------------------------------------------------- | ------------- | ------------- | ------------- |
| L1 Hit (Caffeine lookup)                   | Pure memory lookup, no network I/O                              | **0.001 ms**  | 0.004 ms      | 0.018 ms      |
| L1 Miss → Redis → L1                       | Redis GET RTT + SingleFlight dedup + L1 repopulation            | 0.51 ms       | 0.94 ms       | 2.26 ms       |
| AMQP Publish                               | RabbitMQ channel write (memory-to-memory)                       | 0.02 ms       | 0.11 ms       | 0.27 ms       |
| AMQP E2E Delivery                          | Publish + broker routing + consumer delivery                    | 0.07 ms       | 0.27 ms       | 0.48 ms       |
| Redis GET RTT                              | Pure network round-trip                                         | 0.62 ms       | 1.60 ms       | 4.01 ms       |
| Worker Decision (w/o state machine)        | Sliding window → broadcast HOT, includes jitter+AMQP+L1 polling | **51.64 ms**  | 97.75 ms      | 104.16 ms     |
| State Machine Pipeline (w/ SM, 20 windows) | 20 confirm windows(2s) + AMQP decision broadcast + L1 promotion | **1,983 ms**  | 2,015 ms      | 2,015 ms      |
| **Full Chain (w/o state machine)**         | **Step breakdown below**                                        | **155.81 ms** | **202.14 ms** | **212.69 ms** |
| ┣ L1 Miss → Redis → L1                     | Same as Phase 6                                                 | 0.51 ms       | —             | —             |
| ┣ Report batch wait                        | `report-interval-ms=100ms` half-interval average                | ~50 ms        | —             | —             |
| ┣ AMQP Report Delivery (App→Worker)        | flush() → Report Exchange → Worker                              | ~1 ms         | —             | —             |
| ┣ Worker Sliding Window + TopK             | In-memory, negligible                                           | <1 ms         | —             | —             |
| ┣ AMQP Decision Broadcast (Worker→App)     | WorkerListener receives decision                                | ~1 ms         | —             | —             |
| ┗ Remainder (scheduling + polling + noise) | Two AMQP dispatch + isLocalHotKey() polling + variance          | ~52 ms        | —             | —             |
| **Full Chain w/ SM (20 confirm windows)**  | Full chain + 2s confirm windows, 10/10 keys promoted            | **2,038 ms**  | **2,055 ms**  | **2,055 ms**  |

</details>

> [!WARNING]
> **Extreme parameter tuning — trading reliability for latency:**
>
> Pushing the following parameters to their limits can reduce full-chain latency to **~8ms** (P50, with sliceMs also tuned), but the author **strongly discourages** this in production. HotKey's defaults are deliberately conservative to maximize distributed cluster reliability over raw latency.
>
> | Parameter                                             | Limit             | Effect                                                                                   | Constraint                                       |
> | ----------------------------------------------------- | ----------------- | ---------------------------------------------------------------------------------------- | ------------------------------------------------ |
> | `hotkey.local.report-interval-ms`                     | 0 → min 1         | Nearly disables report batching — flushes almost immediately after `record()`            | `ScheduledExecutorService` requires period > 0   |
> | `hotkey.worker.warmup-jitter-ms`                      | 0                 | Disables warmup jitter (thundering-herd protection lost), Worker decision runs instantly | —                                                |
> | `hotkey.worker.state-machine.confirm-duration-ms`     | 0                 | Disables state-machine confirm windows — broadcasts HOT on first hot window              | —                                                |
> | `hotkey.worker.sliding-window.duration-ms` / `slices` | 1000/10 → 100/100 | Shrinks tick interval, reduces next-tick wait (avg~50ms→~5ms)                            | Window statistical precision drops significantly |
>
> **Estimated latency under extreme tuning:**
>
> | Scenario                       | Default P50 | Extreme P50 | Savings   |
> | ------------------------------ | ----------- | ----------- | --------- |
> | Full Chain (w/o state machine) | 155.81 ms   | ~8 ms       | ~148 ms   |
> | Full Chain + state machine     | 2,038 ms    | ~8 ms       | ~2,030 ms |
>
> **Costs:**
>
> - Report AMQP message volume surges from ~10 batches/s to ~1,000 batches/s (`report-interval-ms=1`), dramatically increasing RabbitMQ load
> - Disabling warmup jitter lets millisecond traffic spikes trigger HOT broadcasts, raising false-positive rates
> - Disabling confirm windows means any transient burst immediately escalates to a global hot key, amplifying false positives
> - Reducing sliding-window `sliceMs` (via duration/slices) loses statistical accuracy — short-lived bursts are more likely to trigger false HOT decisions
>
## Quick Start

### 1. Add dependency (JitPack)

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependency>
    <groupId>io.github.hyshmily</groupId>
    <artifactId>hotkey</artifactId>
    <version>1.1.3</version>
</dependency>
```

Use the latest release as the version. Redis and RabbitMQ dependencies are optional — include them only if you need the corresponding features.

### 2. Configure

Default local configuration (suitable for most scenarios)

> [!IMPORTANT]
> While HotKey itself does not strictly require external dependencies, the author strongly recommends introducing Redis for optimal results in distributed deployments, even though extensive degradation handling has been implemented in the core call chain.

**Optional feature configs:**

| Feature              | Enable                                     | Description                            |
| -------------------- | ------------------------------------------ | -------------------------------------- |
| Redis L2             | Add `RedisTemplate` bean                   | Two-level cache with L2 fallback       |
| Cross-instance sync  | `hotkey.sync.enabled=true`                 | RabbitMQ-based cache invalidation      |
| Worker Listener      | `hotkey.worker-listener.enabled=true`      | Receive HOT/COOL decisions from Worker |
| Worker Mode          | `hotkey.worker.enabled=true`               | Run as dedicated Worker node           |
| `@HotKey` Annotation | `hotkey.annotation.enabled=true` + AspectJ | Declarative caching                    |
| Reporting            | `hotkey.report.enabled=true` (default)     | Report access counts to Worker         |

See [Configure](#2-configure) for all options and [CONFIG.md](docs/CONFIG.md) for the complete property reference.

<details>
<summary><b>Quick-deploy YAML templates</b> (minimal configuration — only required overrides)</summary>

**Local (App side)** — add the `hotkey` dependency and you're ready; uncomment features as needed

```yaml
hotkey:
  # All local parameters use defaults — no explicit config needed

  # —— Optional features, uncomment as needed ——

  # Cross-instance cache sync (requires spring-boot-starter-amqp + spring-boot-starter-data-redis)
  # sync:
  #   enabled: true

  # @HotKey annotation (requires spring-boot-starter-aop)
  # annotation:
  #   enabled: true

  # Worker decision listener (requires spring-boot-starter-amqp + spring-boot-starter-data-redis)
  # worker-listener:
  #   enabled: true
  # sync:
  #   enabled: true     # worker-listener depends on the hotKeyRedisLoader bean
```

**Worker node (standalone)** — add `spring-boot-starter-amqp`

```yaml
hotkey:
  worker:
    enabled: true
    routing:
      app-name: myapp # 【MUST】match hotkey.local.app-name on the App side
      shard-count: 1 # 【MUST】match hotkey.local.shard-count on the App side
      shard-index: 0 # 【MUST】this instance's shard, range [0, shard-count-1]


# Multi-worker example: 3 machines, each with a different shard
# app-name / shard-count are identical across machines; shard-index differs
#
# Machine A:  shard-index: 0
# Machine B:  shard-index: 1
# Machine C:  shard-index: 2
# shard-count: 3 (same on all machines)
```

**All parameters (override defaults)**

```yaml
hotkey:
  local:
    # ——— Cross-node consistency (app and worker sides MUST match) ———
    app-name: "default"                     # must match worker.routing.app-name
    shard-count: 1                          # must match worker.routing.shard-count

    # ——— Instance identity ———
    instance-id: ""                         # explicit instance ID (empty = auto-generated)

    # ——— Reporting ———
    report-exchange: "hotkey.report.exchange"
    report-interval-ms: 100                 # batch send interval (ms)
    queue-capacity: 10000                   # report queue capacity
    queue-offer-timeout-ms: 100             # queue write timeout (ms)
    consumer-count: 0                       # consumer threads (0=auto)

    # ——— HeavyKeeper algorithm ———
    topK: 100                               # max hot key count
    width: 50000                            # Count-Min Sketch width
    depth: 5                                # Count-Min Sketch depth
    decay: 0.92                             # decay factor (multiplied per fading)
    minCount: 10                            # minimum count for hot key
    expelled-queue-capacity: 50000          # expelled hot key staging queue

    # ——— L1 Caffeine cache ———
    local-cache-max-size: 1000              # max cache entries
    local-cache-ttl-minutes: 5              # cache TTL (minutes)
    local-cache-access-ttl-minutes: 0       # access-based TTL (0=disabled)

    # ——— SingleFlight dedup ———
    inflight-max-size: 50000                # max dedup keys
    inflight-ttl-seconds: 5                 # dedup TTL (must > L2 response time)
    inflight-timeout-seconds: 3             # async wait timeout (must < inflight-ttl-seconds)

    # ——— Async executor ———
    executor-core-pool-size: 8              # core threads
    executor-max-pool-size: 32              # max threads
    executor-queue-capacity: 2000           # work queue capacity

    # ——— Refresh & version control ———
    refresh-max-pools: 100                  # refresh thread pool limit
    version-key-ttl-minutes: 60             # Redis version key TTL

    # ——— Normal key TTL ———
    default-hard-ttl-ms: 300000             # default hard TTL (5min)
    hard-ttl-ms: 0                          # override (0=use default)
    default-soft-ttl-ms: 30000              # default soft TTL (30s)
    soft-ttl-ms: 0                          # override (0=use default)

    # ——— Hot key TTL (effective after Worker HOT decision) ———
    default-hot-hard-ttl-ms: 3600000        # default hot key hard TTL (1h)
    hot-hard-ttl-ms: 0                      # override (0=use default)
    default-hot-soft-ttl-ms: 300000         # default hot key soft TTL (5min)
    hot-soft-ttl-ms: 0                      # override (0=use default)

  # Feature toggles
  report:
    enabled: true                           # app→worker reporting
  sync:
    enabled: false                          # cross-instance sync (requires Redis + RabbitMQ)
  annotation:
    enabled: false                          # @HotKey annotation support (requires spring-boot-starter-aop)
  scheduling:
    enabled: true                           # periodic decay & expel drain
  decay-period: 20                          # HeavyKeeper fading interval (s), requires scheduling enabled

  # App-side Worker decision listener (receive HOT/COOL)
  worker-listener:
    enabled: false                          # requires Redis + RabbitMQ
    exchange-name: "hotkey.broadcast.exchange"
    queue-prefix: "hotkey.worker"
    auto-startup: true                      # start with application
    warmup-jitter-ms: 100                   # random delay before processing (thundering herd prevention)
    concurrent-consumers: 2                 # concurrent consumers
    scheduler-pool-size: 2                  # scheduled task thread pool

  # App-side cross-instance cache sync
  sync:
    enabled: false
    exchange-name: "hotkey.sync.exchange"
    queue-prefix: "hotkey.sync"
    auto-startup: true
    dedup-window-seconds: 10                # message dedup window
    dedup-max-size: 10000                   # dedup cache limit
    warmup-jitter-ms: 100                   # random delay before processing
    concurrent-consumers: 3                 # concurrent consumers
    scheduler-pool-size: 4                  # scheduled task thread pool

  # Worker-side standalone node
  worker:
    enabled: false

    routing:
      app-name: "default"                   # must match local.app-name
      shard-count: 1                        # must match local.shard-count
      shard-index: 0                        # this instance's shard [0, shard-count-1]

    messaging:
      report-exchange: "hotkey.report.exchange"
      broadcast-exchange: "hotkey.broadcast.exchange"

    sliding-window:
      duration-ms: 1000                     # window duration (ms)
      slices: 10                            # window slices (must divide duration-ms)

    threshold:
      hot-threshold: 1000                   # absolute QPS threshold (≤0 = use ratio)
      hot-threshold-ratio: 0.01             # relative QPS ratio (1%)

    state-machine:
      confirm-duration-ms: 2000             # confirm window (ms), sustained heat before HOT
      cool-duration-ms: 15000               # cool window (ms), sustained cool before COOL
      pre-cool-grace-ms: 5000               # pre-cool grace period (ms)

    global-qps-dynamic-threshold:
      qps-change-tolerance: 0.5             # QPS change tolerance multiplier
      learning-period-ms: 30000             # learning period (ms)
      hot-threshold-ratio: 0.01             # dynamic threshold ratio
      recalculate-interval-ms: 60000        # recalculation interval (ms)

    topk-validation:
      validate-interval-ms: 60000           # validation interval (ms)
      pre-warm-count: 5                     # pre-warm count
      pre-warm-min-appearances: 2           # min appearances

    heavy-keeper:
      top-k: 100                            # max hot key count
      width: 20000                          # Sketch width
      depth: 10                             # Sketch depth
      decay: 0.9                            # decay factor
      min-count: 10                         # minimum count

    heartbeat:
      ping-interval-ms: 1000                # heartbeat broadcast interval (ms)
```

</details>

### 3. Use

**A. Pure local cache (no L2)**

Only reads L1 (Caffeine), no hot key tracking or reporting.

```java
@Autowired
private HotKey hotKey;

Optional<String> r = hotKey.peek("user:123"); // L1 only, no hot key detection
```

**B. Two-level cache (Redis or any backend)**

```java
@Autowired private HotKey hotKey;
@Autowired private StringRedisTemplate redisTemplate;

// Read: load from Redis on L1 miss
Optional<String> r = hotKey.get("user:123", () -> redisTemplate.opsForValue().get("user:123"));

// Write: update Redis and sync L1
hotKey.putThrough("user:123", newValue, () -> redisTemplate.opsForValue().set("user:123", newValue));
```

**C. DB fallback + null value anti-penetration**

```java
Optional<String> r = hotKey.get("user:123", () -> redisTemplate.opsForValue().get("user:123"));
```

> Note: if the reader returns `null`, HotKey treats it as a miss (`Optional.empty()`). The caller is responsible for handling null values (e.g., using a sentinel object wrapper).

Full example with `@Autowired`:

```java
// Inject HotKey instance (io.github.hyshmily.hotkey.HotKey)
@Autowired
private HotKey hotKey;

// In your business method
public String getUserName(String userId) {
  String cacheKey = "user:" + userId;

  // Null sentinel (distinguishes "no data" from "cache miss")
  Object NULL_SENTINEL = new Object();

  Optional<Object> result = hotKey.get(cacheKey, () -> {
    // 1. Check Redis first
    String val = redisTemplate.opsForValue().get(cacheKey);
    if (val != null) return val;

    // 2. Redis miss, query DB
    User user = userService.getById(userId);
    if (user != null) {
      String name = user.getName();
      redisTemplate.opsForValue().set(cacheKey, name, Duration.ofMinutes(10));
      return name;
    }

    // 3. DB has nothing either, cache short-lived empty value to prevent penetration
    redisTemplate.opsForValue().set(cacheKey, "", Duration.ofMinutes(1));
    return NULL_SENTINEL;
  });

  // Result handling
  return result
    .filter((v) -> v != NULL_SENTINEL)
    .map(Object::toString)
    .orElse(null);
}
```

**D. Helper bean to simplify calls**

```java
@Component
public class RedisHotKeyHelper {

  @Autowired
  private HotKey hotKey;

  @Autowired
  private RedisTemplate<String, Object> redisTemplate;

  public <T> Optional<T> get(String key) {
    return hotKey.get(key, () -> (T) redisTemplate.opsForValue().get(key));
  }

  public void set(String key, Object value) {
    hotKey.putThrough(key, value, () -> redisTemplate.opsForValue().set(key, value));
  }
}
```

**E. Custom L2 (non-Redis)**

Any data source can serve as L2:

```java
Optional<User> r = hotKey.get("user:123", () -> userMapper.selectById(123));

User user = r.orElseGet(() -> createDefaultUser());
```

**F. Redis collections (List, Set, ZSet)**

`putThrough` requires the full new value to update L1, but incremental collection operations (LPUSH, SADD, ZADD) modify only a single element — the caller cannot know the complete new value. Use `putBeforeInvalidate` to invalidate L1 after mutation; the next `get()` automatically re-fetches from Redis.

```java
@Component
public class CollectionHotKeyCache {

  @Autowired
  private HotKey hotKey;

  @Autowired
  private RedisTemplate<String, Object> redisTemplate;

  public Boolean sIsMember(String key, Object member) {
    return hotKey.get(key + "::member::" + member, () -> redisTemplate.opsForSet().isMember(key, member));
  }

  @SuppressWarnings("unchecked")
  public Set<Object> sMembers(String key) {
    return hotKey.get(key, () -> redisTemplate.opsForSet().members(key));
  }

  public void sAdd(String key, Object... members) {
    hotKey.putBeforeInvalidate(key, () -> redisTemplate.opsForSet().add(key, members));
  }

  public List<Object> lRange(String key, long start, long end) {
    String cacheKey = key + "::range::" + start + "::" + end;
    return hotKey.get(cacheKey, () -> redisTemplate.opsForList().range(key, start, end));
  }

  public Double zScore(String key, Object member) {
    return hotKey.get(key + "::score::" + member, () -> redisTemplate.opsForZSet().score(key, member));
  }
}
```

**G. Soft expire (Stale-While-Revalidate)**

Replaces traditional logical expiration — no embedded expiry in Redis values, entirely managed by L1.

| Dimension          | Traditional Logical Expiration                          | HotKey Soft Expire                                         |
| ------------------ | ------------------------------------------------------- | ---------------------------------------------------------- |
| Expiry storage     | Embedded in Redis value (`RedisData{data, expireTime}`) | L1 Caffeine metadata (`softExpireAt`)                      |
| Stale value return | Parse wrapper then return stale data                    | Direct L1 stale value return                               |
| Async rebuild      | Redis distributed lock + custom thread pool             | Singleflight (local) + `hotKeyExecutor` + `refreshLimiter` |
| Redis format       | Wrapped JSON                                            | Raw value (no wrapper)                                     |
| DB fallback        | Manual locking logic                                    | Native `orElseGet` / `orElseThrow`                         |

```java
// Traditional approach (no longer needed):
//   redisData.setExpireTime(LocalDateTime.now().plusSeconds(30L));
//   stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));

// HotKey: Redis stores raw values, soft expiry managed by L1
Optional<String> r = hotKey.getWithSoftExpire("user:123", () -> redisTemplate.opsForValue().get("user:123"));

// L1 hit but soft-expired → returns stale value + triggers async refresh
// L1 miss → singleflight load (same as get())

// Custom per-call softTtl (overrides global default)
Optional<String> r2 = hotKey.getWithSoftExpire("user:456", () -> redisTemplate.opsForValue().get("user:456"), 3000);
```

DB fallback (no distributed lock needed):

```java
String json = hotKey
  .getWithSoftExpire("shop:" + shopId, () -> redisTemplate.opsForValue().get("shop:" + shopId))
  .orElseGet(() -> {
    User u = userMapper.selectById(shopId);
    String s = JSONUtil.toJsonStr(u);
    if (u != null) redisTemplate.opsForValue().set("shop:" + shopId, s);
    return s;
  });

User user = JSONUtil.toBean(json, User.class);
```

**H. Custom per-entry hard TTL**

By default, hot and normal keys use different TTLs. Set independent hard and soft TTLs per entry via `get(key, reader, hardTtlMs, softTtlMs)` or `putThrough(key, value, writer, hardTtlMs, softTtlMs)`.

```java
// 5-minute hard TTL + 30-second soft TTL
Optional<Shop> shop = hotKey.get("shop:" + shopId,
    () -> redisTemplate.opsForValue().get("shop:" + shopId),
    TimeUnit.MINUTES.toMillis(5),
    TimeUnit.SECONDS.toMillis(30));

// Write with TTL
hotKey.putThrough("weather:" + city, weatherData,
    () -> redisTemplate.opsForValue().set("weather:" + city, weatherData),
    TimeUnit.SECONDS.toMillis(30), 0); // 30s hard TTL, no soft TTL
```

> **Per-call TTL semantics:**
>
> - Per-call `hardTtlMs`/`softTtlMs` apply to this call only. Next call without TTL parameters falls back to the key's current state (hot or normal) default.
> - Passing `0` uses the configured default for the key's state.
> - Passing `Long.MAX_VALUE` as `hardTtlMs` achieves permanent caching — the entry is never evicted by TTL (only by Caffeine `maximumSize`). Caffeine's `Expiry` JavaDoc explicitly supports this: _"To indicate no expiration an entry may be given an excessively long period, such as `Long.MAX_VALUE`."_ ([source](https://github.com/ben-manes/caffeine/blob/master/caffeine/src/main/java/com/github/benmanes/caffeine/cache/Expiry.java))
> - Pure logical expiry (soft-only expiry, hard TTL never evicts): pass `hardTtlMs = Long.MAX_VALUE` to `getWithSoftExpire(key, reader, Long.MAX_VALUE, softTtlMs)`. The entry permanently resides in Caffeine — hard TTL will never remove it. After `softExpireAt` expires, reads immediately return the stale value and trigger async refresh (soft expiry **does not** evict the entry). Without `Long.MAX_VALUE`, the default hard TTL may evict the entry from Caffeine first (L1 miss → higher latency), rather than the stale-value + async-refresh path.

**I. Worker mode**

Worker Mode provides cluster-wide hot key detection via a dedicated node. App instances periodically report access counts, the Worker runs a sliding-window + state-machine pipeline, and broadcasts HOT/COOL decisions back to all instances.

Two deployment modes:

| Mode        | `worker.enabled`  | Active Beans                                                                          |
| ----------- | ----------------- | ------------------------------------------------------------------------------------- |
| App-only    | `false` (default) | `HotKeyCache`, TopK, reporter, actuator, sync                                         |
| Worker-only | `true`            | Worker only (no cache — `get()`/`putThrough()` throw `UnsupportedOperationException`) |

In **Worker-only** mode, cache operations throw `UnsupportedOperationException`.

**J. @HotKey annotation**

The `@HotKey` annotation provides declarative caching for method return values — a drop-in AOP alternative to explicit `hotKey.get()` / `putBeforeInvalidate()` / `invalidate()` calls.

```java
@HotKey(key = "'user:' + #id", hardTtlMs = 5000)
public User getUser(Long id) { ... }
```

**Annotation attributes:**

| Attribute         | Type            | Default  | Description                                                                                                                                               |
| ----------------- | --------------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `key`             | `String`        | Required | SpEL expression referencing method parameters via `#paramName`. Requires `-parameters` compiler flag; falls back to `arg0, arg1, ...` without debug info. |
| `operation`       | `OperationType` | `READ`   | `READ` / `WRITE` / `INVALIDATE`                                                                                                                           |
| `hardTtlMs`       | `long`          | `0`      | Hard TTL override in ms. `0` = use configured default.                                                                                                    |
| `softTtlMs`       | `long`          | `0`      | Soft TTL override in ms. `0` = use configured default.                                                                                                    |
| `softExpire`      | `boolean`       | `true`   | Whether to enable stale-while-revalidate on READ. When `false`, behaves like `get()`.                                                                     |
| `fallbackEnabled` | `boolean`       | `false`  | Whether to enable fallback. Triggered by HeavyKeeper hot key detection or `RuntimeException`.                                                             |
| `fallback`        | `String`        | `""`     | SpEL fallback expression. Empty → `{methodName}Fallback` naming convention (method lookup on same bean).                                                  |

> **Hot key fallback:** When `fallbackEnabled=true` and HeavyKeeper detects the key as hot, the Aspect calls the fallback before the normal cache path — bypassing the supplier entirely. `RuntimeException` also triggers fallback. Two resolution modes: SpEL (`fallback` attribute) first, then naming convention `{methodName}Fallback`.

**Three operation modes:**

| Mode             | Facade method                    | Behavior                                                                                                                                                                                      |
| ---------------- | -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `READ` (default) | `getWithSoftExpire()` or `get()` | Method body acts as value supplier. `softExpire=true` → `getWithSoftExpire()` (stale value + async refresh); `false` → `get()`. Returns `Optional` directly, otherwise `orElse(null)` unwrap. |
| `WRITE`          | `putBeforeInvalidate()`          | Method executes as mutation: run, version bump, L1 invalidate, send INVALIDATE broadcast. Exceptions caught and re-thrown after facade call completes.                                        |
| `INVALIDATE`     | `invalidate()`                   | Invalidate L1 + version bump + broadcast TYPE_REFRESH (with version) to peers, then execute method.                                                                                           |

**SpEL key examples:**

```java
@HotKey(key = "'user:' + #id")
public User getUser(Long id) { ... }

@HotKey(key = "'shop:' + #shopId + ':item:' + #itemId")
public Item getItem(Long shopId, Long itemId) { ... }

@HotKey(key = "#user.id.toString()")
public Profile getProfile(User user) { ... }
```

**WRITE / INVALIDATE examples:**

```java
@HotKey(key = "'user:' + #id", operation = OperationType.WRITE)
public User updateUser(Long id, UserUpdate dto) { ... }

@HotKey(key = "'user:' + #id", operation = OperationType.INVALIDATE)
public void clearCache(Long id) { ... }
```

**Configuration:**

```yaml
hotkey:
  annotation:
    enabled: true
```

Requires `spring-boot-starter-aop` on the classpath (provides AspectJ). SpEL parameter name resolution requires the `-parameters` compiler flag (enabled by default in the parent POM).

> [!NOTE]
> `@HotKey(operation=READ)` wraps the original method call as an L2 supplier — the method body serves as both cache reader and database fallback. This is convenient for read-heavy endpoints but bypasses `peek()` semantics. `softExpire=true` (default) enables stale-while-revalidate: stale values are returned immediately while a background thread refreshes asynchronously. When `softExpire=false`, cache misses always invoke the method synchronously.

### 4. Degradation

HotKey forms a three-level degradation chain through the `supplier` callback:

```
hotKey.get(key, supplier)
  ├─ L1(Caffeine) HIT → return directly
  ├─ L1 MISS → supplier()
  │    ├─ Returns data → hot key? → write L1 (with appropriate TTL) + return
  │    ├─ Returns null → Optional.empty() → caller's orElseGet/orElseThrow
  │    │                     (null = miss — HotKey follows Caffeine's convention.
  │    │                      If your backend stores nullable values, wrap them
  │    │                      with a sentinel Object at the caller layer.)
  │    │
  │    │                      Example: Redis stores a nullable value
  │    │                      private static final Object NULL_SENTINEL = new Object();
  │    │                      Optional<Object> r = hotKey.get("k", () -> {
  │    │                          Object val = redisTemplate.opsForValue().get("k");
  │    │                          return val != null ? val : NULL_SENTINEL;
  │    │                      });
  │    │                      Object actual = r.orElse(null); // sentinel back to null
  │    └─ Throws → SingleFlight.load() catches → Optional.empty() → caller's fallback
  └─ HotKey itself fails → exception → fallback (if @HotKey fallbackEnabled=true) → caller
```

Component failure behavior:

| Failed component           | Impact                                                     | Recovery                            |
| -------------------------- | ---------------------------------------------------------- | ----------------------------------- |
| HotKey itself              | L1 unavailable; exception or hot-key fallback (if enabled) | Restart app                         |
| L2 backend (Redis/DB/API)  | Every request hits caller's fallback                       | Auto-recover on backend restoration |
| L1 Caffeine OOM / eviction | Individual keys evicted, next read re-fetches via supplier | Automatic (Caffeine internal)       |

> The caller is always responsible for handling `Optional.empty()` — HotKey never hides backend failures.

Write path failure behavior:

| Write method                                        | Failure scenario                 | Behavior                                                                                       |
| --------------------------------------------------- | -------------------------------- | ---------------------------------------------------------------------------------------------- |
| `putThrough`                                        | Executor queue full (outside tx) | `RejectedExecutionException` propagates to caller                                              |
| `putThrough`                                        | `writer.run()` / Redis fails     | Error logged on `hotKeyExecutor`, L1 version not updated, no broadcast                         |
| `putBeforeInvalidate`                               | `mutation.run()` throws          | Mutation exception caught and logged; local invalidate and broadcast skipped                   |
| `invalidate` / `putBeforeInvalidate` / `putThrough` | `nextVersion()` Redis fails      | Falls back to node-local counter (`Long.MIN_VALUE + counter`, non-persistent, `degraded=true`) |

Worker mode failure behavior:

| Failed component | Impact                                                       | Recovery                                 |
| ---------------- | ------------------------------------------------------------ | ---------------------------------------- |
| Worker crashes   | App instances continue with local TopK; no cluster consensus | Restart Worker; instances auto-reconnect |
| Report channel   | Reports queued/buffered (RabbitMQ)                           | Auto-recover on RabbitMQ restoration     |
| Worker broadcast | No cross-instance HOT/COOL sync; local TopK still functional | Restart Worker broadcaster               |

## HotKey API Reference

The recommended entry point is the `HotKey` facade (auto-configured as a Spring bean). Beyond the `get`/`peek`/`putThrough`/`putBeforeInvalidate` shown above, it exposes:

| Method                                                 | Description                                                                                                                                                             |
| ------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `peek(key)`                                            | Peek at L1 only — no frequency tracking, no L2 read, no reporting                                                                                                       |
| `get(key, reader)`                                     | Read from L1 or L2 via reader; triggers local TopK tracking + app-to-Worker report on every access; hot keys promoted to L1 with hot TTLs, normal keys with normal TTLs |
| `get(key, reader, hardTtlMs, softTtlMs)`               | Same with per-entry hard and soft TTL overrides (pass 0 to use configured default)                                                                                      |
| `getWithSoftExpire(key, reader)`                       | Soft expire — returns stale value + triggers async refresh; triggers local TopK tracking + app-to-Worker report on every access; uses global defaults per key state     |
| `getWithSoftExpire(key, reader, softTtlMs)`            | Same with per-call soft TTL override (ms)                                                                                                                               |
| `getWithSoftExpire(key, reader, hardTtlMs, softTtlMs)` | Same with both per-entry hard TTL and per-call soft TTL override (ms)                                                                                                   |
| `putThrough(key, value, writer)`                       | Write-through: writer.run(), nextVersion(), L1 update (with effective TTLs per key state), optional sync                                                                |
| `putThrough(key, value, writer, hardTtlMs, softTtlMs)` | Same with per-entry hard and soft TTL overrides (pass 0 to use configured default)                                                                                      |
| `putBeforeInvalidate(key, mutation)`                   | Write-then-invalidate for collection ops (LPUSH, SADD, ZADD)                                                                                                            |
| `isLocalHotKey(cacheKey)`                              | Check if a key is in HOT state in L1 (O(1))                                                                                                                             |
| `isWorkerHotKey(cacheKey)`                             | Check if a key is a cluster-wide hot key in the Worker TopK (O(n))                                                                                                      |
| `invalidate(cacheKey)`                                 | Invalidate a single key from all cache layers                                                                                                                           |
| `invalidateAll(cacheKeys...)`                          | Varargs overload — invalidate multiple keys at once                                                                                                                     |
| `invalidateAll(Collection)`                            | Collection overload                                                                                                                                                     |
| `returnLocalHotKeys()`                                 | Snapshot of current app-side Top-K entries (key + count)                                                                                                                |
| `returnLocalExpelledHotKeys()`                         | Access the app-side expelled hot key queue; drained periodically by scheduler                                                                                           |
| `returnLocalTotalDataStreams()`                        | Total number of reads passed through app-side HeavyKeeper                                                                                                               |
| `returnWorkerHotKeys()`                                | Snapshot of current Worker-side (cluster-wide) Top-K entries                                                                                                            |
| `returnWorkerExpelledHotKeys()`                        | Access the Worker-side expelled hot key queue                                                                                                                           |
| `returnWorkerTotalDataStreams()`                       | Total number of reads tracked by Worker-side HeavyKeeper                                                                                                                |

> [!NOTE]
> `invalidate()` generates a monotonic version via Redis `INCR` and broadcasts as `TYPE_REFRESH` with that version — peers reload the value from Redis via `CacheSyncListener`, skipping stale versions. `invalidateAll()` does **not** call `INCR` — it broadcasts as `TYPE_INVALIDATE_ALL` (no version header), so all peers unconditionally remove all listed keys from L1.

## TTL Reference

HotKey uses **differentiated TTLs**: hot keys and normal keys have separate default hard and soft TTLs. Per-call overrides work on top of these defaults.

Default TTLs by key state:

| Key state | Hard TTL (Caffeine eviction)   | Soft TTL (stale-while-revalidate) |
| --------- | ------------------------------ | --------------------------------- |
| Normal    | `default-hard-ttl-ms` (5min)   | `default-soft-ttl-ms` (30s)       |
| Hot       | `default-hot-hard-ttl-ms` (1h) | `default-hot-soft-ttl-ms` (5min)  |

Override via `hard-ttl-ms`, `hot-hard-ttl-ms`, `soft-ttl-ms`, `hot-soft-ttl-ms` (0 = use default).

Method-level TTL behavior:

| Method                                                 | TTL means                                                                                          |
| ------------------------------------------------------ | -------------------------------------------------------------------------------------------------- |
| `get(key, reader)`                                     | Uses effective TTLs based on key state (hot TTLs for hot keys, normal TTLs otherwise)              |
| `get(key, reader, hardTtlMs, softTtlMs)`               | Overrides both hard and soft TTL; pass 0 for either to use configured default                      |
| `getWithSoftExpire(key, reader)`                       | Returns stale value immediately + async refresh; TTLs per key state                                |
| `getWithSoftExpire(key, reader, softTtlMs)`            | Same with per-call soft TTL override (hard TTL from defaults)                                      |
| `getWithSoftExpire(key, reader, hardTtlMs, softTtlMs)` | Same with both overrides                                                                           |
| `putThrough(key, value, writer)`                       | L1 entry always uses normal-key TTLs (stored as KeyState.NORMAL)                                   |
| `putThrough(key, value, writer, hardTtlMs, softTtlMs)` | L1 entry uses overridden TTLs (0 = use configured default)                                         |
| `local-cache-ttl-minutes`                              | Write-based hard TTL for all entries (under `hotkey.local.*`; supplemented by differentiated TTLs) |
| `local-cache-access-ttl-minutes`                       | Access-based hard TTL (resets on read, under `hotkey.local.*`); supplements write-based TTL        |

> **Per-call semantics:** All per-call TTL overrides (hard and soft) are one-time only — the next call without the parameter falls back to the corresponding defaults for the key's state. The sole exception is soft-expire async refreshes, which preserve the original per-entry hard TTL.

## Cache Sync

Enable via `hotkey.sync.enabled=true`.

Each instance declares its own queue (`hotkey.sync:<instance-id>`) bound to a fanout exchange. Four message types:

- **`TYPE_REFRESH`** — Versioned refresh. Peers reload the value from Redis via `CacheSyncListener.handleRefresh()`, respecting the `dataVersion` header to skip stale updates. The 4-case comparison (normal-vs-normal, normal-vs-degraded, degraded-vs-normal, degraded-vs-degraded) guarantees that a normal (Redis INCR) dataVersion always wins over a degraded (node-local) one. Sent by `invalidate()` and `putThrough()`.
- **`TYPE_INVALIDATE`** — Single-key invalidation with version guard. Peers remove the key from L1 only when the incoming `dataVersion` is not stale. Sent by `putBeforeInvalidate()`.
- **`TYPE_INVALIDATE_ALL`** — Batch invalidation (no version guard). Peers immediately remove all keys from L1 without reloading. Sent by `invalidateAll()`.
- **`TYPE_RULES_SYNC`** — Rule set replacement. The body is a JSON-serialized `List<Rule>`; the receiver calls `RuleMatcher.syncRules()` to atomically swap the local rule list. No broadcast storm: `syncRules` saves to Redis if available but does **not** re-broadcast.

> [!SECURITY]
> All three RabbitMQ exchanges (`hotkey.sync.exchange`, `hotkey.report.exchange`, `hotkey.broadcast.exchange`) use plain AMQP connections by default. In production, configure TLS via Spring Boot's `spring.rabbitmq.ssl.*` properties:
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
> See [Spring Boot RabbitMQ SSL docs](https://docs.spring.io/spring-boot/reference/messaging/amqp.html#page-title) for details.

## Rule System

Enable via `hotkey.sync.enabled=true` for cross-instance rule sync. The rule system provides two actions:

| Action            | Effect on matched keys                                                                |
| ----------------- | ------------------------------------------------------------------------------------- |
| `BLOCK`           | `get()` / `getWithSoftExpire()` throws `HotKeyBlockedException`; `putThrough()` skips |
| `ALLOW_NO_REPORT` | Proceed normally but skip Worker report (reduces noise for frequently-accessed keys)  |

### Pattern Types

Rules are auto-detected by `RuleMatcher.of(pattern, action)`:

| Pattern             | Type       | Matches                         |
| ------------------- | ---------- | ------------------------------- |
| `"user:123"`        | `EXACT`    | Exact key                       |
| `"temp:*"`          | `PREFIX`   | Keys starting with `temp:`      |
| `"order:*-detail"`  | `WILDCARD` | Glob-style (`*` / `?`) matching |
| `"regex:user:\\d+"` | `REGEX`    | Java regex                      |

### Persistence & Broadcast

- **With Redis:** every `addRule()`/`removeRule()`/`clearRules()` serializes the rule list to `HotKeyConstants.REDIS_KEY_RULES` (`"hotkey:rules"`). On startup, `RuleMatcher.initRules()` loads from Redis. Changes also broadcast via `TYPE_RULES_SYNC` — peers atomically swap via `RuleMatcher.syncRules()` without re-broadcasting (avoids storms).
- **Without Redis:** same operations broadcast to all peers via the `CacheSyncPublisher` fanout exchange. Each peer holds the full rule set in memory.
- **Manual broadcast:** `hotKey.broadcastAllLocalRulesManually()` loads from Redis (if available) and re-broadcasts the current rule set to all peers.

### Programmatic Usage

```java
// Block access to sensitive keys
hotKey.addBlacklist("secret:*");
hotKey.addBlacklist("regex:token-\\d+");

// Whitelist keys to skip Worker reporting
hotKey.addWhitelist("health:*");
hotKey.addWhitelist("metrics:*");

// Inspect rules
List<Rule> rules = hotKey.getAllRules();
RuleAction action = hotKey.evaluateRule("user:123"); // BLOCK / ALLOW_NO_REPORT / ALLOW

// Remove rules
hotKey.removeBlacklist("secret:*");
hotKey.clearAllRules();
```



## Monitoring

When `spring-boot-starter-actuator` is on the classpath, the HotKey endpoint is automatically registered at `/actuator/hotkey`.

Enable via `management.endpoints.web.exposure.include=health,info,hotkey`.

The response is split into three sections — **local** (app-side detection, cache, reporting, rules, TTLs, version), **worker** (cluster-wide TopK, health, state machine), and **sync** (broadcast dedup). See [MONITOR.md](docs/MONITOR.md) for the full response schema and field descriptions. ([中文版](docs/MONITOR.zh.md))

## Architecture

See [ARCH.md](docs/ARCH.md) for detailed read/write path diagrams (also available in Chinese: [ARCH.zh.md](docs/ARCH.zh.md)).

## License

Apache License 2.0
