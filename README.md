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

// TO-DO: Worker Ping Heartbeat Mechanism

### Foreword: Why HotKey?

In real-world development, I frequently faced the challenge of managing a large number of cache keys. Manually maintaining Caffeine, Redis, and database multi-level caching, configuring logical expiration, and pre-computing and pre-warming hot keys — every step was tedious. Even more challenging, in a distributed cluster environment, ensuring hot keys are correctly shared across nodes and avoiding cache stampedes under high concurrency became a pain point that every developer must address.

My initial thought was simple: find a ready-to-use solution, or at least wrap a convenient multi-level cache utility class.

Unfortunately, existing solutions were either too complex (requiring high deployment and operational costs) or had been abandoned (such as JD's [hotkey](https://gitee.com/jd-platform-opensource/hotkey) project, which as of May 2026 is no longer maintained), making them unsuitable for lightweight, practical use. While many of its ideas aligned with my own, its heavy architecture convinced me to forge a different path.

That's the context in which HotKey was born: a lightweight, portable, ready-to-use hot key caching framework.

It will remain open source — past, present, and future.

> [!TIP]
> **Before you start, have questions?** See [FAQ.md](docs/FAQ.md) for answers to common questions about local vs central detection, Worker delay, MQ throughput, and more.

### Introduction

HotKey is a [high-performance](docs/HotKey_Benchmark_Report.en.md), low-cost, lightweight distributed multi-level caching framework

HotKey is not a general-purpose local cache — it's a lightweight hot key auto-detection & multi-level cache warming framework with optional distributed sync and an optional dedicated Worker node for cluster-wide hot key consensus.

Most local cache solutions store every accessed key in Caffeine. This works fine with small data, but under millions of keys:

- **Memory waste** — most keys are read once and never accessed again
- **Broadcast storm** — full cache invalidation requires full broadcast at scale

HotKey takes a different approach — **cache with differentiated TTLs.**

It uses [HeavyKeeper](https://github.com/go-kratos/aegis) (a Count-Min Sketch variant) to probabilistically detect access frequency. On the **read path**, all loaded keys enter the local Caffeine L1, but with **differentiated TTLs** — hot keys are cached longer (1h default), normal keys expire faster (5min). Optionally, hot keys can be synchronized across instances via RabbitMQ. On the **write path**, `putThrough` writes directly to L1 and broadcasts regardless of hot key status — the caller explicitly owns the write. Non-hot key reads still return values via the caller-supplied `Supplier<T>` — they are simply cached with shorter TTLs in L1.

For **cluster-wide detection**, deploy a dedicated Worker node that aggregates access reports from all instances — solving the single-instance blind spot where "accessed 100 times by one pod" and "accessed once by 100 pods" are indistinguishable locally.

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

  > **Note:** Ensure `hotkey.local.inflight-ttl-seconds` exceeds the slowest L2 response time for your workload, or the cache entry may expire before the future completes, causing duplicate L2 reads.
  > Also ensure `hotkey.local.inflight-timeout-seconds` < `hotkey.local.inflight-ttl-seconds`. On timeout, `SingleFlight.load()` returns `Optional.empty()` — the caller should handle via DB fallback.

- **Soft Expire (Logical Expiration)** — return stale L1 value immediately while asynchronously refreshing in the background; lower p99 at the cost of short-lived staleness. **Fully replaces traditional Redis-side logical expiration** (`RedisData{data, expireTime}` wrapper pattern) — Redis stores raw values, HotKey manages staleness at the L1 Caffeine level
- **Redis Collections** — `putBeforeInvalidate` for List/Set/ZSet incremental writes; no `putThrough` needed
- **Hot Key Sync** — optional RabbitMQ fanout (via `hotkey.sync.*`) to synchronize cache invalidations across instances; separate worker-listener (via `hotkey.worker-listener.*`) for receiving Worker-originated HOT/COOL decisions
- **Rule System (Blacklist / Whitelist)** — pattern-based `BLOCK` (throw `HotKeyBlockedException`) and `ALLOW_NO_REPORT` (skip Worker report) rules; auto-persisted to Redis or broadcast to cluster peers; sync across instances via RabbitMQ `TYPE_RULES_SYNC`
- **Worker Mode** — dedicated cluster-wide hot key detection node; sliding-window + state-machine pipeline for cross-instance consensus; see [WORKER.md](docs/WORKER.md)
- **Report Aggregation** — every `get()` / `getWithSoftExpire()` call reports to local `HotKeyReporter`, which periodically batches access counts to Worker node via RabbitMQ for cluster-wide hot key detection
- **Configurable Thread Pool** — dedicated `TaskExecutor` with bounded queue
- **Spring Boot Auto-Configuration** — drop-in dependency, zero boilerplate

## Degradation

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
    <version>1.1.2</version>
</dependency>
```

Use the latest release as the version. Redis and RabbitMQ dependencies are optional — include them only if you need the corresponding features.

### 2. Configure

```yaml
hotkey:
  local:
    top-k: 100
    width: 50000
    depth: 5
    decay: 0.92
    min-count: 10
    local-cache-max-size: 1000
    local-cache-ttl-minutes: 5
```

**Optional feature configs:**

| Feature              | Enable                                     | Description                            |
| -------------------- | ------------------------------------------ | -------------------------------------- |
| Redis L2             | Add `RedisTemplate` bean                   | Two-level cache with L2 fallback       |
| Cross-instance sync  | `hotkey.sync.enabled=true`                 | RabbitMQ-based cache invalidation      |
| Worker Listener      | `hotkey.worker-listener.enabled=true`      | Receive HOT/COOL decisions from Worker |
| Worker Mode          | `hotkey.worker.enabled=true`               | Run as dedicated Worker node           |
| `@HotKey` Annotation | `hotkey.annotation.enabled=true` + AspectJ | Declarative caching                    |
| Reporting            | `hotkey.report.enabled=true` (default)     | Report access counts to Worker         |

See [Configuration](#configuration) for all options and [CONFIG.md](docs/CONFIG.md) for the complete property reference.

<details>
<summary><b>Quick-deploy YAML templates</b> (lazy mode — only required overrides)</summary>

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

</details>

**All parameters (override defaults)**

```yaml
hotkey:
  # Cross-node consistency (app and worker sides MUST match)
  local:
    app-name: "default"                     # must match worker.routing.app-name
    shard-count: 1                          # must match worker.routing.shard-count

  worker:
    routing:
      app-name: "default"                   # must match local.app-name
      shard-count: 1                        # must match local.shard-count
      shard-index: 0                        # this instance's shard [0, shard-count-1]

  # App-side local cache + algorithm
  local:
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

  # TTL config (normal & hot keys)
  local:
    # ——— Normal keys ———
    default-hard-ttl-ms: 300000             # default hard TTL (5min)
    hard-ttl-ms: 0                          # override (0=use default)
    default-soft-ttl-ms: 30000              # default soft TTL (30s)
    soft-ttl-ms: 0                          # override (0=use default)

    # ——— Hot keys (effective after Worker HOT decision) ———
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
```

### 3. Use

> **Note:** From v1.0.2 includes a **breaking change** — `get(hk, fk)` and `putAndBroadcast(hk, fk, val)` are removed. The library is now decoupled from `RedisTemplate`; callers supply their own read/write callbacks via `Supplier<T>` / `Runnable`.

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
// Null sentinel (distinguishes "no data" from "cache miss")
static final Object NULL_SENTINEL = new Object();

Optional<Object> result = hotKey.get("user:" + userId, () -> {
  // 1. Check Redis
  String val = redisTemplate.opsForValue().get(cacheKey);
  if (val != null) return val;

  // 2. Check DB
  User user = userService.getById(userId);
  if (user != null) {
    String name = user.getName();
    redisTemplate.opsForValue().set(cacheKey, name, Duration.ofMinutes(10));
    return name;
  }

  // 3. DB has nothing either, cache empty value to prevent penetration
  redisTemplate.opsForValue().set(cacheKey, "", Duration.ofMinutes(1));
  return NULL_SENTINEL;
});

// Result handling
String userName = result
  .filter((v) -> v != NULL_SENTINEL)
  .map(Object::toString)
  .orElse(null);
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

For incremental operations (e.g. SADD), use `putBeforeInvalidate` to invalidate L1 — the next read re-fetches from Redis.

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

  public Set<Object> sMembers(String key) {
    return hotKey.get(key, () -> redisTemplate.opsForSet().members(key));
  }

  // Incremental mutation — invalidate L1
  public void sAdd(String key, Object... members) {
    hotKey.putBeforeInvalidate(key, () -> redisTemplate.opsForSet().add(key, members));
  }
}
```

**G. Soft expire (Stale-While-Revalidate)**

Replaces traditional logical expiration — no embedded expiry in Redis values, entirely managed by L1.

```java
// Soft expire: L1 hit but soft-expired → returns stale value + async refresh; miss → singleflight load
Optional<String> r = hotKey.getWithSoftExpire("user:123", () -> redisTemplate.opsForValue().get("user:123"));

// Per-call custom soft TTL
Optional<String> r2 = hotKey.getWithSoftExpire("user:456", () -> redisTemplate.opsForValue().get("user:456"), 3000); // soft TTL = 3s
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

Pure logical expiry: pass `hardTtlMs = Long.MAX_VALUE` to prevent time-based eviction — entries are only removed by Caffeine `maximumSize`. Soft expire only triggers async refresh, never removes entries. See [Configuration](#configuration) for TTL property options.

**H. Custom per-entry TTL**

Set independent hard and soft TTLs per entry.

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

Pass `0` to use the key state's (normal/hot) default. Pass `Long.MAX_VALUE` for no expiry ([Caffeine docs](https://github.com/ben-manes/caffeine/blob/master/caffeine/src/main/java/com/github/benmanes/caffeine/cache/Expiry.java)).

**I. Worker mode**

For cluster-wide hot key detection, enable a dedicated Worker node. See [WORKER.md](docs/WORKER.md).

**J. @HotKey annotation**

Declarative caching via AOP — no explicit API calls needed.

```java
@HotKey(key = "'user:' + #id", hardTtlMs = 5000)
public User getUser(Long id) { ... }
```

Supports `READ` / `WRITE` / `INVALIDATE` operations with SpEL dynamic cache keys.
Enable by adding `spring-boot-starter-aop` and setting `hotkey.annotation.enabled=true`.

**Fallback on hot key:** When `fallbackEnabled=true` and HeavyKeeper detects the key as hot (in Top-K), the aspect calls the fallback _before_ the normal cache path — bypassing the supplier entirely. Falls back on `RuntimeException` too. Two resolution modes: SpEL (`fallback` attribute) or naming convention (`{methodName}Fallback`).

**Key config**

The framework offers rich configuration — TTLs, window parameters, Worker sharding, etc. See [CONFIG.md](docs/CONFIG.md). Core cross-node parameters that MUST match:

- `app-name`: identical across all nodes
- `shard-count`: same on App and Worker
- Exchange names: consistent between App and Worker

**Null handling**

If the reader returns `null`, the framework treats it as a miss and returns `Optional.empty()`. To distinguish "no data" from "cache miss", use a null sentinel (see scenario C).

## HotKey API Reference

The recommended entry point is the `HotKey` facade (auto-configured as a Spring bean). Beyond the `get`/`peek`/`putThrough`/`putBeforeInvalidate` shown above, it exposes:

### Cache Operations

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
| `invalidate(cacheKey)`                                 | Invalidate a single key from all cache layers                                                                                                                           |
| `invalidateAll(cacheKeys...)`                          | Varargs overload — invalidate multiple keys at once                                                                                                                     |
| `invalidateAll(Collection)`                            | Collection overload                                                                                                                                                     |

### Hot Key Inspection

| Method                           | Description                                                                   |
| -------------------------------- | ----------------------------------------------------------------------------- |
| `isLocalHotKey(cacheKey)`        | Check if a key is in HOT state in L1 (O(1))                                   |
| `isWorkerHotKey(cacheKey)`       | Check if a key is a cluster-wide hot key in the Worker TopK (O(n))            |
| `returnLocalHotKeys()`           | Snapshot of current app-side Top-K entries (key + count)                      |
| `returnLocalExpelledHotKeys()`   | Access the app-side expelled hot key queue; drained periodically by scheduler |
| `returnLocalTotalDataStreams()`  | Total number of reads passed through app-side HeavyKeeper                     |
| `returnWorkerHotKeys()`          | Snapshot of current Worker-side (cluster-wide) Top-K entries                  |
| `returnWorkerExpelledHotKeys()`  | Access the Worker-side expelled hot key queue                                 |
| `returnWorkerTotalDataStreams()` | Total number of reads tracked by Worker-side HeavyKeeper                      |

### Rule System

| Method                             | Description                                                                                                |
| ---------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `addBlacklist(keyPattern)`         | Add a block rule — pattern auto-detected (exact, prefix, wildcard, regex). Throws `HotKeyBlockedException` |
| `removeBlacklist(keyPattern)`      | Remove a block rule by pattern                                                                             |
| `addWhitelist(keyPattern)`         | Add an allow-no-report rule — matched keys skip Worker report but still participate in local detection     |
| `removeWhitelist(keyPattern)`      | Remove an allow-no-report rule by pattern                                                                  |
| `getAllRules()`                    | Return a snapshot of all current rules in evaluation order                                                 |
| `clearAllRules()`                  | Remove all rules (both blacklist and whitelist)                                                            |
| `evaluateRule(cacheKey)`           | Return the `RuleAction` for a key without throwing — for programmatic inspection                           |
| `broadcastAllLocalRulesManually()` | Force-broadcast the current rule set to all peers (useful when Redis is absent)                            |

> **Note:** `invalidate()` generates a monotonic version via Redis `INCR` and broadcasts as `TYPE_REFRESH` with that version — peers reload the value from Redis via `CacheSyncListener`, skipping stale versions. `invalidateAll()` does **not** call `INCR` — it broadcasts as `TYPE_INVALIDATE_ALL` (no version header), so all peers unconditionally remove all listed keys from L1.

## Configuration

Minimal setup shown in [Quick Start](#2-configure). For the complete property list, see [CONFIG.md](docs/CONFIG.md). For Worker mode, see [WORKER.md](docs/WORKER.md).

### TTL Override Properties

Override via `hard-ttl-ms`, `hot-hard-ttl-ms`, `soft-ttl-ms`, `hot-soft-ttl-ms` (0 = use default). See [TTL Reference](#ttl-reference) for defaults and per-method behavior.

### Sync & Worker Listener

```yaml
hotkey:
  sync:
    enabled: true # Cross-instance cache sync
  worker-listener:
    enabled: true # Receive Worker HOT/COOL decisions
```

### Worker Mode

Enable `hotkey.worker.enabled=true`. Two deployment modes — see [Worker Mode](#worker-mode) and [WORKER.md](docs/WORKER.md).

### Monitoring

Enable via `management.endpoints.web.exposure.include=health,info,hotkey`.

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
| Legacy `local-cache-ttl-minutes`                       | Write-based hard TTL for all entries (under `hotkey.local.*`; supplemented by differentiated TTLs) |
| Legacy `local-cache-access-ttl-minutes`                | Access-based hard TTL (resets on read, under `hotkey.local.*`); supplements write-based TTL        |

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

### Worker Listener

For receiving HOT/COOL decisions from a dedicated Worker node, enable `hotkey.worker-listener.enabled=true`.

See [WORKER.md](docs/WORKER.md) for detailed Worker mode setup.

## Worker Mode

Worker Mode provides cluster-wide hot key detection via a dedicated node. App instances periodically report access counts, the Worker runs a sliding-window + state-machine pipeline, and broadcasts HOT/COOL decisions back to all instances.

Two deployment modes:

| Mode        | `worker.enabled`  | Active Beans                                                                          |
| ----------- | ----------------- | ------------------------------------------------------------------------------------- |
| App-only    | `false` (default) | `HotKeyCache`, TopK, reporter, actuator, sync                                         |
| Worker-only | `true`            | Worker only (no cache — `get()`/`putThrough()` throw `UnsupportedOperationException`) |

In **Worker-only** mode, cache operations throw `UnsupportedOperationException`.

For full documentation on Worker setup, state machine, sliding window, and configuration, see [WORKER.md](docs/WORKER.md).

## Monitoring

When `spring-boot-starter-actuator` is on the classpath, the HotKey endpoint is automatically registered at `/actuator/hotkey`.

Enable via `management.endpoints.web.exposure.include=health,info,hotkey`.

```json
{
  "topK": [{ "key": "cache:shop:17", "count": 1523 }],
  "topKCount": 1,
  "totalRequests": 158392,
  "l1CacheSize": 87,
  "l1MaxSize": 1000,
  "inflightSize": 3,
  "recentlyExpelled": ["cache:shop:5", "cache:shop:99"],
  "workerTopK": [{ "key": "cache:shop:17", "count": 8921 }],
  "workerTopKCount": 1,
  "workerTotalRequests": 784512,
  "workerRecentlyExpelled": ["cache:shop:3"]
}
```

| Field                       | Description                                   |
| --------------------------- | --------------------------------------------- |
| `topK`                      | App-side Top-K hot keys (descending by count) |
| `topKCount`                 | Number of hot keys in app-side Top-K set      |
| `totalRequests`             | Total requests through app-side detection     |
| `l1CacheSize` / `l1MaxSize` | L1 Caffeine current size / max limit          |
| `inflightSize`              | Current in-flight dedup requests              |
| `recentlyExpelled`          | Recently evicted keys from app-side Top-K     |
| `workerTopK`                | Worker-side (cluster-wide) Top-K hot keys     |
| `workerTopKCount`           | Number of hot keys in Worker-side Top-K set   |
| `workerTotalRequests`       | Total requests through Worker-side detection  |
| `workerRecentlyExpelled`    | Recently evicted keys from Worker-side Top-K  |

## Architecture

See [ARCH.md](docs/ARCH.md) for detailed read/write path diagrams (also available in Chinese: [ARCH.zh.md](docs/ARCH.zh.md)).

## Method Call Chain

See [METHODS.md](docs/METHODS.md) for detailed method call chain diagrams (also available in Chinese: [METHODS.zh.md](docs/METHODS.zh.md)).

## License

Apache License 2.0
