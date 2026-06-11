# HotKey

<p align="center">
  <img src="img/HotKey.png" alt="HotKey" width="300">
</p>

<p align="center">
  <a href="https://jitpack.io/#Hyshmily/HotKey"><img src="https://jitpack.io/v/Hyshmily/HotKey.svg" alt="JitPack"></a>
  <a href="https://www.apache.org/licenses/LICENSE-2.0"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
  <a href="https://openjdk.java.net/"><img src="https://img.shields.io/badge/Java-17-orange" alt="Java"></a>
  <a href="https://spring.io/projects/spring-boot"><img src="https://img.shields.io/badge/Spring%20Boot-3.5.3-brightgreen" alt="Spring Boot"></a>
</p>

[**中文**](README.zh.md)

HotKey is a high-performance, low-cost, lightweight distributed caching framework that integrates **cache read/write (get/put), automatic hot key detection, multi-level cache warming, cross-instance broadcast synchronization, AOP annotation interception, and blacklist/whitelist filtering**.

Most local caches store every entry in Caffeine. But under massive key volumes:

- **Memory waste** — most keys are read only once
- **Broadcast storms** — full cache invalidation means broadcast messages grow linearly with key count

HotKey's strategy: **only cache the truly hot data.**

Via [HeavyKeeper](https://github.com/go-kratos/aegis) (a Count-Min Sketch variant), it enables **cluster-level hot key detection**: deploy Worker nodes that aggregate access reports from all instances — solving the problem of "accessed 100 times by the same pod" vs "accessed once each by 100 pods" that is indistinguishable locally.

## Features

- **HeavyKeeper algorithm** — Count-Min Sketch + probabilistic top-K detection with exponential conflict decay
- **Multi-level cache** — Caffeine (L1) → optional reader callback (L2, e.g. Redis) + DB fallback via caller's `Optional.orElseGet()`, automatic hot key promotion
- **Differentiated TTL** — Hot keys and normal keys use independent hard/soft TTLs; hot keys cache longer (1h/5min), normal keys expire sooner (5min/30s)
- **Request coalescing** — Concurrent requests for the same key on L1 miss share a single L2 read via a dedicated `SingleFlight` bean
- **Soft expiry (logical expiration)** — Returns stale data immediately while refreshing asynchronously in the background; reduces p99 latency at the cost of brief stale reads. **Fully replaces traditional Redis-side logical expiry** (`RedisData{data, expireTime}` wrapper pattern) — Redis stores plain values, HotKey manages expiration at the L1 Caffeine layer
- **Redis collection types** — Supports incremental writes to List/Set/ZSet via `putBeforeInvalidate`, no `putThrough` required
- **Hot key synchronization** — Optional RabbitMQ fanout (via `hotkey.sync.*`) for cross-instance cache invalidation; dedicated worker-listener (via `hotkey.worker-listener.*`) receives HOT/COOL decisions from Worker
- **Worker mode** — Dedicated cluster-level hot key detection nodes; cross-instance consensus via sliding window + state machine pipeline; runtime state machine configuration via `/actuator/hotkey/worker/state` REST endpoint with heartbeat-based peer-to-peer config propagation; see [Worker Mode](#worker-mode)
- **Report aggregation** — Every `get()` / `getWithSoftExpire()` call reports to the local `HotKeyReporter`, which periodically batches access counts to Worker nodes (RabbitMQ) for cluster-level hot key detection
- **TTL jitter (cache avalanche protection)** — `CacheExpireManager` applies &#177;10% random jitter to each hard/soft TTL via `ThreadLocalRandom`, scattering expiration timestamps to prevent cache avalanches
- **Consistent hashing (default)** — Murmur3_32-based consistent hash ring for dynamic Worker routing via heartbeats; elastic scaling without static shard configuration
- **Spring Boot auto-configuration** — Add the dependency and it works, zero boilerplate

## Latency & Performance

> [!WARNING]
> **Extreme performance**
>
> Setting some parameters to extreme values can compress end-to-end latency to **~9ms** (P50), but the author **does not recommend** this for production.
>
> HotKey's defaults are conservative — the framework should prioritize distributed cluster reliability over extreme latency:
>
> The extreme tuning demonstrates **this framework's full-chain performance ceiling** (~9ms) and serves as a reference for extreme scenarios. Unless you fully understand and accept the trade-offs (higher false positive rate, reduced statistical accuracy, increased client CPU load, etc.), use the defaults or thoroughly tested parameters.

<details>
<summary><b>Click to expand — full end-to-end latency breakdown and extreme tuning details</b></summary>

See the [Benchmark Report](docs/HotKey_Benchmark_Report.en.md).

**Extreme parameter adjustments**

| Parameter                                             | Extreme value           | Effect                                                                                   | Limitation                                        |
| ----------------------------------------------------- | ----------------------- | ---------------------------------------------------------------------------------------- | ------------------------------------------------- |
| `hotkey.local.report-interval-ms`                     | 0 &#8594; min 1         | Nearly disables batching, `record()` flushes almost immediately                          | `ScheduledExecutorService` requires period > 0    |
| `hotkey.worker-listener.warmup-jitter-ms`             | 0                       | Disables warmup jitter (herd effect protection lost), Worker decisions execute instantly | —                                                 |
| `hotkey.sync.warmup-jitter-ms`                        | 0                       | Same as above, cross-instance sync listener                                              | —                                                 |
| `hotkey.worker.state-machine.confirm-duration-ms`     | 0                       | Disables state machine confirmation window, first hot window immediately broadcasts HOT  | —                                                 |
| `hotkey.worker.sliding-window.duration-ms` / `slices` | 1000/10 &#8594; 100/100 | Time slice from 100ms to 1ms, reduces tick wait (avg ~50ms &#8594; ~0.5ms)               | Window statistical accuracy significantly reduced |

**Measured extreme latency** ([Extreme tuning comparison](docs/img/extreme_tuning_comparison.png)):

| Scenario                 | Default (3 confirm windows) P50 | Extreme (0 confirm windows) P50 | Extreme P95 | Extreme P99 |
| ------------------------ | ------------------------------- | ------------------------------- | ----------- | ----------- |
| Worker decision pipeline | 56.38 ms                        | **2.41 ms**                     | 11.89 ms    | 12.40 ms    |
| SM confirmation pipeline | 246.46 ms                       | **7.71 ms**                     | 8.53 ms     | 8.53 ms     |
| Full chain               | 298.19 ms                       | **9.23 ms**                     | 10.93 ms    | 10.93 ms    |

**The state machine broadcasts only once per key per lifecycle**, fundamentally eliminating redundant broadcasts that cause client CPU overload and self-inflicted congestion. In extreme testing, the SM 0-confirm path generated only 10 broadcasts, while the stateless-machine path generated massive redundant broadcasts, dramatically amplifying latency.

**The default 300ms confirmation window** (3 confirm windows &#215; 100ms slices) is not sluggishness — it uses 300ms of continuous observation to achieve near-zero false positives in global decision-making. During those 300ms, local HeavyKeeper provides nanosecond-level protection, with zero blocking on user requests.

**Warmup jitter and batch intervals** are classic distributed-system mechanisms for "anti-herd" and "load smoothing", sacrificing tens of milliseconds of latency for cluster-wide stability.

</details>

## Quick Start

### 1. Add Dependency (JitPack)

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
    <version>1.1.4</version>
</dependency>
```

### 2. Configuration

Default local configuration works for most scenarios.

> [!IMPORTANT]
> In distributed deployments, the author strongly recommends adding Redis alongside RabbitMQ for best results, although many fallback mechanisms are built into the core call chain.

**Optional feature configuration:**

| Feature              | How to Enable                              | Description                            |
| -------------------- | ------------------------------------------ | -------------------------------------- |
| Redis L2 cache       | Add `RedisTemplate` Bean                   | Two-level cache, L2 fallback           |
| Cross-instance sync  | `hotkey.sync.enabled=true`                 | RabbitMQ-based cache invalidation      |
| Worker Listener      | `hotkey.worker-listener.enabled=true`      | Receive HOT/COOL decisions from Worker |
| Worker mode          | `hotkey.worker.enabled=true`               | Run dedicated Worker nodes             |
| `@HotKey` annotation | `hotkey.annotation.enabled=true` + AspectJ | Declarative caching                    |
| Access reporting     | `hotkey.report.enabled=true` (default)     | Report access counts to Worker         |

See [Configuration](#configuration) for all options. Full property reference: [CONFIG.md](docs/CONFIG.md).

<details>
<summary><b>Quick deployment YAML templates</b></summary>

**Local (App-side)** — Add the `hotkey` dependency and you're ready; uncomment optional features as needed.

```yaml
hotkey:
  # All local parameters use defaults; explicit config is optional

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
  #   enabled: true     # worker-listener requires hotKeyRedisLoader Bean

  # Consistent hashing is enabled by default (dynamic Worker routing via heartbeats)
  # local:
  #   consistent-hashing:
  #     enabled: true     # (already default)
```

**Single Worker (standalone node)** — Add `spring-boot-starter-amqp`

```yaml
hotkey:
  worker:
    enabled: true
    routing:
      app-name: myapp # [Required] Must match App-side hotkey.local.app-name
      # Consistent hashing is the default; Workers auto-register via heartbeats


# Multi-Worker example: 3 machines, same app-name
# Consistent hashing auto-routes keys to the correct Worker via heartbeats
# No static shard configuration needed — just add more machines
```

**All parameters (overriding defaults)**

```yaml
hotkey:
  local:
    # ——— Must match across nodes (App-side and Worker-side) ———
    app-name: "default"                    # Must match worker.routing.app-name

    # ——— Instance identification ———
    instance-id: ""                        # Explicit instance ID ("" = auto-generated)

    # ——— Report publishing ———
    report-exchange: "hotkey.report.exchange"
    report-interval-ms: 100                # Batch send interval (ms)
    queue-capacity: 10000                  # Report queue capacity
    queue-offer-timeout-ms: 100            # Queue write timeout (ms)
    consumer-count: 0                      # Consumer thread count (0=auto)

    # ——— HeavyKeeper algorithm ———
    topK: 100                              # Hot keys to retain
    width: 50000                           # Count-Min Sketch width
    depth: 5                               # Count-Min Sketch depth
    decay: 0.92                            # Decay factor
    minCount: 10                           # Minimum access count for hot key
    expelled-queue-capacity: 50000         # Expelled hot key queue capacity

    # ——— L1 Caffeine cache ———
    local-cache-max-size: 1000             # Maximum entries
    local-cache-ttl-minutes: 5             # Cache expiry (minutes)
    local-cache-access-ttl-minutes: 0      # Post-access expiry (0=no expiry)

    # ——— SingleFlight deduplication ———
    inflight-max-size: 50000               # Maximum dedup keys
    inflight-ttl-seconds: 5                # Dedup record TTL (must > L2 response time)
    inflight-timeout-seconds: 3            # Async result wait timeout (must < inflight-ttl-seconds)

    # ——— Async executor ———
    executor-core-pool-size: 8             # Core threads
    executor-max-pool-size: 32             # Max threads
    executor-queue-capacity: 2000          # Work queue capacity

    # ——— Refresh & version control ———
    refresh-max-pools: 100                 # Refresh thread pool limit
    version-key-ttl-minutes: 60            # Redis version key TTL

    # ——— Normal key TTL ———
    default-hard-ttl-ms: 300000            # Default hard expiry (5min)
    hard-ttl-ms: 0                         # Override (0=use default)
    default-soft-ttl-ms: 30000             # Default soft expiry (30s)
    soft-ttl-ms: 0                         # Override (0=use default)

    # ——— Hot key TTL (active after Worker marks HOT) ———
    default-hot-hard-ttl-ms: 3600000       # Default hot hard expiry (1h)
    hot-hard-ttl-ms: 0                     # Override (0=use default)
    default-hot-soft-ttl-ms: 300000        # Default hot soft expiry (5min)
    hot-soft-ttl-ms: 0                     # Override (0=use default)

    # ——— Consistent hashing (default; dynamic Worker routing) ———
    consistent-hashing:
      enabled: true                        # Dynamic Worker routing via heartbeats
      virtual-nodes: 500                   # Virtual nodes per physical Worker

  # Feature toggles
  report:
    enabled: true                          # App&#8594;Worker report publishing
  sync:
    enabled: false                         # Cross-instance cache sync (requires Redis + RabbitMQ)
  annotation:
    enabled: false                         # @HotKey annotation support (requires spring-boot-starter-aop)
  scheduling:
    enabled: true                          # Periodic decay & expelled key cleanup
  decay-period: 20                         # HeavyKeeper fading interval (seconds), applies when scheduling enabled

  # App-side — Worker decision listener (receives HOT / COOL)
  worker-listener:
    enabled: false                         # Requires Redis + RabbitMQ
    exchange-name: "hotkey.broadcast.exchange"
    queue-prefix: "hotkey.worker"
    auto-startup: true                     # Start with application
    warmup-jitter-ms: 100                  # Random delay before processing (anti-herd)
    concurrent-consumers: 2                # Concurrent consumers
    scheduler-pool-size: 2                 # Delayed task thread pool size

  # App-side — Cross-instance cache sync
  sync:
    enabled: false
    exchange-name: "hotkey.sync.exchange"
    queue-prefix: "hotkey.sync"
    auto-startup: true
    dedup-window-seconds: 10               # Message dedup window
    dedup-max-size: 10000                  # Dedup cache limit
    warmup-jitter-ms: 100                  # Random delay before processing (anti-herd)
    concurrent-consumers: 3                # Concurrent consumers
    scheduler-pool-size: 4                 # Delayed task thread pool size

  # Worker-side — Standalone deployment node
  worker:
    enabled: false

    routing:
      app-name: "default"                  # Must match local.app-name
      # Routing is auto-managed via consistent hashing and heartbeats

    messaging:
      report-exchange: "hotkey.report.exchange"
      broadcast-exchange: "hotkey.broadcast.exchange"

    sliding-window:
      duration-ms: 1000                    # Window duration (ms)
      slices: 10                           # Window slices (must divide duration-ms evenly)

    threshold:
      hot-threshold: 1000                  # Absolute QPS threshold (&#8804;0 = use ratio threshold)
      hot-threshold-ratio: 0.01            # Relative QPS ratio threshold (1%)

    state-machine:
      confirm-duration-ms: 300             # Confirmation window (ms), sustained heat beyond this = HOT
      cool-duration-ms: 15000              # Cooling window (ms), sustained cool beyond this = COOL
      pre-cool-grace-ms: 5000              # Pre-cooling grace period (ms)

    global-qps-dynamic-threshold:
      qps-change-tolerance: 0.5            # QPS change tolerance multiplier
      learning-period-ms: 30000            # Learning period (ms)
      hot-threshold-ratio: 0.01            # Dynamic threshold ratio
      recalculate-interval-ms: 60000       # Recalculation interval (ms)

    topk-validation:
      validate-interval-ms: 60000          # Validation interval (ms)
      pre-warm-count: 5                    # Pre-warm count
      pre-warm-min-appearances: 2          # Minimum appearances

    heavy-keeper:
      top-k: 100                           # Hot keys to retain
      width: 20000                         # Sketch width
      depth: 10                            # Sketch depth
      decay: 0.9                           # Decay factor
      min-count: 10                        # Minimum count

    heartbeat:
      ping-interval-ms: 1000               # Heartbeat broadcast interval (ms)
```

</details>

### 3. Usage

**A. Local cache only (no L2)**

```java
@Autowired
private HotKey hotKey;

Optional<String> r = hotKey.peek("user:123"); // L1 only, no hot key tracking
```

Equivalent to `peek(cacheKey)` — returns `Optional.empty()` on L1 miss, fully skipping secondary storage.

**B. Two-level cache (Redis or any backend)**

```java
@Autowired
private HotKey hotKey;
@Autowired
private StringRedisTemplate redisTemplate;

Optional<String> r = hotKey.get("user:123", () -> redisTemplate.opsForValue().get("user:123"));

hotKey.putThrough("user:123", newValue, () -> redisTemplate.opsForValue().set("user:123", newValue));
```

**C. Database fallback**

```java
Optional<String> r = hotKey.get("user:123", () -> redisTemplate.opsForValue().get("user:123"));
if (r.isEmpty()) {
    String value = userService.getById(123);                         // DB fallback
    redisTemplate.opsForValue().set("user:123", value);
}
// Note: if reader returns null, HotKey treats it as a miss (Optional.empty()). Callers must handle null values themselves (e.g., with a sentinel object).

@Autowired
private HotKey hotKey;

public String getUserName(String userId) {
  String cacheKey = "user:" + userId;

  Optional<Object> result = hotKey.get(cacheKey, () -> {
    // 1. Check Redis first
    String value = redisTemplate.opsForValue().get(cacheKey);
    if (value != null) {
      return value; // Redis hit
    }

    // 2. Redis miss, query DB
    User user = userService.getById(userId);
    if (user != null) {
      // 3. DB hit, write back to Redis and return
      String userName = user.getName();
      redisTemplate.opsForValue().set(cacheKey, userName, Duration.ofMinutes(10));
      return userName;
    }

    // 4. DB miss too, return sentinel to prevent cache penetration
    redisTemplate.opsForValue().set(cacheKey, "", Duration.ofMinutes(1));
    return NULL_SENTINEL;
  });

  return result
    .filter(val -> val != NULL_SENTINEL)
    .map(Object::toString)
    .orElse(null);
}
```

> [!WARNING]
> **Cache penetration protection — why HotKey does not include a Bloom filter?**
>
> HotKey does not include a built-in Bloom filter. Cache penetration protection is **the caller's responsibility**, not the framework's:
>
> | Strategy                  | HotKey's Position                                                 |
> | ------------------------- | ----------------------------------------------------------------- |
> | `NULL_SENTINEL` sentinel  | Caller implements in the `reader` (demonstrated above)            |
> | `@Blacklist` rule engine  | Built-in — intercepts known attack patterns like `user:-\\d+`     |
> | Bloom filter (Guava)      | Caller adds in the `reader` — false positive cost is the caller's |
> | Gateway layer (Nginx/WAF) | Most effective — intercepted before reaching the app              |
>
> If you still want a Bloom filter, add one line in your reader:
>
> ```java
> BloomFilter<String> bf = BloomFilter.create(Funnels.stringFunnel(UTF_8), 10_000, 0.01);
>
> Optional<Object> r = hotKey.get("user:123", () -> {
>   if (bf.mightContain("user:123")) return null;
>   Object val = dao.findById(123);
>   if (val == null) bf.put("user:123");
>   return val;
> });
> ```

**D. Helper class to avoid repeating lambdas**

```java
@Component
public class RedisHotKeyHelper {

  @Autowired
  private HotKey hotKey;

  @Autowired
  private RedisTemplate<String, Object> redisTemplate;

  public <T> Optional<T> get(String key) {
    return hotKey.get(key, () -> redisTemplate.opsForValue().get(key));
  }

  public void set(String key, Object value) {
    hotKey.putThrough(key, value, () -> redisTemplate.opsForValue().set(key, value));
  }
}
```

**E. Custom L2 cache (non-Redis)**

```java
// Use MySQL, remote API, or any data source as L2
Optional<User> r = hotKey.get("user:123", () -> userMapper.selectById(123));

User user = r.orElseGet(() -> createDefaultUser());
```

**F. Redis collection types (List, Set, ZSet)**

`putThrough` requires the full new value to update L1, but incremental collection operations (LPUSH, SADD, ZADD) only modify individual elements — the caller cannot know the full new value. Use `putBeforeInvalidate` to invalidate L1 after mutation; the next `get()` will fall back to Redis, guaranteeing consistency.

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

**G. Soft expiry — replacing traditional logical expiration**

Soft expiry returns stale data immediately while refreshing asynchronously in the background (stale-while-revalidate). Unlike traditional logical expiration (embedding an `expireTime` in the Redis value), HotKey manages expiration entirely at the L1 Caffeine layer — **Redis stores plain values with no wrapping**.

| Dimension      | Traditional Logical Expiry                              | HotKey Soft Expiry                                         |
| -------------- | ------------------------------------------------------- | ---------------------------------------------------------- |
| Expiry storage | Embedded in Redis value (`RedisData{data, expireTime}`) | L1 Caffeine metadata (`softExpireAt`)                      |
| Stale return   | Parse wrapper, return stale data                        | Return L1 stale value directly                             |
| Async rebuild  | Redis distributed lock + custom thread pool             | Singleflight (local) + `hotKeyExecutor` + `refreshLimiter` |
| Redis format   | Wrapped JSON                                            | Plain value (no wrapper)                                   |
| DB fallback    | Manual locking logic                                    | Native `orElseGet` / `orElseThrow`                         |

```java
// Traditional approach (no longer needed):
//   redisData.setExpireTime(LocalDateTime.now().plusSeconds(30L));
//   stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));

// HotKey: Redis stores plain values, soft expiry managed by L1
Optional<String> r = hotKey.getWithSoftExpire("user:123", () -> redisTemplate.opsForValue().get("user:123"));

// L1 hit but soft-expired &#8594; return stale value + trigger async refresh
// L1 miss &#8594; singleflight fallback (same as get())

// Custom per-call softTtl (overrides global default)
Optional<String> r2 = hotKey.getWithSoftExpire("user:456", () -> redisTemplate.opsForValue().get("user:456"), 3000);
```

DB fallback (no distributed lock required):

```java
String json = hotKey
  .getWithSoftExpire("shop:" + shopId, () -> redisTemplate.opsForValue().get("shop:" + shopId))
  .orElseGet(() -> {
    User u = userMapper.selectById(shopId);
    String s = JSONUtil.toJsonStr(u);
    if (u != null) {
      redisTemplate.opsForValue().set("shop:" + shopId, s);
    }
    return s;
  });

User user = JSONUtil.toBean(json, User.class);
```

**H. Custom per-entry hard TTL**

By default, hot keys and normal keys use different TTLs. Use `get(key, reader, hardTtlMs, softTtlMs)` or `putThrough(key, value, writer, hardTtlMs, softTtlMs)` to set independent hard and soft TTLs for a single entry.

```java
// 5-minute hard TTL + 30-second soft TTL
Optional<Shop> shop = hotKey.get("shop:" + shopId,
    () -> redisTemplate.opsForValue().get("shop:" + shopId),
    TimeUnit.MINUTES.toMillis(5),   // hardTtlMs
    TimeUnit.SECONDS.toMillis(30)); // softTtlMs

// 30-second hard TTL, no soft TTL
hotKey.putThrough("weather:" + city, weatherData,
    () -> redisTemplate.opsForValue().set("weather:" + city, weatherData),
    TimeUnit.SECONDS.toMillis(30),  // hardTtlMs
    0);                              // softTtlMs (uses default)
```

> [!TIP]
> Per-call TTL semantics:
>
> Per-call `hardTtlMs`/`softTtlMs` only apply to the current invocation. On the next call without TTL params, it falls back to the key state's (hot or normal) default TTL.
> Passing `0` means "use the config default for this key state."
> **Fully logical expiry (soft-only, hard TTL never evicts):** Pass `hardTtlMs = Long.MAX_VALUE` to `getWithSoftExpire(key, reader, Long.MAX_VALUE, softTtlMs)`. The entry stays in Caffeine permanently — hard TTL will never remove it. After `softExpireAt` passes, reads return stale data immediately and trigger async refresh (soft expiry **does not** evict the entry). Without `Long.MAX_VALUE`, the default hard TTL may evict the entry from Caffeine first (L1 miss &#8594; higher latency) rather than taking the stale+refresh path.
> Passing `Long.MAX_VALUE` as `hardTtlMs` implements permanent caching — the entry will never be TTL-evicted (only evictable via Caffeine `maximumSize`). Caffeine's `Expiry` Javadoc explicitly supports this: _"To indicate no expiration an entry may be given an excessively long period, such as `Long.MAX_VALUE`."_ ([source](https://github.com/ben-manes/caffeine/blob/master/caffeine/src/main/java/com/github/benmanes/caffeine/cache/Expiry.java))

**I. Worker mode**

Worker mode provides cluster-level hot key detection via dedicated nodes. App instances periodically report access counts, Workers run a sliding window + state machine pipeline, and broadcast HOT/COOL decisions to all instances. State machine parameters (`confirmCount`, `coolCount`, `preCoolGraceCount`) can be adjusted at runtime via `/actuator/hotkey/worker/state` — changes propagate to all peer Workers through heartbeat-based AMQP broadcast.

Two deployment modes:

| Mode        | `worker.enabled`  | Activated Beans                                                                        |
| ----------- | ----------------- | -------------------------------------------------------------------------------------- |
| App-only    | `false` (default) | `HotKeyCache`, TopK, reporter, actuator, sync                                          |
| Worker-only | `true`            | Worker only (no cache — `get()`/`putThrough()` throws `UnsupportedOperationException`) |

In **Worker-only** mode, cache operations throw `UnsupportedOperationException`.

**J. @HotKey annotation**

> Full documentation: [`docs/ANNOTATION.md`](docs/ANNOTATION.md) — covers companion annotations (`@Fallback`, `@Intercept`, `@HotKeyCacheTTL`), priority chain, SpEL condition/unless support, return type handling, and complete examples.

`@HotKey` provides declarative caching for method return values — an AOP-based alternative that eliminates the need for explicit `hotKey.get()` / `putBeforeInvalidate()` / `invalidate()` calls.

```java
@HotKey(key = "'user:' + #id")
public User getUser(Long id) { ... }
```

**Prerequisites:**

```yaml
hotkey:
  annotation:
    enabled: true
```

Requires `spring-boot-starter-aop` on the classpath (provides AspectJ). SpEL parameter name resolution requires the `-parameters` compiler flag (enabled by default in HotKey's parent POM).

### 4. Degradation

HotKey provides a three-level degradation chain via `supplier` callbacks:

```
hotKey.get(key, supplier)
  ├─ L1(Caffeine) hit &#8594; promote if locally hot &#8594; return directly
  ├─ L1 miss &#8594; supplier()
  │    ├─ returns data &#8594; hot key? &#8594; write L1 (with appropriate TTL) + return
  │    ├─ returns null &#8594; Optional.empty() &#8594; caller's orElseGet/orElseThrow
  │    │                     (null = miss. HotKey follows Caffeine convention;
  │    │                      if your backend stores nullable values,
  │    │                      wrap them with a sentinel object on the caller side)
  │    │
  │    │                     Example: Redis stores nullable values
  │    │                     private static final Object NULL_SENTINEL = new Object();
  │    │                     Optional<Object> r = hotKey.get("k", () -> {
  │    │                         Object val = redisTemplate.opsForValue().get("k");
  │    │                         return val != null ? val : NULL_SENTINEL;
  │    │                     });
  │    │                     Object actual = r.orElse(null); // sentinel back to null
  │    └─ throws exception &#8594; SingleFlight.load() catches &#8594; Optional.empty() &#8594; caller fallback
  └─ HotKey internal error &#8594; exception &#8594; degradation (@Fallback if present) &#8594; caller
```

Component failure behavior:

| Failed Component          | Impact                                                  | Recovery                            |
| ------------------------- | ------------------------------------------------------- | ----------------------------------- |
| HotKey itself             | L1 unavailable; exception or degraded mode (if enabled) | App restart                         |
| L2 backend (Redis/DB/API) | Each request falls through to caller                    | Auto-recovery when backend recovers |
| L1 Caffeine OOM/eviction  | Single key evicted, next read falls back to source      | Automatic (Caffeine internal)       |

> Callers must always handle `Optional.empty()` — HotKey never hides backend failures.

Write path failure behavior:

| Write method                                        | Failure scenario                | Behavior                                                                                      |
| --------------------------------------------------- | ------------------------------- | --------------------------------------------------------------------------------------------- |
| `putThrough`                                        | Thread pool queue full (non-tx) | `RejectedExecutionException` propagated to caller                                             |
| `putThrough`                                        | `writer.run()` / Redis fails    | Error logged, L1 version not updated, no broadcast sent                                       |
| `putBeforeInvalidate`                               | `mutation.run()` throws         | Mutation exception caught and logged; local invalidation and broadcast skipped                |
| `invalidate` / `putBeforeInvalidate` / `putThrough` | `nextVersion()` Redis fails     | Falls back to node-local counter (`Long.MIN_VALUE + counter`, non-persisted, `degraded=true`) |

Worker mode failure behavior:

| Failed Component         | Impact                                                                                      | Recovery                                                            |
| ------------------------ | ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| Worker crash (all)       | Local TopK drives L1 TTLs; COOL entries promoted to HOT; Worker verdicts degrade gracefully | Restart Worker cluster; Worker broadcasts override local promotions |
| Worker crash (partial)   | Unaffected shards continue normally                                                         | Restart crashed Worker; auto-reconnect                              |
| Report channel failure   | Reports queue/buffer (RabbitMQ)                                                             | Auto-recovery when RabbitMQ recovers                                |
| Worker broadcast failure | No cross-instance HOT/COOL sync; local TopK works fine                                      | Restart Worker broadcaster                                          |

## HotKey Facade API Reference

The recommended entry point is the `HotKey` facade (auto-configured as a Spring Bean). In addition to the `get`/`peek`/`putThrough`/`putBeforeInvalidate` methods shown above:

| Method                                                 | Description                                                                                                                                                                                                                                    |
| ------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `peek(key)`                                            | L1 lookup only, no frequency tracking, no L2 read, no reporting                                                                                                                                                                                |
| `getLocalCache()`                                      | Exposes the raw Caffeine `Cache<String, Object>` for Caffeine-specific operations (asMap, policy, cleanUp). &#9888;&#65039; Bypasses HotKey orchestration — version tracking, broadcast, and expiry management are all skipped. Local L1 only. |
| `get(key, reader)`                                     | Read from L1 or L2 reader; every access triggers local TopK tracking + App&#8594;Worker reporting; hot keys promoted to L1 (with hot TTL), normal keys use normal TTL                                                                          |
| `get(key, reader, hardTtlMs, softTtlMs)`               | Same as above, with per-entry hard and soft TTL override (pass 0 to use default)                                                                                                                                                               |
| `getWithSoftExpire(key, reader)`                       | Soft expiry — returns stale data + triggers async refresh; every access triggers local TopK tracking + App&#8594;Worker reporting; uses global default TTLs based on key state                                                                 |
| `getWithSoftExpire(key, reader, softTtlMs)`            | Same as above, with per-call soft TTL override (ms)                                                                                                                                                                                            |
| `getWithSoftExpire(key, reader, hardTtlMs, softTtlMs)` | Same as above, with both per-entry hard TTL and per-call soft TTL override (ms)                                                                                                                                                                |
| `putThrough(key, value, writer)`                       | Write-through: writer.run(), nextVersion(), L1 update (with effective TTL based on key state), optional sync                                                                                                                                   |
| `putThrough(key, value, writer, hardTtlMs, softTtlMs)` | Same as above, with per-entry hard and soft TTL override (pass 0 to use default)                                                                                                                                                               |
| `putBeforeInvalidate(key, mutation)`                   | Write then invalidate, for incremental collection operations (LPUSH, SADD, ZADD)                                                                                                                                                               |
| `isLocalHotKey(cacheKey)`                              | Check if key is HOT in L1 (O(1))                                                                                                                                                                                                               |
| `isWorkerHotKey(cacheKey)`                             | Check if key is a cluster hot key in Worker TopK (O(n))                                                                                                                                                                                        |
| `notifyLocalDetector(cacheKey)`                        | Triggers local HeavyKeeper tracking for a key without performing a full cache read. Used by `@Intercept` to keep TopK accurate when the method body is skipped.                                                                                |
| `invalidate(cacheKey)`                                 | Invalidate a single key across all cache layers                                                                                                                                                                                                |
| `invalidateAll(cacheKeys...)`                          | Varargs overload — batch invalidate multiple keys                                                                                                                                                                                              |
| `invalidateAll(Collection)`                            | Collection overload                                                                                                                                                                                                                            |
| `returnLocalHotKeys()`                                 | App-side Top-K snapshot (key + count)                                                                                                                                                                                                          |
| `returnLocalExpelledHotKeys()`                         | Get app-side expelled hot key queue; periodically drained by internal timer                                                                                                                                                                    |
| `returnLocalTotalDataStreams()`                        | Cumulative reads through app-side HeavyKeeper                                                                                                                                                                                                  |
| `returnWorkerHotKeys()`                                | Worker-side (cluster-level) Top-K snapshot                                                                                                                                                                                                     |
| `returnWorkerExpelledHotKeys()`                        | Worker-side expelled hot key queue                                                                                                                                                                                                             |
| `returnWorkerTotalDataStreams()`                       | Worker-side HeavyKeeper cumulative reads                                                                                                                                                                                                       |

> [!NOTE]
> `invalidate()` generates a monotonic version number via Redis `INCR` and broadcasts `TYPE_REFRESH` — the peer reloads from Redis via `CacheSyncListener`, skipping stale versions. `invalidateAll()` does **not** call `INCR` — it broadcasts `TYPE_INVALIDATE_ALL` (no version header), and all peers unconditionally remove all listed keys from L1.

## TTL Reference

HotKey uses **differentiated TTLs**: hot keys and normal keys have independent default hard and soft TTLs. Per-call overrides apply on top of these.

Default TTLs by key state:

| Key State | Hard TTL (Caffeine eviction)   | Soft TTL (stale-while-revalidate) |
| --------- | ------------------------------ | --------------------------------- |
| Normal    | `default-hard-ttl-ms` (5min)   | `default-soft-ttl-ms` (30s)       |
| Hot       | `default-hot-hard-ttl-ms` (1h) | `default-hot-soft-ttl-ms` (5min)  |

Override via `hard-ttl-ms`, `hot-hard-ttl-ms`, `soft-ttl-ms`, `hot-soft-ttl-ms` (0 = use default).

> [!NOTE]
> **Cache avalanche protection:** `CacheExpireManager` applies &#177;10% uniform random jitter via `ThreadLocalRandom` when calculating each expiration timestamp. A 5-minute hard TTL actually expires between 4.5 ~ 5.5 minutes, preventing many keys from expiring simultaneously. The jitter ratio is hardcoded at 10%.

Method-level TTL behavior:

| Method                                                 | TTL Meaning                                                                            |
| ------------------------------------------------------ | -------------------------------------------------------------------------------------- |
| `get(key, reader)`                                     | Uses effective TTL based on current key state (hot uses hot TTL, otherwise normal TTL) |
| `get(key, reader, hardTtlMs, softTtlMs)`               | Overrides hard and soft TTL; pass 0 to use config defaults                             |
| `getWithSoftExpire(key, reader)`                       | Returns stale data immediately + async refresh; TTL determined by key state            |
| `getWithSoftExpire(key, reader, softTtlMs)`            | Same as above, with per-call soft TTL override (hard TTL from defaults)                |
| `getWithSoftExpire(key, reader, hardTtlMs, softTtlMs)` | Same as above, overriding both                                                         |
| `putThrough(key, value, writer)`                       | L1 entry always uses normal key TTL (stored with `KeyState.NORMAL`)                    |
| `putThrough(key, value, writer, hardTtlMs, softTtlMs)` | L1 entry uses override TTL (0 = use config default)                                    |
| Legacy `local-cache-ttl-minutes`                       | Write TTL (under `hotkey.local.*`; supplemented by differentiated TTL)                 |
| Legacy `local-cache-access-ttl-minutes`                | Access-based hard TTL (reset on each read, under `hotkey.local.*`)                     |

> **Per-call semantics:** All per-call TTL overrides (hard and soft) are one-shot — the next call without params falls back to the key state's default TTL. The sole exception is that soft-expiry background refresh preserves the original per-entry hard TTL.

## Cache Synchronization

Enable with `hotkey.sync.enabled=true`.

Each instance declares an exclusive queue (`hotkey.sync:<instanceID>`) bound to a fanout exchange. Four message types:

- **`TYPE_REFRESH`** — Refresh with version number. Peers `CacheSyncListener.handleRefresh()` reload from Redis, skipping stale updates based on `dataVersion`. A 4-case comparison (normal-normal, normal-degraded, degraded-normal, degraded-degraded) ensures that normal (Redis INCR) `dataVersion` always takes priority over degraded (node-local) versions. Sent by `invalidate()` and `putThrough()`.
- **`TYPE_INVALIDATE`** — Single-key invalidation (with version guard). Peers only remove the L1 entry if the incoming `dataVersion` is not stale. Sent by `putBeforeInvalidate()`.
- **`TYPE_INVALIDATE_ALL`** — Batch invalidation (no version guard). Peers immediately remove all listed keys from L1, no reload. Sent by `invalidateAll()`.
- **`TYPE_RULES_SYNC`** — Rule set replacement. Body is a JSON-serialized `List<Rule>`; the receiver calls `RuleMatcher.syncRules()` to atomically replace the local rule list. Does not trigger secondary broadcast.

> [!SECURITY]
> All three RabbitMQ exchanges (`hotkey.sync.exchange`, `hotkey.report.exchange`, `hotkey.broadcast.exchange`) default to plain AMQP connections. In production, configure TLS via Spring Boot's `spring.rabbitmq.ssl.*` properties:
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
> See [Spring Boot RabbitMQ SSL documentation](https://docs.spring.io/spring-boot/reference/messaging/amqp.html#page-title).

## Rule System

Enable `hotkey.sync.enabled=true` to enable cross-instance rule sync. The rule system provides two actions:

| Action            | Effect on matching keys                                                                    |
| ----------------- | ------------------------------------------------------------------------------------------ |
| `BLOCK`           | `get()` / `getWithSoftExpire()` throws `HotKeyBlockedException`; `putThrough()` skips      |
| `ALLOW_NO_REPORT` | Normal processing but skips Worker reporting (reduces noise from frequently accessed keys) |

### Pattern Types

`RuleMatcher.of(pattern, action)` auto-detects the pattern:

| Pattern             | Type       | Matches                    |
| ------------------- | ---------- | -------------------------- |
| `"user:123"`        | `EXACT`    | Exact key                  |
| `"temp:*"`          | `PREFIX`   | Keys starting with `temp:` |
| `"order:*-detail"`  | `WILDCARD` | Glob-style (`*` / `?`)     |
| `"regex:user:\\d+"` | `REGEX`    | Java regex                 |

### Persistence & Broadcast

- **With Redis:** Every `addRule()`/`removeRule()`/`clearRules()` serializes the rule list to `HotKeyConstants.REDIS_KEY_RULES` (`"hotkey:rules"`). `RuleMatcher.initRules()` loads from Redis on startup. Changes also broadcast via `TYPE_RULES_SYNC` — peers call `RuleMatcher.syncRules()` for atomic replacement, no secondary broadcast (storm prevention).
- **Without Redis:** Same operations broadcast via `CacheSyncPublisher` fanout exchange to all peers. Each peer holds the full rule set in memory.
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

HotKey provides two complementary monitoring mechanisms.

See [MONITOR.md](docs/MONITOR.md) for full response formats and field descriptions.

## Architecture & Design Details

See [ARCH.md](docs/ARCH.md) for detailed architecture documentation.

For domain terminology definitions, see [CONTEXT.md](CONTEXT.md).
Architectural Decision Records (ADRs) are maintained in [docs/adr/](docs/adr/0001-local-promotion-worker-fallback.md).

## License

Apache License 2.0
