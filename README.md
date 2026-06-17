# HotKey

<p align="center">
  <a href="https://central.sonatype.com/artifact/io.github.hyshmily/hotkey"><img src="https://img.shields.io/maven-central/v/io.github.hyshmily/hotkey?color=blue" alt="Maven Central"></a>
  <a href="https://jitpack.io/#Hyshmily/HotKey"><img src="https://jitpack.io/v/Hyshmily/HotKey.svg" alt="JitPack"></a>
  <a href="https://coveralls.io/github/hyshmily/hotkey"><img src="https://coveralls.io/repos/github/hyshmily/hotkey/badge.svg" alt="Coveralls"></a>
  <a href="https://www.apache.org/licenses/LICENSE-2.0"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
  <a href="https://openjdk.java.net/"><img src="https://img.shields.io/badge/Java-17-orange" alt="Java"></a>
  <a href="https://spring.io/projects/spring-boot"><img src="https://img.shields.io/badge/Spring%20Boot-3.5.3-brightgreen" alt="Spring Boot"></a>
</p>

[**中文**](README.zh.md)

HotKey is a highly customizable, [high-performance](docs/HotKey_Benchmark_Report.en.md), low-cost, lightweight distributed caching framework that integrates **cache read/write (get/put), automatic hot key detection, multi-level cache pre-warming, cross-instance broadcast synchronization, AOP annotation interception, and allow/blocklist filtering**.

Most local caches store every entry uniformly in Caffeine. But under a massive key space:

- **Memory waste** — most keys are read only once
- **Broadcast storms** — full cache invalidation means broadcast messages grow linearly with key count
- **Cache avalanche** — massive keys share the same TTL, expire simultaneously and all penetrate to the DB

HotKey's strategy: **cache only the truly hot data.**

Via [HeavyKeeper](https://github.com/go-kratos/aegis) (a Count-Min Sketch variant), it enables **cluster-level hot key detection**: deploy Worker nodes that aggregate access reports from all instances — solving the problem of "accessed 100 times by the same pod" vs "accessed once each by 100 pods" that is indistinguishable locally.

HotKey is inspired by [hotkey](https://gitee.com/jd-platform-opensource/hotkey) (JD's open-source hot key middleware). Algorithm support comes from [Aegis](https://github.com/go-kratos/aegis) (Kratos' HeavyKeeper implementation).

## Features

<details>
<summary><b>Click to expand detailed HotKey features</b></summary>

- **HeavyKeeper algorithm** — Count-Min Sketch + probabilistic top-K detection with exponential conflict decay
- **Multi-level cache** — Caffeine (L1) → optional reader callback (L2, e.g. Redis) + DB fallback via caller's `Optional.orElseGet()`, automatic hot key promotion
- **Differentiated TTL** — Hot keys and normal keys use independent hard/soft TTLs; hot keys cache longer (1h/5min), normal keys expire sooner (5min/30s)
- **Request coalescing** — Concurrent requests for the same key on L1 miss share a single L2 read via a dedicated `SingleFlight` bean
- **Soft expiry (logical expiration)** — Returns stale data immediately while refreshing asynchronously in the background; reduces p99 latency at the cost of brief stale reads. **Fully replaces traditional Redis-side logical expiry** (`RedisData{data, expireTime}` wrapper pattern) — Redis stores plain values, HotKey manages expiration at the L1 Caffeine layer
- **Redis collection types** — Supports incremental writes to List/Set/ZSet via `putBeforeInvalidate`, no `putThrough` required
- **Hot key synchronization** — Optional RabbitMQ fanout (via `hotkey.sync.*`) for cross-instance cache invalidation; dedicated worker-listener (via `hotkey.worker-listener.*`) receives HOT/COOL decisions from Worker
- **Worker mode** — Dedicated cluster-level hot key detection nodes; cross-instance consensus via sliding window + state machine pipeline; runtime state machine configuration via `/actuator/hotkey/worker/state` REST endpoint with heartbeat-based peer-to-peer config propagation; see [Worker Mode](#worker-mode)
- **Report aggregation** — Every `get()` / `getWithSoftExpire()` call reports to the local `HotKeyReporter`, which periodically batches access counts to Worker nodes (RabbitMQ) for cluster-level hot key detection
- **TTL jitter (cache avalanche protection)** — `CacheExpireManager` applies configurable random jitter to each hard/soft TTL via `ThreadLocalRandom`, scattering expiration timestamps to prevent cache avalanches (default ±10%, controlled by `hotkey.local.ttl-jitter-enabled` / `ttl-jitter-ratio`)
- **Consistent hashing (default)** — Murmur3_32-based consistent hash ring for dynamic Worker routing via heartbeats; elastic scaling without static shard configuration
- **BBR adaptive rate limiting** — Self-protection via BBR congestion control fused with CPU EMA monitoring; backpressure at the Reporter flush path to prevent RabbitMQ/Worker overload
- **SRE adaptive rate limiting** — WorkerListener HOT-path backpressure using Google SRE formula (`K = 1 / successThreshold`, probabilistic drop when success rate degrades); independent of BBR, protects the HOT decision consumption path
- **CPU monitoring with EMA smoothing** — Dedicated daemon thread polls process CPU load every 500ms with configurable EMA decay for stable overload detection
- **Spring Boot auto-configuration** — Add the dependency and it works, zero boilerplate
- **Worker TopK persistence** — Periodic snapshots of the Worker's HeavyKeeper to Redis; restart recovery in seconds instead of hours of re-accumulation
- **Spring Cache integration** — Standard `@Cacheable` / `@CachePut` / `@CacheEvict` with HotKey hot-key detection, soft-expire, cross-instance sync, and companion annotations (`@HotKeyCacheTTL`, `@Intercept`, `@Fallback`, `@NullCaching`) for TTL, intercept, fallback, and null-caching; gated by `hotkey.spring-cache.enabled=true`
- **Transaction support** — `TransactionSupport` defers cache writes until after Spring `@Transactional` commits, ensuring cache-data consistency on the write path

</details>

## Use Cases & Limitations

HotKey is designed as a **read-hotspot governance** framework, not a general-purpose distributed cache or distributed lock.

### When to Use

| Scenario | Why HotKey Fits |
|---|---|
| **Product detail / pricing page** | During flash sales, a few SKUs see traffic surges; HeavyKeeper auto-detects → extends local TTL → reduces Redis penetration |
| **Viral article / video metadata** | Sudden trending content; HotKey auto-identifies + cross-instance broadcast proactively warms every pod's L1 |
| **Config / rule / dictionary cache** | Read-frequent, write-rare; cross-instance sync via AMQP broadcast keeps all pods consistent |
| **User session / permission snapshot** | Hot users (VIP operations) automatically get extended L1 TTL for their permission cache |
| **Pre-sale warmup (read-side only)** | Coupon face values, product images, descriptions — any read-only field caching **alongside** a separate atomic inventory system |

### When NOT to Use

| Scenario | Why Not | What to Use Instead |
|---|---|---|
| **Atomic inventory deduction** (flash sale stock) | HotKey has no cross-JVM atomicity guarantee; `VersionGuard` is optimistic, transient inconsistencies self-heal on next heartbeat cycle (ADR-0006) | Redis Lua script or distributed lock (Redisson) |
| **Distributed locking** | HotKey provides no distributed lock abstraction | Redisson, ZooKeeper |
| **Strong consistency writes** | Cross-instance sync is at-most-once with self-healing broadcasts (ADR-0004, ADR-0007) | Database transaction, Paxos/Raft |
| **Uniform traffic (no hotspots)** | No detectable hot keys → HotKey provides no benefit over bare Caffeine | Plain Caffeine cache |

> [!WARNING]
> **Extreme performance**
>
> Setting certain parameters to extreme values can reduce full-chain latency to **~9ms** (P50).
>
> HotKey's default parameters are conservatively tuned and support extensive customization to suit different performance requirements.

<details>
<summary><b>Click to expand — full chain latency breakdown and extreme tuning guide</b></summary>

See the [Benchmark Report](docs/HotKey_Benchmark_Report.en.md).

**Extreme parameter tuning**

| Parameter                                             | Extreme value     | Effect                                                                                          | Limitation                                       |
| ----------------------------------------------------- | ----------------- | ----------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| `hotkey.local.report-interval-ms`                     | 0 → min 1         | Nearly disables report batching, `record()` flushes almost instantly                            | `ScheduledExecutorService` requires period > 0   |
| `hotkey.worker-listener.warmup-jitter-ms`             | 0                 | Disables warm-up jitter (thundering herd protection lost), Worker decisions execute immediately | —                                                |
| `hotkey.sync.warmup-jitter-ms`                        | 0                 | Same, cross-instance sync listener                                                              | —                                                |
| `hotkey.worker.state-machine.confirm-duration-ms`     | 0                 | Disables state machine confirmation window, broadcasts HOT from the first hot window            | —                                                |
| `hotkey.worker.sliding-window.duration-ms` / `slices` | 1000/10 → 100/100 | Time slice from 100ms down to 1ms, reduces tick wait (avg ~50ms → ~0.5ms)                       | Window statistics accuracy significantly reduced |

**Measured extreme latency** ([extreme tuning comparison](docs/img/extreme_tuning_comparison.png)):

| Scenario                 | Default (3 confirm windows) P50 | Extreme (0 confirm) P50 | Extreme P95 | Extreme P99 |
| ------------------------ | ------------------------------- | ----------------------- | ----------- | ----------- |
| Worker decision pipeline | 56.38 ms                        | **2.41 ms**             | 11.89 ms    | 12.40 ms    |
| SM confirmation pipeline | 246.46 ms                       | **7.71 ms**             | 8.53 ms     | 8.53 ms     |
| Full chain               | 298.19 ms                       | **9.23 ms**             | 10.93 ms    | 10.93 ms    |

**The state machine broadcasts each key only once per lifetime**, fundamentally eliminating redundant broadcast-induced CPU overload and self-inflicted congestion. In extreme tests, the SM 0-confirm path produced only 10 broadcasts, while the stateless path produced massive redundant broadcasts, dramatically amplifying latency.

**The default 300ms confirmation window** (3 confirm windows × 100ms slices) is not about sluggishness — it trades 300ms of continuous observation for near-zero false positives in global decisions. During that 300ms, the local HeavyKeeper provides nanosecond-scale protection with zero blocking on user requests.

**Warm-up jitter and batch intervals** are classic distributed system techniques — sacrificing tens of milliseconds of latency for cluster-wide stability.

</details>

## Quick Start

### 1. Add Dependency

**Maven Central** (no extra repository needed):

```xml
<dependency>
    <groupId>io.github.hyshmily</groupId>
    <artifactId>hotkey</artifactId>
    <version>1.1.5.Beta</version>
</dependency>
```

**JitPack** (always latest snapshot):

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
    <version>1.1.5.Beta</version>
</dependency>
```

### 2. Configuration

Default local configuration (suitable for most scenarios):

> [!IMPORTANT]
> In distributed deployments, RabbitMQ and Redis are required.

**Optional feature configuration:**

| Feature                  | How to enable                                  | Description                            |
| ------------------------ | ---------------------------------------------- | -------------------------------------- |
| Redis L2 cache           | Add `RedisTemplate` bean                       | Two-level cache, L2 fallback           |
| Cross-instance sync      | `hotkey.sync.enabled=true`                     | RabbitMQ-based cache invalidation      |
| Worker Listener          | `hotkey.worker-listener.enabled=true`          | Receive HOT/COOL decisions from Worker |
| Worker mode              | `hotkey.worker.enabled=true`                   | Run dedicated Worker node              |
| Worker TopK persistence  | `hotkey.worker.persistence.enabled=true`       | Hot start from Redis after restart     |
| Access reporting         | `hotkey.report.enabled=true` (default)         | Report access counts to Worker         |
| Reporter self-protection | `hotkey.local.reporter.enabled=true` (default) | BBR backpressure on Reporter flush     |
| Spring Cache integration | `hotkey.spring-cache.enabled=true`             | `@Cacheable` / `@CachePut` / `@CacheEvict` with HotKey hot-key detection |

All options listed under [Configuration](#configuration). Full property reference at [CONFIG.md](docs/CONFIG.md).

<details>
<summary><b>Quick deploy YAML template</b></summary>

**Local (App side)** — just add the `hotkey` dependency; uncomment as needed:

```yaml
hotkey:
  # local parameters all use defaults, no explicit config needed

  # —— Optional features, uncomment as needed ——

  # Cross-instance cache sync (requires spring-boot-starter-amqp + spring-boot-starter-data-redis)
  # sync:
  #   enabled: true

  # Spring Cache integration (requires spring-boot-starter-cache)
  # spring-cache:
  #   enabled: true

  # Worker decision listener (requires spring-boot-starter-amqp + spring-boot-starter-data-redis)
  # worker-listener:
  #   enabled: true
  # sync:
  #   enabled: true     # worker-listener depends on hotKeyRedisLoader bean

  # Consistent hashing enabled by default (dynamic Worker routing via heartbeats)
  # local:
  #   consistent-hashing:
  #     enabled: true     # (enabled by default)
```

**Single Worker (standalone node)** — add `spring-boot-starter-amqp`:

```yaml
hotkey:
  worker:
    enabled: true
    routing:
      app-name: myapp # 【required】must match App-side hotkey.local.app-name
      # Consistent hashing enabled by default; Worker auto-registers via heartbeat

# Multi-Worker example: 3 machines, same app-name
# Consistent hashing routes keys to the correct Worker automatically via heartbeat
# No static shard configuration needed — just add machines
```

**Full parameters (override defaults):**

```yaml
hotkey:
  local:
    # ——— Must match across all nodes (App and Worker sides) ———
    app-name: "default"                     # must match worker.routing.app-name

    # ——— Instance identity ———
    instance-id: ""                         # explicit instance ID (empty = auto-generated)

    # ——— Report publishing ———
    report-exchange: "hotkey.report.exchange"
    report-interval-ms: 100                 # batch send interval (ms)
    queue-capacity: 10000                   # report queue capacity
    queue-offer-timeout-ms: 100             # queue write timeout (ms)
    consumer-count: 0                       # consumer thread count (0=auto)

    # ——— HeavyKeeper algorithm ———
    topK: 100                               # number of hot keys to track
    width: 50000                            # Count-Min Sketch width
    depth: 5                                # Count-Min Sketch depth
    decay: 0.92                             # decay factor
    minCount: 10                            # minimum access count for hot key
    expelled-queue-capacity: 50000          # expelled hot key queue capacity

    # ——— L1 Caffeine cache ———
    local-cache-max-size: 1000             # maximum entries
    local-cache-ttl-minutes: 5             # cache TTL (minutes)

    # ——— SingleFlight dedup ———
    inflight-max-size: 50000                # max dedup key count
    inflight-ttl-seconds: 5                 # dedup record TTL (must > L2 response time)
    inflight-timeout-seconds: 3             # async result wait timeout (must < inflight-ttl-seconds)

    # ——— Async executor ———
    executor-core-pool-size: 8              # core threads
    executor-max-pool-size: 32              # max threads
    executor-queue-capacity: 2000           # work queue capacity
    scheduler-pool-size: 8                  # scheduled task thread pool

    # ——— TTL Jitter (Cache Avalanche Protection) ———
    ttl-jitter-enabled: true                # enable TTL random jitter
    ttl-jitter-ratio: 0.1                   # jitter ratio (0.0~1.0), 0.1 = ±10%

    # ——— Refresh & version control ———
    refresh-max-pools: 100                  # refresh thread pool limit
    version-key-ttl-minutes: 60             # Redis version key TTL

    # ——— Normal key TTL ———
    default-hard-ttl-ms: 300000             # default hard expiry (5min)
    hard-ttl-ms: 0                          # override (0=use default)
    default-soft-ttl-ms: 30000              # default soft expiry (30s)
    soft-ttl-ms: 0                          # override (0=use default)

    # ——— Hot key TTL (takes effect after Worker marks as HOT) ———
    default-hot-hard-ttl-ms: 3600000        # default hot key hard expiry (1h)
    hot-hard-ttl-ms: 0                      # override (0=use default)
    default-hot-soft-ttl-ms: 300000         # default hot key soft expiry (5min)
    hot-soft-ttl-ms: 0                      # override (0=use default)

    # ——— Consistent hashing (default enabled; dynamic Worker routing) ———
    consistent-hashing:
      enabled: true                         # dynamic Worker routing via heartbeat
      virtual-nodes: 500                    # virtual nodes per physical Worker

    # ——— Heartbeat (App side; Worker health monitoring) ———
    heartbeat:
      exchange-name: "hotkey.heartbeat.exchange"
      timeout-ms: 3000                        # no heartbeat within window marks Worker dead
      verify-interval-ms: 1500                # suspicious Worker verification interval
      ping-timeout-ms: 2000                   # Direct reply-to PING timeout
      degrade-after-failures: 2               # degrade after N consecutive PING failures

    # ——— Reporter rate limiter (BBR + CPU fusion) ———
    reporter:
      enabled: true                           # BBR adaptive rate limiting
      cpu-threshold: 800                      # CPU threshold (0-1000, 800=80%)
      cpu-poll-interval-ms: 500               # CPU poll interval (ms)
      cpu-decay: 0.95                         # EMA smoothing decay factor
      bbr-window-ms: 10000                    # BBR sliding window (ms)
      bbr-window-buckets: 100                 # BBR sliding window buckets
      bbr-cooldown-ms: 1000                   # cool-down after drop (ms)

  # Feature toggles
  report:
    enabled: true                           # App→Worker reporting
  scheduling:
    enabled: true                           # periodic decay & expelled cleanup
  decay-period: 20                          # HeavyKeeper fading interval (s), effective when scheduling enabled

  # App side — Worker decision listener (receives HOT / COOL)
  worker-listener:
    enabled: false                          # requires Redis + RabbitMQ
    exchange-name: "hotkey.broadcast.exchange"
    queue-prefix: "hotkey.worker"
    auto-startup: true                      # start with application
    warmup-jitter-ms: 100                   # random delay before processing (anti-thundering-herd)
    concurrent-consumers: 2                 # concurrent consumers
    scheduler-pool-size: 2                  # delayed task thread pool size
    prefetch-count: 5                       # AMQP prefetch per consumer

    # SRE adaptive rate limiter (HOT decision processing path)
    sre:
      enabled: true                         # enable SRE rate limiter
      window-ms: 3000                       # sliding window (ms)
      buckets: 10                           # sliding window buckets
      min-samples: 20                       # minimum samples before rate limiting
      success-threshold: 0.6                # success rate threshold (0.0-1.0)

  # App side — cross-instance cache sync
  sync:
    enabled: false
    exchange-name: "hotkey.sync.exchange"
    queue-prefix: "hotkey.sync"
    auto-startup: true
    dedup-window-seconds: 10                # message dedup window
    dedup-max-size: 10000                   # dedup cache limit
    warmup-jitter-ms: 100                   # random delay before processing (anti-thundering-herd)
    concurrent-consumers: 3                 # concurrent consumers
    scheduler-pool-size: 4                  # delayed task thread pool size
    prefetch-count: 5                       # AMQP prefetch per consumer

  # Worker side — standalone node
  worker:
    enabled: false

    routing:
      app-name: "default"                   # must match local.app-name
      # routing managed via consistent hashing and heartbeats

    messaging:
      report-exchange: "hotkey.report.exchange"
      broadcast-exchange: "hotkey.broadcast.exchange"
      heartbeat-exchange: "hotkey.heartbeat.exchange"

    sliding-window:
      duration-ms: 1000                     # window duration (ms)
      slices: 10                            # window slices (must divide duration-ms)

    threshold:
      hot-threshold: 1000                   # absolute QPS threshold (≤0 = use ratio)
      hot-threshold-ratio: 0.01             # relative QPS ratio (1%)

    state-machine:
      confirm-duration-ms: 300              # confirmation window (ms); sustained heat beyond this = HOT
      cool-duration-ms: 15000               # cooling window (ms); sustained cool beyond this = COOL
      pre-cool-grace-ms: 5000               # pre-cool grace period (ms)
      evict-interval-ms: 30000              # expired state cleanup interval (ms), must >= coolDurationMs * 2

    global-qps-dynamic-threshold:
      qps-change-tolerance: 0.5             # QPS change tolerance multiplier
      learning-period-ms: 30000             # learning period (ms)
      hot-threshold-ratio: 0.01             # dynamic threshold ratio
      recalculate-interval-ms: 60000        # recalculation interval (ms)

    topk-validation:
      validate-interval-ms: 60000           # validation period (ms)
      pre-warm-count: 5                     # pre-warm count
      pre-warm-min-appearances: 2           # minimum appearances

    heavy-keeper:
      top-k: 100                            # hot keys to track
      width: 20000                          # Sketch width
      depth: 10                             # Sketch depth
      decay: 0.9                            # decay factor
      min-count: 10                         # minimum count

    heartbeat:
      ping-interval-ms: 1000                # heartbeat broadcast interval (ms)

    persistence:
      enabled: false                        # periodic TopK snapshot to Redis (manual enable)
      persist-interval-ms: 30000            # snapshot interval (ms)
      topk-count: 100                       # keys per snapshot
      redis-key-prefix: "hotkey:topk:worker:" # Redis key prefix
      ttl-days: 3                           # Redis data TTL (days)
```

</details>

### 3. Usage

**Read operations**

```java
@Autowired
private HotKey hotKey;

// A. peek — L1 only, no hot key tracking
Optional<String> r = hotKey.peek("user:123"); // returns Optional.empty() on L1 miss

// B. get — two-level cache (Redis or any backend)
Optional<String> r = hotKey.get("user:123", () -> redisTemplate.opsForValue().get("user:123"));

// C. getWithSoftExpire — soft expiry (stale-while-revalidate)
Optional<String> r = hotKey.getWithSoftExpire("user:123", () -> redisTemplate.opsForValue().get("user:123"));

// D. Fluent read API with fallback chain
Optional<User> user = hotKey.read("user:42")
    .withPrimary(userRepo::findById)
    .thenExecute(backupRepo::findById)
    .withHardTtl(30_000)
    .withSoftTtl(10_000)
    .allowBroadcast()
    .execute();
```

**Soft expiry** returns stale data immediately while refreshing asynchronously in the background. Redis stores plain values with no wrapping — HotKey manages expiration entirely at the L1 Caffeine layer.

| Dimension | Traditional logical expiry | HotKey soft expiry |
|---|---|---|
| Expiry storage | Embedded in Redis value (`RedisData{data, expireTime}`) | L1 Caffeine metadata (`softExpireAt`) |
| Stale return | Parse wrapper, return old data | Return L1 value directly |
| Async refresh | Redis distributed lock + custom thread pool | SingleFlight (local) + `hotKeyExecutor` + `refreshLimiter` |
| Redis format | Wrapped JSON | Plain value (no wrapper) |
| DB fallback | Manual locking logic | Native `orElseGet` / `orElseThrow` |

```java
// Custom per-call softTtl (overrides global default)
Optional<String> r2 = hotKey.getWithSoftExpire("user:456", () -> redisTemplate.opsForValue().get("user:456"), 3000);

// DB fallback (no distributed lock required):
String json = hotKey
  .getWithSoftExpire("shop:" + shopId, () -> redisTemplate.opsForValue().get("shop:" + shopId))
  .orElseGet(() -> {
    User u = userMapper.selectById(shopId);
    String s = JSONUtil.toJsonStr(u);
    if (u != null) redisTemplate.opsForValue().set("shop:" + shopId, s);
    return s;
  });
```

**Write operations**

```java
// E. putThrough — write-through with broadcast
hotKey.putThrough("user:123", newValue, () -> redisTemplate.opsForValue().set("user:123", newValue));

// F. putBeforeInvalidate — mutation then invalidate (for collection types)
hotKey.putBeforeInvalidate(key, () -> redisTemplate.opsForSet().add(key, members));

// G. putLocal — local-only write, no broadcast, no version bump
hotKey.putLocal("user:123", cachedValue);
hotKey.putLocal("user:123", cachedValue, hardTtlMs, softTtlMs); // with TTL override

// H. Fluent write API
hotKey.write("user:42").withHardTtl(30_000).putThrough(newValue, dbWriter);
hotKey.write("user:42").putBeforeInvalidate(dbMutation);
hotKey.write("user:42").invalidate();
```

**putBeforeInvalidate** is designed for Redis collection types (List, Set, ZSet). `putThrough` requires the full new value to update L1, but collection mutations (LPUSH, SADD, ZADD) only modify individual elements — the caller cannot obtain the full new value. `putBeforeInvalidate` invalidates L1 after mutation; the next `get()` falls back to Redis automatically.

```java
@Component
public class CollectionHotKeyCache {

  @Autowired private HotKey hotKey;
  @Autowired private RedisTemplate<String, Object> redisTemplate;

  public Boolean sIsMember(String key, Object member) {
    return hotKey.get(key + "::member::" + member, () -> redisTemplate.opsForSet().isMember(key, member));
  }

  public Optional<Set<Object>> sMembers(String key) {
    return hotKey.get(key, () -> redisTemplate.opsForSet().members(key));
  }

  public void sAdd(String key, Object... members) {
    hotKey.putBeforeInvalidate(key, () -> redisTemplate.opsForSet().add(key, members));
  }

  public Optional<List<Object>> lRange(String key, long start, long end) {
    return hotKey.get(key + "::range::" + start + "::" + end,
        () -> redisTemplate.opsForList().range(key, start, end));
  }

  public Optional<Double> zScore(String key, Object member) {
    return hotKey.get(key + "::score::" + member, () -> redisTemplate.opsForZSet().score(key, member));
  }
}
```

**DB fallback & cache penetration protection**

```java
Optional<String> r = hotKey.get("user:123", () -> redisTemplate.opsForValue().get("user:123"));
if (r.isEmpty()) {
    String value = userService.getById(123); // DB fallback
    redisTemplate.opsForValue().set("user:123", value);
}
```

```java
// Helper encapsulation to avoid repeated lambdas
@Component
public class RedisHotKeyHelper {
  @Autowired private HotKey hotKey;
  @Autowired private RedisTemplate<String, Object> redisTemplate;

  public <T> Optional<T> get(String key) {
    return hotKey.get(key, () -> redisTemplate.opsForValue().get(key));
  }
  public void set(String key, Object value) {
    hotKey.putThrough(key, value, () -> redisTemplate.opsForValue().set(key, value));
  }
}

// Custom L2 cache (non-Redis): MySQL, remote API, or any data source
Optional<User> r = hotKey.get("user:123", () -> userMapper.selectById(123));
User user = r.orElseGet(() -> createDefaultUser());
```

**Custom per-entry TTL**

HotKey uses **differentiated TTL**: hot keys and normal keys have independent defaults. Per-call overrides apply on top.

| Key state | Hard TTL (Caffeine eviction) | Soft TTL (stale-while-revalidate) |
|---|---|---|
| Normal | `default-hard-ttl-ms` (5min) | `default-soft-ttl-ms` (30s) |
| Hot | `default-hot-hard-ttl-ms` (1h) | `default-hot-soft-ttl-ms` (5min) |

```java
// 5min hard TTL + 30s soft TTL
Optional<String> shopJson = hotKey.get("shop:" + shopId,
    () -> redisTemplate.opsForValue().get("shop:" + shopId),
    TimeUnit.MINUTES.toMillis(5), TimeUnit.SECONDS.toMillis(30));

// 30s hard TTL, soft TTL uses default
hotKey.putThrough("weather:" + city, weatherData,
    () -> redisTemplate.opsForValue().set("weather:" + city, weatherData),
    TimeUnit.SECONDS.toMillis(30), 0);
```

> [!NOTE]
> **Cache avalanche protection:** `CacheExpireManager` applies configurable uniform random jitter via `ThreadLocalRandom` when computing each expiry timestamp. A 5-minute hard TTL with default ±10% jitter actually expires between 4.5 ~ 5.5 minutes. Control via `hotkey.local.ttl-jitter-enabled` (toggle) and `hotkey.local.ttl-jitter-ratio` (default `0.1` = ±10%).

> [!TIP]
> Per-call TTL semantics: Passing `0` means "use the configured default for this key state." For pure soft expiry (hard TTL never evicts): pass `hardTtlMs = Long.MAX_VALUE` to `getWithSoftExpire(key, reader, Long.MAX_VALUE, softTtlMs)`. The entry stays in Caffeine forever — the hard TTL will never remove it. After `softExpireAt` expires, reads immediately return the stale value and trigger async refresh. Without `Long.MAX_VALUE`, the default hard TTL may evict the entry from Caffeine first (L1 miss → higher latency). This usage is explicitly supported by Caffeine's `Expiry` Javadoc: _"To indicate no expiration an entry may be given an excessively long period, such as `Long.MAX_VALUE`."_ ([source](https://github.com/ben-manes/caffeine/blob/master/caffeine/src/main/java/com/github/benmanes/caffeine/cache/Expiry.java))

**Worker mode**

Worker mode provides cluster-level hot key detection via dedicated nodes. App instances periodically report access counts; Workers run a sliding window + state machine pipeline and broadcast HOT/COOL decisions back to all instances via RabbitMQ. State machine parameters (`confirmCount`, `coolCount`, `preCoolGraceCount`) can be adjusted at runtime via `/actuator/hotkey/worker/state`.

| Mode | `worker.enabled` | Activated beans |
|---|---|---|
| App-only | `false` (default) | `HotKeyCache`, TopK, reporter, actuator, sync |
| Worker-only | `true` | Worker only (no cache — `get()`/`putThrough()` throw `UnsupportedOperationException`) |

In **Worker-only** mode, cache operations throw `UnsupportedOperationException`.

**Worker TopK persistence (hot start):** When `hotkey.worker.persistence.enabled=true`, the Worker periodically snapshots its TopK list to Redis. On restart, `TopKPersistService` loads the last snapshot and replays it into the HeavyKeeper sketch, reducing warm-up from hours to seconds.

**Spring Cache integration**

Enable with `hotkey.spring-cache.enabled=true`. Standard `@Cacheable` / `@CachePut` / `@CacheEvict` are routed through HotKey's hot-key detection, soft-expire, and cross-instance sync.

| Annotation | Role on `@Cacheable` |
|---|---|
| `@HotKeyCacheTTL` | Override hard/soft TTL |
| `@Intercept` | Fire intercept callback on cache hit + resolve via `@Fallback` |
| `@Fallback` | Supply fallback value when interceptor blocks |
| `@NullCaching` | Opt-in to caching null return values (default `true`) |

```java
@Cacheable(cacheNames = "users", key = "#id")
@HotKeyCacheTTL(softTtlMs = 1000)
@Intercept @Fallback
public User getUser(Long id) { ... }
```

Requires `spring-boot-starter-cache` and `spring-boot-starter-aop` on the classpath.

---

## Cache Synchronization

Enable with `hotkey.sync.enabled=true`.

Each instance declares its own queue (`hotkey.sync:<instanceID>`) bound to a fanout exchange. Four message types:

- **`TYPE_REFRESH`** — Versioned refresh. The peer calls `CacheSyncListener.handleRefresh()` to reload from Redis, skipping stale updates based on `dataVersion`. A 4-case comparison (normal-normal, normal-degraded, degraded-normal, degraded-degraded) ensures normal (Redis INCR) dataVersion always wins over degraded (node-local) versions. Sent by `invalidate()` and `putThrough()`.
- **`TYPE_INVALIDATE`** — Single key invalidation (with version guard). The peer removes the L1 entry only when the incoming `dataVersion` is not stale. Sent by `putBeforeInvalidate()`.
- **`TYPE_INVALIDATE_ALL`** — Batch invalidation (no version guard). The peer immediately removes all listed keys from L1 without reloading. Sent by `invalidateAll()`.
- **`TYPE_RULES_SYNC`** — Rule set replacement. The body is a JSON-serialized `List<Rule>`; the receiver calls `RuleMatcher.syncRules()` to atomically replace the local rule list. Does not trigger a secondary broadcast.

> [!SECURITY]
> All three RabbitMQ exchanges (`hotkey.sync.exchange`, `hotkey.report.exchange`, `hotkey.broadcast.exchange`) use plain AMQP connections by default. In production, configure TLS via Spring Boot's `spring.rabbitmq.ssl.*` properties:
>
>  ```yaml
> spring:
>   rabbitmq:
>     ssl:
>       enabled: true
>       key-store: classpath:client.p12
>       key-store-password: changeit
>       trust-store: classpath:truststore.jks
>       trust-store-password: changeit
> . ```
>
> See [Spring Boot RabbitMQ SSL documentation](https://docs.spring.io/spring-boot/reference/messaging/amqp.html#page-title).

## Rule System

Enable `hotkey.sync.enabled=true` to enable cross-instance rule synchronization. The rule system provides two actions:

| Action            | Effect on matching keys                                                                  |
| ----------------- | ---------------------------------------------------------------------------------------- |
| `BLOCK`           | `get()` / `getWithSoftExpire()` throws `HotKeyBlockedException`; `putThrough()` skipped  |
| `ALLOW_NO_REPORT` | Normal processing but skip Worker reporting (reduces noise for frequently accessed keys) |

### Pattern Types

`RuleMatcher.of(pattern, action)` auto-detects the pattern type:

| Pattern             | Type       | Matches                         |
| ------------------- | ---------- | ------------------------------- |
| `"user:123"`        | `EXACT`    | Exact key                       |
| `"temp:*"`          | `PREFIX`   | Keys starting with `temp:`      |
| `"order:*-detail"`  | `WILDCARD` | Glob-style (`*` / `?`) matching |
| `"regex:user:\\d+"` | `REGEX`    | Java regex                      |

### Persistence & Broadcasting

- **With Redis:** Each `addRule()`/`removeRule()`/`clearRules()` serializes the rule list to `HotKeyConstants.REDIS_KEY_RULES` (`"hotkey:rules"`). On startup, `RuleMatcher.initRules()` loads from Redis. Changes are also broadcast via `TYPE_RULES_SYNC` — the peer atomically replaces its rules via `RuleMatcher.syncRules()`, which does not trigger a secondary broadcast (no storm).
- **Without Redis:** Same operations broadcast via `CacheSyncPublisher` fanout exchange to all peers. Each peer holds its own in-memory rule set.
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
Rule.RuleAction action = hotKey.evaluateRule("user:123"); // BLOCK / ALLOW_NO_REPORT / ALLOW
boolean blocked = hotKey.isBlacklisted("user:123");         // true if BLOCK
boolean skipReport = hotKey.isWhitelisted("health:ping");   // true if ALLOW_NO_REPORT

// Remove rules
hotKey.removeBlacklist("secret:*");
hotKey.clearAllRules();

// Cache introspection
long size = hotKey.estimatedSize();    // L1 entry count
HotKeyCacheStats s = hotKey.stats();   // hit rate, eviction, etc.

// Emergency flush (no broadcast)
hotKey.invalidateAll();
```

### Degradation

HotKey forms a three-level degradation chain via `supplier` callbacks:

Component failure behavior:

| Failed component           | Impact                                                    | Recovery                            |
| -------------------------- | --------------------------------------------------------- | ----------------------------------- |
| HotKey itself              | L1 unavailable; exception or hot degradation (if enabled) | Application restart                 |
| L2 backend (Redis/DB/API)  | Every request penetrates to caller fallback               | Auto-recovery when backend recovers |
| L1 Caffeine OOM / eviction | Single key evicted, re-fetched on next read               | Automatic (Caffeine internal)       |

> The caller must always handle `Optional.empty()` — HotKey does not hide backend failures.

Write path failure behavior:

| Write method                                        | Failure scenario                           | Behavior                                                                                       |
| --------------------------------------------------- | ------------------------------------------ | ---------------------------------------------------------------------------------------------- |
| `putLocal`                                          | Any                                                   | No-op (no DB/network dependency)                                                                   |
| `putThrough`                                        | Thread pool queue full (non-transactional)           | `RejectedExecutionException` propagated to caller                                              |
| `putThrough`                                        | `writer.run()` / Redis fails                         | Error logged, L1 version not updated, no broadcast sent                                        |
| `putBeforeInvalidate`                               | `mutation.run()` throws                              | Mutation exception caught and logged; local invalidation and broadcast skipped                 |
| `invalidate` / `putBeforeInvalidate` / `putThrough` | `nextVersion()` Redis fails                          | Falls back to node-local counter (`Long.MIN_VALUE + counter`, non-persistent, `degraded=true`) |

Worker mode failure behavior:

| Failed component          | Impact                                                                                                | Recovery                                                           |
| ------------------------- | ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| All Workers down          | Local TopK drives L1 TTL; COOL entries can upgrade to HOT; Worker decisions gracefully degrade        | Restart Worker cluster; Worker broadcast overrides local promotion |
| Partial Workers down      | Unaffected shards continue working normally                                                           | Restart failed Workers; auto-reconnect                             |
| Report channel failure    | Reports queued/buffered (RabbitMQ)                                                                    | Auto-recover when RabbitMQ recovers                                |
| Worker broadcast failure  | No cross-instance HOT/COOL sync; local TopK continues normally                                        | Restart Worker broadcaster                                         |
| Reporter BBR backpressure | BBR drops batches when concurrency exceeds budget (CPU ≥ threshold); lenient below threshold          | Auto-recover when load decreases                                   |
| Worker TopK persistence   | Redis unavailable → silently skip persistence, `error` log recorded; Worker cold start (no hot start) | Next scheduled persistence succeeds when Redis recovers            |

## Monitoring

HotKey provides two complementary monitoring mechanisms.

See [MONITOR.md](docs/MONITOR.md) for full response formats and field descriptions.

## Design Details

For domain terminology definitions, see [CONTEXT.md](CONTEXT.md).
Architectural Decision Records (ADRs) are maintained in [docs/adr/](docs/adr/0001-local-promotion-worker-fallback.md).

## License

Apache License 2.0
