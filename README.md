# HotKey

<p align="center">
  <a href="https://central.sonatype.com/artifact/io.github.hyshmily/hotkey"><img src="https://img.shields.io/maven-central/v/io.github.hyshmily/hotkey?color=blue" alt="Maven Central"></a>
  <a href="https://jitpack.io/#Hyshmily/HotKey"><img src="https://jitpack.io/v/Hyshmily/HotKey.svg" alt="JitPack"></a>
  <a href="https://coveralls.io/github/Hyshmily/hotkey?branch=master"><img src="https://coveralls.io/repos/github/Hyshmily/hotkey/badge.svg?branch=master" alt="Coveralls"></a>
  <a href="https://github.com/Hyshmily/hotkey/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/Hyshmily/hotkey/ci.yml?branch=master&label=CI&logo=github" alt="CI"></a>
  <a href="https://www.apache.org/licenses/LICENSE-2.0"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
  <a href="https://openjdk.java.net/"><img src="https://img.shields.io/badge/Java-17-orange" alt="Java"></a>
  <a href="https://spring.io/projects/spring-boot"><img src="https://img.shields.io/badge/Spring%20Boot-3.5.3-brightgreen" alt="Spring Boot"></a>
</p>

[**中文**](README.zh.md)

HotKey is a highly configurable, high-performance, low-cost, lightweight distributed caching and cache-warming framework.

It solves cluster-wide **distributed consistent caching** for unforeseen hotspot data at minimal cost:

- **Unpredictable hot keys** — A sudden traffic burst causes certain keys to spike unexpectedly; `HeavyKeeper` algorithm detects them in milliseconds
- **Hot-key synchronization** — RabbitMQ-delivered Worker decisions (HOT/COOL) automatically propagate to every instance's L1 cache; manual broadcast for cross-instance invalidation is also supported
- **Malicious request amplification** — Unknown users flooding the same endpoint; AOP-based interception lets you decide the response (block, circuit-break, return a default value, etc.)

Additionally, HotKey provides:

### 1. Simple Deployment

- **Multiple deployment modes** — Maven / Docker / JAR
- **Spring Boot Starter** — Just add the `hotkey` dependency; zero extra configuration

### 2. Practical Features

- **Spring Cache integration** — Standard `@Cacheable` / `@CachePut` / `@CacheEvict` annotations working with HotKey's hot-key detection, soft expiry, and cross-instance sync. Companion annotations:
  - `@HotKeyCacheTTL`: TTL control
  - `@HotKeyPreload`: Pre-warm keys, skip cold-start wait
  - `@Intercept`: Custom handling for hot-key requests
  - `@Fallback`: SpEL expression or naming-convention fallback
  - `@NullCaching`: Null-value caching to prevent cache penetration
  - `@Broadcast`: Suppress broadcast — local write only
- **Multi-level cache** — Fluent API for N-level fallback: `.withPrimary(reader)` runs the full detection pipeline (counts + reports), `.thenExecute(fallback)` degrades step by step without reporting, results written to L1 with optional broadcast
- **Soft expiry (logical expiration)** — Differentiated softTTL/hardTTL, fully replacing traditional Redis-side logical expiration. Redis stores raw values; HotKey manages expiration at the L1 Caffeine layer. Stale values are returned immediately while the cache refreshes async in the background — lowering P99 latency
- **Blacklist / whitelist filtering** — Blacklisted keys are automatically blocked; whitelisted keys skip reporting and Worker decisions entirely
- **Transaction support** — `TransactionSupport` defers cache writes until `@Transactional` commits, ensuring write-path cache-data consistency
- **Distributed locking** — Redis-based `tryLock` / `tryLockAndRun` with Lua-script safe release
- **Zero-side-effect read (`peek()`)** — Pure L1 lookup, no detection, no reporting, no refresh
- **Actuator endpoints** — `/actuator/hotkey` (hot key list with `?limit=N`), `/actuator/hotkeyring` (consistent hash ring), `/actuator/hotkey/worker/state` (state machine inspection + tuning)
- **Micrometer metrics** — Caffeine hit rate, TopK size, Reporter flush latency, CPU EMA gauge, BBR in-flight count

### 3. Cluster Protection via Multiple Rate Limiters

- **Circuit breaker** — Sliding-window breaker in `SingleFlight.load()`; returns stale L1 value on open, preventing data-source cascading failures
- **BBR adaptive rate limiting** — BBR congestion control fused with CPU EMA monitoring for self-protection; applies backpressure on the Reporter flush path to prevent RabbitMQ/Worker overload
- **SRE adaptive rate limiting** — Google SRE formula backpressure on the WorkerListener HOT-path, independent of BBR, protects the HOT decision consumption path
- **CPU monitoring with EMA smoothing** — Dedicated daemon thread polling process CPU load every 500ms; configurable EMA decay factor for stable overload detection

### 4. Multi-Level Fault Tolerance & Degradation

- **Graceful degradation** — When all Workers are unreachable, Reporter silently drops reports, local TopK assumes authority for HOT promotion; Worker recovery reasserts authority via `decisionVersion >=` — zero manual intervention
- **Write-path degradation** — `VersionController.nextVersion()` falls back to local negative counter when Redis INCR fails; writes never block
- **Reporter backpressure** — BBR + CPU EMA; drops batches when RabbitMQ is congested, never blocks business threads
- **Staleness Guard** — Reporter dispatcher discards batches waiting >5s; Worker-side ReportConsumer also drops messages older than 5s
- **Poison message protection** — ReportConsumer outer catch-all discards exceptions without requeue, preventing infinite retry loops
- **State machine rollback** — Worker auto-rolls back to the pre-evaluation snapshot on broadcast failure, preventing inconsistent "semi-hot" states
- **Transaction consistency** — `TransactionSupport` defers cache writes until `@Transactional` commits
- **TopK persistence** — Worker restores HeavyKeeper snapshots in seconds, avoiding cold-start misclassification
- **TTL jitter (±10%)** — Prevents simultaneous expiration of large key groups, avoiding cache avalanches
- **Consistent hash self-healing** — 32-bit Murmur3 + 500 virtual nodes + heartbeat-driven dynamic routing; Worker scaling requires no static configuration
- **Decision version control** — Per-Worker monotonic `decisionVersion` + degradation-tolerant `dataVersion` — ensures correct cross-Worker decision ordering

> [!TIP]
>
> HotKey supports extensive configuration options; see [CONFIG.md](docs/CONFIG.md) for the full reference.
>
> With default settings, the full-chain end-to-end latency (cache miss on A → report → Worker decision → state machine confirm → RabbitMQ broadcast → cache hit on B) is: **300ms (P99)**.

HotKey is inspired by JD.com's [hotkey](https://gitee.com/jd-platform-opensource/hotkey) project; algorithm support from [Aegis](https://github.com/go-kratos/aegis).

## Use Cases & Limitations

HotKey is a **read-hotspot governance** framework, not a general-purpose distributed cache or distributed lock.

### Suitable Scenarios

| Scenario                                 | Why it fits                                                                                                   |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| **Product details / pricing pages**      | Sudden traffic spikes on a few SKUs; HeavyKeeper auto-detects → extends local TTL → reduces Redis penetration |
| **Viral articles / video metadata**      | Trending content auto-identified; cross-instance broadcast pre-warms every pod's L1                           |
| **Config / rules / dictionary cache**    | Very frequent reads, rare writes; cross-instance sync via AMQP broadcast                                      |
| **User sessions / permission snapshots** | Hot users (VIP operations) automatically get longer L1 TTL                                                    |
| **Preloaded read-only cache (sidecar)**  | Coupon amounts, product images, descriptions — paired with a separate atomic deduction system                 |

## Quick Start

### 1. Add Dependency

#### Local Mode (App-side) — Maven Dependency

Just add the starter to your `pom.xml`; Caffeine L1 + local HeavyKeeper + Reporter activate automatically with zero additional config.

**Maven Central** (no extra repository needed):

```xml
<dependency>
  <groupId>io.github.hyshmily</groupId>
  <artifactId>hotkey</artifactId>
  <version>1.1.53</version>
</dependency>
```

**JitPack** (latest snapshot):

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
    <version>1.1.53</version>
</dependency>
```

**GitHub Packages** (requires GitHub Token):

```xml
<repositories>
  <repository>
    <id>github</id>
    <name>GitHub Packages</name>
    <url>https://maven.pkg.github.com/hyshmily/hotkey</url>
  </repository>
</repositories>

<dependency>
  <groupId>io.github.hyshmily</groupId>
  <artifactId>hotkey</artifactId>
  <version>1.1.53</version>
</dependency>
```

`~/.m2/settings.xml` needs auth:

```xml
<server>
  <id>github</id>
  <username>hyshmily</username>
  <password>${env.GITHUB_TOKEN}</password>
</server>
```

#### Worker Node (standalone deployment) — JAR / Docker

Worker is a standalone Spring Boot app (not published to Maven Central). Pre-built images are hosted on GHCR.

**Prerequisites:** Log in with a GitHub PAT that has `read:packages` scope:

```bash
echo $PAT | docker login ghcr.io -u hyshmily --password-stdin
```

**Full stack via docker compose** (includes Redis + RabbitMQ):

```bash
docker compose -f worker/docker-compose.yml up -d
```

**Scale multiple Worker instances:**

```bash
docker compose -f worker/docker-compose.yml up -d --scale worker=3
```

**Run standalone** (external Redis + RabbitMQ):

```bash
docker run -d --name hotkey-worker -p 8080:8080 \
  -e SPRING_RABBITMQ_HOST=rabbitmq \
  -e SPRING_DATA_REDIS_HOST=redis \
  -e HOTKEY_WORKER_ENABLED=true \
  ghcr.io/hyshmily/hotkey-worker:1.1.53
```

**Run JAR directly** (no Docker):

```bash
mvn clean package -pl worker
java -jar worker/target/hotkey-worker-1.1.53.jar
```

> Worker is built from the `worker/` module. Requires RabbitMQ + Redis.
> Set `HOTKEY_WORKER_ENABLED=true` to activate Worker mode (cache methods throw `HotKeyModeException`).

### 2. Configuration

Default local config (suitable for most scenarios):

> [!IMPORTANT]
> In distributed deployments, RabbitMQ and Redis are required.

**Optional feature toggles:**

| Feature                  | How to Enable                                  | Description                                                            |
| ------------------------ | ---------------------------------------------- | ---------------------------------------------------------------------- |
| Redis L2 cache           | Add `RedisTemplate` bean                       | Two-level cache with L2 fallback                                       |
| Cross-instance sync      | `hotkey.sync.enabled=true`                     | RabbitMQ-based cache invalidation                                      |
| Worker Listener          | `hotkey.worker-listener.enabled=true`          | Receive Worker HOT/COOL decisions                                      |
| Worker mode              | `hotkey.worker.enabled=true`                   | Run dedicated Worker node                                              |
| Worker TopK persistence  | `hotkey.worker.persistence.enabled=true`       | Hot-start from Redis after restart                                     |
| Access reporting         | `hotkey.report.enabled=true` (default)         | Report access counts to Worker                                         |
| Reporter self-protection | `hotkey.local.reporter.enabled=true` (default) | BBR backpressure on Reporter flush                                     |
| Spring Cache integration | `hotkey.spring-cache.enabled=true`             | `@Cacheable` / `@CachePut` / `@CacheEvict` fused with HotKey detection |

All options in [Configuration](#configuration), full property reference in [CONFIG.md](docs/CONFIG.md).

<details>
<summary><b>Quick deployment YAML templates</b></summary>

**Local (App-side)** — Works with just the `hotkey` dependency; uncomment as needed:

```yaml
hotkey:
  # local params all use defaults, no explicit config needed

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
  #   enabled: true     # worker-listener depends on hotKeyRedisLoader Bean

  # Consistent hashing enabled by default (dynamic Worker routing via heartbeats)
  # local:
  #   consistent-hashing:
  #     enabled: true     # (enabled by default)
```

**Single Worker (standalone node)** — Requires `spring-boot-starter-amqp`:

```yaml
hotkey:
  worker:
    enabled: true
    routing:
      app-name: myapp # 【MUST】match hotkey.local.app-name on App side
      # Consistent hashing enabled by default; Workers auto-register via heartbeats

# Multi-Worker example: 3 machines, same app-name
# Consistent hashing automatically routes keys to the correct Worker via heartbeats
# No static shard config needed — just add more machines. Deploy Apps first, then Workers.
```

> [!IMPORTANT]
> **Cluster health threshold** — When `expected-worker-count: 0` (dynamic mode, default), `min-alive-workers: 0` resolves to 1 — **any alive Worker = healthy**. When `expected-worker-count: N` (fixed mode), the majority formula `N/2 + 1` applies. Set `min-alive-workers` explicitly (e.g. `2`) to override either mode. See `docs/CONFIG.md` for details.

**Full parameters (override defaults):**

```yaml
hotkey:
  local:
    # ——— Must match across nodes (App-side and Worker-side) ———
    app-name: "default" # must match worker.routing.app-name

    # ——— Instance identity ———
    instance-id: "" # explicit instance ID (empty=auto-generated)

    # ——— Report publishing ———
    report-exchange: "hotkey.report.exchange" # must match worker.messaging.report-exchange
    report-interval-ms: 50 # batch send interval (ms)
    queue-capacity: 10000 # report queue capacity
    queue-offer-timeout-ms: 100 # queue write timeout (ms)
    consumer-count: 0 # consumer threads (0=auto)

    # ——— HeavyKeeper algorithm ———
    topK: 100 # hot keys to retain
    width: 50000 # Count-Min Sketch width
    depth: 5 # Count-Min Sketch depth
    decay: 0.92 # decay factor (multiplied each fading cycle)
    minCount: 10 # minimum access count for hot key
    expelled-queue-capacity: 50000 # expelled hot key staging queue

    # ——— L1 Caffeine cache ———
    local-cache-max-size: 1000 # max entries
    local-cache-ttl-minutes: 5 # cache TTL (minutes)

    # ——— SingleFlight dedup ———
    inflight-max-size: 50000 # dedup record max keys
    inflight-ttl-seconds: 5 # dedup record TTL (must > L2 response time)
    inflight-timeout-seconds: 3 # async result wait timeout (must < inflight-ttl-seconds)

    # ——— Async executor ———
    executor-core-pool-size: 8 # core threads
    executor-max-pool-size: 32 # max threads
    executor-queue-capacity: 2000 # work queue capacity
    scheduler-pool-size: 8 # scheduler thread pool size

    # ——— TTL jitter (cache avalanche protection) ———
    ttl-jitter-enabled: true # enable TTL random jitter
    ttl-jitter-ratio: 0.1 # jitter ratio (0.0~1.0), 0.1 = ±10%

    # ——— Refresh & version control ———
    refresh-max-pools: 100 # refresh thread pool limit
    version-key-ttl-minutes: 60 # Redis version key TTL

    # ——— Null value TTL ———
    null-value-ttl-seconds: 10 # TTL for null/cache-miss entries (short, avoids cached negative results)

    # ——— Normal key TTL ———
    default-hard-ttl-ms: 300000 # default hard expiry (5min)
    hard-ttl-ms: 0 # override (0=use default)
    default-soft-ttl-ms: 30000 # default soft expiry (30s)
    soft-ttl-ms: 0 # override (0=use default)

    # ——— Hot key TTL (active after Worker marks HOT) ———
    default-hot-hard-ttl-ms: 3600000 # default hot key hard expiry (1h)
    hot-hard-ttl-ms: 0 # override (0=use default)
    default-hot-soft-ttl-ms: 300000 # default hot key soft expiry (5min)
    hot-soft-ttl-ms: 0 # override (0=use default)

    # ——— Consistent hashing (default enabled; dynamic Worker routing) ———
    consistent-hashing:
      enabled: true # dynamic Worker routing via heartbeats
      virtual-nodes: 500 # virtual nodes per physical Worker

    # ——— Heartbeat (App-side; Worker health monitoring) ———
    heartbeat:
      exchange-name: "hotkey.heartbeat.exchange" # must match worker.messaging.heartbeat-exchange
      timeout-ms: 30000 # Worker considered dead if no heartbeat within this window
      verify-interval-ms: 5000 # suspicious Worker verification interval
      ping-timeout-ms: 3000 # Direct reply-to PING timeout
      degrade-after-failures: 3 # degrade after N consecutive PING failures (with exponential backoff)
      verify-max-backoff-ms: 600000 # max exponential backoff between verification probes (10min)
      min-alive-workers: 0 # 0=dynamic (1 alive = healthy); set >0 to require N alive Workers

    # ——— Circuit breaker (optional, disabled by default) ———
    circuit-breaker:
      enabled: false
      window-time-ms: 10000
      window-buckets: 10
      fail-threshold: 0.5
      request-volume-threshold: 20
      single-test-interval-ms: 5000
      log-enabled: true

    # ——— Reporter rate limiter (BBR + CPU fusion) ———
    reporter:
      enabled: true # BBR adaptive rate limiting
      cpu-threshold: 800 # CPU threshold (0-1000, 800=80%)
      cpu-poll-interval-ms: 500 # CPU poll interval (ms)
      cpu-decay: 0.95 # EMA smoothing decay factor
      bbr-window-ms: 10000 # BBR sliding window (ms)
      bbr-window-buckets: 100 # BBR sliding window buckets
      bbr-cooldown-ms: 1000 # cooldown after drop (ms)

  # Feature toggles
  report:
    enabled: true # App→Worker report publishing
  scheduling:
    enabled: true # periodic decay & expel drain
  decay-period: 20 # HeavyKeeper fading interval (seconds), requires scheduling enabled

  # App-side — Worker decision listener (receives HOT / COOL)
  worker-listener:
    enabled: false # requires Redis + RabbitMQ
    exchange-name: "hotkey.broadcast.exchange" # must match worker.messaging.broadcast-exchange
    queue-prefix: "hotkey.worker"
    auto-startup: true
    warmup-jitter-ms: 50 # random delay before processing (thundering-herd protection)
    concurrent-consumers: 2
    scheduler-pool-size: 2
    prefetch-count: 5

    # SRE adaptive rate limiter (HOT decision processing path)
    sre:
      enabled: true
      window-ms: 3000
      buckets: 10
      min-samples: 20
      success-threshold: 0.6

  # App-side — Cross-instance cache sync
  sync:
    enabled: false
    exchange-name: "hotkey.sync.exchange"
    queue-prefix: "hotkey.sync"
    auto-startup: true
    dedup-window-seconds: 10
    dedup-max-size: 10000
    warmup-jitter-ms: 100
    concurrent-consumers: 3
    scheduler-pool-size: 4
    prefetch-count: 5

  # Worker-side — Standalone deployment node
  worker:
    enabled: false

    routing:
      app-name: "default" # must match local.app-name

    messaging:
      report-exchange: "hotkey.report.exchange" # must match local.report-exchange
      broadcast-exchange: "hotkey.broadcast.exchange" # must match worker-listener.exchange-name
      heartbeat-exchange: "hotkey.heartbeat.exchange" # must match local.heartbeat.exchange-name

    sliding-window:
      duration-ms: 1000 # window duration (ms), each slice = 1000/10 = 100ms
      slices: 10 # window slices

    threshold:
      hot-threshold: 1000 # absolute QPS threshold (≤0 = use ratio)
      hot-threshold-ratio: 0.01 # relative QPS ratio (1%)

    state-machine:
      sm-duration-ms: 500 # state-machine slice window (ms), independent of sliding-window
      sm-slices: 10 # slices within state-machine window, each = 500/10 = 50ms
      confirm-duration-ms: 100 # confirm total duration, confirmCount = ceil(100/50) = 2
      cool-duration-ms: 15000 # cool window (ms), sustained cool past this = COOL
      pre-cool-grace-ms: 5000 # pre-cool grace period (ms)
      evict-interval-ms: 30000 # stale state eviction interval (ms), must >= coolDurationMs * 2

    global-qps-dynamic-threshold:
      qps-change-tolerance: 0.5
      learning-period-ms: 30000
      hot-threshold-ratio: 0.01
      recalculate-interval-ms: 60000

    topk-validation:
      validate-interval-ms: 60000
      pre-warm-count: 5
      pre-warm-min-appearances: 2

    heavy-keeper:
      top-k: 100
      width: 20000
      depth: 10
      decay: 0.9
      min-count: 10

    heartbeat:
      ping-interval-ms: 1000

    persistence:
      enabled: false
      persist-interval-ms: 30000
      topk-count: 100
      redis-key-prefix: "hotkey:topk:worker:"
      ttl-days: 3
```

</details>

### 3. Usage

**Read Operations**

```java
@Autowired
private HotKey hotKey;

// A. peek — L1 only, no hot-key tracking
Optional<String> r = hotKey.peek("user:123"); // returns Optional.empty() on L1 miss

// A1. peekAll — batch L1 lookup
Map<String, Object> batch = hotKey.peekAll(List.of("user:1", "user:2", "user:3"));

// B. computeIfAbsent — simplified get (no Optional wrapper)
String val = hotKey.computeIfAbsent("user:123", () -> redisTemplate.opsForValue().get("user:123"));

// B1. computeIfAbsent — batch reload
Map<String, String> users = hotKey.computeIfAbsent(keys, (k) -> loadUser(k));

// C. get — two-level cache (Redis or any backend)
Optional<String> r = hotKey.get("user:123", () -> redisTemplate.opsForValue().get("user:123"));

// D. getWithSoftExpire — stale-while-revalidate
Optional<String> r = hotKey.getWithSoftExpire("user:123", () -> redisTemplate.opsForValue().get("user:123"));

// E. Fluent read API + fallback chain
Optional<User> user = hotKey
  .read("user:42")
  .withPrimary(userRepo::findById)
  .thenExecute(backupRepo::findById)
  .withHardTtl(30_000)
  .withSoftTtl(10_000)
  .allowBroadcast()
  .execute();
```

**Soft expiry** returns stale data immediately while refreshing asynchronously. Redis stores raw values with no wrapper — HotKey manages expiration entirely at the L1 Caffeine layer.

| Dimension         | Traditional logical expiry                              | HotKey soft expiry                                         |
| ----------------- | ------------------------------------------------------- | ---------------------------------------------------------- |
| Expiry storage    | Embedded in Redis value (`RedisData{data, expireTime}`) | L1 Caffeine metadata (`softExpireAt`)                      |
| Return stale data | Parse wrapper, return old data                          | Directly return L1 stale value                             |
| Async rebuild     | Redis distributed lock + custom thread pool             | Singleflight (local) + `hotKeyExecutor` + `refreshLimiter` |
| Redis format      | Wrapped JSON                                            | Raw value (no wrapper)                                     |
| DB fallback       | Manual locking logic                                    | Native `orElseGet` / `orElseThrow`                         |

```java
// Custom per-call softTtl (overrides global default)
Optional<String> r2 = hotKey.getWithSoftExpire("user:456", () -> redisTemplate.opsForValue().get("user:456"), 3000);

// DB fallback (no distributed lock needed):
String json = hotKey
  .getWithSoftExpire("shop:" + shopId, () -> redisTemplate.opsForValue().get("shop:" + shopId))
  .orElseGet(() -> {
    User u = userMapper.selectById(shopId);
    String s = JSONUtil.toJsonStr(u);
    if (u != null) redisTemplate.opsForValue().set("shop:" + shopId, s);
    return s;
  });
```

**Write Operations**

```java
// F. putThrough — write-through + broadcast
hotKey.putThrough("user:123", newValue, () -> redisTemplate.opsForValue().set("user:123", newValue));

// G. putBeforeInvalidate — mutate then invalidate (collection types)
hotKey.putBeforeInvalidate(key, () -> redisTemplate.opsForSet().add(key, members));

// H. putLocal — local write only, no broadcast, no version bump
hotKey.putLocal("user:123", cachedValue);
hotKey.putLocal("user:123", cachedValue, hardTtlMs, softTtlMs);

// H1. putLocal — batch local write
hotKey.putLocal(Map.of("user:1", v1, "user:2", v2));

// I. evictLocal — local eviction only, no broadcast, no version bump
hotKey.evictLocal("user:123");
hotKey.evictLocal(List.of("user:1", "user:2", "user:3"));

// J. refresh — evict locally then load and cache
hotKey.refresh("user:123", () -> loadUser(123));
hotKey.refresh("user:123", () -> loadUser(123), hardTtlMs, softTtlMs);
hotKey.refreshAll(Map.of("user:1", () -> loadUser(1), "user:2", () -> loadUser(2)));

// K. Fluent write API
hotKey.write("user:42").withHardTtl(30_000).putThrough(newValue, dbWriter);
hotKey.write("user:42").putBeforeInvalidate(dbMutation);
hotKey.write("user:42").invalidate();
```

`putBeforeInvalidate` is designed for Redis collection types (List, Set, ZSet). `putThrough` needs the full new value for L1 update, but LPUSH/SADD/ZADD only modify individual elements — the caller cannot know the full new value. After mutation, the key is invalidated from L1; the next `get()` automatically falls back to Redis.

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

  public Optional<Set<Object>> sMembers(String key) {
    return hotKey.get(key, () -> redisTemplate.opsForSet().members(key));
  }

  public void sAdd(String key, Object... members) {
    hotKey.putBeforeInvalidate(key, () -> redisTemplate.opsForSet().add(key, members));
  }

  public Optional<List<Object>> lRange(String key, long start, long end) {
    return hotKey.get(key + "::range::" + start + "::" + end, () -> redisTemplate.opsForList().range(key, start, end));
  }

  public Optional<Double> zScore(String key, Object member) {
    return hotKey.get(key + "::score::" + member, () -> redisTemplate.opsForZSet().score(key, member));
  }
}
```

**Database Fallback & Cache Penetration Protection**

```java
Optional<String> r = hotKey.get("user:123", () -> redisTemplate.opsForValue().get("user:123"));
if (r.isEmpty()) {
    String value = userService.getById(123);
    redisTemplate.opsForValue().set("user:123", value);
}
```

```java
// Helper wrapper to avoid repeated lambdas
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

> [!NOTE]
> **Serialization:** HotKey uses `StringRedisTemplate` internally; value serialization is entirely up to the caller. Recommend **Jackson** (Spring Boot default, JSON) or **Kryo** (binary, max throughput). JDK native serialization is not recommended.

// Custom L2 (non-Redis): MySQL, remote API, or any data source
Optional<User> r = hotKey.get("user:123", () -> userMapper.selectById(123));

User user = r.orElseGet(() -> createDefaultUser());
```

**Distributed Locking**

Distributed locks protect external resources (DB writes, RPC calls) — cache operations are performed as side effects after lock release.

```java
// Lock protects external write, then refreshes cache (outside lock)
boolean ok = hotKey.tryLockAndRun("order:42", 5, TimeUnit.SECONDS, () -> {
    orderService.deductStock(orderId, quantity); // DB write, needs mutual exclusion
    hotKey.invalidate("order:" + orderId);        // post-invalidation, not in lock scope
});

// Custom retry count (negative = fall back to config default)
try (AutoReleaseLock lock = hotKey.tryLock("order:42", 5, TimeUnit.SECONDS, 3, 1, 3)) {
    if (lock != null) { /* critical section */ }
}
```

**Custom Per-Entry TTL**

HotKey uses **differentiated TTLs**: hot keys and normal keys have independent defaults. Per-call overrides apply on top of these.

| Key State | Hard TTL (Caffeine eviction)   | Soft TTL (stale-while-revalidate) |
| --------- | ------------------------------ | --------------------------------- |
| Normal    | `default-hard-ttl-ms` (5min)   | `default-soft-ttl-ms` (30s)       |
| Hot       | `default-hot-hard-ttl-ms` (1h) | `default-hot-soft-ttl-ms` (5min)  |

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
> **Cache avalanche protection:** `CacheExpireManager` applies configurable uniform random jitter (default ±10%) to every expiry timestamp via `ThreadLocalRandom`. A 5min hard TTL under default jitter expires between 4.5 ~ 5.5 minutes. Controlled by `hotkey.local.ttl-jitter-enabled` (switch) and `hotkey.local.ttl-jitter-ratio` (ratio, default `0.1` = ±10%).

> [!TIP]
> Per-call TTL semantics: passing `0` means "use the default for this key state". Purely logical expiry (soft-only, hard TTL never evicts): pass `hardTtlMs = Long.MAX_VALUE` to `getWithSoftExpire(key, reader, Long.MAX_VALUE, softTtlMs)`; the entry stays in Caffeine indefinitely. This usage is explicitly supported by Caffeine's `Expiry` JavaDoc: _"To indicate no expiration an entry may be given an excessively long period, such as `Long.MAX_VALUE`."_ ([source](https://github.com/ben-manes/caffeine/blob/master/caffeine/src/main/java/com/github/benmanes/caffeine/cache/Expiry.java))

**Worker Mode**

Worker mode provides cluster-wide hot-key detection via dedicated nodes. App instances periodically report access counts; Workers run a sliding window + state machine pipeline and broadcast HOT/COOL decisions back to all instances. State machine parameters (`confirmCount`, `coolCount`, `preCoolGraceCount`) are tunable at runtime via `/actuator/hotkey/worker/state`.

| Mode        | `worker.enabled`  | Active Beans                                                                |
| ----------- | ----------------- | --------------------------------------------------------------------------- |
| App-only    | `false` (default) | `HotKeyCache`, TopK, reporter, actuator, sync                               |
| Worker-only | `true`            | Worker only (no cache — `get()`/`putThrough()` throw `HotKeyModeException`) |

**Worker-only** mode cache operations throw `HotKeyModeException`.

**Cross-Worker decisionVersion partitioning:** Each Worker has a unique `nodeId` and `epoch` counter. When broadcasting HOT/COOL decisions, both are attached as AMQP headers. The App-side `WorkerListener` propagates `nodeId`/`epoch` into `CacheEntry.decisionNodeId`/`decisionEpoch` via double-checked locking. `VersionGuard` compares decisions per-Worker partition: only decisions with `epoch` >= cached `decisionEpoch` win, using `nodeId` to distinguish version spaces. This prevents stale decisions from a restarted Worker from overriding newer decisions — each restart increments the `epoch` counter, resetting authority.

**Worker Cluster Health:** Set `hotkey.local.expected-worker-count` to the expected number of Workers in production. When >0, `ClusterHealthView` uses majority quorum (`> expectedWorkerCount / 2`) as the health threshold; when 0 (default), the cluster is always considered unhealthy until at least one heartbeat is received. This enables precise detection of partial Worker failures and graceful degradation decisions.

**Worker TopK Persistence (warm start):** When `hotkey.worker.persistence.enabled=true`, the Worker periodically snapshots its TopK list to Redis. On restart, `TopKPersistService` loads the last snapshot and replays it into the HeavyKeeper sketch, shrinking warm-up from hours to seconds.

**Spring Cache Integration**

Enable with `hotkey.spring-cache.enabled=true`. Standard `@Cacheable` / `@CachePut` / `@CacheEvict` automatically route through HotKey's hot-key detection, soft expiry, and cross-instance sync.

| Annotation        | Effect on `@Cacheable`                                                                                                               |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `@HotKeyCacheTTL` | Override hard/soft TTL                                                                                                               |
| `@HotKeyPreload`  | Inflate HeavyKeeper count, making known hot keys immediately effective                                                               |
| `@Intercept`      | Skip method body via trigger mode (`IS_LOCAL_HOT`/`FORCE`/`QPS`); fallback priority `@Intercept.fallback()` → `@Fallback` → `peek()` |
| `@Fallback`       | Provide fallback value when blocked by blacklist, intercepted, or on exception                                                       |
| `@NullCaching`    | Opt-in to caching null return values (default `true`)                                                                                |
| `@Broadcast`      | Suppress cross-instance sync messages                                                                                                |

```java
@Cacheable(cacheNames = "users", key = "#id")
@HotKeyCacheTTL(softTtlMs = 1000)
@Intercept @Fallback
public User getUser(Long id) { ... }

// QPS-limiting interception
@Cacheable(cacheNames = "products", key = "#id")
@Intercept(trigger = InterceptTrigger.QPS, QPS = 500, fallback = "'throttled'")
@Fallback
public Product getProduct(String id) { ... }

// Flash-sale hot-key preload
@Cacheable(cacheNames = "flash", key = "#id")
@HotKeyPreload(keys = {"item-001", "item-002"})
@Intercept
public String getFlashItem(String id) { ... }
```

Requires `spring-boot-starter-cache` and `spring-boot-starter-aop` on the classpath.

---

## Cache Synchronization

Enable with `hotkey.sync.enabled=true`.

Each instance declares an exclusive queue (`hotkey.sync:<instanceID>`) bound to a fanout exchange. Four message types:

- **`TYPE_REFRESH`** — Versioned refresh. The peer reloads from Redis via `CacheSyncListener.handleRefresh()`, skipping stale updates based on `dataVersion`. The 4-case comparison (normal-normal, normal-degraded, degraded-normal, degraded-degraded) ensures normal (Redis INCR) `dataVersion` always prevails over degraded (node-local) versions. Sent by `invalidate()` and `putThrough()`.
- **`TYPE_INVALIDATE`** — Single-key invalidation (version-guarded). The peer removes the L1 entry only if the incoming `dataVersion` is not stale. Sent by `putBeforeInvalidate()`.
- **`TYPE_INVALIDATE_ALL`** — Bulk invalidation (no version guard). The peer immediately removes all listed keys from L1 without reloading. Sent by `invalidateAllLocal()`.
- **`TYPE_RULES_SYNC`** — Rule-set replacement. Body is a JSON-serialized `List<Rule>`; the receiver calls `RuleMatcher.syncRules()` to atomically replace the local rule list. No secondary broadcast.

> [!SECURITY]
> All three RabbitMQ exchanges (`hotkey.sync.exchange`, `hotkey.report.exchange`, `hotkey.broadcast.exchange`) default to plain AMQP connections. In production, configure TLS via Spring Boot's `spring.rabbitmq.ssl.*` properties:
>
> ````yaml
> spring:
>  rabbitmq:
>    ssl:
>      enabled: true
>      key-store: classpath:client.p12
>      key-store-password: changeit
>      trust-store: classpath:truststore.jks
>      trust-store-password: changeit
> . ```
>
> See [Spring Boot RabbitMQ SSL docs](https://docs.spring.io/spring-boot/reference/messaging/amqp.html#page-title).
> ````

## Rule System

Enable `hotkey.sync.enabled=true` to enable cross-instance rule sync. The rule system provides two actions:

| Action            | Effect on matching keys                                                                  |
| ----------------- | ---------------------------------------------------------------------------------------- |
| `BLOCK`           | `get()` / `getWithSoftExpire()` throw `HotKeyBlockedException`; `putThrough()` skipped   |
| `ALLOW_NO_REPORT` | Normal processing but skip Worker reporting (reduce noise from frequently accessed keys) |

### Pattern Types

`RuleMatcher.of(pattern, action)` auto-detects the pattern:

| Pattern             | Type       | Match                           |
| ------------------- | ---------- | ------------------------------- |
| `"user:123"`        | `EXACT`    | Exact key match                 |
| `"temp:*"`          | `PREFIX`   | Keys starting with `temp:`      |
| `"order:*-detail"`  | `WILDCARD` | Glob-style (`*` / `?`) matching |
| `"regex:user:\\d+"` | `REGEX`    | Java regex                      |

### Persistence & Broadcast

- **With Redis:** Every `addRule()`/`removeRule()`/`clearRules()` serializes the rule list to `HotKeyConstants.REDIS_KEY_RULES` (`"hotkey:rules"`). On startup, `RuleMatcher.initRules()` loads from Redis. Changes are also broadcast via `TYPE_RULES_SYNC` — peers call `RuleMatcher.syncRules()` for atomic replacement (no secondary broadcast to avoid storms).
- **Without Redis:** Same operations broadcast to all peers via the `CacheSyncPublisher` fanout exchange. Each peer holds the full rule set in memory.
- **Manual broadcast:** `hotKey.broadcastAllLocalRulesManually()` loads from Redis (if available) and re-broadcasts the current rule set to all peers.

### Programmatic Use

```java
// Block access to sensitive keys
hotKey.addBlacklist("secret:*");
hotKey.addBlacklist("regex:token-\\d+");

// Whitelist keys to skip Worker reporting
hotKey.addWhitelist("health:*");
hotKey.addWhitelist("metrics:*");

// Check rules
List<Rule> rules = hotKey.getAllRules();
RuleAction action = hotKey.evaluateRule("user:123"); // BLOCK / ALLOW_NO_REPORT / ALLOW
boolean blocked = hotKey.isBlacklisted("user:123");
boolean skipReport = hotKey.isWhitelisted("health:ping");

// Remove rules
hotKey.removeBlacklist("secret:*");
hotKey.clearAllRules();

// Cache monitoring
long size = hotKey.estimatedSize();
HotKeyCacheStats s = hotKey.stats();

// Emergency clear (no broadcast)
hotKey.invalidateAllLocal();

// Batch rule evaluation
Map<String, Rule.RuleAction> actions = hotKey.evaluateRules(List.of("user:1", "user:2"));
Map<String, Boolean> blocked = hotKey.isBlacklisted(List.of("user:1", "user:2"));
Map<String, Boolean> skipReport = hotKey.isWhitelisted(List.of("health:ping", "metrics:qps"));

// Batch hot-key check
Map<String, Boolean> localHots = hotKey.areLocalHotKeys(List.of("user:1", "user:2"));
Map<String, Boolean> workerHots = hotKey.areWorkerHotKeys(List.of("user:1", "user:2"));

// Direct TopK operations
hotKey.notifyLocalDetectorDirect("user:123", 100);

// Top-N queries
List<Item> top10 = hotKey.returnLocalTopNHotKeys(10);
```

### Degradation

HotKey forms a 3-level degradation chain via `supplier` callbacks:

Component failure behavior:

| Failed Component           | Impact                                                        | Recovery                      |
| -------------------------- | ------------------------------------------------------------- | ----------------------------- |
| HotKey itself              | L1 unavailable; exception or hot-key degradation (if enabled) | App restart                   |
| L2 backend (Redis/DB/API)  | Every request penetrates to caller-side fallback              | Automatic on backend recovery |
| L1 Caffeine OOM / eviction | Single key evicted, next read re-fetches from source          | Automatic (Caffeine internal) |

> The caller must always handle `Optional.empty()` — HotKey does not hide backend failures.

Write-path failure behavior:

| Write Method                                        | Failure Scenario                           | Behavior                                                                                       |
| --------------------------------------------------- | ------------------------------------------ | ---------------------------------------------------------------------------------------------- |
| `putLocal`                                          | Any scenario                               | No-op (no DB/network dependency)                                                               |
| `putThrough`                                        | Thread pool queue full (non-transactional) | `RejectedExecutionException` propagated to caller                                              |
| `putThrough`                                        | `writer.run()` / Redis fails               | Error logged, L1 version not updated, no broadcast sent                                        |
| `putBeforeInvalidate`                               | `mutation.run()` throws                    | Mutation exception caught and logged; local invalidation and broadcast skipped                 |
| `invalidate` / `putBeforeInvalidate` / `putThrough` | `nextVersion()` Redis fails                | Falls back to node-local counter (`Long.MIN_VALUE + counter`, non-persistent, `degraded=true`) |

Worker mode failure behavior:

| Failed Component          | Impact                                                                                               | Recovery                                                           |
| ------------------------- | ---------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| All Workers down          | Local TopK drives L1 TTL; COOL entries can promote to HOT; Worker decisions gracefully degraded      | Restart Worker cluster; Worker broadcast overrides local promotion |
| Partial Workers down      | Unaffected shards continue normally                                                                  | Restart failed Workers; auto-reconnect                             |
| Report channel failure    | Reports queued/buffered (RabbitMQ)                                                                   | Auto-recovery when RabbitMQ recovers                               |
| Worker broadcast failure  | No cross-instance HOT/COOL sync; local TopK works fine                                               | Restart Worker broadcaster                                         |
| `expected-worker-count=0` | Cluster health always unhealthy until first heartbeat received; no quorum-based degradation possible | Set `hotkey.local.expected-worker-count` to fixed Worker count     |
| Reporter BBR backpressure | BBR drops batches when concurrency exceeds budget (CPU >= threshold); permissive below threshold     | Auto-recovery on load decrease                                     |
| Worker TopK persistence   | Redis unavailable: silently skip persistence, `error` log; Worker cold start (no warm start)         | Next periodic persistence succeeds on Redis recovery               |

## Monitoring

HotKey provides two complementary monitoring mechanisms.

Full response format and field descriptions in [MONITOR.md](docs/MONITOR.md) ([中文版](docs/MONITOR.zh.md)).

## Design Details

Domain terminology definitions in [CONTEXT.md](CONTEXT.md).
Architecture Decision Records (ADRs) maintained in [docs/adr/](docs/adr/0001-local-promotion-worker-fallback.md).

## License

Apache License 2.0
