# HotKey

<p align="center">
  <a href="https://central.sonatype.com/artifact/io.github.hyshmily/hotkey"><img src="https://img.shields.io/maven-central/v/io.github.hyshmily/hotkey?color=blue" alt="Maven Central"></a>
  <a href="https://jitpack.io/#Hyshmily/HotKey"><img src="https://jitpack.io/v/Hyshmily/HotKey.svg" alt="JitPack"></a>
  <a href="https://github.com/Hyshmily/hotkey/releases"><img src="https://img.shields.io/github/v/release/Hyshmily/hotkey?color=brightgreen" alt="GitHub Release"></a>
  <a href="https://github.com/Hyshmily/hotkey/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/Hyshmily/hotkey/ci.yml?branch=master&label=CI&logo=github" alt="CI"></a>
  <a href="https://coveralls.io/github/Hyshmily/hotkey?branch=master"><img src="https://coveralls.io/repos/github/Hyshmily/hotkey/badge.svg?branch=master" alt="Coveralls"></a>
  <a href="https://openjdk.java.net/"><img src="https://img.shields.io/badge/Java-17-orange" alt="Java"></a>
  <a href="https://spring.io/projects/spring-boot"><img src="https://img.shields.io/badge/Spring%20Boot-3.5.3-brightgreen" alt="Spring Boot"></a>
  <a href="https://github.com/Hyshmily/hotkey/commits/master"><img src="https://img.shields.io/github/last-commit/Hyshmily/hotkey/master" alt="Last Commit"></a>
  <a href="https://www.apache.org/licenses/LICENSE-2.0"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
  <a href="https://visitor-badge.laobi.icu/badge?page_id=Hyshmily.hotkey"><img src="https://visitor-badge.laobi.icu/badge?page_id=Hyshmily.hotkey" alt="Visitors"></a>
</p>

[**中文版**](README.zh.md)

HotKey is a highly configurable, high-performance, low-cost lightweight distributed caching and prefetching framework.

It is designed to solve cluster-wide distributed-consistency hot-key caching problems for arbitrary, unpredictable traffic surges at minimal cost:

- **Unpredictable hot keys** — A sudden burst of requests can cause certain keys to see exploding access volumes. The `HeavyKeeper` algorithm provides millisecond-level real-time detection.
- **Hot-key synchronization** — RabbitMQ automatically pushes Worker decisions (HOT/COOL) to the entire cluster's L1 cache; manual broadcast for cross-instance sync and invalidation is also supported.
- **Malicious hot-interface requests** — Unknown users flooding the same interface. Supports AOP aspect interception, letting the caller decide the handling strategy (block, circuit-break, return default value, etc.).

Beyond that, HotKey also provides:

<details>
<summary><b>1. Easy Deployment</b></summary>

- **Simple configuration** — Three deployment modes: Maven / Docker / JAR
- **Spring Boot Starter** — Just add the `hotkey` dependency; runs out of the box with zero extra configuration.

</details>

<details>
<summary><b>2. Rich Feature Set</b></summary>

- **Spring Cache Integration** — Standard `@Cacheable` / `@CachePut` / `@CacheEvict` work together with HotKey detection, soft-expiry, and cross-instance sync. Companion annotations include:
  - `@HotKeyCacheTTL`: TTL control
  - `@HotKeyPreload`: Proactive preheating, presets hot keys to skip cold-start waiting
  - `@Intercept`: Custom processing for hot-key request interception
  - `@Fallback`: SpEL expression or naming-convention fallback
  - `@NullCaching`: Null-value caching to prevent cache penetration
  - `@Broadcast`: Broadcast suppression, local writes bypass cluster sync
- **Multi-level Cache** — Fluent API for chaining N-level backup data sources. `.withPrimary(reader)` goes through the full detection pipeline (records and reports), `.thenExecute(fallback)` degrades stepwise bypassing reporting. Results are written to L1 with optional broadcast.
- **Soft Expiry (Logical Expiration)** — Differentiated softTTL/hardTTL configuration, completely replacing traditional Redis-side logical expiry. Redis stores pure values, HotKey manages expiry at the L1 Caffeine layer — stale values are returned immediately with async background refresh, reducing P99 latency.
- **Custom Blacklist/Whitelist Interception** — Blacklist automatically blocks matching key requests; whitelist skips reporting and Worker decisions.
- **Transaction Support** — `TransactionSupport` defers writes until after `@Transactional` commits, ensuring write-path cache-data consistency.
- **Distributed Lock** — Redis-based `tryLock` / `tryLockAndRun`, Lua-script safe release.
- **Zero Side-Effect Reads** — `peek()` is a pure L1 lookup with no detection, no reporting, and no refresh.
- **Actuator Endpoints** — `/actuator/hotkey` (hot-key list), `/actuator/hotkeyring` (consistent hash ring), `/actuator/hotkey/worker/state` (Worker state machine).
- **Micrometer Metrics** — Caffeine hit rate, TopK size, Reporter latency, CPU EMA, etc.

</details>

<details>
<summary><b>3. Multiple Rate-Limiting Algorithms for Cluster Protection</b></summary>

- **Circuit Breaker** — SingleFlight has a built-in sliding-window circuit breaker; returns stale L1 values when the data source times out or throws, preventing cascading failures.
- **BBR Adaptive Rate Limiting** — BBR congestion control fused with CPU EMA monitoring for self-protection; applies backpressure on the Reporter flush path to prevent RabbitMQ/Worker overload.
- **SRE Adaptive Rate Limiting** — Google SRE formula backpressure on the WorkerListener HOT path, independent of BBR, protecting the HOT decision consumption path.
- **CPU Monitoring with EMA Smoothing** — A dedicated daemon thread polls process CPU load every 500ms for stable overload detection.

</details>

<details>
<summary><b>4. Multi-Level Fault Protection and Graceful Degradation</b></summary>

- **Graceful Degradation** — When all Workers are down, the Reporter silently drops reports and the local TopK takes over HOT decisions; when Workers recover, they reassert authority — zero manual intervention.
- **Write-Path Degradation** — `VersionController.nextVersion()` falls back to a local negative-space counter when Redis INCR fails; writes are not interrupted.
- **Reporter Backpressure** — BBR + CPU EMA; automatically drops packets at lower frequency when RabbitMQ is congested, without blocking business threads.
- **Staleness Guard** — Batches waiting more than 5s in the Reporter dispatcher are automatically discarded; the Worker side similarly drops stale messages older than 5s.
- **Poison Message Protection** — ReportConsumer has an outer catch-all; exception messages are discarded directly without entering a dead-letter retry loop.
- **State Machine Rollback** — If Worker broadcast fails, it automatically rolls back to the pre-evaluation snapshot, preventing a "half-hot" inconsistent state.
- **Transactional Consistency** — `TransactionSupport` defers cache writes until after `@Transactional` commits.
- **TopK Persistence** — Worker restores TopK snapshots in seconds on restart, avoiding cold-start misjudgment.
- **TTL Jitter (±10%)** — Prevents cache stampedes when large numbers of keys expire simultaneously.
- **Consistent Hash Self-Healing** — 32-bit Murmur3 + 500 virtual nodes + heartbeat-driven dynamic routing; Worker scaling requires no static configuration.
- **Decision Version Control** — `decisionVersion` is monotonically increasing per Worker + `dataVersion` tolerates degradation, ensuring correct cross-Worker decision ordering.

</details>

> [!TIP]
>
> HotKey supports custom parameters (see [CONFIG.zh.md](docs/CONFIG.zh.md) for the full property reference).
>
> Default full-chain end-to-end latency: **300ms (P99)**

```
          ┌──────────────┬──────────────┬──────────────┬───────────────┐
          0ms            50ms          100ms          150ms          200ms
          │              │              │              │
L1 Hit    █┤  0.01ms
          (Caffeine direct hit, instant return)

L1 Miss   ██┤  +~10ms ← SingleFlight(3s timeout) + L2 Redis/DataBase fallback

Report    ███████████┤  +~25ms(P50) ← report-interval-ms=50 triggers flush

↑AMQP pub ██┤  +~5ms ← network + RabbitMQ enqueue

Worker    ██┤  +~5ms ← dequeue + deserialization
consume

Sliding   ██████████████████████████████┤  +~100ms (hot key identifiable within ~100ms)
window

State     ██████████████████████████████┤  +~100ms (2×confirm window, 50ms each)
machine

↓Decision ██┤  +~5ms (AMQP fanout to all App instances)
broadcast

Warmup    ███┤  +~25ms(P50) ← warmupJitterMs=50, uniform avg
jitter

L1 Cache  █┤ +~5ms ← (Caffeine cache , query from Redis)
```

HotKey is inspired by JD.com's [hotkey](https://gitee.com/jd-platform-opensource/hotkey) project; algorithmic support comes from [Aegis](https://github.com/go-kratos/aegis).

## Quick Start

### 1. Add Dependency

Configuration reference:

<details>
<summary><b>Quick-start YAML Template</b></summary>

**Local mode (App side)** — Just add the `hotkey` dependency; uncomment optional features as needed.

```yaml
hotkey:
  # All local parameters use defaults; no explicit configuration required.

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

  # Consistent hashing is enabled by default (dynamic Worker routing via heartbeat)
  # local:
  #   consistent-hashing:
  #     enabled: true     # (enabled by default)
```

**Single Worker (standalone deployment node)** — Add `spring-boot-starter-amqp`

```yaml
hotkey:
  worker:
    enabled: true
    routing:
      app-name: myapp # [Required] Must match App-side hotkey.local.app-name
      # Consistent hashing is enabled by default; Workers auto-register via heartbeat

# Multi-Worker example: 3 machines, same app-name
# Consistent hashing automatically routes keys to the correct Worker via heartbeat.
# No static sharding required — just add machines. It is recommended to deploy the App first, then start Workers.
```

> [!IMPORTANT]
> **Cluster health threshold** — When `expected-worker-count: 0` (dynamic mode, default), `min-alive-workers: 0` is equivalent to **1 alive = healthy**. When `expected-worker-count: N` (fixed mode), the majority formula `N/2 + 1` is used. Setting `min-alive-workers` overrides either mode. See `docs/CONFIG.md` for details.

**Full parameters (overriding defaults)**

```yaml
hotkey:
  local:
    # ——— Must be consistent across all nodes (App side must match Worker side) ———
    app-name: "default" # Must match worker.routing.app-name

    # ——— Instance Identity ———
    instance-id: "" # Explicit instance ID (empty = auto-generated)

    # ——— Report ———
    report-exchange: "hotkey.report.exchange" # Must match worker.messaging.report-exchange
    report-interval-ms: 50 # Batch send interval (ms)
    queue-capacity: 10000 # Report queue capacity
    queue-offer-timeout-ms: 100 # Queue write timeout (ms)
    consumer-count: 0 # Consumer threads (0=auto)

    # ——— HeavyKeeper Algorithm ———
    topK: 100 # Number of hot keys to retain
    width: 50000 # Count-Min Sketch width
    depth: 5 # Count-Min Sketch depth
    decay: 0.92 # Decay factor (applied each fading cycle)
    minCount: 10 # Minimum access count for hot key detection
    expelled-queue-capacity: 50000 # Capacity of the expelled hot key staging queue

    # ——— L1 Caffeine Cache ———
    local-cache-max-size: 1000 # Maximum number of entries
    local-cache-ttl-minutes: 5 # Cache expiry (minutes)

    # ——— SingleFlight Deduplication ———
    inflight-max-size: 50000 # Maximum number of dedup keys tracked
    inflight-ttl-seconds: 5 # Dedup entry TTL (must be > L2 response time)
    inflight-timeout-seconds: 3 # Async result timeout (must be < inflight-ttl-seconds)

    # ——— Async Executor ———
    executor-core-pool-size: 8 # Core thread count
    executor-max-pool-size: 32 # Maximum thread count
    executor-queue-capacity: 2000 # Work queue capacity
    scheduler-pool-size: 8 # Scheduler thread pool size

    # ——— TTL Jitter (Cache Stampede Protection) ———
    ttl-jitter-ratio: 0.05 # Offset ratio (0.0~1.0), 0.05 = ±5% (always enabled)

    # ——— Refresh & Version Control ———
    refresh-max-pools: 100 # Refresh thread pool limit
    version-key-ttl-minutes: 60 # Redis version key TTL

    # ——— Normal Key TTL ———
    default-hard-ttl-ms: 300000 # Default hard expiry (5min)
    hard-ttl-ms: 0 # Override (0 = use default)
    default-soft-ttl-ms: 30000 # Default soft expiry (30s)
    soft-ttl-ms: 0 # Override (0 = use default)

    # ——— Hot Key TTL (effective after Worker marks HOT) ———
    default-hot-hard-ttl-ms: 3600000 # Default hot key hard expiry (1h)
    hot-hard-ttl-ms: 0 # Override (0 = use default)
    default-hot-soft-ttl-ms: 300000 # Default hot key soft expiry (5min)
    hot-soft-ttl-ms: 0 # Override (0 = use default)

    # ——— Null Value TTL ———
    null-value-ttl-seconds: 10 # TTL for null/cache-miss entries (short, to avoid caching negative results too long)

    # ——— Consistent Hashing (enabled by default; dynamic Worker routing) ———
    consistent-hashing:
      enabled: true # Dynamic Worker routing via heartbeat
      virtual-nodes: 500 # Virtual nodes per physical Worker

    # ——— Heartbeat (App side; Worker health monitoring) ———
    heartbeat:
      exchange-name: "hotkey.heartbeat.exchange" # Must match worker.messaging.heartbeat-exchange
      timeout-ms: 30000 # Worker deemed dead if no heartbeat received within this window
      verify-interval-ms: 5000 # Suspected Worker verification interval (supports exponential backoff)
      ping-timeout-ms: 3000 # Direct reply-to PING timeout
      degrade-after-failures: 3 # Degrade after N consecutive PING failures (exponential backoff)
      verify-max-backoff-ms: 600000 # Per-Worker exponential backoff max interval (10min)
      min-alive-workers: 0 # 0=dynamic (1 alive = healthy); >0 requires at least N Workers alive

    # ——— Circuit Breaker (optional, disabled by default) ———
    circuit-breaker:
      enabled: false
      window-time-ms: 10000
      window-buckets: 10
      fail-threshold: 0.5
      request-volume-threshold: 20
      single-test-interval-ms: 5000
      log-enabled: true

    # ——— Reporter Rate Limiter (BBR + CPU Fusion) ———
    reporter:
      enabled: true # BBR adaptive rate limiting
      cpu-threshold: 800 # CPU threshold (0-1000, 800=80%)
      cpu-poll-interval-ms: 500 # CPU poll interval (ms)
      cpu-decay: 0.95 # EMA smoothing decay factor
      bbr-window-ms: 10000 # BBR sliding window (ms)
      bbr-window-buckets: 100 # BBR sliding window buckets
      bbr-cooldown-ms: 1000 # Cooldown after drop (ms)

  # Feature flags
  report:
    enabled: true # App-to-Worker reporting
  scheduling:
    enabled: true # Scheduled decay & expelled cleanup
  decay-period: 20 # HeavyKeeper fading interval (seconds), effective when scheduling enabled

  # App side — Worker Decision Listener (receives HOT / COOL)
  worker-listener:
    enabled: false # Requires Redis + RabbitMQ
    exchange-name: "hotkey.broadcast.exchange" # Must match worker.messaging.broadcast-exchange
    queue-prefix: "hotkey.worker"
    auto-startup: true # Start with application
    warmup-jitter-ms: 50 # Random delay before processing (prevents thundering herd)
    concurrent-consumers: 2 # Number of concurrent consumers
    scheduler-pool-size: 2 # Delayed task thread pool size
    prefetch-count: 5 # AMQP prefetch per consumer

    # SRE adaptive rate limiter (HOT decision processing path)
    sre:
      enabled: true # Enable SRE rate limiter
      window-ms: 3000 # Sliding window duration (ms)
      buckets: 10 # Sliding window buckets
      min-samples: 20 # Minimum samples before rate limiting starts
      success-threshold: 0.6 # Success rate threshold (0.0-1.0)

  # App side — Cross-instance cache sync
  sync:
    enabled: false
    exchange-name: "hotkey.sync.exchange"
    queue-prefix: "hotkey.sync"
    auto-startup: true
    dedup-window-seconds: 10 # Message deduplication window
    dedup-max-size: 10000 # Dedup cache limit
    warmup-jitter-ms: 100 # Random delay before processing (prevents thundering herd)
    concurrent-consumers: 3 # Number of concurrent consumers
    scheduler-pool-size: 4 # Delayed task thread pool size
    prefetch-count: 5 # AMQP prefetch per consumer

  # Worker side — Standalone deployment node
  worker:
    enabled: false

    routing:
      app-name: "default" # Must match local.app-name
      # Routing is managed automatically via consistent hashing and heartbeat

    messaging:
      report-exchange: "hotkey.report.exchange" # Must match local.report-exchange
      broadcast-exchange: "hotkey.broadcast.exchange" # Must match worker-listener.exchange-name
      heartbeat-exchange: "hotkey.heartbeat.exchange" # Must match local.heartbeat.exchange-name

    report-consumer:
      concurrent-consumers: 8 # Number of concurrent consumers for the report queue
      prefetch-count: 50 # Prefetch per consumer

    sliding-window:
      duration-ms: 1000 # Window duration (ms), each slice = 1000/10 = 100ms
      slices: 10 # Number of window slices

    threshold:
      hot-threshold: 1000 # Absolute QPS threshold (<=0 means use ratio threshold)
      hot-threshold-ratio: 0.01 # Relative QPS ratio threshold (1%)

    state-machine:
      sm-duration-ms: 500 # State machine time-slice window duration (ms), independent of sliding window
      sm-slices: 10 # Time slices in state machine window, each slice = 500/10 = 50ms
      confirm-duration-ms: 100 # Total confirmation duration, confirmCount = ceil(100/50) = 2
      cool-duration-ms: 600000 # Cooling window (ms), sustained cold beyond this marks COOL
      pre-cool-grace-ms: 60000 # Pre-cooling grace period (ms)
      evict-interval-ms: 30000 # Expired state eviction interval (ms), must be >= coolDurationMs * 2

    global-qps-dynamic-threshold:
      qps-change-tolerance: 0.5 # QPS change tolerance multiplier
      learning-period-ms: 30000 # Learning period (ms)
      hot-threshold-ratio: 0.01 # Dynamic threshold ratio
      recalculate-interval-ms: 60000 # Recalculation interval (ms)

    topk-validation:
      validate-interval-ms: 60000 # Validation interval (ms)
      pre-warm-count: 5 # Pre-warm count
      pre-warm-min-appearances: 2 # Minimum appearances

    heavy-keeper:
      top-k: 100 # Number of hot keys to retain
      width: 20000 # Sketch width
      depth: 10 # Sketch depth
      decay: 0.9 # Decay factor
      min-count: 10 # Minimum count

    heartbeat:
      ping-interval-ms: 1000 # Heartbeat broadcast interval (ms)

    persistence:
      enabled: false # Periodically snapshot TopK to Redis (requires manual enabling)
      persist-interval-ms: 30000 # Snapshot interval (ms)
      topk-count: 100 # Number of keys saved per snapshot
      redis-key-prefix: "hotkey:topk:worker:" # Redis key prefix
      ttl-days: 3 # Redis data expiry (days)
```

</details>

#### Local Mode (App Side) — Maven Dependency

Simply add the starter to your `pom.xml`; Caffeine L1 + local HeavyKeeper + Reporter activate automatically with zero extra configuration.

**Maven Central** (no extra repository needed):

```xml
<dependency>
  <groupId>io.github.hyshmily</groupId>
  <artifactId>hotkey</artifactId>
  <version>1.1.53</version>
</dependency>
```

**JitPack** (always the latest snapshot):

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

`~/.m2/settings.xml` requires authentication:

```xml
<server>
  <id>github</id>
  <username>hyshmily</username>
  <password>${env.GITHUB_TOKEN}</password>
</server>
```

#### Worker Node (Standalone) — JAR / Docker

> [!IMPORTANT]
>
> **Prerequisites:** Redis + RabbitMQ

The Worker is a standalone Spring Boot application (not published to Maven Central). Pre-built images are hosted on GHCR.

**Pull:** Log in with a GitHub PAT that has `read:packages` scope:

```bash
echo $PAT | docker login ghcr.io -u hyshmily --password-stdin
```

**Full-stack startup via docker compose** (includes Redis + RabbitMQ):

```bash
docker compose -f worker/docker-compose.yml up -d
```

**Scale out multiple Worker instances:**

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

### 2. Configuration

Default local configuration works for most scenarios.

> [!IMPORTANT]
> In a distributed deployment, RabbitMQ and Redis are required.

**Optional feature configuration:**

| Feature                  | Enable By                                      | Description                                                            |
| ------------------------ | ---------------------------------------------- | ---------------------------------------------------------------------- |
| Redis L2 cache           | Add `RedisTemplate` Bean                       | Two-level cache with L2 fallback                                       |
| Cross-instance sync      | `hotkey.sync.enabled=true`                     | RabbitMQ-based cache invalidation                                      |
| Worker Listener          | `hotkey.worker-listener.enabled=true`          | Receive HOT/COOL decisions from Worker                                 |
| Worker mode              | `hotkey.worker.enabled=true`                   | Run a dedicated Worker node                                            |
| Worker TopK persistence  | `hotkey.worker.persistence.enabled=true`       | Warm start from Redis after restart                                    |
| Access reporting         | `hotkey.report.enabled=true` (default)         | Report access counts to Worker                                         |
| Reporter self-protection | `hotkey.local.reporter.enabled=true` (default) | BBR backpressure on Reporter flush                                     |
| Spring Cache integration | `hotkey.spring-cache.enabled=true`             | `@Cacheable` / `@CachePut` / `@CacheEvict` fused with HotKey detection |

See [CONFIG.md](docs/CONFIG.md) for the full property reference.

### 3. Usage

**Read Operations**

```java
@Autowired
private HotKey hotKey;

// A. peek — L1 lookup only, no hot-key tracking
Optional<String> r = hotKey.peek("user:123"); // Returns Optional.empty() on L1 miss

// A1. peekAll — Batch L1 lookup
Map<String, Object> batch = hotKey.peekAll(List.of("user:1", "user:2", "user:3"));

// B. computeIfAbsent — Simplified get (no Optional wrapper)
String val = hotKey.computeIfAbsent("user:123", () -> redisTemplate.opsForValue().get("user:123"));

// B1. computeIfAbsent — Batch reload
Map<String, String> users = hotKey.computeIfAbsent(keys, (k) -> loadUser(k));

// C. get — Two-level cache (Redis or any backend)
Optional<String> r = hotKey.get("user:123", () -> redisTemplate.opsForValue().get("user:123"));

// D. getWithSoftExpire — Soft expiry (stale-while-revalidate)
Optional<String> r = hotKey.getWithSoftExpire("user:123", () -> redisTemplate.opsForValue().get("user:123"));

// E. Fluent read API with fallback chain
Optional<User> user = hotKey
  .read("user:42")
  .withPrimary(userRepo::findById)
  .thenExecute(backupRepo::findById)
  .withHardTtl(30_000)
  .withSoftTtl(10_000)
  .allowBroadcast()
  .execute();
```

**Soft expiry** returns stale values immediately while triggering an async background refresh. Redis stores pure values without wrapping — HotKey manages expiry entirely at the L1 Caffeine layer.

| Dimension     | Traditional Logical Expiry                              | HotKey Soft Expiry                                         |
| ------------- | ------------------------------------------------------- | ---------------------------------------------------------- |
| Expiry store  | Embedded in Redis value (`RedisData{data, expireTime}`) | L1 Caffeine metadata (`softExpireAt`)                      |
| Stale return  | Parse wrapper, return old data                          | Return L1 stale value directly                             |
| Async rebuild | Redis distributed lock + custom thread pool             | SingleFlight (local) + `hotKeyExecutor` + `refreshLimiter` |
| Redis format  | Wrapped JSON                                            | Pure value (no wrapping)                                   |
| DB fallback   | Manual locking logic                                    | Native `orElseGet` / `orElseThrow`                         |

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
// F. putThrough — Write-through + broadcast
hotKey.putThrough("user:123", newValue, () -> redisTemplate.opsForValue().set("user:123", newValue));

// G. putBeforeInvalidate — Mutate then invalidate (collection types)
hotKey.putBeforeInvalidate(key, () -> redisTemplate.opsForSet().add(key, members));

// H. putLocal — Local only, no broadcast, no version bump
hotKey.putLocal("user:123", cachedValue);
hotKey.putLocal("user:123", cachedValue, hardTtlMs, softTtlMs); // Custom TTL

// H1. putLocal — Batch local write
hotKey.putLocal(Map.of("user:1", v1, "user:2", v2));

// I. evictLocal — Evict from local cache only, no broadcast, no version bump
hotKey.evictLocal("user:123");                          // Single key
hotKey.evictLocal(List.of("user:1", "user:2", "user:3")); // Batch

// J. refresh — Local eviction then load and cache
hotKey.refresh("user:123", () -> loadUser(123));
hotKey.refresh("user:123", () -> loadUser(123), hardTtlMs, softTtlMs); // With TTL override
hotKey.refreshAll(Map.of("user:1", () -> loadUser(1), "user:2", () -> loadUser(2))); // Batch

// K. Fluent write API
hotKey.write("user:42").withHardTtl(30_000).putThrough(newValue, dbWriter);
hotKey.write("user:42").putBeforeInvalidate(dbMutation);
hotKey.write("user:42").invalidate();
```

`putBeforeInvalidate` is designed for Redis collection types (List, Set, ZSet). `putThrough` requires a complete new value to update L1, but LPUSH/SADD/ZADD modify only a single element — the caller cannot know the full new value. After mutation, L1 is invalidated; the next `get()` automatically falls back to Redis.

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

**Database Fallback and Cache Penetration Protection**

```java
Optional<String> r = hotKey.get("user:123", () -> redisTemplate.opsForValue().get("user:123"));
if (r.isEmpty()) {
    String value = userService.getById(123); // DB fallback
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
> **Serialization:** HotKey uses `StringRedisTemplate` internally; value serialization is entirely up to the caller. **Jackson** (Spring Boot default, JSON) or **Kryo** (binary, maximum throughput) are recommended. JDK native serialization is not recommended.

// Custom L2 cache (non-Redis): MySQL, remote API, or any data source
Optional<User> r = hotKey.get("user:123", () -> userMapper.selectById(123));

User user = r.orElseGet(() -> createDefaultUser());
```

**Distributed Lock**

Distributed locks protect external resources (DB writes, RPC calls) — cache operations are performed as side effects after the lock is released.

```java
// Lock protects external writes; refresh cache after unlock (outside lock)
boolean ok = hotKey.tryLockAndRun("order:42", 5, TimeUnit.SECONDS, () -> {
    orderService.deductStock(orderId, quantity); // DB write requires mutual exclusion
    hotKey.invalidate("order:" + orderId);        // Post-invalidation, not inside lock
});

// Custom retry count (negative falls back to default)
try (AutoReleaseLock lock = hotKey.tryLock("order:42", 5, TimeUnit.SECONDS, 3, 1, 3)) {
    if (lock != null) { /* critical section */ }
}
```

**Custom Per-Entry TTL**

HotKey uses **differentiated TTLs**: hot keys and normal keys have separate default values. Per-call overrides work on top of these.

| Key State | Hard TTL (Caffeine eviction)   | Soft TTL (stale-while-revalidate) |
| --------- | ------------------------------ | --------------------------------- |
| Normal    | `default-hard-ttl-ms` (5min)   | `default-soft-ttl-ms` (30s)       |
| Hot       | `default-hot-hard-ttl-ms` (1h) | `default-hot-soft-ttl-ms` (5min)  |

```java
// 5-minute hard TTL + 30-second soft TTL
Optional<String> shopJson = hotKey.get("shop:" + shopId,
    () -> redisTemplate.opsForValue().get("shop:" + shopId),
    TimeUnit.MINUTES.toMillis(5), TimeUnit.SECONDS.toMillis(30));

// 30-second hard TTL, soft TTL uses default
hotKey.putThrough("weather:" + city, weatherData,
    () -> redisTemplate.opsForValue().set("weather:" + city, weatherData),
    TimeUnit.SECONDS.toMillis(30), 0);
```

> [!NOTE]
> **Cache stampede protection:** `CacheExpireManager` applies a uniform random offset (default ±5%) to each expiry timestamp via `DelayUtil.computeTtlJitter()`. With the default offset, a 5-minute hard TTL actually expires between 4.75 and 5.25 minutes. Controlled by `hotkey.local.ttl-jitter-ratio` (default `0.05` = ±5%). Always enabled.

> [!TIP]
> Per-call TTL semantics: passing `0` means "use the configured default for this key state." For pure logical expiry (hard TTL never evicts): pass `hardTtlMs = Long.MAX_VALUE` to `getWithSoftExpire(key, reader, Long.MAX_VALUE, softTtlMs)` — the entry stays in Caffeine permanently. This usage is explicitly supported by Caffeine's `Expiry` JavaDoc: _"To indicate no expiration an entry may be given an excessively long period, such as `Long.MAX_VALUE`."_ ([source](https://github.com/ben-manes/caffeine/blob/master/caffeine/src/main/java/com/github/benmanes/caffeine/cache/Expiry.java))

**Worker Mode**

Worker mode provides cluster-wide hot-key detection through dedicated nodes. App instances periodically report access counts; Workers run a sliding window + state machine pipeline, broadcasting HOT/COOL decisions back to all instances. State machine parameters (`confirmCount`, `coolCount`, `preCoolGraceCount`) can be adjusted at runtime via `/actuator/hotkey/worker/state`.

| Mode        | `worker.enabled`  | Active Beans                                                                |
| ----------- | ----------------- | --------------------------------------------------------------------------- |
| App-only    | `false` (default) | `HotKeyCache`, TopK, reporter, actuator, sync                               |
| Worker-only | `true`            | Worker only (no cache — `get()`/`putThrough()` throw `HotKeyModeException`) |

In **Worker-only** mode, cache operations throw `HotKeyModeException`.

**Cross-Worker decisionVersion partitioning:** Each Worker has a unique `nodeId` and `epoch` counter. When broadcasting HOT/COOL decisions, the Worker attaches both as AMQP headers. The App-side `WorkerListener` propagates `nodeId`/`epoch` to `CacheEntry.decisionNodeId`/`decisionEpoch` through a double-checked locking path. `VersionGuard` compares decisions per-Worker partition: an incoming decision only wins if its `epoch` >= the cached `decisionEpoch`, using `nodeId` to distinguish version spaces. This prevents stale decisions from a restarted Worker from overriding newer ones — each startup increments the `epoch` counter, resetting authority.

**Worker cluster health:** Set `hotkey.local.expected-worker-count` to the expected number of Workers in production. When set to >0, `ClusterHealthView` uses majority quorum (`> expectedWorkerCount / 2`) as the threshold for healthy Worker count; when 0 (default), the cluster is always considered unhealthy until at least one heartbeat is received. This enables precise partial-Worker failure detection and graceful degradation decisions.

**Worker TopK Persistence (Warm Start):** When `hotkey.worker.persistence.enabled=true`, the Worker periodically snapshots its TopK list to Redis. On restart, `TopKPersistService` loads the last snapshot and replays it into the HeavyKeeper sketch, reducing warm-up from hours to seconds.

**Spring Cache Integration**

Enable with `hotkey.spring-cache.enabled=true`. Standard `@Cacheable` / `@CachePut` / `@CacheEvict` are automatically routed through HotKey's hot-key detection, soft expiry, and cross-instance sync.

| Annotation        | Effect on `@Cacheable`                                                                                                                                |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `@HotKeyCacheTTL` | Override hard/soft TTLs                                                                                                                               |
| `@HotKeyPreload`  | Pre-inflate HeavyKeeper counts so known hot keys take effect immediately                                                                              |
| `@Intercept`      | Skip method body via trigger mode (`IS_LOCAL_HOT`/`FORCE`/`QPS`); falls back per priority: `@Intercept.fallback()` &rarr; `@Fallback` &rarr; `peek()` |
| `@Fallback`       | Provide a fallback value when blocked by blacklist, intercepted, or on exception                                                                      |
| `@NullCaching`    | Optionally cache null return values (default `true`)                                                                                                  |
| `@Broadcast`      | Suppress cross-instance sync messages                                                                                                                 |

```java
@Cacheable(cacheNames = "users", key = "#id")
@HotKeyCacheTTL(softTtlMs = 1000)
@Intercept @Fallback
public User getUser(Long id) { ... }

// QPS rate-limiting interception
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

Each instance declares its own queue (`hotkey.sync:<instanceID>`) bound to a fanout exchange. Four message types:

- **`TYPE_REFRESH`** — Versioned refresh. The peer calls `CacheSyncListener.handleRefresh()` to reload from Redis, skipping stale updates based on `dataVersion`. The 4-case comparison (normal-normal, normal-degraded, degraded-normal, degraded-degraded) ensures that a normal (Redis INCR) dataVersion always takes precedence over a degraded (node-local) one. Sent by `invalidate()` and `putThrough()`.
- **`TYPE_INVALIDATE`** — Single-key invalidation (with version guard). The peer removes the L1 entry only if the incoming `dataVersion` is not stale. Sent by `putBeforeInvalidate()`.
- **`TYPE_INVALIDATE_ALL`** — Batch invalidation (no version guard). The peer immediately removes all listed keys from L1 without reloading. Sent by `invalidateAllLocal()`.
- **`TYPE_RULES_SYNC`** — Rule set replacement. Body is a JSON-serialized `List<Rule>`; the receiver calls `RuleMatcher.syncRules()` to atomically replace the local rule list. Does not trigger a secondary broadcast.

> [!CAUTION]
> All three RabbitMQ exchanges (`hotkey.sync.exchange`, `hotkey.report.exchange`, `hotkey.broadcast.exchange`) use plain AMQP connections by default. In production, configure TLS via Spring Boot's `spring.rabbitmq.ssl.*` properties:
>
> See [Spring Boot RabbitMQ SSL documentation](https://docs.spring.io/spring-boot/reference/messaging/amqp.html#page-title).

## Rule System

Enable `hotkey.sync.enabled=true` to enable cross-instance rule sync. The rule system provides two actions:

| Action            | Effect on Matching Key                                                                           |
| ----------------- | ------------------------------------------------------------------------------------------------ |
| `BLOCK`           | `get()` / `getWithSoftExpire()` throw `HotKeyBlockedException`; `putThrough()` skipped           |
| `ALLOW_NO_REPORT` | Processed normally but Worker reporting is skipped (reduces noise from frequently accessed keys) |

### Pattern Types

`RuleMatcher.of(pattern, action)` auto-detects the pattern:

| Pattern             | Type       | Matches                         |
| ------------------- | ---------- | ------------------------------- |
| `"user:123"`        | `EXACT`    | Exact key                       |
| `"temp:*"`          | `PREFIX`   | Keys starting with `temp:`      |
| `"order:*-detail"`  | `WILDCARD` | Glob-style (`*` / `?`) matching |
| `"regex:user:\\d+"` | `REGEX`    | Java regex                      |

### Persistence and Broadcasting

- **With Redis:** Each `addRule()`/`removeRule()`/`clearRules()` serializes the rule list to `HotKeyConstants.REDIS_KEY_RULES` (`"hotkey:rules"`). On startup, `RuleMatcher.initRules()` loads from Redis. Changes are also broadcast via `TYPE_RULES_SYNC` — peers atomically replace via `RuleMatcher.syncRules()` without triggering a secondary broadcast (avoiding storms).
- **Without Redis:** The same operations are broadcast to all peers via the `CacheSyncPublisher` fanout exchange. Each peer holds the full rule set in memory.
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
boolean blocked = hotKey.isBlacklisted("user:123");         // true if blocked
boolean skipReport = hotKey.isWhitelisted("health:ping");   // true if reporting skipped

// Remove rules
hotKey.removeBlacklist("secret:*");
hotKey.clearAllRules();

// Cache monitoring
long size = hotKey.estimatedSize();    // L1 entry count
HotKeyCacheStats s = hotKey.stats();   // Hit rate, eviction count, etc.

// Emergency flush (no broadcast)
hotKey.invalidateAllLocal();

// Batch rule evaluation
Map<String, Rule.RuleAction> actions = hotKey.evaluateRules(List.of("user:1", "user:2"));
Map<String, Boolean> blocked = hotKey.isBlacklisted(List.of("user:1", "user:2"));
Map<String, Boolean> skipReport = hotKey.isWhitelisted(List.of("health:ping", "metrics:qps"));

// Batch hot-key check
Map<String, Boolean> localHots = hotKey.areLocalHotKeys(List.of("user:1", "user:2"));
Map<String, Boolean> workerHots = hotKey.areWorkerHotKeys(List.of("user:1", "user:2"));

// Direct TopK operation
hotKey.notifyLocalDetectorDirect("user:123", 100); // Batch increment local TopK count by 100

// Top-N query
List<Item> top10 = hotKey.returnLocalTopNHotKeys(10);
```

### Degradation

HotKey forms a three-level degradation chain through `supplier` callbacks:

Component fault behavior:

| Faulty Component           | Impact                                                        | Recovery                        |
| -------------------------- | ------------------------------------------------------------- | ------------------------------- |
| HotKey itself              | L1 unavailable; exception or hot-key degradation (if enabled) | Application restart             |
| L2 backend (Redis/DB/API)  | Each request falls through to caller fallback                 | Automatic upon backend recovery |
| L1 Caffeine OOM / eviction | Single key evicted; next read re-fetches from source          | Automatic (Caffeine internal)   |

Write-path fault behavior:

| Write Method                                        | Failure Scenario                | Behavior                                                                                       |
| --------------------------------------------------- | ------------------------------- | ---------------------------------------------------------------------------------------------- |
| `putLocal`                                          | Any scenario                    | No-op (no DB/network dependency)                                                               |
| `putThrough`                                        | Thread pool queue full (non-tx) | `RejectedExecutionException` propagated to caller                                              |
| `putThrough`                                        | `writer.run()` / Redis fails    | Error logged; L1 version not updated; no broadcast sent                                        |
| `putBeforeInvalidate`                               | `mutation.run()` throws         | Mutation exception caught and logged; local invalidation and broadcast skipped                 |
| `invalidate` / `putBeforeInvalidate` / `putThrough` | `nextVersion()` Redis fails     | Falls back to node-local counter (`Long.MIN_VALUE + counter`, non-persistent, `degraded=true`) |

Worker mode fault behavior:

| Faulty Component          | Impact                                                                                         | Recovery                                                             |
| ------------------------- | ---------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| All Workers down          | Local TopK drives L1 TTL; COOL entries may upgrade to HOT; Worker decisions gracefully degrade | Restart Worker cluster; Worker broadcast overrides local promotion   |
| Partial Worker crash      | Unaffected shards continue normal operation                                                    | Restart crashed Worker; auto-reconnect                               |
| Report channel failure    | Reports are queued/buffered (RabbitMQ)                                                         | Automatic recovery when RabbitMQ recovers                            |
| Worker broadcast failure  | No cross-instance HOT/COOL sync; local TopK operates normally                                  | Restart Worker broadcaster                                           |
| `expected-worker-count=0` | Cluster always unhealthy until first heartbeat; no quorum-based degradation                    | Set `hotkey.local.expected-worker-count` to a fixed Worker count     |
| Reporter BBR backpressure | BBR drops batches when concurrency exceeds budget (CPU >= threshold); lenient below threshold  | Automatic recovery when load decreases                               |
| Worker TopK persistence   | Silently skipped when Redis unavailable; `error` log entry; Worker cold start (no warm start)  | Next periodic persistence succeeds automatically when Redis recovers |

## Monitoring

HotKey provides two complementary monitoring mechanisms.

See [MONITOR.md](docs/MONITOR.md) for the full response format and field descriptions ([中文版](docs/MONITOR.zh.md)).

## Design Details

Domain term definitions are in [CONTEXT.md](CONTEXT.md).
Architecture Decision Records (ADRs) are maintained in [docs/adr/](docs/adr/0001-local-promotion-worker-fallback.md).

## License

Apache License 2.0
