# Zeta

<p align="center">
  <a href="https://central.sonatype.com/artifact/io.github.hyshmily/zeta"><img src="https://img.shields.io/maven-central/v/io.github.hyshmily/zeta?color=blue" alt="Maven Central"></a>
  <a href="https://jitpack.io/#Hyshmily/Zeta"><img src="https://jitpack.io/v/Hyshmily/Zeta.svg" alt="JitPack"></a>
  <a href="https://github.com/Hyshmily/Zeta/releases"><img src="https://img.shields.io/github/v/release/Hyshmily/Zeta?color=brightgreen" alt="GitHub Release"></a>
  <a href="https://github.com/Hyshmily/Zeta/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/Hyshmily/Zeta/ci.yml?branch=master&label=CI&logo=github" alt="CI"></a>
  <a href="https://coveralls.io/github/Hyshmily/Zeta?branch=master"><img src="https://coveralls.io/repos/github/Hyshmily/Zeta/badge.svg?branch=master" alt="Coveralls"></a>
  <a href="https://openjdk.java.net/"><img src="https://img.shields.io/badge/Java-17-orange" alt="Java"></a>
  <a href="https://spring.io/projects/spring-boot"><img src="https://img.shields.io/badge/Spring%20Boot-3.5.3-brightgreen" alt="Spring Boot"></a>
  <a href="https://github.com/Hyshmily/zeta/commits/master"><img src="https://img.shields.io/github/last-commit/Hyshmily/zeta/master" alt="Last Commit"></a>
  <a href="https://www.apache.org/licenses/LICENSE-2.0"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
  <a href="https://visitor-badge.laobi.icu/badge?page_id=Hyshmily.zeta"><img src="https://visitor-badge.laobi.icu/badge?page_id=Hyshmily.zeta" alt="Visitors"></a>
</p>

[**中文**](README.zh.md)

Zeta is a configurable, high-performance, low-cost lightweight distributed cache and preheating framework, designed to solve cluster-wide distributed consistent caching problems for arbitrary sudden hotspot data at minimal cost, fully decoupling business code from distributed coordination infrastructure via Redis and RabbitMQ.

### Detection

Zeta provides two-tier hot-key detection — a local in-process HeavyKeeper probabilistic sketch and a remote Worker cluster's sliding-window + Bayesian state machine pipeline.

```java
private final Zeta zeta;

// Tag key in the same namespace
zeta.tag("product:123");

// get key
zeta.peek("product:123");
```

- The double-buffered counter (`BufferedCounter`) batches high-frequency increments: the active `ConcurrentHashMap<String, LongAdder>` is CAS-swapped when it reaches 80% capacity, and a background thread flushes into `HeavyKeeper` every 500ms.

- Each application instance runs a local TopK sketch that tracks frequently accessed keys. When a key enters the local TopK set, its L1 Caffeine cache TTL is automatically extended — no Worker feedback required. On L1 miss, the SingleFlight mechanism merges concurrent requests for the same key to prevent cache breakdown. Soft expiration is also supported — when the soft TTL expires but the hard TTL has not, stale entries are served immediately while a background async refresh is triggered, ensuring response latency.

- `KeyReporter` aggregates local counts through a second BufferedCounter (50ms flush, 100k key cap), then passes them through a CPU-BBR rate limiter (CPU threshold 80%, sliding window 10s/100 buckets) before batch-reporting to RabbitMQ via `DirectExchange` with routing key `report.{appName}.{nodeId}`.

- The Worker cluster aggregates access reports from all application instances and runs a **two-path evaluation pipeline**:
  - **FastLane**: For keys matching user-configured glob rules (e.g. `product:*`), the sliding-window sum is compared directly against the rule's threshold. When the threshold is met, the key is promoted to `CONFIRMED_HOT` immediately — no Bayesian confidence gating, no confirm windows. Under default configuration, end-to-end latency: **~60ms (P99)**.
  - **Bayesian path**: For all other keys, sliding-window frequency analysis combined with a Bayesian confidence state machine (Normal-Normal conjugate posterior with per-key evidence accumulation) produces decisions: `HIGH (≥0.95)` → `CONFIRMED_HOT` broadcasts HOT; `MEDIUM ([0.76, 0.95))` → `CANDIDATE_HOT` accumulates evidence; `LOW (<0.76)` → retention strategy (first 2 occurrences with `hotStreak=confirmCount-1` fast re-evaluation, 3rd occurrence full reset). Promotion latency depends on traffic intensity: a strongly hot key (well above threshold) reaches CONFIRMED_HOT in **~50–150ms**, while a borderline key (near-threshold) may require multiple evaluation windows.

### Multi-Node Cache Coherency

Much like how a primary-backup database synchronizes writes through a log replication protocol, Zeta synchronizes cache mutations across application instances through a publish-subscribe mechanism backed by RabbitMQ.

When any instance performs a write-through (putThrough) or invalidation, it increments a per-key dataVersion (via Redis INCR, with a local fallback for degraded mode) and broadcasts the event to all peers. Each peer compares the incoming dataVersion against its local version — stale messages are silently dropped. When all Workers are unreachable, the local TopK assumes full authority over promotions and the reporter drops silently; when Workers recover, they automatically reclaim control via a higher version number — no manual intervention required.

This version comparison mechanism ensures eventual coherency without the overhead of distributed consensus protocols like Paxos or Raft.

Benchmarks:

- peek ~16M ops/s (pure Caffeine lookup, no side effects)
- get (L1 hit) ~15M ops/s (full path including TopK + Reporter)

Inspired by JD.com's [hotkey](https://gitee.com/jd-platform-opensource/hotkey) project; algorithm support from [Aegis](https://github.com/go-kratos/aegis)、[neural](https://github.com/yu120/neural/tree/master)
.

## Quick Start

### 1. Add Dependency

Configuration reference:

<details>
<summary><b>Quick Deploy YAML Templates</b></summary>

**Local mode (App side)** — just add the `zeta` dependency to run; uncomment optional features as needed

```yaml
zeta:
  # local parameters all use defaults, no explicit config needed

  # —— Optional features, uncomment as needed ——
  # App report to worker
  #report:
  #  enabled: false

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
  #   enabled: true     # worker-listener depends on hotKeyRedisLoader (CacheLoader) Bean
  #                     # Customize: implement CacheLoader -> @Bean (see "Customizing Data Source" below)

  # Consistent hashing is enabled by default (dynamic Worker routing via heartbeat)
  # local:
  #   consistent-hashing:
  #     enabled: true     # (already enabled by default)
```

**Single Worker (standalone node)** — add `spring-boot-starter-amqp`

```yaml
zeta:
  worker:
    enabled: true
    routing:
      app-name: myapp # 【Required】Must match App-side zeta.local.app-name
      # Consistent hashing is enabled by default; Workers register via heartbeat

# Multi-Worker example: 3 machines, same app-name
# Consistent hashing routes keys to the correct Worker via heartbeat automatically
# No static sharding config needed — just add more machines
# Recommended: deploy local App first, then start Workers
```

**Cluster health threshold** — by default (`min-alive-workers: 0`), the cluster is healthy when at least **one third of observed Workers** are alive (rounded up, minimum 1): e.g. 3 observed Workers → 1 alive is healthy; 5 observed → 2 alive; 9 observed → 3 alive. A single surviving Worker is intentionally treated as healthy (see ADR-0028). Set `min-alive-workers: N` to require an absolute number of alive Workers. See `docs/CONFIG.md` for details.

**All parameters:**
See [CONFIG.md](docs/CONFIG.md)

</details>

#### Local Mode (App Side) — Maven Dependency

**Maven Central** (no extra repository needed):

```xml
<dependency>
  <groupId>io.github.hyshmily</groupId>
  <artifactId>zeta</artifactId>
  <version>1.1.56</version>
</dependency>
```

**JitPack:**

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependency>
    <groupId>io.github.hyshmily</groupId>
    <artifactId>zeta</artifactId>
 <version>1.1.56</version>
</dependency>
```

**GitHub Packages:**

```xml
<repositories>
  <repository>
    <id>github</id>
    <name>GitHub Packages</name>
    <url>https://maven.pkg.github.com/hyshmily/zeta</url>
  </repository>
</repositories>

<dependency>
  <groupId>io.github.hyshmily</groupId>
  <artifactId>zeta</artifactId>
  <version>1.1.56</version>
</dependency>
```

#### Worker Node (Standalone) — JAR / Docker

> [!IMPORTANT]
>
> **Prerequisites:**
> Redis + RabbitMQ

Pre-built images are hosted on GHCR.

**Pull:** Log in with a GitHub PAT that has `read:packages` scope:

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
docker run -d --name zeta-worker -p 8080:8080 \
  -e SPRING_RABBITMQ_HOST=rabbitmq \
  -e SPRING_DATA_REDIS_HOST=redis \
  -e ZETA_WORKER_ENABLED=true \
  ghcr.io/hyshmily/zeta-worker:1.1.56
```

**Run JAR directly** (no Docker):

```bash
mvn clean package -pl worker
java -jar worker/target/zeta-worker-1.1.56.jar
```

### 2. Configuration

Default local configuration:

**Feature configuration:**

| Feature                  | How to Enable                                | Description                                                          |
| ------------------------ | -------------------------------------------- | -------------------------------------------------------------------- |
| Redis L2 Cache           | Add `RedisTemplate` Bean                     | Two-level cache, L2 fallback                                         |
| Cross-instance Sync      | `zeta.sync.enabled=true`                     | RabbitMQ-based cache invalidation                                    |
| Worker Listener          | `zeta.worker-listener.enabled=true`          | Receive HOT/COOL decisions from Worker                               |
| Worker Mode              | `zeta.worker.enabled=true`                   | Run a dedicated Worker node                                          |
| Access Reporting         | `zeta.report.enabled=true` (default)         | Report access counts to Worker                                       |
| Reporter Self-Protection | `zeta.local.reporter.enabled=true` (default) | BBR backpressure for Reporter flush                                  |
| Spring Cache Integration | `zeta.spring-cache.enabled=true`             | `@Cacheable` / `@CachePut` / `@CacheEvict` fused with Zeta detection |

See [CONFIG.md](docs/CONFIG.md) for the full property reference.

### 3. Usage

See [CONFIG.md](docs/CONFIG.md) for the full property reference.

**Tag Operations** (feed detection without cache read)

```java
// K. tag — mark a key as potentially hot, no cache read
zeta.tag("product:456"); // updates local TopK + enqueues Worker report

// L. tag with fine-grained control
zeta.tag("product:456", true, false); // skip detection, still report to Worker
zeta.tag("product:456", false, true); // detect locally, skip Worker report
```

**Read Operations**

```java
@Autowired
private Zeta zeta;

// A. peek — L1 only, no hot key tracking
Optional<String> r = zeta.peek("user:123"); // returns Optional.empty() on L1 miss

// B. computeIfAbsent — simplified get (no Optional wrapper)
String val = zeta.computeIfAbsent("user:123", () -> redisTemplate.opsForValue().get("user:123"));

// C. get — two-level cache (Redis or any backend)
Optional<String> r = zeta.get("user:123", () -> redisTemplate.opsForValue().get("user:123"));

// D. getWithSoftExpire — soft expiration (stale-while-revalidate)
Optional<String> r = zeta.getWithSoftExpire("user:123", () -> redisTemplate.opsForValue().get("user:123"));

// E. Fluent read API + fallback chain
User user = zeta
  .read("user:42")
  .withPrimary(userRepo::findById)
  .thenExecute(backupRepo::findById)
  .withHardTtl(30_000)
  .withSoftTtl(10_000)
  .allowBroadcast()
  .executeOrNull();
```

**CachePolicy API** (explicit policy object for per-invocation control)

```java
// P. get with CachePolicy — lazy TTL evaluation, null-caching, stale-policy
CachePolicy policy = CachePolicy.of(30_000L, 10_000L, true, true, StalePolicy.SOFT_REFRESH);

Optional<User> user = zeta.get("user:123", userRepo::findById, policy);

// Q. computeIfAbsentWithSoftExpire with CachePolicy
User user = zeta.computeIfAbsentWithSoftExpire(
  "user:123",
  () -> loadUser(123),
  CachePolicy.of(60_000L, 30_000L, false, false),
  true
); // allowReport
```

**Write Operations**

```java
// R. putThrough — write-through + broadcast
zeta.putThrough("user:123", newValue, () -> redisTemplate.opsForValue().set("user:123", newValue));

// S. putThrough with broadcast control
zeta.putThrough("user:123", newValue, () -> redisTemplate.opsForValue().set("user:123", newValue), false); // skip broadcast

// T. putThrough with explicit hard TTL
zeta.putThrough("user:123", newValue, () -> redisTemplate.opsForValue().set("user:123", newValue), 60_000L);

// U. invalidateAfterPut — mutate then invalidate (collection types)
zeta.invalidateAfterPut(key, () -> redisTemplate.opsForSet().add(key, members));

// V. putLocal — local write only, no broadcast, no version bump
zeta.putLocal("user:123", cachedValue, hardTtlMs, softTtlMs); // custom TTL

// W. refresh — local evict then load and cache
zeta.refresh("user:123", () -> loadUser(123), hardTtlMs, softTtlMs); // with TTL override

// X. Fluent write API
zeta.write("user:42").withHardTtl(30_000).putThrough(newValue, dbWriter);
zeta.write("user:42").putBeforeInvalidate(dbMutation);
```

**Custom per-entry TTL**

Zeta uses **differentiated TTLs**: hot keys and normal keys have independent defaults. Per-call overrides take effect on top.

| Key State | Hard TTL (Caffeine eviction)   | Soft TTL (stale-while-revalidate) |
| --------- | ------------------------------ | --------------------------------- |
| Normal    | `default-hard-ttl-ms` (5min)   | `default-soft-ttl-ms` (30s)       |
| Hot       | `default-hot-hard-ttl-ms` (1h) | `default-hot-soft-ttl-ms` (5min)  |

```java
// 5 min hard TTL + 30s soft TTL
Optional<String> shopJson = zeta.get("shop:" + shopId,
    () -> redisTemplate.opsForValue().get("shop:" + shopId),
    TimeUnit.MINUTES.toMillis(5), TimeUnit.SECONDS.toMillis(30));

// 30s hard TTL, soft TTL uses default
zeta.putThrough("weather:" + city, weatherData,
    () -> redisTemplate.opsForValue().set("weather:" + city, weatherData),
    TimeUnit.SECONDS.toMillis(30), 0);

```

> [!NOTE]
> **Cache avalanche protection:** `ExpireManager` applies a uniform random offset via `DelayUtil.computeTtlJitter()` to every expiration timestamp (default ±5%). A 5-minute hard TTL actually expires between 4.75 ~ 5.25 minutes under the default offset. Controlled by `zeta.local.ttl-jitter-ratio` (ratio, default `0.05` = ±5%, `0` to disable).

> [!TIP]
> Per-call TTL semantics: passing `0` uses the configured default for that key state. For pure logical expiration (hard TTL never evicts, soft expire only): pass `hardTtlMs = Long.MAX_VALUE` to `getWithSoftExpire(key, reader, Long.MAX_VALUE, softTtlMs)` — the entry permanently resides in Caffeine. This usage is explicitly supported by Caffeine's `Expiry` JavaDoc: _"To indicate no expiration an entry may be given an excessively long period, such as `Long.MAX_VALUE`."_ ([source](https://github.com/ben-manes/caffeine/blob/master/caffeine/src/main/java/com/github/benmanes/caffeine/cache/Expiry.java))

Both operations are delegation-based: the caller is responsible for re-reading or re-writing after a successful CAS. There is no L2 lock — the guard is the L1 cache entry's current value at the time of call. Returns `true` if the condition matched and the operation was applied; `false` otherwise.

## Worker Mode

Worker mode provides cluster-wide hotspot detection via dedicated nodes. App instances periodically report access counts; the Worker runs a sliding window + state machine pipeline and broadcasts HOT/COOL decisions back to all instances. State machine parameters (`confirmCount`, `coolCount`, `preCoolGraceCount`) can be adjusted at runtime via `/actuator/hotkey/worker/state`.

| Mode        | `worker.enabled`  | Activated Beans                                                           |
| ----------- | ----------------- | ------------------------------------------------------------------------- |
| App-only    | `false` (default) | `HotKeyCache`, TopK, reporter, actuator, sync                             |
| Worker-only | `true`            | Worker only (no cache — `get()`/`putThrough()` throw `ZetaModeException`) |

**Worker Cluster Health:** The cluster is healthy when at least one third of the Workers observed via heartbeats are alive (rounded up, minimum 1) — e.g. 3 Workers observed → 1 alive is healthy; 9 observed → 3 alive. A single surviving Worker is intentionally treated as a healthy cluster: Worker-side report traffic is compressed and the survivor can serve the cluster; degradation (local COOL→HOT takeover) triggers only when fewer than one third of observed Workers remain. Set `zeta.local.heartbeat.min-alive-workers: N` for an absolute minimum (see ADR-0028).

**FastLane (Immediate Promotion Bypass):** FastLane is an evaluation path that bypasses the Bayesian confidence gating entirely. Keys matching user-configured glob rules (e.g. `product:*`) are promoted to `CONFIRMED_HOT` as soon as the sliding-window sum reaches the rule's threshold — no confirm windows, no confidence scoring, no streak counting. End-to-end latency: **~60ms** (P99).

Configure FastLane rules via properties at startup:

```yaml
zeta:
  worker:
    fast-lane:
      rules:
        - key-pattern: "product:*" # glob pattern
          threshold: 500 # sliding-window sum threshold
        - key-pattern: "flashsale:*"
          threshold: 1000
        - key-pattern: "news:breaking:*"
          threshold: 200
```

Or manage rules at runtime via actuator REST API (no restart needed):

| Method   | Path                                  | Action                |
| -------- | ------------------------------------- | --------------------- |
| `GET`    | `/actuator/hotkey/fastlane`           | List all rules        |
| `POST`   | `/actuator/hotkey/fastlane`           | Add rule              |
| `PUT`    | `/actuator/hotkey/fastlane`           | Update rule threshold |
| `DELETE` | `/actuator/hotkey/fastlane/{pattern}` | Remove rule           |

Example: `curl -X POST -H 'Content-Type: application/json' -d '{"keyPattern":"promo:*","threshold":300}' http://worker:8080/actuator/hotkey/fastlane`

**FastLane state transitions:** A matched key jumps directly to `CONFIRMED_HOT` from any current state (COLD, CANDIDATE_HOT, PRE_COOLING). The `hotStreak` is immediately set to the required confirmation count, `coolStreak` is reset to 0, and a HOT decision is broadcast — even from PRE_COOLING (where normal-path silent revive would suppress the broadcast). Already-confirmed keys simply refresh their timestamp.

**When to use:** Flash-sale product IDs, breaking-news slugs, promotional item IDs — any key where false positives are tolerable and sub-second detection latency is required. For keys requiring high-confidence decisions with minimal false positives, use the default Bayesian path instead.

### Notes

**`hot-threshold: -1` Disables Hot Detection During Learning Period**

When `zeta.worker.threshold.hot-threshold` is set to `-1` (ratio-based mode), the Worker initializes the sliding-window threshold to `Long.MAX_VALUE` — effectively no key can be classified as HOT during the 30-second learning period (`learning-period-ms`, default 30s). After the learning period, `ThresholdLearner` calculates `threshold = globalQps × hotThresholdRatio` and normal operation begins.

**Mitigation:** Set a reasonable absolute threshold alongside the ratio:

```yaml
zeta:
  worker:
    threshold:
      hot-threshold: 1000 # fallback during learning period
    global-qps-dynamic-threshold:
      learning-period-ms: 5000 # shorten for faster convergence
```

Or disable the learning period entirely (`learning-period-ms: 0`) if the traffic pattern is known upfront.

**Worker Broadcast Exchange Declaration**

The Worker module (`zeta-worker`) declares the broadcast exchange (`zeta.send.exchange` by default) as a Spring `FanoutExchange` bean. If deploying Workers separately (not through the provided `WorkerAutoConfiguration`), ensure the exchange exists in RabbitMQ — otherwise the Worker's HOT/COOL broadcasts will fail with a channel-level `not_found` exception.

**PING/PONG Verification Is Auxiliary**

The `WorkerHeartbeatVerifier` periodically sends PING messages to Workers that appear **not alive** (heartbeat timeout > `heartbeat.timeout-ms`, default 10s). Transient failures at startup are expected and safe — the first PING may arrive before the Worker's verify queue is ready. The primary heartbeat path (Worker → `zeta.heartbeat.exchange` → App heartbeat queue) is the authoritative mechanism for HealthView updates and RingManager routing.

**Customizing the Data Source for HOT Promotion**

When the Worker broadcasts a HOT decision for a key, the app-side `WorkerListener` loads the authoritative value via `CacheLoader.load(key)` before promoting the entry to L1 with extended TTL. The default implementation reads from Redis (`RedisCacheLoader`).

To use an alternative data source (database, remote service, etc.), implement the `CacheLoader` interface:

```java
@Bean
@ConditionalOnMissingBean(CacheLoader.class)
public CacheLoader dbCacheLoader(YourRepository repo) {
  return new CacheLoader() {
    @Override
    public Object load(String cacheKey) {
      // Parse entity type from key convention, e.g. "User:123"
      // → table "users", id=123; then query DB and return serialized value
    }
  };
}
```

`@ConditionalOnMissingBean` ensures your bean replaces the default `RedisCacheLoader`. Both `WorkerListener.handleHot()` and `CacheSyncListener.handleRefresh()` consume values through this interface — no other wiring changes needed.

**Observing Broadcast Decisions with Hooks**

Implement `WorkerDecisionHook` and/or `SyncHook` to observe broadcast outcomes without modifying core logic:

```java
@Bean
public WorkerDecisionHook myMonitorHook() {
  return new WorkerDecisionHook() {
    @Override
    public void afterHotPromotion(String cacheKey, WorkerMessage wm, CacheEntry entry) {
      metrics.counter("hot.promotion").increment();
    }

    @Override
    public void onHotSkipped(String cacheKey, WorkerMessage wm, HotSkipReason reason) {
      log.warn("HOT skipped for {}: {}", cacheKey, reason);
    }
  };
}
```

| Hook                 | Method               | When                                            |
| -------------------- | -------------------- | ----------------------------------------------- |
| `WorkerDecisionHook` | `afterHotPromotion`  | Key promoted to HOT with extended TTL           |
| `WorkerDecisionHook` | `afterCoolDowngrade` | Key reverted to normal TTL                      |
| `WorkerDecisionHook` | `onHotSkipped`       | HOT skipped (throttled / stale / no value)      |
| `WorkerDecisionHook` | `onCoolSkipped`      | COOL skipped (no entry in L1)                   |
| `SyncHook`           | `afterRefresh`       | Key refreshed from data store by peer broadcast |
| `SyncHook`           | `afterInvalidate`    | Key evicted from L1 by peer broadcast           |
| `SyncHook`           | `onRefreshSkipped`   | REFRESH skipped (version stale / value missing) |

All methods are `default` no-ops — implement only what you need. Exceptions from a hook are caught and logged individually; they never interrupt other hooks or the broadcast handler. Multiple hook beans are supported and run in iteration order.

**Replacing the Default Broadcast Processing Logic**

For full control over how HOT/COOL decisions or cache-sync messages are processed, implement the strategy interface and expose it as a `@Bean` with `@ConditionalOnMissingBean`:

```java
@Bean
@ConditionalOnMissingBean(WorkerDecisionHandler.class)
public WorkerDecisionHandler myHandler() {
  return new WorkerDecisionHandler() {
    @Override
    public void handleHot(WorkerMessage wm) {
      // Custom HOT logic — replaces Redis load, DCL, compute, etc.
    }

    @Override
    public void handleCool(WorkerMessage wm) {
      // Custom COOL logic — replaces TTL reset, key state change, etc.
    }
  };
}
```

| Interface               | Methods                                                                                 | Replaces                                                                                    |
| ----------------------- | --------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `WorkerDecisionHandler` | `handleHot`, `handleCool`                                                               | `WorkerListener`'s default HOT/COOL processing (Redis-backed, SRE-limited, version-guarded) |
| `SyncDecisionHandler`   | `handleRefresh`, `handleLocalInvalidate`, `handleLocalInvalidateAll`, `handleRulesSync` | `CacheSyncListener`'s default sync processing                                               |
| `Evaluator`             | `evaluate`, `evictStale` (default no-op)                                                | Worker's hot-key detection pipeline (fast-lane + Bayesian confidence state machine)         |

The default implementations (`DefaultWorkerDecisionHandler`, `DefaultSyncDecisionHandler`) still invoke any registered `WorkerDecisionHook` / `SyncHook` beans. Custom implementations may choose to invoke hooks or not.

Two approaches are available for `Evaluator` — **full replacement** or **wrapping** the default logic:

```java
// Approach 1: Full replacement — completely bypass the default evaluation pipeline.
// No sliding window, CV, EMA, or Bayesian gating.
@Bean
@ConditionalOnMissingBean(Evaluator.class)
public Evaluator myEvaluator() {
  return (key, count) -> {
    if (key.startsWith("priority:")) return ZetaDecision.hot(key, null);
    return ZetaDecision.none(key, null);
  };
}
```

```java
// Approach 2: Wrap the default — run the default evaluation, then add side effects.
@Bean
public Evaluator wrappedEvaluator(DefaultEvaluator delegate) {
  return new Evaluator() {
    @Override
    public ZetaDecision evaluate(String key, long count) {
      ZetaDecision decision = delegate.evaluate(key, count);
      metrics.counter("eval." + decision.type().name()).increment();
      return decision;
    }
    // evictStale is default no-op — not needed here
  };
}
```

Note: when wrapping, you must NOT use `@ConditionalOnMissingBean` (otherwise your wrapper prevents itself from being created). Inject the `DefaultEvaluator` as a dependency and measure/extend selectively.

## Spring Cache Integration

Enable `zeta.spring-cache.enabled=true`. Standard `@Cacheable` / `@CachePut` / `@CacheEvict` are automatically routed through Zeta's hotspot detection, soft expiration, and cross-instance sync.

**Requires `@EnableCaching`:** The `CacheExtensionAspect` wraps Spring's `CacheInterceptor` — without `@EnableCaching`, the interceptor is not registered and `@Cacheable` methods will execute without being cached.

```java
@SpringBootApplication
@EnableCaching
@EnableScheduling
public class MyApplication { ... }
```

**Extension Annotations** (processed by `CacheExtensionAspect` at `HIGHEST_PRECEDENCE`; full combination matrix in [docs/ANNOTATION.md](docs/ANNOTATION.md)):

| Annotation        | Target | Role on `@Cacheable`                                                                                                                                                                                                                                                                           |
| ----------------- | ------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `@CacheTTL`       | M/T    | Override hard/soft TTL. Static values and SpEL (`hardTtlSpEl`, `softTtlSpEl`). SpEL evaluated at most once per call, only on miss/promotion/refresh                                                                                                                                            |
| `@Intercept`      | M      | Skip method body via trigger mode (`IS_LOCAL_HOT`/`FORCE`/`QPS`/`CONCURRENT_THREADS`). Mode-specific config via nested `@Intercept.QpsConfig` (`threshold`, `blockDurationMs`) and `@Intercept.ConcurrentConfig` (`threshold`). Fallback via `@Intercept.fallback()`, `@Fallback`, or `peek()` |
| `@Fallback`       | M      | Fallback value (SpEL) or convention method (`{methodName}Fallback`) when blocked/intercepted/exception                                                                                                                                                                                         |
| `@NullCaching`    | M      | Opt-out of null caching — null results are cached by default (short-TTL sentinel); `@NullCaching(false)` disables it for the method                                                                                                                                                            |
| `@SkipBroadcast`  | M      | Suppress cross-instance AMQP sync messages (local-only write/evict)                                                                                                                                                                                                                            |
| `@Preload`        | M      | Pre-inflate HeavyKeeper counts for known hot keys (static `keys[]` or dynamic `keyExpr` SpEL)                                                                                                                                                                                                  |
| `@CacheCondition` | M      | SpEL `unless` with purge semantics — result not kept AND any existing entry evicted (broadcast unless `@SkipBroadcast`)                                                                                                                                                                        |
| `@Tag`            | M      | Feed a SpEL-resolved key into detection/reporting without a cache lookup; `skipDetection`/`skipReport` suppress each side; `cacheName` aligns the key namespace                                                                                                                                |

```java
@Cacheable(cacheNames = "users", key = "#id")
@CacheTTL(hardTtlMs = 60000, softTtlMs = 10000)
@Intercept @Fallback
public User getUser(Long id) { ... }

// Dynamic TTL from SpEL
@Cacheable(cacheNames = "users", key = "#id")
@CacheTTL(hardTtlSpEl = "#id.startsWith('vip') ? 600000 : 60000")
@Intercept
public User getUserVip(Long id) { ... }

// QPS rate-limit interception (nested @Intercept.QpsConfig)
@Cacheable(cacheNames = "products", key = "#id")
@Intercept(type = InterceptType.QPS, qps = @Intercept.QpsConfig(threshold = 500), fallback = "'throttled'")
@Fallback
public Product getProduct(String id) { ... }

// QPS rate-limit with block duration (5s cooling-off after breach)
@Cacheable(cacheNames = "flash-sale", key = "#id")
@Intercept(type = InterceptType.QPS, qps = @Intercept.QpsConfig(threshold = 100, blockDurationMs = 5000))
public Item getFlashItem(String id) { ... }

// Concurrent threads interception (nested @Intercept.ConcurrentConfig)
@Cacheable(cacheNames = "orders", key = "#id")
@Intercept(type = InterceptType.CONCURRENT_THREADS, concurrent = @Intercept.ConcurrentConfig(threshold = 10), fallback = "'busy'")
@Fallback
public Order getOrder(Long id) { ... }

// Hot key preloading
@Cacheable(cacheNames = "flash", key = "#id")
@Preload(keys = {"item-001", "item-002"})
@Intercept
public String getFlashItem(String id) { ... }

// Purge the entry when the result is disabled
@Cacheable(cacheNames = "products", key = "#id")
@CacheCondition(unless = "#result == null || #result.disable()")
public Product getProduct(String id) { ... }

// Explicitly refuse to cache null results for this method
@Cacheable(cacheNames = "maybe", key = "#id")
@NullCaching(false)
public Product findMaybe(String id) { ... }

// Tag a key into the same namespace without a cache lookup
@Tag(value = "#id", cacheName = "products", skipReport = true)
public void touchProduct(String id) { ... }

// Local-only write, no broadcast
@CachePut(cacheNames = "local", key = "#id")
@SkipBroadcast
public String updateLocal(String id, String val) { ... }
```

Requires `spring-boot-starter-cache` and `spring-boot-starter-aop` on the classpath.

## Cache Sync

Enable `zeta.sync.enabled=true`.

## Rule System

Enable `zeta.sync.enabled=true` to enable cross-instance rule synchronization. The rule system supports two actions:

| Action            | Effect on matching keys                                                             |
| ----------------- | ----------------------------------------------------------------------------------- |
| `BLOCK`           | `get()` / `getWithSoftExpire()` throw `ZetaBlockedException`; `putThrough()` skips  |
| `ALLOW_NO_REPORT` | Process normally but skip Worker reporting (reduces noise from high-frequency keys) |

### Pattern Types

`RuleMatcher.of(pattern, action)` auto-detects the pattern:

| Pattern             | Type       | Matches                      |
| ------------------- | ---------- | ---------------------------- |
| `"user:123"`        | `EXACT`    | Exact key                    |
| `"temp:*"`          | `PREFIX`   | Keys starting with `temp:`   |
| `"order:*-detail"`  | `WILDCARD` | Glob-style (`*` / `?`) match |
| `"regex:user:\\d+"` | `REGEX`    | Java regex                   |

### Persistence & Broadcast

- **With Redis:** Each `addRule()`/`removeRule()`/`clearRules()` serializes the rule list to `ZetaConstants.Redis.KEY_RULES` (`"zeta:rules"`). On startup, `RuleMatcher.initRules()` loads from Redis. Changes are also broadcast via `TYPE_RULES_SYNC` — peers call `RuleMatcher.syncRules()` for atomic replacement without triggering secondary broadcasts (loop-free).
- **Without Redis:** Same operations are broadcast to all peers via the `CacheSyncPublisher` fanout exchange. Each peer holds the full rule set in memory.
- **Manual broadcast:** `zeta.broadcastAllLocalRulesManually()` loads from Redis (if available) and re-broadcasts the current rule set to all peers.

## Monitoring

Zeta provides two complementary monitoring mechanisms.

See [MONITOR.md](docs/MONITOR.md) for the full response format and field descriptions.

## Design Details

See [CONTEXT.md](CONTEXT.md) for domain terminology.
Architecture Decision Records (ADRs) are maintained in [docs/adr/](docs/adr/0001-local-promotion-worker-fallback.md).

## License

Apache License 2.0
