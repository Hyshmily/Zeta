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

### Local-Distributed Collaborative Detection

Zeta provides two-tier hot-key detection — a local in-process HeavyKeeper probabilistic sketch and a remote Worker cluster — and automatically warms up the L1 cache based on the detection results.

- Each application instance runs a local TopK sketch that tracks frequently accessed keys. When a key enters the local TopK set, its L1 Caffeine cache TTL is automatically extended — no Worker feedback required. On L1 miss, the SingleFlight mechanism merges concurrent requests for the same key to prevent cache breakdown. Soft expiration is also supported — when the soft TTL expires but the hard TTL has not, stale entries are served immediately while a background async refresh is triggered, ensuring response latency.
- The reporting path is protected by a BBR congestion control algorithm that automatically throttles based on CPU load, preventing burst traffic from overwhelming the channel. The Worker cluster aggregates access reports from all application instances, runs sliding-window frequency analysis combined with a Bayesian confidence state machine, and broadcasts HOT/COOL decisions back to every instance. Each cache entry (CacheEntry) carries two orthogonal version numbers — dataVersion and decisionVersion — and a KeyState marking its lifecycle state (HOT/COOL/NORMAL). Worker decisions override local promotions via a monotonically increasing decisionVersion, ensuring cluster-wide consistency.

### Multi-Node Cache Coherency

Much like how a primary-backup database synchronizes writes through a log replication protocol, Zeta synchronizes cache mutations across application instances through a publish-subscribe mechanism backed by RabbitMQ.

When any instance performs a write-through (putThrough) or invalidation, it increments a per-key dataVersion (via Redis INCR, with a local fallback for degraded mode) and broadcasts the event to all peers. Each peer compares the incoming dataVersion against its local version — stale messages are silently dropped. When all Workers are unreachable, the local TopK assumes full authority over promotions and the reporter drops silently; when Workers recover, they automatically reclaim control via a higher version number — no manual intervention required.

This version comparison mechanism ensures eventual coherency without the overhead of distributed consensus protocols like Paxos or Raft.

End-to-end latency under default settings: ~300ms (P99).

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

**Cluster health threshold** — when `expected-worker-count: 0` (dynamic mode, default), `min-alive-workers: 0` means **1 alive Worker is healthy**. When `expected-worker-count: N` (fixed mode), uses majority formula `N/2 + 1`. Setting `min-alive-workers` overrides either mode. See `docs/CONFIG.md` for details.

**All parameters:**
See [CONFIG.md](docs/CONFIG.md)

</details>

#### Local Mode (App Side) — Maven Dependency

**Maven Central** (no extra repository needed):

```xml
<dependency>
  <groupId>io.github.hyshmily</groupId>
  <artifactId>zeta</artifactId>
  <version>1.1.55</version>
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
    <version>1.1.55</version>
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
  <version>1.1.55</version>
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
  ghcr.io/hyshmily/zeta-worker:1.1.55
```

**Run JAR directly** (no Docker):

```bash
mvn clean package -pl worker
java -jar worker/target/zeta-worker-1.1.55.jar
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
| Worker TopK Persist      | `zeta.worker.persistence.enabled=true`       | Warm start from Redis after restart                                  |
| Access Reporting         | `zeta.report.enabled=true` (default)         | Report access counts to Worker                                       |
| Reporter Self-Protection | `zeta.local.reporter.enabled=true` (default) | BBR backpressure for Reporter flush                                  |
| Spring Cache Integration | `zeta.spring-cache.enabled=true`             | `@Cacheable` / `@CachePut` / `@CacheEvict` fused with Zeta detection |

See [CONFIG.md](docs/CONFIG.md) for the full property reference.

### 3. Usage

See [CONFIG.md](docs/CONFIG.md) for the full property reference.

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

**Write Operations**

```java
// F. putThrough — write-through + broadcast
zeta.putThrough("user:123", newValue, () -> redisTemplate.opsForValue().set("user:123", newValue));

// G. invalidateAfterPut — mutate then invalidate (collection types)
zeta.invalidateAfterPut(key, () -> redisTemplate.opsForSet().add(key, members));

// H. putLocal — local write only, no broadcast, no version bump
zeta.putLocal("user:123", cachedValue, hardTtlMs, softTtlMs); // custom TTL

// I. refresh — local evict then load and cache
zeta.refresh("user:123", () -> loadUser(123), hardTtlMs, softTtlMs); // with TTL override

// J. Fluent write API
zeta.write("user:42").withHardTtl(30_000).putThrough(newValue, dbWriter);
zeta.write("user:42").putBeforeInvalidate(dbMutation);
zeta.write("user:42").invalidate();
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

// registerRefresh / updateRefresh — scheduled background refresh (softTtlMs = interval)
  zeta.registerRefresh("user:123", () -> loadUser(123), 300_000L, 60_000L);  // every 60s
  zeta.updateRefresh("user:123", () -> loadUser(123), 300_000L, 30_000L);    // change to 30s
  zeta.unregisterRefresh("user:123");                                         // stop
```

> [!NOTE]
> **Cache avalanche protection:** `ExpireManager` applies a uniform random offset via `DelayUtil.computeTtlJitter()` to every expiration timestamp (default ±5%). A 5-minute hard TTL actually expires between 4.75 ~ 5.25 minutes under the default offset. Controlled by `zeta.local.ttl-jitter-ratio` (ratio, default `0.05` = ±5%, `0` to disable).

> [!TIP]
> Per-call TTL semantics: passing `0` uses the configured default for that key state. For pure logical expiration (hard TTL never evicts, soft expire only): pass `hardTtlMs = Long.MAX_VALUE` to `getWithSoftExpire(key, reader, Long.MAX_VALUE, softTtlMs)` — the entry permanently resides in Caffeine. This usage is explicitly supported by Caffeine's `Expiry` JavaDoc: _"To indicate no expiration an entry may be given an excessively long period, such as `Long.MAX_VALUE`."_ ([source](https://github.com/ben-manes/caffeine/blob/master/caffeine/src/main/java/com/github/benmanes/caffeine/cache/Expiry.java))

**Atomic Operations**

CAS-style operations for lock-free conditional updates:

```java
// compareAndSet — atomic swap if current value matches expected
boolean ok = zeta.compareAndSet("user:123", oldValue, newValue);

// compareAndInvalidate — invalidate only if current value matches expected
boolean ok = zeta.compareAndInvalidate("user:123", staleValue);
```

Both operations are delegation-based: the caller is responsible for re-reading or re-writing after a successful CAS. There is no L2 lock — the guard is the L1 cache entry's current value at the time of call. Returns `true` if the condition matched and the operation was applied; `false` otherwise.

**Worker Mode**

Worker mode provides cluster-wide hotspot detection via dedicated nodes. App instances periodically report access counts; the Worker runs a sliding window + state machine pipeline and broadcasts HOT/COOL decisions back to all instances. State machine parameters (`confirmCount`, `coolCount`, `preCoolGraceCount`) can be adjusted at runtime via `/actuator/hotkey/worker/state`.

| Mode        | `worker.enabled`  | Activated Beans                                                           |
| ----------- | ----------------- | ------------------------------------------------------------------------- |
| App-only    | `false` (default) | `HotKeyCache`, TopK, reporter, actuator, sync                             |
| Worker-only | `true`            | Worker only (no cache — `get()`/`putThrough()` throw `ZetaModeException`) |

**Worker Cluster Health:** Set `zeta.local.expected-worker-count` to the expected number of Workers in production. When set >0, `ClusterHealthView` uses majority quorum (`> expectedWorkerCount / 2`) as the healthy Worker threshold; when 0 (default), the cluster is considered unhealthy until at least one heartbeat is received. This enables precise detection of partial Worker failures and graceful degradation decisions.

**Worker TopK Persistence (Warm Start):** When `zeta.worker.persistence.enabled=true`, the Worker periodically snapshots the TopK list to Redis. On restart, `TopKPersistService` loads the last snapshot and replays it into the HeavyKeeper sketch, reducing warmup from hours to seconds.

**Spring Cache Integration**

Enable `zeta.spring-cache.enabled=true`. Standard `@Cacheable` / `@CachePut` / `@CacheEvict` are automatically routed through Zeta's hotspot detection, soft expiration, and cross-instance sync.

**Extension Annotations** (processed by `CacheExtensionAspect` at `HIGHEST_PRECEDENCE`):

| Annotation        | Target | Role on `@Cacheable`                                                                                                                                  |
| ----------------- | ------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `@CacheTTL`       | M/T    | Override hard/soft TTL. Supports static values and SpEL (`hardTtlSpEl`, `softTtlSpEl`). SpEL evaluated per invocation.                                |
| `@Intercept`      | M      | Skip method body via trigger mode (`IS_LOCAL_HOT`/`FORCE`/`QPS`/`CONCURRENT_THREADS`); fallback via `@Intercept.fallback()`, `@Fallback`, or `peek()` |
| `@Fallback`       | M      | Fallback value (SpEL) or convention method (`{methodName}Fallback`) when blocked/intercepted/exception                                                |
| `@NullCaching`    | M      | Allow caching null return values (sentinel-based, default `true`)                                                                                     |
| `@SkipBroadcast`  | M      | Suppress cross-instance AMQP sync messages (local-only write)                                                                                         |
| `@SkipDetection`  | M      | Bypass TopK detection + Worker reporting for this method's keys                                                                                       |
| `@Preload`        | M      | Pre-inflate HeavyKeeper counts for known hot keys (static `keys[]` or dynamic `keyExpr` SpEL)                                                         |
| `@CacheCondition` | M      | SpEL `unless` — skip caching result when expression evaluates true (uses `#result` + method params)                                                   |

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

// QPS rate-limit interception
@Cacheable(cacheNames = "products", key = "#id")
@Intercept(type = InterceptType.QPS, qps = 500, fallback = "'throttled'")
@Fallback
public Product getProduct(String id) { ... }

// Concurrent threads interception
@Cacheable(cacheNames = "orders", key = "#id")
@Intercept(type = InterceptType.CONCURRENT_THREADS, concurrentThreads = 10, fallback = "'busy'")
@Fallback
public Order getOrder(Long id) { ... }

// Hot key preloading
@Cacheable(cacheNames = "flash", key = "#id")
@Preload(keys = {"item-001", "item-002"})
@Intercept
public String getFlashItem(String id) { ... }

// Skip caching null results conditionally
@Cacheable(cacheNames = "products", key = "#id")
@CacheCondition(unless = "#result == null || #result.disable()")
public Product getProduct(String id) { ... }

// Skip detection entirely (static config, no hot-key tracking needed)
@Cacheable(cacheNames = "config", key = "#key")
@SkipDetection
public String getConfig(String key) { ... }

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
