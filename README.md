# HotKey

[![JitPack](https://jitpack.io/v/Hyshmily/HotKey.svg)](https://jitpack.io/#Hyshmily/HotKey) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0) [![Java](https://img.shields.io/badge/Java-25-orange)](https://openjdk.java.net/) [![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.5.3-brightgreen)](https://spring.io/projects/spring-boot)

[**中文版**](README.zh.md)

### Foreword: Why HotKey?

In real-world development, I frequently faced the challenge of managing a large number of cache keys. Manually maintaining Caffeine, Redis, and database multi-level caching, configuring logical expiration, and pre-computing and pre-warming hot keys — every step was tedious. Even more challenging, in a distributed cluster environment, ensuring hot keys are correctly shared across nodes and avoiding cache stampedes under high concurrency became a pain point that every developer must address.

My initial thought was simple: find a ready-to-use solution, or at least wrap a convenient multi-level cache utility class.

Unfortunately, existing solutions were either too complex (requiring high deployment and operational costs) or had been abandoned (such as JD's [hotkey](https://gitee.com/jd-platform-opensource/hotkey) project, which as of May 2026 is no longer maintained), making them unsuitable for lightweight, practical use. While many of its ideas aligned with my own, its heavy architecture convinced me to forge a different path.

That's the context in which HotKey was born: a lightweight, portable, ready-to-use hot key caching framework.

It will remain open source — past, present, and future.

### Introduction

HotKey — HeavyKeeper Top-K hot key detection + multi-level cache auto-warming + distributed cache sync & cluster-wide Worker mode Spring Boot Starter

HotKey is not a general-purpose local cache — it's a lightweight hot key auto-detection & multi-level cache warming framework with optional distributed sync and an optional dedicated Worker node for cluster-wide hot key consensus.

Most local cache solutions store every accessed key in Caffeine. This works fine with small data, but under millions of keys:

- **Memory waste** — most keys are read once and never accessed again
- **Broadcast storm** — full cache invalidation requires full broadcast at scale

HotKey takes a different approach — **cache only the hot keys.**

It uses [HeavyKeeper](https://github.com/go-kratos/aegis) (a Count-Min Sketch variant) to probabilistically detect access frequency. On the **read path**, only keys that enter the Top-K set are promoted into the local Caffeine L1, and optionally synchronized across instances via RabbitMQ. Hot keys and normal keys get **differentiated TTLs** — hot keys are cached longer (1h default), normal keys expire faster (5min). On the **write path**, `putThrough` writes directly to L1 and broadcasts regardless of hot key status — the caller explicitly owns the write. Non-hot key reads still return values via the caller-supplied `Supplier<T>` — they are simply not cached with long TTLs in L1.

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

> [!Important]
> Due to the author's limited capabilities, the reliability and stability in production cannot be fully guaranteed.

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
- **Worker Mode** — dedicated cluster-wide hot key detection node; sliding-window + state-machine pipeline for cross-instance consensus; see [README.WORKER.md](README.WORKER.md)
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
  │    └─ Throws → SingleFlight.load() catches → Optional.empty() → caller's fallback
  └─ HotKey itself fails → exception propagates to caller (no auto fallback)
```

Component failure behavior:

| Failed component           | Impact                                                     | Recovery                            |
| -------------------------- | ---------------------------------------------------------- | ----------------------------------- |
| HotKey itself              | L1 unavailable; exception propagates to caller             | Restart app                         |
| L2 backend (Redis/DB/API)  | Every request hits caller's fallback                       | Auto-recover on backend restoration |
| L1 Caffeine OOM / eviction | Individual keys evicted, next read re-fetches via supplier | Automatic (Caffeine internal)       |

> The caller is always responsible for handling `Optional.empty()` — HotKey never hides backend failures.

Write path failure behavior:

| Write method                                        | Failure scenario                 | Behavior                                                                     |
| --------------------------------------------------- | -------------------------------- | ---------------------------------------------------------------------------- | ------------------------------------------------------ |
| `putThrough`                                        | Executor queue full (outside tx) | `RejectedExecutionException` propagates to caller                            |
| `putThrough`                                        | `writer.run()` / Redis fails     | Error logged on `hotKeyExecutor`, L1 version not updated, no broadcast       |
| `putBeforeInvalidate`                               | `mutation.run()` throws          | Mutation exception caught and logged; local invalidate and broadcast skipped |
| `invalidate` / `putBeforeInvalidate` / `putThrough` | `nextVersion()` Redis fails      | Falls back to node-local counter (nodeId << 32 &#124; counter, non-persistent version with `degraded=true`) |

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
    <version>1.1.0</version>
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

| Feature | Enable | Description |
|---------|--------|-------------|
| Redis L2 | Add `RedisTemplate` bean | Two-level cache with L2 fallback |
| Cross-instance sync | `hotkey.sync.enabled=true` | RabbitMQ-based cache invalidation |
| Worker Listener | `hotkey.worker-listener.enabled=true` | Receive HOT/COOL decisions from Worker |
| Worker Mode | `hotkey.worker.enabled=true` | Run as dedicated Worker node |
| `@HotKey` Annotation | `hotkey.annotation.enabled=true` + AspectJ | Declarative caching |
| Reporting | `hotkey.report.enabled=true` (default) | Report access counts to Worker |

See [Configuration](#configuration) for all options and [README.CONFIG.md](README.CONFIG.md) for the complete property reference.

### 3. Use

> **Note:** From v1.0.2 includes a **breaking change** — `get(hk, fk)` and `putAndBroadcast(hk, fk, val)` are removed. The library is now decoupled from `RedisTemplate`; callers supply their own read/write callbacks via `Supplier<T>` / `Runnable`.

**A. Pure local cache (no L2)**

```java
@Autowired
private HotKey hotKey;

Optional<String> r = hotKey.peek("user:123"); // Caffeine L1 peek only (no hot key tracking)
```

Calls `peek(cacheKey)` — returns `Optional.empty()` if L1 miss, skips secondary storage entirely.

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
```

**D. Helper bean to avoid repetitive lambdas**

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

**E. Custom L2 (non-Redis)**

```java
// Use MySQL, remote API, or any data source as L2
Optional<User> r = hotKey.get("user:123", () -> userMapper.selectById(123));

User user = r.orElseGet(() -> createDefaultUser());
```

**F. Redis collections (List, Set, ZSet)**

`putThrough` requires the full new value for L1 update, but collection incremental operations (LPUSH, SADD, ZADD) modify only a single element — the caller cannot know the full collection state. Use `putBeforeInvalidate` to invalidate L1 after the mutation; the next `get()` re-fetches from Redis, ensuring consistency.

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

**G. Soft expire — replaces traditional logical expiration**

Soft expire returns the stale L1 value immediately while asynchronously refreshing (stale-while-revalidate). Unlike traditional logical expiration (which embeds `expireTime` in Redis values), HotKey manages staleness purely at the L1 Caffeine level — **Redis stores raw values, no wrappers needed**.

| Aspect         | Traditional Logical Expiration                          | HotKey Soft Expire                                         |
| -------------- | ------------------------------------------------------- | ---------------------------------------------------------- |
| Expiry storage | Embedded in Redis value (`RedisData{data, expireTime}`) | L1 Caffeine metadata (`softExpireAt`)                      |
| Stale delivery | Returns old data                                        | Returns old L1 value                                       |
| Async rebuild  | Redis distributed lock + custom thread pool             | Singleflight (local) + `hotKeyExecutor` + `refreshLimiter` |
| Redis format   | Wrapped JSON                                            | Raw value (clean, no wrapper)                              |
| DB fallback    | Manual locking logic                                    | Native `orElseGet` / `orElseThrow`                         |

```java
// Traditional approach (no longer needed):
//   redisData.setExpireTime(LocalDateTime.now().plusSeconds(30L));
//   stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));

// HotKey: Redis stores raw value, soft expire managed at L1
Optional<String> r = hotKey.getWithSoftExpire("user:123", () -> redisTemplate.opsForValue().get("user:123"));

// L1 hit + soft expired → returns stale value + triggers async refresh
// L1 miss → singleflight load (same as get())

// Per-call custom soft TTL (overrides global default)
Optional<String> r2 = hotKey.getWithSoftExpire("user:456", () -> redisTemplate.opsForValue().get("user:456"), 3000);
```

Database fallback (no distributed lock required):

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

See [Configuration](#configuration) for TTL property options.

**H. Per-entry hard TTL**

By default, hot keys and normal keys get different TTLs. Use `get(key, reader, hardTtlMs, softTtlMs)` or `putThrough(key, value, writer, hardTtlMs, softTtlMs)` to override both hard and soft TTL for an individual entry.

```java
// 5-minute hard TTL, 30-second soft TTL for this key
Optional<Shop> shop = hotKey.get("shop:" + shopId,
    () -> redisTemplate.opsForValue().get("shop:" + shopId),
    TimeUnit.MINUTES.toMillis(5),   // hardTtlMs
    TimeUnit.SECONDS.toMillis(30)); // softTtlMs

// 30-second hard TTL, no soft TTL via putThrough
hotKey.putThrough("weather:" + city, weatherData,
    () -> redisTemplate.opsForValue().set("weather:" + city, weatherData),
    TimeUnit.SECONDS.toMillis(30),  // hardTtlMs
    0);                              // softTtlMs (use default)
```

> **Notes on per-call TTL semantics:**
>
> - A per-call `hardTtlMs`/`softTtlMs` applies to this invocation only. The next call without these parameters falls back to the configured defaults (which differ for hot vs normal keys).
> - Pass `0` for either TTL to use the configured default for the key's current state (hot vs normal).
> - When combined with `getWithSoftExpire`, the per-entry hard TTL is preserved across background refreshes.

**I. Worker mode**

For cluster-wide hot key detection across multiple instances, see [README.WORKER.md](README.WORKER.md).

**J. @HotKey annotation**

The `@HotKey` annotation provides declarative caching on method return values — an AOP-based alternative to explicit `hotKey.get()` / `putBeforeInvalidate()` / `invalidate()` calls.

```java
@HotKey(key = "'user:' + #id", hardTtlMs = 5000)
public User getUser(Long id) { ... }
```

**Annotation attributes:**

| Attribute  | Type          | Default  | Description |
|-----------|---------------|----------|-------------|
| `key`      | `String`      | required | SpEL expression for cache key. Method parameters available as `#paramName`. Requires `-parameters` compiler flag; falls back to `arg0, arg1, ...`. |
| `operation`| `OperationType`| `READ`   | `READ` / `WRITE` / `INVALIDATE` |
| `hardTtlMs`| `long`        | `0`      | Hard TTL override in ms. `0` = use configured default. |
| `softTtlMs`| `long`        | `0`      | Soft TTL override in ms. `0` = use configured default. |
| `softExpire`| `boolean`    | `true`   | Use stale-while-revalidate on READ. When `false`, behaves as plain `get()`. |

**Three operation modes:**

| Mode | Facade method | Behavior |
|------|--------------|----------|
| `READ` (default) | `getWithSoftExpire()` or `get()` | Method body = value supplier. `softExpire=true` → `getWithSoftExpire()` (stale-while-revalidate); `false` → plain `get()`. If method returns `Optional`, passed through; otherwise unwrapped via `orElse(null)`. |
| `WRITE` | `putBeforeInvalidate()` | Method executed as mutation: runs, version incremented, L1 invalidated, INVALIDATE broadcast sent. Exceptions captured and rethrown after facade call. |
| `INVALIDATE` | `invalidate()` | Invalidates key from L1 + increments version + broadcasts TYPE_REFRESH (versioned) to peers, then proceeds with method. |

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

Requires `spring-boot-starter-aop` on the classpath (provides AspectJ). Requires `-parameters` compiler flag for SpEL parameter name resolution (enabled by default in the parent POM).

> **Note:** `@HotKey(operation=READ)` wraps the original method call as the L2 supplier — the method body serves as both cache reader and database fallback. This is convenient for read-heavy endpoints but bypasses `peek()` semantics. When `softExpire=true` (default), stale-while-revalidate is enabled: expired values return immediately while a background thread refreshes. When `softExpire=false`, cache misses always invoke the method synchronously.

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
| `isHotKey(cacheKey)`                                   | Check if a key is in HOT state in L1 (O(1))                                                                                                                             |
| `invalidate(cacheKey)`                                 | Invalidate a single key from all cache layers                                                                                                                           |
| `invalidateAll(cacheKeys...)`                          | Varargs overload — invalidate multiple keys at once                                                                                                                     |
| `invalidateAll(Collection)`                            | Collection overload                                                                                                                                                     |
| `returnHotKeys()`                                      | Snapshot of current app-side Top-K entries (key + count)                                                                                                                |
| `returnExpelledHotKeys()`                              | Access the app-side expelled hot key queue; drained periodically by internal scheduler                                                                                  |
| `returnTotalDataStreams()`                             | Total number of reads passed through app-side HeavyKeeper                                                                                                               |
| `returnWorkerHotKeys()`                                | Snapshot of current Worker-side (cluster-wide) Top-K entries                                                                                                            |
| `returnWorkerExpelledHotKeys()`                        | Access the Worker-side expelled hot key queue                                                                                                                           |
| `returnWorkerTotalDataStreams()`                       | Total number of reads tracked by Worker-side HeavyKeeper                                                                                                                |

> **Note:** `invalidate()` generates a monotonic version via Redis `INCR` and broadcasts as `TYPE_REFRESH` with that version — peers reload the value from Redis via `CacheSyncListener`, skipping stale versions. `invalidateAll()` does **not** call `INCR` — it broadcasts as `TYPE_INVALIDATE` with version `0L`, so all peers unconditionally remove the key from L1.

## Configuration

Minimal setup shown in [Quick Start](#2-configure). For the complete property list, see [README.CONFIG.md](README.CONFIG.md). For Worker mode, see [README.WORKER.md](README.WORKER.md).

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

Enable `hotkey.worker.enabled=true`. Two deployment modes — see [Worker Mode](#worker-mode) and [README.WORKER.md](README.WORKER.md).

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

Each instance declares its own queue (`hotkey.sync:<instance-id>`) bound to a fanout exchange. Two message types:

- **`TYPE_REFRESH`** — Versioned invalidation. Peers reload the value from Redis via `CacheSyncListener.handleRefresh()`, respecting the {@code dataVersion} header to skip stale updates. The 4-case comparison (normal-vs-normal, normal-vs-degraded, degraded-vs-normal, degraded-vs-degraded) guarantees that a normal (Redis INCR) dataVersion always wins over a degraded (node-local) one.
- **`TYPE_INVALIDATE`** — Bulk invalidation (`invalidateAll`). Peers immediately remove the key from L1 without reloading.

### Worker Listener

For receiving HOT/COOL decisions from a dedicated Worker node, enable `hotkey.worker-listener.enabled=true`.

See [README.WORKER.md](README.WORKER.md) for detailed Worker mode setup.

## Worker Mode

Worker Mode provides cluster-wide hot key detection via a dedicated node. App instances periodically report access counts, the Worker runs a sliding-window + state-machine pipeline, and broadcasts HOT/COOL decisions back to all instances.

Two deployment modes:

| Mode                     | `worker.enabled` | Active Beans                                                                          |
| ------------------------ | ---------------- | ------------------------------------------------------------------------------------- |
| App-only                 | `false` (default)| `HotKeyCache`, TopK, reporter, actuator, sync                                         |
| Worker-only              | `true`           | Worker only (no cache — `get()`/`putThrough()` throw `UnsupportedOperationException`) |

In **Worker-only** mode, cache operations throw `UnsupportedOperationException`.

For full documentation on Worker setup, state machine, sliding window, and configuration, see [README.WORKER.md](README.WORKER.md).

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

See [README.ARCH.md](README.ARCH.md) for detailed read/write path diagrams (also available in Chinese: [README.zh.ARCH.md](README.zh.ARCH.md)).

## License

Apache License 2.0
