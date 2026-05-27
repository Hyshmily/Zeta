# HotKey

[![JitPack](https://jitpack.io/v/Hyshmily/HotKey.svg)](https://jitpack.io/#Hyshmily/HotKey) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0) [![Java](https://img.shields.io/badge/Java-25-orange)](https://openjdk.java.net/) [![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.5.3-brightgreen)](https://spring.io/projects/spring-boot)

[**中文版**](README.zh.md)

HotKey — HeavyKeeper Top-K hot key detection + multi-level cache auto-warming + distributed broadcast Spring Boot Starter

HotKey is not a general-purpose local cache — it's a lightweight hot key auto-detection & multi-level cache warming framework with optional distributed broadcast.

Most local cache solutions store every accessed key in Caffeine. This works fine with small data, but under millions of keys:

- **Memory waste** — most keys are read once and never accessed again
- **Broadcast storm** — full cache invalidation requires full broadcast at scale

HotKey takes a different approach — **cache only the hot keys.**

It uses [HeavyKeeper](https://github.com/go-kratos/aegis) (a Count-Min Sketch variant) to probabilistically detect access frequency. Only keys that enter the Top-K set are promoted into the local Caffeine L1, and optionally synchronized across instances via RabbitMQ fanout. Non-hot key reads are delegated back to the caller via `Supplier<T>` — the framework makes no assumption about your data source.

### When to use

| Suitable | Not suitable |
|---|---|
| Read-heavy workloads (String / List / Set / ZSet) | Write-heavy / atomic operations (seckill, Lua) |
| Large key space with Pareto distribution | Small key space (< 200), manual Caffeine is fine |
| Read-many-write-few, eventually consistent | Strong read-after-write consistency required |
| Spring Boot 3.x + Java 17+ | Non-Spring-Boot projects |
| Optional Redis (default L2) + optional RabbitMQ (multi-instance) |

> [!Important]
> This is an experience module summarized by the author during development. Reliability and stability in production cannot be guaranteed. For a complete production-ready hot key auto-detection and higher-precision version, please refer to [hotkey](https://gitee.com/jd-platform-opensource/hotkey)

> See [CHANGELOG.md](CHANGELOG.md) for version history.

## Features

- **HeavyKeeper Algorithm** — probabilistic top-k detection with Count-Min Sketch + exponential conflict decay
- **Three-Level Cache** — Caffeine (L1) → Redis (L2, optional) → DB fallback, with automatic hot-key promotion
- **In-Flight Dedup** — concurrent L1 miss requests share a single Redis read via `Caffeine<key, CompletableFuture>`

  > **Note:** Ensure `hotkey.inflight-ttl-seconds` exceeds the slowest Redis response time for your workload, or the cache entry may expire before the future completes, causing duplicate Redis reads.
  > Also ensure `hotkey.inflight-timeout-seconds` < `hotkey.inflight-ttl-seconds`. On timeout, `loadSingleflight` returns `Optional.empty()` — the caller should handle via DB fallback.
- **Soft Expire** — return stale L1 value immediately while asynchronously refreshing in the background; lower p99 at the cost of short-lived staleness
- **Redis Collections** — `putBeforeInvalidate` for List/Set/ZSet incremental writes; no `putThrough` needed
- **Hot Key Broadcast** — optional RabbitMQ fanout to synchronize hot keys across instances
- **Configurable Thread Pool** — dedicated `TaskExecutor` with bounded queue
- **Spring Boot Auto-Configuration** — drop-in dependency, zero boilerplate


## Architecture

```
┌──────────────┐   L1 hit + add(key,1) ┌──────────────┐
│   Request    │ ────────────────────→ │  Caffeine L1 │
│              │ ←──────────────────── │  (local)     │
└──────┬───────┘   Optional.of(value)  └──────┬───────┘
       │ L1 miss           (auto unwrap       │ isHotKey()?
       ↓ (inflight dedup)  VersionedValue)    ↓
┌──────────────┐   redisReader     ┌───────────────┐
│  L2 Storage  │ ←───────────────  │     TopK      │
│  (pluggable) │ ───────────────→  │  (interface)  │
└──┬───────┬───┘  add(key,1)       ├───────────────┤
   │ hit   │ null                  │ add()→Result  │
   ↓       ↓                       │ list()        │
Optional   Optional.empty()        │ total()       │
.of(value)   r.isEmpty() → DB      │ expelled()    │
                                   │ fading()      │
                                   └───────┬───────┘
                                           │ isHotKey()
                                           ↓
                              Caffeine.put(key,
                                VersionedValue(value, version=0L))
                              + broadcastHotKey with version header
```

Write path (user-initiated):
`putThrough(cacheKey, value, writer)`
├─ `writer.run()` — L2 write (caller-supplied Runnable)
├─ `nextVersion(cacheKey)` — Redis INCR → monotonic version
├─ Caffeine.put(cacheKey, VersionedValue(value, version))
└─ RabbitMQ fanout with version header (if enabled)

For incremental collection mutations (LPUSH, SADD, ZADD):
`putBeforeInvalidate(cacheKey, mutation)`
├─ `mutation.run()` — L2 write (caller-supplied Runnable)
├─ `nextVersion(cacheKey)` — Redis INCR → monotonic version
├─ Caffeine local cache **invalidate**
└─ RabbitMQ fanout with version header (if enabled)

> **Note:** Between `mutation.run()` and `caffeineCache.invalidate()` there is a ~1ms window where a concurrent `get()` may hit the L1 stale value. This is a deliberate trade-off — invalidating before the mutation would cause a worse race where `get()` re-populates L1 with old Redis data. The window is bounded to a single Redis round-trip (`nextVersion` call).

Soft Expire Read Path (`getWithSoftExpire`):

```
         ┌──────────────┐   L1 hit ┌───────────────┐
         │   Request    │ ───────→ │ softExpireAt  │
         │              │ ←─────── │  time check   │
         └──────┬───────┘  stale   └───────┬───────┘
                │ soft expired?            │ expired?
                ↓ true                     ↓ yes
           Return stale          triggerAsyncRefresh
           value +                     ├─ refreshLimiter.tryAcquire()
           check TopK                  └─ Async L2 → Caffeine.put
                │                            + update softExpireAt
                 │ L1 miss (falls through to normal path)
                 ↓
            loadSingleflight(cacheKey, redisReader)
            (see Normal Read Path above)
            Caffeine.put(key, VersionedValue(value, 0L))
```

## Degradation

HotKey forms a three-level degradation chain through the `supplier` callback:

```
hotKey.get(key, supplier)
  ├─ L1(Caffeine) HIT → return directly
  ├─ L1 MISS → supplier()
  │    ├─ Returns data → hot key? → write L1 + return
  │    ├─ Returns null → Optional.empty() → caller's orElseGet/orElseThrow
  │    └─ Throws → loadSingleflight catches → Optional.empty() → caller's fallback
  └─ HotKey itself throws → skip L1, supplier called directly
```

Component failure behavior:

| Failed component | Impact | Recovery |
|-----------------|--------|----------|
| HotKey itself | Skips L1, falls through to supplier directly | Restart app |
| L2 backend (Redis/DB/API) | Every request hits caller's fallback | Auto-recover on backend restoration |
| L1 Caffeine OOM / eviction | Individual keys evicted, next read re-fetches via supplier | Automatic (Caffeine internal) |

> The caller is always responsible for handling `Optional.empty()` — HotKey never hides backend failures.

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
    <groupId>com.github.hyshmily</groupId>
    <artifactId>hotkey</artifactId>
    <version>1.0.7</version>
</dependency>
```

Use a Git tag as the version (e.g. `1.0.7`). Redis and RabbitMQ dependencies are optional — include them only if you need the corresponding features.

### 2. Configure

```yaml
hotkey:
  top-k: 100
  width: 100000
  depth: 5
  decay: 0.92
  min-count: 10
  local-cache-max-size: 1000
  local-cache-ttl-minutes: 5
  broadcast:
    enabled: false
    exchange-name: hotkey.broadcast.exchange
    queue-prefix: hotkey.broadcast
    instance-id: ${server.port}
```

### 3. Use

> **Note:** From v1.0.2 includes a **breaking change** — `get(hk, fk)` and `putAndBroadcast(hk, fk, val)` are removed. The library is now decoupled from `RedisTemplate`; callers supply their own read/write callbacks via `Supplier<T>` / `Runnable`.

**A. Pure local cache (no L2)**

```java
@Autowired
private HotKey hotKey;

Optional<String> r = hotKey.peek("user:123"); // Caffeine L1 + hot key detection only
```

Calls `peek(cacheKey)` — returns `Optional.empty()` if L1 miss, skips secondary storage entirely.

**B. Two-level cache (Redis or any backend)**

```java
@Autowired
private HotKey hotKey;
@Autowired
private RedisTemplate<String, Object> redisTemplate;

Optional<String> r = hotKey.get("user:123", () -> redisTemplate.opsForValue().get("user:123"));

hotKey.putThrough("user:123", newValue, () -> redisTemplate.opsForValue().set("user:123", newValue));
```

**C. Database fallback**

```java
Optional<String> r = hotKey.get("user:123", () -> redisTemplate.opsForValue().get("user:123"));
if (r.isEmpty()) {
    String value = userService.getById(123);   // DB fallback
    redisTemplate.opsForValue().set("user:123", value);
}
```

**D. Helper bean to avoid repetitive lambdas**

```java
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
```

**E. Custom L2 (non-Redis)**

```java
// Use MySQL, remote API, or any data source as L2
Optional<User> r = hotKey.get("user:123", () -> userMapper.selectById(123));
User user = r.orElseGet(() -> createDefaultUser());
```

**F. Redis collections (List, Set, ZSet)**

`putThrough` requires the full new value for L1 update, but collection incremental operations (LPUSH, SADD, ZADD) modify only a single element — the caller cannot know the full collection state. Use `putInvalidate` to invalidate L1 after the mutation; the next `get()` re-fetches from Redis, ensuring consistency.

```java
@Component
public class CollectionHotKeyCache {
    @Autowired private HotKey hotKey;
    @Autowired private RedisTemplate<String, Object> redisTemplate;

    public Boolean sIsMember(String key, Object member) {
        return hotKey.get(key + "::member::" + member,
            () -> redisTemplate.opsForSet().isMember(key, member));
    }

    @SuppressWarnings("unchecked")
    public Set<Object> sMembers(String key) {
        return hotKey.get(key,
            () -> redisTemplate.opsForSet().members(key));
    }

    public void sAdd(String key, Object... members) {
        hotKey.putBeforeInvalidate(key,
            () -> redisTemplate.opsForSet().add(key, members));
    }

    public List<Object> lRange(String key, long start, long end) {
        String cacheKey = key + "::range::" + start + "::" + end;
        return hotKey.get(cacheKey,
            () -> redisTemplate.opsForList().range(key, start, end));
    }

    public Double zScore(String key, Object member) {
        return hotKey.get(key + "::score::" + member,
            () -> redisTemplate.opsForZSet().score(key, member));
    }
}
```

**G. Soft expire + singleflight re-source**

Soft expire returns the stale L1 value immediately while asynchronously refreshing in the background. Use when short-lived staleness is acceptable in exchange for lower p99 latency.

```java
Optional<String> r = hotKey.getWithSoftExpire("user:123",
    () -> redisTemplate.opsForValue().get("user:123"));
// L1 hit + soft expired → returns stale value + triggers async refresh
// L1 miss → singleflight load (same as get())
```

Configuration:
```yaml
hotkey:
  soft-ttl-ms: 5000               # enable soft expire with 5s soft TTL
  refresh-concurrency: 50         # limit concurrent async refreshes
```

## HotKey API Reference

The recommended entry point is the `HotKey` facade (auto-configured as a Spring bean). Beyond the `get`/`peek`/`putThrough`/`putInvalidate` shown above, it exposes:

| Method | Description |
|--------|-------------|
| `isHotKey(cacheKey)` | Check if a key is in the current Top-K hot set |
| `invalidateAll(cacheKeys...)` | Varargs overload — invalidate multiple keys at once |
| `invalidateAll(Collection)` | Collection overload |
| `returnHotKeys()` | Snapshot of current Top-K entries (key + count) |
| `returnExpelledHotKeys()` | Drain expelled hot key queue (recently evicted from Top-K) |
| `returnTotalDataStreams()` | Total number of reads passed through HeavyKeeper |

## Broadcast

```yaml
hotkey:
  broadcast:
    enabled: true
```

Each instance declares its own queue (`hotkey.broadcast:<pod-id>`) bound to a fanout exchange. When a hot key is promoted, the instance broadcasts the key. Peers load the value from Redis on first broadcast miss. Invalidations remove the local cache entry immediately.

## Monitoring

When `spring-boot-starter-actuator` is on the classpath, the HotKey endpoint is automatically registered at `/actuator/hotkey`.

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,hotkey
```

```json
{
  "topK": [{"key": "cache:shop:17", "count": 1523}],
  "topKCount": 1,
  "totalRequests": 158392,
  "l1CacheSize": 87,
  "l1MaxSize": 1000,
  "inflightSize": 3,
  "recentlyExpelled": ["cache:shop:5", "cache:shop:99"]
}
```

| Field | Description |
|-------|-------------|
| `topK` | Current Top-K hot keys (descending by count) |
| `topKCount` | Number of hot keys in Top-K set |
| `totalRequests` | Total requests passed through HotKey detection |
| `l1CacheSize` / `l1MaxSize` | L1 Caffeine current size / max limit |
| `inflightSize` | Current in-flight dedup requests |
| `recentlyExpelled` | Recently evicted keys from Top-K (up to 10) |

## Troubleshooting

### HotKeyCache bean not created when Redis is on the classpath

**Symptom**: Application context fails with:

```
Field hotKey in com.example.SomeService required a bean of type
'io.github.hyshmily.hotkey.hotkeycache.HotKeyCache' that could not be found.
```

**Root cause**:

Two auto-configuration classes compete to create the `HotKeyCache` bean. When `spring-boot-starter-data-redis` is on the classpath, both can be skipped:

| Class | Condition on `hotKeyCache()` | Why it skips |
|---|---|---|
| `HotKeyAutoConfiguration` | `@ConditionalOnMissingBean(type = "RedisTemplate")` | A `RedisTemplate` bean exists in the context (auto-configured by Redis starter) |
| `HotKeyRedisAutoConfiguration` | `@AutoConfiguration(after = ...)` + `@ConditionalOnBean(StringRedisTemplate.class)` | Runs **after** `HotKeyAutoConfiguration`. By the time its `HotKeyCache` is registered, `HotKeyAutoConfiguration`'s `hotKey()` method — which has `@ConditionalOnBean(HotKeyCache.class)` — has already evaluated to **false** |

Gap: `HotKeyRedisAutoConfiguration` creates `HotKeyCache`, but nobody creates the `HotKey` facade bean.

**Workaround** — define both beans manually in your project:

```java
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyCache;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HotKeyConfig {

    @Bean
    @ConditionalOnMissingBean
    public HotKeyCache hotKeyCache(
            TopK hotKeyDetector,
            Cache<String, Object> hotLocalCache,
            Cache<String, CompletableFuture<Object>> inflightLoads,
            @Qualifier("hotKeyExecutor") Executor hotKeyExecutor,
            HotKeyProperties properties
    ) {
        return new HotKeyCache(
            hotKeyDetector, hotLocalCache, inflightLoads,
            Optional.empty(), hotKeyExecutor,
            properties.getInflightTimeoutSeconds(),
            properties.getSoftTtlMs(),
            properties.getRefreshConcurrency(),
            properties.getSoftExpireMaxSize(),
            properties.getSoftExpireTtlMinutes(),
            properties.getVersionKeyTtlMinutes()
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public HotKey hotKey(HotKeyCache hotKeyCache, TopK hotKeyDetector) {
        return new HotKey(hotKeyCache, hotKeyDetector);
    }
}
```

`@ConditionalOnMissingBean` ensures these beans only kick in when the library's auto-configuration fails — no conflict, no duplication.

## Configuration Reference

| Property | Default | Description |
|---|---|---|
| `hotkey.top-k` | `100` | Top-K set size |
| `hotkey.width` | `50000` | Count-Min Sketch width |
| `hotkey.depth` | `5` | Count-Min Sketch depth (rows) |
| `hotkey.decay` | `0.92` | Conflict decay factor |
| `hotkey.min-count` | `10` | Minimum count threshold for hot key |
| `hotkey.local-cache-max-size` | `1000` | Caffeine L1 max entries |
| `hotkey.local-cache-ttl-minutes` | `5` | Caffeine L1 TTL in minutes |
| `hotkey.inflight-max-size` | `50000` | In-flight dedup max entries |
| `hotkey.inflight-ttl-seconds` | `5` | In-flight dedup entry TTL (must exceed slowest Redis response) |
| `hotkey.inflight-timeout-seconds` | `3` | Inflight load timeout (must be < inflight-ttl-seconds). On timeout returns `Optional.empty()` — caller should fallback to DB |
| `hotkey.executor-core-pool-size` | `8` | Thread pool core size |
| `hotkey.executor-max-pool-size` | `32` | Thread pool max size |
| `hotkey.executor-queue-capacity` | `2000` | Thread pool queue capacity |
| `hotkey.broadcast.enabled` | `false` | Enable RabbitMQ broadcast |
| `hotkey.broadcast.exchange-name` | `hotkey.broadcast.exchange` | Fanout exchange name |
| `hotkey.broadcast.queue-prefix` | `hotkey.broadcast` | Queue name prefix |
| `hotkey.broadcast.dedup-window-seconds` | `10` | Broadcast dedup window (seconds) |
| `hotkey.broadcast.dedup-max-size` | `10000` | Broadcast dedup max entries |
| `hotkey.decay-period` | `20` | (Deprecated) Decay period in seconds, backward compatibility only |
| `hotkey.broadcast.instance-id` | `${server.port:instance}-${HOSTNAME:${random.uuid}}` | Unique instance identifier |
| `hotkey.soft-ttl-ms` | `0` | Soft expire TTL (ms), 0 = disabled |
| `hotkey.soft-expire-max-size` | `50000` | Soft expire timestamp cache max entries |
| `hotkey.soft-expire-ttl-minutes` | `60` | Soft expire timestamp cache internal entry TTL (minutes) |
| `hotkey.refresh-concurrency` | `100` | Max concurrent async refreshes for soft expire |
| `hotkey.version-key-ttl-minutes` | `60` | Redis version key TTL (minutes), 0 = no expire |
| `hotkey.local-cache-access-ttl-minutes` | `0` | Caffeine L1 access-based TTL (minutes), 0 = disabled. Supplements write-based TTL |
| `hotkey.broadcast.concurrent-consumers` | `3` | Number of concurrent RabbitMQ consumers for broadcast queue |
| `hotkey.broadcast.scheduler-pool-size` | `4` | Thread pool size for async broadcast jitter delay scheduling |
| `hotkey.broadcast.warmup-jitter-ms` | `100` | Random jitter (ms) before processing broadcast messages to prevent thundering herd |
| `hotkey.scheduling.enabled` | `true` | Enable internal scheduler for HeavyKeeper decay and expelled queue drain. Set to `false` if you use your own `@EnableScheduling` or don't need periodic decay |


## Modules
| Module | Dependency | Auto-Config |
|--------|-----------|-------------|
| `algorithm` | none | always |
| `cache` (Redis) | `spring-boot-starter-data-redis` | `@ConditionalOnClass` |
| `broadcast` (RabbitMQ) | `spring-boot-starter-amqp` | `@ConditionalOnClass` + property |
| `actuator` | `spring-boot-starter-actuator` | `@ConditionalOnClass` |



## License

Apache License 2.0
