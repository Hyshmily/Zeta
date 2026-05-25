# HotKey

[**中文版**](README.zh.md)

HotKey — HeavyKeeper Top-K hot key detection + multi-level cache auto-warming + distributed broadcast Spring Boot Starter

HotKey is not a general-purpose local cache — it's a lightweight hot key auto-detection & multi-level cache warming framework with optional distributed broadcast.

Most local cache solutions store every accessed key in Caffeine. This works fine with small data, but under millions of keys:

- **Memory waste** — most keys are read once and never accessed again
- **Broadcast storm** — full cache invalidation requires full broadcast at scale

HotKey takes a different approach — **cache only the hot keys.**

It uses [HeavyKeeper](https://github.com/go-kratos/aegis) (a Count-Min Sketch variant) to probabilistically detect access frequency. Only keys that enter the Top-K set are promoted into the local Caffeine L1, and optionally synchronized across instances via RabbitMQ fanout. Non-hot key reads are delegated back to the caller via `Supplier<T>` / `Function<String, Object>` — the framework makes no assumption about your data source.

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

## Features

- **HeavyKeeper Algorithm** — probabilistic top-k detection with Count-Min Sketch + exponential conflict decay
- **Three-Level Cache** — Caffeine (L1) → Redis (L2, optional) → DB fallback, with automatic hot-key promotion
- **In-Flight Dedup** — concurrent L1 miss requests share a single Redis read via `Caffeine<key, CompletableFuture>`

  > **Note:** Ensure `hotkey.inflight-ttl-seconds` exceeds the slowest Redis response time for your workload, or the cache entry may expire before the future completes, causing duplicate Redis reads.
  > Also ensure `hotkey.inflight-timeout-seconds` < `hotkey.inflight-ttl-seconds`. On timeout, `loadSingleflight` returns `Optional.empty()` — the caller should handle via DB fallback.
- **Soft Expire** — return stale L1 value immediately while asynchronously refreshing in the background; lower p99 at the cost of short-lived staleness
- **Redis Collections** — `invalidateAfterWriteSync` for List/Set/ZSet incremental writes; no `putAndBroadcast` needed
- **Hot Key Broadcast** — optional RabbitMQ fanout to synchronize hot keys across instances
- **Configurable Thread Pool** — dedicated `TaskExecutor` with bounded queue
- **Spring Boot Auto-Configuration** — drop-in dependency, zero boilerplate


## Architecture

```
┌──────────────┐   L1 hit + add(key,1) ┌──────────────┐
│   Request    │ ────────────────────→ │  Caffeine L1 │
│              │ ←──────────────────── │   (local)    │
└──────┬───────┘   Optional.of(value)  └──────┬───────┘
       │ L1 miss                              │ isHotKey()?
       ↓ (inflight dedup)                     ↓
┌──────────────┐   redisReader     ┌───────────────┐
│  L2 Storage  │ ←───────────────  │     TopK      │
│  (pluggable) │ ───────────────→  │  (interface)  │
└──┬───────┬───┘  add(key,1)       ├───────────────┤
   │ hit   │ null                  │ add()→Result  │ ← called on every read
   ↓       ↓                       │ list()        │ ← public
Optional   Optional.empty()        │ total()       │ ← public
.of(value)   r.isEmpty() → DB      │ expelled()    │ ← internal (drainExpelled)
                                   │ fading()      │ ← internal (cleanHotKeys)
                                   └───────┬───────┘
                                           │ isHotKey()
                                           ↓
                                     Caffeine.put(key, value)
                                     + broadcastHotKey(cacheKey)
```

Write path (user-initiated):
`hotKeyCache.putAndBroadcast(cacheKey, value, writer)`
├─ `writer.run()` — L2 write (caller-supplied Runnable)
├─ Caffeine local cache update
└─ RabbitMQ fanout broadcast (if enabled)

For incremental collection mutations (LPUSH, SADD, ZADD):
`hotKeyCache.invalidateAfterWriteSync(cacheKey, mutation)`
├─ `mutation.run()` — L2 write (caller-supplied Runnable)
├─ Caffeine local cache **invalidate** (next read re-fetches)
└─ RabbitMQ fanout broadcast (if enabled)

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
```

## Quick Start

### 1. Add dependency

```xml
<dependency>
    <groupId>io.github.hyshmily</groupId>
    <artifactId>hotkey</artifactId>
    <version>1.0.3</version>
</dependency>
```

Redis and RabbitMQ dependencies are optional — include them only if you need the corresponding features.

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
  decay-period: 20
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
private HotKeyCache hotKeyCache;

Optional<String> r = hotKeyCache.get("user:123"); // Caffeine L1 + hot key detection only
```

Calls `get(cacheKey, () -> null)` — returns `Optional.empty()` if L1 miss, skips secondary storage entirely.

**B. Two-level cache (Redis or any backend)**

```java
@Autowired
private HotKeyCache hotKeyCache;
@Autowired
private RedisTemplate<String, Object> redisTemplate;

Optional<String> r = hotKeyCache.get("user:123", () -> redisTemplate.opsForValue().get("user:123"));

hotKeyCache.putAndBroadcast("user:123", newValue, () -> redisTemplate.opsForValue().set("user:123", newValue));
```

**C. Database fallback**

```java
Optional<String> r = hotKeyCache.get("user:123", () -> redisTemplate.opsForValue().get("user:123"));
if (r.isEmpty()) {
    String value = userService.getById(123);   // DB fallback
    redisTemplate.opsForValue().set("user:123", value);
}
```

**D. Helper bean to avoid repetitive lambdas**

```java
@Component
public class RedisHotKeyHelper {
    @Autowired private HotKeyCache hotKeyCache;
    @Autowired private RedisTemplate<String, Object> redisTemplate;

    public <T> Optional<T> get(String key) {
        return hotKeyCache.get(key, () -> redisTemplate.opsForValue().get(key));
    }

    public void set(String key, Object value) {
        hotKeyCache.putAndBroadcast(key, value, () -> redisTemplate.opsForValue().set(key, value));
    }
}
```

**E. Custom L2 (non-Redis)**

```java
// Use MySQL, remote API, or any data source as L2
Optional<User> r = hotKeyCache.get("user:123", () -> userMapper.selectById(123));
User user = r.orElseGet(() -> createDefaultUser());
```

**F. Redis collections (List, Set, ZSet)**

`putAndBroadcast` requires the full new value for L1 update, but collection incremental operations (LPUSH, SADD, ZADD) modify only a single element — the caller cannot know the full collection state. Use `invalidateAfterWriteSync` to invalidate L1 after the mutation; the next `get()` re-fetches from Redis, ensuring consistency.

```java
@Component
public class CollectionHotKeyCache {
    @Autowired private HotKeyCache hotKeyCache;
    @Autowired private RedisTemplate<String, Object> redisTemplate;

    public Boolean sIsMember(String key, Object member) {
        return hotKeyCache.get(key + "::member::" + member,
            () -> redisTemplate.opsForSet().isMember(key, member));
    }

    @SuppressWarnings("unchecked")
    public Set<Object> sMembers(String key) {
        return hotKeyCache.get(key,
            () -> redisTemplate.opsForSet().members(key));
    }

    public void sAdd(String key, Object... members) {
        hotKeyCache.invalidateAfterWriteSync(key,
            () -> redisTemplate.opsForSet().add(key, members));
    }

    public List<Object> lRange(String key, long start, long end) {
        String cacheKey = key + "::range::" + start + "::" + end;
        return hotKeyCache.get(cacheKey,
            () -> redisTemplate.opsForList().range(key, start, end));
    }

    public Double zScore(String key, Object member) {
        return hotKeyCache.get(key + "::score::" + member,
            () -> redisTemplate.opsForZSet().score(key, member));
    }
}
```

**G. Soft expire + singleflight re-source**

Soft expire returns the stale L1 value immediately while asynchronously refreshing in the background. Use when short-lived staleness is acceptable in exchange for lower p99 latency.

```java
Optional<String> r = hotKeyCache.getWithSoftExpire("user:123",
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

### 4. With broadcast (multi-instance)

```yaml
hotkey:
  broadcast:
    enabled: true
```

Each instance declares its own queue (`hotkey.broadcast:<pod-id>`) bound to a fanout exchange. When a hot key is promoted, the instance broadcasts the key. Peers load the value from Redis on first broadcast miss. Invalidations remove the local cache entry immediately.

### 5. Configuration Reference

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


## Modules
| Module | Dependency | Auto-Config |
|--------|-----------|-------------|
| `algorithm` | none | always |
| `cache` (Redis) | `spring-boot-starter-data-redis` | `@ConditionalOnClass` |
| `broadcast` (RabbitMQ) | `spring-boot-starter-amqp` | `@ConditionalOnClass` + property |



## License

Apache License 2.0
