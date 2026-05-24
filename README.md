# HotKey

[**中文版**](README.zh.md)

HotKey — [HeavyKeeper](https://github.com/go-kratos/aegis) top-k hot key detection & Caffeine/Redis multi-level cache auto-warming Spring Boot Starter (low-precision version)

> [!Important]
> This is an experience module summarized by the author during development. Reliability and stability in production cannot be guaranteed. For a complete production-ready hot key auto-detection and higher-precision version, please refer to [hotkey](https://gitee.com/jd-platform-opensource/hotkey)

## Features

- **HeavyKeeper Algorithm** — probabilistic top-k detection with Count-Min Sketch + exponential conflict decay
- **Two-Level Cache** — Caffeine (L1) + Redis (L2) with automatic hot-key promotion
- **Hot Key Broadcast** — optional RabbitMQ fanout for distributed multi-instance cache warming
- **Spring Boot Auto-Configuration** — drop-in dependency, zero boilerplate


## Quick Start

### 1. Add dependency

```xml
<dependency>
    <groupId>io.github.hyshmily</groupId>
    <artifactId>hotkey-spring-boot-starter</artifactId>
    <version>1.0.2</version>
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

> **Note:** v1.0.2 introduces a **breaking change** — `get(hk, fk)` and `putAndBroadcast(hk, fk, val)` are removed. The library is now decoupled from `RedisTemplate`; callers supply their own read/write callbacks via `Supplier<T>` / `Runnable`.

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

### 4. With broadcast (multi-instance)

```yaml
hotkey:
  broadcast:
    enabled: true
```

Broadcast mode synchronizes hot keys across all instances via RabbitMQ fanout.

## Architecture

The request flow: Caffeine L1 → L2 (pluggable) → HeavyKeeper detection:

```
┌──────────────┐   Caffeine hit?    ┌──────────────┐
│   Request    │ ────────────────→  │  Caffeine L1 │
│              │ ←────────────────  │   (local)    │
└──────┬───────┘   Optional.of(v)   └──────┬───────┘
       │ Caffeine miss?                     │ Hot key?
       ↓                                   ↓
┌──────────────┐                   ┌───────────────┐
│  L2 Storage  │                   │ HeavyKeeper   │
│  (pluggable) │                   │   Detector    │
└──┬───────┬───┘                   └──────┬────────┘
   │ hit   │ L2/Source null                │ promoted?
   ↓       ↓                               ↓
Optional   Optional.empty() ───→ DB        Caffeine.put
.of(value)   r.isEmpty()      fallback     + broadcast
```

Write path (user-initiated):
`hotKeyCache.putAndBroadcast(cacheKey, value, writer)`
├─ `writer.run()` — L2 write (caller-supplied Runnable)
├─ Caffeine local cache update
└─ RabbitMQ fanout broadcast (if enabled)

## Modules

| Module | Dependency | Auto-Config |
|--------|-----------|-------------|
| `algorithm` | none | always |
| `cache` (Redis) | `spring-boot-starter-data-redis` | `@ConditionalOnClass` |
| `broadcast` (RabbitMQ) | `spring-boot-starter-amqp` | `@ConditionalOnClass` + property |



## License

Apache License 2.0
