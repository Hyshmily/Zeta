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
    <version>1.0.0</version>
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

```java
@Autowired
private HotKeyCache hotKeyCache;

public Object getData(String hashKey, String fieldKey) {
    return hotKeyCache.get(hashKey, fieldKey);
}

public void updateData(String hashKey, String fieldKey, Object value) {
    // update database...
    hotKeyCache.updateCaffeineIfPresent(hashKey, fieldKey, value);
}
```

### 4. With broadcast (multi-instance)

```yaml
hotkey:
  broadcast:
    enabled: true
```

Broadcast mode synchronizes hot keys across all instances via RabbitMQ fanout.

### 5. Advanced

`hotKeyCache.get()` returns `Object` — cast to your expected type:

```java
String value = (String) hotKeyCache.get(hashKey, fieldKey);
```

If `null` is returned, both Caffeine and Redis were missed (value does not exist in Redis). You can use this to trigger a database fallback, though this logic is **not yet implemented** (pending). It is left for the user to handle. Future versions will provide richer result types.


## Architecture

The request flow: Caffeine L1 → Redis L2 → HeavyKeeper detection:

```
┌──────────────┐   Caffeine hit?    ┌──────────────┐
│   Request    │ ────────────────→  │  Caffeine L1 │
│              │ ←────────────────  │   (local)    │
└──────┬───────┘   return value     └──────┬───────┘
       │ Caffeine miss?                     │ Hot key?
       ↓                                   ↓
┌──────────────┐                   ┌───────────────┐
│   Redis L2   │                   │ HeavyKeeper   │
│  (global)    │                   │   Detector    │
└──┬───────┬───┘                   └──────┬────────┘
   │ hit   │ null (TODO)                  │ promoted?
   ↓       ↓                               ↓
refresh   return null ───→ DB fallback    Caffeine.put
cache                                       + broadcast
```

## Modules

| Module | Dependency | Auto-Config |
|--------|-----------|-------------|
| `algorithm` | none | always |
| `cache` (Redis) | `spring-boot-starter-data-redis` | `@ConditionalOnClass` |
| `broadcast` (RabbitMQ) | `spring-boot-starter-amqp` | `@ConditionalOnClass` + property |



## License

Apache License 2.0
