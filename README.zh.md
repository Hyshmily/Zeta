# HotKey

[**English**](README.md)

HotKey — 基于 [HeavyKeeper](https://github.com/go-kratos/aegis) 算法的 Top-K 热点 Key 检测 & Caffeine/Redis 多级缓存自动预热 Spring Boot Starter（低精度版本）


> [!Important]
> 这是作者在开发过程中总结的经验模块，不能确保生产过程中的可靠性与稳定性。如需完善的热点 key 自动监测和更高精度版本，请参考 [hotkey](https://gitee.com/jd-platform-opensource/hotkey)

## 特点

- **HeavyKeeper 算法** —  Count-Min Sketch + 指数冲突衰减的概率性 Top-K 检测
- **两级缓存** — Caffeine (L1) + Redis (L2)，自动热点缓存
- **热点广播** — 可选 RabbitMQ fanout，分布式的多实例缓存预热
- **Spring Boot 自动配置** — 引入依赖即用，零样板代码

## 快速开始

### 1. 添加依赖

```xml
<dependency>
    <groupId>io.github.hyshmily</groupId>
    <artifactId>hotkey-spring-boot-starter</artifactId>
    <version>1.0.2</version>
</dependency>
```

Redis 和 RabbitMQ 依赖均为可选——仅在使用对应功能时才需引入。

### 2. 配置

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

### 3. 使用

> **注意：** v1.0.2 包含**破坏性变更** — `get(hk, fk)` 和 `putAndBroadcast(hk, fk, val)` 已移除。库已与 `RedisTemplate` 解耦，调用方通过 `Supplier<T>` / `Runnable` 自行提供读写回调。

**A. 纯本地缓存（无二级存储）**

```java
@Autowired
private HotKeyCache hotKeyCache;

Optional<String> r = hotKeyCache.get("user:123"); // 仅 Caffeine L1 + 热点检测
```

等价于 `get(cacheKey, () -> null)`，L1 未命中返回 `Optional.empty()`，完全跳过二级存储。

**B. 两级缓存（Redis 或任意后端）**

```java
@Autowired
private HotKeyCache hotKeyCache;
@Autowired
private RedisTemplate<String, Object> redisTemplate;

Optional<String> r = hotKeyCache.get("user:123", () -> redisTemplate.opsForValue().get("user:123"));

hotKeyCache.putAndBroadcast("user:123", newValue, () -> redisTemplate.opsForValue().set("user:123", newValue));
```

**C. 数据库兜底**

```java
Optional<String> r = hotKeyCache.get("user:123", () -> redisTemplate.opsForValue().get("user:123"));
if (r.isEmpty()) {
    String value = userService.getById(123);   // DB 回退
    redisTemplate.opsForValue().set("user:123", value);
}
```

**D. 封装 Helper 避免重复 lambda**

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

**E. 自定义二级存储（非 Redis）**

```java
// 使用 MySQL、远程 API 或任意数据源作为 L2
Optional<User> r = hotKeyCache.get("user:123", () -> userMapper.selectById(123));
User user = r.orElseGet(() -> createDefaultUser());
```

### 4. 启用广播（多实例）

```yaml
hotkey:
  broadcast:
    enabled: true
```

广播模式通过 RabbitMQ fanout 在所有实例间同步热点 key。


## 架构

请求流程：Caffeine L1 → L2（可插拔）→ HeavyKeeper 检测：

```
┌──────────────┐   Caffeine 命中?   ┌──────────────┐
│   Request    │ ────────────────→  │  Caffeine L1 │
│              │ ←────────────────  │   (本地)     │
└──────┬───────┘   Optional.of(v)   └──────┬───────┘
       │ Caffeine 未命中?                   │ Hot key?
       ↓                                   ↓
┌──────────────┐                   ┌───────────────┐
│  L2 Storage  │                   │ HeavyKeeper   │
│  (可插拔)    │                   │   检测器      │
└──┬───────┬───┘                   └──────┬────────┘
   │ 找到值 │ L2/Source null               │ 被提升?
   ↓       ↓                               ↓
Optional   Optional.empty() ───→ DB        Caffeine.put
.of(value)   r.isEmpty()      回退          + 广播
```

写路径（用户主动调用）：
`hotKeyCache.putAndBroadcast(cacheKey, value, writer)`
├─ `writer.run()` — L2 写入（调用方传入的 Runnable）
├─ Caffeine 本地缓存更新
└─ RabbitMQ fanout 广播（如启用）

## 模块

| 模块 | 依赖 | 自动配置 |
|------|------|---------|
| `algorithm` | 无 | 始终生效 |
| `cache` (Redis) | `spring-boot-starter-data-redis` | `@ConditionalOnClass` |
| `broadcast` (RabbitMQ) | `spring-boot-starter-amqp` | `@ConditionalOnClass` + 属性开关 |


## 许可证

Apache License 2.0
