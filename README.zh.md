# HotKey

[![JitPack](https://jitpack.io/v/Hyshmily/HotKey.svg)](https://jitpack.io/#Hyshmily/HotKey) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0) [![Java](https://img.shields.io/badge/Java-25-orange)](https://openjdk.java.net/) [![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.5.3-brightgreen)](https://spring.io/projects/spring-boot)

[**English**](README.md)

HotKey — HeavyKeeper Top-K 热点检测 + 多级缓存自动预热 + 分布式广播 Spring Boot Starter

HotKey 不是一个通用的本地缓存——它是一个带有可选分布式广播的轻量热点 key 自动检测与多级缓存预热框架。

大多数本地缓存在 Caffeine 里存每一个访问过的 key。数据量小时没问题，但在海量 key 场景下：

- **内存浪费** — 绝大多数 key 只读一次就再也不会访问
- **广播风暴** — 全量缓存要求全量失效，广播量随 key 数线性增长

而 HotKey 却 **只缓存真正热门的 key。**

通过 [HeavyKeeper](https://github.com/go-kratos/aegis)（Count-Min Sketch 变体）概率检测访问频率。**读路径**上，只有进入 Top-K 集合的 key 才会被自动提升到本地 Caffeine L1，并通过可选的 RabbitMQ fanout 跨实例同步。**写路径**上，`putThrough` 无论 key 是否为热点，都直接写入 L1 并广播——调用方显式拥有写入权。非热点 key 的读取仍然通过调用方提供的 `Supplier<T>` 获取并返回值——只是不缓存到 L1。框架对你的数据源不做任何假设。

### 适用场景

| 适合                                       | 不适合                                |
| ------------------------------------------ | ------------------------------------- |
| 读密集型场景（String / List / Set / ZSet） | 写密集型 / 原子操作（秒杀 Lua）       |
| 大量 key，访问呈帕累托分布                 | key 数少（< 200），手动 Caffeine 即可 |
| 读多写少，最终一致即可                     | 要求写后立即可见                      |
| Spring Boot 3.x + Java 17+                 | 非 Spring Boot 项目                   |
| 可选 Redis + 可选 RabbitMQ（多实例）       |                                       |

> [!Important]
> 这是作者在开发过程中总结的经验模块，不能确保生产过程中的可靠性与稳定性。如需完善的热点 key 自动监测和更高精度版本，请参考 [hotkey](https://gitee.com/jd-platform-opensource/hotkey)

> 更新日志见 [CHANGELOG.zh.md](CHANGELOG.zh.md)。

## 特点

- **HeavyKeeper 算法** — Count-Min Sketch + 指数冲突衰减的概率性 Top-K 检测
- **多级缓存** — Caffeine (L1) → 可选的 reader 回调（L2，如 Redis）+ 调用方通过 `Optional.orElseGet()` 实现的 DB 回退，自动热点提升
- **请求合并** — L1 未命中时同 key 并发请求共享同一 L2 读，通过专门的 `SingleFlight` bean

  > **注意：** 确保 `hotkey.inflight-ttl-seconds` 大于最慢的 L2 响应耗时，否则缓存条目可能在 Future 完成前过期，导致重复的 L2 读。
  > 同时确保 `hotkey.inflight-timeout-seconds` < `hotkey.inflight-ttl-seconds`。超时后 `SingleFlight.load()` 返回 `Optional.empty()`，调用方需通过 DB 回退处理。

- **软失效（逻辑过期）** — 立即返回过期旧值，同时后台异步刷新；降低 p99 延迟，代价是短暂脏读。**完全替代传统 Redis 侧逻辑过期**（`RedisData{data, expireTime}` 包装模式）——Redis 纯值存储，由 HotKey 在 L1 Caffeine 层管理过期
- **Redis 集合类型** — 通过 `putBeforeInvalidate` 支持 List/Set/ZSet 增量写入，无需 `putThrough`
- **热点广播** — 可选 RabbitMQ fanout，跨实例同步热点 key
- **可配置线程池** — 专用 `TaskExecutor`，有界队列
- **Spring Boot 自动配置** — 引入依赖即用，零样板代码

## 架构

```
┌──────────────┐   L1 hit + add(key,1) ┌──────────────┐
│   Request    │ ────────────────────→ │  Caffeine L1 │
│              │ ←──────────────────── │  (local)     │
└──────┬───────┘   Optional.of(value)  └──────┬───────┘
       │ L1 miss           (自动解包           │ isHotKey()?
       ↓ (inflight dedup)  CacheEntry)        ↓
┌──────────────┐  ──── reader ────→ ┌───────────────┐
│  L2 Storage  │  ───add(key,1)───→ │     TopK      │
│  (pluggable) │                    │  (interface)  │
└──┬───────┬───┘                    ├───────────────┤
   │ hit   │ null                   │ add()→Result  │
   ↓       ↓                        │ list()        │
Optional   Optional.empty()         │ total()       │
.of(value)   r.isEmpty() → DB       │ contains()    │
                                    │ expelled()    │
                                    │ fading()      │
                                    └───────┬───────┘
                                            │ isHotKey()
                                            ↓
                              Caffeine.put(key,
                                 CacheEntry(value, version=0L, isVersionDegraded=false, expireAtMs))
                              + broadcastHotKey with version header
```

写路径（用户主动调用）：
`putThrough(cacheKey, value, writer)`
├─（在 Spring 事务中延迟到 afterCommit 执行）
├─ `writer.run()` — L2 写入（调用方传入的 Runnable）
├─ `nextVersion(cacheKey)` — Redis INCR → `VersionResult(version, isVersionDegraded)`
├─ `SoftExpireManager.refresh()` — 更新软过期时间戳
├─ Caffeine.put(cacheKey, CacheEntry(value, version, isVersionDegraded, expireAtMs))
└─ RabbitMQ fanout 广播 + version + `isVersionDegraded` headers（如启用）

**写路径的事务延迟：** `putThrough`、`putBeforeInvalidate`、`invalidate`、`invalidateAll` 在 Spring 事务中被调用时都会延迟到 `afterCommit` 执行。

> **注意：** `putThrough` 在**非事务**状态下与其他写方法行为不同——它会在 `hotKeyExecutor` 上**异步**执行（调用方立即返回，writer、版本号递增、L1 更新和广播在后台线程中运行）。非事务状态下，其他方法（`invalidate`、`invalidateAll`、`putBeforeInvalidate`）在调用方线程上同步执行。

对于集合增量操作（LPUSH、SADD、ZADD）：
`putBeforeInvalidate(cacheKey, mutation)`
├─（在 Spring 事务中延迟到 afterCommit 执行）
├─ `mutation.run()` — L2 写入（调用方传入的 Runnable）
│ └─ 发生异常 → 跳过本地失效和广播，记录错误日志
├─ `nextVersion(cacheKey)` — Redis INCR → `VersionResult(version, isVersionDegraded)`
├─ Caffeine 本地缓存**失效**
└─ RabbitMQ fanout 广播 + version + `isVersionDegraded` headers（如启用）

> **注意：** `mutation.run()` 到 L1 缓存失效之间存在约 1ms 的不一致窗口，期间并发的 `get()` 可能命中 L1 旧值。这是刻意的取舍——如果在 mutation 之前失效，`get()` 会从 Redis 读到旧值重新填入 L1，造成更严重的数据污染。该窗口宽度由一次 Redis 往返（`nextVersion` 调用）决定。

软失效读路径（`getWithSoftExpire`）：

```
         ┌──────────────┐   L1 命中 ┌───────────────┐
         │   Request    │ ───────→  │ softExpireAt  │
         │              │ ←───────  │  时间检查     │
         └──────┬───────┘  返回旧值 └───────┬───────┘
                │ 已软过期?                 │ 过期?
                ↓ 是                        ↓ 是
           返回过期旧值              triggerAsyncRefresh
           + TopK 检测                  ├─ refreshLimiter.tryAcquire()
                │                       └─ 异步 L2 → Caffeine.put
                │ L1 未命中（走正常路径）    + 更新 softExpireAt
                ↓
            SingleFlight.load(cacheKey, reader)
            (参见上方正常读路径)
            Caffeine.put(key, CacheEntry(value, 0L, false, expireAt(hardTtlMs)))
```

## 降级

HotKey 通过 `supplier` 回调形成三级降级链路：

```
hotKey.get(key, supplier)
  ├─ L1(Caffeine) 命中 → 直接返回
  ├─ L1 未命中 → supplier()
  │    ├─ 返回数据 → 热 key？ → 写 L1 + 返回
  │    ├─ 返回 null → Optional.empty() → 调用方 orElseGet/orElseThrow
  │    └─ 抛出异常 → SingleFlight.load() 捕获 → Optional.empty() → 调用方兜底
  └─ HotKey 自身异常 → 异常传播到调用方（无自动兜底）
```

各组件故障表现：

| 故障组件               | 影响                            | 恢复方式              |
| ---------------------- | ------------------------------- | --------------------- |
| HotKey 自身            | L1 不可用；异常传播到调用方     | 应用重启              |
| L2 后端 (Redis/DB/API) | 每次请求穿透到调用方兜底        | 后端恢复后自动恢复    |
| L1 Caffeine OOM / 驱逐 | 单 key 被驱逐，下次读取重新回源 | 自动（Caffeine 内部） |

> 调用方始终需要处理 `Optional.empty()` — HotKey 不会隐藏后端故障。

写路径故障表现：

| 写方法                                              | 故障场景                    | 表现                                                   |
| --------------------------------------------------- | --------------------------- | ------------------------------------------------------ |
| `putThrough`                                        | 线程池队列满（非事务）      | `RejectedExecutionException` 传播到调用方              |
| `putThrough`                                        | `writer.run()` / Redis 失败 | 错误记录到日志，L1 版本号未更新，不发送广播            |
| `putBeforeInvalidate`                               | `mutation.run()` 抛出异常   | 捕获突变异常并记录日志；跳过本地失效和广播             |
| `invalidate` / `putBeforeInvalidate` / `putThrough` | `nextVersion()` Redis 失败  | 回退到 `System.nanoTime()`（单调递增但非持久化版本号） |

## 快速开始

### 1. 添加依赖（JitPack）

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
    <version>1.0.9</version>
</dependency>
```

版本号使用 `1.0.9`。Redis 和 RabbitMQ 依赖均为可选——仅在使用对应功能时才需引入。

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
  broadcast:
    enabled: false
    exchange-name: hotkey.broadcast.exchange
    queue-prefix: hotkey.broadcast
```

### 3. 使用

> **注意：** 自v1.0.2起包含**破坏性变更** — `get(hk, fk)` 和 `putAndBroadcast(hk, fk, val)` 已移除。库已与 `RedisTemplate` 解耦，调用方通过 `Supplier<T>` / `Runnable` 自行提供读写回调。

**A. 纯本地缓存（无二级存储）**

```java
@Autowired
private HotKey hotKey;

Optional<String> r = hotKey.peek("user:123"); // 仅查 L1，不做热点追踪
```

等价于 `peek(cacheKey)`，L1 未命中返回 `Optional.empty()`，完全跳过二级存储。

**B. 两级缓存（Redis 或任意后端）**

```java
@Autowired
private HotKey hotKey;
@Autowired
private RedisTemplate<String, Object> redisTemplate;

Optional<String> r = hotKey.get("user:123", () -> redisTemplate.opsForValue().get("user:123"));

hotKey.putThrough("user:123", newValue, () -> redisTemplate.opsForValue().set("user:123", newValue));
```

**C. 数据库兜底**

```java
Optional<String> r = hotKey.get("user:123", () -> redisTemplate.opsForValue().get("user:123"));
if (r.isEmpty()) {
    String value = userService.getById(123);   // DB 回退
    redisTemplate.opsForValue().set("user:123", value);
}
```

**D. 封装 Helper 避免重复 lambda**

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

**E. 自定义二级存储（非 Redis）**

```java
// 使用 MySQL、远程 API 或任意数据源作为 L2
Optional<User> r = hotKey.get("user:123", () -> userMapper.selectById(123));

User user = r.orElseGet(() -> createDefaultUser());
```

**F. Redis 集合类型（List、Set、ZSet）**

`putThrough` 需要传入完整新值来更新 L1，但集合的增量操作（LPUSH、SADD、ZADD）只修改单个元素——调用方无法获知全量新值。使用 `putBeforeInvalidate` 在突变后失效 L1，下次 `get()` 自动回源 Redis，保证一致性。

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

**G. 软失效——替代传统逻辑过期**

软失效立即返回过期旧值同时后台异步刷新（stale-while-revalidate）。与传统逻辑过期（在 Redis 值里嵌入 `expireTime`）不同，HotKey 完全在 L1 Caffeine 层管理过期——**Redis 存纯值，无需任何包装**。

| 维度       | 传统逻辑过期                                   | HotKey 软失效                                             |
| ---------- | ---------------------------------------------- | --------------------------------------------------------- |
| 过期存储   | 嵌入 Redis 值（`RedisData{data, expireTime}`） | L1 Caffeine 元数据（`softExpireAt`）                      |
| 返回旧值   | 解析包装后返回旧数据                           | 直接返回 L1 旧值                                          |
| 异步重建   | Redis 分布式锁 + 自定义线程池                  | Singleflight（本地）+ `hotKeyExecutor` + `refreshLimiter` |
| Redis 格式 | 包裹 JSON                                      | 纯值（无需包装）                                          |
| DB 回退    | 手动加锁逻辑                                   | 原生 `orElseGet` / `orElseThrow`                          |

```java
// 传统方式（不再需要）：
//   redisData.setExpireTime(LocalDateTime.now().plusSeconds(30L));
//   stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));

// HotKey：Redis 存纯值，软过期由 L1 管理
Optional<String> r = hotKey.getWithSoftExpire("user:123", () -> redisTemplate.opsForValue().get("user:123"));

// L1 命中但已软过期 → 返回旧值 + 触发异步刷新
// L1 未命中 → singleflight 回源（同 get()）

// 自定义 per-call softTtl（覆盖全局 hotkey.soft-ttl-ms）
Optional<String> r2 = hotKey.getWithSoftExpire("user:456", () -> redisTemplate.opsForValue().get("user:456"), 3000);
```

DB 回退（无需分布式锁）：

```java
User user = hotKey
  .getWithSoftExpire("shop:" + shopId, () -> redisTemplate.opsForValue().get("shop:" + shopId))
  .orElseGet(() -> {
    User u = userMapper.selectById(shopId);
    if (u != null) {
      redisTemplate.opsForValue().set("shop:" + shopId, JSONUtil.toJsonStr(u));
    }
    return u;
  });
```

配置示例：

```yaml
hotkey:
  soft-ttl-ms: 5000 # 启用软失效，5s 软 TTL（默认 0 = 禁用）
  refresh-concurrency: 50 # 限制异步刷新并发数
```

**H. 自定义 per-entry 硬 TTL**

默认所有 entry 共享全局 `hotkey.local-cache-ttl-minutes`。通过 `get(key, reader, ttlMs)` 或 `putThrough(key, value, writer, ttlMs)` 可为单个 entry 设置独立的 Caffeine 硬 TTL。未传 ttlMs 的调用仍使用全局配置。

```java
// 5 分钟硬 TTL
Optional<Shop> shop = hotKey.get("shop:" + shopId,
    () -> redisTemplate.opsForValue().get("shop:" + shopId),
    TimeUnit.MINUTES.toMillis(5));

// 30 秒硬 TTL
hotKey.putThrough("weather:" + city, weatherData,
    () -> redisTemplate.opsForValue().set("weather:" + city, weatherData),
    TimeUnit.SECONDS.toMillis(30));
```

> **注意：** 与 `getWithSoftExpire` 配合使用时，per-entry 硬 TTL 在后台刷新中会被保留。如果 key 加载时设置了自定义 `ttlMs`，后续软过期刷新会保持原始硬过期时间，不会重置为全局默认值。

## HotKey 门面 API 参考

推荐入口是 `HotKey` 门面（已自动配置为 Spring Bean）。除了上面展示的 `get`/`peek`/`putThrough`/`putBeforeInvalidate` 之外，还提供：

| 方法                                                   | 说明                                                                  |
| ------------------------------------------------------ | --------------------------------------------------------------------- |
| `peek(key)`                                            | 仅查 L1，不做频率追踪，不读 L2                                        |
| `get(key, reader)`                                     | 从 L1 或 L2 reader 读取；热点 key 自动提升到 L1                       |
| `get(key, reader, hardTtlMs)`                          | 同上，带 per-entry Caffeine 硬 TTL（毫秒）                            |
| `getWithSoftExpire(key, reader)`                       | 软失效—返回过期旧值+触发异步刷新；使用全局 `soft-ttl-ms`              |
| `getWithSoftExpire(key, reader, softTtlMs)`            | 同上，带 per-call 软 TTL 覆盖（毫秒）                                 |
| `getWithSoftExpire(key, reader, hardTtlMs, softTtlMs)` | 同上，同时带 per-entry 硬 TTL 和 per-call 软 TTL（毫秒）              |
| `putThrough(key, value, writer)`                       | 写穿透：writer.run()、nextVersion()、L1 更新、可选广播                |
| `putThrough(key, value, writer, hardTtlMs)`            | 同上，带 per-entry Caffeine 硬 TTL（毫秒）                            |
| `putThrough(key, value, writer, hardTtlMs, softTtlMs)` | 同上，同时带 per-entry 硬 TTL 和软 TTL（毫秒）                        |
| `putBeforeInvalidate(key, mutation)`                   | 先写后失效，用于集合增量操作（LPUSH、SADD、ZADD）                     |
| `isHotKey(cacheKey)`                                   | 检查 key 是否在当前的 Top-K 热点集合中（O(1)）                        |
| `invalidate(cacheKey)`                                 | 使单个 key 在所有缓存层失效                                           |
| `invalidateAll(cacheKeys...)`                          | 可变参数重载 — 批量失效多个 key                                       |
| `invalidateAll(Collection)`                            | Collection 重载                                                       |
| `returnHotKeys()`                                      | Top-K 快照（key + 计数）                                              |
| `returnExpelledHotKeys()`                              | 获取被挤出的热点 key 队列（最近被挤出 Top-K）；由内部定时器周期性清空 |
| `returnTotalDataStreams()`                             | 经过 HeavyKeeper 的累计读取数                                         |

> **注意：** `invalidate()` 通过 Redis `INCR` 生成单调版本号，以 `TYPE_HOT` 广播——对端通过 `handleVersionedHotKey` 从 Redis 重新加载，跳过旧版本。`invalidateAll()` **不调用** `INCR`——它以 `TYPE_INVALIDATE` 广播，版本 `0L`，所有对端无条件从 L1 移除该 key。

## TTL 参考

| 方法                                                   | TTL 含义                                                                | 默认值                             |
| ------------------------------------------------------ | ----------------------------------------------------------------------- | ---------------------------------- |
| `get(key, reader)`                                     | 无 Caffeine 硬 TTL 覆盖（等效 `Long.MAX_VALUE`）                        | N/A                                |
| `get(key, reader, hardTtlMs)`                          | Caffeine 硬 TTL——到期直接淘汰                                           | 第 3 参数必传                      |
| `getWithSoftExpire(key, reader)`                       | Caffeine 软 TTL——返回旧值 + 触发异步刷新                                | 全局 `hotkey.soft-ttl-ms`          |
| `getWithSoftExpire(key, reader, softTtlMs)`            | 同上，单次调用覆盖                                                      | 调用者传入                         |
| `getWithSoftExpire(key, reader, hardTtlMs, softTtlMs)` | 同时带硬 TTL（Caffeine 淘汰）和软 TTL（异步刷新）                       | 第 3、4 参数必传                   |
| `putThrough(key, value, writer)`                       | 无 per-entry 硬 TTL 覆盖（等效 `Long.MAX_VALUE`）                       | N/A                                |
| `putThrough(key, value, writer, hardTtlMs)`            | 写入条目的 Caffeine 硬 TTL                                              | 调用者传入                         |
| `putThrough(key, value, writer, hardTtlMs, softTtlMs)` | 同时带硬 TTL 和软 TTL                                                   | 第 4、5 参数必传                   |
| 全局 `hotkey.local-cache-ttl-minutes`                  | 所有 entry 的默认硬 TTL（当未传 per-call TTL 时）                       | `5` 分钟                           |
| 全局 `hotkey.soft-ttl-ms`                              | 未传 per-call 值时的默认软 TTL                                          | `0`（禁用）                        |
| 全局 `hotkey.local-cache-access-ttl-minutes`           | 基于访问的硬 TTL（每次读取重置），作为 `local-cache-ttl-minutes` 的补充 | `0`（禁用）                        |

## 广播

```yaml
hotkey:
  broadcast:
    enabled: true
```

每个实例声明独立队列（`hotkey.broadcast:<pod-id>`）绑定到 fanout 交换机。两种消息类型：

- **`TYPE_HOT`** — 热点提升或带版本号的单 key 失效。对端通过 `handleVersionedHotKey` 从 Redis 重新加载，根据版本号跳过旧更新。
  - 消息携带 `isVersionDegraded` 头部。当版本来自降级源（Redis 失败后的 `System.nanoTime()` 回退）时，对端即使本地已有非降级条目也会接受该更新，防止版本号比较导致有效更新被拒绝。
- **`TYPE_INVALIDATE`** — 批量失效（`invalidateAll`）。对端立即从 L1 移除该 key，不重新加载。

对端首次收到广播后从 Redis 回读。

## 监控

当 classpath 包含 `spring-boot-starter-actuator` 时，HotKey Endpoint 自动注册在 `/actuator/hotkey`。

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,hotkey
```

```json
{
  "topK": [{ "key": "cache:shop:17", "count": 1523 }],
  "topKCount": 1,
  "totalRequests": 158392,
  "l1CacheSize": 87,
  "l1MaxSize": 1000,
  "inflightSize": 3,
  "recentlyExpelled": ["cache:shop:5", "cache:shop:99"]
}
```

| 字段                        | 说明                                  |
| --------------------------- | ------------------------------------- |
| `topK`                      | 当前 TopK 热点 key 列表（按计数降序） |
| `topKCount`                 | 热点 key 数量                         |
| `totalRequests`             | 累计经过 HotKey 检测的请求总数        |
| `l1CacheSize` / `l1MaxSize` | L1 Caffeine 当前大小 / 最大限制       |
| `inflightSize`              | 当前 inflight dedup 中的请求数        |
| `recentlyExpelled`          | 最近被挤出 TopK 的 key（最近 10 个）  |

## 配置属性参考

| 属性                                    | 默认值                      | 说明                                                                                                                       |
| --------------------------------------- | --------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `hotkey.top-k`                          | `100`                       | Top-K 集合大小                                                                                                             |
| `hotkey.width`                          | `50000`                     | Count-Min Sketch 宽度                                                                                                      |
| `hotkey.depth`                          | `5`                         | Count-Min Sketch 深度（行数）                                                                                              |
| `hotkey.decay`                          | `0.92`                      | 冲突衰减系数                                                                                                               |
| `hotkey.min-count`                      | `10`                        | 热点最小计数阈值                                                                                                           |
| `hotkey.local-cache-max-size`           | `1000`                      | Caffeine L1 最大条目数                                                                                                     |
| `hotkey.local-cache-ttl-minutes`        | `5`                         | Caffeine L1 过期时间（分钟）                                                                                               |
| `hotkey.inflight-max-size`              | `50000`                     | 请求合并最大条目数                                                                                                         |
| `hotkey.inflight-ttl-seconds`           | `5`                         | 请求合并条目 TTL（需大于最慢 Redis 响应耗时）                                                                              |
| `hotkey.inflight-timeout-seconds`       | `3`                         | 请求合并超时（秒），必须小于 inflight-ttl-seconds。超时返回 Optional.empty()，调用方应回退 DB                              |
| `hotkey.executor-core-pool-size`        | `8`                         | 线程池核心线程数                                                                                                           |
| `hotkey.executor-max-pool-size`         | `32`                        | 线程池最大线程数                                                                                                           |
| `hotkey.executor-queue-capacity`        | `2000`                      | 线程池队列容量                                                                                                             |
| `hotkey.broadcast.enabled`              | `false`                     | 启用 RabbitMQ 广播                                                                                                         |
| `hotkey.broadcast.exchange-name`        | `hotkey.broadcast.exchange` | Fanout 交换机名                                                                                                            |
| `hotkey.broadcast.queue-prefix`         | `hotkey.broadcast`          | 队列名前缀                                                                                                                 |
| `hotkey.broadcast.dedup-window-seconds` | `10`                        | 广播去重窗口（秒）                                                                                                         |
| `hotkey.broadcast.dedup-max-size`       | `10000`                     | 广播去重最大条目数                                                                                                         |
| `hotkey.decay-period`                   | `20`                        | （已废弃）衰减周期（秒），仅做向后兼容                                                                                     |
| `hotkey.broadcast.instance-id`          | `-`                         | 由 `server.port` + hostname/UUID 自动生成（不可通过 YAML 配置）                                                            |
| `hotkey.soft-ttl-ms`                    | `0`                         | 软失效 TTL（毫秒），0 = 禁用                                                                                               |
| `hotkey.soft-expire-max-size`           | `50000`                     | 软失效时间表最大条目数                                                                                                     |
| `hotkey.soft-expire-ttl-minutes`        | `60`                        | 软失效时间表内部条目 TTL（分钟）                                                                                           |
| `hotkey.refresh-concurrency`            | `100`                       | 异步刷新最大并发数                                                                                                         |
| `hotkey.version-key-ttl-minutes`        | `60`                        | Redis 版本 key 过期时间（分钟），0 = 永不过期                                                                              |
| `hotkey.local-cache-access-ttl-minutes` | `0`                         | Caffeine L1 访问过期时间（分钟），0 = 禁用。作为写过期的补充策略                                                           |
| `hotkey.broadcast.concurrent-consumers` | `3`                         | 广播队列并发消费者数                                                                                                       |
| `hotkey.broadcast.scheduler-pool-size`  | `4`                         | 异步广播抖动延迟调度线程池大小                                                                                             |
| `hotkey.broadcast.warmup-jitter-ms`     | `100`                       | 广播消息处理前的随机抖动（毫秒），防止惊群效应                                                                             |
| `hotkey.scheduling.enabled`             | `true`                      | 启用内部定时调度（HeavyKeeper 衰减 + 被挤出队列清理）。如果使用自定义 `@EnableScheduling` 或不需要定时衰减，可设为 `false` |

## 模块

| 模块                   | 依赖                                                          | 自动配置                                                |
| ---------------------- | ------------------------------------------------------------- | ------------------------------------------------------- |
| `algorithm`            | 无                                                            | 始终生效                                                |
| `cache` (Redis)        | `spring-boot-starter-data-redis`                              | `@ConditionalOnClass`                                   |
| `broadcast` (RabbitMQ) | `spring-boot-starter-amqp` + `spring-boot-starter-data-redis` | `@ConditionalOnClass` + 属性开关                        |
| `actuator`             | `spring-boot-starter-actuator`                                | `@ConditionalOnClass`                                   |
| `scheduling`           | 无                                                            | 始终生效（通过 `hotkey.scheduling.enabled=false` 关闭） |

## 许可证

Apache License 2.0
