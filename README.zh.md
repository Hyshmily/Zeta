# HotKey

[**English**](README.md)

HotKey — HeavyKeeper Top-K 热点检测 + 多级缓存自动预热 + 分布式广播 Spring Boot Starter

HotKey 不是一个通用的本地缓存——它是一个带有可选分布式广播的轻量热点 key 自动检测与多级缓存预热框架。

大多数本地缓存在 Caffeine 里存每一个访问过的 key。数据量小时没问题，但在海量 key 场景下：

- **内存浪费** — 绝大多数 key 只读一次就再也不会访问
- **广播风暴** — 全量缓存要求全量失效，广播量随 key 数线性增长

而HotKey却 **只缓存真正热门的 key。**

通过 [HeavyKeeper](https://github.com/go-kratos/aegis)（Count-Min Sketch 变体）概率检测访问频率。只有进入 Top-K 集合的 key 才会被自动提升到本地 Caffeine L1，并通过可选的 RabbitMQ fanout 跨实例同步。非热点 key 的读取通过 `Supplier<T>` 回调委托给调用方——框架对你的数据源不做任何假设。

### 适用场景

| 适合                                           | 不适合                                |
| ---------------------------------------------- | ------------------------------------- |
| 读密集型场景（String / List / Set / ZSet）     | 写密集型 / 原子操作（秒杀 Lua）       |
| 大量 key，访问呈帕累托分布                     | key 数少（< 200），手动 Caffeine 即可 |
| 读多写少，最终一致即可                         | 要求写后立即可见                      |
| Spring Boot 3.x + Java 17+                     | 非 Spring Boot 项目                   |
| 可选 Redis（默认 L2）+ 可选 RabbitMQ（多实例） |

> [!Important]
> 这是作者在开发过程中总结的经验模块，不能确保生产过程中的可靠性与稳定性。如需完善的热点 key 自动监测和更高精度版本，请参考 [hotkey](https://gitee.com/jd-platform-opensource/hotkey)

## 1.0.6 更新内容

- **版本化缓存失效** — 用 Redis INCR 全局版本号替代广播驱动的缓存失效。`putThrough` 写入 L2、更新本地缓存、附带单调递增版本的广播。对端节点先比较版本再决定是否回源，消除冗余 Redis 加载。
- **异常安全版本降级** — `nextVersion()` 捕获 Redis 异常后降级到 `System.nanoTime()`，写入操作不会被版本生成阻断。
- **版本 key TTL** — 新增 `hotkey.version-key-ttl-minutes`（默认 60 分钟），自动过期 Redis 版本号 key，防止版本键无限膨胀。
- **API 重命名** — `getStale` 更名为 `getRelaxed`，`putInvalidate` 更名为 `putBeforeInvalidate`。
- **`peek()` 版本感知** — 自动解包 `VersionedValue`，对调用方透明。

## 特点

- **HeavyKeeper 算法** — Count-Min Sketch + 指数冲突衰减的概率性 Top-K 检测
- **三级缓存** — Caffeine (L1) → Redis (L2，可选) → DB 回退，自动热点提升
- **请求合并** — L1 未命中时同 key 并发请求共享同一 Redis 读，通过 `Caffeine<key, CompletableFuture>`

  > **注意：** 确保 `hotkey.inflight-ttl-seconds` 大于最慢的 Redis 响应耗时，否则缓存条目可能在 Future 完成前过期，导致重复的 Redis 读。
  > 同时确保 `hotkey.inflight-timeout-seconds` < `hotkey.inflight-ttl-seconds`。超时后 `loadSingleflight` 返回 `Optional.empty()`，调用方需通过 DB 回退处理。

- **软失效** — 立即返回过期旧值，同时后台异步刷新；降低 p99 延迟，代价是短暂脏读
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

写路径（用户主动调用）：
`putThrough(cacheKey, value, writer)`
├─ `writer.run()` — L2 写入（调用方传入的 Runnable）
├─ `nextVersion(cacheKey)` — Redis INCR → 单调递增版本号
├─ Caffeine.put(cacheKey, VersionedValue(value, version))
└─ RabbitMQ fanout 广播 + version header（如启用）

对于集合增量操作（LPUSH、SADD、ZADD）：
`putBeforeInvalidate(cacheKey, mutation)`
├─ `mutation.run()` — L2 写入（调用方传入的 Runnable）
├─ `nextVersion(cacheKey)` — Redis INCR → 单调递增版本号
├─ Caffeine 本地缓存**失效**
└─ RabbitMQ fanout 广播 + version header（如启用）

软失效读路径（`getStale`）：

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
           loadSingleflight(cacheKey, redisReader)
           (参见上方正常读路径)
```

## 降级

HotKey 通过 `supplier` 回调形成三级降级链路：

```
hotKey.get(key, supplier)
  ├─ L1(Caffeine) 命中 → 直接返回
  ├─ L1 未命中 → supplier()
  │    ├─ 返回数据 → 热 key？ → 写 L1 + 返回
  │    ├─ 返回 null → Optional.empty() → 调用方 orElseGet/orElseThrow
  │    └─ 抛出异常 → loadSingleflight 捕获 → Optional.empty() → 调用方兜底
  └─ HotKey 自身异常 → 跳过 L1，直接调用 supplier
```

各组件故障表现：

| 故障组件               | 影响                            | 恢复方式              |
| ---------------------- | ------------------------------- | --------------------- |
| HotKey 自身            | 跳过 L1，回退到 supplier        | 应用重启              |
| L2 后端 (Redis/DB/API) | 每次请求穿透到调用方兜底        | 后端恢复后自动恢复    |
| L1 Caffeine OOM / 驱逐 | 单 key 被驱逐，下次读取重新回源 | 自动（Caffeine 内部） |

> 调用方始终需要处理 `Optional.empty()` — HotKey 不会隐藏后端故障。

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
    <groupId>com.github.hyshmily</groupId>
    <artifactId>hotkey</artifactId>
    <version>1.0.6</version>
</dependency>
```

版本号使用 Git tag（例如 `1.0.6`）。Redis 和 RabbitMQ 依赖均为可选——仅在使用对应功能时才需引入。

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
    instance-id: ${server.port}
```

### 3. 使用

> **注意：** 自v1.0.2起包含**破坏性变更** — `get(hk, fk)` 和 `putAndBroadcast(hk, fk, val)` 已移除。库已与 `RedisTemplate` 解耦，调用方通过 `Supplier<T>` / `Runnable` 自行提供读写回调。

**A. 纯本地缓存（无二级存储）**

```java
@Autowired
private HotKey hotKey;

Optional<String> r = hotKey.peek("user:123"); // 仅 Caffeine L1 + 热点检测
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

`putThrough` 需要传入完整新值来更新 L1，但集合的增量操作（LPUSH、SADD、ZADD）只修改单个元素——调用方无法获知全量新值。使用 `putInvalidate` 在突变后失效 L1，下次 `get()` 自动回源 Redis，保证一致性。

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

**G. 软失效 + Singleflight 回源**

软失效立即返回已过期的旧值同时后台异步刷新。适用于允许短暂脏读以换取更低延迟的场景。

```java
Optional<String> r = hotKey.getRelaxed("user:123", () -> redisTemplate.opsForValue().get("user:123"));
// L1 命中但已软过期 → 返回旧值 + 触发异步刷新
// L1 未命中 → singleflight 回源（同 get()）
```

配置示例：

```yaml
hotkey:
  soft-ttl-ms: 5000 # 启用软失效，5s 软 TTL
  refresh-concurrency: 50 # 限制异步刷新并发数
```

## HotKey 门面 API 参考

推荐入口是 `HotKey` 门面（已自动配置为 Spring Bean）。除了上面展示的 `get`/`peek`/`putThrough`/`putBeforeInvalidate` 之外，还提供：

| 方法                          | 说明                                    |
| ----------------------------- | --------------------------------------- |
| `isHotKey(cacheKey)`          | 检查 key 是否在当前的 Top-K 热点集合中  |
| `invalidateAll(cacheKeys...)` | 可变参数重载 — 批量失效多个 key         |
| `invalidateAll(Collection)`   | Collection 重载                         |
| `returnHotKeys()`             | Top-K 快照（key + 计数）                |
| `returnExpelledHotKeys()`     | 排出的热点 key 队列（最近被挤出 Top-K） |
| `returnTotalDataStreams()`    | 经过 HeavyKeeper 的累计读取数           |

## 广播

```yaml
hotkey:
  broadcast:
    enabled: true
```

每个实例声明独立队列（`hotkey.broadcast:<pod-id>`）绑定到 fanout 交换机。热点提升时广播 key 通知对端，对端首次收到后从 Redis 回读。失效广播立即清除本地缓存。

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

| 属性                                    | 默认值                                               | 说明                                                                                          |
| --------------------------------------- | ---------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| `hotkey.top-k`                          | `100`                                                | Top-K 集合大小                                                                                |
| `hotkey.width`                          | `50000`                                              | Count-Min Sketch 宽度                                                                         |
| `hotkey.depth`                          | `5`                                                  | Count-Min Sketch 深度（行数）                                                                 |
| `hotkey.decay`                          | `0.92`                                               | 冲突衰减系数                                                                                  |
| `hotkey.min-count`                      | `10`                                                 | 热点最小计数阈值                                                                              |
| `hotkey.local-cache-max-size`           | `1000`                                               | Caffeine L1 最大条目数                                                                        |
| `hotkey.local-cache-ttl-minutes`        | `5`                                                  | Caffeine L1 过期时间（分钟）                                                                  |
| `hotkey.inflight-max-size`              | `50000`                                              | 请求合并最大条目数                                                                            |
| `hotkey.inflight-ttl-seconds`           | `5`                                                  | 请求合并条目 TTL（需大于最慢 Redis 响应耗时）                                                 |
| `hotkey.inflight-timeout-seconds`       | `3`                                                  | 请求合并超时（秒），必须小于 inflight-ttl-seconds。超时返回 Optional.empty()，调用方应回退 DB |
| `hotkey.executor-core-pool-size`        | `8`                                                  | 线程池核心线程数                                                                              |
| `hotkey.executor-max-pool-size`         | `32`                                                 | 线程池最大线程数                                                                              |
| `hotkey.executor-queue-capacity`        | `2000`                                               | 线程池队列容量                                                                                |
| `hotkey.broadcast.enabled`              | `false`                                              | 启用 RabbitMQ 广播                                                                            |
| `hotkey.broadcast.exchange-name`        | `hotkey.broadcast.exchange`                          | Fanout 交换机名                                                                               |
| `hotkey.broadcast.queue-prefix`         | `hotkey.broadcast`                                   | 队列名前缀                                                                                    |
| `hotkey.broadcast.dedup-window-seconds` | `10`                                                 | 广播去重窗口（秒）                                                                            |
| `hotkey.broadcast.dedup-max-size`       | `10000`                                              | 广播去重最大条目数                                                                            |
| `hotkey.decay-period`                   | `20`                                                 | （已废弃）衰减周期（秒），仅做向后兼容                                                        |
| `hotkey.broadcast.instance-id`          | `${server.port:instance}-${HOSTNAME:${random.uuid}}` | 实例唯一标识                                                                                  |
| `hotkey.soft-ttl-ms`                    | `0`                                                  | 软失效 TTL（毫秒），0 = 禁用                                                                  |
| `hotkey.soft-expire-max-size`           | `50000`                                              | 软失效时间表最大条目数                                                                        |
| `hotkey.soft-expire-ttl-minutes`        | `60`                                                 | 软失效时间表内部条目 TTL（分钟）                                                              |
| `hotkey.refresh-concurrency`            | `100`                                                | 异步刷新最大并发数                                                                            |
| `hotkey.version-key-ttl-minutes`        | `60`                                                 | Redis 版本 key 过期时间（分钟），0 = 永不过期                                                 |

## 模块

| 模块                   | 依赖                             | 自动配置                         |
| ---------------------- | -------------------------------- | -------------------------------- |
| `algorithm`            | 无                               | 始终生效                         |
| `cache` (Redis)        | `spring-boot-starter-data-redis` | `@ConditionalOnClass`            |
| `broadcast` (RabbitMQ) | `spring-boot-starter-amqp`       | `@ConditionalOnClass` + 属性开关 |
| `actuator`             | `spring-boot-starter-actuator`   | `@ConditionalOnClass`            |

## 许可证

Apache License 2.0
