# Zeta 注解集成

基于 Spring 标准 `@Cacheable` / `@CachePut` / `@CacheEvict` 的注解驱动入口，由 Zeta 伴生注解扩展。启用方式：

```yaml
zeta:
  spring-cache:
    enabled: true
```

并在配置类上添加 `@EnableCaching`（必需——没有它 Spring 的 `CacheInterceptor` 不会注册，`@Cacheable` 方法会执行但结果不会被缓存）。

三层模型的设计原理见 [ADR-0023](adr/0023-annotation-three-layer-responsibility-model.md)。

---

## 1. 架构：三层职责 + 单一策略对象

```
@Cacheable 方法
   │
   ▼
CacheExtensionAspect      决定方法体"是否"执行：
   │                      @Intercept（触发→兜底）、@Fallback（异常）、
   │                      @Preload（探测器注热）、组合校验
   │ 构建 ──► GetOptions（不可变：惰性 TTL）+ skipSync 标志
   ▼
ZetaCacheContext          仅做运输 —— 单个 ThreadLocal<Snapshot{GetOptions, skipSync}>
   ▼
ZetaSpringCache           决定结果"如何"存储：
   │                      TTL 路由、@CacheCondition 入库前否决、
   │                      put/evict 的同步路由（skipSync）
   ▼
Zeta / HotKeyCache
```

`GetOptions` 的 TTL supplier **每次缓存调用最多求值一次**，且仅在 miss / 提升 / 刷新时求值——普通缓存命中从不求值，因此 SpEL TTL 表达式在命中路径上零开销。

---

## 2. 注解参考

| 注解              | 目标    | 生效操作     | 说明                                                                                                                                                                                                                                                                                                                    |
| ----------------- | ------- | ------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `@CacheTtl`       | 方法/类 | `@Cacheable` | 覆盖硬/软 TTL。支持静态值与 SpEL（`hardTtlSpEl`、`softTtlSpEl`）。SpEL 每次调用最多求值一次，仅在 miss/提升/刷新时。类级注解作为所有方法的回退                                                                                                                                                                          |
| `@Intercept`      | 方法    | `@Cacheable` | 按触发模式（`IS_LOCAL_HOT` / `FORCE` / `QPS` / `CONCURRENT_THREADS`）跳过方法体。模式专属配置通过嵌套注解：`@Intercept.QpsConfig`（`threshold`、`blockDurationMs`）和 `@Intercept.ConcurrentConfig`（`threshold`）。兜底优先级：`@Intercept.fallback()` → `@Fallback` → `peek()`。拦截时递增本地探测器（不上报 Worker） |
| `@Fallback`       | 方法    | `@Cacheable` | 拦截/异常时的兜底值（SpEL）或命名约定方法（`{方法名}Fallback`）；黑名单 BLOCK 永不进兜底，直接抛给调用方                                                                                                                                                                                                                |
| `@SkipSync`       | 方法    | 全部三种     | 禁止跨实例同步消息（仅本地写/失效）。null 永不缓存——没有 null 缓存开关（v2 契约）                                                                                                                                                                                                                                       |
| `@Preload`        | 方法    | `@Cacheable` | 预膨胀已知热 key 的 HeavyKeeper 计数（静态 `keys[]` 或动态 `keyExpr` SpEL）                                                                                                                                                                                                                                             |
| `@CacheCondition` | 方法    | `@Cacheable` | SpEL `unless`，**入库前否决语义**——条件成立时结果不写入 L1、不发集群广播；表达式仅在 miss 回填时求值一次，命中路径不再评估                                                                                                                                                                                              |
| `@Tag`            | 方法    | 任意         | 将 SpEL 解析的 key 送入检测/上报，不做缓存查找。`skipDetection` / `skipReport` 分别抑制一侧；`cacheName` 对齐 `@Cacheable` 命名空间                                                                                                                                                                                     |

---

## 3. 操作适用矩阵

| 注解              | `@Cacheable` | `@CachePut`        | `@CacheEvict`      |
| ----------------- | ------------ | ------------------ | ------------------ |
| `@CacheTtl`       | ✅ TTL 覆盖  | ⚠️ 忽略（R2 警告） | ⚠️ 忽略（R2 警告） |
| `@Intercept`      | ✅           | ⚠️ 忽略（R2 警告） | ⚠️ 忽略（R2 警告） |
| `@Fallback`       | ✅           | ⚠️ 忽略（R2 警告） | ⚠️ 忽略（R2 警告） |
| `@SkipSync`       | ✅           | ✅                 | ✅                 |
| `@Preload`        | ✅           | ⚠️ 忽略（R2 警告） | ⚠️ 忽略（R2 警告） |
| `@CacheCondition` | ✅           | ⚠️ 忽略（R2 警告） | ⚠️ 忽略（R2 警告） |

"忽略"指：该注解今天是静默 no-op，切面会记录一次性 WARN（规则 R2）让错误可见。

**注解读路径上的检测（所有 `@Cacheable` 路由）：** 每次 `@Cacheable` 读取都计入热 key 检测，恪守"每次读取同时触发检测与上报"的核心不变量。非 sync 路由（`cache.get(key)` → `lookup`）经由单次规则感知的 `peekAndTag` 查找（每次读一次哈希查找 + 一次规则评估）服务读取——命中或未命中皆如此——并在每次服务时同时馈入本地 HeavyKeeper 检测器与 Worker 上报器（白名单命中只检测不上报，阻断键抛出异常）；sync 路由（`get(key, Callable)`）进入带检测的 compute 路径。未命中时执行方法体，结果经**本地读回填**入库（`populateFromReadMiss`：只读版本探测，不做 Redis INCR，不做集群广播，不延迟到事务提交——读绝不伪装成集群写；回填本身不再计数，因此一次逻辑读恰好计数一次）。只有 `@CachePut` 走写透广播路径。非 sync 命中仅从 L1 服务是有意为之：lookup 绝不调用 loader。

**读回填与版本比较：** 回填的版本守卫与带 loader 的写路径共用同一层信封——未打戳的探测（fail-open，例如 Redis 不可达）不构成任何版本声明，因此既不会在与既有条目的比较中胜出或落败，也不会把对方的版本改写为默认值。回填被拒绝当且仅当：探测已打戳、既有条目尚未越过硬 TTL、且既有条目持有的版本至少同样新（相等则应用，ADR-0066）。实际效果：Redis 抖动不再静默丢弃那些已有带版本条目的 key 的 `@Cacheable` 未命中回填，且硬过期条目总能被替换。被拒绝的回填不写入任何内容，因此也不会作废任何 SingleFlight 加载结果——只有值真正落地时才触发 SingleFlight 失效。

**`@CachePut` 跨实例语义：** `@CachePut` 写透（`putThrough`）并广播 REFRESH 同步消息。Zeta 自身从不向 Redis 的缓存键命名空间写值，因此在默认装配下接收方无法加载新值——REFRESH 处理器回退为**本地失效**（ADR-0031）：对端副本被逐出，下次读经应用 reader 重新加载。若集成方自行在 Redis 中维护该键的值，则走推送路径。本地写请使用 `@SkipSync`。

---

## 4. 交互规则

### 4.1 `@Intercept` × `@Preload`

`@Intercept(IS_LOCAL_HOT)` 在本地 TopK 判定 key 为热时拦截。拦截时切面调用 `zeta.notifyLocalDetector(key)`——本地 HeavyKeeper 递增，**不**上报 Worker——key 的热度随访问自然维持，不会振荡（拦截 → 衰减 → 不热 → 执行 → 变热 → …）。`@Preload` 仍可用于在自然流量积累前让 key 提前变热，但不再是稳定性的必要条件。

### 4.2 `@Intercept(FORCE)` × 存储类注解

`FORCE` 在 `proceed()` 之前返回，Spring 的 `CacheInterceptor` 永不执行，**缓存永不写入**。同方法上的 `@CacheTtl`、`@CacheCondition` 均为 no-op（R1 警告）。

### 4.3 `@Tag` × `@Cacheable`

同方法共存 → 每次调用 key 被计数两次（tag 路径一次、缓存读路径一次）——热度膨胀 ×2（R3 警告）。若想刻意为 `@Cacheable` 的 key 加热，应移除 `@Tag`，或以明确目的使用 `@Tag(value=..., cacheName="<相同缓存名>", skipReport=true)`。`@Tag(cacheName)` 按与 `@Cacheable` 相同的方式拼接前缀（`cacheName + keySeparator + key`）；不设置时 `@Tag` 工作在裸 key 命名空间。

### 4.4 `@CacheCondition` × Spring `unless=`

两者同时存在时都会被评估（R4 警告），现语义已对齐：

|                    | Spring `@Cacheable(unless=...)` | Zeta `@CacheCondition(unless=...)`                             |
| ------------------ | ------------------------------- | -------------------------------------------------------------- |
| 否决时机           | 方法执行后、**入库前**          | 方法执行后、**入库前**（已对齐）                               |
| 对已缓存旧值       | 保留（下次命中继续被读到）      | 保留（下次命中继续被读到）                                     |
| 缓存命中时是否评估 | 从不                            | 从不——表达式仅在 miss 回填入库前求值一次，命中路径不再重复求值 |
| 否决时的集群广播   | 不适用（从未入库）              | 无——没有入库，也就无需失效                                     |

否决谓词经由 `ZetaCacheContext` 从切面下发到 `ZetaSpringCache`，在 L1 写入
之前执行：被否决的结果不写条目、不发广播、不存在脏读窗口（取代早期
"先入库再清除"的 purge 设计及其有界竞态窗口，ADR-0013）。

### 4.5 嵌套 `@Cacheable` 调用

切面无条件推送解析后的 `GetOptions`（连同 skipSync 标志），并在 `finally` 中恢复前一个快照，因此内层缓存方法绝不会看到外层方法的策略。运输层绑定线程：**不要**与 `@Async` 或任何在切面与缓存适配器之间跨线程的模式联用。

**loader 线程亲和性：** 缓存未命中时，方法体在 SingleFlight 执行器线程（`hotKeyExecutor`）上执行，而非调用方线程——与直接 `get()` 路径一致（ADR-0030）。调用方准备的 ThreadLocal 状态（请求上下文、事务、安全上下文）在未命中时**不可见**于方法体内；loader 不得依赖调用方线程亲和性。命中时方法体根本不会执行。

---

## 5. TTL 优先级链

对一次读操作：

1. **NORMAL 条目创建**——正的 `@CacheTtl`（静态或 SpEL）覆盖优先生效；为 0 时用配置默认值（`zeta.local.*`）。
2. **提升 HOT / HOT 续期**——有效热 TTL = `max(覆盖值, 热默认值)`：覆盖只能抬高下限，提升永不缩短条目寿命。普通命中时覆盖 supplier 不求值，提升使用配置的热默认值。
3. **软过期（stale-while-revalidate）**——仅由全局配置决定。注解 `sync=true` 读路径经软过期入口进入，功能全局关闭时自动降级；默认非 sync 路径经带检测的 peek（`peekAndTag`，含过期检查）服务，无后台刷新。TTL 覆盖不再对其构成门槛。
4. **TTL 抖动**——默认 ±5%（`zeta.local.ttl-jitter-ratio`），作用于所有过期时间戳。

---

## 6. null 语义（v2 契约：null 永不缓存）

- `null` 方法结果在任何路径上都不写入条目（loader miss、`@CachePut` 返回 null、非 sync `@Cacheable` 入库、显式写入 Spring `NullValue`——Spring 自带的 `NullValue` 标记永远不会被持久化）。下次读重新执行方法。
- 没有 null 缓存开关：原 `@NullCaching` 注解与短 TTL null 哨兵已移除（v2 null 契约）。需要防穿透的调用方在自己的 loader 中实现（占位值 + `shouldCache`）。
- `ZetaSpringCache` 的 `allowNullValues` 恒被强制为 `false`，与配置无关。
- **sync 读失败语义（`sync = true`）**：下发的策略被强制为快速失败——抛异常的方法体表现为 Spring 的 `ValueRetrievalException`（永不吞成 `null` miss），被抑制的加载（熔断关闭时的超时/执行器饱和）表现为包裹 `ZetaSuppressedException` 的 `ValueRetrievalException`。熔断打开且无可服务的 stale 条目时仍返回 `null`（优雅降级优先于快速失败）。

---

## 7. 校验器警告（每方法一次）

| 规则 | 触发                                                | 含义                                    |
| ---- | --------------------------------------------------- | --------------------------------------- |
| R1   | `@Intercept(FORCE)` + `@CacheTtl`/`@CacheCondition` | FORCE 下存储类注解为 no-op              |
| R2   | `@CachePut`/`@CacheEvict` 上的读路径注解            | 那里只有 `@SkipSync` 生效               |
| R3   | 同方法 `@Tag` + `@Cacheable`                        | key 在 HeavyKeeper 中被双重计数         |
| R4   | 同方法 `@CacheCondition` + Spring `unless=`         | 双重评估（均为入库前否决）              |
| R5   | `@Intercept(blockDurationMs=...)` 且 `type` ≠ `QPS` | blockDurationMs 在非 QPS 模式下静默忽略 |

---

## 8. 示例

```java
// 静态 TTL + 热 key 拦截 + 命名约定兜底
@Cacheable("products")
@CacheTtl(hardTtlMs = 60_000, softTtlMs = 10_000)
@Intercept @Fallback
public Product getProduct(String id) { ... }

// SpEL 动态 TTL（仅在 miss/提升/刷新时求值）
@Cacheable("users")
@CacheTtl(hardTtlSpEl = "#id.startsWith('vip') ? 600000 : 60000")
public User findUser(String id) { ... }

// QPS 限流 + SpEL 兜底（嵌套 @Intercept.QpsConfig）
@Cacheable("orders")
@Intercept(type = InterceptType.QPS, qps = @Intercept.QpsConfig(threshold = 500), fallback = "'throttled'")
public Order getOrder(String id) { ... }

// QPS 限流 + 冷却封锁（超限后冷却 5s）
@Cacheable("flash-sale")
@Intercept(type = InterceptType.QPS, qps = @Intercept.QpsConfig(threshold = 100, blockDurationMs = 5000))
public Item getFlashItem(String id) { ... }

// 并发守卫（嵌套 @Intercept.ConcurrentConfig）
@Cacheable("reports")
@Intercept(type = InterceptType.CONCURRENT_THREADS, concurrent = @Intercept.ConcurrentConfig(threshold = 10), fallback = "'busy'")
public Report getReport(String id) { ... }

// 预加载秒杀热 key（一开始就稳定为热）
@Cacheable("items")
@Preload(keys = {"item-001", "item-002"})
public Item getItem(String id) { ... }

// 结果被禁用时清除
@Cacheable("configs")
@CacheCondition(unless = "#result == null || #result.disabled()")
public Config getConfig(String key) { ... }

// 仅本地写（无跨实例同步）
@CachePut("sessions")
@SkipSync
public Session updateSession(Session s) { ... }

// 在 @Cacheable 命名空间中标记 key，不做缓存查找
@Tag(value = "#id", cacheName = "products", skipReport = true)
public void touchProduct(String id) { ... }
```
