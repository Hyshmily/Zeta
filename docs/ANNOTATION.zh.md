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
   │ 构建 ──► CachePolicy（不可变：惰性 TTL、nullCaching、skipBroadcast）
   ▼
ZetaCacheContext          仅做运输 —— 单个 ThreadLocal<CachePolicy>
   ▼
ZetaSpringCache           决定结果"如何"存储：
   │                      TTL 路由、null 哨兵决策、@CacheCondition 清除、
   │                      put/evict 的广播路由
   ▼
Zeta / HotKeyCache
```

`CachePolicy` 的 TTL supplier **每次缓存调用最多求值一次**，且仅在 miss / 提升 / 刷新时求值——普通缓存命中从不求值，因此 SpEL TTL 表达式在命中路径上零开销。

---

## 2. 注解参考

| 注解 | 目标 | 生效操作 | 说明 |
| --- | --- | --- | --- |
| `@CacheTTL` | 方法/类 | `@Cacheable` | 覆盖硬/软 TTL。支持静态值与 SpEL（`hardTtlSpEl`、`softTtlSpEl`）。SpEL 每次调用最多求值一次，仅在 miss/提升/刷新时。类级注解作为所有方法的回退 |
| `@Intercept` | 方法 | `@Cacheable` | 按触发模式（`IS_LOCAL_HOT` / `FORCE` / `QPS` / `CONCURRENT_THREADS`）跳过方法体；兜底优先级：`@Intercept.fallback()` → `@Fallback` → `peek()`。拦截时递增本地探测器（不上报 Worker） |
| `@Fallback` | 方法 | `@Cacheable` | 被封锁/拦截/异常时的兜底值（SpEL）或命名约定方法（`{方法名}Fallback`） |
| `@NullCaching` | 方法 | `@Cacheable` | **opt-out**：null 结果默认缓存（短 TTL 哨兵，防穿透）；`@NullCaching(false)` 禁止该方法的 null 缓存 |
| `@SkipBroadcast` | 方法 | 全部三种 | 禁止跨实例 AMQP 同步消息（仅本地写/失效，`@CacheCondition` 清除也仅本地） |
| `@Preload` | 方法 | `@Cacheable` | 预膨胀已知热 key 的 HeavyKeeper 计数（静态 `keys[]` 或动态 `keyExpr` SpEL） |
| `@CacheCondition` | 方法 | `@Cacheable` | SpEL `unless`，**清除语义**——条件成立时结果不保留，且已存在的条目被主动失效（除非 `@SkipBroadcast`，否则广播） |
| `@Tag` | 方法 | 任意 | 将 SpEL 解析的 key 送入检测/上报，不做缓存查找。`skipDetection` / `skipReport` 分别抑制一侧；`cacheName` 对齐 `@Cacheable` 命名空间 |

---

## 3. 操作适用矩阵

| 注解 | `@Cacheable` | `@CachePut` | `@CacheEvict` |
| --- | --- | --- | --- |
| `@CacheTTL` | ✅ TTL 覆盖 | ⚠️ 忽略（R2 警告） | ⚠️ 忽略（R2 警告） |
| `@Intercept` | ✅ | ⚠️ 忽略（R2 警告） | ⚠️ 忽略（R2 警告） |
| `@Fallback` | ✅ | ⚠️ 忽略（R2 警告） | ⚠️ 忽略（R2 警告） |
| `@NullCaching` | ✅ | ⚠️ 忽略（R2 警告） | ⚠️ 忽略（R2 警告） |
| `@SkipBroadcast` | ✅ | ✅ | ✅ |
| `@Preload` | ✅ | ⚠️ 忽略（R2 警告） | ⚠️ 忽略（R2 警告） |
| `@CacheCondition` | ✅ | ⚠️ 忽略（R2 警告） | ⚠️ 忽略（R2 警告） |

"忽略"指：该注解今天是静默 no-op，切面会记录一次性 WARN（规则 R2）让错误可见。

---

## 4. 交互规则

### 4.1 `@Intercept` × `@Preload`

`@Intercept(IS_LOCAL_HOT)` 在本地 TopK 判定 key 为热时拦截。拦截时切面调用 `zeta.notifyLocalDetector(key)`——本地 HeavyKeeper 递增，**不**上报 Worker——key 的热度随访问自然维持，不会振荡（拦截 → 衰减 → 不热 → 执行 → 变热 → …）。`@Preload` 仍可用于在自然流量积累前让 key 提前变热，但不再是稳定性的必要条件。

### 4.2 `@Intercept(FORCE)` × 存储类注解

`FORCE` 在 `proceed()` 之前返回，Spring 的 `CacheInterceptor` 永不执行，**缓存永不写入**。同方法上的 `@CacheTTL`、`@NullCaching`、`@CacheCondition` 均为 no-op（R1 警告）。

### 4.3 `@Tag` × `@Cacheable`

同方法共存 → 每次调用 key 被计数两次（tag 路径一次、缓存读路径一次）——热度膨胀 ×2（R3 警告）。若想刻意为 `@Cacheable` 的 key 加热，应移除 `@Tag`，或以明确目的使用 `@Tag(value=..., cacheName="<相同缓存名>", skipReport=true)`。`@Tag(cacheName)` 按与 `@Cacheable` 相同的方式拼接前缀（`cacheName + keySeparator + key`）；不设置时 `@Tag` 工作在裸 key 命名空间。

### 4.4 `@CacheCondition` × Spring `unless=`

两者同时存在时都会被评估（R4 警告），语义不同：

| | Spring `@Cacheable(unless=...)` | Zeta `@CacheCondition(unless=...)` |
| --- | --- | --- |
| 否决时机 | 方法执行后、**入库前** | 入库后、再失效 |
| 对已缓存旧值 | 保留（下次命中继续被读到） | **主动失效**（purge） |
| 缓存命中时是否评估 | 从不 | 是——`#result` 为缓存值 |
| 失效是否广播 | 不适用（从未入库） | 除非 `@SkipBroadcast`，否则广播 |

先存后删的竞态窗口有界，按 ADR-0013 予以接受。

### 4.5 嵌套 `@Cacheable` 调用

切面无条件推送解析后的 `CachePolicy`，并在 `finally` 中恢复前一个，因此内层缓存方法绝不会看到外层方法的策略。运输层绑定线程：**不要**与 `@Async` 或任何在切面与缓存适配器之间跨线程的模式联用。

---

## 5. TTL 优先级链

对一次读操作：

1. **NORMAL 条目创建**——正的 `@CacheTTL`（静态或 SpEL）覆盖优先生效；为 0 时用配置默认值（`zeta.local.*`）。
2. **提升 HOT / HOT 续期**——有效热 TTL = `max(覆盖值, 热默认值)`：覆盖只能抬高下限，提升永不缩短条目寿命。普通命中时覆盖 supplier 不求值，提升使用配置的热默认值。
3. **软过期（stale-while-revalidate）**——仅由全局配置决定。注解读路径始终经软过期入口进入，功能全局关闭时自动降级。TTL 覆盖不再对其构成门槛。
4. **TTL 抖动**——默认 ±5%（`zeta.local.ttl-jitter-ratio`），作用于所有过期时间戳。

---

## 6. null 缓存语义

- **默认（无注解或 `@NullCaching(true)`）**：`null` 结果以内部 `NullValue` 哨兵存储，TTL 为 `zeta.local.null-value-ttl-seconds`（短 TTL）。命中有效哨兵返回 `null` 且**不再调用方法体**；该访问仍计入热 key 检测。
- **`@NullCaching(false)`**：`null` 结果不留条目；下次调用重新执行方法。
- 三条读路径（`get`、`getWithSoftExpire`、`computeIfAbsent[WithSoftExpire]`）与 fluent API（`read(key).notAllowNull()`）语义一致。

---

## 7. 校验器警告（每方法一次）

| 规则 | 触发 | 含义 |
| --- | --- | --- |
| R1 | `@Intercept(FORCE)` + `@CacheTTL`/`@NullCaching`/`@CacheCondition` | FORCE 下存储类注解为 no-op |
| R2 | `@CachePut`/`@CacheEvict` 上的读路径注解 | 那里只有 `@SkipBroadcast` 生效 |
| R3 | 同方法 `@Tag` + `@Cacheable` | key 在 HeavyKeeper 中被双重计数 |
| R4 | 同方法 `@CacheCondition` + Spring `unless=` | 双重评估，语义不同 |

---

## 8. 示例

```java
// 静态 TTL + 热 key 拦截 + 命名约定兜底
@Cacheable("products")
@CacheTTL(hardTtlMs = 60_000, softTtlMs = 10_000)
@Intercept @Fallback
public Product getProduct(String id) { ... }

// SpEL 动态 TTL（仅在 miss/提升/刷新时求值）
@Cacheable("users")
@CacheTTL(hardTtlSpEl = "#id.startsWith('vip') ? 600000 : 60000")
public User findUser(String id) { ... }

// QPS 限流 + SpEL 兜底
@Cacheable("orders")
@Intercept(type = InterceptType.QPS, qps = 500, fallback = "'throttled'")
public Order getOrder(String id) { ... }

// 并发守卫
@Cacheable("reports")
@Intercept(type = InterceptType.CONCURRENT_THREADS, concurrentThreads = 10, fallback = "'busy'")
public Report getReport(String id) { ... }

// 预加载秒杀热 key（一开始就稳定为热）
@Cacheable("items")
@Preload(keys = {"item-001", "item-002"})
public Item getItem(String id) { ... }

// 结果被禁用时清除
@Cacheable("configs")
@CacheCondition(unless = "#result == null || #result.disabled()")
public Config getConfig(String key) { ... }

// 显式拒绝缓存 null
@Cacheable("maybe")
@NullCaching(false)
public Data findMaybe(String id) { ... }

// 仅本地写（无 AMQP 同步）
@CachePut("sessions")
@SkipBroadcast
public Session updateSession(Session s) { ... }

// 在 @Cacheable 命名空间中标记 key，不做缓存查找
@Tag(value = "#id", cacheName = "products", skipReport = true)
public void touchProduct(String id) { ... }
```
