[← 返回首页](README.zh.md)

# HotKey 架构 — 完整参考

本文档整合了 HotKey 库的完整架构、方法调用链、Worker 模式和补充图表。

---

## 目录

- [架构概览](#架构概览)
- [方法调用链](#方法调用链)
- [Worker 模式](#worker-模式)
- [补充图表](#补充图表)
- [设计说明](#设计说明)

---

## 架构概览

### 正常读路径（`get`）

<!-- Source: HotKeyCache.java:138-162, loadAndCache:247-312 -->

```
╔══════════════════════════╗                           ╔══════════════════════════════╗
║   请求                   │ ──── record(key) ──────▶ ║  Caffeine L1 (本地)           ║
║                          │ ◀──── Optional.of(v) ─── ║  CacheEntry wrapper          ║
╚══════════════╤═══════════╝                           ╚══════════════╤═══════════════╝
               │ L1 命中：解包 + 上报                            │ isLocalHotKey()?
               │ (inflight 去重)                                 ▼
               │                                      ╔═══════════════════════════════╗
               │                                      ║   TopK (HeavyKeeper)          ║
               │                                      ║   add() → AddResult           ║
               │                                      ║   list() / listTopN(n)        ║
               ▼                                      ║   total() / contains()        ║
╔══════════════════════════╗  reader.run()            ║   expelled() / fading()       ║
║  L2 存储 (可插拔)         ║ ────────────────────▶   ╚══════════════╤════════════════╝
║  Redis / DB / 自定义      ║ ──── add(key,1) ──────▶                │
╚════════╤═══════╤═════════╝ ──── record(key) ─────▶                ▼
  命中   ▼       ▼ null                                ╔════════════════════════════╗
  ┌─────────┐  ┌─────────────┐                         ║  根据 KeyState 选择 TTL:   ║
  │Optional.│  │Optional.    │                         ║  HOT   → 热点 TTL          ║
  │ of(v)   │  │empty()→DB   │                         ║  other → 普通 TTL          ║
  └────┬────┘  └──────┬──────┘                         ╚══════════════╤═════════════╝
       └──────┬───────┘                                               │
              ▼                                                       ▼
╔══════════════════════════════════════╗  ┌──────────────────────────────────────────┐
║  SingleFlight.load(key, reader)      ║  │  Caffeine.put(key, CacheEntry {          ║
║  → supplier.get()                    ║  │    value, dataVersion=0L,                ║
║  → add(key,1) + record(key)          ║  │    isVersionDegraded=false,              ║
║  → Caffeine.put(key, entry)          ║  │    decisionVersion=0L,                   ║
║  (读路径不触发同步)                   ║  │    hardTtlMs, hardExpireAtMs,            ║
╚══════════════════════════════════════╝  │    softTtlMs, softExpireAtMs,            ║
                                          │    keyState,                             ║
                                          │    normalHardTtlMs, normalSoftTtlMs })   ║
                                          └──────────────────────────────────────────┘
```

> **注意：** `isLocalHotKey()` 检查 L1 中 key 的 `KeyState` 是否为 HOT。key 以 `CacheEntry` 包装形式存储，包含完整 TTL 元数据——`get()` 路径自动解包为原始值。

### 写路径 — `putThrough`

<!-- Source: HotKeyCache.java:375-416 -->

```
putThrough(cacheKey, value, writer[, hardTtlMs, softTtlMs])
│
├─ (Spring 事务内) → TransactionSupport.runAsyncAfterCommit(...)
│                    afterCommit 在 hotKeyExecutor 上异步执行下方步骤
│  (非事务)        → hotKeyExecutor.submit(() → 执行下方步骤)
│
├─ writer.run()  ──── L2 / DB 写入（调用方 Runnable）
│   └─ 异常时 → log.error，继续执行版本/L1/广播
│
├─ nextVersion(cacheKey)  ──── 版本生成
│   ├─ Redis 可用: Lua "INCR KEYS[1]; EXPIRE KEYS[1] ARGV[1]"
│   │              → VersionResult(dataVersion > 0, degraded = false)
│   └─ Redis 不可用: fallbackVersionCounter.incrementAndGet()
│                   → VersionResult(dataVersion = MIN_VALUE + counter,
│                                    degraded = true)
│
├─ caffeineCache.put(cacheKey, CacheEntry {
│      value,
│      dataVersion, isVersionDegraded,
│      decisionVersion = 0L,            ← 写穿透时始终重置
│      hardTtlMs,  hardExpireAtMs,
│      softTtlMs,  softExpireAtMs,
│      keyState = NORMAL,               ← 始终 NORMAL 写入
│      normalHardTtlMs, normalSoftTtlMs })
│
└─ CacheSyncPublisher.broadcastRefresh(key, version, degraded)
    └─ sendDeduped(key, "REFRESH", version, degraded)
        ├─ recentBroadcasts.compute("REFRESH:"+key, (_, old) →
        │     old != null && old > version ? old : version)
        │     └─ return != version → 已有更新版本，跳过本次发送
        └─ rabbitTemplate.send(exchange, "", msg)
              [header: type = REFRESH, version, isVersionDegraded]
```

**写路径——事务延迟：** `putThrough`、`putBeforeInvalidate`、`invalidate` 和 `invalidateAll` 在 Spring 事务内调用时都会延迟到 `afterCommit` 执行。

> **注意：** `putThrough` 在**事务外**的行为与其他写方法不同——它会在 `hotKeyExecutor` 上异步执行（调用方立即返回，writer、版本递增、L1 更新和同步在后台线程完成）。事务外，其他方法（`invalidate`、`invalidateAll`、`putBeforeInvalidate`）在调用方线程上同步执行。

### 集合写入路径 — `putBeforeInvalidate`

<!-- Source: HotKeyCache.java:422-446 -->

用于集合增量操作（LPUSH、SADD、ZADD）：

```
putBeforeInvalidate(cacheKey, mutation)
│
├─ (Spring 事务内) → TransactionSupport.runNowOrAfterCommit(...)
│  (非事务)        → 调用方线程同步执行
│
├─ mutation.run()  ──── L2 / DB 增量写入（调用方 Runnable）
│   └─ 异常时 → log.error，跳过 L1 失效和广播
│
├─ nextVersion(cacheKey)  ──── 同 putThrough
│
├─ caffeineCache.invalidate(cacheKey)  ──── L1 移除（非 put）
│
└─ CacheSyncPublisher.broadcastLocalInvalidate(key, version, degraded)
    └─ sendDeduped(key, "INVALIDATE", version, degraded)
        └─ rabbitTemplate.send(...)
              [header: type = INVALIDATE, version, isVersionDegraded]
```

> **注意：** `mutation.run()` 与 L1 缓存失效之间存在约 1ms 窗口，并发 `get()` 可能命中 L1 旧值。这是刻意的取舍——在修改前失效会导致更严重的竞态（`get()` 用旧 Redis 数据重新填充 L1）。该窗口受限于一次 Redis 往返（`nextVersion` 调用）。

### 失效路径

<!-- Source: HotKeyCache.java:321-358, CacheSyncListener.java:188-230, VersionGuard.java:73-98 -->

```
invalidate(cacheKey)
│
├─ invalidCacheKey(key) == true → log.debug + return
│
├─ TransactionSupport.runNowOrAfterCommit(...)
│   ├─ (事务内) → afterCommit 延迟
│   └─ (非事务) → 同步
│
├─ nextVersion(key)  ──── Redis INCR → VersionResult(dataVersion)
│
├─ caffeineCache.invalidate(key)  ──── L1 移除
│
└─ CacheSyncPublisher.broadcastRefresh(key, version, degraded)
    └─ TYPE_REFRESH（对端从 Redis 重新加载，跳过旧版本）


invalidateAll(cacheKeys)
│
├─ stream().filter(k → !invalidCacheKey(k)).toList()
│   └─ 空列表 → log.debug + return
│
├─ TransactionSupport.runNowOrAfterCommit(...)
│
├─ caffeineCache.invalidateAll(validKeys)  ──── L1 批量移除
│
└─ (有 syncPublisher)
    └─ CacheSyncPublisher.broadcastLocalInvalidateAll(validKeys)
        └─ 单条 AMQP 消息
              body = JSON 数组 [key1, key2, ...]
              header: type = INVALIDATE_ALL
              ⚠ 无 INCR、无版本检查
```

### 软过期读路径（`getWithSoftExpire`）

<!-- Source: HotKeyCache.java:189-236, CacheExpireManager.java -->

```
                           L1 命中
┌──────────────┐  ────────────────────────────────▶  ┌──────────────────────┐
│   请求       │                                      │  CacheEntry          │
│              │ ◀─────────────────────────────────  │  value, keyState,    │
└──────┬───────┘    Optional.of(stale value)          │  hardTtlMs, softTtlMs│
       │ L1 未命中                                    └──────────┬───────────┘
       │                                                        │
       │                                          ┌─────────────┴────────────┐
       │                                          │ 已逻辑过期？              │
       │                                          │  (hardExpireAtMs)        │
       │                                          └──────┬──────────┬────────┘
       │                                          true   ▼          ▼ false
       │                                    invalidate + ┌────────────────────┐
       │                                    return empty │ KeyState ==        │
       │                                                 │ HOT 或 COOL?       │
       │                                                 └──┬─────────────┬───┘
       │                                                no  ▼             ▼ yes
       │                                            return +       软过期？
       │                                            record(key,1)         │
       │                                                             true ▼
       │                                                 触发异步刷新:
       │                                                         ├─ refreshLimiter
       │                                                         │  .tryAcquire()
       │                                                         │  └─ 繁忙 → 跳过
       │                                                         └─ executor.submit:
       │                                                              L2 reader → Caffeine.put
       │                                                              + 更新 softExpireAtMs
       │                                                              + 保留原始 hardTtlMs
       │
       │ L1 未命中 → SingleFlight.load(cacheKey, reader)
       │              └─ Caffeine.put(key, CacheEntry(
       │                   value, 0L, false, 0L,
       │                   hardTtlMs, hardExpireAtMs,
       │                   softTtlMs, softExpireAtMs,
       │                   keyState, normalHardTtlMs, normalSoftTtlMs))
```

> **注意：** 软过期适用于 HOT 和 COOL 条目。NORMAL 条目总是直接返回，不会触发异步刷新——它们会在硬 TTL 过期后通过 `loadAndCache` 重新加载。如需纯粹的逻辑过期（硬 TTL 永不淘汰），向 `getWithSoftExpire` 传入 `hardTtlMs = Long.MAX_VALUE`——entry 永久驻留 Caffeine；仅 Caffeine `maximumSize` 可淘汰它，软过期永不移除 entry（只返回旧值并异步刷新）。

### 实例间缓存同步

<!-- Source: CacheSyncPublisher.java, CacheSyncListener.java, VersionGuard.java -->

当 `hotkey.sync.enabled=true` 时，所有写操作（`putThrough`、`putBeforeInvalidate`、`invalidate`、`invalidateAll`）通过 `CacheSyncPublisher` 触发放大：

```
┌──────────────┐    putThrough / invalidate      ┌───────────────────┐
│  实例 A      │ ──── CacheSyncPublisher ─────→  │ hotkey.sync       │
│  (写入方)    │                                 │  (fanout 交换机)   │
└──────────────┘                                 └────────┬──────────┘
                                                           │
                                             ┌─────────────┼─────────────┐
                                             ↓             ↓             ↓
                                      ┌──────────┐  ┌──────────┐  ┌──────────┐
                                      │实例 B    │  │实例 C    │   │   ...    │
                                      │Listener  │  │Listener  │  │Listener  │
                                      └────┬─────┘  └────┬─────┘  └────┬─────┘
                                           │              │              │
                                      ┌────┴────┐    ┌────┴────┐    ┌────┴────┐
                                      │版本守卫  │    │版本守卫  │   │版本守卫  │
                                      │compare()│    │compare()│    │compare()│
                                      └────┬────┘    └────┬────┘    └────┬────┘
                                           │              │              │
                                      ┌────┴────┐    ┌────┴────┐    ┌────┴────┐
                                      │L1 更新  │    │L1 更新  │    │L1 更新   │
                                      │或       │    │或       │    │或       │
                                      │失效     │    │失效     │    │失效      │
                                      └─────────┘    └─────────┘    └─────────┘
```

> [!NOTE] 版本守卫使用 `dataVersion`（来自 Redis INCR 或退化回退的版本）。见下方[版本空间](#版本空间)了解 dataVersion 与 decisionVersion 的区别。

**版本比较（4 种情况）——仅适用于 `dataVersion`：**

<!-- Source: VersionGuard.java:73-98 -->

| 本地 dataVersion | 传入 dataVersion | 结果                                                     |
| ---------------- | ---------------- | -------------------------------------------------------- |
| Normal           | Normal           | 数值大的版本获胜（传入 > 本地时替换）                    |
| Normal           | Degraded         | 本地胜出（跳过更新——退化对端数据不能覆盖健康的本地版本） |
| Degraded         | Normal           | 传入胜出（覆盖——健康的对端数据替换退化的本地版本）       |
| Degraded         | Degraded         | 数值大的版本获胜                                         |

### 版本空间

<!-- Source: model/CacheEntry.java, VersionGuard.java, HotKeyStateMachine.java -->

CacheEntry 维护**两个独立的版本空间**：

| 版本字段          | 来源                                                                | 可能退化？ | 使用方                                                 |
| ----------------- | ------------------------------------------------------------------- | ---------- | ------------------------------------------------------ |
| `dataVersion`     | `HotKeyCache.nextVersion()` — Redis INCR 或节点本地回退             | 是         | `VersionGuard.shouldSkipForSync()` (CacheSyncListener) |
| `decisionVersion` | `WorkerBroadcaster.decisionVersionCounter` — `AtomicLong`，永不退化 | 否         | `VersionGuard.shouldSkipForWorker()` (WorkerListener)  |

**规则：**

- `dataVersion` 跟踪实际数据变更版本，用于跨实例缓存同步。Redis 不可用时退化到节点本地计数器。
- `decisionVersion` 跟踪 Worker HOT/COOL 决策顺序。始终是 Worker 上干净的 `AtomicLong`——永不退化。
- 两个版本**正交**：数据变更不影响 `decisionVersion`，Worker 决策不影响 `dataVersion`。
- `putThrough` 始终设置 `decisionVersion=0L`（不保留——写穿透替换了值，任何之前的 Worker 决策均失效）。
- `loadAndCache` 也同样设置 `decisionVersion=0L`（首次加载尚无 Worker 决策）。
- `CacheSyncListener` 在跨实例同步刷新期间保留现有条目的 `decisionVersion`。
- **`isVersionDegraded` 安全网：** 当现有 entry 的 `isVersionDegraded=true`（创建于 Redis 宕机期），`shouldSkipForWorker` 将无条件接受 Worker 决策，跳过 `decisionVersion` 比较。这防止了 Worker 重启（AtomicLong 重置）后被退化期 entry 的旧 `decisionVersion` 阻挡——退化期 entry 产生于不稳定期，应让位于更新的 Worker 决策。

### CacheEntry 字段

<!-- Source: model/CacheEntry.java -->

| 字段                | 类型       | 说明                                                     |
| ------------------- | ---------- | -------------------------------------------------------- |
| `value`             | `Object`   | 缓存值                                                   |
| `dataVersion`       | `long`     | 数据变更版本——Redis `INCR` 产生，退化时用节点本地计数器  |
| `isVersionDegraded` | `boolean`  | `dataVersion` 是否来自回退（节点本地计数器）而非 Redis   |
| `decisionVersion`   | `long`     | Worker HOT/COOL 决策版本——`AtomicLong`，永不退化         |
| `hardTtlMs`         | `long`     | 当前硬 TTL（毫秒），取决于 key 状态或 per-entry 覆盖     |
| `hardExpireAtMs`    | `long`     | 硬 TTL 过期绝对时间（epoch 毫秒）                        |
| `softTtlMs`         | `long`     | 当前软 TTL（毫秒），取决于 key 状态或 per-entry 覆盖     |
| `softExpireAtMs`    | `long`     | 软 TTL 过期绝对时间（epoch 毫秒）                        |
| `keyState`          | `KeyState` | 当前 key 状态：`NORMAL` / `HOT` / `PRE_COOL` / `COOL`    |
| `normalHardTtlMs`   | `long`     | 条目在 `NORMAL` 状态创建时的原始硬 TTL；跨状态变迁时保留 |
| `normalSoftTtlMs`   | `long`     | 条目在 `NORMAL` 状态创建时的原始软 TTL；跨状态变迁时保留 |

> `normal*TtlMs` 记录条目在 `NORMAL` 状态创建时的原始 TTL。它们会跨 HOT/COOL 状态变迁保留，以便决策将状态切回 `NORMAL` 时恢复原始 TTL。

---

## 方法调用链

### 读取路径 — `get`

<!-- Source: HotKeyCache.java:138-162, loadAndCache:247-312 -->

```
HotKey.get(cacheKey, reader[, hardTtlMs, softTtlMs])
└─ HotKeyCache.get(cacheKey, reader, effectiveHardTtl, effectiveSoftTtl)
      ├─ TransactionSupport.runNowOrAfterCommit()              [事务感知，仅 L2 异步写入路径]
      ├─ caffeineCache.getIfPresent(key)                       [L1 查询]
      │    ├─ 命中 → unwrap CacheEntry
      │    │    ├─ KeyState == HOT → 使用热点 TTL（hotHardTtl / hotSoftTtl）
      │    │    └─ KeyState != HOT → 使用普通 TTL（normalHardTtl / normalSoftTtl）
      │    ├─ hotKeyDetector.add(key, 1)                       [本地 HeavyKeeper 频率计数]
      │    ├─ hotKeyReporter.record(key)                       [App→Worker 上报]
      │    └─ return Optional.of(entry.value)
      │
      └─ 未命中 → SingleFlight.execute(key, supplier)          [并发合并]
           ├─ supplier.get() → reader.get()                    [用户提供的 L2/DB 读取]
           ├─ hotKeyDetector.add(key, 1)
           ├─ hotKeyReporter.record(key)
           └─ loadAndCache（在 SingleFlight 内部）:
                ├─ hotKeyDetector.add(cacheKey, 1).isHotKey()
                │    ├─ hot  → CacheEntry(keyState=HOT, 热点 TTL)
                │    │         [dataVersion=0L, decisionVersion=0L]
                │    └─ cold → CacheEntry(keyState=NORMAL, 普通 TTL)
                │              [dataVersion=0L, decisionVersion=0L]
                ├─ caffeineCache.put(key, entry)
                └─ return Optional.of(value)
```

>[!NOTE]  L1 命中时 `CacheEntry` 由 sync 广播（REFRESH/INVALIDATE/INVALIDATE_ALL）或 Worker 决策（HOT/COOL）写入，其中的 `dataVersion` 和 `decisionVersion` 由发送方决定。L1 未命中由 `loadAndCache` 创建，版本字段均为 0。`loadAndCache` 缓存所有加载的 key（不仅热点），根据 `KeyState` 区分 TTL。

### 软过期读取路径 — `getWithSoftExpire`

<!-- Source: HotKeyCache.java:189-236, CacheExpireManager.java -->

```
HotKey.getWithSoftExpire(key, reader[, softTtlMs][, hardTtlMs, softTtlMs])
└─ HotKeyCache.getWithSoftExpire(key, reader, ...)
      ├─ caffeineCache.getIfPresent(key) → CacheEntry
      │    ├─ 硬 TTL 未过期 → 同 get 路径
      │    ├─ 硬 TTL 已过期 → 返回 empty（同 get 路径）
      │    └─ 软 TTL 已过期但硬 TTL 未过期
      │         ├─ 立即返回旧值（stale）
      │         ├─ hotKeyDetector.add(key, 1)
      │         ├─ hotKeyReporter.record(key)
      │         └─ 异步提交刷新任务:
      │              ├─ refreshLimiter.tryAcquire()
      │              │    └─ 限流命中 → 跳过本次刷新
      │              └─ executor.submit(() → reader.get())
      │                   ├─ caffeineCache.put(key, newEntry)
      │                   └─ 更新 softExpireAtMs
      │
      └─ 未命中 → SingleFlight.execute → 同 get 路径
```

> [!NOTE] 异步刷新保留原 CacheEntry 的硬 TTL（`hardTtlMs` / `hardExpireAtMs`），仅更新值和软过期时间。

### 窥视路径 — `peek`

<!-- Source: HotKeyCache.java:170-180 -->

```
HotKey.peek(cacheKey)
└─ HotKeyCache.peek(cacheKey)
      └─ caffeineCache.getIfPresent(key)                       [仅 L1，无副作用]
           └─ return Optional.ofNullable(entry.value)
           [⚠ 不调用 hotKeyDetector.add / hotKeyReporter.record / L2 reader]
```

### 原始缓存访问 — `getLocalCache`

暴露底层 Caffeine `Cache<String, Object>`，用于 Caffeine 特定操作（`asMap`、`policy`、`cleanUp`）。

<!-- Source: HotKeyCache.java:542-544 -->

```
HotKey.getLocalCache()
└─ HotKeyCache.getLocalCache()
      └─ return caffeineCache                                   [直接 Caffeine 引用]
      [⚠ 绕过 HotKey 编排层——版本追踪、广播和过期管理均被跳过]
```

> [!WARNING]
> `getLocalCache()` 返回**原始** Caffeine 缓存。通过此引用写入的任何内容都不经过版本生成、广播同步或过期管理。仅用于自省或 Caffeine 特定维护（`asMap().keySet()`、`policy().expireAfterWrite()`、`cleanUp()`）。切勿通过此引用写入条目——请使用 `putThrough()` 或 `putBeforeInvalidate()`。

### 写穿透路径 — `putThrough`

<!-- Source: HotKeyCache.java:375-416 -->

```
HotKey.putThrough(key, value, writer[, hardTtlMs, softTtlMs])
└─ HotKeyCache.putThrough(key, value, writer, effectiveHardTtl, effectiveSoftTtl)
      ├─ (事务内) → TransactionSupport.registerAfterCommit()
      │    └─ afterCommit → 执行以下全部
      │
      ├─ (非事务) → 异步: hotKeyExecutor.submit(() → 以下全部)
      │
      ├─ writer.run()                                          [用户写入 L2/DB]
      │    └─ 异常 → log.error, 流程继续（L1 和广播仍执行）
      │
      ├─ nextVersion(key)                                      [版本生成]
      │    ├─ Redis 可用:
      │    │    └─ Lua: "INCR KEYS[1]; EXPIRE KEYS[1] ARGV[1]"
      │    │         → dataVersion=正数, degraded=false
      │    └─ Redis 不可用:
      │         └─ fallbackVersionCounter.incrementAndGet()
      │              → dataVersion=Long.MIN_VALUE + counter, degraded=true
      │
      ├─ caffeineCache.put(key, CacheEntry(
      │      value, dataVersion, degraded,
      │      decisionVersion=0L,                                 [写穿透始终重置]
      │      keyState=NORMAL,                                   [始终写入 NORMAL]
      │      hardTtlMs, softTtlMs, ...))
      │
      └─ CacheSyncPublisher.broadcastRefresh(key, version, degraded)
           └─ sendDeduped(key, "REFRESH", version, degraded)
                ├─ recentBroadcasts.compute("REFRESH:"+key, (_, old) →
                │    old != null && old > version ? old : version)
                │    └─ 返回 != version → 已有更新版本，跳过广播
                │    └─ 返回 == version → 发送消息
                └─ rabbitTemplate.send(exchange, "", msg)
                     [header: type=REFRESH, version, isVersionDegraded]
```

### 集合写入路径 — `putBeforeInvalidate`

<!-- Source: HotKeyCache.java:422-446 -->

```
HotKey.putBeforeInvalidate(key, mutation)
└─ HotKeyCache.putBeforeInvalidate(key, mutation)
      ├─ (事务内) → TransactionSupport.registerAfterCommit()
      │    └─ afterCommit → 执行以下全部
      │
      ├─ (非事务) → 同步执行（调用者线程）
      │
      ├─ mutation.run()                                        [用户变更 L2/DB]
      │    └─ 异常 → log.error, return（终止，跳过后续 L1 失效和广播）
      │
      ├─ nextVersion(key)                                      [同 putThrough]
      ├─ caffeineCache.invalidate(key)                         [L1 移除，非 put]
      └─ CacheSyncPublisher.broadcastLocalInvalidate(key, version, degraded)
           └─ 同 putThrough（但 type=INVALIDATE 非 REFRESH）
```

> [!NOTE]  `mutation.run()` 和 L1 失效之间存在约 1ms 窗口，并发 `get()` 可能命中旧值。这是故意的权衡——先失效再变更会导致 `get()` 读取到老数据并重新填充 L1，窗口更长。

### 单 key 失效 — `invalidate`

<!-- Source: HotKeyCache.java:321-358 -->

```
HotKey.invalidate(cacheKey)
└─ HotKeyCache.invalidate(cacheKey)
      ├─ invalidCacheKey(key) → return                          [跳过 null/空]
      ├─ TransactionSupport.runNowOrAfterCommit()
      │    ├─ (事务内) → 延后到 afterCommit
      │    └─ (非事务) → 同步执行
      │
      ├─ nextVersion(key)
      │    ├─ Redis 可用: INCR + EXPIRE → 正数版本, degraded=false
      │    └─ Redis 不可用: Long.MIN_VALUE + counter → degraded=true
      │
      ├─ caffeineCache.invalidate(key)                          [L1 移除]
      │
      └─ CacheSyncPublisher.broadcastRefresh(key, version, degraded)
           └─ sendDeduped(key, "REFRESH", version, degraded)
                ├─ recentBroadcasts.compute(...)                [去重]
                └─ rabbitTemplate.send()
                     ↓
                 ┌─ 对端: CacheSyncListener.handleRefresh(msg)
                 │    ├─ 提取 dataVersion + isVersionDegraded
                 │    ├─ VersionGuard.shouldSkipForSync():
                 │    │    ├─ 正常 vs 正常: 数字大者胜
                 │    │    ├─ 正常 vs 降级: 正常胜（跳过降级）
                 │    │    ├─ 降级 vs 正常: 正常胜（加载正常）
                 │    │    └─ 降级 vs 降级: 数字大者胜
                 │    ├─ 通过 → redisReader.get(key)              [从 Redis 重新加载]
                 │    ├─ caffeineCache.put(key, newEntry)
                 │    │    [decisionVersion=保留现有值]
                 │    └─ return
                 └─
```

### 批量失效 — `invalidateAll`

<!-- Source: HotKeyCache.java:336-358 -->

```
HotKey.invalidateAll(keys...) / invalidateAll(Collection)
└─ HotKeyCache.invalidateAll(keys)
      ├─ stream().filter(k → !invalidCacheKey(k)).toList()      [跳过 null/空]
      ├─ 空列表 → log.debug, return
      │
      ├─ TransactionSupport.runNowOrAfterCommit()
      │    ├─ (事务内) → 延后到 afterCommit
      │    └─ (非事务) → 同步执行
      │
      ├─ caffeineCache.invalidateAll(validKeys)                 [L1 批量移除]
      │
      └─ (有 syncPublisher)
           └─ CacheSyncPublisher.broadcastLocalInvalidateAll(validKeys)
                └─ 单条 AMQP 消息
                     │    body = JSON 数组 [key1, key2, ...]
                     │    header: type=INVALIDATE_ALL
                     ↓
                 ┌─ 对端: CacheSyncListener.handleInvalidateAll(msg)
                 │    └─ caffeineCache.invalidateAll(keys)       [批量 L1 移除]
                 │    [⚠ 不调 Redis 重新加载，不检查版本号]
                 └─
```

### 状态查询 — `isLocalHotKey`

<!-- Source: HotKeyCache.java:164-168 -->

```
HotKey.isLocalHotKey(cacheKey)
└─ HotKeyCache.isLocalHotKey(cacheKey)
      └─ caffeineCache.getIfPresent(key)
           ├─ 存在且 keyState == HOT → true
           └─ 其他 → false
           [⚠ 纯 L1 查询，无任何副作用]
```

### 状态查询 — `isWorkerHotKey`

<!-- Source: HotKey.java (delegates to workerTopKAlgorithm) -->

```
HotKey.isWorkerHotKey(cacheKey)
└─ workerTopKAlgorithm.list().stream().anyMatch(item -> item.key().equals(cacheKey))
      ├─ key 在 Worker TopK 中 → true
      └─ 不在 → false
      [⚠ 遍历 Worker TopK 列表，O(n)；无网络调用]
```

### TopK 查询方法

<!-- Source: HotKey.java -->

```
HotKey.returnLocalHotKeys()
└─ (topKAlgorithm != null)
      ├─ yes → topKAlgorithm.list()                             [本地 HeavyKeeper Top-K]
      └─ no  → Collections.emptyList()

HotKey.returnLocalExpelledHotKeys()
└─ (topKAlgorithm != null)
      ├─ yes → topKAlgorithm.expelledItems()                    [被挤出的热点 key 队列]
      └─ no  → 空 LinkedBlockingQueue

HotKey.returnLocalTotalDataStreams()
└─ (topKAlgorithm != null)
      ├─ yes → topKAlgorithm.totalDataStreams()                 [累计访问量]
      └─ no  → 0L

HotKey.returnWorkerHotKeys()
└─ (workerTopKAlgorithm != null)
      ├─ yes → workerTopKAlgorithm.list()                       [Worker 侧集群 Top-K]
      └─ no  → Collections.emptyList()

HotKey.returnWorkerExpelledHotKeys()
└─ (workerTopKAlgorithm != null)
      ├─ yes → workerTopKAlgorithm.expelledItems()
      └─ no  → 空 LinkedBlockingQueue

HotKey.returnWorkerTotalDataStreams()
└─ (workerTopKAlgorithm != null)
      ├─ yes → workerTopKAlgorithm.totalDataStreams()
      └─ no  → 0L
```

> **TopK 空安全：** 以上 6 个查询方法在对应 TopK 不可用时（Worker-only 模式或未配置）返回空值/0，不会抛出异常。这与 `get`/`putThrough` 等方法不同（后者在 Worker-only 模式下调用 `requireCache()` 抛出 `UnsupportedOperationException`）。

### 规则管理 API

<!-- Source: RuleMatcher.java -->

```
HotKey.addBlacklist(keyPattern)
└─ HotKeyCache.addBlacklist(keyPattern)
      └─ ruleMatcher.addRule(Rule.of(keyPattern, BLOCK))
           ├─ [有 Redis] → 持久化到 Redis
           └─ [有 syncPublisher] → broadcastAllLocalRules()
           [自动检测模式类型: 精确、前缀、通配符、正则]

HotKey.removeBlacklist(keyPattern)
└─ HotKeyCache.unBlacklist(keyPattern)
      └─ ruleMatcher.removeRule(keyPattern, BLOCK)
           ├─ [有 Redis] → 持久化到 Redis
           └─ [有 syncPublisher] → broadcastAllLocalRules()

HotKey.addWhitelist(keyPattern)
└─ HotKeyCache.addWhitelist(keyPattern)
      └─ ruleMatcher.addRule(Rule.of(keyPattern, ALLOW_NO_REPORT))
           ├─ [有 Redis] → 持久化到 Redis
           └─ [有 syncPublisher] → broadcastAllLocalRules()

HotKey.removeWhitelist(keyPattern)
└─ HotKeyCache.unWhitelist(keyPattern)
      └─ ruleMatcher.removeRule(keyPattern, ALLOW_NO_REPORT)
           ├─ [有 Redis] → 持久化到 Redis
           └─ [有 syncPublisher] → broadcastAllLocalRules()

HotKey.evaluateRule(cacheKey)
└─ ruleMatcher.evaluateRule(cacheKey)
      └─ 返回 BLOCK / ALLOW_NO_REPORT / ALLOW
      [get/putThrough/putBeforeInvalidate 内部调用；公开供手动检查]

HotKey.getAllRules()
└─ HotKeyCache.getAllRules()
      └─ ruleMatcher.getAllRules() → List<Rule>
      [当前规则的快照；无缓存时返回空列表]

HotKey.clearAllRules()
└─ HotKeyCache.clearAllRules()
      └─ ruleMatcher.clearAllRules()
           ├─ [有 Redis] → 删除 Redis 中规则
           └─ [有 syncPublisher] → broadcastAllLocalRules()
      [移除黑名单和白名单规则]

HotKey.broadcastAllLocalRulesManually()
└─ HotKeyCache.broadcastAllLocalRulesManually()
      └─ ruleMatcher.exportRulesJson()
           └─ CacheSyncPublisher.broadcastAllLocalRules(json)
                └─ 单条 AMQP 消息，body = JSON 规则数组
                     header: type=RULES_SYNC
      [手动触发初始化集群同步]
```

---

## Worker 模式

Worker 模式是一种可选部署拓扑：一个专用节点聚合所有应用实例的访问报告，通过滑动窗口 + 状态机管道执行集群维度的热点检测，并将 HOT/COOL 决策广播回每个实例。

此方案解决了**单实例盲区问题**——应用本地的 HeavyKeeper 无法区分"被同一个 pod 访问 100 次"与"被 100 个 pod 各访问 1 次"。Worker 模式提供集群共识的热点检测，无需中心代理。

### Worker 架构

<!-- Source: HotKeyReporter.java, ReportPublisher.java, ReportConsumer.java,
     SlidingWindowDetector.java, HotKeyStateMachine.java:61-148,
     WorkerBroadcaster.java, WorkerListener.java -->

```
┌─────────────────────┐      RabbitMQ fanout      ┌───────────────────────┐
│  App 实例 1         │  ─── 报告（定时）────────→ │                       │
│  HotKeyReporter     │                           │   Worker 节点         │
├─────────────────────┤                           │                       │
│  App 实例 2         │  ─── 报告（定时）────────→ │  ┌─────────────────┐  │
│  HotKeyReporter     │                           │  │ ReportConsumer  │  │
├─────────────────────┤                           │  │ (AMQP 消费者)   │  │
│  App 实例 N         │                           │  └────────┬────────┘  │
│  HotKeyReporter     │  ─── 报告（定时）────────→ │           │           │
└─────────────────────┘                           │           ↓           │
                                                  │  ┌─────────────────┐  │
                                                  │  │HotKeyStateMachine│ │
                                                  │  │ (按 key 的 FSM) │  │
                                                  │  └────────┬────────┘  │
                                                  │           │           │
                                                  │  ┌─────────────────┐  │
                                                  │  │WorkerBroadcaster│  │
                                                  │  │ (HOT/COOL 通过  │  │
                                                  │  │  RabbitMQ)      │  │
                                                  │  └────────┬────────┘  │
                                                  └───────────┼───────────┘
                                                              │
                          RabbitMQ (hotkey.broadcast.exchange)|
                                                              │
                                                              ↓
                           ┌──────────────────────────────────────────┐
                           │     所有 App 实例 (WorkerListener)        │
                           │  ┌──────────┐  ┌──────────┐  ┌────────┐  │
                           │  │ 实例 1   │  │ 实例 2   │   │  ...   │  │
                           │  └──────────┘  └──────────┘  └────────┘  │
                           └──────────────────────────────────────────┘
```

### 报告流程

1. 每次 `get()` / `getWithSoftExpire()` 调用触发 `hotKeyReporter.record(key)`，包括 L1 命中或 L2 读取路径
2. `HotKeyReporter` 在本地累计 per-key 计数（Caffeine 30s 过期，最大 100k key），周期性发布 `ReportMessage` 到 `hotkey.report.exchange` RabbitMQ 交换机，按 `app-name` 和 `shard-index` 路由
3. Worker 节点的 `ReportConsumer` 接收报告，将计数投喂到 `workerTopK.add()`，丢弃超过 5s 的陈旧报告
4. Worker 异步处理报告——不阻塞 App 实例

### 报告分片

`shard-count > 1` 时，报告分布到多个 Worker 实例。每个 App 实例计算 `abs(hash(cacheKey)) % shard-count` 并发布到对应分片路由键。每个 Worker 绑定自己的队列（`hotkey.report.{appName}.{shardIndex}`），支持 Worker 层水平扩展。

### 滑动窗口检测器

<!-- Source: SlidingWindowDetector.java -->

`SlidingWindowDetector` 是一个无锁时间序列计数器，在可配置窗口内跟踪每个 key 的访问计数。

| 属性                                       | 默认值 | 说明                             |
| ------------------------------------------ | ------ | -------------------------------- |
| `hotkey.worker.sliding-window.duration-ms` | `1000` | 窗口总时长（1 秒）               |
| `hotkey.worker.sliding-window.slices`      | `10`   | 每个窗口的时间片数（每片 100ms） |

每个 key 维护一个按时间片索引的 `long` 计数器数组。每次报告滴答时，回收最旧的时间片，重新计算各 key 的有效计数（所有时间片之和）。这提供了准确的 QPS 估算，无需逐次访问加锁。

### 热点状态机

<!-- Source: HotKeyStateMachine.java:61-148 -->

每个被跟踪的 key 由 `HotKeyStateMachine` 管理其生命周期：

```
                    ┌─────────────────────────────────────────────────┐
                    │                                                 │
                    ↓                                                 │
          ┌──────────────────┐  低于阈值持续          ┌──────────────┐ │
          │  CONFIRMED_HOT   │ ──(coolCount - grace)→│ PRE_COOLING  │ │
          └──────────────────┘                       └──────┬───────┘ │
                    ↑                                       │         │
                    │        低于阈值                        │         │
                    │     ┌─────────────────────────────────┘         │
                    │     ↓                                           │
          ┌──────────┴────┐     宽限期结束     ┌──────────────┐        │
          │     COLD      │ ←─────────────────│   COLD       │        │
          └───────────────┘                   └──────────────┘        │
                    ↑                                                 │
                    └─────────────────────────────────────────────────┘

  COLD ─────────── 低于阈值 ──→ PRE_COOLING（宽限期）
  PRE_COOLING ──── 宽限期结束 ──→ COLD（广播 TYPE_COOL）
  PRE_COOLING ──── 重新升温（静默）──→ CONFIRMED_HOT（不广播）
  CONFIRMED_HOT ── 低于阈值持续 (coolCount - grace) ──→ PRE_COOLING（宽限期）
```

- **COLD**：key 存在但低于热阈值。跟踪访问但不发送广播。
- **CONFIRMED_HOT**：key 超过阈值持续 `confirm-duration-ms`。Worker 向所有实例广播 `TYPE_HOT`。保持 HOT 直到持续冷却。
- **PRE_COOLING**：key 降至阈值以下，进入宽限期。期间若重新升温，静默返回 CONFIRMED_HOT 状态，避免广播抖动。

### 状态机配置

| 属性                                              | 默认值  | 说明                                |
| ------------------------------------------------- | ------- | ----------------------------------- |
| `hotkey.worker.threshold.hot-threshold`           | `1000`  | 绝对热阈值（设为 -1 启用比率模式）  |
| `hotkey.worker.threshold.hot-threshold-ratio`     | `0.01`  | 热阈值占估计全局 QPS 的比例         |
| `hotkey.worker.state-machine.confirm-duration-ms` | `300`   | 高于阈值确认 HOT 的持续时间（300ms） |
| `hotkey.worker.state-machine.cool-duration-ms`    | `15000` | 低于阈值确认 COOL 的持续时间（15s） |
| `hotkey.worker.state-machine.pre-cool-grace-ms`   | `5000`  | 静默重新升温的宽限期（5s）          |

### 动态阈值（全局 QPS）

Worker 根据流量模式自适应调整热阈值：

```
hotThreshold = max(minCount, estimatedGlobalQPS * hotThresholdRatio)
```

| 属性                                                                 | 默认值  | 说明                                    |
| -------------------------------------------------------------------- | ------- | --------------------------------------- |
| `hotkey.worker.global-qps-dynamic-threshold.recalculate-interval-ms` | `60000` | 重新计算间隔（60s）                     |
| `hotkey.worker.global-qps-dynamic-threshold.qps-change-tolerance`    | `0.5`   | 触发阈值更新所需的 QPS 变化幅度（±50%） |
| `hotkey.worker.global-qps-dynamic-threshold.learning-period-ms`      | `30000` | QPS 估算的学习周期                      |
| `hotkey.worker.global-qps-dynamic-threshold.hot-threshold-ratio`     | `0.01`  | 热阈值占估计全局 QPS 的比例             |

`qps-change-tolerance` 防止正常流量波动导致阈值抖动——仅显著 QPS 变化触发重新计算。

### Top-K 交叉验证

Worker 定期将自身集群维度 Top-K 与应用端 HeavyKeeper Top-K 进行交叉验证，确保一致性并支持预预热。

| 属性                                                     | 默认值  | 说明                          |
| -------------------------------------------------------- | ------- | ----------------------------- |
| `hotkey.worker.topk-validation.validate-interval-ms`     | `60000` | 交叉验证间隔（60s）           |
| `hotkey.worker.topk-validation.pre-warm-count`           | `5`     | 符合预预热条件的 Top-K 条目数 |
| `hotkey.worker.topk-validation.pre-warm-min-appearances` | `2`     | 预预热所需的最小连续出现次数  |

连续多个验证周期出现在 Worker Top-K 中的条目将成为预预热候选。Worker 可主动将这些 key 推送给 App 实例，早于本地自然检测。

### 部署模式

两种部署模式：

| 模式        | `worker.enabled` | 激活的 Bean                                                           |
| ----------- | ---------------- | --------------------------------------------------------------------- |
| App-only    | `false`（默认）  | `HotKeyCache`、TopK 检测器、reporter、actuator、sync                  |
| Worker-only | `true`           | 仅 Worker（SlidingWindow、StateMachine、ReportConsumer、Broadcaster） |

**Worker-only** 模式下，`HotKey.isLocalHotKey()` / `get()` / `putThrough()` 抛出 `UnsupportedOperationException`——这些操作需要应用端缓存。Worker-TopK 查询（`returnWorkerHotKeys()`）仍可用。

### 配置示例

```yaml
hotkey:
  worker:
    # 必须显式设为 true 才能启用 Worker 模式
    enabled: true

    routing:
      app-name: my-service
      shard-count: 1
      shard-index: 0

    sliding-window:
      duration-ms: 1000
      slices: 10

    threshold:
      hot-threshold-ratio: 0.01

    state-machine:
      confirm-duration-ms: 300
      cool-duration-ms: 15000
      pre-cool-grace-ms: 5000

    global-qps-dynamic-threshold:
      recalculate-interval-ms: 60000
      qps-change-tolerance: 0.5
      learning-period-ms: 30000
      hot-threshold-ratio: 0.01

    topk-validation:
      validate-interval-ms: 60000
      pre-warm-count: 5
      pre-warm-min-appearances: 2

    heavy-keeper:
      top-k: 100
      width: 20000
      depth: 10
      decay: 0.9
      min-count: 10

spring:
  rabbitmq:
    host: localhost
    port: 5672
```

> [!IMPORTANT]
> 以上示例使用明文 AMQP 连接。生产环境中应通过 `spring.rabbitmq.ssl.*` 启用 TLS：
>
> ```yaml
> spring:
>   rabbitmq:
>     ssl:
>       enabled: true
>       key-store: classpath:client.p12
>       key-store-password: changeit
>       trust-store: classpath:truststore.jks
>       trust-store-password: changeit
> ```
>
> 详见 [Spring Boot RabbitMQ SSL 文档](https://docs.spring.io/spring-boot/reference/messaging/amqp.html#page-title)。

### Worker Listener（App 实例端配置）

```yaml
hotkey:
  worker-listener:
    enabled: true
```

每个 App 实例需启用 `worker-listener` 以接收 Worker 发出的 HOT/COOL 决策。监听器绑定到队列 `hotkey.worker:{instanceId}` 并处理收到的决策。

### 报告配置（App 实例端）

```yaml
hotkey:
  local:
    app-name: my-service
    report-interval-ms: 100
    shard-count: 1
```

### 故障表现

| 故障            | 影响                                            | 恢复方式                  |
| --------------- | ----------------------------------------------- | ------------------------- |
| Worker 崩溃     | App 实例继续使用本地 TopK；无集群 HOT/COOL 决策 | 重启 Worker；实例自动重连 |
| 报告通道故障    | App 报告排队/缓冲（RabbitMQ 持久化）            | RabbitMQ 恢复后自动恢复   |
| Worker 广播故障 | 无跨实例 HOT/COOL 同步；本地 TopK 仍正常        | 重启 Worker broadcaster   |

---

## 补充图表

### SingleFlight 去重（惊群保护）

<!-- Source: hotkeycache/SingleFlight.java, HotKeyStressTest: singleFlight_extremeDedup, singleFlight_cacheStampede -->

```
     thread-1   thread-2   thread-3  ...  thread-50
         │          │          │              │
         ▼          ▼          ▼              ▼
     ┌───────────────────────────────────────────────────┐
     │           SingleFlight.inflightCache              │
     │  ┌───────────────────────────────────────────────┐│
     │  │ key = "cache:shop:17"                          
     │  │   ▼                                            
     │  │ future = CompletableFuture.supplyAsync(...)   
     │  │ computeIfAbsent(key, future) ◀─ 1st thread:   
     │  │   absent → supplyAsync 创建，所有线程共享        
     │  │   同一个 future  ◀──────────────────────────┐  
     │  │   present → block-wait on future  ◀─────┐   │ 
     │  └──────────────────────────────────────────┼   │ 
     │                                             │   │ 
     │  ┌──────────────────────────────────────────▼─┐ │ 
     │  │ supplier.get()  ─── L2 / DB read           │ │ 
     │  │ add(key,1)                                 │ │ 
     │  │ record(key)                                │ │ 
     │  │ Caffeine.put(key, entry)                   │ │ 
     │  │ future.complete(value) ────────────────────│─┘
     │  └────────────────────────────────────────────┘   
     └───────────────────────────────────────────────────┘
                                                  │
                                                  ▼
     thread-1 ── future.get() = value     ◀──────┘
     thread-2 ── future.get() = value
     thread-3 ── future.get() = value
     ...
     thread-50 ─ future.get() = value

  inflight-ttl-seconds   = 5s   (must > slowest L2 response)
  inflight-timeout-seconds = 3s  (orTimeout → CompletableFuture.failedException)
  Stresstest result: 50 threads → 1 execution (99.0% dedup)
```

### VersionGuard 4-case 数据流

<!-- Source: VersionGuard.java:73-98 -->

```
                     Incoming SyncMessage
                     (dataVersion, isVersionDegraded)
                                 │
                                 ▼
            ┌───────────────────────────────────────────────┐
            │  shouldSkipForSync(cache, key, incomingVer,   │
            │                    incomingDegraded)          │
            │                                               │
            │  existing = cache.getIfPresent(key)           │
            │  if !(existing instanceof CacheEntry):        │
            │     → return false (allow write)              │
            └──────────────────────┬────────────────────────┘
                                   │
                    ┌───────────────┴───────────────┐
                    │ existing.isVersionDegraded?   │
                    │                               │
         ┌──────────┼──────────────┬────────────────┤
         │ no       │ no           │ yes            │ yes
         │ incoming │ degraded     │ incoming       │ degraded
         │ no       │              │ no             │
         ▼          ▼              ▼                ▼
   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
   │ existing │  │ always   │  │ always   │  │ existing │
   │ >= inc?  │  │ skip     │  │ allow    │  │ >= inc?  │
   │ Y→skip   │  │ (normal  │  │ (normal  │  │ Y→skip   │
   │ N→allow  │  │  wins)   │  │overwrites│  │ N→allow  │
   │          │  │          │  │degraded) │  │          │
   └──────────┘  └──────────┘  └──────────┘  └──────────┘
```

### HeavyKeeper 衰减管道

<!-- Source: algorithm/HeavyKeeper.java, HotKeySchedulingConfiguration.java -->

```
┌────────────┐   add(key, 1)   ┌─────────────────────────────────────────────┐
│  Request   │ ──────────────▶│  HeavyKeeper (Count-Min Sketch)             │
│  enters    │                 │                                             │
└────────────┘                 │  for each of depth (5) rows:                │
                               │    idx = hash(row, key) % width             │
                               │    count = bucket[row][idx]                 │
                               │    if count == 0:                           │
                               │       bucket[row][idx] = 1                  │
                               │       heap.add(key, count)                  │
                               │    else:                                    │
                               │       prob = 1 / (count ^ decay)            │
                               │       if random() < prob:                   │
                               │          bucket[row][idx] -= 1  (fading)    │
                               │          bucket[row][idx] += 1              │
                               └────────────────────┬────────────────────────┘
                                                    │
                              every decay-period (20s) via @Scheduled
                                                    ▼
                               ┌─────────────────────────────────────────────┐
                               │  decay()                                    │
                               │  each bucket count -= 1 (min 0)             │
                               │ 配合 fading(): exp(-1/count) 衰减最小计数     │
                               └────────────────────┬────────────────────────┘
                                                    │
                                                    ▼
                               ┌─────────────────────────────────────────────┐
                               │  TopK Heap (min-heap, K=100)                │
                               │  isHotKey() = heap.contains(key)            │
                               │  list()          → current hot keys         │
                               │  expelledItems() → recently evicted         │
                               │  fading()        → exp decay per entry      │
                               │  totalDataStreams() → total access count    │
                               └─────────────────────────────────────────────┘
```

### Worker 报告分片路由

<!-- Source: HotKeyReporter.java, ReportPublisher.java, ReportConsumer.java -->

```
  App 实例端                                                Worker 端
  ┌──────────────────────┐
  │  HotKeyReporter      │
  │  （每实例             │
  │   Caffeine 计数器,    │
  │   30s 过期,           │
  │   最大 100k key）     │
  └──────────┬───────────┘
             │ 每 report-interval-ms (100ms)
             │ ReportMessage { appName, shardCount,
             │   Map<key, count> }
             ▼
  ┌─────────────────────────────────────────────────────────────────────┐
  │  hotkey.report.exchange  (direct)                                   │
  │  routing-key = appName + "." + (abs(hash(key)) % shardCount)        │
  │                                                                     │
  │  ┌───────────────────┐  ┌───────────────────┐  ┌─────────────────┐  │
  │  │ binding:          │  │ binding:          │  │ binding:        │  │
  │  │ hotkey.report.    │  │ hotkey.report.    │  │ hotkey.report.  │  │
  │  │ my-svc.0          │  │ my-svc.1          │  │ my-svc.2        │  │
  │  └────────┬──────────┘  └────────┬──────────┘  └────────┬────────┘  │
  └───────────┼──────────────────────┼──────────────────────┼───────────┘
              │                      │                      │
              ▼                      ▼                      ▼
       ╔═════════════╗        ╔═════════════╗        ╔═════════════╗
       ║  Worker 0   ║        ║  Worker 1   ║        ║  Worker 2   ║
       ║  shard = 0  ║        ║  shard = 1  ║        ║  shard = 2  ║
       ║  1/3 keys   ║        ║  1/3 keys   ║        ║  1/3 keys   ║
       ╚═════════════╝        ╚═════════════╝        ╚═════════════╝

  shard-count 默认 = 1；增大可线性降低每队列吞吐量
  哈希路由保证: 同一 key → 同一分片，跨 Worker 重启一致
  consumer-count = max(1, shardCount / 2) 默认
```

---

## 设计说明

**为什么 `invalidate` 发 REFRESH 而 `invalidateAll` 发 INVALIDATE_ALL？**
单 key 失效预期很快会被读取，发送 REFRESH 让对端主动从 Redis 重新加载，减少下一次 `get()` 的延迟。批量失效可能涉及大量 key 且不一定立即被访问——`invalidateAll` 用一条 TYPE_INVALIDATE_ALL 消息（JSON key 数组）让对端惰性加载。

**为什么 `invalidateAll` 不走 INCR？**
每次 INCR 是一次 Redis 调用。`invalidateAll` 设计用于批量清理（缓存过期、批量数据变更通知），性能优先于版本一致性。如每个 key 都需要版本控制，应循环调用 `invalidate()`。

**为什么 `fallbackVersion` 用 `Long.MIN_VALUE + counter`？**
所有降级版本为负数，在 `sendDeduped` 的 `> version` 比较中始终低于正常（正数）Redis INCR 版本。确保正常版本永远优先于降级本地版本，无需额外 `degraded` 标志逻辑。

**`putThrough` 非事务时为什么异步，而其他写方法同步？**
`putThrough` 的 `writer.run()` 可能涉及 L2/DB 网络 IO，异步执行避免阻塞调用者。`invalidate`/`invalidateAll`/`putBeforeInvalidate` 预期轻量（仅缓存操作+广播），同步执行保证调用者返回时广播已发出。

**TTL 随机偏移（缓存雪崩防护）：**
`CacheExpireManager.toHardExpireTimestamp()` 和 `toSoftExpireTimestamp()` 在计算每个过期时间戳时，使用 `ThreadLocalRandom` 施加 ±10% 的均匀随机偏移。这打散了拥有相同配置 TTL 的大量 key 的实际淘汰时间，防止同时大规模淘汰（缓存雪崩）。偏移比例硬编码为 10%——详见 `CacheExpireManager.java:48,136-155`。
