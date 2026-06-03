[← 返回首页](README.zh.md)

## 架构

### 正常读路径（`get`）

```
┌──────────────┐   L1 命中              ┌──────────────┐
│   请求       │ ─── add(key,1) ─────→  │  Caffeine L1 │
│             │ ─── record(key) ────→   │  (本地)      │
│             │ ←────────────────────   │              │
└──────┬───────┘   Optional.of(value)   └──────┬───────┘
       │ L1 未命中        (自动解包           │ isLocalHotKey()?
       ↓ (inflight 去重)  CacheEntry)          ↓
┌──────────────┐  ──── reader ────→   ┌───────────────┐
│  L2 存储     │  ──add(key,1)───→    │     TopK      │
│  (可插拔)    │  ──record(key)──→    │  (接口)        │
└──┬───────┬───┘                      ├───────────────┤
    │ 命中  │ null                    │ add()→Result  │
    ↓       ↓                         │ list()        │
 Optional   Optional.empty()          │ listTopN(n)   │
 .of(value)   r.isEmpty() → DB        │ total()       │
                                      │ contains()    │
                                      │ expelled()    │
                                      │ fading()      │
                                      └───────┬───────┘
                                             │ isLocalHotKey()?
                                             ↓
                                  ┌─────────────────────┐
                                  │  根据 KeyState 选择  │
                                  │  TTL:               │
                                  │  HOT   → 热点 TTL   │
                                  │  other → 普通 TTL   │
                                  └──────────┬──────────┘
                                             ↓
                                  Caffeine.put(key, CacheEntry(
                                    value,
                                    dataVersion=0L,
                                    isVersionDegraded=false,
                                    decisionVersion=0L,
                                    hardTtlMs, hardExpireAtMs,
                                    softTtlMs, softExpireAtMs,
                                    keyState,
                                    normalHardTtlMs, normalSoftTtlMs))
                                   （读路径不触发同步）
```

> **注意：** `isLocalHotKey()` 检查 L1 中 key 的 `KeyState` 是否为 HOT。key 以 `CacheEntry` 包装形式存储，包含完整 TTL 元数据——`get()` 路径自动解包为原始值。

### 写路径 — `putThrough`

```
putThrough(cacheKey, value, writer)
├─ （如果在 Spring 事务内，延迟到 afterCommit）
├─ writer.run() — L2 写入（调用方提供的 Runnable）
├─ nextVersion(cacheKey) — Redis INCR → VersionResult(dataVersion, isVersionDegraded)
│  └─ Redis 失败时 → 节点本地计数器回退（degraded=true）
├─ Caffeine.put(cacheKey, CacheEntry(
│    value,
│    dataVersion, isVersionDegraded,
│    decisionVersion=existingEntry.decisionVersion（保留）,
│    hardTtlMs, hardExpireAtMs,
│    softTtlMs, softExpireAtMs,
│    keyState,
│    normalHardTtlMs, normalSoftTtlMs))
└─ CacheSyncPublisher.send()
     └─ RabbitMQ fanout (hotkey.sync.exchange)
          └─ TYPE_REFRESH 带 dataVersion 和 isVersionDegraded 头部
```

**写路径——事务延迟：** `putThrough`、`putBeforeInvalidate`、`invalidate` 和 `invalidateAll` 在 Spring 事务内调用时都会延迟到 `afterCommit` 执行。

> **注意：** `putThrough` 在**事务外**的行为与其他写方法不同——它会在 `hotKeyExecutor` 上异步执行（调用方立即返回，writer、版本递增、L1 更新和同步在后台线程完成）。事务外，其他方法（`invalidate`、`invalidateAll`、`putBeforeInvalidate`）在调用方线程上同步执行。

### 集合写入路径 — `putBeforeInvalidate`

用于集合增量操作（LPUSH、SADD、ZADD）：

```
putBeforeInvalidate(cacheKey, mutation)
├─ （如果在 Spring 事务内，延迟到 afterCommit）
├─ mutation.run() — L2 写入（调用方提供的 Runnable）
│  └─ 异常时 → 跳过本地失效和同步，记录错误日志
├─ nextVersion(cacheKey) — Redis INCR → VersionResult(dataVersion, isVersionDegraded)
├─ Caffeine 本地缓存失效
└─ CacheSyncPublisher.send()
     └─ RabbitMQ fanout (hotkey.sync.exchange)
          └─ TYPE_REFRESH 带 dataVersion 和 isVersionDegraded 头部
```

> **注意：** `mutation.run()` 与 L1 缓存失效之间存在约 1ms 窗口，并发 `get()` 可能命中 L1 旧值。这是刻意的取舍——在修改前失效会导致更严重的竞态（`get()` 用旧 Redis 数据重新填充 L1）。该窗口受限于一次 Redis 往返（`nextVersion` 调用）。

### 失效路径

```
invalidate(cacheKey)
├─ nextVersion(cacheKey) — Redis INCR → VersionResult(dataVersion)
├─ Caffeine 本地缓存失效
└─ CacheSyncPublisher.send()
     └─ TYPE_REFRESH（对端从 Redis 重新加载，跳过旧版本）

invalidateAll(cacheKeys)
├─ Caffeine 本地缓存失效（所有 key）
└─ CacheSyncPublisher.send() — 每个 key
     └─ TYPE_INVALIDATE with dataVersion=0L（对端无条件移除）
```

### 软过期读路径（`getWithSoftExpire`）

```
          ┌──────────────┐   L1 命中┌-──────────────┐
          │   请求       │ ───────→ │ softExpireAt  │
          │             │ ←───────  │  时间检查     |
          └──────┬───────┘  过期    └───────┬───────┘
                 │ 软过期？                │ 过期？
                 ↓ true                     ↓ yes
            返回过期旧值          triggerAsyncRefresh
            + add(key,1) +        ├─ refreshLimiter.tryAcquire()
            record(key)           │  （信号量，最大 refresh-concurrency）
                                  │  └─ 繁忙时跳过（下次 get 重试）
                                  └─ 异步（hotKeyExecutor）:
                                       L2 reader → Caffeine.put
                                       + 更新 softExpireAt
                                       + 保留原始 hardTtlMs
                │ L1 未命中（走正常路径）
                ↓
              SingleFlight.load(cacheKey, reader)
              （参见上方正常读路径）
               Caffeine.put(key, CacheEntry(
                 value, 0L, false, 0L,
                 hardTtlMs, hardExpireAtMs,
                 softTtlMs, softExpireAtMs,
                 keyState,
                 normalHardTtlMs, normalSoftTtlMs))
```

> **注意：** 软过期适用于 HOT 和 COOL 条目。NORMAL 条目总是直接返回，不会触发异步刷新——它们会在硬 TTL 过期后通过 `loadAndCache` 重新加载。如需纯粹的逻辑过期（硬 TTL 永不淘汰），向 `getWithSoftExpire` 传入 `hardTtlMs = Long.MAX_VALUE`——entry 永久驻留 Caffeine；仅 Caffeine `maximumSize` 可淘汰它，软过期永不移除 entry（只返回旧值并异步刷新）。

### 实例间缓存同步

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
                                     │实例 B    │  │实例 C    │  │   ...     │
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

> **注意：** 版本守卫使用 `dataVersion`（来自 Redis INCR 或退化回退的版本）。见下方[版本空间](#版本空间)了解 dataVersion 与 decisionVersion 的区别。

**版本比较（4 种情况）——仅适用于 `dataVersion`：**

| 本地 dataVersion | 传入 dataVersion | 结果                                                     |
| ---------------- | ---------------- | -------------------------------------------------------- |
| Normal           | Normal           | 数值大的版本获胜（传入 > 本地时替换）                    |
| Normal           | Degraded         | 本地胜出（跳过更新——退化对端数据不能覆盖健康的本地版本） |
| Degraded         | Normal           | 传入胜出（覆盖——健康的对端数据替换退化的本地版本）       |
| Degraded         | Degraded         | 数值大的版本获胜                                         |

### 版本空间

CacheEntry 维护**两个独立的版本空间**：

| 版本字段          | 来源                                                                | 可能退化？ | 使用方                                                 |
| ----------------- | ------------------------------------------------------------------- | ---------- | ------------------------------------------------------ |
| `dataVersion`     | `HotKeyCache.nextVersion()` — Redis INCR 或节点本地回退             | 是         | `VersionGuard.shouldSkipForSync()` (CacheSyncListener) |
| `decisionVersion` | `WorkerBroadcaster.decisionVersionCounter` — `AtomicLong`，永不退化 | 否         | `VersionGuard.shouldSkipForWorker()` (WorkerListener)  |

**规则：**

- `dataVersion` 跟踪实际数据变更版本，用于跨实例缓存同步。Redis 不可用时退化到节点本地计数器。
- `decisionVersion` 跟踪 Worker HOT/COOL 决策顺序。始终是 Worker 上干净的 `AtomicLong`——永不退化。
- 两个版本**正交**：数据变更不影响 `decisionVersion`，Worker 决策不影响 `dataVersion`。
- `putThrough` 保留 L1 条目的现有 `decisionVersion`。`loadAndCache` 设置 `decisionVersion=0L`（尚无 Worker 决策）。
- `CacheSyncListener` 在跨实例同步刷新期间保留现有条目的 `decisionVersion`。
- **`isVersionDegraded` 安全网：** 当现有 entry 的 `isVersionDegraded=true`（创建于 Redis 宕机期），`shouldSkipForWorker` 将无条件接受 Worker 决策，跳过 `decisionVersion` 比较。这防止了 Worker 重启（AtomicLong 重置）后被退化期 entry 的旧 `decisionVersion` 阻挡——退化期 entry 产生于不稳定期，应让位于更新的 Worker 决策。

### CacheEntry 字段

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

### 报告与 Worker 架构

用于集群维度热点检测：App 实例定期向专用 Worker 节点报告访问计数：

```
┌──────────────────────┐              ┌──────────────────────────────┐
│  App 实例            │              │  Worker 节点                 │
│                      │              │                              │
│  HotKeyReporter      │  定时        │  ReportConsumer              │
│  （每 100ms 批量     │ ──────────→  │  （接收报告 via               │
│   打包 TopK 数据）   │  RabbitMQ    │   hotkey.report.exchange）    │
│                      │              │         │                    │
│  ReportPublisher     │              │         ↓                    │
│  （发送到            │              │  SlidingWindowDetector       │
│   hotkey.report.     │              │  （无锁时间序列计数器         │
│   exchange）         │              │   每个 key 独立跟踪）         │
└──────────────────────┘              │         │                    │
                                      │         ↓                    │
                                      │  HotKeyStateMachine          │
                                      │  （key 级状态机：             │
                                      │   NORMAL→HOT→PRE_COOL→COOL   │
                                      │   →NORMAL）                  │
                                      │         │                    │
                                      │         ↓                    │
                                       │  WorkerBroadcaster           │
                                      │  （HOT/COOL 决策 via         │
                                      │   hotkey.broadcast.exchange） │
                                      │         │                    │
                                      └─────────┼────────────────────┘
                                                │ RabbitMQ fanout
                                                ↓
                              ┌─────────────────────────────────────┐
                              │  所有 App 实例                       │
                              │                                     │
                              │  WorkerListener（hotkey.worker-     │
                              │  listener.*）处理 HOT/COOL：        │
                              │  ┌─ TYPE_HOT  → 提升到 L1           │
                              │  │   （保留现有 dataVersion         │
                              │  │    + isVersionDegraded，存储     │
                              │  │    wm.decisionVersion())         │
                              │  └─ TYPE_COOL → 降级 L1             │
                              │      （保留两个版本，                │
                              │       存储 wm.decisionVersion())    │
                              └─────────────────────────────────────┘
```

> 详细 Worker 模式搭建、状态机转换和配置见 [WORKER.zh.md](WORKER.zh.md)。
