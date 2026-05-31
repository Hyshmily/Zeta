[← 返回首页](README.zh.md)

## 架构

### 正常读路径（`get`）

```
┌──────────────┐   L1 命中              ┌──────────────┐
│   请求       │ ─── add(key,1) ─────→  │  Caffeine L1 │
│             │ ←────────────────────   │  (本地)      │
└──────┬───────┘   Optional.of(value)   └──────┬───────┘
       │ L1 未命中        (自动解包           │ isHotKey()?
       ↓ (inflight 去重)  CacheEntry)          ↓
┌──────────────┐  ──── reader ────→  ┌───────────────┐
│  L2 存储     │  ──add(key,1)───→   │     TopK      │
│  (可插拔)    │                     │  (接口)        │
└──┬───────┬───┘                     ├───────────────┤
    │ 命中  │ null                    │ add()→Result  │
    ↓       ↓                         │ list()        │
 Optional   Optional.empty()          │ listTopN(n)   │
 .of(value)   r.isEmpty() → DB        │ total()       │
                                      │ contains()    │
                                      │ expelled()    │
                                      │ fading()      │
                                      └───────┬───────┘
                                             │ isHotKey()?
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
                                   version=0L,
                                   isVersionDegraded=false,
                                   hardTtlMs, hardExpireAtMs,
                                   softTtlMs, softExpireAtMs,
                                   keyState,
                                   normalHardTtlMs, normalSoftTtlMs))
                                  （读路径不触发同步）
```

> **注意：** `isHotKey()` 检查 L1 中 key 的 `KeyState` 是否为 HOT。key 以 `CacheEntry` 包装形式存储，包含完整 TTL 元数据——`get()` 路径自动解包为原始值。

### 写路径 — `putThrough`

```
putThrough(cacheKey, value, writer)
├─ （如果在 Spring 事务内，延迟到 afterCommit）
├─ writer.run() — L2 写入（调用方提供的 Runnable）
├─ nextVersion(cacheKey) — Redis INCR → VersionResult(version, isVersionDegraded)
│  └─ Redis 失败时 → System.nanoTime() 回退（degraded=true）
├─ Caffeine.put(cacheKey, CacheEntry(
│    value, version, isVersionDegraded,
│    hardTtlMs, hardExpireAtMs,
│    softTtlMs, softExpireAtMs,
│    keyState,
│    normalHardTtlMs, normalSoftTtlMs))
└─ CacheSyncPublisher.send()
     └─ RabbitMQ fanout (hotkey.sync.exchange)
          └─ TYPE_REFRESH 带版本号和 isVersionDegraded 头部
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
├─ nextVersion(cacheKey) — Redis INCR → VersionResult(version, isVersionDegraded)
├─ Caffeine 本地缓存失效
└─ CacheSyncPublisher.send()
     └─ RabbitMQ fanout (hotkey.sync.exchange)
          └─ TYPE_REFRESH 带版本号和 isVersionDegraded 头部
```

> **注意：** `mutation.run()` 与 L1 缓存失效之间存在约 1ms 窗口，并发 `get()` 可能命中 L1 旧值。这是刻意的取舍——在修改前失效会导致更严重的竞态（`get()` 用旧 Redis 数据重新填充 L1）。该窗口受限于一次 Redis 往返（`nextVersion` 调用）。

### 失效路径

```
invalidate(cacheKey)
├─ nextVersion(cacheKey) — Redis INCR → VersionResult
├─ Caffeine 本地缓存失效
└─ CacheSyncPublisher.send()
     └─ TYPE_REFRESH（对端从 Redis 重新加载，跳过旧版本）

invalidateAll(cacheKeys)
├─ Caffeine 本地缓存失效（所有 key）
└─ CacheSyncPublisher.send() — 每个 key
     └─ TYPE_INVALIDATE with version=0L（对端无条件移除）
```

### 软过期读路径（`getWithSoftExpire`）

```
         ┌──────────────┐   L1 命中┌-──────────────┐
         │   请求       │ ───────→ │ softExpireAt  │
         │             │ ←───────  │  时间检查      |
         └──────┬───────┘  过期    └───────┬───────┘
                │ 软过期？                │ 过期？
                ↓ true                     ↓ yes
           返回过期旧值          triggerAsyncRefresh
           + 检查 TopK            ├─ refreshLimiter.tryAcquire()
                                  │  （信号量，最大 refresh-concurrency）
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
                value, 0L, false,
                hardTtlMs, hardExpireAtMs,
                softTtlMs, softExpireAtMs,
                keyState,
                normalHardTtlMs, normalSoftTtlMs))
```

> **注意：** 软过期适用于 HOT 和 COOL 条目。普通条目始终新鲜加载。异步刷新保留原始 per-entry 硬 TTL。

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

**版本比较（4 种情况）：**

| 本地版本 | 传入版本 | 结果                   |
| -------- | -------- | ---------------------- |
| Normal   | Normal   | 高版本获胜（数值比较） |
| Normal   | Degraded | 本地胜出（跳过更新）   |
| Degraded | Normal   | 传入胜出（覆盖）       |
| Degraded | Degraded | 高版本获胜（数值比较） |

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
                                      │   hotkey.worker.exchange）   │
                                      │         │                    │
                                      └─────────┼────────────────────┘
                                                │ RabbitMQ fanout
                                                ↓
                              ┌─────────────────────────────────────┐
                              │  所有 App 实例                       │
                              │                                     │
                              │  WorkerListener（hotkey.worker-      │
                              │  listener.*）处理 HOT/COOL：         │
                              │  ┌─ TYPE_HOT  → 提升到 L1            │
                              │  └─ TYPE_COOL → 降级 L1              │
                              └─────────────────────────────────────┘
```

> 详细 Worker 模式搭建、状态机转换和配置见 [README.zh.WORKER.md](README.zh.WORKER.md)。
