[← Back to Home](README.md)

## Method Call Chain

### Read Path — `get`

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
           ├─ caffeineCache.put(key, new CacheEntry(...), ttl)
           │    ├─ hot  → keyState=HOT, hot TTLs (hotHardTtl/hotSoftTtl)
           │    │        [dataVersion=0L, decisionVersion=0L]
           │    └─ not hot → keyState=NORMAL, normal TTLs
           │                 [dataVersion=0L, decisionVersion=0L]
           └─ return Optional.of(value)
```

> **Note:** L1 命中时 `CacheEntry` 由 sync 广播（REFRESH/INVALIDATE/INVALIDATE_ALL）或 Worker 决策（HOT/COOL）写入，其中的 `dataVersion` 和 `decisionVersion` 由发送方决定。L1 未命中由 `loadAndCache` 创建，版本字段均为 0。`loadAndCache` 缓存所有加载的 key（不仅仅是热点），根据 `KeyState` 区分 TTL。

### Soft Expire Read Path — `getWithSoftExpire`

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

> **Note:** 异步刷新保留原 CacheEntry 的硬 TTL（`hardTtlMs` / `hardExpireAtMs`），仅更新值和软过期时间。

### Peek Path — `peek`

```
HotKey.peek(cacheKey)
└─ HotKeyCache.peek(cacheKey)
     └─ caffeineCache.getIfPresent(key)                       [仅 L1，无副作用]
          └─ return Optional.ofNullable(entry.value)
          [⚠ 不调用 hotKeyDetector.add / hotKeyReporter.record / L2 reader]
```

### Write-Through Path — `putThrough`

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

### Collection Write Path — `putBeforeInvalidate`

```
HotKey.putBeforeInvalidate(key, mutation)
└─ HotKeyCache.putBeforeInvalidate(key, mutation)
     ├─ (事务内) → TransactionSupport.registerAfterCommit()
     │    └─ afterCommit → 执行以下全部
     │
     ├─ (非事务) → 同步执行（调用者线程）
     │
     ├─ mutation.run()                                        [用户变更 L2/DB]
     │    └─ 异常 → log.error, 跳过后续 L1 失效和广播
     │
     ├─ nextVersion(key)                                      [同 putThrough]
     ├─ caffeineCache.invalidate(key)                         [L1 移除，非 put]
      └─ CacheSyncPublisher.broadcastLocalInvalidate(key, version, degraded)
           └─ 同 putThrough (但 type=INVALIDATE instead of REFRESH)
```

> **Note:** `mutation.run()` 和 L1 失效之间存在约 1ms 窗口，并发 `get()` 可能命中旧值。这是故意的权衡——先失效再变更会导致 `get()` 读取到老数据并重新填充 L1，窗口更长。

### Single Key Invalidate — `invalidate`

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

### Batch Invalidate — `invalidateAll`

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

### State Query — `isLocalHotKey`

```
HotKey.isLocalHotKey(cacheKey)
└─ HotKeyCache.isLocalHotKey(cacheKey)
     └─ caffeineCache.getIfPresent(key)
          ├─ 存在且 keyState == HOT → true
          └─ 其他 → false
          [⚠ 纯 L1 查询，无任何副作用]
```

### State Query — `isWorkerHotKey`

```
HotKey.isWorkerHotKey(cacheKey)
└─ workerTopKAlgorithm.list().stream().anyMatch(item -> item.key().equals(cacheKey))
     ├─ key 在 Worker TopK 中 → true
     └─ 不在 → false
     [⚠ 遍历 Worker TopK 列表，O(n)；无网络调用]
```

### TopK Query Methods

```
HotKey.returnHotKeys()
└─ (topKAlgorithm != null)
     ├─ yes → topKAlgorithm.list()                             [本地 HeavyKeeper Top-K]
     └─ no  → Collections.emptyList()

HotKey.returnExpelledHotKeys()
└─ (topKAlgorithm != null)
     ├─ yes → topKAlgorithm.expelledItems()                    [被挤出的热点 key 队列]
     └─ no  → 空 LinkedBlockingQueue

HotKey.returnTotalDataStreams()
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

> **TopK null safety:** All 6 query methods above return empty/0 when the corresponding TopK is unavailable (Worker-only mode or not configured). This differs from `get`/`putThrough` etc., which throw `UnsupportedOperationException` in Worker-only mode via `requireCache()`.

### Rule Management API

```
HotKey.addBlacklist(keyPattern)
└─ HotKeyCache.addBlacklist(keyPattern)
     └─ ruleMatcher.addRule(Rule.of(keyPattern, BLOCK))
          └─ [if Redis] → persist to Redis
          └─ [if syncPublisher] → broadcastAllLocalRules()
          [auto-detects pattern type: exact, prefix (* suffix), wildcard, regex]

HotKey.removeBlacklist(keyPattern)
└─ HotKeyCache.unBlacklist(keyPattern)
     └─ ruleMatcher.removeRule(keyPattern, BLOCK)
          └─ [if Redis] → persist to Redis
          └─ [if syncPublisher] → broadcastAllLocalRules()

HotKey.addWhitelist(keyPattern)
└─ HotKeyCache.addWhitelist(keyPattern)
     └─ ruleMatcher.addRule(Rule.of(keyPattern, ALLOW_NO_REPORT))
          └─ [if Redis] → persist to Redis
          └─ [if syncPublisher] → broadcastAllLocalRules()

HotKey.removeWhitelist(keyPattern)
└─ HotKeyCache.unWhitelist(keyPattern)
     └─ ruleMatcher.removeRule(keyPattern, ALLOW_NO_REPORT)
          └─ [if Redis] → persist to Redis
          └─ [if syncPublisher] → broadcastAllLocalRules()

HotKey.evaluateRule(cacheKey)
└─ ruleMatcher.evaluateRule(cacheKey)
     └─ returns BLOCK / ALLOW_NO_REPORT / ALLOW
     [called internally by get/putThrough/putBeforeInvalidate; public for manual check]

HotKey.getAllRules()
└─ HotKeyCache.getAllRules()
     └─ ruleMatcher.getAllRules() → List<Rule>
     [snapshot of current rules in evaluation order; empty if no cache]

HotKey.clearAllRules()
└─ HotKeyCache.clearAllRules()
     └─ ruleMatcher.clearAllRules()
          └─ [if Redis] → delete from Redis
          └─ [if syncPublisher] → broadcastAllLocalRules()
     [removes both blacklist and whitelist rules]

HotKey.broadcastAllLocalRulesManually()
└─ HotKeyCache.broadcastAllLocalRulesManually()
     └─ ruleMatcher.exportRulesJson()
          └─ CacheSyncPublisher.broadcastAllLocalRules(json)
               └─ Single AMQP message, body = JSON rule array
                    header: type=RULES_SYNC
     [manual trigger for initial cluster sync]
```

### Design Notes

**Why does `invalidate` send REFRESH while `invalidateAll` sends INVALIDATE_ALL?**
Single-key invalidation typically means the key will be read soon — REFRESH tells peers to actively reload from Redis, reducing the next `get()` latency. Batch invalidation may involve many keys not immediately accessed — `invalidateAll` uses a single TYPE_INVALIDATE_ALL message (JSON array of keys) to let peers lazily reload on next `get()`.

**Why doesn't `invalidateAll` call INCR?**
Each INCR is a Redis call. `invalidateAll` is designed for bulk cleanup (cache expiration, batch data change notifications), prioritizing performance over version consistency. For per-key version control, loop over `invalidate()` instead.

**Why does `fallbackVersion` use `Long.MIN_VALUE + counter`?**
All degraded versions are negative, sorting below normal (positive) Redis INCR versions in `sendDeduped`'s `> version` comparison. This ensures normal versions always win over degraded local versions without extra `degraded` flag logic.

**Why does `putThrough` run async (non-transactional) while other write methods run sync?**
`putThrough`'s `writer.run()` may involve L2/DB network I/O — async execution avoids blocking the caller. `invalidate`/`invalidateAll`/`putBeforeInvalidate` are lightweight (cache operation + broadcast), so synchronous execution guarantees the broadcast is sent by the time the caller returns.
