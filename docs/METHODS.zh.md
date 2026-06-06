[← 返回首页](README.zh.md)

## 方法调用路径

### 读取路径 — `get`

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
           │    ├─ hot  → keyState=HOT, 热点 TTL (hotHardTtl/hotSoftTtl)
           │    │        [dataVersion=0L, decisionVersion=0L]
           │    └─ not hot → keyState=NORMAL, 普通 TTL
           │                 [dataVersion=0L, decisionVersion=0L]
           └─ return Optional.of(value)
```

> **注意：** L1 命中时 `CacheEntry` 由 sync 广播（REFRESH/INVALIDATE/INVALIDATE_ALL）或 Worker 决策（HOT/COOL）写入，其中的 `dataVersion` 和 `decisionVersion` 由发送方决定。L1 未命中由 `loadAndCache` 创建，版本字段均为 0。`loadAndCache` 缓存所有加载的 key（不仅热点），根据 `KeyState` 区分 TTL。

### 软过期读取路径 — `getWithSoftExpire`

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

> **注意：** 异步刷新保留原 CacheEntry 的硬 TTL（`hardTtlMs` / `hardExpireAtMs`），仅更新值和软过期时间。

### 窥视路径 — `peek`

```
HotKey.peek(cacheKey)
└─ HotKeyCache.peek(cacheKey)
     └─ caffeineCache.getIfPresent(key)                       [仅 L1，无副作用]
          └─ return Optional.ofNullable(entry.value)
          [⚠ 不调用 hotKeyDetector.add / hotKeyReporter.record / L2 reader]
```

### 写穿透路径 — `putThrough`

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
           └─ 同 putThrough（但 type=INVALIDATE 非 REFRESH）
```

> **注意：** `mutation.run()` 和 L1 失效之间存在约 1ms 窗口，并发 `get()` 可能命中旧值。这是故意的权衡——先失效再变更会导致 `get()` 读取到老数据并重新填充 L1，窗口更长。

### 单 key 失效 — `invalidate`

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

```
HotKey.isLocalHotKey(cacheKey)
└─ HotKeyCache.isLocalHotKey(cacheKey)
     └─ caffeineCache.getIfPresent(key)
          ├─ 存在且 keyState == HOT → true
          └─ 其他 → false
          [⚠ 纯 L1 查询，无任何副作用]
```

### 状态查询 — `isWorkerHotKey`

```
HotKey.isWorkerHotKey(cacheKey)
└─ workerTopKAlgorithm.list().stream().anyMatch(item -> item.key().equals(cacheKey))
     ├─ key 在 Worker TopK 中 → true
     └─ 不在 → false
     [⚠ 遍历 Worker TopK 列表，O(n)；无网络调用]
```

### TopK 查询方法

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

```
HotKey.addBlacklist(keyPattern)
└─ HotKeyCache.addBlacklist(keyPattern)
     └─ ruleMatcher.addRule(Rule.of(keyPattern, BLOCK))
          └─ [有 Redis] → 持久化到 Redis
          └─ [有 syncPublisher] → broadcastAllLocalRules()
          [自动检测模式类型: 精确、前缀、通配符、正则]

HotKey.removeBlacklist(keyPattern)
└─ HotKeyCache.unBlacklist(keyPattern)
     └─ ruleMatcher.removeRule(keyPattern, BLOCK)
          └─ [有 Redis] → 持久化到 Redis
          └─ [有 syncPublisher] → broadcastAllLocalRules()

HotKey.addWhitelist(keyPattern)
└─ HotKeyCache.addWhitelist(keyPattern)
     └─ ruleMatcher.addRule(Rule.of(keyPattern, ALLOW_NO_REPORT))
          └─ [有 Redis] → 持久化到 Redis
          └─ [有 syncPublisher] → broadcastAllLocalRules()

HotKey.removeWhitelist(keyPattern)
└─ HotKeyCache.unWhitelist(keyPattern)
     └─ ruleMatcher.removeRule(keyPattern, ALLOW_NO_REPORT)
          └─ [有 Redis] → 持久化到 Redis
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
          └─ [有 Redis] → 删除 Redis 中规则
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

### 设计说明

**为什么 `invalidate` 发 REFRESH 而 `invalidateAll` 发 INVALIDATE_ALL？**
单 key 失效预期很快会被读取，发送 REFRESH 让对端主动从 Redis 重新加载，减少下一次 `get()` 的延迟。批量失效可能涉及大量 key 且不一定立即被访问——`invalidateAll` 用一条 TYPE_INVALIDATE_ALL 消息（JSON key 数组）让对端惰性加载。

**为什么 `invalidateAll` 不走 INCR？**
每次 INCR 是一次 Redis 调用。`invalidateAll` 设计用于批量清理（缓存过期、批量数据变更通知），性能优先于版本一致性。如每个 key 都需要版本控制，应循环调用 `invalidate()`。

**为什么 `fallbackVersion` 用 `Long.MIN_VALUE + counter`？**
所有降级版本为负数，在 `sendDeduped` 的 `> version` 比较中始终低于正常（正数）Redis INCR 版本。确保正常版本永远优先于降级本地版本，无需额外 `degraded` 标志逻辑。

**`putThrough` 非事务时为什么异步，而其他写方法同步？**
`putThrough` 的 `writer.run()` 可能涉及 L2/DB 网络 IO，异步执行避免阻塞调用者。`invalidate`/`invalidateAll`/`putBeforeInvalidate` 预期轻量（仅缓存操作+广播），同步执行保证调用者返回时广播已发出。
