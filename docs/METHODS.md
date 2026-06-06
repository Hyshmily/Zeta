[← Back to Home](README.md)

## Method Call Chain

### Read Path — `get`

```
HotKey.get(cacheKey, reader[, hardTtlMs, softTtlMs])
└─ HotKeyCache.get(cacheKey, reader, effectiveHardTtl, effectiveSoftTtl)
      ├─ TransactionSupport.runNowOrAfterCommit()              [tx-aware, L2 async write path only]
      ├─ caffeineCache.getIfPresent(key)                       [L1 lookup]
      │    ├─ hit → unwrap CacheEntry
      │    ├─ KeyState == HOT → use hot TTLs (hotHardTtl / hotSoftTtl)
      │    └─ KeyState != HOT → use normal TTLs (normalHardTtl / normalSoftTtl)
      ├─ hotKeyDetector.add(key, 1)                       [local HeavyKeeper frequency count]
      ├─ hotKeyReporter.record(key)                       [app→Worker report]
      └─ return Optional.of(entry.value)
      │
      └─ miss → SingleFlight.execute(key, supplier)          [concurrency dedup]
           ├─ supplier.get() → reader.get()                    [caller-supplied L2/DB read]
          ├─ hotKeyDetector.add(key, 1)
           ├─ hotKeyReporter.record(key)
           ├─ caffeineCache.put(key, new CacheEntry(...), ttl)
           │    ├─ hot  → keyState=HOT, hot TTLs (hotHardTtl/hotSoftTtl)
           │    │        [dataVersion=0L, decisionVersion=0L]
           │    └─ not hot → keyState=NORMAL, normal TTLs
           │                 [dataVersion=0L, decisionVersion=0L]
           └─ return Optional.of(value)
```

> **Note:** When L1 is hit, the `CacheEntry` was written by a sync broadcast (REFRESH/INVALIDATE/INVALIDATE_ALL) or Worker decision (HOT/COOL). The `dataVersion` and `decisionVersion` are determined by the sender. L1 miss entries are created by `loadAndCache` with version fields set to 0. `loadAndCache` caches all loaded keys (not just hot ones), differentiating TTLs by `KeyState`.

### Soft Expire Read Path — `getWithSoftExpire`

```
HotKey.getWithSoftExpire(key, reader[, softTtlMs][, hardTtlMs, softTtlMs])
└─ HotKeyCache.getWithSoftExpire(key, reader, ...)
      ├─ caffeineCache.getIfPresent(key) → CacheEntry
      │    ├─ hard TTL not expired → same as get path
      │    ├─ hard TTL expired → return empty (same as get path)
      │    └─ soft TTL expired but hard TTL not expired
      │         ├─ return stale value immediately
      │         ├─ hotKeyDetector.add(key, 1)
      │         ├─ hotKeyReporter.record(key)
      │         └─ async refresh task:
      │              ├─ refreshLimiter.tryAcquire()
      │              │    └─ rate limited → skip this refresh
      │              └─ executor.submit(() → reader.get())
      │                   ├─ caffeineCache.put(key, newEntry)
      │                   └─ update softExpireAtMs
      │
      └─ miss → SingleFlight.execute → same as get path
```

> **Note:** Async refresh preserves the original CacheEntry's hard TTL (`hardTtlMs` / `hardExpireAtMs`), only updating the value and soft expire time.

### Peek Path — `peek`

```
HotKey.peek(cacheKey)
└─ HotKeyCache.peek(cacheKey)
      └─ caffeineCache.getIfPresent(key)                       [L1 only, no side effects]
           └─ return Optional.ofNullable(entry.value)
           [⚠ does not call hotKeyDetector.add / hotKeyReporter.record / L2 reader]
```

### Write-Through Path — `putThrough`

```
HotKey.putThrough(key, value, writer[, hardTtlMs, softTtlMs])
└─ HotKeyCache.putThrough(key, value, writer, effectiveHardTtl, effectiveSoftTtl)
      ├─ (tx) → TransactionSupport.registerAfterCommit()
      │    └─ afterCommit → execute all below
      │
      ├─ (non-tx) → async: hotKeyExecutor.submit(() → execute all below)
     │
      ├─ writer.run()                                          [caller writes to L2/DB]
      │    └─ exception → log.error, continue (L1 and broadcast still execute)
      │
      ├─ nextVersion(key)                                      [version generation]
      │    ├─ Redis available:
      │    │    └─ Lua: "INCR KEYS[1]; EXPIRE KEYS[1] ARGV[1]"
      │    │         → dataVersion=positive, degraded=false
      │    └─ Redis unavailable:
     │         └─ fallbackVersionCounter.incrementAndGet()
     │              → dataVersion=Long.MIN_VALUE + counter, degraded=true
     │
     ├─ caffeineCache.put(key, CacheEntry(
     │      value, dataVersion, degraded,
      │      decisionVersion=0L,                                 [always reset on write-through]
      │      keyState=NORMAL,                                   [always stored as NORMAL]
     │      hardTtlMs, softTtlMs, ...))
     │
     └─ CacheSyncPublisher.broadcastRefresh(key, version, degraded)
          └─ sendDeduped(key, "REFRESH", version, degraded)
               ├─ recentBroadcasts.compute("REFRESH:"+key, (_, old) →
               │    old != null && old > version ? old : version)
│    └─ return != version → newer version exists, skip broadcast
│    └─ return == version → send message
               └─ rabbitTemplate.send(exchange, "", msg)
                    [header: type=REFRESH, version, isVersionDegraded]
```

### Collection Write Path — `putBeforeInvalidate`

```
HotKey.putBeforeInvalidate(key, mutation)
└─ HotKeyCache.putBeforeInvalidate(key, mutation)
      ├─ (tx) → TransactionSupport.registerAfterCommit()
      │    └─ afterCommit → execute all below
      │
      ├─ (non-tx) → sync (caller thread)
     │
     ├─ mutation.run()                                        [caller mutates L2/DB]
     │    └─ exception → log.error, skip subsequent L1 invalidation and broadcast
     │
     ├─ nextVersion(key)                                      [same as putThrough]
     ├─ caffeineCache.invalidate(key)                         [L1 remove, not put]
       └─ CacheSyncPublisher.broadcastLocalInvalidate(key, version, degraded)
            └─ same as putThrough (but type=INVALIDATE instead of REFRESH)
```

> **Note:** There is a ~1ms window between `mutation.run()` and L1 invalidation where a concurrent `get()` may hit the stale L1 value. This is a deliberate trade-off — invalidating before the mutation would cause a worse race where `get()` re-populates L1 with old data, creating a longer window.

### Single Key Invalidate — `invalidate`

```
HotKey.invalidate(cacheKey)
└─ HotKeyCache.invalidate(cacheKey)
     ├─ invalidCacheKey(key) → return                          [skip null/empty]
     ├─ TransactionSupport.runNowOrAfterCommit()
     │    ├─ (tx) → deferred to afterCommit
     │    └─ (non-tx) → sync
     │
     ├─ nextVersion(key)
│    ├─ Redis available: INCR + EXPIRE → positive version, degraded=false
│    └─ Redis unavailable: Long.MIN_VALUE + counter → degraded=true
     │
     ├─ caffeineCache.invalidate(key)                          [L1 remove]
     │
     └─ CacheSyncPublisher.broadcastRefresh(key, version, degraded)
          └─ sendDeduped(key, "REFRESH", version, degraded)
                ├─ recentBroadcasts.compute(...)                [dedup]
               └─ rabbitTemplate.send()
                    ↓
                ┌─ remote: CacheSyncListener.handleRefresh(msg)
                │    ├─ extract dataVersion + isVersionDegraded
                │    ├─ VersionGuard.shouldSkipForSync():
                │    │    ├─ Normal vs Normal: higher wins
                │    │    ├─ Normal vs Degraded: Normal wins (skip degraded)
                │    │    ├─ Degraded vs Normal: Normal wins (load normal)
                │    │    └─ Degraded vs Degraded: higher wins
                │    ├─ pass → redisReader.get(key)              [reload from Redis]
                │    ├─ caffeineCache.put(key, newEntry)
                │    │    [decisionVersion=preserve existing]
                │    └─ return
                └─
```

### Batch Invalidate — `invalidateAll`

```
HotKey.invalidateAll(keys...) / invalidateAll(Collection)
└─ HotKeyCache.invalidateAll(keys)
      ├─ stream().filter(k → !invalidCacheKey(k)).toList()      [skip null/empty]
      ├─ empty list → log.debug, return
      │
      ├─ TransactionSupport.runNowOrAfterCommit()
      │    ├─ (tx) → deferred to afterCommit
      │    └─ (non-tx) → sync
     │
       ├─ caffeineCache.invalidateAll(validKeys)                 [L1 batch remove]
      │
       └─ (has syncPublisher)
           └─ CacheSyncPublisher.broadcastLocalInvalidateAll(validKeys)
                 └─ single AMQP message
                      │    body = JSON array [key1, key2, ...]
                     │    header: type=INVALIDATE_ALL
                     ↓
                 ┌─ remote: CacheSyncListener.handleInvalidateAll(msg)
                 │    └─ caffeineCache.invalidateAll(keys)       [L1 batch remove]
                 │    [⚠ no Redis reload, no version check]
                └─
```

### State Query — `isLocalHotKey`

```
HotKey.isLocalHotKey(cacheKey)
└─ HotKeyCache.isLocalHotKey(cacheKey)
      └─ caffeineCache.getIfPresent(key)
           ├─ exists and keyState == HOT → true
           └─ otherwise → false
           [⚠ pure L1 lookup, no side effects]
```

### State Query — `isWorkerHotKey`

```
HotKey.isWorkerHotKey(cacheKey)
└─ workerTopKAlgorithm.list().stream().anyMatch(item -> item.key().equals(cacheKey))
     ├─ key in Worker TopK → true
     └─ not in → false
     [⚠ iterates Worker TopK list, O(n); no network call]
```

### TopK Query Methods

```
HotKey.returnLocalHotKeys()
└─ (topKAlgorithm != null)
      ├─ yes → topKAlgorithm.list()                             [local HeavyKeeper Top-K]
      └─ no  → Collections.emptyList()

HotKey.returnLocalExpelledHotKeys()
└─ (topKAlgorithm != null)
     ├─ yes → topKAlgorithm.expelledItems()                    [expelled hot key queue]
     └─ no  → empty LinkedBlockingQueue

HotKey.returnLocalTotalDataStreams()
└─ (topKAlgorithm != null)
     ├─ yes → topKAlgorithm.totalDataStreams()                 [total access count]
     └─ no  → 0L

HotKey.returnWorkerHotKeys()
└─ (workerTopKAlgorithm != null)
     ├─ yes → workerTopKAlgorithm.list()                       [Worker-side cluster Top-K]
     └─ no  → Collections.emptyList()

HotKey.returnWorkerExpelledHotKeys()
└─ (workerTopKAlgorithm != null)
     ├─ yes → workerTopKAlgorithm.expelledItems()
     └─ no  → empty LinkedBlockingQueue

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
