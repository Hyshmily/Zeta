# HotKey Request Paths

## 1. Get (Read Path)

### 1.1 `get(cacheKey, reader, hardTtlMs)`

```
caller
  │
  ├─ cacheKey invalid? → return Optional.empty()
  │
  ├─ caffeineCache.getIfPresent(cacheKey)
  │    │
  │    ├─ HIT ──→ extract value from CacheEntry/raw
  │    │           if (MODE_LOCAL) hotKeyDetector.add(key, 1)
  │    │           return value
  │    │
  │    └─ MISS ─→ singleFlight.load(key, reader)
  │                  │
  │                  ├─ MODE_WORKER:
  │                  │    caffeineCache.put(key, new CacheEntry(
  │                  │      value, VERSION_DEFAULT, false, expireAt(shortTtlMs), NORMAL, 0L))
  │                  │    hotKeyReporter.record(key)
  │                  │    return value
  │                  │
  │                  └─ MODE_LOCAL:
  │                       hotKeyDetector.add(key, TOPK_INCR)
  │                       │
  │                       ├─ isHotKey()? → caffeineCache.put(key, new CacheEntry(
  │                       │     value, VERSION_DEFAULT, false, expireAt(hardTtlMs), NORMAL,
  │                       │     softExpireManager.computeDefaultSoftExpireAt()))
  │                       │   broadcastPublisher.broadcastHotKey(key)
  │                       │   return value
  │                       │
  │                       └─ not hot → return value (not cached)
```

### 1.2 `getWithSoftExpire(cacheKey, reader, hardTtlMs, softTtlMs)`

```
caller
  │
  ├─ softExpire disabled? → fallback to get()
  │
  ├─ caffeineCache.getIfPresent(cacheKey)
  │    │
  │    ├─ HIT ──→ switch (mode)
  │    │           │
  │    │           ├─ WORKER:
  │    │           │    keyState == HOT && isExpired()?
  │    │           │      → softExpireManager.triggerAsyncRefresh(key, reader, softTtlMs)
  │    │           │    return cached
  │    │           │
  │    │           └─ LOCAL (default):
  │    │                isExpired()?
  │    │                  → softExpireManager.triggerAsyncRefresh(key, reader, softTtlMs)
  │    │                hotKeyDetector.add(key, TOPK_INCR)
  │    │                return cached
  │    │
  │    └─ MISS ─→ same as get() + softExpireAt field in CacheEntry
```

### 1.3 Soft Expire Refresh (async)

```
triggerAsyncRefresh(key, reader, softTtlMs)
  │
  ├─ disabled or rate-limited (Semaphore.tryAcquire) → skip
  │
  └─ CompletableFuture.supplyAsync(reader, executor)
       │
       └─ whenComplete((value, error) -> {
              error? → log WARN, release permit
              value != null?
                → caffeineCache.getIfPresent(key)
                  ├─ found → new CacheEntry(value, keep version/degraded/keyState/expireAt, computeSoftExpireAt(softTtlMs))
                  └─ not found → new CacheEntry(value, VERSION_DEFAULT, false, Long.MAX_VALUE, NORMAL, computeSoftExpireAt(softTtlMs))
              release permit
            })
```

## 2. PutThrough (Write Path)

### 2.1 `putThrough(cacheKey, value, writer, hardTtlMs, softTtlMs)`

```
caller
  │
  └─ TransactionSupport.runAfterCommit(task, hotKeyExecutor)
       │
       ├─ IN transaction? → register TransactionSynchronization.afterCommit
       │                     → deferred until commit
       │
       └─ OUTSIDE transaction? → CompletableFuture.runAsync(task, hotKeyExecutor)
                                  → async, non-blocking
                                         │
                                         task:
                                           writer.run()          // DB mutation
                                           nextVersion(key)      // Redis INCR
                                           caffeineCache.put(key, new CacheEntry(
                                             value, vr.version(), vr.degraded(),
                                             expireAt(hardTtlMs), NORMAL,
                                             softExpireManager.computeSoftExpireAt(softTtlMs)))
                                           switch (mode):
                                             WORKER → broadcastRefresh(key, version, degraded)
                                             LOCAL  → broadcastHotKeyWithVersion(key, version, degraded)
```

### 2.2 `nextVersion(cacheKey)` — Version Generation

```
caller
  │
  ├─ RedisTemplate present?
  │    ├─ YES → EVAL script: INCR hotkey:ver:{key}
  │    │        EXPIRE hotkey:ver:{key} versionKeyTtlMinutes*60
  │    │        success → VersionResult(v, false)
  │    │        catch   → VersionResult(System.nanoTime(), true)  // degraded=true
  │    │
  │    └─ NO  → VersionResult(System.nanoTime(), true)  // degraded=true
```

## 3. Invalidate Path

### 3.1 `invalidate(cacheKey)`

```
caller
  │
  └─ TransactionSupport.runNowOrAfterCommit(task)
       │
       ├─ IN transaction? → deferred until commit
       │
       └─ OUTSIDE transaction? → run immediately (sync)
                                  │
                                  task:
                                    nextVersion(key)           // Redis INCR
                                    caffeineCache.invalidate(key)
                                    switch (mode):
                                      WORKER → broadcastInvalidate(key, version, degraded)
                                      LOCAL  → broadcastHotKeyWithVersion(key, version, degraded)
```

### 3.2 `invalidateAll(Collection)`

```
caller
  │
  └─ TransactionSupport.runNowOrAfterCommit(task)
       │
       ├─ IN transaction? → deferred until commit
       │
       └─ OUTSIDE transaction? → run immediately (sync)
                                  │
                                  task:
                                    caffeineCache.invalidateAll(validKeys)
                                    broadcastPublisher?
                                      → forEach key:
                                          nextVersion(key)
                                          broadcastInvalidate/broadcastHotKeyWithVersion
```

### 3.3 `putBeforeInvalidate(cacheKey, mutation)`

```
caller
  │
  └─ TransactionSupport.runNowOrAfterCommit(task)
       │
       ├─ IN transaction? → deferred until commit
       │
       └─ OUTSIDE transaction? → run immediately (sync)
                                  │
                                  task:
                                    try { mutation.run() }    // DB mutation
                                    catch → log ERROR, return (skip invalidate)
                                    nextVersion(key)
                                    caffeineCache.invalidate(key)
                                    switch (mode):
                                      WORKER → broadcastInvalidate(key, version, degraded)
                                      LOCAL  → broadcastHotKeyWithVersion(key, version, degraded)
```

## 4. TransactionSupport Decision Matrix

| Method | IN transaction | OUTSIDE transaction | Why |
|--------|---------------|-------------------|-----|
| `runAfterCommit(task, executor)` | defer to `afterCommit` | `CompletableFuture.runAsync(task, executor)` | Writer may be slow (DB write), don't block caller |
| `runNowOrAfterCommit(task)` | defer to `afterCommit` | `task.run()` (sync) | Cache-only ops (invalidate/putBeforeInvalidate), need timeliness |

### Used By

| Public API | TransactionSupport method | Rationale |
|------------|--------------------------|-----------|
| `putThrough()` | `runAfterCommit` | writer.run() is caller-supplied DB write — async outside TX |
| `invalidate()` | `runNowOrAfterCommit` | lightweight cache invalidation — sync outside TX |
| `invalidateAll()` | `runNowOrAfterCommit` | batch invalidation — sync outside TX |
| `putBeforeInvalidate()` | `runNowOrAfterCommit` | mutation + invalidation — sync outside TX |

## 5. Broadcast Path

### 5.1 Publish Side

```
putThrough/invalidate
  │
  ├─ MODE_WORKER:
  │    broadcastRefresh(cacheKey, version, degraded)     → routingKey: "hotkey.broadcast.{appName}"
  │    broadcastInvalidate(cacheKey, version, degraded)  → routingKey: "hotkey.broadcast.{appName}"
  │
  └─ MODE_LOCAL:
       broadcastHotKeyWithVersion(cacheKey, version, degraded)  → routingKey: "hotkey.broadcast.{appName}"
```

Message headers (AMQP):
```
type:              HOT | COOL | REFRESH | INVALIDATE
version:           data version (long)
decisionVersion:   same as version (for dedup)
isVersionDegraded: false | true
source:            "topk_pre_warm" | "sliding_window" | null
```

### 5.2 Receive Side (`BroadcastListener`)

```
RabbitMQ message
  │
  ├─ Dedup check (decisionVersion > oldVersion?)
  │    → skip if already processed newer version
  │
  ├─ TYPE_HOT:
  │    handleHotKey()
  │      → isVersionDegraded Grade Guard (4-case comparison)
  │      → worker mode: broadcastListener directly puts to caffeine
  │      → sets KeyState.HOT, softExpireAt
  │
  ├─ TYPE_COOL:
  │    → set KeyState.COOL, clear softExpireAt
  │
  ├─ TYPE_REFRESH:
  │    → version-based freshness check
  │    → re-read from Redis (redisLoader)
  │    → update CacheEntry preserving keyState/expireAt/softExpireAt
  │
  └─ TYPE_INVALIDATE:
       → broadcastVersionGradeGuard
       → caffeineCache.invalidate(key)
```

## 6. Worker Mode Path

### 6.1 Client Side

```
caller (MODE_WORKER)
  │
  ├─ get(): cache miss → caffeineCache.put(value, shortTtlMs)
  │         → hotKeyReporter.record(key)
  │
  └─ Reporter sends to Worker (via RabbitMQ: "hotkey.report.{appName}.{shardIndex}")
```

### 6.2 Worker Side

```
Worker receives report
  │
  ├─ TopKValidator:
  │    SlidingWindowDetector + HeavyKeeper
  │    if (topK.contains(key)) → broadcaster.broadcastHot(key, "topk_pre_warm")
  │
  ├─ ReportConsumer:
  │    SlidingWindowDetector
  │    if (detected) → broadcaster.broadcastHot(key, "sliding_window")
  │
  └─ WorkerBroadcaster:
       → RabbitMQ message (raw bytes + AMQP headers)
       → routingKey: "hot.{appName}" | "cool.{appName}"
       → source identifies which detector triggered
```

### 6.3 Receiving Back on Client

```
WorkerBroadcaster message → RabbitMQ → BroadcastListener
  │
  ├─ TYPE_HOT (source="topk_pre_warm" | "sliding_window"):
  │    → handleHotKey() / broadcastVersionGradeGuard
  │    → caffeineCache.put(key, CacheEntry(
  │         value, 0L (version from worker), false,
  │         Long.MAX_VALUE, KeyState.HOT,
  │         computeSoftExpireAt(softTtlMs)))
  │
  └─ TYPE_COOL:
       → set KeyState.COOL
```

## 7. KeyState Lifecycle

```
     putThrough() / get()
          │
          ▼
      KeyState.NORMAL      ← default, not yet classified
          │
          │ (Worker broadcast received)
          ▼
      KeyState.HOT          ← confirmed hot by Worker
          │
          │ (Worker broadcast: key cooled down)
          ▼
      KeyState.COOL         ← no longer hot
          │
          │ (transition state, Worker mode)
          ▼
      KeyState.PRE_COOL     ← about to cool down
```

### Where KeyState is Checked

| Location | Check | Meaning |
|----------|-------|---------|
| `isHotKey(cacheKey)` (WORKER) | `KeyState.HOT == keyState` | "Is this key actively hot?" |
| `getWithSoftExpire()` (WORKER) | `KeyState.HOT == keyState` | "Should async refresh this entry?" |

`KeyState.NORMAL` entries are treated as non-hot by both checks (enum equality returns false).
