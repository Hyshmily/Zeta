[← Back to Home](README.md)

# HotKey Architecture — Complete Reference

This document consolidates the full architecture, method call chains, Worker mode, and supplementary diagrams for the HotKey library.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Method Call Chains](#method-call-chains)
- [Worker Mode](#worker-mode)
- [Supplementary Diagrams](#supplementary-diagrams)
- [Design Notes](#design-notes)

---

## Architecture Overview

### Normal Read Path (`get`)

<!-- Source: HotKeyCache.java:138-162, loadAndCache:247-312 -->

```
╔══════════════════════════╗                           ╔══════════════════════════════╗
║   Request                │ ──── record(key) ──────▶ ║  Caffeine L1 (local)         ║
║                          │ ◀──── Optional.of(v) ─── ║  CacheEntry wrapper          ║
╚══════════════╤═══════════╝                           ╚══════════════╤═══════════════╝
               │ L1 hit: unwrap + report                          │ isLocalHotKey()?
               │ (inflight dedup)                                 ▼
               │                                      ╔═══════════════════════════════╗
               │                                      ║   TopK (HeavyKeeper)          ║
               │                                      ║   add() → AddResult           ║
               │                                      ║   list() / listTopN(n)        ║
               ▼                                      ║   total() / contains()        ║
╔══════════════════════════╗  reader.run()            ║   expelled() / fading()       ║
║  L2 Storage (pluggable) ║ ────────────────────▶    ╚══════════════╤════════════════╝
║  Redis / DB / Custom    ║ ──── add(key,1) ──────▶                 │
╚════════╤═══════╤═════════╝ ──── record(key) ─────▶                ▼
     hit ▼       ▼ null                                  ╔════════════════════════════╗
  ┌─────────┐  ┌─────────────┐                           ║  Choose TTL by KeyState:   ║
  │Optional.│  │Optional.    │                           ║  HOT   → hot TTLs          ║
  │ of(v)   │  │empty()→DB   │                           ║  other → normal TTLs       ║
  └────┬────┘  └──────┬──────┘                           ╚══════════════╤═════════════╝
       └──────┬───────┘                                                 │
              ▼                                                         ▼
╔══════════════════════════════════════╗  ┌──────────────────────────────────────────┐
║  SingleFlight.load(key, reader)      ║  │  Caffeine.put(key, CacheEntry {          ║
║  → supplier.get()                    ║  │    value, dataVersion=0L,                ║
║  → add(key,1) + record(key)          ║  │    isVersionDegraded=false,              ║
║  → Caffeine.put(key, entry)          ║  │    decisionVersion=0L,                   ║
║  (no sync on read path)              ║  │    hardTtlMs, hardExpireAtMs,            ║
╚══════════════════════════════════════╝  │    softTtlMs, softExpireAtMs,            ║
                                          │    keyState,                             ║
                                          │    normalHardTtlMs, normalSoftTtlMs })   ║
                                          └──────────────────────────────────────────┘
```

> [!NOTE] `isLocalHotKey()` checks for HOT `KeyState` in L1. Keys are stored as `CacheEntry` wrappers with full TTL metadata — the `get()` path automatically unwraps to the raw value.

### Write Path — `putThrough`

<!-- Source: HotKeyCache.java:375-416 -->

```
putThrough(cacheKey, value, writer[, hardTtlMs, softTtlMs])
│
├─ (Spring tx) → TransactionSupport.runAsyncAfterCommit(...)
│                afterCommit runs on hotKeyExecutor
│  (non-tx)   → hotKeyExecutor.submit(() → execute all below)
│
├─ writer.run()  ──── L2 / DB write (caller-supplied Runnable)
│   └─ on exception → log.error, continue
│
├─ nextVersion(cacheKey)  ──── version generation
│   ├─ Redis available: Lua "INCR KEYS[1]; EXPIRE KEYS[1] ARGV[1]"
│   │                    → VersionResult(dataVersion > 0, degraded = false)
│   └─ Redis unavailable: fallbackVersionCounter.incrementAndGet()
│                          → VersionResult(dataVersion = MIN_VALUE + counter,
│                                           degraded = true)
│
├─ caffeineCache.put(cacheKey, CacheEntry {
│      value,
│      dataVersion, isVersionDegraded,
│      decisionVersion = 0L,            ← always reset on write-through
│      hardTtlMs,  hardExpireAtMs,
│      softTtlMs,  softExpireAtMs,
│      keyState = NORMAL,               ← always stored as NORMAL
│      normalHardTtlMs, normalSoftTtlMs })
│
└─ CacheSyncPublisher.broadcastRefresh(key, version, degraded)
    └─ sendDeduped(key, "REFRESH", version, degraded)
        ├─ recentBroadcasts.compute("REFRESH:"+key, (_, old) →
        │     old != null && old > version ? old : version)
        │     └─ return != version → newer version exists, skip broadcast
        └─ rabbitTemplate.send(exchange, "", msg)
              [header: type=REFRESH, version, isVersionDegraded]
```

**Write path — transaction deferral:** `putThrough`, `putBeforeInvalidate`, `invalidate`, and `invalidateAll` all defer execution to `afterCommit` when called inside a Spring transaction.

> [!NOTE]`putThrough` behaves differently from the other write methods when called **outside** a transaction — it executes asynchronously on `hotKeyExecutor` (the caller returns immediately while the writer, version bump, L1 update, and sync run on a background thread). Outside a transaction, the other methods (`invalidate`, `invalidateAll`, `putBeforeInvalidate`) run synchronously on the caller's thread.

### Collection Write Path — `putBeforeInvalidate`

<!-- Source: HotKeyCache.java:422-446 -->

For incremental collection mutations (LPUSH, SADD, ZADD):

```
putBeforeInvalidate(cacheKey, mutation)
│
├─ (Spring tx) → TransactionSupport.runNowOrAfterCommit(...)
│  (non-tx)   → sync on caller thread
│
├─ mutation.run()  ──── L2 / DB mutation (caller-supplied Runnable)
│   └─ on exception → log.error, skip L1 invalidation and broadcast
│
├─ nextVersion(cacheKey)  ──── same as putThrough
│
├─ caffeineCache.invalidate(cacheKey)  ──── L1 remove (not put)
│
└─ CacheSyncPublisher.broadcastLocalInvalidate(key, version, degraded)
    └─ sendDeduped(key, "INVALIDATE", version, degraded)
        └─ rabbitTemplate.send(...)
              [header: type=INVALIDATE, version, isVersionDegraded]
```

> [!NOTE] Between `mutation.run()` and L1 cache invalidation there is a ~1ms window where a concurrent `get()` may hit the L1 stale value. This is a deliberate trade-off — invalidating before the mutation would cause a worse race where `get()` re-populates L1 with old Redis data. The window is bounded to a single Redis round-trip (`nextVersion` call).

### Invalidate Path

<!-- Source: HotKeyCache.java:321-358, CacheSyncListener.java:188-230, VersionGuard.java:73-98 -->

```
invalidate(cacheKey)
│
├─ invalidCacheKey(key) == true → log.debug + return
│
├─ TransactionSupport.runNowOrAfterCommit(...)
│   ├─ (tx)   → deferred to afterCommit
│   └─ (non-tx) → sync
│
├─ nextVersion(key)  ──── Redis INCR → VersionResult(dataVersion)
│
├─ caffeineCache.invalidate(key)  ──── L1 remove
│
└─ CacheSyncPublisher.broadcastRefresh(key, version, degraded)
    └─ TYPE_REFRESH (peers reload from Redis, skip stale versions)


invalidateAll(cacheKeys)
│
├─ stream().filter(k → !invalidCacheKey(k)).toList()
│   └─ empty → log.debug + return
│
├─ TransactionSupport.runNowOrAfterCommit(...)
│
├─ caffeineCache.invalidateAll(validKeys)  ──── L1 batch remove
│
└─ (has syncPublisher)
    └─ CacheSyncPublisher.broadcastLocalInvalidateAll(validKeys)
        └─ single AMQP message
              body = JSON array [key1, key2, ...]
              header: type = INVALIDATE_ALL
              ⚠ no Redis INCR, no version check
```

### Soft Expire Read Path (`getWithSoftExpire`)

<!-- Source: HotKeyCache.java:189-236, CacheExpireManager.java -->

```
                           L1 hit
┌──────────────┐  ────────────────────────────────▶ ┌──────────────────────┐
│   Request    │                                     │  CacheEntry          │
│              │ ◀───────────────────────────────── │  value, keyState,    │
└──────┬───────┘    Optional.of(stale value)         │  hardTtlMs, softTtlMs│
       │ L1 miss                                     └──────────┬───────────┘
       │                                                        │
       │                                          ┌─────────────┴────────────┐
       │                                          │ isLogicallyExpired?      │
       │                                          │  (hardExpireAtMs)        │
       │                                          └──────┬──────────┬────────┘
       │                                            true ▼            ▼ false
       │                                    invalidate +      ┌────────────────────┐
       │                                    return empty      │ KeyState ==        │
       │                                                      │ HOT or COOL?       │
       │                                                      └──┬─────────────┬───┘
       │                                                      no ▼              ▼ yes
       │                                                  return +       isSoftExpired?
       │                                                  record(key,1)         │
       │                                                                    true ▼
       │                                                         triggerAsyncRefresh:
       │                                                         ├─ refreshLimiter
       │                                                         │  .tryAcquire()
       │                                                         │  └─ busy → skip
       │                                                         └─ executor.submit(
       │                                                              reader.get() →
       │                                                              Caffeine.put(
       │                                                                preserve hardTtlMs,
       │                                                                update softExpireAt)
       │                                                         )
       ▼
┌─────────────────────────────────────────────────────┐
│  SingleFlight.load(key, reader)                     │
│  → add(key,1) + record(key)                         │
│  → Caffeine.put(key, CacheEntry {                   │
│      value, dv=0, degraded=false, hv=0L,            │
│      hardTtlMs, hardExpireAtMs,                     │
│      softTtlMs, softExpireAtMs,                     │
│      keyState, normalHardTtlMs, normalSoftTtlMs })  │
└─────────────────────────────────────────────────────┘

※ Soft expire applies to HOT and COOL entries.
  NORMAL entries always load fresh via SingleFlight.
  hardTtlMs = Long.MAX_VALUE → pure logical expiry (no hard eviction).
```

### Instance-to-Instance Cache Sync

<!-- Source: CacheSyncPublisher.java, CacheSyncListener.java, VersionGuard.java -->

When `hotkey.sync.enabled=true`, all write operations (`putThrough`, `putBeforeInvalidate`, `invalidate`, `invalidateAll`) trigger a message via `CacheSyncPublisher`:

```
╔════════════════════╗  putThrough / invalidate     ╔══════════════════════════════╗
║  Instance A        │ ─── CacheSyncPublisher ───▶ ║  hotkey.sync.exchange        ║
║  (writer)          │                              ║  (fanout exchange)           ║
╚════════════════════╝                              ╚════╤═══════════╤═════════════╝
                                                         │           │
                                             ┌───────────┘           └──────────┐
                                             ▼                                  ▼
                              ┌─────────────────────────┐        ┌─────────────────────────┐
                              │  Instance B             │        │  Instance N             │
                              │  CacheSyncListener      │  ...   │  CacheSyncListener      │
                              └───────────┬─────────────┘        └───────────┬─────────────┘
                                          │                                   │
                              ┌───────────▼─────────────┐        ┌───────────▼─────────────┐
                              │  VersionGuard           │        │  VersionGuard           │
                              │  shouldSkipForSync()    │        │  shouldSkipForSync()    │
                              │  4-case dataVersion     │        │  4-case dataVersion     │
                              └───────────┬─────────────┘        └───────────┬─────────────┘
                                          │ pass                             │ pass
                              ┌───────────▼─────────────┐        ┌───────────▼─────────────┐
                              │  DCL + Redis reload     │        │  DCL + Redis reload     │
                              │  → Caffeine.compute(    │        │  → Caffeine.compute(    │
                              │     preserve decisionV) │        │     preserve decisionV) │
                              └─────────────────────────┘        └─────────────────────────┘
```

> [!NOTE] The version guard operates on `dataVersion` (the version from Redis INCR or degraded fallback). See [Version Spaces](#version-spaces) below for the distinction between data version and decision version.

**Version comparison (4-case) — applies to `dataVersion` only:**

<!-- Source: VersionGuard.java:73-98 -->

| Local dataVersion | Incoming dataVersion | Result                        |
| ----------------- | -------------------- | ----------------------------- |
| Normal            | Normal               | Higher wins (numeric compare) |
| Normal            | Degraded             | Local wins (skip update)      |
| Degraded          | Normal               | Incoming wins (overwrite)     |
| Degraded          | Degraded             | Higher wins (numeric compare) |

### Version Spaces

<!-- Source: model/CacheEntry.java, VersionGuard.java, HotKeyStateMachine.java -->

CacheEntry maintains **two independent version spaces** with different semantics:

| Version Field     | Source                                                                    | Degraded possible? | Used By                                                |
| ----------------- | ------------------------------------------------------------------------- | ------------------ | ------------------------------------------------------ |
| `dataVersion`     | `HotKeyCache.nextVersion()` — Redis INCR or node-local fallback           | Yes                | `VersionGuard.shouldSkipForSync()` (CacheSyncListener) |
| `decisionVersion` | `WorkerBroadcaster.decisionVersionCounter` — `AtomicLong`, never degraded | No                 | `VersionGuard.shouldSkipForWorker()` (WorkerListener)  |

**Rules:**

- `dataVersion` tracks the actual data mutation version for cross-instance cache sync. It can degrade to a node-local counter if Redis is unavailable.
- `decisionVersion` tracks Worker HOT/COOL decision ordering. It is always a clean `AtomicLong` on the Worker — never degraded.
- These versions are **orthogonal**: a data mutation does not affect `decisionVersion`, and a Worker decision does not affect `dataVersion`.
- `putThrough` always sets `decisionVersion=0L` (never preserves — the write-through replaces the value so any prior Worker decision is invalidated).
- `loadAndCache` also sets `decisionVersion=0L` (no Worker decision on first load).
- `CacheSyncListener` preserves `decisionVersion` from the existing entry during cross-instance sync refreshes.
- **`isVersionDegraded` safety net in `shouldSkipForWorker`:** When an existing entry has `isVersionDegraded=true` (i.e. it was created during a Redis outage), Worker decisions are unconditionally accepted regardless of `decisionVersion`. This prevents Worker restart (which resets `AtomicLong`) from being blocked by degraded entries — the degraded entry was created in an unstable period and should yield to any newer Worker decision.

### CacheEntry Fields

<!-- Source: model/CacheEntry.java -->

| Field               | Type       | Description                                                                                         |
| ------------------- | ---------- | --------------------------------------------------------------------------------------------------- |
| `value`             | `Object`   | The cached value                                                                                    |
| `dataVersion`       | `long`     | Mutation version from Redis `INCR`, or node-local counter when degraded                             |
| `isVersionDegraded` | `boolean`  | Whether `dataVersion` comes from fallback (node-local counter) instead of Redis                     |
| `decisionVersion`   | `long`     | Worker HOT/COOL decision version — `AtomicLong`, never degraded                                     |
| `hardTtlMs`         | `long`     | Current hard TTL in ms (key-state-dependent or per-entry override)                                  |
| `hardExpireAtMs`    | `long`     | Absolute epoch-ms when hard TTL expires                                                             |
| `softTtlMs`         | `long`     | Current soft TTL in ms (key-state-dependent or per-entry override)                                  |
| `softExpireAtMs`    | `long`     | Absolute epoch-ms when soft TTL expires                                                             |
| `keyState`          | `KeyState` | Current hot key state: `NORMAL` / `HOT` / `PRE_COOL` / `COOL`                                       |
| `normalHardTtlMs`   | `long`     | Original hard TTL from when entry was created in `NORMAL` state; preserved across state transitions |
| `normalSoftTtlMs`   | `long`     | Original soft TTL from when entry was created in `NORMAL` state; preserved across state transitions |

> The `normal*TtlMs` fields are the original TTLs recorded when the entry was created in `NORMAL` state. They are preserved across HOT/COOL state transitions so that when a decision changes the state back to `NORMAL`, the original TTLs can be restored.

---

## Method Call Chains

### Read Path — `get`

<!-- Source: HotKeyCache.java:138-162, loadAndCache:247-312 -->

```
HotKey.get(cacheKey, reader[, hardTtlMs, softTtlMs])
└─ HotKeyCache.get(cacheKey, reader, effectiveHardTtl, effectiveSoftTtl)
      ├─ TransactionSupport.runNowOrAfterCommit()              [tx-aware, L2 async write path only]
      ├─ caffeineCache.getIfPresent(key)                       [L1 lookup]
      │    ├─ hit → unwrap CacheEntry
      │    │    ├─ KeyState == HOT → use hot TTLs (hotHardTtl / hotSoftTtl)
      │    │    └─ KeyState != HOT → use normal TTLs (normalHardTtl / normalSoftTtl)
      │    ├─ promoteLocalHotkeyIfNeeded(key, entry)           [upgrade NORMAL/COOL to HOT if locally hot]
      │    ├─ hotKeyDetector.add(key, 1)                       [local HeavyKeeper frequency count]
      │    ├─ hotKeyReporter.record(key)                       [app→Worker report]
      │    └─ return Optional.of(entry.value)
      │
      └─ miss → SingleFlight.execute(key, supplier)            [concurrency dedup]
           ├─ supplier.get() → reader.get()                    [caller-supplied L2/DB read]
           ├─ hotKeyDetector.add(key, 1)
           ├─ hotKeyReporter.record(key)
           └─ loadAndCache (inside SingleFlight):
                ├─ hotKeyDetector.add(cacheKey, 1).isHotKey()
                │    ├─ hot  → CacheEntry(keyState=HOT, hot TTLs)
                │    │         [dataVersion=0L, decisionVersion=0L]
                │    └─ cold → CacheEntry(keyState=NORMAL, normal TTLs)
                │              [dataVersion=0L, decisionVersion=0L]
                ├─ caffeineCache.put(key, entry)
                └─ return Optional.of(value)
```

> [!NOTE] When L1 is hit, the `CacheEntry` was written by a sync broadcast (REFRESH/INVALIDATE/INVALIDATE_ALL) or Worker decision (HOT/COOL). The `dataVersion` and `decisionVersion` are determined by the sender. L1 miss entries are created by `loadAndCache` with version fields set to 0. `loadAndCache` caches all loaded keys (not just hot ones), differentiating TTLs by `KeyState`.

### Soft Expire Read Path — `getWithSoftExpire`

<!-- Source: HotKeyCache.java:189-236, CacheExpireManager.java -->

```
HotKey.getWithSoftExpire(key, reader[, softTtlMs][, hardTtlMs, softTtlMs])
└─ HotKeyCache.getWithSoftExpire(key, reader, ...)
      ├─ caffeineCache.getIfPresent(key) → CacheEntry
      │    ├─ hard TTL not expired → same as get path
      │    ├─ hard TTL expired → return empty (same as get path)
      │    └─ soft TTL expired but hard TTL not expired
      │         ├─ return stale value immediately
      │         ├─ promoteLocalHotkeyIfNeeded(key, entry)  [upgrade NORMAL/COOL to HOT if locally hot]
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

> [!NOTE] Async refresh preserves the original CacheEntry's hard TTL (`hardTtlMs` / `hardExpireAtMs`), only updating the value and soft expire time.

### Peek Path — `peek`

<!-- Source: HotKeyCache.java:170-180 -->

```
HotKey.peek(cacheKey)
└─ HotKeyCache.peek(cacheKey)
      └─ caffeineCache.getIfPresent(key)                       [L1 only, no side effects]
           └─ return Optional.ofNullable(entry.value)
           [⚠ does not call hotKeyDetector.add / hotKeyReporter.record / L2 reader]
```

### Raw Cache Access — `getLocalCache`

Exposes the underlying Caffeine `Cache<String, Object>` for Caffeine-specific operations (`asMap`, `policy`, `cleanUp`).

<!-- Source: HotKeyCache.java:542-544 -->

```
HotKey.getLocalCache()
└─ HotKeyCache.getLocalCache()
      └─ return caffeineCache                                   [direct Caffeine reference]
      [⚠ bypasses HotKey orchestration — version tracking, broadcast, expiry management are skipped]
```

> [!WARNING]
> `getLocalCache()` returns the **raw** Caffeine cache. Nothing written through this reference goes through version generation, broadcast sync, or expire management. Use it only for introspection or Caffeine-specific housekeeping (`asMap().keySet()`, `policy().expireAfterWrite()`, `cleanUp()`). Never write entries via this reference — use `putThrough()` or `putBeforeInvalidate()` instead.

### Write-Through Path — `putThrough`

<!-- Source: HotKeyCache.java:375-416 -->

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

<!-- Source: HotKeyCache.java:422-446 -->

```
HotKey.putBeforeInvalidate(key, mutation)
└─ HotKeyCache.putBeforeInvalidate(key, mutation)
      ├─ (tx) → TransactionSupport.registerAfterCommit()
      │    └─ afterCommit → execute all below
      │
      ├─ (non-tx) → sync (caller thread)
      │
      ├─ mutation.run()                                        [caller mutates L2/DB]
      │    └─ exception → log.error, return (terminate, skip L1 invalidation and broadcast)
      │
      ├─ nextVersion(key)                                      [same as putThrough]
      ├─ caffeineCache.invalidate(key)                         [L1 remove, not put]
      └─ CacheSyncPublisher.broadcastLocalInvalidate(key, version, degraded)
           └─ same as putThrough (but type=INVALIDATE instead of REFRESH)
```

> [!NOTE] There is a ~1ms window between `mutation.run()` and L1 invalidation where a concurrent `get()` may hit the stale L1 value. This is a deliberate trade-off — invalidating before the mutation would cause a worse race where `get()` re-populates L1 with old data, creating a longer window.

### Single Key Invalidate — `invalidate`

<!-- Source: HotKeyCache.java:321-358 -->

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

<!-- Source: HotKeyCache.java:336-358 -->

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

<!-- Source: HotKeyCache.java:164-168 -->

```
HotKey.isLocalHotKey(cacheKey)
└─ HotKeyCache.isLocalHotKey(cacheKey)
      └─ caffeineCache.getIfPresent(key)
           ├─ exists and keyState == HOT → true
           └─ otherwise → false
           [⚠ pure L1 lookup, no side effects]
```

### State Query — `isWorkerHotKey`

<!-- Source: HotKey.java (delegates to workerTopKAlgorithm) -->

```
HotKey.isWorkerHotKey(cacheKey)
└─ workerTopKAlgorithm.list().stream().anyMatch(item -> item.key().equals(cacheKey))
      ├─ key in Worker TopK → true
      └─ not in → false
      [⚠ iterates Worker TopK list, O(n); no network call]
```

### TopK Query Methods

<!-- Source: HotKey.java -->

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

<!-- Source: RuleMatcher.java -->

```
HotKey.addBlacklist(keyPattern)
└─ HotKeyCache.addBlacklist(keyPattern)
      └─ ruleMatcher.addRule(Rule.of(keyPattern, BLOCK))
           ├─ [if Redis] → persist to Redis
           └─ [if syncPublisher] → broadcastAllLocalRules()
           [auto-detects pattern type: exact, prefix (* suffix), wildcard, regex]

HotKey.removeBlacklist(keyPattern)
└─ HotKeyCache.unBlacklist(keyPattern)
      └─ ruleMatcher.removeRule(keyPattern, BLOCK)
           ├─ [if Redis] → persist to Redis
           └─ [if syncPublisher] → broadcastAllLocalRules()

HotKey.addWhitelist(keyPattern)
└─ HotKeyCache.addWhitelist(keyPattern)
      └─ ruleMatcher.addRule(Rule.of(keyPattern, ALLOW_NO_REPORT))
           ├─ [if Redis] → persist to Redis
           └─ [if syncPublisher] → broadcastAllLocalRules()

HotKey.removeWhitelist(keyPattern)
└─ HotKeyCache.unWhitelist(keyPattern)
      └─ ruleMatcher.removeRule(keyPattern, ALLOW_NO_REPORT)
           ├─ [if Redis] → persist to Redis
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
           ├─ [if Redis] → delete from Redis
           └─ [if syncPublisher] → broadcastAllLocalRules()
      [removes both blacklist and whitelist rules]

HotKey.broadcastAllLocalRulesManually()
└─ HotKeyCache.broadcastAllLocalRulesManually()
      └─ ruleMatcher.exportRulesJson()
           └─ CacheSyncPublisher.broadcastAllLocalRules(json)
                └─ single AMQP message, body = JSON rule array
                     header: type=RULES_SYNC
      [manual trigger for initial cluster sync]
```

---

## Worker Mode

Worker Mode is an optional deployment topology where a dedicated node aggregates access reports from all application instances, runs cluster-wide hot key detection via a sliding-window + state-machine pipeline, and broadcasts HOT/COOL decisions back to every instance.

This approach solves the **single-instance blind spot** — an app instance's local HeavyKeeper cannot distinguish between "accessed 100 times by one pod" and "accessed once by 100 pods." Worker Mode provides cluster-consensus hot key detection without a centralized proxy.

### Worker Architecture

<!-- Source: HotKeyReporter.java, ReportPublisher.java, ReportConsumer.java,
     SlidingWindowDetector.java, HotKeyStateMachine.java:61-148,
     WorkerBroadcaster.java, WorkerListener.java -->

```
┌─────────────────────┐      RabbitMQ fanout      ┌───────────────────────┐
│  App Instance 1     │  ─── report (periodic) ──→│  Worker Node          │
│  HotKeyReporter     │                           │                       │
├─────────────────────┤                           │  ┌─────────────────┐  │
│  App Instance 2     │  ─── report (periodic) ──→│  │ ReportConsumer  │  │
│  HotKeyReporter     │                           │  │ (AMQP consumer) │  │
├─────────────────────┤                           │  └────────┬────────┘  │
│  App Instance N     │                           │           │           │
│  HotKeyReporter     │  ─── report (periodic) ──→│           ↓           │
└─────────────────────┘                           │  ┌─────────────────┐  │
                                                  │  │HotKeyStateMachine│ │
                                                  │  │ (per-key FSM)   │  │
                                                  │  └────────┬────────┘  │
                                                  │           │           │
                                                  │  ┌─────────────────┐  │
                                                  │  │WorkerBroadcaster│  │
                                                  │  │ (HOT/COOL via   │  │
                                                  │  │  RabbitMQ)      │  │
                                                  │  └────────┬────────┘  │
                                                  └───────────┼───────────┘
                                                              │
                          RabbitMQ (hotkey.broadcast.exchange)
                                                              │
                                                              ↓
                           ┌──────────────────────────────────────────┐
                           │   All App Instances (WorkerListener)     │
                           │  ┌──────────┐  ┌──────────┐  ┌────────┐  │
                           │  │Instance 1│  │Instance 2│  │  ...   │  │
                           │  └──────────┘  └──────────┘  └────────┘  │
                           └──────────────────────────────────────────┘
```

### Report Flow

1. Every `get()` / `getWithSoftExpire()` call triggers `hotKeyReporter.record(key)` on both L1 hit and L2 miss paths.
2. `HotKeyReporter` aggregates per-key counts locally (Caffeine, 30s expiry, max 100k keys) and periodically publishes `ReportMessage` records to the `hotkey.report.exchange` RabbitMQ exchange, routed by `app-name` and `shard-index`.
3. The Worker node's `ReportConsumer` receives reports and feeds access counts into `workerTopK.add()`, discarding stale reports (>5s old).
4. The Worker processes reports asynchronously — it does not block app instances.

### Report Sharding

When `shard-count > 1`, reports are distributed across multiple Worker instances. Each app instance computes `Math.floorMod(cacheKey.hashCode(), shard-count)` and publishes to the corresponding shard routing key. Each Worker binds to its own queue (`hotkey.report.{appName}.{shardIndex}`), enabling horizontal scaling of the Worker plane.

### Sliding Window Detector

<!-- Source: SlidingWindowDetector.java -->

The `SlidingWindowDetector` is a lock-free time-series counter that tracks per-key access counts within a configurable window.

| Property                                           | Default | Description                                   |
| -------------------------------------------------- | ------- | --------------------------------------------- |
| `hotkey.worker.sliding-window.duration-ms`         | `1000`  | Total window duration (1 second)              |
| `hotkey.worker.sliding-window.slices`              | `10`    | Number of time slices per window (100ms each) |

Each key maintains an array of `long` counters indexed by time slice. On each report tick, the oldest slice is recycled and the effective count for each key is recomputed as the sum across all slices. This gives an accurate QPS estimate without per-access locking.

### Hot Key State Machine

<!-- Source: HotKeyStateMachine.java:61-148 -->

Each tracked key follows a lifecycle managed by `HotKeyStateMachine`:

```
                    ┌─────────────────────────────────────────────────┐
                    │                                                 │
                    ↓                                                 │
          ┌──────────────────┐  below threshold for    ┌──────────────┐
          │  CONFIRMED_HOT   │ ──(coolCount - grace)─→ │ PRE_COOLING  │
          └──────────────────┘                         └──────┬───────┘
                    ↑                                         │
                    │         below threshold                 │
                    │     ┌───────────────────────────────────┘
                    │     ↓
          ┌──────────┴────┐     grace expired     ┌──────────────┐
          │     COLD      │ ←──────────────────── │   COLD       │
          └───────────────┘                       └──────────────┘

  COLD ─────────── below threshold ──→ PRE_COOLING (grace period)
  PRE_COOLING ──── grace expired ────→ COLD (broadcast TYPE_COOL)
  PRE_COOLING ──── re-heat (silent) ─→ CONFIRMED_HOT (no broadcast)
  CONFIRMED_HOT ── below threshold for (coolCount - grace) ──→ PRE_COOLING (grace period)
```

- **COLD**: Key exists but below hot threshold. Access tracked but no broadcast sent.
- **CONFIRMED_HOT**: Key exceeds threshold for `confirm-duration-ms`. Worker broadcasts `TYPE_HOT` to all instances. Stays HOT until sustained cooldown.
- **PRE_COOLING**: Key dropped below threshold but is in a grace period. If it rises again during this period, it silently returns to CONFIRMED_HOT without a broadcast, avoiding toggle storms.

### State Machine Configuration

| Property                                                  | Default | Description                                            |
| --------------------------------------------------------- | ------- | ------------------------------------------------------ |
| `hotkey.worker.threshold.hot-threshold`                   | `1000`  | Absolute hot threshold (use `-1` to use ratio instead) |
| `hotkey.worker.threshold.hot-threshold-ratio`             | `0.01`  | Hot threshold as fraction of estimated global QPS      |
| `hotkey.worker.state-machine.confirm-duration-ms`         | `300`   | Duration above threshold to confirm HOT (300ms)        |
| `hotkey.worker.state-machine.cool-duration-ms`            | `15000` | Duration below threshold to confirm COOL (15s)         |
| `hotkey.worker.state-machine.pre-cool-grace-ms`           | `5000`  | Grace period for silent re-heat (5s)                   |

### Dynamic Threshold (Global QPS)

The Worker adapts to traffic patterns by periodically recalculating the hot threshold based on estimated global QPS:

```
hotThreshold = max(minCount, estimatedGlobalQPS * hotThresholdRatio)
```

| Property                                                                     | Default  | Description                                          |
| ---------------------------------------------------------------------------- | -------- | ---------------------------------------------------- |
| `hotkey.worker.global-qps-dynamic-threshold.recalculate-interval-ms`         | `60000`  | Recalculation interval (60s)                         |
| `hotkey.worker.global-qps-dynamic-threshold.qps-change-tolerance`            | `0.5`    | ±50% QPS change required to trigger threshold update |
| `hotkey.worker.global-qps-dynamic-threshold.learning-period-ms`              | `30000`  | Learning period for QPS estimation                   |
| `hotkey.worker.global-qps-dynamic-threshold.hot-threshold-ratio`             | `0.01`   | Hot threshold as fraction of estimated global QPS    |

The `qps-change-tolerance` prevents threshold churn during normal traffic fluctuations — only significant QPS shifts trigger a recalculation.

### Top-K Cross-Validation

The Worker periodically validates the app-side HeavyKeeper Top-K against its own cluster-wide Top-K, ensuring consistency and enabling pre-warming.

| Property                                                      | Default | Description                              |
| ------------------------------------------------------------- | ------- | ---------------------------------------- |
| `hotkey.worker.topk-validation.validate-interval-ms`          | `60000` | Cross-validation interval (60s)          |
| `hotkey.worker.topk-validation.pre-warm-count`                | `5`     | Top-K entries eligible for pre-warming   |
| `hotkey.worker.topk-validation.pre-warm-min-appearances`      | `2`     | Min consecutive appearances for pre-warm |

Top-K entries appearing in the Worker's Top-K across consecutive validation intervals are candidates for pre-warming. The Worker can proactively push these keys to app instances before they would naturally be detected locally.

### Deployment Modes

Two deployment modes:

| Mode                     | `worker.enabled` | Active Beans                                                                          |
| ------------------------ | ---------------- | ------------------------------------------------------------------------------------- |
| App-only                 | `false` (default)| `HotKeyCache`, TopK detector, reporter, actuator, sync                                |
| Worker-only              | `true`           | Worker-only (SlidingWindow, StateMachine, ReportConsumer, Broadcaster)                |

In **Worker-only** mode, `HotKey.isLocalHotKey()` / `get()` / `putThrough()` throw `UnsupportedOperationException` — these operations require the app-side cache. Worker-TopK queries (`returnWorkerHotKeys()`) remain available.

### Configuration Example

```yaml
hotkey:
  worker:
    # Must explicitly set to true to activate Worker mode
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
> The example above uses a plain AMQP connection. In production, enable TLS via `spring.rabbitmq.ssl.*`:
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
> See [Spring Boot RabbitMQ SSL docs](https://docs.spring.io/spring-boot/reference/messaging/amqp.html#page-title).

### Worker Listener (on app instances)

```yaml
hotkey:
  worker-listener:
    enabled: true
```

Each app instance must enable the `worker-listener` to receive HOT/COOL decisions from the Worker. The listener binds to queue `hotkey.worker:{instanceId}` and processes incoming decisions.

### Report Configuration (on app instances)

```yaml
hotkey:
  local:
    app-name: my-service
    report-interval-ms: 100
    shard-count: 1
```

### Failure Behavior

| Failure                   | Impact                                                                     | Recovery                                          |
| ------------------------- | -------------------------------------------------------------------------- | ------------------------------------------------- |
| Worker crashes            | App instances continue with local TopK; no cluster-wide HOT/COOL decisions | Restart Worker; instances reconnect automatically |
| Report channel fails      | App reports queued/buffered (RabbitMQ persistence)                         | Auto-recover on RabbitMQ restoration              |
| Worker broadcast fails    | No cross-instance HOT/COOL sync; local TopK still functional               | Restart Worker broadcaster                        |

---

## Supplementary Diagrams

### SingleFlight Deduplication (Thundering Herd Protection)

<!-- Source: hotkeycache/SingleFlight.java, HotKeyStressTest: singleFlight_extremeDedup, singleFlight_cacheStampede -->

```
     thread-1   thread-2   thread-3  ...  thread-50
         │          │          │              │
         ▼          ▼          ▼              ▼
     ┌───────────────────────────────────────────────────┐
     │           SingleFlight.inflightCache              │
     │  ┌───────────────────────────────────────────────┐ 
     │  │ key = "cache:shop:17"                         
     │  │   ▼                                           
     │  │ future = CompletableFuture.supplyAsync(...)   
     │  │ computeIfAbsent(key, future) ◀─ 1st thread: 
     │  │   absent → supplyAsync created, all threads  
     │  │   share same future  ◀──────────────────────┐
     │  │   present → block-wait on future  ◀─────┐   │ 
     │  └──────────────────────────────────────────┼   | 
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

### VersionGuard 4-Case Data Flow

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

### HeavyKeeper Decay Pipeline

<!-- Source: algorithm/HeavyKeeper.java, HotKeySchedulingConfiguration.java -->

```
┌────────────┐   add(key, 1)    ┌─────────────────────────────────────────────┐
│  Request    │ ──────────────▶│  HeavyKeeper (Count-Min Sketch)             │
│  enters     │                 │                                             │
└────────────┘                  │  for each of depth (5) rows:                │
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
                               │ Combined with fading(): exp(-1/count) decays minimum counts  │
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

### Worker Report Sharding Routing

<!-- Source: HotKeyReporter.java, ReportPublisher.java, ReportConsumer.java -->

```
  App Instance Side                                          Worker Side
  ┌──────────────────────┐
  │  HotKeyReporter      │
  │  (per-instance       │
  │   Caffeine counter,  │
  │   30s expiry,        │
  │   max 100k keys)     │
  └──────────┬───────────┘
             │ every report-interval-ms (100ms)
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

  shard-count default = 1; increase to linearly reduce per-queue throughput
  hash routing ensures: same key → same shard across Worker restarts
  consumer-count = max(1, shardCount / 2) default
```

---

## Design Notes

**Why does `invalidate` send REFRESH while `invalidateAll` sends INVALIDATE_ALL?**
Single-key invalidation typically means the key will be read soon — REFRESH tells peers to actively reload from Redis, reducing the next `get()` latency. Batch invalidation may involve many keys not immediately accessed — `invalidateAll` uses a single TYPE_INVALIDATE_ALL message (JSON array of keys) to let peers lazily reload on next `get()`.

**Why doesn't `invalidateAll` call INCR?**
Each INCR is a Redis call. `invalidateAll` is designed for bulk cleanup (cache expiration, batch data change notifications), prioritizing performance over version consistency. For per-key version control, loop over `invalidate()` instead.

**Why does `fallbackVersion` use `Long.MIN_VALUE + counter`?**
All degraded versions are negative, sorting below normal (positive) Redis INCR versions in `sendDeduped`'s `> version` comparison. This ensures normal versions always win over degraded local versions without extra `degraded` flag logic.

**Why does `putThrough` run async (non-transactional) while other write methods run sync?**
`putThrough`'s `writer.run()` may involve L2/DB network I/O — async execution avoids blocking the caller. `invalidate`/`invalidateAll`/`putBeforeInvalidate` are lightweight (cache operation + broadcast), so synchronous execution guarantees the broadcast is sent by the time the caller returns.

**TTL Jitter (Cache Avalanche Protection):**
`CacheExpireManager.toHardExpireTimestamp()` and `toSoftExpireTimestamp()` apply a ±10% uniform random offset (`ThreadLocalRandom`) to every computed expiry timestamp. This scatters the eviction times of many keys that share the same configured TTL, preventing simultaneous mass eviction (cache stampede). The jitter ratio is hard-coded at 10% — see `CacheExpireManager.java:48,136-155`.
