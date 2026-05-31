[← Back to Home](README.md)

## Architecture

### Normal Read Path (`get`)

```
┌──────────────┐   L1 hit               ┌──────────────┐
│   Request    │ ─── add(key,1) ─────→  │  Caffeine L1 │
│              │ ─── record(key) ────→  │  (local)     │
│              │ ←────────────────────  │              │
└──────┬───────┘   Optional.of(value)   └──────┬───────┘
       │ L1 miss          (auto unwrap         │ isHotKey()?
       ↓ (inflight dedup) CacheEntry)          ↓
┌──────────────┐  ──── reader ────→  ┌───────────────┐
│  L2 Storage  │  ──add(key,1)───→   │     TopK      │
│  (pluggable) │  ──record(key)──→   │  (interface)  │
└──┬───────┬───┘                     ├───────────────┤
    │ hit   │ null                   │ add()→Result  │
    ↓       ↓                        │ list()        │
 Optional   Optional.empty()         │ listTopN(n)   │
 .of(value)   r.isEmpty() → DB       │ total()       │
                                     │ contains()    │
                                     │ expelled()    │
                                     │ fading()      │
                                     └───────┬───────┘
                                             │ isHotKey()?
                                             ↓
                                  ┌─────────────────────┐
                                  │  Choose TTL by      │
                                  │  KeyState:          │
                                  │  HOT   → hot TTLs   │
                                  │  other → normal TTLs│
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
                                  (no sync on read path)
```

> **Note:** `isHotKey()` checks for HOT `KeyState` in L1. Keys are stored as `CacheEntry` wrappers with full TTL metadata — the `get()` path automatically unwraps to the raw value.

### Write Path — `putThrough`

```
putThrough(cacheKey, value, writer)
├─ (deferred to afterCommit if inside a Spring transaction)
├─ writer.run() — L2 write (caller-supplied Runnable)
├─ nextVersion(cacheKey) — Redis INCR → VersionResult(version, isVersionDegraded)
│  └─ On Redis failure → System.nanoTime() fallback (degraded=true)
├─ Caffeine.put(cacheKey, CacheEntry(
│    value, version, isVersionDegraded,
│    hardTtlMs, hardExpireAtMs,
│    softTtlMs, softExpireAtMs,
│    keyState,
│    normalHardTtlMs, normalSoftTtlMs))
└─ CacheSyncPublisher.send()
     └─ RabbitMQ fanout (hotkey.sync.exchange)
          └─ TYPE_REFRESH with version + isVersionDegraded headers
```

**Write path — transaction deferral:** `putThrough`, `putBeforeInvalidate`, `invalidate`, and `invalidateAll` all defer execution to `afterCommit` when called inside a Spring transaction.

> **Note:** `putThrough` behaves differently from the other write methods when called **outside** a transaction — it executes asynchronously on `hotKeyExecutor` (the caller returns immediately while the writer, version bump, L1 update, and sync run on a background thread). Outside a transaction, the other methods (`invalidate`, `invalidateAll`, `putBeforeInvalidate`) run synchronously on the caller's thread.

### Collection Write Path — `putBeforeInvalidate`

For incremental collection mutations (LPUSH, SADD, ZADD):

```
putBeforeInvalidate(cacheKey, mutation)
├─ (deferred to afterCommit if inside a Spring transaction)
├─ mutation.run() — L2 write (caller-supplied Runnable)
│  └─ On exception → skip local invalidate and sync, log error
├─ nextVersion(cacheKey) — Redis INCR → VersionResult(version, isVersionDegraded)
├─ Caffeine local cache invalidate
└─ CacheSyncPublisher.send()
     └─ RabbitMQ fanout (hotkey.sync.exchange)
          └─ TYPE_REFRESH with version + isVersionDegraded headers
```

> **Note:** Between `mutation.run()` and L1 cache invalidation there is a ~1ms window where a concurrent `get()` may hit the L1 stale value. This is a deliberate trade-off — invalidating before the mutation would cause a worse race where `get()` re-populates L1 with old Redis data. The window is bounded to a single Redis round-trip (`nextVersion` call).

### Invalidate Path

```
invalidate(cacheKey)
├─ nextVersion(cacheKey) — Redis INCR → VersionResult
├─ Caffeine local cache invalidate
└─ CacheSyncPublisher.send()
     └─ TYPE_REFRESH (peers reload from Redis, skip stale versions)

invalidateAll(cacheKeys)
├─ Caffeine local cache invalidate (all keys)
└─ CacheSyncPublisher.send() — per key
     └─ TYPE_INVALIDATE with version=0L (peers remove unconditionally)
```

### Soft Expire Read Path (`getWithSoftExpire`)

```
          ┌──────────────┐   L1 hit ┌───────────────┐
          │   Request    │ ───────→ │ softExpireAt  │
          │              │ ←─────── │  time check   │
          └──────┬───────┘  stale   └───────┬───────┘
                 │ soft expired?            │ expired?
                 ↓ true                     ↓ yes
            Return stale          triggerAsyncRefresh
            value +                ├─ refreshLimiter.tryAcquire()
            add(key,1) +          │  (Semaphore, max concurrency)
            record(key)           │  └─ On busy → skip (retry next get)
                                    └─ Async (hotKeyExecutor):
                                         L2 read → Caffeine.put
                                         + update softExpireAt
                                         + preserve hardTtlMs
                 │ L1 miss (falls through to normal path)
                 ↓
             SingleFlight.load(cacheKey, reader)
             (see Normal Read Path above)
              Caffeine.put(key, CacheEntry(
                value, 0L, false,
                hardTtlMs, hardExpireAtMs,
                softTtlMs, softExpireAtMs,
                keyState,
                normalHardTtlMs, normalSoftTtlMs))
```

> **Note:** Soft expire applies to both HOT and COOL entries. Normal entries are always loaded fresh. The async refresh preserves the original per-entry hard TTL across background refreshes.

### Instance-to-Instance Cache Sync

When `hotkey.sync.enabled=true`, all write operations (`putThrough`, `putBeforeInvalidate`, `invalidate`, `invalidateAll`) trigger a message via `CacheSyncPublisher`:

```
┌──────────────┐    putThrough / invalidate     ┌───────────────────┐
│  Instance A  │ ──── CacheSyncPublisher ─────→ │ hotkey.sync       │
│  (writer)    │                                │  (fanout exchange)│
└──────────────┘                                └────────┬──────────┘
                                                          │
                                          ┌─────────────┼─────────────┐
                                          ↓             ↓             ↓
                                     ┌──────────┐  ┌──────────┐  ┌──────────┐
                                     │Instance B│  │Instance C│  │   ...    │
                                     │Listener  │  │Listener  │  │Listener  │
                                     └────┬─────┘  └────┬─────┘  └────┬─────┘
                                          │             │             │
                                     ┌────┬─────┐  ┌────┬─────┐  ┌────┬─────┐
                                     │ Version  │  │ Version  │  │ Version  │
                                     │  Guard   │  │  Guard   │  │  Guard   │
                                     │compare() │  │compare() │  │compare() │
                                     └────┴─────┘  └────┴─────┘  └────┴─────┘
                                          │             │             │
                                     ┌────┬─────┐  ┌────┬─────┐  ┌────┬─────┐
                                     │L1 update │  │L1 update │  │L1 update │
                                     │   or     │  │   or     │  │   or     │
                                     │invalidate│  │invalidate│  │invalidate│
                                     └────┴─────┘  └────┴─────┘  └────┴─────┘
```

**Version comparison (4-case):**

| Local version | Incoming version | Result                        |
| ------------- | ---------------- | ----------------------------- |
| Normal        | Normal           | Higher wins (numeric compare) |
| Normal        | Degraded         | Local wins (skip update)      |
| Degraded      | Normal           | Incoming wins (overwrite)     |
| Degraded      | Degraded         | Higher wins (numeric compare) |

### Report & Worker Architecture

For cluster-wide hot key detection, app instances periodically report access counts to a dedicated Worker node:

```
┌──────────────────────┐              ┌──────────────────────────────┐
│  App Instance        │              │  Worker Node                 │
│                      │              │                              │
│  HotKeyReporter      │  periodic    │  ReportConsumer              │
│  (batches TopK data  │ ──────────→  │  (receives reports via       │
│   every 100ms)       │  RabbitMQ   │   hotkey.report.exchange)     │
│                      │              │         │                    │
│  ReportPublisher     │              │         ↓                    │
│  (sends to           │              │  SlidingWindowDetector       │
│   hotkey.report.     │              │  (lock-free time-series      │
│   exchange)          │              │   counter per key)           │
└──────────────────────┘              │         │                    │
                                      │         ↓                    │
                                      │  HotKeyStateMachine          │
                                      │  (per-key FSM:               │
                                      │   NORMAL→HOT→PRE_COOL→COOL   │
                                      │   →NORMAL)                   │
                                      │         │                    │
                                      │         ↓                    │
                                      │  WorkerBroadcaster           │
                                      │  (HOT/COOL decisions via     │
                                      │   hotkey.worker.exchange)    │
                                      │         │                    │
                                      └─────────┼────────────────────┘
                                                │ RabbitMQ fanout
                                                ↓
                              ┌─────────────────────────────────────┐
                              │  All App Instances                  │
                              │                                     │
                              │  WorkerListener (hotkey.worker-     │
                              │  listener.*) processes HOT/COOL:    │
                              │  ┌─ TYPE_HOT  → promote to L1       │
                              │  └─ TYPE_COOL → demote in L1        │
                              └─────────────────────────────────────┘
```

> See [README.WORKER.md](README.WORKER.md) for detailed Worker mode setup, state machine transitions, and configuration.
