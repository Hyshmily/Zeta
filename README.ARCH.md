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
                                    dataVersion=0L,
                                    isVersionDegraded=false,
                                    decisionVersion=0L,
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
├─ nextVersion(cacheKey) — Redis INCR → VersionResult(dataVersion, isVersionDegraded)
│  └─ On Redis failure → node-local counter fallback (degraded=true)
├─ Caffeine.put(cacheKey, CacheEntry(
│    value,
│    dataVersion, isVersionDegraded,
│    decisionVersion=existingEntry.decisionVersion (preserved),
│    hardTtlMs, hardExpireAtMs,
│    softTtlMs, softExpireAtMs,
│    keyState,
│    normalHardTtlMs, normalSoftTtlMs))
└─ CacheSyncPublisher.send()
     └─ RabbitMQ fanout (hotkey.sync.exchange)
          └─ TYPE_REFRESH with dataVersion + isVersionDegraded headers
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
├─ nextVersion(cacheKey) — Redis INCR → VersionResult(dataVersion, isVersionDegraded)
├─ Caffeine local cache invalidate
└─ CacheSyncPublisher.send()
     └─ RabbitMQ fanout (hotkey.sync.exchange)
          └─ TYPE_REFRESH with dataVersion + isVersionDegraded headers
```

> **Note:** Between `mutation.run()` and L1 cache invalidation there is a ~1ms window where a concurrent `get()` may hit the L1 stale value. This is a deliberate trade-off — invalidating before the mutation would cause a worse race where `get()` re-populates L1 with old Redis data. The window is bounded to a single Redis round-trip (`nextVersion` call).

### Invalidate Path

```
invalidate(cacheKey)
├─ nextVersion(cacheKey) — Redis INCR → VersionResult(dataVersion)
├─ Caffeine local cache invalidate
└─ CacheSyncPublisher.send()
     └─ TYPE_REFRESH (peers reload from Redis, skip stale versions)

invalidateAll(cacheKeys)
├─ Caffeine local cache invalidate (all keys)
└─ CacheSyncPublisher.send() — per key
     └─ TYPE_INVALIDATE with dataVersion=0L (peers remove unconditionally)
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
                 value, 0L, false, 0L,
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

> **Note:** The version guard operates on `dataVersion` (the version from Redis INCR or degraded fallback). See [Version Spaces](#version-spaces) below for the distinction between data version and decision version.

**Version comparison (4-case) — applies to `dataVersion` only:**

| Local dataVersion | Incoming dataVersion | Result                        |
| ----------------- | -------------------- | ----------------------------- |
| Normal            | Normal               | Higher wins (numeric compare) |
| Normal            | Degraded             | Local wins (skip update)      |
| Degraded          | Normal               | Incoming wins (overwrite)     |
| Degraded          | Degraded             | Higher wins (numeric compare) |

### Version Spaces

CacheEntry maintains **two independent version spaces** with different semantics:

| Version Field     | Source                                                                    | Degraded possible? | Used By                                                |
| ----------------- | ------------------------------------------------------------------------- | ------------------ | ------------------------------------------------------ |
| `dataVersion`     | `HotKeyCache.nextVersion()` — Redis INCR or node-local fallback           | Yes                | `VersionGuard.shouldSkipForSync()` (CacheSyncListener) |
| `decisionVersion` | `WorkerBroadcaster.decisionVersionCounter` — `AtomicLong`, never degraded | No                 | `VersionGuard.shouldSkipForWorker()` (WorkerListener)  |

**Rules:**

- `dataVersion` tracks the actual data mutation version for cross-instance cache sync. It can degrade to a node-local counter if Redis is unavailable.
- `decisionVersion` tracks Worker HOT/COOL decision ordering. It is always a clean `AtomicLong` on the Worker — never degraded.
- These versions are **orthogonal**: a data mutation does not affect `decisionVersion`, and a Worker decision does not affect `dataVersion`.
- `putThrough` preserves the existing `decisionVersion` from the L1 entry. `loadAndCache` sets `decisionVersion=0L` (no Worker decision yet).
- `CacheSyncListener` preserves `decisionVersion` from the existing entry during cross-instance sync refreshes.
- **`isVersionDegraded` safety net in `shouldSkipForWorker`:** When an existing entry has `isVersionDegraded=true` (i.e. it was created during a Redis outage), Worker decisions are unconditionally accepted regardless of `decisionVersion`. This prevents Worker restart (which resets `AtomicLong`) from being blocked by degraded entries — the degraded entry was created in an unstable period and should yield to any newer Worker decision.

### CacheEntry Fields

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

### Report & Worker Architecture

For cluster-wide hot key detection, app instances periodically report access counts to a dedicated Worker node:

```
┌──────────────────────┐              ┌──────────────────────────────┐
│  App Instance        │              │  Worker Node                 │
│                      │              │                              │
│  HotKeyReporter      │  periodic    │  ReportConsumer              │
│  (batches TopK data  │ ──────────→  │  (receives reports via       │
│   every 100ms)       │  RabbitMQ    │   hotkey.report.exchange)    │
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
                               │  │   (preserves existing dataVersion│
                               │  │    + isVersionDegraded, stores   │
                               │  │    wm.decisionVersion())         │
                               │  └─ TYPE_COOL → demote in L1        │
                               │      (preserves both versions,      │
                               │       stores wm.decisionVersion())  │
                               └─────────────────────────────────────┘
```

> See [README.WORKER.md](README.WORKER.md) for detailed Worker mode setup, state machine transitions, and configuration.
