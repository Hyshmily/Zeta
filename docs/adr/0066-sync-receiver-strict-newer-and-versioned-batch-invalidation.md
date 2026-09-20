# 0066. Sync-Receiver Strict-Newer Guard & Per-Key Versioned Batch Invalidation

## Status

Accepted (2026-09-12)

## Amendment 2026-09-13 — Writer-side equal-version apply

A full call-chain audit found that the Decision bullet reserving the plain
`shouldSkipForSync` (`>=`) for `buildPutThroughEntry` rests on a wrong premise
for that one site. Its rationale — "those guards compare against an entry the
*caller itself* just produced, where equality means 'already current'" — holds
for `ExpireManagerImpl.applyRefreshTask` (its probe IS the caller's own), but
not for `buildPutThroughEntry`: the existing entry inside its compute can be a
**raced read-path load**, stamped with the writer's own INCR by ADR-0033's
probe-after-read (the reader loaded pre-write data, the writer's INCR landed,
the probe read the post-INCR version, and the load's store beat the writer's
compute). With the sender's own REFRESH dropped by the origin filter
(ADR-0067), the `>=` equality skip then pinned the stale value on the *writer*
instance for the entry TTL — the one instance that can never be healed by the
broadcast, because it authored it.

`buildPutThroughEntry` therefore now guards with `shouldSkipForRefresh`
(skip only **strictly** newer; equal applies). The writer's value is
authoritative at the version it just allocated, so an equal-version overwrite
is always correct: the equal case is either the over-stamped load (healed) or
a peer's copy of the same write (idempotent). Strictly-newer concurrent writes
still win, and the 4-case degraded matrix is unchanged — the degraded fallback
rewrite keeps its "apply only when no newer normal entry holds the key"
posture. The dead `forceUpdate` parameter (both call sites passed `false`) is
removed. Cost profile unchanged from the REFRESH-receiver analysis: a genuine
duplicate pays one idempotent local overwrite.

Companion changes shipped with the same audit pass (regression tests in
`HotKeyCacheConflictTest`):

- **`putLocal` joined the ADR-0067 dedup invariant** — it now calls
  `singleFlight.invalidate` next to its L1 write. putLocal carries no version
  bump, so a completed pre-write SingleFlight result replayed onto the next
  miss had no guard left to stop it re-caching the pre-write value over the
  local write.
- **`putLocal` preserves the Worker's TTL regime on HOT/COOL entries** —
  value-only update, mirroring `loadCacheEntry`'s refusal branch. The former
  `applyTtl` rewrite left a HOT entry wearing normal TTLs that the Worker
  could never repair (its rebroadcast with the same `decisionVersion` is
  skipped by the decision guard).
- **`refreshSoftExpire` refreshes NORMAL entries** (getWithSoftExpire /
  batch paths) — aligning with `computeInLock`'s SOFT_REFRESH branch, the
  `registerRefresh` "every cycle triggers an async refresh" contract, and the
  stale-while-revalidate semantics CONFIG.md already documented. The hard TTL
  stays the absolute bound (ADR-0034). Bare values are skipped; sentinels
  never reach the method.

## Context

A call-chain audit of the data-sync plane found four compounding defects that
together pinned stale values on peer instances — in the worst case for the full
entry TTL, on exactly the concurrent-write hot keys the library targets:

1. **`BroadcastBuffer.record` regressed the pending version.** The pending entry
   was replaced unconditionally (last-*record*-wins), but the record happens
   after the L1 apply, so two concurrent `putThrough` calls can record out of
   INCR order: `record(key, 6)` then `record(key, 5)` left v5 pending, the flush
   sent `REFRESH(5)`, and every peer skipped it (`existing >= incoming`) — the
   v6 write's REFRESH was never sent at all. ADR-0013's "next cycle" convergence
   does not exist for the data plane; on a quiet key the staleness lasted until
   the next write or the whole TTL.
2. **Equal-version REFRESH was swallowed by over-stamped entries.** ADR-0033's
   probe-after-read can stamp an entry one write *ahead* of its data (the reader
   loaded the pre-write value, the writer's INCR landed before the probe). The
   write's own `REFRESH(5)` — the only thing that heals that entry — was skipped
   by the plain sync matrix's `>=` equality case at both guard sites of
   `DefaultSyncDecisionHandler.handleRefresh`.
3. **Batch invalidation carried no version.** `HotKeyCache.invalidate(Iterable,
   true)` sent one unversioned `INVALIDATE_ALL` message; a delayed batch could
   not be ordered against a newer REFRESH on the same key and blindly wiped the
   freshly-applied entry (and its Worker decision stamp) on every peer. The
   single-key path bumps a version per key — the batch was the outlier
   (`invalidateAfterPut(Map)` already used per-key `bumpAndInvalidate`).
4. **The REFRESH no-value fallback invalidated without any guard.** When Redis
   holds no value for the key (the default per ADR-0031), `handleRefresh` fell
   back to a bare `caffeineCache.invalidate` — no version check, no watermark —
   which could delete a strictly newer local write that landed while the REFRESH
   was in flight.

## Decision

- **`VersionGuard.shouldSkipForRefresh`** — a REFRESH-receiver variant of the
  4-case matrix that skips only on a **strictly newer** existing version
  (degraded rules unchanged). An **equal** version applies: re-fetching the
  authoritative value (or falling back to a guarded invalidation when no value
  channel exists) heals the over-stamped entry; skipping would pin the stale
  value. The extra cost for a genuine duplicate is one Redis fetch plus an
  idempotent overwrite. The plain `shouldSkipForSync` (`>=`) is unchanged for
  `buildPutThroughEntry`, `handleLocalInvalidate`, and
  `ExpireManagerImpl.applyRefreshTask` — those guards compare against an
  entry the *caller itself* just produced, where equality means "already
  current", not "possibly over-stamped".
- **`BroadcastBuffer.record` merges by version ordering**: within the same
  space (normal/normal, degraded/degraded) the strictly newer allocation wins —
  an out-of-order record can never regress the pending version. Across the
  degraded boundary the newest **record** wins (legacy last-writer-wins): the
  record order reflects real write order there, and the receiver-side matrix
  decides what applies (a newer degraded write must still reach peers without
  entries; peers holding normal entries skip it on their own). The record also
  re-checks the pending map after its compute and re-records into the live map
  when a concurrent flush swapped it, closing the record-vs-flush swap window
  that could silently strand an entry outside both the flushed snapshot and the
  fresh map.
- **Batch invalidation is per-key and versioned.** `invalidate(Iterable, true)`
  now routes each key through the same `bumpAndInvalidate` path as
  `invalidate(String, true)` — one async version bump plus one INVALIDATE per
  key, matching `invalidateAfterPut(Map)`. The unversioned `INVALIDATE_ALL`
  receiver is kept for rolling upgrades against older peers; it now preserves
  Worker-managed HOT/COOL entries for the same reason the version-less
  single-key path does. Cost: one version bump per key on a rare, explicit
  operation.
- **`handleRefresh`'s no-value fallback invalidation is guarded**: it removes
  the entry via a compute that preserves Worker-managed HOT/COOL entries and
  strictly newer local writes, records the invalidation watermark, and fires
  `afterInvalidate` — the same observable contract as `handleLocalInvalidate`.
- **`handleCool` no longer resurrects hard-expired entries**: a logically
  expired entry is left untouched (it dies on schedule and the next read
  reloads), instead of being rewritten with fresh normal TTLs — which extended
  stale values past the ADR-0034 hard-TTL bound and stretched NullValue
  sentinels far beyond their penetration-protection TTL.
- **`putThrough`'s degraded fallback rewrite keeps the version guard**
  (`forceUpdate=false`), so it can never overwrite a newer normal entry, and the
  unreachable `RejectedExecutionException` catch (REFRESH scheduling degrades to
  a synchronous flush inside `BroadcastBuffer.record`) is removed.
- **`triggerBackgroundRefresh` reserves the key in the compute and creates the
  task outside it**, so a synchronous executor can no longer run the completion
  callback (and its same-key map removal) inside the mapping function — an
  unsupported recursive update the old fast-reader dance survived only by
  reentrancy luck.

## Consequences

- An ADR-0033 over-stamped entry is healed by the write's own REFRESH within
  one message round trip, instead of persisting for the entry TTL.
- A lost/regressed REFRESH broadcast cannot happen from record ordering; the
  buffer's flush latency contract ("worst case `maxDeferMs`") now holds for
  out-of-order records too.
- Large batch invalidations (e.g. `@CacheEvict(allEntries = true)` over a huge
  namespace) cost one version bump and one message per key instead of one
  message — the price of ordering, paid on a rare explicit operation.
- `shouldSkipForRefresh` lets an equal-version duplicate REFRESH re-fetch Redis;
  in the default no-value-channel deployment this surfaces as one extra local
  reload when a reader raced a concurrent write — the conservative direction,
  since the entry it drops may be the over-stamped one.
- A dropped COOL decision remains unrecoverable by design (ADR-0024 never
  rebroadcasts COOL; ADR-0035 covers only dead Worker incarnations) — the
  staleness is bounded by the hot hard TTL and is now documented as such in
  `WorkerListener` instead of being misattributed to the heartbeat cycle.

## Verification

The full suite is green after the change: 1949 common + 313 worker tests.
New regressions pin each fix: `BroadcastBufferMergeTest` (same-space newest,
cross-space newest-record, identical-record reuse), `DefaultSyncDecisionHandlerTest`
(equal-version REFRESH heals; fallback preserves strictly-newer and HOT entries;
legacy batch preserves HOT/COOL), `DefaultWorkerDecisionHandlerTest`
(handleCool skips logically-expired entries, still cools live ones),
`SingleFlightTest` (failOnError timeout resolves empty and retries), and the
updated `ZetaCacheTest` wiring test (per-key versioned batch invalidates).
