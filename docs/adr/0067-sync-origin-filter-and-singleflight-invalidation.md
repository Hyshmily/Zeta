# 0067. Sync Origin Filtering & Write-Path SingleFlight Invalidation

## Status

Accepted (2026-09-12)

## Context

A post-ADR-0066 call-chain audit (a run-to-completion reproduction, not just a
code read) found two compounding defects on the data plane:

1. **The sender's own REFRESH undid its own write.** The sync fanout delivers
   every broadcast back to the sender's per-instance queue
   (`zeta.sync:{instanceId}`), and sync messages carried no origin identity —
   `CacheSyncPublisher.doSend` set only type/version/degraded/messageId and the
   listener had no self-check. ADR-0066's `shouldSkipForRefresh` applies an
   **equal**-version REFRESH (to heal ADR-0033's over-stamped peer entries), so
   the sender's own `REFRESH(v)` — the message for the write it *just* applied —
   equal-applied too. In the default no-value-channel deployment (ADR-0031)
   `invalidateForValuelessRefresh` then **removed the entry `putThrough` had
   just written**, deterministically, on every `putThrough(broadcast=true)` /
   `@CachePut` store: the write-through's local cache update was undone
   ~`flushDelayMs` after every write. ADR-0066's Consequences had priced
   equal-apply as "one extra local reload when a reader raced a concurrent
   write" — on peers. Self-delivery made it unconditional on the sender.
2. **Completed SingleFlight futures replayed pre-invalidation values.**
   ADR-0002 keeps a *completed* dedup future cached for the 5s
   `inflight-ttl-seconds` window (catch-only invalidate). No write or
   invalidation path cleared that entry — the `SingleFlight` interface had no
   invalidate at all — so any L1 loss for a key within 5s of a load (peer
   INVALIDATE, eviction, defect 1's self-REFRESH drop, `invalidate()`,
   `invalidateAfterPut`) was followed by a post-loss miss that *replayed the
   pre-invalidation value* from the dedup future, re-invoking neither the
   reader nor anything else, and re-cached it **unstamped** (`VERSION_DEFAULT`)
   — the stale value then persisted for the full entry TTL. The audit's
   compound repro: `get→A` / `putThrough→B` / self-REFRESH drops the entry /
   `get` → returned `A`, fresh reader never invoked, L1 repopulated with
   `(A, v0)`.

## Decision

- **Origin-instance filtering for REFRESH.** `CacheSyncPublisher` stamps every
  sync message with `HEADER_ORIGIN_INSTANCE` (`InstanceIdGenerator.get()`);
  `CacheSyncListener` drops the message when origin == self **and** type ==
  `REFRESH`. Other self-message types keep their historical processing: a
  self-INVALIDATE heals invalidate-vs-reload repopulations stamped with older
  versions (the load started before the write can only land a lower version —
  its probe predates the INCR), and RULES_SYNC / INVALIDATE_ALL
  self-processing is already a version-guarded no-op. Messages without the
  header (pre-0067 senders) are always processed — the filter is
  wire-compatible in both directions of a rolling upgrade.
  Rejected: blanket self-drop (loses the self-INVALIDATE heal), sender-side
  watermark blocking (equal-version REFRESH — the ADR-0066 healing case —
  would still apply; the two cases are indistinguishable by version alone).
  The sender-side over-stamp race the self-REFRESH used to heal (~500ms later)
  is instead closed at the source by the load-path guard below.
- **Load-path version guard.** `HotKeyCache.loadCacheEntry`'s compute refuses a
  *stamped* load whose probed `dataVersion` is not strictly newer than a
  healthy existing entry (`VersionGuard.shouldSkipForSync` — equal skips): the
  reader may predate the write it raced (ADR-0033's probe-after-read stamps
  pre-write data with a post-write version), so the store must not regress L1.
  Hard-expired entries and Worker-managed entries keep their existing paths.
- **Write-path dedup invalidation.** `SingleFlight.invalidate(String)` is new:
  it drops the dedup entry (in-flight or completed) so the next miss re-runs
  the reader. Called on the writer side — after a successful `putThrough`
  mutation and next to every L1 removal (`invalidate` single/batch,
  `invalidateAfterPut` single/batch) — and on the receiver side:
  `DefaultSyncDecisionHandler` invalidates the dedup entry for every applied
  removal (versioned INVALIDATE, value-less REFRESH fallback, legacy batch) via
  an optional `SingleFlight` collaborator (legacy 5-arg constructor keeps
  `null` = historical behavior). `invalidate` absorbs the Caffeine
  recursive-update exception from a re-entrant call (a reader performing a
  synchronous `putThrough`): the future is not yet inserted, the eviction is
  impossible, and the load-path guard covers the resulting store.
- **Null-sentinel version guard.** `buildNullValueEntry` no longer replaces a
  normal entry at or above the null load's probed version (and never replaces
  one against an unstamped null, which carries no version authority).
  Worker-managed entries keep the historical semantics — the null answer is
  authoritative for the value, the sentinel carries the decision stamp forward
  (pinned by the existing stamp-preservation test).
- **REFRESH no longer re-enables a disabled soft TTL.** `handleRefresh`
  recomputed soft expiry from the entry's stored `softTtlMs`; `0` (the
  documented "disabled" convention) was fed to `computeSoftExpireAt`, which
  resolves 0 to the configured default — one peer REFRESH silently re-enabled
  the stale-while-revalidate window the caller had turned off. `softTtlMs=0`
  now stays 0.
- **`@Fallback` no longer swallows `ZetaBlockedException`.** A BLOCK rule is a
  cache-policy rejection before the method body, not a method failure — the
  `@throws ZetaBlockedException` contract of every read API now holds through
  the annotation path even when a `@Fallback` is configured.
- The `putThrough` degraded-fallback comment was corrected: the rewrite keeps
  `forceUpdate=false`, so when the guard skips (existing normal entry vs a
  degraded version) the local L1 stays on its pre-write value until the next
  successful write, a peer broadcast, or the hard TTL — the comment previously
  claimed unconditional coherence.

## Consequences

- The sender's L1 write-through survives its own broadcast; a write's local
  cache update is no longer deterministically dropped. The redundant per-write
  self-REFRESH fetch (value-channel deployments) is gone too.
- A post-invalidation miss always re-invokes the reader within the dedup TTL —
  the documented `invalidate` contract ("the next local get will re-fetch from
  the reader") now actually holds; previously it held only outside the 5s
  dedup window.
- The re-entrant-load + putThrough race is closed twice over: the load's
  stale result is refused by the version guard (equal version ⇒ skip), and the
  caller still sees what its own reader loaded — the read-your-race window for
  the *response* is inherent and unchanged.
- One AMQP header per sync message (~30 bytes) and one dedup-cache invalidation
  per write/invalidation — the dedup `invalidate` is a single hash removal on
  paths that already paid a Caffeine invalidate or an AMQP send.
- Instance-ID collision (two JVMs resolving the same `HOSTNAME:port`) would now
  also suppress cross-instance REFRESH — but such a collision already merges
  the two instances' sync queues (`zeta.sync:{instanceId}`), so no *new*
  failure mode.
- ADR-0002's "exception-only invalidate" claim is amended (see its dated
  amendment): invalidation now also happens from the write/invalidation paths.

## Verification

Full suite green: 1963 common + 313 worker. New regressions pin each fix:
`CacheSyncListenerTest` (own REFRESH dropped and own entry survives; peer
REFRESH processed; own INVALIDATE still processed; header-less legacy REFRESH
processed), `DefaultSyncDecisionHandlerTest` (applied removals invalidate the
dedup entry — verified by post-removal reader re-invocation; REFRESH preserves
a disabled soft TTL), `ZetaCacheTest$DedupCoherenceTest` (real-SingleFlight
fixtures: putThrough/invalidate invalidate the dedup entry — the compound
audit repro now serves the fresh value; stamped load not newer than the
existing entry is refused; null sentinel guards — same-version preserved,
unstamped preserved, strictly-older replaced with a stamped sentinel,
worker-managed replaced with the stamp carried forward).

## 2026-09-13 amendment — remaining removal paths covered

A full call-chain audit of the public API found three L1-removal sites the
original pass did not wire to the dedup-invalidation contract, all fixed:

- `HotKeyCache.compareAndInvalidate` — a matching entry is removed inside the
  conditional compute, but the dedup entry survived. The next `get` replayed
  the completed future and re-cached the value the caller had just
  invalidated (the same compound shape the original pass fixed for
  `invalidate`/`invalidateAfterPut`). The removal now calls
  `SingleFlight.invalidate` — only when the compute actually removed
  something.
- `HotKeyCache.invalidateAllLocal` — the emergency flush cleared the L1 but
  left every dedup entry; any flushed key with a completed future was
  re-cached from the replay within the dedup TTL. The interface gains
  `SingleFlight.invalidateAll()` (a `default` no-op for backward
  compatibility with custom beans; the Caffeine implementation clears the
  whole dedup cache) and the flush calls it.
- `HotKeyCache.revalidateSoftExpired` — the REVALIDATE drop-and-reload
  removed the entry but the documented "the loader re-invoked" contract could
  silently replay the pre-drop value from the dedup future. The identity-guarded
  removal now drops the dedup entry first, so the reload genuinely re-invokes
  the loader.
- `HotKeyCache.putLocal` (write side, second audit pass) — the invariant is
  phrased over removals, but a local **write** obsoletes prior load results
  just the same, and putLocal is the one write path with no version bump to
  protect it: a completed pre-write dedup result replayed onto the next miss
  re-cached the pre-write value over the local write (loadCacheEntry's
  unstamped-entry branch cannot refuse a stamped replay against an unstamped
  putLocal entry). The write now calls `SingleFlight.invalidate` next to its
  compute, matching `putThrough`'s ordering (same-dedup-contract comment).

Residual (documented, not fixed): the read-path hard-expiry removal
(`ExpireManagerImpl.invalidateIfIsLogicallyExpired`) still allows a ≤5s dedup
replay to re-cache the expired value. The staleness is bounded by the dedup
TTL and the load-path version guard, and fixing it would couple ExpireManager
to SingleFlight across every hit check — not worth the wiring for the bounded
window.

Companion facade fix in the same pass: `Zeta.registerRefresh`'s
put-then-cancel replacement let two concurrent registrations cancel each
other's replacement and leave a dead future registered (the timed refresh
silently stopped). The replace is now an atomic `compute` (cancel the
displaced future inside the remapping), and the per-tick `CachePolicy` build
moved out of the tick lambda. `Zeta.refresh`'s javadoc was corrected to
describe the actual load→evict→cache order and its non-atomicity against
concurrent writers (behavior unchanged).

New regressions: `ZetaCacheTest$DedupCoherenceTest`
(`compareAndInvalidate_invalidatesDedupEntry_nextMissRereadsSource`,
`compareAndInvalidate_mismatch_keepsEntryAndDedupFuture`,
`invalidateAllLocal_clearsDedupCache_nextMissRereadsSource`,
`revalidateDropOfSoftExpiredEntry_reinvokesLoader` — each verified to fail
with the fix sites disabled) and `ZetaTest.registerRefresh_concurrentRegistrations_leaveExactlyOneAliveFuture`.
