# ADR-0095: Caller-Perspective Second Pass (contract vs. runtime, breaking)

Second caller-perspective audit after ADR-0094. Same question per entry
point — what does the contract promise, what does the path do — applied to
the gaps 0094 left open, plus regressions found since. All fixes are
behaviour changes with the contract rewritten to match; `computeIfAbsent*`
is explicitly retained (caller uses the `V`-returning form).

- Status: Accepted
- Date: 2026-10-11
- Related: ADR-0007 (no confirms), ADR-0013 (race fading), ADR-0033
  (version stamping), ADR-0067 (dedup), ADR-0086 (bump-before-drop),
  ADR-0088 (policy split), ADR-0094 (first contract pass)

## Decisions

1. **Refresh allocates before loading** (`HotKeyCache.refresh`). The old
   load-then-bump let a concurrent `putThrough` land mid-load with an older
   version, which the refresh's post-load bump then overwrote — a stale load
   regressing fresher data. The refresh now burns its version up front; the
   `shouldSkipForRefresh` guard skips the apply when a newer entry exists. A
   burned version on loader failure is a watermark hole, the same semantics
   `decisionVersion` already documents.
2. **Invalidation is observable, not acknowledged.**
   `HotKeyCache.invalidate` returns the allocated version (`-1` local-only);
   the batch returns per-key versions; `Zeta` gains
   `invalidateAndGetVersion(s)`; `flushLocalEmergency` replaces
   `invalidateAllLocal` (kept as a deprecated alias). True acknowledgement
   is rejected — impossible under ADR-0007 fire-and-forget — so the fix is
   observability (correlate against peer state/logs), not synchrony.
3. **Batch `invalidateAfterPut(Map)` refuses transactions.** The returned
   failed-keys set is unfulfillable when mutations run at after-commit, so it
   throws instead of lying; single-key stays deferred.
4. **`putThrough` accepts `null` (sentinel) and rejects permanent+broadcast.**
   The engine already sentinel-mapped null; only the facade's
   `requireNonNull` blocked it. `Long.MAX_VALUE` + broadcast fails fast —
   the entry outlives the 7-day version key and peers silently reject later
   updates.
5. **Worker-only fails fast.** `putLocal`'s silent no-op and
   `snapshot()`'s `null` became `ZetaModeException`, matching every other
   data-plane method. `tryLock` keeps its null/false degradation (no Redis is
   an environmental state, not a wiring error).
6. **`tag()` honors rules; intercept fallback tries every namespace.**
   `recordAccess` now throws on BLOCK and downgrades whitelisted keys to
   detect-only, matching the read paths. The intercept fallback peeks each
   blocked namespace in order instead of only the first.
7. **Fluent defaults: broadcast on, reusable builders.** Fallback values are
   cluster state — `putThrough` unless `notAllowBroadcast()`. The
   single-use guards on `ZetaReadQuery`/`ZetaWriteCommand` are removed (the
   read path is idempotent); a reader-less `execute()` fails fast, and a
   reader-less miss resolves empty stale-first instead of NPEing in flight.
8. **Batch TTL is once-per-batch, documented.** `ReadPolicy` suppliers take
   no key, so per-key evaluation cannot differ; the remaining
   `getAllWithSoftExpire(policy)` stale-force is fixed to honor the policy
   like the single-key path.
9. **`peekAndTag` moves to the `ZetaInternal` SPI.** `ZetaSpringCache`
   prefers it at runtime, falling back to the deprecated `Zeta` member for
   legacy implementations. Full removal rides with the next breaking turn.

## Rejected

- Deleting `computeIfAbsent*` (kept: the `V`-returning form is in use).
- Acked invalidation (needs confirms/quorum — contradicts ADR-0007).
- `WritePolicy.nullCaching` expansion (record churn for a flag the engine
  already implements as always-sentinel).
- Batch per-key TTL engine rework (suppliers are key-independent by type).

## Consequences

Breaking: `HotKeyCache.invalidate` return types, `Zeta.invalidateAllLocal`
deprecation, `putLocal`/`snapshot()` Worker-only throws, refresh ordering,
fluent broadcast default, builder reuse, batch-tx rejection,
permanent+broadcast rejection. Tests updated to the new contracts; full
common suite green except one pre-existing flaky feed-loop timing test
(passes in isolation).
