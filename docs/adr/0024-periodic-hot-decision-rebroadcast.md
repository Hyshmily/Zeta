
## Addendum (2026-09-17, ADR-0068): configuration bounds

The rebroadcast interval is the self-healing upper bound for a lost HOT decision under ADR-0007's fire-and-forget semantics, so a misconfigured value directly widens the preheat loss window. `WorkerProperties.StateMachine.rebroadcastIntervalMs` now carries `@Min(1000)` (pre-existing) **and `@Max(60_000)`** (new), enforced fail-fast at properties-binding time:

- below 1s the rebroadcast degenerates into a per-report broadcast storm for every continuously-hot key (only the broadcaster's 100 ms local debounce caps it);
- above 60s a lost HOT can leave new instances un-prewarmed for minutes — beyond that, a units mix-up (ms vs s) is the overwhelmingly likely cause.

Default remains 10s. See ADR-0068 for the full context (shared-broker MQ link review, 2026-09-17).
anout broadcasts per second, absorbed only by the broadcaster's 100 ms debounce cache — steady-state broadcast amplification across every App queue.

## Decision

Promote rebroadcasting to a first-class state-machine semantic: **broadcast on transition, then rebroadcast periodically while the key stays hot.**

1. **`KeyState.lastBroadcastAt`** (volatile long, epoch millis, 0 = never) records when the last HOT broadcast for the key was emitted.
2. **Transition broadcasts** (COLD→HIGH, CANDIDATE_HOT→HIGH, fast-lane promotion) stamp `lastBroadcastAt = now` when returning the HOT decision.
3. **Steady-state branches rebroadcast**: the `evaluateHot` `default` branch (CONFIRMED_HOT + hot window, previously always `NONE`) and the `fastlane()` already-CONFIRMED_HOT branch now return HOT only when `now - lastBroadcastAt >= rebroadcastIntervalMs` (default **10 s**, `zeta.worker.state-machine.rebroadcast-interval-ms`, min 1 s), stamping on emission; otherwise they return `NONE`. This simultaneously fixes the fast-lane amplification (I5) and the lost-HOT unrecoverability (D1).
4. **Rollback clears the stamp**: `rollbackToPreviousState(key, snapshot)` (invoked when an AMQP send fails) sets `lastBroadcastAt = 0`, so the next evaluation window retries immediately. Stamping is optimistic (at decision time). The rollback carries a per-key `mutationSeq` guard (see C10 fix): a snapshot rolls back only while no later evaluation advanced the state — with the Worker's parallel report consumers, a stale rollback would clobber concurrently-advanced state; when it is skipped, the periodic rebroadcast (rule 3) retries the failed decision instead.

**Explicitly excluded:**

- **COOL is not rebroadcast.** A lost COOL fails in the lenient direction (Apps hold hot TTL a while longer; hard TTL and `evictStale` bound it). A lost HOT fails in the strict direction (Apps never promote). Asymmetric treatment mirrors the ADR-0001 local-promotion safety net.
- **PRE_COOLING silent revive does not stamp or broadcast.** Apps already consider the key HOT throughout the cooling grace period; the semantics are correct as-is.
- The broadcaster's 100 ms debounce cache is kept as a secondary guard (it also dedupes cross-path duplicates), but it is no longer the primary anti-amplification mechanism.

## Cost Analysis

Each rebroadcast carries a fresh `decisionVersion`, so every App applies it fully: one Redis GET + TTL re-application in `WorkerListener.handleHot`. With H hot keys, interval I, and N App instances: `H/I` broadcasts/s × N queues. At H=1000, I=10 s, N=20 → 100 msg/s × 20 = 2 000 deliveries/s + 2 000 Redis GET/s cluster-wide — negligible. Side benefits: hot-key TTLs are periodically renewed and stale values refreshed. Rebroadcasts also consume App-side SRE rate-limiter budget, so they self-throttle under load — desired behaviour.

## Alternatives Considered

- **Do nothing (rely on Local TopK):** rejected — local promotion is a safety net with different TTL semantics, not a substitute for the cluster-wide decision; a lost HOT for a locally-cold key is permanently missed.
- **Publisher confirms + retry:** rejected — contradicts ADR-0007 (fire-and-forget); adds broker round-trip latency to the consumer hot path for a problem solvable by idempotent periodic re-emission.
- **Rebroadcast COOL as well:** rejected — lenient-direction failure, bounded by existing mechanisms; doubling rebroadcast traffic buys nothing.
- **Keep fast-lane every-evaluation emission:** rejected — the amplification scales with App count and report rate for zero decision-quality gain.

## Consequences

1. `ZetaBayesianSM` gains a `rebroadcastIntervalMs` constructor parameter; the previous 5-arg constructor delegates with the 10 s default (call-site and test-source compatibility preserved).
2. App-side `handleHot` does meaningfully more work per rebroadcast (Redis GET per key per interval per App). Operators tuning `rebroadcast-interval-ms` below ~5 s on large fleets should size Redis accordingly.
3. `decisionVersion` advances faster than before (one increment per rebroadcast). The App-side `decisionVersion >=` guard makes replays idempotent; no ordering semantics change.
4. State-machine-related ADRs (0016) describing "broadcast once on transition" are superseded by this ADR for the HOT path.
