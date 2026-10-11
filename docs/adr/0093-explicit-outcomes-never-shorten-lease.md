# Explicit Load Outcomes, Never-Shorten Lease, and Lease Observability

Six `Optional.empty()` shapes (null source, breaker suppression, timeout,
executor rejection, reader failure, plus the batch fail-fast throw) flowed
through one type, so `failOnError` failed on exactly the transient paths and
the Spring sync read (`ZetaSpringCache.get`) swallowed loader failures into
`null` against the Spring contract (its unit test mocked the facade, so the
swallow never showed). The lease halved healthy budgets on the first failure
(`remaining 3300s → 1650s`) and its "bounded staleness" claim had no counter
behind it. Decided:

1. **`LoaderOutcome`/`LoadStatus` (six states).** `SingleFlight` gains
   `loadOutcome` primaries (single + two-phase batch) that never throw;
   reader failures surface as `FAILED` with the cause, suppression as three
   distinct `SUPPRESSED_*` states (breaker / timeout / executor — the ADR-0029
   trio, no longer collapsed), null source as `SOURCE_EMPTY`. The legacy
   `Optional` overloads stay as thin bridges (rethrow `FAILED`) so existing
   callers compile; production paths (`HotKeyCache` × 3) own `failOnError`
   in exactly one place each. New failure type `ZetaSuppressedException`
   (keeps its stack — a fail-fast diagnostic, not hot-path control flow).
2. **Stale-first ordering.** Breaker-open + hard-valid stale wins even under
   `failOnError` on all three read paths (single, batch, `computeInLock` —
   the last had the order backwards). Suppressed-with-closed-breaker throws
   under the flag (amends ADR-0029's "timeouts resolve empty in both modes":
   that stays true only for breaker-open degradation). The Spring sync read
   forces fail-fast, so loader failures surface as `ValueRetrievalException`
   and suppression as `ValueRetrievalException(ZetaSuppressedException)`;
   breaker-open-without-stale still resolves `null` (degradation wins there).
3. **Never-shorten lease.** `lease = max(remaining, floor, remaining/2)`
   with the floor following the entry's own scale (`min(120s, hardTtlMs)`);
   hard TTLs below `2 × refreshTimeout` refuse the lease (no retry window
   fits). First failure on a healthy budget is now a soft-timestamp re-arm,
   not a hard-timestamp cut.
4. **Classifier + birth cap.** `leaseOnFailure` takes the peeled `Throwable`
   (one `CompletionException` layer removed — the old code dropped `error`
   on the floor); the `LeaseClassifier` SPI leases everything but `Error` by
   default. `CacheEntry.birthStampSec` (monotonic seconds-since-boot, 4 B,
   `0` = unknown ⇒ admit; stamped on creation, reset on fresh value, carried
   over by `editEntry`) caps total staleness at 30 min, on by default.
5. **Observability on the seam, not in `ZetaCacheStats`.** ADR-0076 rejected
   folding stall causes into the L1 snapshot record, so the three counters
   (`refreshFailureLeased` / `leaseStaleCapped` /
   `leaseSuppressedByClassifier`) plus the bounded (1024) `staleDebt` view
   live on `BackgroundRefresher`, surfaced via `/actuator/hotkey` and four
   `zeta.expire.refresh.*` gauges.
6. **`raisedThreshold` floor step.** `threshold + max(1, threshold>>2)`:
   thresholds below 4 shifted to zero and the hysteresis gate stood open.
   Inert at all shipped thresholds (10 → 12 both ways); pinned by a
   streak-based test that fails on the old formula.

## Considered Options

- **Four states (drop executor/failed):** executor rejection would pollute
  the timeout counter and `FAILED` would stay an exception — the two-layer
  `failOnError` duplication (SingleFlight flag + `executeCore`) survives.
  Six states move the decision to exactly one owner per path.
- **Keep halving (decay by division):** punishes the healthy case to shape
  the sick case; wall-time passage already decays the budget, halving only
  added the first-failure cut. Rejected.
- **Lease counters in `ZetaCacheStats`:** rejected by ADR-0076 (pure L1
  snapshot); the refresher seam owns them.
- **birthStamp as `long` wall time:** +8 B/entry for minute-scale bound
  precision nobody needs; relative monotonic seconds are 4 B and immune to
  NTP steps. Rejected.
- **Parking the tide thread to save idle wakeups:** the scheduler is shared
  (8 threads); parking eats 1/8 of it. Idle cost is one wakeup per base
  interval — cancel-the-`ScheduledFuture` (already the mechanism) stays.
  Rejected, no code change.

## Consequences

- `failOnError=true` now distinguishes missing (`empty` + sentinel) from
  failing (throws) from suppressed (throws `ZetaSuppressedException`)
  except breaker-open degradation (stale/sentinel wins). Spring
  `@Cacheable(sync=true)` loader failures throw per the Spring contract.
- Long entries survive source outages at full remaining budget; short
  entries extend to their own-scale floor; sub-60 s entries and 30 min-old
  values hard-expire; `Error`s never lease. `staleDebt` makes the bound
  auditable per cluster.
- Breaking surface (additive landing, old signatures kept as bridges):
  `SingleFlight` gains outcomes, `CacheEntry` gains an `int`,
  `BackgroundRefresher` gains counters, `ZetaSpringCache` forces fail-fast.
  Full suites green: common 2328 + worker (incl. 3 new lease-cap/classifier
  tests, 10 outcome tests, 2 Spring contract tests, 1 hysteresis test).
- Follow-up (no behavior change): the three per-path FAILED/SUPPRESSED
  switches are folded into one throw-policy kernel
  (`HotKeyCache.throwIfFailFast`) with per-path degrade tails — the throw
  matrix (blocked/error always, interrupt restores + degrades, closed +
  flag throws, open degrades) can no longer drift apart. `executeCore`
  stays separate (it guards side-effect code, not load outcomes). Enum
  switches carry a throwing `default` so a future `LoadStatus` fails loudly
  instead of resolving as a miss.
