# Circuit Breaker Asymmetric Probe Credit Across Half-Open Episodes

The breaker's OPEN→HALF_OPEN→OPEN flap cycle treated every episode identically: each recovery attempt started with the full `halfOpenMaxProbes` quota no matter how many previous episodes had failed. A chronically flapping data source (recovering enough to pass the single-test interval, failing under the first real load) kept re-attacking with full aggression. We introduce a persistent **probe credit** that tightens the quota across failed episodes and recovers it on clean successes — the asymmetry RocksDB's `write_controller.cc::SetupDelay` uses for write-rate feedback (`kIncSlowdownRatio=0.8` vs `kDecSlowdownRatio=1.25`, "penalty > reward of recovering").

## Status

accepted

## Context

ADR-0018's anti-flapping design already pins two facts this change must respect: the `consecutiveSuccessThreshold` gates HALF_OPEN→CLOSED, and the CLOSED fast path must stay a single volatile read (ADR-0071) — so any recovery machinery must live entirely in the HALF_OPEN/OPEN branches. The breaker's only direct consumer is `SingleFlightImpl` (plus `HotKeyCache`'s degraded-read check through `isBreakerOpen()`), which means a failed recovery episode directly maps to "the data source could not take even `halfOpenMaxProbes` concurrent probes".

RocksDB's `SetupDelay` couples the write rate to compaction debt with deliberately asymmetric factors: debt worsening ×0.8, debt cleared ×1.25, near-stop ×0.6, with a rate floor. The shape — penalty steeper than reward, hard floor, persistent between adjustments — is what prevents oscillation; the same shape transfers to a breaker quota multiplier.

## Decision

- New `volatile double probeCredit`, clamped to `[PROBE_CREDIT_FLOOR=0.25, PROBE_CREDIT_BASELINE=1.0]`, starting at 1.0.
- `onFailure` in HALF_OPEN (→OPEN): `probeCredit = max(FLOOR, probeCredit × 0.8)` — **persists across episodes**; chronic flapping converges to single-probe episodes (`max(1, (int)(maxProbes × 0.25))` ≥ 1 always).
- `onSuccess` in HALF_OPEN: `probeCredit = min(BASELINE, probeCredit × 1.25)` — recovers toward baseline *within* an episode, so a probe sequence that behaves well widens the remaining live quota back to the configured cap.
- HALF_OPEN→CLOSED (full recovery): `probeCredit = 1.0` — a healed data source gets a clean slate; the tightening memory is meaningful only while the source keeps failing recoveries.
- The quota stays `max(1, (int)(halfOpenMaxProbes × probeCredit))` — the credit **only ever tightens**; `halfOpenMaxProbes` remains the hard ceiling. This is the deliberate divergence from the borrowing report's original sketch (which proposed letting successes exceed the base quota): an operator knob must not be silently exceeded by an internal multiplier, and the existing pinned tests (`halfOpenProbeQuota_notLeakedByTransitionerOnSuccess`, `halfOpenQuota_floorsAtZeroUnderPerKeyOverRelease`) encode exactly that contract.
- The coefficients are constants, not configuration, per the ADR-0018 convention: count-type thresholds are config, strategy-type ratios are code. `transitionToHalfOpen` deliberately does **not** reset the credit (that persistence is the feature).

## Considered Options

- **Let successes raise the quota above `halfOpenMaxProbes` (ceiling 2-4×):** contradicts the protective purpose of the knob and breaks the pinned quota-cap tests. Rejected.
- **Exponentially decreasing consecutive-success threshold (gradual closing):** reshapes a configured semantic (ADR-0018), needs new state to define "recovery progress", and is hard to pin deterministically. Rejected.
- **RocksDB-style credit token bucket on probe throughput:** the probe path is a boolean quota, not a byte-throughput budget — the credit model has no surface to act on. Rejected.
- **Any credit check on the CLOSED fast path:** would re-introduce work into the highest-QPS path ADR-0071 just cleaned. Rejected — CLOSED resets the credit and pays nothing.

## Consequences

1. Chronic flapping now converges: each failed episode multiplies the next episode's quota by 0.8 (5 failed episodes ≈ 0.33 → near the 1-probe floor), while every full recovery restores full aggression.
2. Within a single episode, probes that succeed widen the live quota back toward `halfOpenMaxProbes`, so a genuinely recovering source is not throttled by one stale failure.
3. `probeCredit=1.0` is behavior-identical to the previous implementation (`(int)(n × 1.0) = n`), so all pre-existing choreographies pass unchanged; three new tests pin the tighten / recover / reset choreographies.
4. The credit is a `volatile double` with plain volatile writes — concurrent probe settles may overshoot by one step, bounded by the clamps; no CAS loop is warranted on a path that is already probe-gated.
