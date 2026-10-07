# Size-Aware Sketch Takeover for Batched Increments

HeavyKeeper's collision decay was designed for +1 increments, but `HotKeyDetector` now
feeds it 500 ms WaveCounter batches (tens to thousands per key per tide). A newcomer
whose rows are all occupied with small sums could never admit — its own collisions
pinned the blockers below `minCount` forever (measured: 2 of 10 rotation keys at
est=0 across 49 tides, with and without fading). Decided: a strictly bigger batch
takes the slot outright (`increment > cur`, shared `takeoverSlot` body with the
lottery-takeover); the probabilistic lottery now adjudicates only ties and smaller
claimants. Hot-slot protection is intact (a smaller newcomer still takes the decay
path), ties keep the old lottery, and the drift harness pins 10/10 prompt admission.
Builds on ADR-0026's sampling (untouched for the remaining lottery case) and addresses
the complementary unresponsiveness: not immortal high-cur slots, but pinned small-cur
occupants under saturation.

## Considered Options

- **Credit the incoming increment on non-takeover**: inflates every colliding slot's
  estimate (double counting across keys) — destroys the accuracy the sketch exists for.
- **Tie takeover (`>=`)**: cold-on-cold churn would rewrite fingerprints constantly
  for zero information gain; strict `>` reserves determinism for real dominance.
- **Pure decay retuning (thresholds/ratios)**: moves the pinning equilibrium but cannot
  remove it — the lockout is structural (deterministic rows + self-pinning), not parametric.
- **Rehashing / slot reservation for newcomers**: new sketch state and a bigger blast
  radius; disproportionate while the one-comparison takeover closes the measured gap.

## Consequences

- New heat admits on its first colliding tide at any sketch occupancy; estimates track
  the current dominant claimant rather than historical accumulation — membership
  (the TopK product) gets more accurate, per-slot counts for contested rows lag
  reality (observed: 30150/33935 vs 49000 on formerly blocked rows).
- Near-equal claimants can alternate slot ownership per tide; both stay far above
  admission so membership is stable — counts wobble, the set does not.
- Takeover path skips RNG (cheaper than sampling); no new state, locks, config, or API.
  Full `common` suite green (2295 run, 0 failures); the paired before/after evidence is
  the deterministic drift harness (seed-fixed, tide-driven — the sandbox-methodology
  analog for a sketch change), not a throughput benchmark: the changed branch is
  collision-only, off the match-fingerprint fast path.
