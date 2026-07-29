# HeavyKeeper collision-decay rework: constant per-unit survival probability

`decayCollisionSlot()` was computing decay as `sampleBinomial(increment, decay^cur)` with `increment` (typically 1) as the binomial trials. This was flawed in two ways: it used the wrong population (`increment` instead of `cur`) and the per-unit survival probability `decay^cur` made high-count slots effectively immortal — a slot with cur=100 required ~4000 collisions to decrement once.

**Decision:** Replace with `decays = cur - sampleBinomial(cur, decay)` — a constant per-unit survival probability with `cur` as the binomial trials. The `lookupTable` and `logDecay` fields (previously used for `decay^cur`) are removed; only `survivalProb = decay` is stored.

## Comparison

| Aspect | Before (`decay^cur` + `increment` trials) | After (`decay` + `cur` trials) |
|---|---|---|
| Per-unit survival prob | `decay^cur` (varies with cur) | `decay` (constant, default 0.92) |
| Binomial trials | `increment` (typically 1) | `cur` (the slot's actual count) |
| Expected decays per collision, cur=1 | `1 × (1-0.92¹) = 0.08` | `1 - Binom(1, 0.92) ≈ 0.08` |
| Expected decays per collision, cur=100 | `1 × (1-0.92¹⁰⁰) ≈ 0` | `100 - Binom(100, 0.92) ≈ 8` |
| Expected decays per collision, cur=10k | `1 × (1-0.92¹⁰⁰⁰⁰) ≈ 0` | `10000 - Binom(10000, 0.92) ≈ 800` |
| High-cur slot behavior | **Stuck**: almost never decays | **Stable**: decays ~8% per collision |

## Rationale

1. **`increment` as trials was semantically wrong.** The pool of units to sample for decay is the slot's existing count (`cur`), not the current operation's delta (`increment`).

2. **`decay^cur` made high-cur slots immortal.** The survival probability per unit drops exponentially with cur. For cur=100 and decay=0.92, `decay^cur ≈ 2.8e-35`. With `increment=1`, the expected decays per collision is ≈0. A slot that accumulated 100 counts would effectively never relinquish it, making the sketch unresponsive to popularity shifts.

3. **Constant decay provides predictable, stable behavior across all cur scales.** Expected decays = `cur × (1-decay)` = 8% of cur per collision, regardless of magnitude. `PROTECTION_THRESHOLD` (MAX_DECAY_RATIO = 25%) prevents excessive single-collision decay on very large counters.

4. **The new formula is a mathematically sound batch approximation** of the sequential model: `cur` independent Bernoulli trials, each with identical survival probability `decay`.

## Trade-offs

- **Deviates from the sequential paper model** (one coin flip, at most one decrement per collision). The batch model processes all `cur` units in one shot, which can remove many units in a single collision. The `PROTECTION_THRESHOLD / MAX_DECAY_RATIO` guard limits per-collision decay to 25% of cur.
- **Constant decay is a coarser approximation** than the original `decay^cur` formula, but the original formula was only correct for single-step sequential decay, not for batch binomial sampling.
