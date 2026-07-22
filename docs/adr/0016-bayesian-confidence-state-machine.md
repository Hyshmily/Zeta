# Bayesian Confidence-Gated State Machine for HOT/COOL Decisions

The Worker's per-key state machine gates every HOT/COOL broadcast through a Normal-Normal conjugate Bayesian posterior probability rather than a simple threshold comparison. This ADR documents the full model, the three-tier confidence classification, the CV-based dynamic variance scaling, the state machine transition matrix, and the rationale for each design choice.

## Motivation

A naive approach: compare the sliding-window sum against a threshold and broadcast HOT when crossed, COOL when fallen below. This fails because:

1. **Burst vs sustained:** A single bursty window may cross the threshold, but broadcasting HOT for a transient spike wastes AMQP bandwidth and causes needless L1 churn on every App instance.
2. **Oscillation:** A key oscillating just above and below the threshold would produce rapid HOT/COOL toggle storms.
3. **No uncertainty signal:** A binary comparator cannot express "I'm not sure yet" — leading to either aggressive false positives or conservative missed detections.

The Bayesian approach addresses all three by producing a **posterior probability** P(true frequency > threshold) that naturally filters noise, provides a three-tier action space (broadcast / defer / suppress), and degrades gracefully under bursty traffic via CV-based dynamic variance scaling.

## Evaluation Pipeline

```
Report arrival (batch)
        │
        ▼
SlidingWindowDetector.addCount(key, count)
        │  ├── isHot (boolean): windowSum >= threshold
        │  └── windowSum: sum of recent slices
        │
        ▼
KeyEvaluator.evaluate(key, count)
        │  ├── detector.isHot + windowSum
        │  ├── workerTopK.estimatedCount(key)    ← HeavyKeeper sketch
        │  └── WindowSumHistory.addAndGetCv(sum)  ← CV from last 20 windows
        │
        ▼
EvaluationContext(cmsCount, windowSum, threshold, cv, logThreshold)
        │
        ▼
ConfidenceEvaluator.evaluate(cmsCount or windowSum, logThreshold, cv)
        │  └── BayesianConfidenceEstimator.evaluate(observed, logThreshold, cv)
        │       └── NormalCdfTable.phi(z)
        │
        ▼
ProbabilityResult(probability, level, posteriorMean, posteriorStd, cv)
        │
        ▼
ZetaStateMachineImpl.evaluate(key, isHot, ctx)
        └── Decision: HOT / COOL / NONE
```

The observation fed to the Bayesian model is `cmsCount` (HeavyKeeper estimate) when available (> 0), falling back to `windowSum` when the sketch has no record for the key. This prefers the cross-instance global estimate over the local window view.

## Model: Normal-Normal Conjugate

### Mathematical formulation

| Component          | Expression                                                                |
| ------------------ | ------------------------------------------------------------------------- |
| Prior              | μ ~ N(μ₀, σ₀²), where μ₀ = priorMean, σ₀ = priorStd                       |
| Likelihood         | y \| μ ~ N(μ, σ²), where y = ln(max(observedCount, 1)), σ = likelihoodStd |
| Posterior          | μ \| y ~ N(μₙ, σₙ²)                                                       |
| Posterior mean     | μₙ = (μ₀/σ₀² + y/σ²) / (1/σ₀² + 1/σ²)                                     |
| Posterior variance | σₙ² = 1 / (1/σ₀² + 1/σ²)                                                  |
| Z-score            | z = (ln(threshold) - μₙ) / σₙ                                             |
| Hot probability    | P(μ > ln(threshold)) = 1 - Φ(z)                                           |

### Default parameters

| Parameter     | Value           | Rationale                                                                              |
| ------------- | --------------- | -------------------------------------------------------------------------------------- |
| priorMean     | ln(10) ≈ 2.3026 | A key with 10 observed accesses is neutral (posterior = prior).                        |
| priorStd      | 1.0             | One order of magnitude uncertainty around the prior.                                   |
| likelihoodStd | 0.5             | Observation noise at half the prior uncertainty; 4× precision weight on data vs prior. |

### Prior calibration rationale

The prior is calibrated so that:

- **observedCount < 10:** posterior mean shifts **below** prior mean → decreased hot probability.
- **observedCount ≈ 10:** posterior mean ≈ prior mean → neutral (evidence matches prior belief).
- **observedCount > 10:** posterior mean shifts **above** prior mean → increased hot probability.

The choice of 10 was empirical: analysis of production access patterns showed that keys with fewer than 10 accesses per evaluation window (typically 50 ms) were almost never genuinely hot in the cluster-wide view. Keys consistently above 10 per window warranted attention.

The prior precision relative to likelihood precision is 1:4 (priorStd=1.0 vs likelihoodStd=0.5), meaning the prior contributes ~20% weight to the posterior. After ~5 consistent hot windows, the posterior converges toward the data regardless of prior — the prior only delays the first broadcast, not the steady-state behavior.

## Three-Tier Confidence Classification

```
Probability           ConfidenceLevel        State Machine Action
─────────────────────────────────────────────────────────────────
p ≥ 0.95             HIGH                  → CONFIRMED_HOT + broadcast
0.80 ≤ p < 0.95      MEDIUM                → CANDIDATE_HOT (defer broadcast)
p < 0.80             LOW                   → suppress, reset or hold
```

### Threshold rationale

The 0.80/0.95 split was chosen empirically:

- **0.95 (HIGH):** at this threshold, the false-positive rate is approximately 5%. A HOT broadcast triggers L1 promotion across all Apps (up to 1h TTL), so the cost of a false positive is measurable — the Bayesian model requires strong evidence.
- **0.80 (MEDIUM):** at this threshold, evidence is suggestive but not conclusive. The key is held in CANDIDATE_HOT (tracked but not broadcast) so that a single additional hot window can push it to HIGH. If the next window is cold, the key drops back to COLD silently — no broadcast noise.
- **Below 0.80 (LOW):** insufficient evidence. The streak is decremented rather than fully reset, giving the key a second chance on the next window rather than requiring a full restart.

## CV-Based Dynamic Likelihood Adjustment

The likelihood standard deviation σ is adjusted by the coefficient of variation (CV) of recent sliding-window sums. CV measures traffic stability:

- **Low CV (< 0.2):** stable, predictable traffic → reduce σ (higher confidence in the observation)
- **Normal CV (0.2–0.5):** typical variation → use base σ
- **High CV (> 0.5):** bursty, erratic traffic → increase σ (lower confidence)

### Piecewise adjustment function

```
σ_adjusted = σ_base × f(CV)

           ⎧ 0.5 + (CV / 0.2) × 0.5      if CV < 0.2
f(CV) =    ⎨ 1.0                          if 0.2 ≤ CV ≤ 0.5
           ⎩ 1.0 + min((CV - 0.5) / 0.5, 2.0)   if CV > 0.5
```

| CV    | f(CV) | Interpretation                    |
| ----- | ----- | --------------------------------- |
| 0.0   | 0.50  | Perfectly stable: tighten σ by 2× |
| 0.1   | 0.75  | Mildly stable                     |
| 0.2   | 1.00  | Normal threshold                  |
| 0.5   | 1.00  | Upper normal bound                |
| 1.0   | 2.00  | Bursty: loosen σ by 2×            |
| ≥ 1.5 | 3.00  | Maximum dampening: loosen σ by 3× |

The CV is computed from the last 20 window sums per key, maintained in a ring buffer (`WindowSumHistory` in `BayesianEvaluator`). The first 5 samples are required before CV is reported (returns `null`), which triggers use of the base σ unchanged.

## State Machine Transition Matrix

### States

| State         | Meaning                                   | Broadcast?                                |
| ------------- | ----------------------------------------- | ----------------------------------------- |
| COLD          | Default, no tracking state exists         | Never                                     |
| CANDIDATE_HOT | Hot-streak met but only MEDIUM confidence | Never                                     |
| CONFIRMED_HOT | Actively hot, HIGH confidence             | Yes (HOT)                                 |
| PRE_COOLING   | Traffic dropped, grace period             | Conditional (COOL only when cooled fully) |

### Hot window transitions (windowSum ≥ threshold)

| Current                     | Condition  | Next                      | Decision | Rationale                                         |
| --------------------------- | ---------- | ------------------------- | -------- | ------------------------------------------------- |
| COLD, streak < confirmCount | —          | COLD                      | NONE     | Not enough consecutive hot windows                |
| COLD, streak ≥ confirmCount | HIGH       | CONFIRMED_HOT             | **HOT**  | Strong evidence ready for broadcast               |
| COLD, streak ≥ confirmCount | MEDIUM     | CANDIDATE_HOT             | NONE     | Promising but hold for more evidence              |
| COLD, streak ≥ confirmCount | LOW        | COLD (streak decremented) | NONE     | Weak evidence, one more window needed             |
| CANDIDATE_HOT               | HIGH       | CONFIRMED_HOT             | **HOT**  | Additional window pushed to HIGH                  |
| CANDIDATE_HOT               | MEDIUM/LOW | CANDIDATE_HOT (stay)      | NONE     | Still not enough for broadcast                    |
| CONFIRMED_HOT               | any        | CONFIRMED_HOT (stay)      | NONE     | Already hot, no action needed                     |
| PRE_COOLING                 | any        | CONFIRMED_HOT             | NONE     | **Silent revive:** traffic returned, no broadcast |

### Cold window transitions (windowSum < threshold)

| Current       | Condition                      | Next                             | Decision | Rationale                                |
| ------------- | ------------------------------ | -------------------------------- | -------- | ---------------------------------------- |
| COLD          | any                            | COLD                             | NONE     | Already cold                             |
| CANDIDATE_HOT | any                            | COLD                             | NONE     | Single cold window drops candidate       |
| CONFIRMED_HOT | streak < grace                 | CONFIRMED_HOT (stay)             | NONE     | Within grace period                      |
| CONFIRMED_HOT | streak ≥ grace                 | PRE_COOLING                      | NONE     | Enter grace period, check immediate COOL |
| PRE_COOLING   | streak < coolCount             | PRE_COOLING (stay)               | NONE     | Still cooling                            |
| PRE_COOLING   | streak ≥ coolCount, MEDIUM/LOW | COLD                             | **COOL** | Fully cooled, confident                  |
| PRE_COOLING   | streak ≥ coolCount, HIGH       | PRE_COOLING (streak decremented) | NONE     | Bayesian still thinks it's hot           |

### Key design properties

1. **Asymmetric promotion/demotion:** Promotion can happen in as little as 1 window (with HIGH confidence). Demotion requires `coolCount` cold windows (default configurable, typically much larger than `confirmCount`). This hysteresis prevents HOT/COOL oscillation.

2. **Silent revive:** During PRE_COOLING, a single hot window returns the key to CONFIRMED_HOT without broadcasting. This avoids sending a redundant HOT message for a key that was never announced as COOL.

3. **CANDIDATE_HOT as noise filter:** Keys with MEDIUM confidence accumulate in CANDIDATE_HOT without affecting the cluster. This absorbs transient spikes without broadcast cost.

4. **Bayesian override on COOL:** Even after `coolCount` cold windows, the Bayesian model can override: if confidence is still HIGH (the key's frequency estimate from HeavyKeeper suggests it's still hot globally), the COOL broadcast is suppressed and the key stays in PRE_COOLING for another window.

## Relation to ThresholdLearner

The state machine's binary "is this window hot?" gate (`isHotThisWindow = windowSum >= threshold`) uses a **separate** threshold maintained by `ThresholdLearner` in the Worker module. The `ThresholdLearner` estimates global QPS and adjusts the threshold dynamically.

The Bayesian confidence evaluation is **orthogonal** to this threshold: even when `isHotThisWindow = true`, the Bayesian posterior may still be LOW (e.g., the HeavyKeeper sketch count is low, indicating this is a local spike not visible globally). Conversely, when `isHotThisWindow = false`, the internal re-check (`ctx.windowSum() >= ctx.threshold()`) using the same threshold provides a second opinion within the lock.

## Implementation Components

| Class                         | Location                     | Role                                                |
| ----------------------------- | ---------------------------- | --------------------------------------------------- |
| `BayesianConfidenceEstimator` | `worker/.../confidence/`     | Core Normal-Normal conjugate computation            |
| `ConfidenceEvaluator`         | `worker/.../confidence/`     | Thin facade, decouples state machine from estimator |
| `EvaluationContext`           | `common/.../model/`          | Data carrier for all evaluation inputs              |
| `ProbabilityResult`           | `worker/.../confidence/`     | Posterior output with level classification          |
| `ConfidenceLevel`             | `worker/.../confidence/`     | HIGH/MEDIUM/LOW enum                                |
| `NormalCdfTable`              | `worker/.../confidence/`     | Pre-computed Φ(z) table, Abramowitz & Stegun approx |
| `ZetaBayesianSM`              | `worker/.../detection/impl/` | Per-key state machine with Bayesian gating          |
| `BayesianEvaluator`           | `worker/.../detection/`      | Pipeline orchestrator, CV computation               |
| `SlidingWindowDetector`       | `worker/.../detection/`      | Lock-free circular buffer for per-key frequency     |
| `ThresholdLearner`            | `worker/.../detection/`      | Adaptive threshold based on global QPS              |

### NormalCdfTable performance note

The standard Normal CDF Φ(z) is pre-computed across z ∈ [-6, 6] at step 0.001 (12,001 entries) using the Abramowitz & Stegun approximation 26.2.17 (max error 7.5×10⁻⁸). Linear interpolation between entries keeps total error below 1×10⁻⁶. This is far below the granularity of the three-tier classification (0.15 between thresholds) — the table introduces no classification noise while avoiding `Math.exp` + polynomial evaluation on every call.

## Considered Options

- **Fixed threshold comparator** — simpler but cannot distinguish "consistently hot" from "bursty noise". Rejected because false-positive broadcasts waste AMQP bandwidth and cause unnecessary L1 churn on Apps.
- **Frequentist hypothesis test (z-test)** — similar computational cost but requires choosing a significance level that has no natural mapping to the three-tier action space (HOT broadcast / CANDIDATE_HOT wait / COOL broadcast). Bayesian posterior maps directly.
- **MCMC or variational inference** — too expensive for per-key evaluation on every evaluation window (~50 ms).
- **Normal-Normal conjugate (chosen)** — closed form, O(1), three-tier confidence maps directly to state machine transitions.

## Consequences

1. **Prior calibration is static.** The ln(10) prior was chosen empirically based on production traffic analysis. No auto-tuning mechanism exists. If traffic patterns change significantly (e.g., a new use case with much lower or higher baseline QPS), the prior may need manual recalibration via `zeta.worker.bayesian.*` properties.

2. **CV history memory.** Each tracked key holds a 20-element `double[]` ring buffer (~200 bytes per key). Under 100k tracked keys, this adds ~20 MB to Worker heap. `evictStale` on the same cadence as state machine cleanup keeps this bounded.

3. **NormalCdfTable loads at class init.** The 12,001-element table (~96 KB) is populated once via static initializer. Cold-start latency impact is negligible, and the memory is shared across all Worker shards in the same JVM.

4. **Three-tier thresholds are uncalibrated.** The 0.80/0.95 splits were chosen based on developer intuition and ad-hoc testing, not systematic ROC analysis. If production data shows excessive false HOT broadcasts or missed detections, these thresholds should be revisited.

5. **State machine code in worker module.** `ZetaBayesianSM` and all confidence types reside in the `worker` module, never shipped in the Maven Central `zeta` starter JAR. Only the `ZetaBayesianSM` interface stays in `common` for type safety across module boundaries. The Worker module depends on `common` for the interface and packages the implementation exclusively in its own artifact.
