/*
 * Copyright 2026 Hyshmily. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.hyshmily.zeta.worker.detection;

import io.github.hyshmily.zeta.detection.ZetaBayesianSM;
import io.github.hyshmily.zeta.model.EvaluationContext;
import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.worker.rule.FastLaneRuleManager;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Hot-key evaluator with integrated fast-lane support.
 *
 * <p>Evaluation pipeline (two paths):
 *
 * <ol>
 *   <li><b>Fast-lane path:</b> If the key matches a configured fast-lane rule
 *       and the sliding-window sum meets the threshold, the key is promoted to
 *       {@code CONFIRMED_HOT} immediately via {@link ZetaBayesianSM#fastlane},
 *       bypassing all Bayesian confidence gating. Below the fast-lane threshold
 *       the evaluation falls through to the Bayesian path — the same standard
 *       cooling pipeline used for non-fast-lane keys. The path is active only
 *       when the evaluator is constructed with {@code fastLaneEnabled=true}
 *       (wired from {@code zeta.worker.fast-lane.enabled}); when disabled the
 *       rule manager is not consulted at all — rules gossip and storage keep
 *       running elsewhere, so re-enabling needs only the property flip.</li>
 *   <li><b>Bayesian path:</b> For non-matching keys, the standard two-stage
 *       pipeline runs: sliding-window sum → Bayesian confidence-gated
 *       state machine.</li>
 * </ol>
 *
 * <p>The sliding window is updated <em>before</em> the fast-lane check so
 * that the window is always current regardless of which path is taken. The
 * CV history for Bayesian likelihood adjustment is maintained only for
 * non-fast-lane keys.
 *
 * <p>Trend normalisation: {@code ReportConsumer} samples the
 * {@link GlobalQpsEstimator} window total <b>once per report batch</b> and
 * passes the sampled ratio (vs the previous batch's sample) into
 * {@link #evaluate(String, long, double)}. A per-key (per-message) sample
 * would be microseconds apart and yield a ratio of ≈1.000 always, so the
 * documented global-fluctuation normalisation never engaged — batch
 * granularity makes the signal meaningful, and a sampled total of
 * {@code <= 0} maps to the neutral ratio {@code 1.0} (no division, no
 * trend inflation).
 *
 * <p>Fast-lane rules are managed by a {@link FastLaneRuleManager} that
 * supports runtime CRUD via {@link
 * io.github.hyshmily.zeta.worker.endpoint.FastLaneEndpoint}.
 */
public class DefaultEvaluator implements Evaluator {

  /**
   * Number of window sums retained per key for the CV (coefficient of
   * variation) estimate. Must be a power of two.
   *
   * <p>16 is a deliberate memory/precision trade-off: the CV needs at least 5
   * samples and the trend uses the 3 preceding windows, so 16 keeps 3× the
   * minimum while halving the per-key buffer (was 32). The CV estimation
   * noise roughly doubles (standard error ∝ 1/√(2n)) — validated by the
   * worker test suite for decision stability.
   */
  private static final int CV_HISTORY_SIZE = 16;
  private static final int CV_HISTORY_MASK = CV_HISTORY_SIZE - 1;

  /** Sliding-window detector shared with the evaluation pipeline. */
  private final SlidingWindowDetector detector;

  /**
   * Momentum time constant in milliseconds — one full sliding-window span
   * ({@code windowSize × timeMillisPerSlice}). The per-key moving average
   * decays with this constant and is fed the window-sum increment attributable
   * to the elapsed time, so a key with a constant window level {@code W} holds
   * an average ≈ {@code W} and the momentum ratio is calibrated to ≈ 1 for
   * sustained traffic.
   */
  private final long momentumWindowSpanMs;

  /** Per-key lifecycle state machine. */
  private final ZetaBayesianSM stateMachine;

  /** Runtime-managed fast-lane rules (CRUD via endpoint). */
  private final FastLaneRuleManager fastLaneRuleManager;

  /**
   * Whether the fast-lane path is active. Wired from
   * {@code zeta.worker.fast-lane.enabled}. When {@code false} the rule manager
   * is never consulted by {@link #evaluate} — rules gossip/storage keep running
   * elsewhere, so re-enabling is a property flip.
   */
  private final boolean fastLaneEnabled;

  /**
   * Per-key evaluation state (CV history + window moving average) in ONE map
   * entry.
   *
   * <p>Merging the previous two parallel maps ({@code windowSumHistories} and
   * {@code cmsCounts}) halves the per-evaluation map lookups: both components
   * are always touched together by the Bayesian path, so one freshness
   * timestamp serves both. All per-key mutation runs under the entry's
   * intrinsic monitor ({@code synchronized (state)}), which replaces both the
   * old CHM bin-lock lambda (EMA) and the old per-key {@code synchronized}
   * history method (CV) with a single lock acquisition and removes the
   * per-call holder allocation the EMA lambda needed.
   */
  private final ConcurrentHashMap<String, PerKeyEvalState> evalStates = new ConcurrentHashMap<>();

  /**
   * The detector threshold's log, cached: {@code Math.log} was recomputed per
   * key per batch while the threshold itself changes at most once per
   * {@code recalculate-interval-ms} (60 s default). Recomputed lazily when the
   * volatile source field moves; a racing recompute is idempotent.
   */
  private volatile long cachedLogThresholdSource = Long.MIN_VALUE;
  private volatile double cachedLogThreshold = 0.0;

  /**
   * Size gate for the periodic sweep of {@link #evalStates}: the sweep runs
   * only when the map exceeds this bound, decaying stale cells by elapsed
   * time and removing those below 1.0. Bounds the map's memory even when
   * dead keys (values ≥ 1.0) would otherwise linger between sweeps.
   */
  private static final int MAX_TRACKED_CMS_KEYS = 100_000;

  /**
   * Constructs the evaluator with the given dependencies, keeping the
   * pre-gate behaviour (fast-lane rules always consulted). Prefer the
   * 4-arg constructor wired from {@code zeta.worker.fast-lane.enabled}.
   *
   * @param detector             the sliding-window detector
   * @param stateMachine         the per-key lifecycle state machine
   * @param fastLaneRuleManager  runtime-managed fast-lane rules
   */
  public DefaultEvaluator(
    SlidingWindowDetector detector,
    ZetaBayesianSM stateMachine,
    FastLaneRuleManager fastLaneRuleManager
  ) {
    this(detector, stateMachine, fastLaneRuleManager, true);
  }

  /**
   * Constructs the evaluator with the given dependencies and the fast-lane gate.
   *
   * @param detector             the sliding-window detector
   * @param stateMachine         the per-key lifecycle state machine
   * @param fastLaneRuleManager  runtime-managed fast-lane rules
   * @param fastLaneEnabled      {@code true} to consult fast-lane rules on every
   *                             evaluation; {@code false} to bypass the fast-lane
   *                             path entirely (the rule manager is not consulted)
   */
  public DefaultEvaluator(
    SlidingWindowDetector detector,
    ZetaBayesianSM stateMachine,
    FastLaneRuleManager fastLaneRuleManager,
    boolean fastLaneEnabled
  ) {
    this.detector = detector;
    this.stateMachine = stateMachine;
    this.fastLaneRuleManager = fastLaneRuleManager;
    this.fastLaneEnabled = fastLaneEnabled;
    // Momentum time constant = one full sliding window. A mocked detector (or
    // a degenerate configuration) yields 0 — clamp to 1ms to keep the
    // time-constant divisions finite.
    this.momentumWindowSpanMs = Math.max(1L, detector.getWindowSize() * detector.getTimeMillisPerSlice());
  }

  /**
   * Evaluate a single key access report with a neutral global ratio (no
   * normalisation). Prefer {@link #evaluate(String, long, double)} — this
   * overload is the compatibility entry point.
   *
   * @param key   the cache key being reported
   * @param count the access count in this report batch
   * @return a non-null {@link ZetaDecision} — {@code HOT}, {@code COOL},
   *         or {@code NONE}
   */
  @Override
  public ZetaDecision evaluate(String key, long count) {
    return evaluate(key, count, 1.0);
  }

  /**
   * Evaluate a single key access report and return the action to take.
   *
   * <p>The sliding window is always updated first. Then — when the fast-lane
   * gate is enabled — the fast-lane rules are consulted. If the key matches a
   * rule the fast-lane path is taken; otherwise the full Bayesian pipeline
   * runs with the batch-sampled {@code globalRatio} trend normalisation.
   *
   * @param key         the cache key being reported
   * @param count       the access count in this report batch
   * @param globalRatio batch-sampled global traffic ratio (see
   *                    {@link Evaluator#evaluate(String, long, double)});
   *                    non-positive values are treated as the neutral {@code 1.0}
   * @return a non-null {@link ZetaDecision} — {@code HOT}, {@code COOL},
   *         or {@code NONE}
   */
  @Override
  public ZetaDecision evaluate(String key, long count, double globalRatio) {
    long windowSum = detector.addCount(key, count);

    FastLaneRuleManager.FastLaneRule rule = fastLaneEnabled ? fastLaneRuleManager.match(key) : null;
    boolean isFastlane = rule != null && windowSum >= rule.threshold();

    return isFastlane ? toFastlane(key) : toBayesianlane(key, windowSum, globalRatio);
  }

  /**
   * Fast-lane path: promote unconditionally, bypassing all Bayesian gating.
   *
   * <p>Called when the fast-lane gate is enabled, the key matched a fast-lane
   * rule, and the current window sum meets or exceeds the rule threshold.
   * Promotes the key to CONFIRMED_HOT without consulting the confidence
   * estimator.
   *
   * @param key the cache key being evaluated
   * @return a non-null {@link ZetaDecision} — {@code HOT} if promoted
   */
  public ZetaDecision toFastlane(String key) {
    return stateMachine.evaluate(key, true, true, EvaluationContext.FASTLANE, () -> 0L);
  }

  /**
   * Bayesian evaluation path: sliding-window sum, trend normalisation, window
   * moving average, and confidence-gated state machine.
   *
   * <p>Assembles an {@link EvaluationContext} with all per-key metrics needed
   * for the Bayesian posterior computation:
   *
   * <ul>
   *   <li><b>windowSum</b> — exact count in the current sliding window
   *       (primary observation)</li>
   *   <li><b>CV</b> — coefficient of variation for dynamic likelihood std
   *       adjustment</li>
   *   <li><b>trendStrength</b> — ratio of the current window sum (normalised
   *       by the batch-sampled {@code globalRatio}) to the mean of the three
   *       preceding windows (upward/downward momentum)</li>
   *   <li><b>windowAverage</b> — per-key time-decayed moving average of the
   *       window sums (one-window-span time constant) for gradual-decay
   *       inertia</li>
   *   <li><b>adjustedLogThreshold</b> — {@code log(threshold) - log(momentum)}
   *       where {@code momentum = clamp(windowAverage / windowSum, 0.1, 10.0)}.
   *       Momentum &gt; 1 lowers the bar (sustained key stays HOT more
   *       easily); momentum &lt; 1 raises it (burst spike requires stronger
   *       evidence)</li>
   * </ul>
   *
   * @param key         the cache key being evaluated
   * @param windowSum   the current sliding-window sum (pre-computed by caller)
   * @param globalRatio batch-sampled global traffic ratio; non-positive values
   *                    are treated as the neutral {@code 1.0}
   * @return a non-null {@link ZetaDecision} — {@code HOT}, {@code COOL},
   *         or {@code NONE}
   */
  @SuppressWarnings("all")
  public ZetaDecision toBayesianlane(String key, long windowSum, double globalRatio) {
    long threshold = detector.getThreshold();
    boolean isWindowHot = windowSum >= threshold;

    // Normalise per-key trend by global traffic: when global QPS doubles, a
    // key that doubles is not trending — it is keeping pace. The ratio is
    // sampled ONCE per report batch by ReportConsumer and passed down here;
    // a non-positive sample (estimator absent, cold shard, first batch) maps
    // to the neutral 1.0 — no division, no trend inflation.
    double ratio = globalRatio > 0 && !Double.isNaN(globalRatio) ? globalRatio : 1.0;

    PerKeyEvalState state = evalStates.computeIfAbsent(key, k -> new PerKeyEvalState());
    long now = TimeSource.monotonicMillis();
    double cv;
    double trendStrength;
    double windowAverage;
    synchronized (state) {
      state.lastEvalTime = now;
      cv = state.history.addAndGetCv(windowSum, ratio);
      trendStrength = state.history.getTrendStrength();
      windowAverage = state.updateWindowAverage(windowSum, now, momentumWindowSpanMs);
    }

    // Momentum = windowAverage / windowSum — how much sustained history the
    // key carries relative to its current burst size. Both sides share the
    // same unit (window sums), so a key holding a steady window level sits at
    // momentum ≈ 1 (no adjustment); a burst spike leaves the average behind
    // (momentum < 1) and a cooling key keeps the average above its shrinking
    // window (momentum > 1).
    double momentum = (windowAverage > 0 && windowSum > 0)
      ? Math.max(0.1, Math.min(windowAverage / windowSum, 10.0))
      : 1.0;

    // Adjusted logThreshold: momentum > 1 lowers the bar (sustained key),
    // momentum < 1 raises it (first-time spike needs more confidence).
    double rawLogThresh = logThreshold(threshold);
    // momentum == 1.0 (no history yet: windowAverage or windowSum is zero)
    // makes the adjustment exactly zero — skip the native log call on that
    // common path.
    double adjustedLogThresh = momentum == 1.0 ? rawLogThresh : rawLogThresh - Math.log(momentum);
    EvaluationContext ctx = new EvaluationContext(
      (long) windowAverage,
      windowSum,
      threshold,
      cv,
      rawLogThresh,
      adjustedLogThresh,
      trendStrength
    );

    return stateMachine.evaluate(key, isWindowHot, false, ctx, () -> detector.getWindowSum(key));
  }

  /**
   * Returns {@code log(max(threshold, 1))}, recomputed only when the detector
   * threshold changes (at most once per recalculate interval).
   *
   * @param threshold the current detector threshold
   * @return the cached natural log of the threshold
   */
  private double logThreshold(long threshold) {
    if (threshold == cachedLogThresholdSource) {
      return cachedLogThreshold;
    }
    double computed = Math.log(Math.max(threshold, 1.0));
    cachedLogThresholdSource = threshold;
    cachedLogThreshold = computed;
    return computed;
  }

  /**
   * Evicts stale evaluation state for keys that have not been evaluated
   * within the given time window.
   *
   * <p>The merged per-key state needs no periodic EMA decay pass — decay is
   * applied lazily on read (see {@link PerKeyEvalState#updateEma}). A
   * size-gated sweep still runs when the map exceeds
   * {@link #MAX_TRACKED_CMS_KEYS}, decaying stale EMA cells by elapsed time
   * and dropping entries whose EMA has decayed below 1.0 so dead keys cannot
   * accumulate past the bound between eviction scans.
   *
   * @param staleAfterMs maximum idle time in milliseconds before an entry
   *                     is considered stale and removed
   */
  @Override
  public void evictStale(long staleAfterMs) {
    long now = TimeSource.monotonicMillis();
    evalStates.values().removeIf(state -> now - state.lastEvalTime > staleAfterMs);
    if (evalStates.size() > MAX_TRACKED_CMS_KEYS) {
      // The sweep's decay runs under the entry's monitor — the same one
      // updateEma uses — so it can never mutate a live cell concurrently with
      // an evaluation. (The pre-merge sweep mutated the double[] cell in
      // place outside any lock; torn reads could yield NaN and poison the
      // momentum/z-score classification.)
      evalStates.values().removeIf(state -> state.decayAndCheckDead(now, momentumWindowSpanMs));
    }
  }

  /**
   * Per-key evaluation state: CV history + window moving average + freshness,
   * guarded by the entry's intrinsic monitor. Callers synchronize on the state
   * for every mutation ({@link #updateWindowAverage},
   * {@link WindowSumHistory#addAndGetCv}, {@link #decayAndCheckDead}), so
   * concurrent evaluations of the same key serialize without a second lock.
   */
  private static final class PerKeyEvalState {

    /** Sliding-window sum history for the CV estimate. */
    final WindowSumHistory history = new WindowSumHistory();

    /**
     * Time-decayed moving average of the key's sliding-window sums (the
     * momentum reference). Mutated under the state monitor.
     */
    double windowAverage;

    /**
     * Monotonic timestamp of the last average update (the decay anchor);
     * {@code 0} until the first update. Mutated under the state monitor.
     */
    long averageUpdateMillis;

    /**
     * Monotonic timestamp of the last evaluation (volatility: the eviction
     * scan reads it lock-free; a stale read only delays removal by one scan).
     */
    volatile long lastEvalTime;

    /**
     * Time-decayed moving average of the window sums, with a one-window-span
     * time constant — the reference the momentum ratio is calibrated against.
     * Each update decays the previous value by the elapsed time and adds the
     * window-sum increment attributable to that elapsed time
     * ({@code windowSum × Δt/span}), so a key holding a constant window level
     * {@code W} converges to an average ≈ {@code W}. Must be called under the
     * state monitor so concurrent evaluations of the same key cannot lose an
     * update.
     *
     * <p>The first update for a key seeds the average with one full window
     * sum — no elapsed-time evidence exists yet, and momentum 1.0 (neutral)
     * is exactly the right first impression.
     *
     * <p>The source term is capped at one window's worth: across a long
     * reporting gap the only evidence is the end-of-gap window sum, and the
     * decay ({@code exp(-Δ/span)}, uncapped) forgets the pre-gap history.
     *
     * @param windowSum the current sliding-window sum
     * @param now       current monotonic millis
     * @param spanMs    the momentum time constant (one sliding-window span)
     * @return the updated average
     */
    double updateWindowAverage(long windowSum, long now, long spanMs) {
      long last = averageUpdateMillis;
      if (last == 0) {
        windowAverage = windowSum;
        averageUpdateMillis = now;
        return windowAverage;
      }
      long elapsed = Math.max(0, now - last);
      double decay = Math.exp(-elapsed / (double) spanMs);
      double sourceFraction = Math.min(1.0, elapsed / (double) spanMs);
      windowAverage = windowAverage * decay + windowSum * sourceFraction;
      averageUpdateMillis = now;
      return windowAverage;
    }

    /**
     * Decay-sweep step for one entry: advances the moving average's decay to
     * {@code now} and reports whether the value has decayed below 1.0 (the
     * entry is dead and the caller may remove it). Must run under the state
     * monitor — {@code removeIf} invokes it, so the method itself takes the
     * monitor to serialize against {@link #updateWindowAverage}.
     *
     * @param now current monotonic millis
     * @return {@code true} if the decayed average is below 1.0 (entry removable)
     */
    synchronized boolean decayAndCheckDead(long now, long spanMs) {
      if (averageUpdateMillis == 0) {
        return true;
      }
      long elapsed = Math.max(0, now - averageUpdateMillis);
      windowAverage *= Math.exp(-elapsed / (double) spanMs);
      averageUpdateMillis = now;
      return windowAverage < 1.0;
    }
  }

  /**
   * Per-key sliding-window sum history used to compute the coefficient of
   * variation (CV) for Bayesian likelihood adjustment.
   *
   * <p>Maintains a circular buffer of the last {@link #CV_HISTORY_SIZE}
   * window sums. The CV is returned as {@code Double.NaN} until at least 5
   * samples have been collected or the mean is below 1.0.
   *
   * <p>Not internally synchronized: callers mutate it under the owning
   * {@link PerKeyEvalState}'s monitor, which also covers the EMA — one lock
   * acquisition serves the whole per-key evaluation.
   */
  private static final class WindowSumHistory {

    private static final double MAX_TREND_RATIO = 10.0;

    private final double[] buffer = new double[CV_HISTORY_SIZE];
    private int writeIndex = 0;
    private int count = 0;

    /**
     * Cached trend strength (current / mean of preceding three windows).
     * Written inside {@link #addAndGetCv} (under the state monitor for
     * buffer safety), read outside the lock via {@link #getTrendStrength}.
     */
    private volatile double trendStrength = 0.0;

    /**
     * Record a new window sum and return the current CV.
     *
     * <p>Also computes and caches {@link #trendStrength} as the ratio of
     * {@code windowSum} (optionally normalised by {@code globalRatio}) to
     * the mean of the three preceding non-zero window sums (t-3, t-2, t-1).
     * A ratio above 1.0 indicates an upward trend; below 1.0 indicates a
     * downward trend. {@code 0.0} is returned when fewer than 5 samples are
     * available or all preceding windows are zero.
     *
     * <p>Global ratio normalisation prevents global traffic fluctuations
     * from being mistaken for per-key trends: if the overall QPS doubles
     * and a key's window sum also doubles, the normalised trend strength
     * is 1.0 (flat) rather than 2.0 (strong upward). The ratio is sampled
     * once per report batch by {@code ReportConsumer} (not per key), and is
     * always strictly positive — a non-meaningful sample arrives as the
     * neutral {@code 1.0}, never as {@code 0} or {@code NaN}.
     *
     * @param windowSum   the latest sliding-window sum
     * @param globalRatio the batch-sampled global traffic ratio
     *                    ({@code 1.0} = no change, {@code >1.0} = traffic increase;
     *                    guaranteed strictly positive)
     * @return the coefficient of variation, or {@code Double.NaN} if insufficient
     *         data is available
     */
    @SuppressWarnings("all")
    double addAndGetCv(long windowSum, double globalRatio) {
      buffer[writeIndex] = windowSum;
      writeIndex = (writeIndex + 1) & CV_HISTORY_MASK;
      if (count < CV_HISTORY_SIZE) {
        count++;
      }

      if (count < 5) {
        trendStrength = 0.0;
      } else {
        // Normalised trend = (windowSum / globalRatio) / mean of three preceding non-zero windows.
        // Dividing by globalRatio removes the effect of global traffic changes from the trend signal.
        double normSum = windowSum / Math.max(globalRatio, 0.1);
        double sum = 0;
        int samples = 0;
        for (int i = 2; i <= 4; i++) {
          double prev = buffer[(writeIndex - i) & CV_HISTORY_MASK];
          if (prev > 0) {
            sum += prev;
            samples++;
          }
        }
        trendStrength = samples == 0 ? 0.0 : Math.min(normSum / (sum / samples), MAX_TREND_RATIO);
      }

      if (count < 5) {
        return Double.NaN;
      }

      // Welford's single-pass mean/variance: one pass over the buffer instead
      // of two (sum, then sum of squared deviations around the mean), and
      // numerically at least as stable.
      double mean = 0.0;
      double m2 = 0.0;
      for (int i = 0; i < count; i++) {
        double value = buffer[i];
        double delta = value - mean;
        mean += delta / (i + 1);
        m2 += delta * (value - mean);
      }
      if (mean < 1.0) {
        return Double.NaN;
      }

      return Math.sqrt(m2 / count) / mean;
    }

    /**
     * Return the cached trend strength computed during the last
     * {@link #addAndGetCv} call. Lock-free volatile read.
     *
     * @return trend ratio, or {@code 0.0} if insufficient data
     */
    double getTrendStrength() {
      return trendStrength;
    }
  }
}
