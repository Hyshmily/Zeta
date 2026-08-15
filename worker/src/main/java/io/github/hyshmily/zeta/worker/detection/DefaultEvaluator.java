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
 *       cooling pipeline used for non-fast-lane keys.</li>
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
 * <p>Fast-lane rules are managed by a {@link FastLaneRuleManager} that
 * supports runtime CRUD via {@link
 * io.github.hyshmily.zeta.worker.endpoint.FastLaneEndpoint}.
 */
public class DefaultEvaluator implements Evaluator {

  /** Number of recent window sums retained for CV computation. Must be a power of two. */
  /**
   * Number of window sums retained per key for the CV (coefficient of
   * variation) estimate.
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

  /** Per-key lifecycle state machine. */
  private final ZetaBayesianSM stateMachine;

  /** Runtime-managed fast-lane rules (CRUD via endpoint). */
  private final FastLaneRuleManager fastLaneRuleManager;

  /**
   * Per-key CV history for Bayesian likelihood adjustment.
   * Only populated for keys that go through the Bayesian path.
   */
  private final ConcurrentHashMap<String, WindowSumHistory> windowSumHistories = new ConcurrentHashMap<>();

  /**
   * Per-key EMA (Exponential Moving Average) for momentum-based logThreshold
   * adjustment. Each cell is {@code double[2]}: {@code [0]} the EMA value,
   * {@code [1]} the monotonic timestamp of the last update. Decay is applied
   * lazily on read as {@code prev × α^(elapsed / EVICT_CYCLE_MS)}, so inactive
   * keys need no periodic full-map pass — the wall-clock decay is equivalent
   * to one {@code ×α} tick per 30s eviction cycle.
   */
  private final ConcurrentHashMap<String, double[]> cmsCounts = new ConcurrentHashMap<>();

  /** EMA decay factor: 0.98 ≈ 35-cycle half-life. */
  private static final double CMS_ALPHA = 0.98;

  /**
   * Reference decay cycle in milliseconds — matches the default of
   * {@code zeta.worker.state-machine.evict-interval-ms} (30000). The lazy
   * formula anchors decay to wall time rather than to tick count, so a
   * different eviction interval stays approximately equivalent to the old
   * per-tick decay.
   */
  private static final double EVICT_CYCLE_MS = 30_000.0;

  /**
   * Size gate for the periodic sweep of {@link #cmsCounts}: the sweep runs
   * only when the map exceeds this bound, decaying stale cells by elapsed
   * time and removing those below 1.0. Bounds the map's memory even when
   * dead keys (values ≥ 1.0) would otherwise linger between sweeps.
   */
  private static final int MAX_TRACKED_CMS_KEYS = 100_000;

  /** Global QPS estimator for traffic-normalised trend detection. Nullable. */
  private final GlobalQpsEstimator globalQpsEstimator;

  /** Previous snapshot of {@link GlobalQpsEstimator#getWindowTotal} for ratio computation. */
  private volatile long prevGlobalWindowTotal;

  /**
   * Constructs the evaluator with the given dependencies.
   *
   * @param detector             the sliding-window detector
   * @param stateMachine         the per-key lifecycle state machine
   * @param fastLaneRuleManager  runtime-managed fast-lane rules
   * @param globalQpsEstimator   global QPS estimator for trend normalisation (may be {@code null})
   */
  public DefaultEvaluator(
    SlidingWindowDetector detector,
    ZetaBayesianSM stateMachine,
    FastLaneRuleManager fastLaneRuleManager,
    GlobalQpsEstimator globalQpsEstimator
  ) {
    this.detector = detector;
    this.stateMachine = stateMachine;
    this.fastLaneRuleManager = fastLaneRuleManager;
    this.globalQpsEstimator = globalQpsEstimator;
  }

  /**
   * Evaluate a single key access report and return the action to take.
   *
   * <p>The sliding window is always updated first. Then the fast-lane rules
   * are consulted. If the key matches a rule the fast-lane path is taken;
   * otherwise the full Bayesian pipeline runs.
   *
   * @param key   the cache key being reported
   * @param count the access count in this report batch
   * @return a non-null {@link ZetaDecision} — {@code HOT}, {@code COOL},
   *         or {@code NONE}
   */
  @Override
  public ZetaDecision evaluate(String key, long count) {
    long windowSum = detector.addCount(key, count);

    FastLaneRuleManager.FastLaneRule rule = fastLaneRuleManager.match(key);
    boolean isFastlane = rule != null && windowSum >= rule.threshold();

    return isFastlane ? toFastlane(key) : toBayesianlane(key, count, windowSum);
  }

  /**
   * Fast-lane path: promote unconditionally, bypassing all Bayesian gating.
   *
   * <p>Called when the key matched a fast-lane rule and the current window sum
   * meets or exceeds the rule threshold. Promotes the key to CONFIRMED_HOT
   * without consulting the confidence estimator.
   *
   * @param key the cache key being evaluated
   * @return a non-null {@link ZetaDecision} — {@code HOT} if promoted
   */
  public ZetaDecision toFastlane(String key) {
    return stateMachine.evaluate(key, true, true, EvaluationContext.FASTLANE, () -> 0L);
  }

  /**
   * Bayesian evaluation path: sliding-window sum, trend normalisation, EMA
   * momentum, and confidence-gated state machine.
   *
   * <p>Assembles an {@link EvaluationContext} with all per-key metrics needed
   * for the Bayesian posterior computation:
   *
   * <ul>
   *   <li><b>windowSum</b> — exact count in the current sliding window
   *       (primary observation)</li>
   *   <li><b>CV</b> — coefficient of variation for dynamic likelihood std
   *       adjustment</li>
   *   <li><b>trendStrength</b> — ratio of current window sum to the mean of
   *       the three preceding windows (upward/downward momentum)</li>
   *   <li><b>EMA cmsCount</b> — per-key exponential moving average
   *       ({@code cms = prev × CMS_ALPHA + count}) for gradual-decay
   *       inertia</li>
   *   <li><b>adjustedLogThreshold</b> — {@code log(threshold) - log(momentum)}
   *       where {@code momentum = clamp(cms / windowSum, 0.1, 10.0)}.
   *       Momentum &gt; 1 lowers the bar (sustained key stays HOT more
   *       easily); momentum &lt; 1 raises it (burst spike requires stronger
   *       evidence)</li>
   * </ul>
   *
   * @param key       the cache key being evaluated
   * @param count     the access count in this report batch
   * @param windowSum the current sliding-window sum (pre-computed by caller)
   * @return a non-null {@link ZetaDecision} — {@code HOT}, {@code COOL},
   *         or {@code NONE}
   */
  public ZetaDecision toBayesianlane(String key, long count, long windowSum) {
    long threshold = detector.getThreshold();
    boolean isWindowHot = windowSum >= threshold;

    // Compute global traffic ratio for trend normalisation.
    // When global QPS doubles, a key that doubles is not trending — it is keeping pace.
    long globalTotal = globalQpsEstimator != null ? Math.max(0, globalQpsEstimator.getWindowTotal()) : 0L;
    double globalRatio = globalTotal / (prevGlobalWindowTotal > 0 ? prevGlobalWindowTotal : 1.0);
    prevGlobalWindowTotal = globalTotal;

    WindowSumHistory hist = windowSumHistories.computeIfAbsent(key, k -> new WindowSumHistory());
    Double cv = hist.addAndGetCv(windowSum, globalRatio);
    double trendStrength = hist.getTrendStrength();

    // cmsCount = cmsCount * α^(elapsed/cycle) + count
    // High cmsCount + low windowSum = key was hot but cooling (momentum < 1)
    // Low cmsCount + high windowSum = sudden spike with no history
    double cms = cmsUpdate(key, count);

    // Momentum = cmsCount / windowSum — how much "history" the key carries
    // relative to its current burst size.
    double momentum = (cms > 0 && windowSum > 0) ? Math.max(0.1, Math.min(cms / windowSum, 10.0)) : 1.0;

    // Adjusted logThreshold: momentum > 1 lowers the bar (sustained key),
    // momentum < 1 raises it (first-time spike needs more confidence).
    double rawLogThresh = Math.log(Math.max(threshold, 1.0));
    double adjustedLogThresh = rawLogThresh - Math.log(momentum);
    EvaluationContext ctx = new EvaluationContext(
      (long) cms,
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
   * Lazy time-based EMA update for a key: applies the decay that accumulated
   * since the key's last evaluation, then adds {@code count}. Atomic per key
   * (via {@link ConcurrentHashMap#compute}) so concurrent evaluations of the
   * same key cannot lose an update.
   *
   * @param key   the cache key
   * @param count the access count in this report batch
   * @return the updated EMA value
   */
  private double cmsUpdate(String key, long count) {
    long now = TimeSource.monotonicMillis();
    double[] holder = new double[1];
    cmsCounts.compute(key, (k, cell) -> {
      double prev = 0.0;
      long lastUpdate = now;
      if (cell != null) {
        prev = cell[0];
        // cell[1] stores a millis timestamp as double; the value fits a long
        // exactly (far below 2^53), so the narrowing cast is lossless.
        lastUpdate = (long) cell[1];
      } else {
        cell = new double[2];
      }
      // Fast path: keys are re-evaluated every report cycle (tens of ms) while
      // EVICT_CYCLE_MS is minutes — the integer exponent is 0 and
      // pow(α, 0) == 1.0, so the ~50-100ns native Math.pow call is pure waste
      // on every evaluation of every key. Only cross a decay tick when a whole
      // cycle has actually elapsed (the truncating cast is exact: EVICT_CYCLE_MS
      // is a whole number, so (long)(elapsed / 30000.0) == elapsed / 30000).
      long elapsedCycles = (long) ((now - lastUpdate) / EVICT_CYCLE_MS);
      double decayed = elapsedCycles <= 0 ? prev : prev * Math.pow(CMS_ALPHA, elapsedCycles);
      cell[0] = decayed + count;
      cell[1] = now;
      holder[0] = cell[0];
      return cell;
    });
    return holder[0];
  }

  /**
   * Evict stale CV history entries for keys that have not been evaluated
   * within the given time window.
   *
   * <p>The EMA map needs no periodic full decay pass — decay is applied
   * lazily on read (see {@link #cmsUpdate}). A size-gated sweep runs only
   * when the map exceeds {@link #MAX_TRACKED_CMS_KEYS}, decaying stale cells
   * by elapsed time and dropping those below 1.0 so dead keys cannot
   * accumulate past the bound.
   *
   * @param staleAfterMs maximum idle time in milliseconds before an entry
   *                     is considered stale and removed
   */
  @Override
  public void evictStale(long staleAfterMs) {
    long now = TimeSource.monotonicMillis();
    windowSumHistories.values().removeIf(h -> now - h.lastAccessTime > staleAfterMs);
    if (cmsCounts.size() > MAX_TRACKED_CMS_KEYS) {
      // Decay must run under the per-key bin lock: the previous removeIf
      // mutated the live double[] cell in place (cell[0] *= ...) outside any
      // lock while cmsUpdate's compute read/wrote the same array — a data
      // race whose torn reads can yield NaN and poison the momentum/z-score
      // classification. computeIfPresent serializes the decay+remove per key.
      cmsCounts.forEach((key, ignored) ->
        cmsCounts.computeIfPresent(key, (k, cell) -> {
          // cell[1] stores a millis timestamp as double; the narrowing cast is
          // lossless (see cmsUpdate).
          long elapsedCycles = (long) ((now - (long) cell[1]) / EVICT_CYCLE_MS);
          double decayed = elapsedCycles <= 0 ? cell[0] : cell[0] * Math.pow(CMS_ALPHA, elapsedCycles);
          if (decayed < 1.0) {
            return null;
          }
          return new double[] { decayed, now };
        })
      );
    }
  }

  /**
   * Per-key sliding-window sum history used to compute the coefficient of
   * variation (CV) for Bayesian likelihood adjustment.
   *
   * <p>Maintains a circular buffer of the last {@link #CV_HISTORY_SIZE}
   * window sums. The CV is returned as {@code null} until at least 5
   * samples have been collected or the mean is below 1.0.
   */
  private static final class WindowSumHistory {

    private static final double MAX_TREND_RATIO = 10.0;

    private final double[] buffer = new double[CV_HISTORY_SIZE];
    private int writeIndex = 0;
    private int count = 0;
    volatile long lastAccessTime;

    /**
     * Cached trend strength (current / mean of preceding three windows).
     * Written inside {@link #addAndGetCv} (under the intrinsic lock for
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
     * is 1.0 (flat) rather than 2.0 (strong upward).
     *
     * @param windowSum   the latest sliding-window sum
     * @param globalRatio the ratio of current global QPS to the previous
     *                    window (1.0 = no change, &gt;1.0 = traffic increase)
     * @return the coefficient of variation, or {@code null} if insufficient
     *         data is available
     */
    @SuppressWarnings("all")
    synchronized Double addAndGetCv(long windowSum, double globalRatio) {
      lastAccessTime = TimeSource.monotonicMillis();
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
        return null;
      }

      double sum = 0;
      for (int i = 0; i < count; i++) {
        sum += buffer[i];
      }
      double mean = sum / count;
      if (mean < 1.0) {
        return null;
      }

      double sumSq = 0;
      for (int i = 0; i < count; i++) {
        double d = buffer[i] - mean;
        sumSq += d * d;
      }
      return Math.sqrt(sumSq / count) / mean;
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
