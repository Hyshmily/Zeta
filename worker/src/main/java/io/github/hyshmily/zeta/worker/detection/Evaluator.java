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
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.model.EvaluationContext;
import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.worker.rule.FastLaneRuleManager;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Hot-key evaluator with integrated fast-lane support.
 *
 * <p>Evaluation pipeline (two paths):
 *
 * <ol>
 *   <li><b>Fast-lane path:</b> If the key matches a configured fast-lane rule,
 *       the rule's threshold is compared directly against the sliding-window
 *       sum. On a match, the key is promoted to {@code CONFIRMED_HOT}
 *       immediately via {@link ZetaBayesianSM#fastlane}, bypassing all
 *       Bayesian confidence gating. Below threshold, the evaluation returns
 *       {@code NONE} — stale eviction ({@link ZetaBayesianSM#evictStale})
 *       handles the eventual COOL broadcast for previously-promoted keys.</li>
 *   <li><b>Bayesian path:</b> For non-matching keys, the standard three-stage
 *       pipeline runs: sliding-window sum → HeavyKeeper cross-instance
 *       frequency → Bayesian confidence-gated state machine.</li>
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
public class Evaluator {

  /** Number of recent window sums retained for CV computation. */
  private static final int CV_HISTORY_SIZE = 20;

  /** Sliding-window detector shared with the evaluation pipeline. */
  private final SlidingWindowDetector detector;

  /** Per-key lifecycle state machine. */
  private final ZetaBayesianSM stateMachine;

  /** Worker-scoped HeavyKeeper sketch for cross-instance frequency estimation. */
  private final TopK workerTopK;

  /** Runtime-managed fast-lane rules (CRUD via endpoint). */
  private final FastLaneRuleManager fastLaneRuleManager;

  /**
   * Per-key CV history for Bayesian likelihood adjustment.
   * Only populated for keys that go through the Bayesian path.
   */
  private final ConcurrentHashMap<String, WindowSumHistory> windowSumHistories = new ConcurrentHashMap<>();

  /** Global QPS estimator for traffic-normalised trend detection. Nullable. */
  private final GlobalQpsEstimator globalQpsEstimator;

  /** Previous snapshot of {@link GlobalQpsEstimator#getWindowTotal} for ratio computation. */
  private long prevGlobalWindowTotal;

  /**
   * Constructs the evaluator with the given dependencies.
   *
   * @param detector             the sliding-window detector
   * @param stateMachine         the per-key lifecycle state machine
   * @param workerTopK           the worker-scoped HeavyKeeper sketch
   * @param fastLaneRuleManager  runtime-managed fast-lane rules
   * @param globalQpsEstimator   global QPS estimator for trend normalisation (may be {@code null})
   */
  public Evaluator(
    SlidingWindowDetector detector,
    ZetaBayesianSM stateMachine,
    TopK workerTopK,
    FastLaneRuleManager fastLaneRuleManager,
    GlobalQpsEstimator globalQpsEstimator
  ) {
    this.detector = detector;
    this.stateMachine = stateMachine;
    this.workerTopK = workerTopK;
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
  public ZetaDecision evaluate(String key, long count) {
    long windowSum = detector.addCount(key, count);

    FastLaneRuleManager.FastLaneRule rule = fastLaneRuleManager.match(key);
    boolean isFastlane = rule != null && windowSum >= rule.threshold();

    long threshold = detector.getThreshold();
    boolean isHot = windowSum >= threshold;
    long cmsCount = workerTopK.estimatedCount(key);

    // Compute global traffic ratio for trend normalisation.
    // When global QPS doubles, a key that doubles is not trending — it is keeping pace.
    double globalRatio = 1.0;
    if (globalQpsEstimator != null) {
      long globalTotal = globalQpsEstimator.getWindowTotal();
      if (prevGlobalWindowTotal > 0) {
        globalRatio = (double) globalTotal / Math.max(prevGlobalWindowTotal, 1);
      }
      prevGlobalWindowTotal = globalTotal;
    }

    WindowSumHistory hist = windowSumHistories.computeIfAbsent(key, k -> new WindowSumHistory());
    Double cv = hist.addAndGetCv(windowSum, globalRatio);
    double trendStrength = hist.getTrendStrength();
    EvaluationContext ctx = new EvaluationContext(cmsCount, windowSum, threshold, cv, trendStrength);

    return stateMachine.evaluate(key, isHot, isFastlane, ctx);
  }

  /**
   * Evict stale CV history entries for keys that have not been evaluated
   * within the given time window.
   *
   * @param staleAfterMs maximum idle time in milliseconds before an entry
   *                     is considered stale and removed
   */
  public void evictStale(long staleAfterMs) {
    long now = System.currentTimeMillis();
    windowSumHistories.values().removeIf(h -> now - h.lastAccessTime > staleAfterMs);
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
      lastAccessTime = System.currentTimeMillis();
      buffer[writeIndex] = windowSum;
      writeIndex = (writeIndex + 1) % CV_HISTORY_SIZE;
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
          double prev = buffer[(writeIndex - i + CV_HISTORY_SIZE) % CV_HISTORY_SIZE];
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
