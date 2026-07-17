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

import io.github.hyshmily.zeta.confidence.EvaluationContext;
import io.github.hyshmily.zeta.detection.ZetaStateMachine;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.model.ZetaDecision;
import java.util.concurrent.ConcurrentHashMap;
import lombok.RequiredArgsConstructor;

/**
 * Unified entry point for the Worker's per-report evaluation pipeline.
 *
 * <p>Orchestrates the three-stage evaluation in a single call:
 * <ol>
 *   <li><b>Sliding-window detection:</b> feeds the raw count into
 *       {@link SlidingWindowDetector#addCount} and obtains the binary
 *       is-hot flag plus the window sum.</li>
 *   <li><b>HeavyKeeper lookup:</b> retrieves the cross-instance frequency
 *       estimate for the key via {@link TopK#estimatedCount}.</li>
 *   <li><b>State machine evaluation:</b> packages all signals into an
 *       {@link EvaluationContext} and delegates to
 *       {@link ZetaStateMachine#evaluate(String, boolean, EvaluationContext)}.</li>
 * </ol>
 *
 * <p>Before this facade existed, the pipeline was inlined in
 * {@link io.github.hyshmily.zeta.worker.ingest.ReportConsumer#doOnReport},
 * mixing detection logic with message-consumer concerns. Extracting
 * {@code KeyEvaluator} keeps the consumer focused on batching and
 * broadcasting, and makes the evaluation pipeline independently testable.
 */
@RequiredArgsConstructor
public class KeyEvaluator {

  private static final int CV_HISTORY_SIZE = 20;

  /** Sliding-window detector that tracks per-key frequency over time. */
  private final SlidingWindowDetector detector;

  /** Per-key lifecycle state machine with Bayesian confidence gating. */
  private final ZetaStateMachine stateMachine;

  /** Worker-scoped HeavyKeeper sketch for cross-instance frequency estimation. */
  private final TopK workerTopK;

  /**
   * Per-key sliding-window history for CV computation.
   * Memory per entry is ~200 bytes. Idle entries are evicted by
   * {@link #evictStale} on the same schedule as the state machine cleanup.
   */
  private final ConcurrentHashMap<String, WindowSumHistory> windowSumHistories = new ConcurrentHashMap<>();

  /**
   * Evaluates a single key report and returns a decision.
   *
   * <p>Updates the sliding window, queries the HeavyKeeper sketch,
   * computes the per-key coefficient of variation from recent window
   * sums, and invokes the state machine with full Bayesian context
   * in a single call. The returned {@link ZetaDecision} tells the
   * caller what action, if any, to take (HOT broadcast, COOL
   * broadcast, or none).
   *
   * @param key   the cache key
   * @param count the raw access count from the batch report
   * @return a non-null {@link ZetaDecision} with the action to take
   */
  public ZetaDecision evaluate(String key, long count) {
    boolean isHot = detector.addCount(key, count);
    long windowSum = detector.getWindowSum(key);
    long threshold = detector.getThreshold();
    long cmsCount = workerTopK.estimatedCount(key);

    Double cv = windowSumHistories.computeIfAbsent(key, k -> new WindowSumHistory()).addAndGetCv(windowSum);
    EvaluationContext ctx = new EvaluationContext(cmsCount, windowSum, threshold, cv);

    return stateMachine.evaluate(key, isHot, ctx);
  }

  /**
   * Removes entries whose last access time is older than {@code staleAfterMs}.
   *
   * <p>Called by the scheduled {@code EvictStaleTask} on the same cadence as
   * the state machine and sliding-window detector eviction.
   *
   * @param staleAfterMs maximum idle time in milliseconds before eviction
   */
  public void evictStale(long staleAfterMs) {
    long now = System.currentTimeMillis();
    windowSumHistories.values().removeIf(h -> now - h.lastAccessTime > staleAfterMs);
  }

  /**
   * Ring buffer of recent sliding-window sums for one key, used to compute
   * the coefficient of variation that dynamically adjusts the Bayesian
   * likelihood standard deviation.
   */
  private static final class WindowSumHistory {

    private final double[] buffer = new double[CV_HISTORY_SIZE];
    private int writeIndex = 0;
    private int count = 0;
    volatile long lastAccessTime;

    /**
     * Records a new window sum and returns the current CV, or {@code null}
     * if there are fewer than 5 samples or the mean is near zero.
     */
    @SuppressWarnings("all")
    synchronized Double addAndGetCv(long windowSum) {
      lastAccessTime = System.currentTimeMillis();
      buffer[writeIndex] = windowSum;
      writeIndex = (writeIndex + 1) % CV_HISTORY_SIZE;
      if (count < CV_HISTORY_SIZE) {
        count++;
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
  }
}
