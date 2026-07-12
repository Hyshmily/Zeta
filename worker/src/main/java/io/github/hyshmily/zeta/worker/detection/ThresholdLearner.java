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

import io.github.hyshmily.zeta.worker.config.WorkerProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Periodically recalculates the hot‑key threshold based on estimated global QPS
 * and updates the {@link io.github.hyshmily.zeta.worker.detection.SlidingWindowDetector}.
 */
@RequiredArgsConstructor
@Slf4j
public class ThresholdLearner implements Runnable {

  /** Global QPS estimator providing the overall throughput baseline. */
  private final GlobalQpsEstimator qpsEstimator;
  /** Sliding-window detector whose hot-key threshold will be dynamically adjusted. */
  private final SlidingWindowDetector detector;
  /** Worker configuration properties for threshold tuning parameters (ratio, tolerance, learning period). */
  private final WorkerProperties properties;

  /** Worker startup time (when this bean is constructed). */
  private final long workerStartTime = System.currentTimeMillis();

  private volatile double smoothedQps = 0.0;

  private static final double ALPHA = 0.1;

  /**
   * Executes one threshold recalculation cycle.
   *
   * <p>During the learning period the update is skipped and the fixed
   * threshold is retained.  After the learning period the current QPS is
   * multiplied by the configured ratio to derive a new threshold value;
   * small fluctuations within the tolerance window are ignored.
   *
   * <p>Edge cases handled internally:
   * <ul>
   *   <li>Zero or negative QPS → update is skipped (logged at debug).</li>
   *   <li>Computed threshold below 10 → clamped to 10 to prevent noise.</li>
   *   <li>Non-positive tolerance → all changes are applied unconditionally.</li>
   *   <li>Any exception during calculation → logged as error without crashing the scheduler.</li>
   * </ul>
   */
  @Override
  @SuppressWarnings("all")
  public void run() {
    try {
      // Learning period: skip updates
      if (
        System.currentTimeMillis() - workerStartTime < properties.getGlobalQpsDynamicThreshold().getLearningPeriodMs()
      ) {
        return;
      }

      double currentQps = qpsEstimator.getQps();
      if (currentQps <= 0) {
        return;
      }

      if (smoothedQps == 0.0) {
        smoothedQps = currentQps;
      } else {
        smoothedQps = ALPHA * currentQps + (1 - ALPHA) * smoothedQps;
      }
      long newThreshold = (long) (smoothedQps * properties.getGlobalQpsDynamicThreshold().getHotThresholdRatio());
      // Clamp: never below the configured fixed hot threshold (only tighten, never loosen)
      newThreshold = Math.max(properties.getThreshold().getHotThreshold(), newThreshold);

      long oldThreshold = detector.getThreshold();

      // Tolerance check: avoid small fluctuations
      if (properties.getGlobalQpsDynamicThreshold().getQpsChangeTolerance() > 0) {
        double changeRate =
          oldThreshold == 0 ? Double.MAX_VALUE : Math.abs(newThreshold - oldThreshold) / (double) oldThreshold;
        if (changeRate <= properties.getGlobalQpsDynamicThreshold().getQpsChangeTolerance()) {
          return;
        }
      }

      detector.setThreshold(newThreshold);
      log.debug("Threshold updated: {} -> {} (QPS: {})", oldThreshold, newThreshold, currentQps);
    } catch (Exception e) {
      log.error("Error during threshold learning: {}", e.getMessage(), e);
    }
  }
}
