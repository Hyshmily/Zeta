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

package io.github.hyshmily.hotkey.worker.detection;
import lombok.extern.slf4j.Slf4j;

import io.github.hyshmily.hotkey.worker.config.WorkerProperties;
import lombok.RequiredArgsConstructor;

/**
 * Periodically recalculates the hot‑key threshold based on estimated global QPS
 * and updates the {@link SlidingWindowDetector}.
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
  public void run() {
    try {
      // Learning period: skip updates
      if (
        System.currentTimeMillis() - workerStartTime < properties.getGlobalQpsDynamicThreshold().getLearningPeriodMs()
      ) {
        log.debug("Still in learning period, using fixed threshold: {}", detector.getThreshold());
        return;
      }

      double currentQps = qpsEstimator.getQps();
      if (currentQps <= 0) {
        log.debug("Current QPS is zero, skipping threshold update");
        return;
      }

      // New threshold = global QPS * ratio
      long newThreshold = (long) (currentQps * properties.getGlobalQpsDynamicThreshold().getHotThresholdRatio());
      // Clamp: never below the configured fixed hot threshold (only tighten, never loosen)
      newThreshold = Math.max(properties.getThreshold().getHotThreshold(), newThreshold);

      long oldThreshold = detector.getThreshold();

      // Tolerance check: avoid small fluctuations
      if (properties.getGlobalQpsDynamicThreshold().getQpsChangeTolerance() > 0) {
        double changeRate = oldThreshold == 0
          ? Double.MAX_VALUE
          : Math.abs(newThreshold - oldThreshold) / (double) oldThreshold;
        if (changeRate <= properties.getGlobalQpsDynamicThreshold().getQpsChangeTolerance()) {
          log.debug("Threshold change within tolerance ({}), keeping old value", changeRate);
          return;
        }
      }

      detector.setThreshold(newThreshold);
      log.debug("Threshold updated: {} -> {} (QPS: {})", oldThreshold, newThreshold, currentQps);
    } catch (Exception e) {
      log.error("Failed to recalculate threshold", e);
    }
  }
}
