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

package io.github.hyshmily.hotkey.worker;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Periodically recalculates the hot‑key threshold based on estimated global QPS
 * and updates the {@link SlidingWindowDetector}.
 */
@Slf4j
@RequiredArgsConstructor
public class ThresholdLearner implements Runnable {

  private final GlobalQpsEstimator qpsEstimator;
  private final SlidingWindowDetector detector;
  private final WorkerProperties properties;

  /** Worker startup time (when this bean is constructed). */
  private final long workerStartTime = System.currentTimeMillis();

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
      // Absolute minimum to prevent noise
      newThreshold = Math.max(10, newThreshold);

      long oldThreshold = detector.getThreshold();

      // Tolerance check: avoid small fluctuations
      if (properties.getGlobalQpsDynamicThreshold().getQpsChangeTolerance() > 0) {
        double changeRate = Math.abs(newThreshold - oldThreshold) / (double) oldThreshold;
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
