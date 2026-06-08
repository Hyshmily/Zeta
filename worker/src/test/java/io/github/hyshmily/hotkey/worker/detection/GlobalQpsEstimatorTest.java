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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link GlobalQpsEstimator}.
 */
class GlobalQpsEstimatorTest {

  @Test
  void shouldConstructWithInitializedSlices() {
    GlobalQpsEstimator estimator = new GlobalQpsEstimator(1000, 10);
    assertThat(estimator.getWindowTotal()).isZero();
    assertThat(estimator.getQps()).isZero();
  }

  @Test
  void shouldComputeWindowTotalAfterAddingCounts() {
    GlobalQpsEstimator estimator = new GlobalQpsEstimator(10_000, 10);
    estimator.addTotal(100);
    estimator.addTotal(200);
    long total = estimator.getWindowTotal();
    assertThat(total).isEqualTo(300);
  }

  @Test
  void shouldComputeQpsCorrectly() {
    GlobalQpsEstimator estimator = new GlobalQpsEstimator(1000, 10);
    // window = 1000ms / 10 = 100ms per slice, windowSize = 10
    // total window = 10 * 100ms = 1000ms = 1s
    estimator.addTotal(500);
    double qps = estimator.getQps();
    assertThat(qps).isEqualTo(500.0);
  }

  @Test
  void shouldHandleMultipleAddTotalCalls() {
    GlobalQpsEstimator estimator = new GlobalQpsEstimator(1000, 10);
    estimator.addTotal(50);
    estimator.addTotal(150);
    estimator.addTotal(300);
    assertThat(estimator.getWindowTotal()).isEqualTo(500);
  }
}
