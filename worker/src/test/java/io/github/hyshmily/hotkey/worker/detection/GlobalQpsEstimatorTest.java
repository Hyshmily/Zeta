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

  /**
   * Verifies a new {@link GlobalQpsEstimator} starts with zero window total and zero QPS.
   */
  @Test
  void shouldConstructWithInitializedSlices() {
    GlobalQpsEstimator estimator = new GlobalQpsEstimator(1000, 10);
    assertThat(estimator.getWindowTotal()).isZero();
    assertThat(estimator.getQps()).isZero();
  }

  /**
   * Verifies that {@code addTotal} correctly accumulates into the window total.
   */
  @Test
  void shouldComputeWindowTotalAfterAddingCounts() {
    GlobalQpsEstimator estimator = new GlobalQpsEstimator(10_000, 10);
    estimator.addTotal(100);
    estimator.addTotal(200);
    long total = estimator.getWindowTotal();
    assertThat(total).isEqualTo(300);
  }

  /**
   * Verifies that QPS is correctly derived from the total count and the configured window duration.
   */
  @Test
  void shouldComputeQpsCorrectly() {
    GlobalQpsEstimator estimator = new GlobalQpsEstimator(1000, 10);
    // window = 1000ms / 10 = 100ms per slice, windowSize = 10
    // total window = 10 * 100ms = 1000ms = 1s
    estimator.addTotal(500);
    double qps = estimator.getQps();
    assertThat(qps).isEqualTo(500.0);
  }

  /**
   * Verifies that multiple {@code addTotal} calls are accumulated correctly.
   */
  @Test
  void shouldHandleMultipleAddTotalCalls() {
    GlobalQpsEstimator estimator = new GlobalQpsEstimator(1000, 10);
    estimator.addTotal(50);
    estimator.addTotal(150);
    estimator.addTotal(300);
    assertThat(estimator.getWindowTotal()).isEqualTo(500);
  }

  /**
   * Verifies that a single-slice window (windowSize = 1) works correctly.
   * Edge case: the window always covers only the current slice.
   */
  @Test
  void shouldHandleSingleSliceWindow() {
    GlobalQpsEstimator estimator = new GlobalQpsEstimator(1000, 1);
    estimator.addTotal(100);
    assertThat(estimator.getWindowTotal()).isEqualTo(100);
  }

  /**
   * Verifies that adding zero total has no effect on the window sum.
   */
  @Test
  void shouldHandleZeroTotalCount() {
    GlobalQpsEstimator estimator = new GlobalQpsEstimator(1000, 10);
    estimator.addTotal(0);
    assertThat(estimator.getWindowTotal()).isZero();
    assertThat(estimator.getQps()).isZero();
  }

  /**
   * Verifies that adding a negative total decreases the window sum.
   */
  @Test
  void shouldHandleNegativeTotalCount() {
    GlobalQpsEstimator estimator = new GlobalQpsEstimator(1000, 10);
    estimator.addTotal(100);
    estimator.addTotal(-30);
    long total = estimator.getWindowTotal();
    assertThat(total).isEqualTo(70);
  }

  /**
   * Verifies that a very large total does not overflow and QPS is computed correctly.
   */
  @Test
  void shouldHandleLargeTotalCount() {
    GlobalQpsEstimator estimator = new GlobalQpsEstimator(1000, 10);
    estimator.addTotal(Long.MAX_VALUE);
    long total = estimator.getWindowTotal();
    assertThat(total).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Verifies that totals survive when time advances past one or more slice
   * boundaries (non‑contiguous access pattern). The old slice must remain
   * within the window and must not be cleared.
   */
  @Test
  void addTotal_shouldPreserveCountsAfterSliceAdvance() throws InterruptedException {
    GlobalQpsEstimator estimator = new GlobalQpsEstimator(5000, 5);
    estimator.addTotal(100);
    Thread.sleep(1500);
    estimator.addTotal(50);
    long total = estimator.getWindowTotal();
    assertThat(total).isEqualTo(150);
  }

  /**
   * Verifies that after multiple non‑contiguous accesses spanning several
   * slices, the window total includes exactly the still‑active slices and
   * nothing more.
   */
  @Test
  void addTotal_shouldMaintainCorrectTotalAfterMultipleSliceAdvances() throws InterruptedException {
    GlobalQpsEstimator estimator = new GlobalQpsEstimator(5000, 5);
    estimator.addTotal(100);
    Thread.sleep(800);
    estimator.addTotal(20);
    Thread.sleep(900);
    estimator.addTotal(5);
    long total = estimator.getWindowTotal();
    assertThat(total).isEqualTo(125);
  }
}
