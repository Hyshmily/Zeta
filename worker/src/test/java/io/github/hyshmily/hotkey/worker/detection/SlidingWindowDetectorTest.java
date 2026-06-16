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
 * Tests for {@link SlidingWindowDetector}.
 */
class SlidingWindowDetectorTest {

  /**
   * Verifies that the {@link SlidingWindowDetector} is constructed with the expected parameter values.
   */
  @Test
  void shouldConstructWithValidParameters() {
    SlidingWindowDetector detector = new SlidingWindowDetector(1000, 10, 500);
    assertThat(detector.getWindowSize()).isEqualTo(10);
    assertThat(detector.getTimeMillisPerSlice()).isEqualTo(100);
    assertThat(detector.getThreshold()).isEqualTo(500);
  }

  /**
   * Verifies that {@code addCount} returns {@code true} when the window sum exceeds the threshold.
   */
  @Test
  void shouldReturnTrueWhenWindowSumExceedsThreshold() {
    SlidingWindowDetector detector = new SlidingWindowDetector(1000, 10, 3);
    assertThat(detector.addCount("key1", 5)).isTrue();
  }

  /**
   * Verifies that {@code addCount} returns {@code false} when the window sum is below the threshold.
   */
  @Test
  void shouldReturnFalseWhenWindowSumBelowThreshold() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 100);
    assertThat(detector.addCount("key2", 1)).isFalse();
  }

  /**
   * Verifies that {@code getWindowSum} returns a positive value for a key that has been tracked.
   */
  @Test
  void shouldReturnWindowSumForTrackedKey() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 1000);
    detector.addCount("key3", 42);
    assertThat(detector.getWindowSum("key3")).isPositive();
  }

  /**
   * Verifies that {@code getWindowSum} returns zero for a key that has never been tracked.
   */
  @Test
  void shouldReturnZeroForUnknownKey() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 1000);
    assertThat(detector.getWindowSum("unknown")).isZero();
  }

  /**
   * Verifies that stale keys are evicted after calling {@code evictStale}.
   */
  @Test
  void shouldEvictStaleKeys() throws InterruptedException {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 1000);
    detector.addCount("staleKey", 1);
    assertThat(detector.getActiveKeyCount()).isEqualTo(1);
    Thread.sleep(10);
    detector.evictStale(1);
    assertThat(detector.getActiveKeyCount()).isZero();
  }

  /**
   * Verifies that the active key count changes as keys are added to the detector.
   */
  @Test
  void shouldReportActiveKeyCount() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 1000);
    assertThat(detector.getActiveKeyCount()).isZero();
    detector.addCount("a", 1);
    detector.addCount("b", 1);
    detector.addCount("c", 1);
    assertThat(detector.getActiveKeyCount()).isEqualTo(3);
  }

  /**
   * Verifies that addCount returns true when the window sum exactly equals the threshold.
   * Boundary: {@code >= threshold} includes equality.
   */
  @Test
  void shouldReturnTrueWhenWindowSumEqualsThreshold() {
    SlidingWindowDetector detector = new SlidingWindowDetector(1000, 10, 5);
    assertThat(detector.addCount("key", 5)).isTrue();
  }

  /**
   * Verifies that adding zero count does not change the hot verdict for a tracked key,
   * and the window sum remains unchanged.
   */
  @Test
  void shouldHandleZeroCount() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 100);
    detector.addCount("key", 50);
    long sumBefore = detector.getWindowSum("key");
    assertThat(detector.addCount("key", 0)).isFalse();
    assertThat(detector.getWindowSum("key")).isEqualTo(sumBefore);
  }

  /**
   * Verifies that adding a negative count does not cause failures and decreases the window sum.
   */
  @Test
  void shouldHandleNegativeCount() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 100);
    detector.addCount("key", 50);
    assertThat(detector.addCount("key", -20)).isFalse();
    assertThat(detector.getWindowSum("key")).isEqualTo(30);
  }

  /**
   * Verifies that {@code getWindowSum} returns zero for a key that has been evicted.
   */
  @Test
  void shouldReturnZeroForEvictedKey() throws InterruptedException {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 100);
    detector.addCount("ephemeral", 42);
    assertThat(detector.getWindowSum("ephemeral")).isPositive();
    Thread.sleep(10);
    detector.evictStale(1);
    assertThat(detector.getWindowSum("ephemeral")).isZero();
  }

  /**
   * Verifies that getWindowSum correctly aggregates across multiple time slices when
   * time advances enough to rotate into a new slice.
   */
  @Test
  void shouldAggregateAcrossMultipleSlices() throws InterruptedException {
    // 10 second window, 1000ms per slice — long slices so rotation doesn't lose data
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 100);
    detector.addCount("key", 30);
    Thread.sleep(150); // advance past at least one slice boundary
    detector.addCount("key", 20);
    // the sum should include both additions (within the window)
    assertThat(detector.getWindowSum("key")).isEqualTo(50);
  }

  /**
   * Verifies that evictStale with staleAfterMs = 0 eventually evicts all keys
   * that were accessed in the past (boundary: zero stale timeout).
   */
  @Test
  void shouldEvictAllWithZeroStaleTimeout() throws InterruptedException {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 1000);
    detector.addCount("key", 1);
    Thread.sleep(1);
    detector.evictStale(0);
    assertThat(detector.getActiveKeyCount()).isZero();
  }

  /**
   * Verifies that getWindowSum handles Long.MAX_VALUE without overflow.
   */
  @Test
  void shouldHandleMaxLongCount() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, Long.MAX_VALUE);
    boolean firstResult = detector.addCount("key", Long.MAX_VALUE);
    assertThat(firstResult).isTrue(); // Long.MAX_VALUE >= Long.MAX_VALUE (threshold)
    assertThat(detector.getWindowSum("key")).isPositive();
    // adding zero should not change sum
    detector.addCount("key", 0);
    assertThat(detector.getWindowSum("key")).isEqualTo(Long.MAX_VALUE);
  }
}
