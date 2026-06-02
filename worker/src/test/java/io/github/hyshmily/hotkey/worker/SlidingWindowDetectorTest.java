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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SlidingWindowDetector}.
 */
class SlidingWindowDetectorTest {

  @Test
  void shouldConstructWithValidParameters() {
    SlidingWindowDetector detector = new SlidingWindowDetector(1000, 10, 500);
    assertThat(detector.getWindowSize()).isEqualTo(10);
    assertThat(detector.getTimeMillisPerSlice()).isEqualTo(100);
    assertThat(detector.getThreshold()).isEqualTo(500);
  }

  @Test
  void shouldReturnTrueWhenWindowSumExceedsThreshold() {
    SlidingWindowDetector detector = new SlidingWindowDetector(1000, 10, 3);
    assertThat(detector.addCount("key1", 5)).isTrue();
  }

  @Test
  void shouldReturnFalseWhenWindowSumBelowThreshold() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 100);
    assertThat(detector.addCount("key2", 1)).isFalse();
  }

  @Test
  void shouldReturnWindowSumForTrackedKey() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 1000);
    detector.addCount("key3", 42);
    assertThat(detector.getWindowSum("key3")).isPositive();
  }

  @Test
  void shouldReturnZeroForUnknownKey() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 1000);
    assertThat(detector.getWindowSum("unknown")).isZero();
  }

  @Test
  void shouldEvictStaleKeys() throws InterruptedException {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 1000);
    detector.addCount("staleKey", 1);
    assertThat(detector.getActiveKeyCount()).isEqualTo(1);
    Thread.sleep(10);
    detector.evictStale(1);
    assertThat(detector.getActiveKeyCount()).isZero();
  }

  @Test
  void shouldReportActiveKeyCount() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 1000);
    assertThat(detector.getActiveKeyCount()).isZero();
    detector.addCount("a", 1);
    detector.addCount("b", 1);
    detector.addCount("c", 1);
    assertThat(detector.getActiveKeyCount()).isEqualTo(3);
  }
}
