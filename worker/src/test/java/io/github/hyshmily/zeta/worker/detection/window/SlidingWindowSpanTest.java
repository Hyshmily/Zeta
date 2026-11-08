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
package io.github.hyshmily.zeta.worker.detection.window;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.util.TimeSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pins the {@code windowSize + 1} summation span: exactly one window of silence still reads the
 * last write, and one more slice does not.
 *
 * <p>Time is driven through {@link TimeSource}'s offset hook. The helper advances to an absolute
 * <em>zero-based</em> instant immediately before each read, so class-loading or GC between two
 * statements cannot push the sample into the next slice.
 */
class SlidingWindowSpanTest {

  /** 1000ms / 16 slices = 62ms per slice, effective window 992ms. */
  private static final long SLICE_MS = 62L;

  private static final long WINDOW_MS = 16 * SLICE_MS;

  /** Offset currently applied to the monotonic clock; {@code now == raw + offset}. */
  private long offset;

  @BeforeEach
  void resetClock() {
    offset = 0L;
    TimeSource.setTimeOffsetForTest(0, 0);
  }

  @AfterEach
  void clearClock() {
    TimeSource.setTimeOffsetForTest(0, 0);
  }

  /** Sets the monotonic clock so the next read is exactly {@code target} (zero-based). */
  private void advanceTo(long target) {
    long now = TimeSource.monotonicMillis();
    offset += target - now;
    TimeSource.setTimeOffsetForTest(0, offset);
  }

  /** Starts the zero-based frame at {@code 0} and returns a detector built on it. */
  private SlidingWindowDetector detectorAtZero() {
    long base = TimeSource.monotonicMillis();
    offset = -base;
    TimeSource.setTimeOffsetForTest(0, offset);
    return new SlidingWindowDetector(WindowGeometry.of(1_000, 16), 0);
  }

  private static long addCount(SlidingWindowDetector detector, String key, long count) {
    return detector.addCountAtSlice(key, count, detector.sliceIndex(TimeSource.monotonicMillis()));
  }

  @Test
  void readOnlySumKeepsTheLastWriteForExactlyOneWindow() {
    SlidingWindowDetector detector = detectorAtZero();
    assertThat(addCount(detector, "k", 5)).isEqualTo(5);

    advanceTo(WINDOW_MS);
    assertThat(detector.windowSum("k")).as("the guard slice still covers the last write").isEqualTo(5);

    advanceTo(WINDOW_MS + SLICE_MS);
    assertThat(detector.windowSum("k")).as("one slice past the span reads zero").isZero();
  }

  @Test
  void readOnlySumAtTheEndOfTheSecondWindowDoesNotReadTheWrappedSlot() {
    SlidingWindowDetector detector = detectorAtZero();
    assertThat(addCount(detector, "k", 5)).isEqualTo(5);

    advanceTo(WINDOW_MS);
    assertThat(addCount(detector, "k", 1)).as("the guard slice still covers the first write").isEqualTo(6);

    advanceTo(2 * WINDOW_MS);
    assertThat(detector.windowSum("k"))
      .as("only the write from one window ago remains; the slot from two windows ago is out of range")
      .isEqualTo(1);
  }

  @Test
  void addAtExactlyOneWindowOfSilenceKeepsTheGuardSlice() {
    SlidingWindowDetector detector = detectorAtZero();
    addCount(detector, "k", 5);

    advanceTo(WINDOW_MS);
    assertThat(addCount(detector, "k", 1)).as("the write from exactly one window ago is still in range").isEqualTo(6);

    advanceTo(WINDOW_MS + SLICE_MS);
    // The 5 from slice 0 left the range; both subsequent 1s are inside it.
    assertThat(addCount(detector, "k", 1)).as("the stale slice is cleared, the live ones remain").isEqualTo(2);
  }

  @Test
  void sumIsStableAcrossEveryPhaseOfTheSlice() {
    SlidingWindowDetector detector = detectorAtZero();
    for (int i = 0; i < 100; i++) {
      addCount(detector, "phased", 1);
    }

    // Every phase inside the final slice reports the same 100: the summation range is
    // slice-aligned, so `now % sliceMs` no longer decides whether a threshold is met.
    for (long phase = 0; phase < SLICE_MS; phase++) {
      advanceTo(WINDOW_MS + phase);
      assertThat(detector.windowSum("phased")).as("phase %d", phase).isEqualTo(100);
    }
  }
}
