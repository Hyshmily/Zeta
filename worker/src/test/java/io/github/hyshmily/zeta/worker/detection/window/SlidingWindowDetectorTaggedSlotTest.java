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

import org.junit.jupiter.api.Test;

/**
 * Pins the packed-slot contract: a count carries the absolute slice that wrote it, so a slot left
 * by a writer that stalled across a rotation is ignored instead of being read as current traffic.
 */
class SlidingWindowDetectorTaggedSlotTest {

  private static final int WINDOW = 4;
  private static final int MASK = 2 * WINDOW - 1;

  @Test
  void sumRangeIgnoresASlotWhoseTagBelongsToAnotherRotation() {
    long[] slots = new long[2 * WINDOW];
    // A writer sampled slice 1, stalled, and landed after the row had rotated. Its count sits in
    // the slot that slice 9 reuses.
    slots[1 & MASK] = SlidingWindowDetector.pack(1L, 100);
    slots[6 & MASK] = SlidingWindowDetector.pack(6L, 1);

    long sum = SlidingWindowDetector.sumRange(slots, 9L, WINDOW + 1, MASK);

    assertThat(sum).as("the stale slice-1 count is not attributed to slice 9").isEqualTo(1L);
  }

  /** The forward walk must add both contiguous runs when the range crosses the ring's end. */
  @Test
  void sumRangeWalksBothRunsAcrossTheRingEnd() {
    long[] slots = new long[2 * WINDOW];
    long endSlice = 9L;
    for (long slice = endSlice - WINDOW; slice <= endSlice; slice++) {
      slots[(int) (slice & MASK)] = SlidingWindowDetector.pack(slice, (int) (slice - 4L));
    }

    assertThat(SlidingWindowDetector.sumRange(slots, endSlice, WINDOW + 1, MASK))
      .as("counts 1..5 from slices 5..9")
      .isEqualTo(15L);
  }

  @Test
  void aNewWriterToAReusedSlotStartsAtZeroAndKeepsItsOwnCount() {
    // 1 ms slices: absolute slice 9 reuses the ring slot written by slice 1.
    SlidingWindowDetector detector = new SlidingWindowDetector(WindowGeometry.of(4, 4), 0);

    assertThat(detector.addCountAtSlice("k", 100, 1L)).isEqualTo(100L);
    assertThat(detector.addCountAtSlice("k", 1, 9L)).isEqualTo(1L);
  }

  /**
   * The signed tag difference cannot tell a newer rotation from a tag written between {@code 2^31}
   * and {@code 2^32} slices ago. The window's 64-bit last write disambiguates: a real rotation is at
   * least {@code 2 * windowSize} ahead, so a last write still inside the summation range proves the
   * tag is stale and the slot must be replaced from zero instead of dropping the add.
   */
  @Test
  void aTagPastHalfTheTagCycleDoesNotDropTheWrite() {
    SlidingWindowDetector detector = new SlidingWindowDetector(WindowGeometry.of(4, 4), 0);
    long written = 1L;
    long later = written + (1L << 31) + 2L * WINDOW;

    assertThat(detector.addCountAtSlice("k", 100, written)).isEqualTo(100L);
    assertThat(detector.addCountAtSlice("k", 1, later))
      .as("the half-cycle-old tag is stale, not a newer rotation")
      .isEqualTo(1L);
  }

  /**
   * A writer that stalled across a full rotation must not replace the newer rotation's word. Its own
   * slice is already outside every reader's range, so dropping the add loses no visible traffic;
   * overwriting would erase the count the current window still sums.
   */
  @Test
  void aStaleAddDoesNotReplaceANewerRotation() {
    SlidingWindowDetector detector = new SlidingWindowDetector(WindowGeometry.of(4, 4), 0);
    long stale = 1L;
    long newer = stale + 2L * WINDOW;
    int index = (int) (stale & MASK);
    SlidingWindowDetector.KeyWindow window = new SlidingWindowDetector.KeyWindow(2 * WINDOW, newer, index, 7);

    detector.addToSlot(window, index, 100, stale);

    assertThat(window.slices[index]).isEqualTo(SlidingWindowDetector.pack(newer, 7));
  }
}
