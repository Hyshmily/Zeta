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

  @Test
  void aNewWriterToAReusedSlotStartsAtZeroAndKeepsItsOwnCount() {
    // 1 ms slices: absolute slice 9 reuses the ring slot written by slice 1.
    SlidingWindowDetector detector = new SlidingWindowDetector(WindowGeometry.of(4, 4), 0);

    assertThat(detector.addCountAtSlice("k", 100, 1L)).isEqualTo(100L);
    assertThat(detector.addCountAtSlice("k", 1, 9L)).isEqualTo(1L);
  }

  /**
   * A writer that stalled across a full rotation must not replace the newer rotation's word. Its own
   * slice is already outside every reader's range, so dropping the add loses no visible traffic;
   * overwriting would erase the count the current window still sums.
   */
  @Test
  void aStaleAddDoesNotReplaceANewerRotation() {
    long[] slots = new long[2 * WINDOW];
    long stale = 1L;
    long newer = stale + 2L * WINDOW;
    int index = (int) (stale & MASK);
    slots[index] = SlidingWindowDetector.pack(newer, 7);

    SlidingWindowDetector.addToSlot(index, 100, slots, stale);

    assertThat(slots[index]).isEqualTo(SlidingWindowDetector.pack(newer, 7));
  }
}
