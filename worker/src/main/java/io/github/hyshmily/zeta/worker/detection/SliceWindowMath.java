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

import java.util.function.IntConsumer;
import org.springframework.util.Assert;

/**
 * Shared arithmetic for the doubled circular slice windows used by
 * {@link SlidingWindowDetector} and {@link GlobalQpsEstimator}: the
 * power-of-two slice alignment with its division-by-zero guard, and the
 * W-1 pre-clear invariant that keeps a sliding summation range free of
 * pre-gap garbage. Both owners carry storage-specific slot arrays
 * ({@code AtomicLongArray} vs padded volatile holders), so only the
 * arithmetic is shared — the delicate invariant gets exactly one home.
 */
public final class SliceWindowMath {

  private SliceWindowMath() {}

  /**
   * Aligns {@code slices} up to the next power of two and guards the
   * per-slice duration against zero. The pre-alignment guard
   * ({@code windowDurationMs >= slices}) is NOT sufficient: aligning slices
   * UP to the next power of two can push {@code windowDurationMs / aligned}
   * to 0 (e.g. durationMs=15, slices=10 → aligned=16 → 0), which would throw
   * ArithmeticException on every add and window read.
   *
   * @param windowDurationMs total duration of the sliding window in milliseconds
   * @param slices           requested number of slices
   * @return the aligned power-of-two slice count
   * @throws IllegalArgumentException if {@code slices} is non-positive or the
   *                                  aligned slice count would make the per-slice
   *                                  duration zero
   */
  public static int alignedSlices(long windowDurationMs, int slices) {
    Assert.isTrue(slices > 0, "slices must be positive, got " + slices);

    int aligned = slices;
    if ((aligned & (aligned - 1)) != 0) {
      aligned = Integer.highestOneBit(aligned - 1) << 1;
    }
    Assert.isTrue(
      windowDurationMs >= aligned,
      "windowDurationMs (" +
        windowDurationMs +
        ") must be >= aligned slices (" +
        aligned +
        ") to avoid division by zero"
    );
    return aligned;
  }

  /**
   * Zeros the slots of a doubled circular buffer that the new summation range
   * {@code [currentIndex - windowSize + 1, currentIndex]} is about to read
   * but that still carry data from before the access gap.
   *
   * <p>The <b>W-1 pre-clear invariant</b> (the one home for the arithmetic
   * both window owners share): the clear start is shifted one slot earlier
   * than the natural stale boundary
   * {@code (currentIndex - elapsedSlices - windowSize + 1) mod length}, so
   * each crossing clears {@code elapsedSlices - 1} truly stale slots plus the
   * one "guard" slot between the previous clear region and the new range —
   * the slot that left the summation range one crossing ago is cleared by the
   * next crossing. A gap of {@code windowSize} or more slices makes every
   * previously written slot stale, expressed here as zeroing the entire
   * buffer.
   *
   * <p>Clearing is skipped entirely when {@code prevTs <= 0} (no previous
   * touch recorded) — the buffer was created zeroed.
   *
   * @param windowSize         number of slices in one summation window (power of two)
   * @param lengthMask         bitmask for the doubled buffer index ({@code 2 * windowSize - 1})
   * @param timeMillisPerSlice duration of one slice in milliseconds
   * @param prevTs             monotonic timestamp of the previous touch (write)
   * @param now                current monotonic millis
   * @param currentIndex       current slice index in the doubled buffer
   * @param zeroSlot           zeroes one buffer slot (storage-specific)
   */
  static void clearStaleSlots(
    int windowSize,
    int lengthMask,
    long timeMillisPerSlice,
    long prevTs,
    long now,
    int currentIndex,
    IntConsumer zeroSlot
  ) {
    if (prevTs <= 0) {
      return;
    }

    long elapsedSlices = (now - prevTs) / timeMillisPerSlice;
    if (elapsedSlices >= windowSize) {
      // Infrequent-call gap: all previously written data is stale — reset
      // the entire buffer.
      for (int i = 0; i <= lengthMask; i++) {
        zeroSlot.accept(i);
      }
    } else if (elapsedSlices > 0) {
      int clearStart = (currentIndex + windowSize - (int) elapsedSlices) & lengthMask;
      for (int i = 0; i < elapsedSlices; i++) {
        zeroSlot.accept((clearStart + i) & lengthMask);
      }
    }
  }
}
