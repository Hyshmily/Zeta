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

import java.util.concurrent.atomic.AtomicLong;

/**
 * A sliding‑window based estimator of the overall QPS (queries per second)
 * across all keys in the current shard.
 *
 * <p>Uses the same circular‑buffer algorithm as {@link SlidingWindowDetector}
 * but aggregates all key counts into a single window.
 */
public class GlobalQpsEstimator {

  /** Number of time slices that form one complete sliding window. */
  private final int windowSize;
  /** Duration of a single time slice in milliseconds. */
  private final long timeMillisPerSlice;
  /** Doubled circular buffer of per-slice aggregate counters. */
  private final AtomicLong[] slices;

  /**
   * Creates a global QPS estimator with a sliding window partitioned into the given number of
   * slices.
   *
   * @param windowDurationMs total duration of the sliding window in milliseconds
   * @param slices           number of slices within the window
   */
  public GlobalQpsEstimator(long windowDurationMs, int slices) {
    this.windowSize = slices;
    this.timeMillisPerSlice = windowDurationMs / slices;
    this.slices = new AtomicLong[slices * 2];
    for (int i = 0; i < this.slices.length; i++) {
      this.slices[i] = new AtomicLong(0);
    }
  }

  /**
   * Adds the sum of all per-key counts in a batch to the current time slice.
   *
   * <p>Also clears stale slices that have fallen out of the window, keeping
   * the circular buffer consistent.  Must be called from a single consumer
   * thread (or externally synchronised) to avoid races on the clear + addDirect
   * sequence.
   *
   * @param totalCount the total number of access counts across all keys in the batch
   */
  public void addTotal(long totalCount) {
    long now = System.currentTimeMillis();
    int currentIndex = (int) ((now / timeMillisPerSlice) % slices.length);

    // Clear stale slices that are one full window behind
    int clearStart = (currentIndex + windowSize) % slices.length;
    for (int i = 0; i < windowSize; i++) {
      int idx = (clearStart + i) % slices.length;
      slices[idx].set(0);
    }

    slices[currentIndex].addAndGet(totalCount);
  }

  /**
   * Returns the total count in the current sliding window.
   *
   * @return sum of all slice counters within the active window
   */
  public long getWindowTotal() {
    long now = System.currentTimeMillis();
    int currentIndex = (int) ((now / timeMillisPerSlice) % slices.length);
    long sum = 0;
    for (int i = 0; i < windowSize; i++) {
      int idx = (currentIndex - i + slices.length) % slices.length;
      sum += slices[idx].get();
    }
    return sum;
  }

  /**
   * Returns the estimated queries per second based on the current window.
   *
   * <p>The estimate is computed as {@code getWindowTotal() / windowDurationSeconds}.
   * Returns {@code 0.0} if no accesses have been recorded in the current window.
   *
   * @return the estimated QPS value (may be {@code 0.0})
   */
  public double getQps() {
    long total = getWindowTotal();
    double windowSeconds = (windowSize * timeMillisPerSlice) / 1000.0;
    return total / windowSeconds;
  }
}
