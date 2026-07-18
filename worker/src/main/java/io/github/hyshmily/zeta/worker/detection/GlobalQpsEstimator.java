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

import java.util.concurrent.atomic.AtomicLong;

/**
 * A sliding‑window based estimator of the overall qps (queries per second)
 * across all keys in the current shard.
 *
 * <p>Uses the same circular‑buffer algorithm as {@link SlidingWindowDetector}
 * but aggregates all key counts into a single window rather than maintaining
 * per-key buffers.  This provides a lightweight, global throughput estimate
 * that drives dynamic threshold adaptation via {@link ThresholdLearner}.
 *
 * <p>The write method ({@link #addTotal}) is {@code synchronized} and safe
 * to call from multiple consumer threads concurrently.  The read-only methods
 * ({@link #getWindowTotal}, {@link #getQps}) may be called concurrently
 * without blocking; a stale-but-consistent snapshot is acceptable — the
 * {@code AtomicLong} array guarantees individual element visibility.
 *
 *
 * <p><b>Known limitation — per‑shard only:</b> This estimator only sees
 * traffic routed to <em>this</em> Worker shard by the consistent‑hash ring.
 * It does <b>not</b> reflect global cluster qps. When the cluster scales
 * (e.g. 5→3 Workers), per‑shard load redistributes non‑linearly, and the
 * derived threshold will shift accordingly. Not suitable for
 * cluster‑wide adaptive decisions without cross‑Worker coordination.
 *
 * @see ThresholdLearner
 * @see SlidingWindowDetector
 */
public class GlobalQpsEstimator {

  /** Number of time slices that form one complete sliding window. */
  private final int windowSize;

  /** Duration of a single time slice in milliseconds. */
  private final long timeMillisPerSlice;

  /** Doubled circular buffer of per-slice aggregate counters.
   *  Pre-initialised to zero-valued {@link AtomicLong} instances. */
  private final AtomicLong[] slices;

  /**
   * Creates a global qps estimator with a sliding window partitioned into the
   * given number of slices.
   *
   * <p>The window duration must be evenly divisible by the number of slices,
   * otherwise rounding inaccuracies will occur in window-boundary calculations.
   *
   * @param windowDurationMs total duration of the sliding window in milliseconds;
   *                         must be positive
   * @param slices           number of slices within the window; must be at least 1
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
   * the circular buffer consistent.  This method is {@code synchronized} to
   * protect the clear-before-add sequence against concurrent calls from
   * multiple {@link io.github.hyshmily.zeta.worker.ingest.ReportConsumer}
   * threads.
   *
   * @param totalCount the total number of access counts across all keys in
   *                   the batch; must be non-negative
   */
  public synchronized void addTotal(long totalCount) {
    long now = System.currentTimeMillis();
    int currentIndex = (int) ((now / timeMillisPerSlice) % slices.length);

    // Clear stale slices that are one full window behind
    int clearStart = (currentIndex + windowSize) % slices.length;
    for (int i = 0; i < windowSize; i++) {
      // Walk backwards to avoid clearing slices still within the current window.
      int idx = (clearStart - i + slices.length) % slices.length;
      slices[idx].set(0);
    }

    slices[currentIndex].addAndGet(totalCount);
  }

  /**
   * Returns the total access count in the current sliding window.
   *
   * <p>Walks backwards from the current time slice to sum the most recent
   * {@link #windowSize} slices.  This is a lock-free read and may return a
   * slightly stale value if called concurrently with {@link #addTotal}.
   *
   * @return sum of all slice counters within the active window; {@code 0}
   *         if no accesses have been recorded
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
   * <p>The estimate is computed as {@code getWindowTotal() / windowDurationSeconds},
   * where {@code windowDurationSeconds = (windowSize * timeMillisPerSlice) / 1000.0}.
   * Returns {@code 0.0} if no accesses have been recorded in the current window.
   *
   * <p>Consumed by {@link ThresholdLearner} to dynamically adjust the hot-key
   * threshold in response to overall traffic changes.
   *
   * @return the estimated qps value (may be {@code 0.0}; never negative)
   */
  public double getQps() {
    long total = getWindowTotal();
    double windowSeconds = (windowSize * timeMillisPerSlice) / 1000.0;
    return total / windowSeconds;
  }
}
