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
package io.github.hyshmily.hotkey.util.window;

/**
 * A fixed-size time-based sliding window backed by a circular {@code long[]}.
 *
 * <p>Buckets rotate automatically on every write — expired buckets are zeroed
 * before reuse.  All operations are synchronized so the window is safe for
 * concurrent producers and readers at moderate contention.
 */
public final class RollingWindow {

  private final long[] buckets;
  private final int windowSize;
  private final long bucketDurationMs;

  private long windowStart;
  private int currentBucket;

  /**
   * Creates a sliding window with the given number of buckets spanning the given duration.
   *
   * @param windowSize       number of buckets that form one full window
   * @param windowDurationMs total duration of the sliding window in milliseconds
   */
  public RollingWindow(int windowSize, long windowDurationMs) {
    this.windowSize = windowSize;
    this.bucketDurationMs = windowDurationMs / windowSize;
    this.buckets = new long[windowSize];
    this.windowStart = System.currentTimeMillis();
  }

  /**
   * Add {@code value} to the current bucket.  Thread-safe.
   *
   * @param value the value to add to the current bucket
   */
  public synchronized void add(long value) {
    tick();
    buckets[currentBucket] += value;
  }

  /**
   * Sum of all buckets.  Thread-safe.
   *
   * @return the sum across all buckets
   */
  public synchronized long sum() {
    tick();
    long s = 0;
    for (long v : buckets) {
      s += v;
    }
    return s;
  }

  /**
   * Maximum value across all buckets.
   *
   * @return the maximum value across all buckets, or 0 if all buckets are zero
   */
  public synchronized long max() {
    tick();
    long m = 0;
    for (long v : buckets) {
      if (v > m) {
        m = v;
      }
    }
    return m;
  }

  /**
   * Minimum non-zero value across all buckets.
   * Returns {@link Long#MAX_VALUE} if every bucket is zero.
   */
  public synchronized long minNonZero() {
    tick();
    long m = Long.MAX_VALUE;
    for (long v : buckets) {
      if (v > 0 && v < m) {
        m = v;
      }
    }
    return m;
  }

  /** Zero every bucket and reset the window clock. */
  public synchronized void reset() {
    java.util.Arrays.fill(buckets, 0);
    windowStart = System.currentTimeMillis();
    currentBucket = 0;
  }

  /**
   * Returns the number of buckets in the window.  Thread-safe.
   *
   * @return the number of buckets
   */
  public synchronized int size() {
    return windowSize;
  }

  /** Advance the window, zeroing buckets that have elapsed. */
  private void tick() {
    long now = System.currentTimeMillis();
    long elapsed = now - windowStart;
    if (elapsed < bucketDurationMs) {
      return;
    }

    int steps = (int) Math.min(elapsed / bucketDurationMs, windowSize);
    for (int i = 0; i < steps; i++) {
      currentBucket = (currentBucket + 1) % windowSize;
      buckets[currentBucket] = 0;
    }
    windowStart += steps * bucketDurationMs;
  }
}
