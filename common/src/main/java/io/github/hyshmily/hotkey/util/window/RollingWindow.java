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

import static io.github.hyshmily.hotkey.util.TimeSource.currentTimeMillis;

/**
 * A fixed-size time-based sliding window backed by a circular {@code long[]}.
 *
 * <p>The window is divided into {@code windowSize} equally-sized buckets spanning
 * {@code windowDurationMs} milliseconds. Each bucket represents a time slice and
 * holds a cumulative value. On every access ({@link #add(long)}, {@link #sum()},
 * etc.), expired buckets are detected and zeroed before the operation proceeds
 * via the internal {@link #tick()} method.
 *
 * <p>This design provides O(1) amortized writes and O(windowSize) reads, with
 * automatic time-based bucket rotation. No background threads are needed.
 *
 * <p>All public methods are {@code synchronized}, making the window safe for
 * concurrent producers and readers at moderate contention. For very high
 * contention scenarios, consider striped or lock-free alternatives (the current
 * design targets the HotKey reporter and SRE limiter use cases where call rates
 * are in the hundreds to low thousands per second).
 *
 * @see io.github.hyshmily.hotkey.util.ratelimit.SreRateLimiter
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
   * <p>Each bucket covers {@code windowDurationMs / windowSize} milliseconds.
   * The window clock starts at construction time. All buckets are initially zero.
   *
   * @param windowSize       number of buckets that form one full window; must be positive
   * @param windowDurationMs total duration of the sliding window in milliseconds; must be
   *                         positive and evenly divisible by {@code windowSize} for
   *                         precise bucket boundaries
   */
  public RollingWindow(int windowSize, long windowDurationMs) {
    this.windowSize = windowSize;
    this.bucketDurationMs = windowDurationMs / windowSize;
    this.buckets = new long[windowSize];
    this.windowStart = currentTimeMillis();
  }

  /**
   * Add {@code value} to the current (most recent) bucket.
   *
   * <p>Bucket drift is corrected via {@link #tick()} before the addition,
   * ensuring that the value lands in the correct time-aligned bucket even
   * if no other operation has occurred for an extended period.
   *
   * @param value the value to add to the current bucket (may be negative, though
   *              typical usage patterns use non-negative counts)
   */
  public synchronized void add(long value) {
    tick();
    buckets[currentBucket] += value;
  }

  /**
   * Sum of all buckets in the window.
   *
   * <p>Expired buckets are zeroed via {@link #tick()} before summing, so the
   * returned value reflects only the data from the current window. This is an
   * O(windowSize) operation.
   *
   * @return the sum across all buckets (may be 0 if all buckets are zero)
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
   * Maximum value across all buckets in the window.
   *
   * <p>Expired buckets are zeroed via {@link #tick()} before computing.
   * O(windowSize) operation.
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
   * Minimum non-zero value across all buckets in the window.
   *
   * <p>Expired buckets are zeroed via {@link #tick()} before computing.
   * Useful for detecting the minimum "background" rate when most buckets
   * have positive values. O(windowSize) operation.
   *
   * @return the minimum positive value across all buckets, or {@link Long#MAX_VALUE}
   *         if every bucket is zero
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

  /**
   * Zero every bucket and reset the window clock to the current time.
   *
   * <p>After calling this method, the window behaves as if newly constructed:
   * all buckets are zero and the time origin is reset. This is useful when
   * the monitored metric resets (e.g. after a configuration change, a new
   * sampling period, or a rate-limit cooldown).
   *
   * <p>O(windowSize) operation.
   */
  public synchronized void reset() {
    java.util.Arrays.fill(buckets, 0);
    windowStart = currentTimeMillis();
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
    long now = currentTimeMillis();
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
