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

import java.util.concurrent.atomic.AtomicLongArray;

/**
 * A fixed-size time-based sliding window backed by an {@link AtomicLongArray}
 * circular buffer.
 *
 * <p>The window is divided into {@code windowSize} equally-sized buckets spanning
 * {@code windowDurationMs} milliseconds. Each bucket represents a time slice and
 * holds a cumulative value. On every access ({@link #add(long)}, {@link #sum()},
 * etc.), expired buckets are detected and zeroed before the operation proceeds
 * via the internal {@link #tick()} method.
 *
 * <p>{@link AtomicLongArray} provides lock-free atomic access to individual
 * buckets, and {@code tick()} uses a private lock only when buckets actually
 * need rotation (once per bucket duration).  For the common case (no bucket
 * boundary crossed) there is zero lock contention.  This design targets the
 * HotKey reporter and SRE limiter use cases where call rates are in the
 * hundreds to low thousands per second.
 *
 * <p>Tick races are self-correcting: if two threads rotate simultaneously,
 * some buckets may be zeroed twice (harmless) or a value may land in a
 * bucket that is about to be zeroed (lost increment, acceptable for
 * rate-limiter approximations).  The next tick will converge.
 *
 * @see io.github.hyshmily.hotkey.util.ratelimit.SreRateLimiter
 */
public final class RollingWindow {

  private final AtomicLongArray buckets;
  private final int windowSize;
  private final long bucketDurationMs;

  private volatile long windowStart;
  private volatile int currentBucket;

  /** Private lock for tick() — only acquired when bucket rotation is actually needed. */
  private final Object tickLock = new Object();

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
    this.buckets = new AtomicLongArray(windowSize);
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
  public void add(long value) {
    tick();
    buckets.addAndGet(currentBucket, value);
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
  public long sum() {
    tick();
    long s = 0;
    for (int i = 0; i < windowSize; i++) {
      s += buckets.get(i);
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
  public long max() {
    tick();
    long m = 0;
    for (int i = 0; i < windowSize; i++) {
      long v = buckets.get(i);
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
  public long minNonZero() {
    tick();
    long m = Long.MAX_VALUE;
    for (int i = 0; i < windowSize; i++) {
      long v = buckets.get(i);
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
  public void reset() {
    synchronized (tickLock) {
      for (int i = 0; i < windowSize; i++) {
        buckets.set(i, 0);
      }
      windowStart = currentTimeMillis();
      currentBucket = 0;
    }
  }

  /**
   * Returns the number of buckets in the window.  Thread-safe.
   *
   * @return the number of buckets
   */
  public int size() {
    return windowSize;
  }

  /** Advance the window, zeroing buckets that have elapsed. */
  private void tick() {
    // Fast path (no lock) — 99.9%+ of calls hit this.
    if (currentTimeMillis() - windowStart < bucketDurationMs) {
      return;
    }

    synchronized (tickLock) {
      long now = currentTimeMillis();
      long elapsed = now - windowStart;
      if (elapsed < bucketDurationMs) {
        return; // double-check: another thread already rotated
      }

      int steps = (int) Math.min(elapsed / bucketDurationMs, windowSize);
      for (int i = 0; i < steps; i++) {
        currentBucket = (currentBucket + 1) % windowSize;
        buckets.set(currentBucket, 0);
      }
      windowStart += steps * bucketDurationMs;
    }
  }
}
