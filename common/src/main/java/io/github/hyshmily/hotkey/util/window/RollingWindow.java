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

import io.github.hyshmily.hotkey.Internal;
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
@Internal
public final class RollingWindow {

  private final AtomicLongArray buckets;
  private final int windowSize;
  private final long bucketDurationMs;

  private static final class WindowField extends RwPadding.WindowRef {}

  private final WindowField windowField = new WindowField();

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
    windowField.windowStart = currentTimeMillis();
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
    buckets.addAndGet(windowField.currentBucket, value);
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
      windowField.windowStart = currentTimeMillis();
      windowField.currentBucket = 0;
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
    if (currentTimeMillis() - windowField.windowStart < bucketDurationMs) {
      return;
    }

    synchronized (tickLock) {
      long now = currentTimeMillis();
      long elapsed = now - windowField.windowStart;
      if (elapsed < bucketDurationMs) {
        return; // double-check: another thread already rotated
      }

      int steps = (int) Math.min(elapsed / bucketDurationMs, windowSize);
      for (int i = 0; i < steps; i++) {
        windowField.currentBucket = (windowField.currentBucket + 1) % windowSize;
        buckets.set(windowField.currentBucket, 0);
      }
      windowField.windowStart += steps * bucketDurationMs;
    }
  }
}

/** Cache-line padding namespace — adapted from Caffeine. */
final class RwPadding {

  private RwPadding() {}

  @SuppressWarnings("all")
  abstract static class PadWindow {

    byte p000, p001, p002, p003, p004, p005, p006, p007;
    byte p008, p009, p010, p011, p012, p013, p014, p015;
    byte p016, p017, p018, p019, p020, p021, p022, p023;
    byte p024, p025, p026, p027, p028, p029, p030, p031;
    byte p032, p033, p034, p035, p036, p037, p038, p039;
    byte p040, p041, p042, p043, p044, p045, p046, p047;
    byte p048, p049, p050, p051, p052, p053, p054, p055;
    byte p056, p057, p058, p059, p060, p061, p062, p063;
    byte p064, p065, p066, p067, p068, p069, p070, p071;
    byte p072, p073, p074, p075, p076, p077, p078, p079;
    byte p080, p081, p082, p083, p084, p085, p086, p087;
    byte p088, p089, p090, p091, p092, p093, p094, p095;
    byte p096, p097, p098, p099, p100, p101, p102, p103;
    byte p104, p105, p106, p107, p108, p109, p110, p111;
    byte p112, p113, p114, p115, p116, p117, p118, p119;
  }

  abstract static class WindowRef extends PadWindow {

    volatile long windowStart;
    volatile int currentBucket;
  }
}
