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
package io.github.hyshmily.hotkey.reporting;

import static io.github.hyshmily.hotkey.util.TimeSource.currentTimeMillis;

import io.github.hyshmily.hotkey.util.SystemLoadMonitor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * BBR (Bottleneck Bandwidth and Round-trip) adaptive rate limiter.
 *
 * <p>Inspired by the aegis /golang BBR implementation and Alibaba Sentinel.
 * Uses a sliding-window max-pass and min-RTT to compute the optimal concurrency
 * limit per Little's Law: {@code concurrency = throughput × latency}.
 *
 * <p>When the system CPU is below threshold the limiter is permissive,
 * only dropping when concurrency exceeds the limit <em>and</em> a cooldown
 * period is active.  When CPU is above threshold the limiter enforces the
 * concurrency budget strictly.
 *
 * <p><b>Application:</b> Used by {@link HotKeyReporter} to skip flush cycles
 * when the reporting pipeline is saturated, providing back-pressure that is
 * proportional to system load.
 */
public class BbrRateLimiter {

  private static final long DEFAULT_WINDOW_MS = 10_000;
  private static final int DEFAULT_BUCKETS = 100;
  private static final int DEFAULT_CPU_THRESHOLD = 800; // 0–1000 scale, 800 = 80 %
  private static final long DEFAULT_COOLDOWN_MS = 1_000;

  private final SystemLoadMonitor cpuMonitor;
  private final int cpuThreshold; // 0–1000
  private final long cooldownMs;

  private final long[] passBuckets;
  private final long[] rtBuckets;
  private final int[] rtCounts;
  private final int bucketCount;
  private final long bucketDurationMs;
  private final int bucketPerSecond;

  private final Object bucketLock = new Object();
  private int currentBucket;
  private long windowStart;

  private final AtomicLong inFlight = new AtomicLong(0);
  private final AtomicLong totalPassed = new AtomicLong(0);
  private final AtomicLong totalDropped = new AtomicLong(0);

  private final AtomicLong maxPassCache = new AtomicLong(1);
  private final AtomicLong minRtCache = new AtomicLong(1);
  private volatile long lastDropTime = 0;

  /** Minimum concurrency budget floor — prevents BBR from over-limiting in low-concurrency scenarios.
   *  Set dynamically to the number of active Worker nodes by {@code HotKeyReporter.flush()}. */
  private volatile int minInFlight = 1;

  /**
   * @deprecated Use {@link #BbrRateLimiter(SystemLoadMonitor, int, long, int, long)}
   *     for explicit configuration of threshold, window, buckets, and cooldown.
   */
  @Deprecated
  public BbrRateLimiter(SystemLoadMonitor cpuMonitor) {
    this(cpuMonitor, DEFAULT_CPU_THRESHOLD, DEFAULT_WINDOW_MS, DEFAULT_BUCKETS, DEFAULT_COOLDOWN_MS);
  }

  /**
   * Constructs a BBR rate limiter with explicit configuration.
   *
   * @param cpuMonitor  the system CPU load monitor used to derive the load signal
   * @param cpuThreshold CPU threshold on a 0-1000 scale; the limiter enforces strictly above this
   * @param windowMs    duration of the sliding window in milliseconds
   * @param bucketCount number of buckets within the sliding window
   * @param cooldownMs  duration of the cooldown period after a drop in milliseconds
   */
  public BbrRateLimiter(
    SystemLoadMonitor cpuMonitor,
    int cpuThreshold,
    long windowMs,
    int bucketCount,
    long cooldownMs
  ) {
    this.cpuMonitor = cpuMonitor;
    this.cpuThreshold = cpuThreshold;
    this.cooldownMs = cooldownMs;
    this.bucketCount = bucketCount;
    this.bucketDurationMs = windowMs / bucketCount;
    this.bucketPerSecond = (int) (1000L / this.bucketDurationMs);
    this.passBuckets = new long[bucketCount];
    this.rtBuckets = new long[bucketCount];
    this.rtCounts = new int[bucketCount];
    this.windowStart = System.currentTimeMillis();
  }

  /**
   * Check whether the current flush cycle is allowed.
   * <p>
   * If allowed, the caller <b>must</b> call {@link #onSuccess(long)} or
   *  afterward.  {@link #onEnqueue()} must be called after
   * a successful enqueue.
   *
   * @return {@code true} if the flush is allowed
   */
  public boolean tryAcquire() {
    synchronized (bucketLock) {
      tick();

      long currentInFlight = inFlight.get();
      long maxInFlight = maxInFlight();

      double cpuLoad = cpuMonitor.getCpuLoadEMA() * 1000.0; // convert 0-1 → 0-1000
      if (cpuLoad < cpuThreshold) {
        return currentInFlight <= maxInFlight || !isCooldown();
      } else {
        return currentInFlight <= maxInFlight;
      }
    }
  }

  /** Record one unit of in-flight work (batch enqueued). */
  public void onEnqueue() {
    inFlight.incrementAndGet();
  }

  /**
   * Record a successful publishing with its measured round-trip time.
   * <p>
   * The RTT is the wall-clock duration from batch assembly (flush timestamp)
   * to publish completion.  This drives the min-RT sliding window.
   *
   * @param rtMs round-trip time in milliseconds
   */
  public void onSuccess(long rtMs) {
    synchronized (bucketLock) {
      tick();
      passBuckets[currentBucket]++;
      rtBuckets[currentBucket] += rtMs;
      rtCounts[currentBucket]++;
    }
    inFlight.decrementAndGet();
    totalPassed.incrementAndGet();
  }

  /** Record a dropped flush from the consumer (stale/failed batch — was enqueued, so decrement inFlight). */
  public void onConsumerDrop() {
    lastDropTime = currentTimeMillis();
    totalDropped.incrementAndGet();
    inFlight.decrementAndGet();
  }

  /** Record a dropped flush from the gate (tryAcquire failed — never enqueued, inFlight unchanged).
   *  Does NOT trigger global cooldown — the next flush cycle is allowed to retry immediately.
   *  This prevents a transient drop from blocking all subsequent flushes for {@code cooldownMs}.
   *  The cooldown mechanism is preserved for {@link #onConsumerDrop()} (consumer-side drops). */
  public void onGateDrop() {
    totalDropped.incrementAndGet();
  }

  /** Total flush cycles that passed the limiter. */
  public long getTotalPassed() {
    return totalPassed.get();
  }

  /** Total flush cycles that were dropped by the limiter. */
  public long getTotalDropped() {
    return totalDropped.get();
  }

  /** Current number of in-flight (enqueued but not yet published) batches. */
  public long getInFlight() {
    return inFlight.get();
  }

  /** Dynamically adjust the min concurrency floor to match the number of active Worker nodes.
   *  Called automatically each flush cycle by {@code HotKeyReporter}. */
  public void setMinInFlight(int count) {
    this.minInFlight = Math.max(1, count);
  }

  /** Current computed max concurrency limit. */
  public long getCurrentMaxInFlight() {
    synchronized (bucketLock) {
      tick();
      return maxInFlight();
    }
  }

  /** Advance the sliding window forward, zeroing any buckets that have elapsed. */
  private void tick() {
    long now = currentTimeMillis();
    long elapsed = now - windowStart;
    if (elapsed < bucketDurationMs) {
      return;
    }
    int steps = (int) Math.min(elapsed / bucketDurationMs, bucketCount);
    for (int i = 0; i < steps; i++) {
      currentBucket = (currentBucket + 1) % bucketCount;
      passBuckets[currentBucket] = 0;
      rtBuckets[currentBucket] = 0;
      rtCounts[currentBucket] = 0;
    }
    windowStart += steps * bucketDurationMs;
  }

  /** Compute the concurrency budget: floor(maxPASS × minRT × bucketPerSecond / 1000 + 0.5). Caller must hold bucketLock. */
  private long maxInFlight() {
    long mp = maxPASS();
    long mr = minRT();

    if (mp == 0 || mr == 0) {
      return Long.MAX_VALUE;
    }
    return Math.max(minInFlight, (long) Math.floor(((double) mp * mr * bucketPerSecond) / 1000.0 + 0.5));
  }

  /** Peak pass rate per bucket in the sliding window. Caller must hold bucketLock. */
  private long maxPASS() {
    long max = 0;
    for (long v : passBuckets) {
      if (v > max) {
        max = v;
      }
    }
    if (max == 0) {
      return maxPassCache.get();
    }
    final long observed = max;
    maxPassCache.updateAndGet(c -> (c + observed) / 2);
    return max;
  }

  /** Minimum average response time per bucket in the sliding window. Caller must hold bucketLock. */
  private long minRT() {
    long min = Long.MAX_VALUE;
    for (int i = 0; i < bucketCount; i++) {
      if (rtCounts[i] > 0) {
        long avg = rtBuckets[i] / rtCounts[i];
        if (avg < min) {
          min = avg;
        }
      }
    }
    if (min == Long.MAX_VALUE) {
      return minRtCache.get();
    }
    final long observed = min;
    minRtCache.updateAndGet(c -> (c + observed) / 2);
    return min;
  }

  private boolean isCooldown() {
    return currentTimeMillis() - lastDropTime < cooldownMs;
  }
}
