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
package io.github.hyshmily.zeta.reporting.impl;

import static io.github.hyshmily.zeta.util.TimeSource.currentTimeMillis;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.reporting.BbrRateLimiter;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.util.SystemLoadMonitor;
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
 * <p><b>Application:</b> Used by {@link KeyReporter} to skip flush cycles
 * when the reporting pipeline is saturated, providing back-pressure that is
 * proportional to system load.
 */
@Internal
public class BbrRateLimiterImpl implements BbrRateLimiter {

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

  private static final class InFlightField extends BbrPadding.InFlightRef {}

  private final InFlightField inFlightField = new InFlightField();

  private static final class MaxPassMinRtField extends BbrPadding.MaxPassMinRtRef {}

  private final MaxPassMinRtField maxPassMinRtField = new MaxPassMinRtField();

  private static final class DropTimeMinFlightField extends BbrPadding.DropTimeMinFlightRef {}

  private final DropTimeMinFlightField dropTimeMinFlightField = new DropTimeMinFlightField();

  private final AtomicLong totalPassed = new AtomicLong(0);
  private final AtomicLong totalDropped = new AtomicLong(0);

  /**
   * @deprecated Use {@link #BbrRateLimiterImpl(io.github.hyshmily.zeta.util.SystemLoadMonitor, int, long, int, long)}
   *     for explicit configuration of threshold, window, buckets, and cooldown.
   */
  @Deprecated
  @SuppressWarnings("DeprecatedIsStillUsed")
  public BbrRateLimiterImpl(SystemLoadMonitor cpuMonitor) {
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
  public BbrRateLimiterImpl(
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
    this.windowStart = currentTimeMillis();
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

      long currentInFlight = inFlightField.value.get();
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
    inFlightField.value.incrementAndGet();
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
    inFlightField.value.decrementAndGet();
    totalPassed.incrementAndGet();
  }

  /** Record a dropped flush from the consumer (stale/failed batch — was enqueued, so decrement inFlight). */
  public void onConsumerDrop() {
    dropTimeMinFlightField.lastDropTime = currentTimeMillis();
    totalDropped.incrementAndGet();
    inFlightField.value.decrementAndGet();
  }

  /** Record a dropped flush from the gate (tryAcquire failed — never enqueued, inFlight unchanged).
   *  Does NOT type global cooldown — the next flush cycle is allowed to retry immediately.
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
    return inFlightField.value.get();
  }

  /** Dynamically adjust the min concurrency floor to match the number of active Worker nodes.
   *  Called automatically each flush cycle by {@code HotKeyReporter}. */
  public void setMinInFlight(int count) {
    dropTimeMinFlightField.minInFlight = Math.max(1, count);
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
    return Math.max(
      dropTimeMinFlightField.minInFlight,
      (long) Math.floor(((double) mp * mr * bucketPerSecond) / 1000.0 + 0.5)
    );
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
      return maxPassMinRtField.maxPassCache.get();
    }
    final long observed = max;
    maxPassMinRtField.maxPassCache.updateAndGet(c -> (c + observed) / 2);
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
      return maxPassMinRtField.minRtCache.get();
    }
    final long observed = min;
    maxPassMinRtField.minRtCache.updateAndGet(c -> (c + observed) / 2);
    return min;
  }

  private boolean isCooldown() {
    return currentTimeMillis() - dropTimeMinFlightField.lastDropTime < cooldownMs;
  }
}

/** Cache-line padding namespace — adapted from Caffeine. */
final class BbrPadding {

  private BbrPadding() {}

  @SuppressWarnings("all")
  abstract static class PadInFlight {

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

  abstract static class InFlightRef extends PadInFlight {

    /** In-flight counter, isolated on its own cache line. */
    final AtomicLong value = new AtomicLong(0);
  }

  @SuppressWarnings("all")
  abstract static class PadMaxPassMinRt {

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

  abstract static class MaxPassMinRtRef extends PadMaxPassMinRt {

    final AtomicLong maxPassCache = new AtomicLong(1);
    final AtomicLong minRtCache = new AtomicLong(1);
  }

  @SuppressWarnings("all")
  abstract static class PadDropTimeMinFlight {

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

  abstract static class DropTimeMinFlightRef extends PadDropTimeMinFlight {

    volatile long lastDropTime = 0;
    volatile int minInFlight = 1;
  }
}
