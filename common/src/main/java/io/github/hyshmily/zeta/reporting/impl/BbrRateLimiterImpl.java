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
import org.springframework.util.Assert;

/**
 * Damped adaptive rate limiter (BBR-flavored), modeled on the Linux writeback
 * throttle in {@code mm/page-writeback.c}.
 *
 * <p>The limiter regulates the number of in-flight batches with three layers,
 * mirroring the kernel's dirty-page throttling structure:
 *
 * <ol>
 *   <li><b>Freerun band</b> (kernel {@code dirty_freerun_ceiling}): at or below
 *       half the damped baseline the control loop does not restrict at all,
 *       eliminating measurement noise in the common case. A recent consumer
 *       drop (cooldown) bypasses the band and enforces the budget strictly.
 *   <li><b>Position control</b> (kernel {@code pos_ratio_polynom}): the
 *       admission budget is the damped baseline scaled by a cubic position
 *       ratio around the setpoint — fast response far from the setpoint, flat
 *       slope near it. CPU pressure no longer acts as a two-state hard switch:
 *       it continuously derates the hard limit over a ±20 pp ramp around the
 *       configured threshold, which pulls the setpoint down smoothly.
 *   <li><b>Damped baseline</b> (kernel {@code wb_update_dirty_ratelimit},
 *       adjusted every 200 ms): a slow-moving budget baseline that tracks a
 *       Little's-Law estimate ({@code maxPass × minRt}) through a direction
 *       gate (only moves toward the position-error side), a three-way clamp
 *       against outlier estimates, and step-size decay. Measurement noise
 *       cannot become budget action in a single step.
 * </ol>
 *
 * <p><b>Convergence</b> (supersedes the old maxPassCache-only argument):
 * continuous gate drops mean no new passes enter the sliding window, so
 * {@code maxPASS()} reads all-zero buckets and {@code est} decays via the
 * maxPassCache ×0.99 path. In-flight strictly drains while gate drops persist
 * (denied cycles never enqueue), so once in-flight reaches the budget the
 * freerun band / budget check admits again. When in-flight overshoots the
 * baseline far enough to engage the downward direction gate, the baseline also
 * follows the decaying estimate with a bounded step (≤ ~12.5% of the remaining
 * gap per 200 ms update). The design converges without lock-up; the freerun
 * band shrinks with the baseline.
 *
 * <p><b>Application:</b> Used by {@link KeyReporter} to skip flush cycles
 * when the reporting pipeline is saturated, providing back-pressure that is
 * proportional to system load.
 */
@Internal
public class BbrRateLimiterImpl implements BbrRateLimiter {

  /** Q10 fixed-point shift, mirrors kernel {@code RATELIMIT_CALC_SHIFT}. */
  private static final int RATELIMIT_CALC_SHIFT = 10;
  /** pos_ratio value representing 1.0 in Q10. */
  private static final long POS_RATIO_ONE = 1L << RATELIMIT_CALC_SHIFT;
  /** pos_ratio upper clamp (kernel {@code clamp(pos_ratio, 0, 2 << SHIFT)}). */
  private static final long POS_RATIO_MAX = 2L << RATELIMIT_CALC_SHIFT;
  /** Pre-cube clamp on the normalized error; beyond this pos_ratio saturates anyway. */
  private static final long POS_RATIO_X_CLAMP = 4L << RATELIMIT_CALC_SHIFT;
  /** CPU ratio floor: at full CPU load 1/4 of the ceiling is retained. */
  private static final long CPU_RATIO_FLOOR = 256;
  /** CPU derating is a continuous ramp of ±20 pp around the configured threshold. */
  private static final long CPU_RAMP_BAND = 200;
  /** Baseline adjustment cadence, mirrors the kernel's 200 ms damping period. */
  private static final long BASELINE_INTERVAL_MS = 200;
  /** Clock rollback / huge-jump guard: re-anchor instead of integrating a bogus delta. */
  private static final long BASELINE_STALE_MS = 60_000;
  /** Default absolute in-flight ceiling when not configured. */
  private static final long DEFAULT_MAX_IN_FLIGHT_CEILING = 128;

  private final SystemLoadMonitor cpuMonitor;
  private final int cpuThreshold; // 0–1000
  private final long cooldownMs;
  /** Quasi-static hard limit (kernel {@code thresh}); also caps the Little-Law estimate. */
  private final long maxInFlightCeiling;

  private final long[] passBuckets;
  private final long[] rtBuckets;
  private final int[] rtCounts;
  private final int bucketCount;
  private final long bucketDurationMs;
  private final int bucketPerSecond;

  private final Object bucketLock = new Object();
  private int currentBucket;
  private long windowStart;

  /** Damped baseline, analog of kernel {@code wb->dirty_ratelimit}. Guarded by bucketLock. */
  private long balancedInFlight = 1;
  /** Previous undamped estimate, analog of kernel {@code wb->balanced_dirty_ratelimit}. Guarded by bucketLock. */
  private long lastBalanced = 1;
  /** Whether the baseline has been seeded from a real window estimate. Guarded by bucketLock. */
  private boolean baselineWarmed;
  /** Last baseline adjustment timestamp, guarded by bucketLock. */
  private long lastBaselineUpdateMs;

  private static final class InFlightField extends BbrPadding.InFlightRef {}

  private final InFlightField inFlightField = new InFlightField();

  private static final class MaxPassMinRtField extends BbrPadding.MaxPassMinRtRef {}

  private final MaxPassMinRtField maxPassMinRtField = new MaxPassMinRtField();

  private static final class DropTimeMinFlightField extends BbrPadding.DropTimeMinFlightRef {}

  private final DropTimeMinFlightField dropTimeMinFlightField = new DropTimeMinFlightField();

  private final AtomicLong totalPassed = new AtomicLong(0);
  private final AtomicLong totalDropped = new AtomicLong(0);

  /**
   * Constructs a BBR rate limiter with explicit configuration and the default
   * absolute in-flight ceiling.
   *
   * @param cpuMonitor  the system CPU load monitor used to derive the load signal
   * @param cpuThreshold CPU threshold on a 0-1000 scale; center of the continuous
   *                     derating ramp (see class javadoc)
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
    this(cpuMonitor, cpuThreshold, windowMs, bucketCount, cooldownMs, DEFAULT_MAX_IN_FLIGHT_CEILING);
  }

  /**
   * Constructs a BBR rate limiter with explicit configuration including the
   * absolute in-flight ceiling.
   *
   * <p>The ceiling is the quasi-static position reference (kernel {@code thresh}):
   * the Little-Law estimate and the damped baseline are both capped by it, and
   * CPU pressure derates it continuously. It should exceed the expected worker
   * count ({@code minInFlight}) so the floor never overrides it.
   *
   * @param cpuMonitor          the system CPU load monitor
   * @param cpuThreshold        CPU threshold on a 0-1000 scale (ramp center)
   * @param windowMs            duration of the sliding window in milliseconds
   * @param bucketCount         number of buckets within the sliding window
   * @param cooldownMs          duration of the cooldown period after a drop in milliseconds
   * @param maxInFlightCeiling  absolute in-flight ceiling; must be positive
   */
  public BbrRateLimiterImpl(
    SystemLoadMonitor cpuMonitor,
    int cpuThreshold,
    long windowMs,
    int bucketCount,
    long cooldownMs,
    long maxInFlightCeiling
  ) {
    Assert.isTrue(bucketCount > 0 && windowMs > 0, "windowMs and bucketCount must be positive");
    Assert.isTrue(maxInFlightCeiling > 0, "maxInFlightCeiling must be positive");
    long duration = windowMs / bucketCount;
    Assert.isTrue(duration > 0, "windowMs(" + windowMs + ") must be >= bucketCount(" + bucketCount + ")");
    this.cpuMonitor = cpuMonitor;
    this.cpuThreshold = cpuThreshold;
    this.cooldownMs = cooldownMs;
    this.maxInFlightCeiling = maxInFlightCeiling;
    this.bucketCount = bucketCount;
    this.bucketDurationMs = duration;
    this.bucketPerSecond = Math.max(1, (int) (1000L / duration));
    this.passBuckets = new long[bucketCount];
    this.rtBuckets = new long[bucketCount];
    this.rtCounts = new int[bucketCount];
    this.windowStart = currentTimeMillis();
  }

  /**
   * Check whether the current flush cycle is allowed.
   * <p>
   * If allowed, the caller <b>must</b> call {@link #onSuccess(long)} or
   * {@link #onConsumerDrop()} afterward.  {@link #onEnqueue()} must be called
   * after a successful enqueue.
   *
   * @return {@code true} if the flush is allowed
   */
  @Override
  public boolean tryAcquire() {
    // Read the EMA outside the bucket lock: the monitor may aggregate samples
    // itself, and holding bucketLock across it would amplify lock contention
    // on every flush cycle. The value is a smoothed average, so a read taken
    // a few microseconds before the admission decision is not stale.
    double cpuLoad = cpuMonitor.getCpuLoadEMA() * 1000.0; // convert 0-1 → 0-1000
    synchronized (bucketLock) {
      tick();
      updateBaseline(cpuLoad);

      long currentInFlight = inFlightField.value.get();
      // Freerun band (kernel dirty_freerun_ceiling): below half the damped
      // baseline the control loop does not restrict at all. A recent consumer
      // drop (cooldown) bypasses the band — the budget is then strict.
      if (currentInFlight <= freerunCeiling() && !isCooldown()) {
        return true;
      }
      return currentInFlight <= effectiveBudget(cpuLoad, currentInFlight);
    }
  }

  /** Record one unit of in-flight work (batch enqueued). */
  @Override
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
  @Override
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
  @Override
  public void onConsumerDrop() {
    dropTimeMinFlightField.lastDropTime = currentTimeMillis();
    totalDropped.incrementAndGet();
    inFlightField.value.decrementAndGet();
  }

  /**
   * Record a dropped flush from the gate (tryAcquire failed — never enqueued, inFlight unchanged).
   *
   * <p>Does NOT update the cooldown timestamp — the next flush cycle is allowed to retry immediately.
   * This prevents a transient drop from blocking all subsequent flushes for {@code cooldownMs}.
   *
   * <p><b>Convergence proof:</b> continuous gate drops mean no new passes enter the sliding window,
   * so maxPASS() eventually reads all-zero buckets and the Little-Law estimate decays via the
   * maxPassCache ×0.99 path. Once in-flight exceeds the damped baseline, the direction gate allows
   * downward moves and the baseline follows the decaying estimate (bounded step per 200 ms update).
   * The budget therefore shrinks until in-flight drains below it and tryAcquire permits again.
   * The design converges without lock-up.
   */
  @Override
  public void onGateDrop() {
    totalDropped.incrementAndGet();
  }

  /** Total batches that passed the limiter (one increment per completed publish — not per flush cycle). */
  @Override
  public long getTotalPassed() {
    return totalPassed.get();
  }

  /** Total batches dropped by the limiter (gate drops + consumer drops, one increment per batch). */
  @Override
  public long getTotalDropped() {
    return totalDropped.get();
  }

  /** Current number of in-flight (enqueued but not yet published) batches. */
  @Override
  public long getInFlight() {
    return inFlightField.value.get();
  }

  /** Dynamically adjust the min concurrency floor to match the number of active Worker nodes.
   *  Called automatically each flush cycle by {@code HotKeyReporter}. */
  @Override
  public void setMinInFlight(int count) {
    dropTimeMinFlightField.minInFlight = Math.max(1, count);
  }

  /**
   * Current effective admission budget: the damped baseline scaled by the CPU-derated
   * position ratio for the current in-flight level. This is exactly what
   * {@link #tryAcquire} enforces above the freerun band.
   */
  @Override
  public long getCurrentMaxInFlight() {
    double cpuLoad = cpuMonitor.getCpuLoadEMA() * 1000.0;
    synchronized (bucketLock) {
      tick();
      updateBaseline(cpuLoad);
      return effectiveBudget(cpuLoad, inFlightField.value.get());
    }
  }

  /** Current damped baseline (kernel {@code wb->dirty_ratelimit} analog). Observability curve. */
  @Override
  public long getBalancedInFlight() {
    synchronized (bucketLock) {
      return balancedInFlight;
    }
  }

  /** Current sliding-window max pass per bucket (the maxPass half of the Little-Law estimate). */
  @Override
  public long getCurrentMaxPass() {
    synchronized (bucketLock) {
      tick();
      return maxPASS();
    }
  }

  /** Current sliding-window min average RT in ms (the minRt half of the Little-Law estimate). */
  @Override
  public long getCurrentMinRt() {
    synchronized (bucketLock) {
      tick();
      return minRT();
    }
  }

  /**
   * Peak pass rate per bucket in the sliding window. Caller must hold bucketLock.
   *
   * <p>When no usable samples exist (all buckets empty) the maxPassCache slowly
   * decays (×0.99 per empty-window read) so the Little-Law estimate — and with
   * it the damped baseline — loosens under persistent gate drops instead of
   * anchoring permanently to a historical peak.
   */
  private long maxPASS() {
    long max = 0;
    for (long v : passBuckets) {
      if (v > max) {
        max = v;
      }
    }
    if (max == 0) {
      return maxPassMinRtField.maxPassCache.updateAndGet(c -> Math.max(1, (long) (c * 0.99)));
    }
    final long observed = max;
    maxPassMinRtField.maxPassCache.updateAndGet(c -> (c + observed) >> 1);
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
    maxPassMinRtField.minRtCache.updateAndGet(c -> (c + observed) >> 1);
    return min;
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

  /**
   * Slow path: adjust the damped baseline at most once per {@link #BASELINE_INTERVAL_MS}.
   * Caller must hold bucketLock.
   *
   * <p>Port of kernel {@code wb_update_dirty_ratelimit} (mm/page-writeback.c:1319-1468):
   * a linear Little-Law estimate ({@code est}) is filtered through a direction gate
   * (move only toward the position-error side, :1436-1446), clamped against
   * {@code lastBalanced}/{@code est}/{@code task} outliers, and applied with
   * step-size decay (:1453-1457) so the baseline moves smoothly even when the
   * estimate jumps.
   */
  private void updateBaseline(double cpuLoad) {
    long now = currentTimeMillis();
    long elapsed = now - lastBaselineUpdateMs;
    if (elapsed >= 0 && elapsed < BASELINE_INTERVAL_MS) {
      return;
    }
    lastBaselineUpdateMs = now;
    // Time-rollback / huge-jump guard (PELT-style, kernel pelt.c:184-194):
    // a negative or absurd delta must never enter the damping loop — re-anchor
    // and let the next cycle use fresh window data.
    if (elapsed < 0 || elapsed > BASELINE_STALE_MS) {
      return;
    }

    long est = estimateBalanced();
    long inFlight = inFlightField.value.get();

    if (!baselineWarmed) {
      // Seed the baseline directly from the first real estimate instead of
      // ramping one rounded step at a time — the decayed step would otherwise
      // take seconds to reach a sane operating point from the seed of 1.
      baselineWarmed = true;
      balancedInFlight = Math.max(dropTimeMinFlightField.minInFlight, Math.max(1, est));
      lastBalanced = balancedInFlight;
      return;
    }

    long limit = effectiveLimit(cpuLoad);
    long setpoint = Math.min(balancedInFlight, limit);
    // task_ratelimit analog (kernel :1346-1348): currently effective budget
    // scaled by the position ratio; the +1 helps ramp from tiny values.
    long task =
      ((balancedInFlight * posRatioPolynom(inFlight, setpoint, positionCeiling(setpoint)))
        >> RATELIMIT_CALC_SHIFT) + 1;

    // Direction gate (kernel :1436-1446): the position error direction decides
    // whether only upward or only downward moves are allowed — not the CPU signal.
    long x;
    long step = 0;
    if (inFlight < setpoint) {
      x = Math.min(lastBalanced, Math.min(est, task));
      if (balancedInFlight < x) {
        step = x - balancedInFlight;
      }
    } else {
      x = Math.max(lastBalanced, Math.max(est, task));
      if (balancedInFlight > x) {
        step = balancedInFlight - x;
      }
    }

    // Step-size decay (kernel :1453-1457): the closer the baseline is to the
    // step target, the smaller the relative move — eliminates pointless tremors.
    if (step > 0) {
      long shift = balancedInFlight / (2 * step + 1);
      step = shift < 63 ? (((step >> shift) + 7) >> 3) : 0;
    }

    balancedInFlight = Math.max(1, balancedInFlight + ((balancedInFlight < est) ? step : -step));
    lastBalanced = est;
  }

  /**
   * Little-Law estimate of the sustainable in-flight level from the sliding window
   * (the {@code balanced} rate analog), capped at the quasi-static ceiling — the
   * analog of kernel {@code balanced_dirty_ratelimit > write_bw → write_bw} (:1385-1386).
   * Caller must hold bucketLock.
   */
  private long estimateBalanced() {
    long mp = maxPASS();
    long mr = minRT();
    if (mp == 0 || mr == 0) {
      return 0;
    }
    long est = (long) Math.floor(((double) mp * mr * bucketPerSecond) / 1000.0 + 0.5);
    return Math.min(est, maxInFlightCeiling);
  }

  /**
   * Effective admission budget for the current in-flight level. Caller must hold bucketLock.
   *
   * <p>{@code baseline × posRatio} with the baseline as the position setpoint: when
   * in-flight sits at the baseline the ratio is 1.0, far below it up to 2.0, above it
   * smoothly below 1.0. The position control runs on a narrow band (saturation at
   * twice the setpoint, kernel-like {@code limit - setpoint} span) — a wide band out
   * to the config ceiling would dilute the position error and stall the loop for
   * small baselines. The result is capped by the (CPU-derated) hard limit and floored
   * by {@code minInFlight} so the degenerate-window case can never unlock unbounded
   * admission.
   */
  private long effectiveBudget(double cpuLoad, long inFlight) {
    long limit = effectiveLimit(cpuLoad);
    long setpoint = Math.min(balancedInFlight, limit);
    long posRatio = posRatioPolynom(inFlight, setpoint, positionCeiling(setpoint));
    long budget = Math.min(limit, (balancedInFlight * posRatio) >> RATELIMIT_CALC_SHIFT);
    return Math.max(dropTimeMinFlightField.minInFlight, budget);
  }

  /**
   * Upper end of the position-control band: setpoint plus a span of
   * {@code max(4, setpoint)} — the position ratio saturates at twice the
   * setpoint. The +4 floor keeps the band meaningful for tiny baselines
   * where a purely proportional span would collapse to a single unit.
   */
  private static long positionCeiling(long setpoint) {
    return setpoint + Math.max(4, setpoint);
  }

  /** Freerun band: half the damped baseline. Caller must hold bucketLock. */
  private long freerunCeiling() {
    return balancedInFlight >> 1;
  }

  /**
   * CPU-derated hard limit: the configured ceiling scaled by a continuous ratio.
   * Caller must hold bucketLock.
   */
  private long effectiveLimit(double cpuLoad) {
    return Math.max(1, (maxInFlightCeiling * cpuRatio(cpuLoad)) >> RATELIMIT_CALC_SHIFT);
  }

  /**
   * Continuous CPU derating ratio in Q10: 1.0 below {@code threshold − band},
   * {@link #CPU_RATIO_FLOOR} above {@code threshold + band}, linear in between.
   * This replaces the old two-state permissive/strict switch — the CPU signal
   * now lowers the hard limit smoothly instead of flipping a mode bit.
   *
   * @param cpuLoad CPU load on a 0-1000 scale
   * @return Q10 ratio in {@code [CPU_RATIO_FLOOR, POS_RATIO_ONE]}
   */
  private long cpuRatio(double cpuLoad) {
    long start = Math.max(0, cpuThreshold - CPU_RAMP_BAND);
    long end = Math.min(1000, cpuThreshold + CPU_RAMP_BAND);
    if (cpuLoad <= start) {
      return POS_RATIO_ONE;
    }
    if (cpuLoad >= end) {
      return CPU_RATIO_FLOOR;
    }
    long span = Math.max(1, end - start);
    long over = (long) cpuLoad - start;
    return POS_RATIO_ONE - (over * (POS_RATIO_ONE - CPU_RATIO_FLOOR)) / span;
  }

  /**
   * Port of kernel {@code pos_ratio_polynom} (mm/page-writeback.c:960-975):
   * {@code f(setpoint) = 1.0}, negative feedback with a cubic curve — fast
   * response on large errors, small oscillation near the setpoint, and the
   * kernel's explicit {@code clamp(pos_ratio, 0, 2 << SHIFT)} bounds. The
   * normalized error is pre-clamped before cubing so the Q10 arithmetic
   * cannot overflow, and the divisor carries the kernel's {@code | 1}
   * divide-by-zero guard.
   *
   * @param inFlight current position
   * @param setpoint target position (must be ≤ limit)
   * @param limit    hard limit
   * @return Q10 position ratio in {@code [0, POS_RATIO_MAX]}
   */
  private static long posRatioPolynom(long inFlight, long setpoint, long limit) {
    long denom = (limit - setpoint) | 1;
    long x = ((setpoint - inFlight) << RATELIMIT_CALC_SHIFT) / denom;
    if (x > POS_RATIO_X_CLAMP) {
      x = POS_RATIO_X_CLAMP;
    } else if (x < -POS_RATIO_X_CLAMP) {
      x = -POS_RATIO_X_CLAMP;
    }
    long posRatio = x;
    posRatio = (posRatio * x) >> RATELIMIT_CALC_SHIFT;
    posRatio = (posRatio * x) >> RATELIMIT_CALC_SHIFT;
    posRatio += POS_RATIO_ONE;
    return Math.max(0, Math.min(POS_RATIO_MAX, posRatio));
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
