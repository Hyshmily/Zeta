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
package io.github.hyshmily.zeta.reporting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.reporting.impl.BbrRateLimiterImpl;
import io.github.hyshmily.zeta.util.SystemLoadMonitor;
import io.github.hyshmily.zeta.util.TimeSource;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link BbrRateLimiter} covering the damped adaptive control loop
 * (freerun band, CPU-derated position ratio, damped baseline), bucket
 * management, cooldown logic, and internal state propagation.
 *
 * <p>Baseline dynamics reference (default ceiling 128, test window 500 ms /
 * 5 buckets → bucketPerSecond 10): Little-Law estimate {@code est =
 * maxPass × minRt × 10 / 1000}. E.g. 5 passes with avg RT 100 ms → est 5.
 */
class BbrRateLimiterTest {

  static {
    TimeSource.start();
  }

  private static final int CPU_THRESHOLD = 800;
  private static final long WINDOW_MS = 500;
  private static final int BUCKETS = 5;
  private static final long COOLDOWN_MS = 1000;

  private SystemLoadMonitor cpuMonitor;
  private BbrRateLimiterImpl limiter;

  @BeforeEach
  void setUp() {
    cpuMonitor = mock(SystemLoadMonitor.class);
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.5);
    limiter = new BbrRateLimiterImpl(cpuMonitor, CPU_THRESHOLD, WINDOW_MS, BUCKETS, COOLDOWN_MS);
  }

  // ── tryAcquire – freerun band (below half the damped baseline) ──

  @Test
  void tryAcquire_whenInFlightWithinFreerunBand_shouldAllow() {
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.3);
    assertThat(limiter.tryAcquire()).isTrue();
  }

  @Test
  void tryAcquire_whenCpuAboveAndInFlightWithinFreerunBand_shouldAllow() {
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.9);
    assertThat(limiter.tryAcquire()).isTrue();
  }

  // ── tryAcquire – budget enforcement (the budget is real, not advisory) ──

  @Test
  void tryAcquire_whenInFlightExceedsBudgetAndNoCooldown_shouldDeny() {
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.3);
    for (int i = 0; i < 5; i++) {
      limiter.onEnqueue();
      limiter.onSuccess(100);
    }
    for (int i = 0; i < 7; i++) {
      limiter.onEnqueue();
    }
    // Warm baseline: est = 5 passes × 100 ms × 10 bps / 1000 = 5 → budget ≈ 4-5.
    // Contract change (ADR-0011): the budget is enforced above the freerun band
    // even without a recent consumer drop — the old permissive branch admitted
    // everything as long as no drop had happened.
    warmBaseline();
    assertThat(limiter.tryAcquire()).isFalse();
  }

  @Test
  void tryAcquire_whenInFlightExceedsBudgetAndCooldown_shouldDeny() {
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.3);
    for (int i = 0; i < 5; i++) {
      limiter.onEnqueue();
      limiter.onSuccess(100);
    }
    for (int i = 0; i < 7; i++) {
      limiter.onEnqueue();
    }
    warmBaseline();
    limiter.onConsumerDrop();
    // Cooldown bypasses the freerun band; in-flight (6) > budget (~4) → deny.
    assertThat(limiter.tryAcquire()).isFalse();
  }

  @Test
  void tryAcquire_whenCpuAboveAndInFlightExceedsBudget_shouldDeny() {
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.9);
    for (int i = 0; i < 5; i++) {
      limiter.onEnqueue();
      limiter.onSuccess(100);
    }
    for (int i = 0; i < 7; i++) {
      limiter.onEnqueue();
    }
    warmBaseline();
    // CPU 90% → continuous derate (no hard switch): limit = 128 × 448/1024 = 56,
    // still far above baseline 5 — the denial comes from the position ratio.
    assertThat(limiter.tryAcquire()).isFalse();
  }

  @Test
  void tryAcquire_withVeryHighInFlight_shouldDenyWithoutOverflow() {
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.3);
    for (int i = 0; i < 5; i++) {
      limiter.onEnqueue();
      limiter.onSuccess(100);
    }
    for (int i = 0; i < 1_000_000; i++) {
      limiter.onEnqueue();
    }
    warmBaseline();
    // 1M in-flight vs baseline 5: position error saturates the cubic, budget
    // collapses to the minInFlight floor, admission is denied — no overflow.
    assertThatCode(() -> assertThat(limiter.tryAcquire()).isFalse()).doesNotThrowAnyException();
  }

  // ── freerun band under CPU derate ──

  @Test
  void freerunBand_bypassesDeratedLimitBelowHalfBaseline() {
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(1.0); // CPU 100% → limit = 128 × 256/1024 = 32
    populatePasses(50, 100); // est = 50 → baseline 50, freerun = 25
    warmBaseline();
    // 15 in-flight: below the freerun band → control loop does not restrict,
    // even though the CPU-derated limit (32) ... admits this too, but the
    // band guarantees admission without running the position control.
    for (int i = 0; i < 15; i++) {
      limiter.onEnqueue();
    }
    assertThat(limiter.tryAcquire()).isTrue();
    // 40 in-flight: above the band and above the derated limit → deny.
    for (int i = 15; i < 40; i++) {
      limiter.onEnqueue();
    }
    assertThat(limiter.tryAcquire()).isFalse();
  }

  // ── CPU derating is continuous, not a two-state switch ──

  @Test
  void cpuDerate_lowersBudgetContinuously() {
    populatePasses(50, 100); // baseline 50
    warmBaseline();

    // CPU 50%: no derate (below ramp start 600) → budget = min(128, 50 × 1.94) = 96
    // (posLimit 100, kernel |1 odd-ified divisor 51 → x=1003 → ratio 1986/1024)
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.5);
    long budgetLowCpu = limiter.getCurrentMaxInFlight();

    // CPU 90%: mid-ramp ratio 448/1024 → limit 56 → budget capped at 56
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.9);
    long budgetMidCpu = limiter.getCurrentMaxInFlight();

    // CPU 100%: ratio floor 256/1024 → limit 32 → budget capped at 32
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(1.0);
    long budgetFullCpu = limiter.getCurrentMaxInFlight();

    assertThat(budgetLowCpu).isEqualTo(96);
    assertThat(budgetMidCpu).isEqualTo(56);
    assertThat(budgetFullCpu).isEqualTo(32);
    assertThat(budgetLowCpu).isGreaterThan(budgetMidCpu);
    assertThat(budgetMidCpu).isGreaterThan(budgetFullCpu);
  }

  // ── damped baseline ──

  @Test
  void baseline_warmsFromFirstRealEstimate() {
    populatePasses(5, 100); // est = 5
    warmBaseline();
    assertThat(limiter.getBalancedInFlight()).isEqualTo(5);
  }

  @Test
  void baseline_coldStartSeedsAtFloor() {
    warmBaseline(); // empty window → est 0 → baseline = max(minInFlight, 1) = 1
    assertThat(limiter.getBalancedInFlight()).isEqualTo(1);
  }

  @Test
  void baseline_capsAtConfiguredCeiling() {
    BbrRateLimiterImpl capped =
      new BbrRateLimiterImpl(cpuMonitor, CPU_THRESHOLD, WINDOW_MS, BUCKETS, COOLDOWN_MS, 64);
    populatePassesOn(capped, 200, 100); // est = 200 → capped at ceiling 64
    forceBaselineUpdate(capped);
    capped.getCurrentMaxInFlight();
    assertThat(capped.getBalancedInFlight()).isEqualTo(64);
    // Budget never exceeds the ceiling even with the position ratio saturated.
    assertThat(capped.getCurrentMaxInFlight()).isEqualTo(64);
  }

  @Test
  void baseline_holdsThroughSingleDegenerateWindow() throws Exception {
    populatePasses(5, 100);
    warmBaseline(); // baseline 5
    forceZeroAllBuckets();
    // Window now empty: maxPassCache decays 3 → 2, minRtCache = 50,
    // est = 1 — but a single degenerate window must not move the damped
    // baseline (position below setpoint → direction gate blocks the move).
    forceBaselineUpdate();
    assertThat(limiter.getCurrentMaxInFlight()).isEqualTo(10);
    // The decayed cache values are visible through the observability getters
    // (this read decays maxPassCache once more: 2 → 1).
    assertThat(limiter.getCurrentMaxPass()).isEqualTo(1);
    assertThat(limiter.getCurrentMinRt()).isEqualTo(50);
  }

  @Test
  void baseline_descendsWhenInFlightOvershootsAndEstimateFalls() {
    populatePasses(50, 100);
    warmBaseline(); // baseline 50
    assertThat(limiter.getBalancedInFlight()).isEqualTo(50);

    // Simulate a sustained overload: 60 in-flight, one consumer drop.
    for (int i = 0; i < 60; i++) {
      limiter.onEnqueue();
    }
    limiter.onConsumerDrop();

    // Drain in-flight batch-by-batch (publishes completing) with forced
    // baseline updates; admission must resume once in-flight reaches the
    // budget (~50). Gate drops never enqueue, so in-flight strictly drains.
    boolean admitted = false;
    for (int i = 0; i < 40 && !admitted; i++) {
      forceBaselineUpdate();
      if (limiter.tryAcquire()) {
        admitted = true;
      } else {
        limiter.onGateDrop();
        limiter.onSuccess(100); // one in-flight batch completes
      }
    }
    assertThat(admitted).isTrue();
    assertThat(limiter.getBalancedInFlight()).isBetween(1L, 128L);
  }

  @Test
  void baseline_rollbackGuard_reanchorsWithoutIntegratingBogusDelta() throws Exception {
    populatePasses(5, 100);
    warmBaseline(); // baseline 5

    // Clock jumps backwards (lastBaselineUpdateMs in the future).
    Field f = BbrRateLimiterImpl.class.getDeclaredField("lastBaselineUpdateMs");
    f.setAccessible(true);
    f.setLong(limiter, TimeSource.currentTimeMillis() + 10_000);

    populatePasses(5, 100); // window would now suggest est ≈ 10
    // elapsed < 0 → re-anchor only, no update: baseline holds at 5.
    limiter.getCurrentMaxInFlight();
    assertThat(limiter.getBalancedInFlight()).isEqualTo(5);

    // After re-anchoring the loop resumes and the baseline climbs toward the
    // new estimate — damped (task-clamped +1 steps killed by the /8 decay at
    // first, then ~×1.125 per step), so it approaches 10 without overshooting.
    for (int i = 0; i < 10; i++) {
      forceBaselineUpdate();
      limiter.getCurrentMaxInFlight();
    }
    assertThat(limiter.getBalancedInFlight()).isBetween(8L, 10L);
  }

  // ── posRatioPolynom bounds (kernel pos_ratio_polynom port) ──

  @Test
  void posRatioPolynom_returnsUnityAtSetpoint() throws Exception {
    assertThat(invokePosRatioPolynom(5, 5, 128)).isEqualTo(1024);
  }

  @Test
  void posRatioPolynom_saturatesAtKernelClamps() throws Exception {
    // Far below setpoint → upper clamp 2.0 (kernel clamp(pos_ratio, 0, 2 << SHIFT)).
    assertThat(invokePosRatioPolynom(-100_000, 5, 128)).isEqualTo(2048);
    // Far above setpoint → lower clamp 0.
    assertThat(invokePosRatioPolynom(100_000, 5, 128)).isEqualTo(0);
    // x = (96-64)<<10 / ((128-96)|1) = 32768/33 = 992 → 1 + 992³>>20 ≈ 1.91
    // (the kernel's | 1 odd-ification of the divisor keeps even spans from
    // dividing exactly — page-writeback.c:968).
    assertThat(invokePosRatioPolynom(64, 96, 128)).isEqualTo(1954);
  }

  @Test
  void posRatioPolynom_guardsDivideByZeroWhenLimitEqualsSetpoint() throws Exception {
    // denom = (limit - setpoint) | 1 → 1 when limit == setpoint; must not throw.
    assertThatCode(() -> invokePosRatioPolynom(0, 128, 128)).doesNotThrowAnyException();
    assertThat(invokePosRatioPolynom(0, 128, 128)).isEqualTo(2048);
  }

  // ── cpuRatio continuous ramp ──

  @Test
  void cpuRatio_isFlatBelowTheRamp() throws Exception {
    assertThat(invokeCpuRatio(500.0)).isEqualTo(1024);
    assertThat(invokeCpuRatio(600.0)).isEqualTo(1024); // ramp start = 800 - 200
  }

  @Test
  void cpuRatio_isLinearInsideTheRamp() throws Exception {
    assertThat(invokeCpuRatio(700.0)).isEqualTo(832);
    assertThat(invokeCpuRatio(800.0)).isEqualTo(640);
    assertThat(invokeCpuRatio(900.0)).isEqualTo(448);
  }

  @Test
  void cpuRatio_floorsAboveTheRamp() throws Exception {
    assertThat(invokeCpuRatio(1000.0)).isEqualTo(256);
    assertThat(invokeCpuRatio(1500.0)).isEqualTo(256);
  }

  // ── onEnqueue ──

  @Test
  void onEnqueue_shouldIncrementInFlight() {
    assertThat(limiter.getInFlight()).isEqualTo(0);
    limiter.onEnqueue();
    assertThat(limiter.getInFlight()).isEqualTo(1);
    limiter.onEnqueue();
    assertThat(limiter.getInFlight()).isEqualTo(2);
  }

  // ── onSuccess ──

  @Test
  void onSuccess_shouldDecrementInFlightAndIncrementTotalPassed() {
    limiter.onEnqueue();
    limiter.onEnqueue();
    assertThat(limiter.getInFlight()).isEqualTo(2);
    limiter.onSuccess(50);
    assertThat(limiter.getInFlight()).isEqualTo(1);
    assertThat(limiter.getTotalPassed()).isEqualTo(1);
    limiter.onSuccess(30);
    assertThat(limiter.getInFlight()).isEqualTo(0);
    assertThat(limiter.getTotalPassed()).isEqualTo(2);
  }

  // ── onDrop variants ──

  @Test
  void onGateDrop_shouldIncrementTotalDropped() {
    assertThat(limiter.getTotalDropped()).isEqualTo(0);
    limiter.onGateDrop();
    assertThat(limiter.getTotalDropped()).isEqualTo(1);
    limiter.onGateDrop();
    assertThat(limiter.getTotalDropped()).isEqualTo(2);
  }

  @Test
  void onGateDrop_shouldNotEnterCooldown() {
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.3);
    populatePasses(5, 100);
    // 2 in-flight ≤ freerun band (baseline 5 / 2) → admitted.
    limiter.onEnqueue();
    limiter.onEnqueue();
    warmBaseline();
    assertThat(limiter.tryAcquire()).isTrue();
    limiter.onGateDrop();
    // onGateDrop does not trigger cooldown — next cycle is allowed again.
    assertThat(limiter.tryAcquire()).isTrue();
  }

  // ── Getters ──

  @Test
  void getTotalPassed_shouldReflectCurrentState() {
    assertThat(limiter.getTotalPassed()).isEqualTo(0);
    limiter.onSuccess(10);
    assertThat(limiter.getTotalPassed()).isEqualTo(1);
    limiter.onSuccess(20);
    assertThat(limiter.getTotalPassed()).isEqualTo(2);
  }

  @Test
  void getTotalDropped_shouldReflectCurrentState() {
    assertThat(limiter.getTotalDropped()).isEqualTo(0);
    limiter.onGateDrop();
    assertThat(limiter.getTotalDropped()).isEqualTo(1);
  }

  @Test
  void getInFlight_shouldReflectCurrentState() {
    assertThat(limiter.getInFlight()).isEqualTo(0);
    limiter.onEnqueue();
    assertThat(limiter.getInFlight()).isEqualTo(1);
    limiter.onSuccess(1);
    assertThat(limiter.getInFlight()).isEqualTo(0);
  }

  @Test
  void getCurrentMaxInFlight_shouldReflectWindowData() {
    // Empty window, cold baseline: budget = max(1, baseline 1 × unity-ish ratio) = 1.
    assertThat(limiter.getCurrentMaxInFlight()).isEqualTo(1);
    // After populating: est = 5 passes × 100 ms × 10 bps / 1000 = 5 → baseline 5;
    // at in-flight 0 the position ratio saturates at 2.0 → budget = 10.
    populatePasses(5, 100);
    forceBaselineUpdate();
    assertThat(limiter.getCurrentMaxInFlight()).isEqualTo(10);
  }

  // ── maxInFlight ──

  @Test
  void maxInFlight_whenDataPresent_shouldComputeBudget() {
    populatePasses(5, 100);
    forceBaselineUpdate();
    // 5 passes x 100ms avg RT x 10 bps / 1000 = 5 → baseline 5;
    // in-flight 0 is far below the setpoint → position ratio 2.0 → budget 10.
    assertThat(limiter.getCurrentMaxInFlight()).isEqualTo(10);
  }

  @Test
  void maxInFlight_whenZeroPass_shouldFallBackToFloor() throws Exception {
    Field mpmrField = BbrRateLimiterImpl.class.getDeclaredField("maxPassMinRtField");
    mpmrField.setAccessible(true);
    Object mpmr = mpmrField.get(limiter);
    Field cacheField = mpmr.getClass().getSuperclass().getDeclaredField("maxPassCache");
    cacheField.setAccessible(true);
    AtomicLong cache = (AtomicLong) cacheField.get(mpmr);
    cache.set(0);
    // With zero pass buckets, maxPASS decays cache to floor(1), so the baseline
    // seeds at minInFlight(1) and the budget can never unlock unbounded admission.
    assertThat(limiter.getCurrentMaxInFlight()).isEqualTo(1);
  }

  @Test
  void maxInFlight_whenZeroRt_shouldFallBackToMinInFlight() throws Exception {
    Field mpmrField = BbrRateLimiterImpl.class.getDeclaredField("maxPassMinRtField");
    mpmrField.setAccessible(true);
    Object mpmr = mpmrField.get(limiter);
    Field cacheField = mpmr.getClass().getSuperclass().getDeclaredField("minRtCache");
    cacheField.setAccessible(true);
    AtomicLong cache = (AtomicLong) cacheField.get(mpmr);
    cache.set(0);
    // A degenerate zero-RT reading must not unlock unbounded admission —
    // the budget falls back to the minInFlight floor (1 here).
    assertThat(limiter.getCurrentMaxInFlight()).isEqualTo(1);
  }

  // ── maxPASS ──

  @Test
  void maxPASS_shouldTrackPeakAcrossBuckets() throws Exception {
    // Populate bucket 0 with 7 passes.
    populatePasses(7, 100);
    // Tick advances to next bucket, zeroing only the advanced-into bucket.
    advanceBuckets(1);
    // Populate bucket 1 with 3 passes.
    populatePasses(3, 100);
    // Peak across all buckets should be 7 (from bucket 0) → baseline 7;
    // at in-flight 0 the position ratio saturates → budget = 7 × 2.0 = 14.
    forceBaselineUpdate();
    assertThat(limiter.getCurrentMaxInFlight()).isEqualTo(14);
  }

  @Test
  void maxPASS_whenEmpty_shouldReturnCachedValue() {
    assertThat(limiter.getCurrentMaxPass()).isEqualTo(1);
  }

  // ── minRT ──

  @Test
  void minRT_shouldTrackMinAverageAcrossBuckets() throws Exception {
    // Populate bucket 0 with high avg RT (200).
    populatePasses(10, 200);
    advanceBuckets(1);
    // Populate bucket 1 with low avg RT (50).
    populatePasses(5, 50);
    // minRT should pick the smallest avg (50 from bucket 1).
    assertThat(limiter.getCurrentMinRt()).isEqualTo(50);
  }

  @Test
  void minRT_whenEmpty_shouldReturnCachedValue() {
    assertThat(limiter.getCurrentMinRt()).isEqualTo(1);
  }

  // ── tick ──

  @Test
  void tick_whenNoTimeElapsed_shouldDoNothing() throws Exception {
    Field wsField = BbrRateLimiterImpl.class.getDeclaredField("windowStart");
    wsField.setAccessible(true);
    wsField.set(limiter, System.currentTimeMillis());

    Field cbField = BbrRateLimiterImpl.class.getDeclaredField("currentBucket");
    cbField.setAccessible(true);
    int before = cbField.getInt(limiter);

    invokeTick();

    int after = cbField.getInt(limiter);
    assertThat(after).isEqualTo(before);
  }

  @Test
  void tick_whenAdvancing_shouldZeroBucketsAndAdvanceIndex() throws Exception {
    populatePasses(5, 100);

    Field wsField = BbrRateLimiterImpl.class.getDeclaredField("windowStart");
    wsField.setAccessible(true);
    Field cbField = BbrRateLimiterImpl.class.getDeclaredField("currentBucket");
    cbField.setAccessible(true);
    Field pbField = BbrRateLimiterImpl.class.getDeclaredField("passBuckets");
    pbField.setAccessible(true);

    int beforeBucket = cbField.getInt(limiter);
    long[] passBuckets = (long[]) pbField.get(limiter);
    assertThat(passBuckets[beforeBucket]).isGreaterThan(0);

    // Advance 3 buckets.
    long bucketDurationMs = WINDOW_MS / BUCKETS;
    wsField.set(limiter, TimeSource.currentTimeMillis() - (3 * bucketDurationMs + 10));

    invokeTick();

    int afterBucket = cbField.getInt(limiter);
    assertThat(afterBucket).isEqualTo((beforeBucket + 3) % BUCKETS);

    // Original bucket data survives (tick zeroes only the buckets it advances INTO).
    assertThat(passBuckets[beforeBucket]).isGreaterThan(0);
    // The last advanced-into bucket is zeroed.
    assertThat(passBuckets[afterBucket]).isEqualTo(0);
  }

  @Test
  void tick_whenFullWindowElapsed_shouldZeroAllBuckets() throws Exception {
    populatePasses(5, 100);

    Field wsField = BbrRateLimiterImpl.class.getDeclaredField("windowStart");
    wsField.setAccessible(true);
    Field cbField = BbrRateLimiterImpl.class.getDeclaredField("currentBucket");
    cbField.setAccessible(true);
    Field pbField = BbrRateLimiterImpl.class.getDeclaredField("passBuckets");
    pbField.setAccessible(true);

    int beforeBucket = cbField.getInt(limiter);
    long[] passBuckets = (long[]) pbField.get(limiter);
    assertThat(passBuckets[beforeBucket]).isGreaterThan(0);

    long bucketDurationMs = WINDOW_MS / BUCKETS;
    wsField.set(limiter, TimeSource.currentTimeMillis() - (BUCKETS * bucketDurationMs + 10));

    invokeTick();

    assertThat(cbField.getInt(limiter)).isEqualTo(beforeBucket);
    for (int i = 0; i < BUCKETS; i++) {
      assertThat(passBuckets[i]).isEqualTo(0);
    }
  }

  // ── cooldown (consumer-drop backpressure, bypasses the freerun band) ──

  @Test
  void cooldown_afterConsumerDrop_shouldEnforceBudgetStrictly() {
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.3);
    populatePasses(5, 100);
    for (int i = 0; i < 7; i++) {
      limiter.onEnqueue();
    }
    warmBaseline();
    limiter.onConsumerDrop();
    // Cooldown active and in-flight (6) above the budget → deny even though
    // no freerun-band shortcut applies.
    assertThat(limiter.tryAcquire()).isFalse();
  }

  @Test
  void cooldown_afterCooldownExpires_shouldReallow() throws Exception {
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.3);
    populatePasses(5, 100);
    for (int i = 0; i < 7; i++) {
      limiter.onEnqueue();
    }
    warmBaseline();
    limiter.onConsumerDrop();
    assertThat(limiter.tryAcquire()).isFalse();

    // Simulate cooldown expiry via reflection.
    Field dtmfField = BbrRateLimiterImpl.class.getDeclaredField("dropTimeMinFlightField");
    dtmfField.setAccessible(true);
    Object dtmf = dtmfField.get(limiter);
    Field dropField = dtmf.getClass().getSuperclass().getDeclaredField("lastDropTime");
    dropField.setAccessible(true);
    dropField.set(dtmf, TimeSource.currentTimeMillis() - COOLDOWN_MS - 100);

    // Drain in-flight into the freerun band (2 = baseline 5 / 2).
    limiter.onSuccess(50);
    limiter.onSuccess(50);
    limiter.onSuccess(50);
    limiter.onSuccess(50);
    assertThat(limiter.getInFlight()).isEqualTo(2);
    assertThat(limiter.tryAcquire()).isTrue();
  }

  // ── Integration: full cycle ──

  @Test
  void fullCycle_enqueueAcquireSuccessDrop() {
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.3);
    populatePasses(5, 100);
    warmBaseline(); // baseline 5, freerun band 2
    assertThat(limiter.getInFlight()).isEqualTo(0);
    limiter.onEnqueue();
    limiter.onEnqueue();
    limiter.onEnqueue();
    assertThat(limiter.getInFlight()).isEqualTo(3);
    assertThat(limiter.tryAcquire()).isTrue();
    limiter.onSuccess(50);
    assertThat(limiter.getInFlight()).isEqualTo(2);
    assertThat(limiter.getTotalPassed()).isEqualTo(6);
    limiter.onConsumerDrop();
    assertThat(limiter.getTotalDropped()).isEqualTo(1);
    assertThat(limiter.getInFlight()).isEqualTo(1); // consumerDrop decrements inFlight
  }

  // ── Edge Cases ──

  @Test
  void onSuccess_withZeroRt_shouldNotDivideByZero() {
    limiter.onEnqueue();
    limiter.onSuccess(0);
    assertThat(limiter.getTotalPassed()).isEqualTo(1);
    assertThat(limiter.getCurrentMaxInFlight()).isGreaterThanOrEqualTo(0);
  }

  @Test
  void onSuccess_withNegativeRt_shouldBeHandled() {
    limiter.onEnqueue();
    limiter.onSuccess(-10);
    assertThat(limiter.getTotalPassed()).isEqualTo(1);
    assertThat(limiter.getInFlight()).isEqualTo(0);
  }

  @Test
  void onSuccess_withoutOnEnqueue_shouldGoNegative() {
    limiter.onSuccess(10);
    assertThat(limiter.getInFlight()).isNegative();
  }

  @Test
  void multipleConsecutiveGateDrops_shouldTrackCorrectly() {
    for (int i = 0; i < 10; i++) {
      limiter.onGateDrop();
    }
    assertThat(limiter.getTotalDropped()).isEqualTo(10);
  }

  @Test
  void tryAcquire_afterFullWindowZeroData_shouldFallbackToCache() throws Exception {
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.3);
    populatePasses(5, 100);
    forceZeroAllBuckets();
    assertThat(limiter.tryAcquire()).isTrue();
  }

  // ── setMinInFlight ──

  @Test
  void setMinInFlight_shouldFloorMaxInFlight() {
    populatePasses(5, 100);
    limiter.setMinInFlight(20);
    forceBaselineUpdate();
    // Baseline warms to max(minInFlight 20, est 5) = 20; at in-flight 0 the
    // position ratio (posLimit 40, |1 divisor 21 → 1904/1024) gives
    // budget = min(128, 37) = 37 ≥ floor 20.
    assertThat(limiter.getCurrentMaxInFlight()).isEqualTo(37);
  }

  // ── onConsumerDrop edge cases ──

  @Test
  void onConsumerDrop_withoutOnEnqueue_shouldGoNegative() {
    limiter.onConsumerDrop();
    assertThat(limiter.getInFlight()).isNegative();
  }

  // ── Concurrency ──

  @Test
  void concurrentAccess_shouldNotCorruptState() throws InterruptedException {
    int threads = 10;
    int iterations = 1000;
    CountDownLatch latch = new CountDownLatch(threads);
    for (int t = 0; t < threads; t++) {
      new Thread(() -> {
        for (int j = 0; j < iterations; j++) {
          limiter.onEnqueue();
          limiter.onSuccess(10);
        }
        latch.countDown();
      })
        .start();
    }
    latch.await();
    assertThat(limiter.getTotalPassed()).isEqualTo((long) threads * iterations);
  }

  // ── Helpers ──

  /** Record {@code count} passes with the given RT (enqueues + successes, net-zero in-flight). */
  private void populatePasses(int count, long rtMs) {
    populatePassesOn(limiter, count, rtMs);
  }

  private void populatePassesOn(BbrRateLimiter target, int count, long rtMs) {
    for (int i = 0; i < count; i++) {
      target.onEnqueue();
      target.onSuccess(rtMs);
    }
  }

  /**
   * Force the next limiter call to run a baseline update: the 200 ms cadence
   * would otherwise skip it inside a fast-running test.
   */
  private void warmBaseline() {
    forceBaselineUpdate();
    limiter.getCurrentMaxInFlight();
  }

  private void forceBaselineUpdate() {
    forceBaselineUpdate(limiter);
  }

  private void forceBaselineUpdate(BbrRateLimiterImpl target) {
    try {
      Field f = BbrRateLimiterImpl.class.getDeclaredField("lastBaselineUpdateMs");
      f.setAccessible(true);
      f.setLong(target, TimeSource.currentTimeMillis() - 250);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  private long invokePosRatioPolynom(long inFlight, long setpoint, long limit) throws Exception {
    Method m = BbrRateLimiterImpl.class.getDeclaredMethod("posRatioPolynom", long.class, long.class, long.class);
    m.setAccessible(true);
    return (Long) m.invoke(null, inFlight, setpoint, limit);
  }

  private long invokeCpuRatio(double cpuLoad) throws Exception {
    Method m = BbrRateLimiterImpl.class.getDeclaredMethod("cpuRatio", double.class);
    m.setAccessible(true);
    return (Long) m.invoke(limiter, cpuLoad);
  }

  private void invokeTick() throws Exception {
    Method tick = BbrRateLimiterImpl.class.getDeclaredMethod("tick");
    tick.setAccessible(true);
    tick.invoke(limiter);
  }

  private void advanceBuckets(int count) throws Exception {
    Field wsField = BbrRateLimiterImpl.class.getDeclaredField("windowStart");
    wsField.setAccessible(true);
    long bucketDurationMs = WINDOW_MS / BUCKETS;
    wsField.set(limiter, TimeSource.currentTimeMillis() - (count * bucketDurationMs + 10));
    invokeTick();
  }

  private void forceZeroAllBuckets() throws Exception {
    Field wsField = BbrRateLimiterImpl.class.getDeclaredField("windowStart");
    wsField.setAccessible(true);
    long bucketDurationMs = WINDOW_MS / BUCKETS;
    wsField.set(limiter, TimeSource.currentTimeMillis() - (BUCKETS * bucketDurationMs + 10));
    invokeTick();
  }
}
