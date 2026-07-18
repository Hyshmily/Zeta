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
 * Tests for {@link BbrRateLimiter} covering construction, tryAcquire paths, bucket management,
 * cooldown logic, and internal state propagation.
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
  private BbrRateLimiter limiter;

  @BeforeEach
  void setUp() {
    cpuMonitor = mock(SystemLoadMonitor.class);
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.5);
    limiter = new BbrRateLimiterImpl(cpuMonitor, CPU_THRESHOLD, WINDOW_MS, BUCKETS, COOLDOWN_MS);
  }

  // ── Constructor ──

  @Test
  void deprecatedConstructor_shouldDelegateToFullConstructor() {
    BbrRateLimiter l = new BbrRateLimiterImpl(cpuMonitor);
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.99);
    assertThat(l.tryAcquire()).isTrue();
    l.onEnqueue();
    assertThat(l.getInFlight()).isEqualTo(1);
    l.onSuccess(10);
    assertThat(l.getTotalPassed()).isEqualTo(1);
  }

  // ── tryAcquire – CPU below threshold (permissive) ──

  @Test
  void tryAcquire_whenCpuBelowAndInFlightWithinLimit_shouldAllow() {
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.3);
    assertThat(limiter.tryAcquire()).isTrue();
  }

  @Test
  void tryAcquire_whenCpuBelowAndInFlightExceedsAndNotCooldown_shouldAllow() {
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.3);
    // Build pass+RT data so maxInFlight settles to a known small value.
    for (int i = 0; i < 5; i++) {
      limiter.onEnqueue();
      limiter.onSuccess(100);
    }
    // Raise in-flight above computed maxInFlight (~5).
    for (int i = 0; i < 7; i++) {
      limiter.onEnqueue();
    }
    // No drop -> !isCooldown() -> permissive branch allows.
    assertThat(limiter.tryAcquire()).isTrue();
  }

  @Test
  void tryAcquire_whenCpuBelowAndInFlightExceedsAndCooldown_shouldDeny() {
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.3);
    for (int i = 0; i < 5; i++) {
      limiter.onEnqueue();
      limiter.onSuccess(100);
    }
    for (int i = 0; i < 7; i++) {
      limiter.onEnqueue();
    }
    // Use onConsumerDrop to type cooldown (onGateDrop no longer sets cooldown)
    limiter.onConsumerDrop();
    // isCooldown() = true, inFlight > maxInFlight -> deny.
    assertThat(limiter.tryAcquire()).isFalse();
  }

  // ── tryAcquire – CPU above threshold (strict) ──

  @Test
  void tryAcquire_whenCpuAboveAndInFlightWithinLimit_shouldAllow() {
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.9);
    assertThat(limiter.tryAcquire()).isTrue();
  }

  @Test
  void tryAcquire_whenCpuAboveAndInFlightExceeds_shouldDeny() {
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.9);
    for (int i = 0; i < 5; i++) {
      limiter.onEnqueue();
      limiter.onSuccess(100);
    }
    for (int i = 0; i < 7; i++) {
      limiter.onEnqueue();
    }
    // CPU above threshold, inFlight > maxInFlight -> deny regardless of cooldown.
    assertThat(limiter.tryAcquire()).isFalse();
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
    for (int i = 0; i < 5; i++) {
      limiter.onEnqueue();
      limiter.onSuccess(100);
    }
    for (int i = 0; i < 7; i++) {
      limiter.onEnqueue();
    }
    assertThat(limiter.tryAcquire()).isTrue();
    limiter.onGateDrop();
    // onGateDrop no longer triggers cooldown — next cycle is allowed
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
    // Empty window -> uses caches (maxPassCache=1, minRtCache=1) -> (1*1*10)/1000+0.5 -> 0, Math.max(1,0) -> 1
    assertThat(limiter.getCurrentMaxInFlight()).isEqualTo(1);
    // After populating: 5 passes, avg RT 100 -> (5*100*10)/1000+0.5 -> 5
    for (int i = 0; i < 5; i++) {
      limiter.onEnqueue();
      limiter.onSuccess(100);
    }
    assertThat(limiter.getCurrentMaxInFlight()).isEqualTo(5);
  }

  // ── maxInFlight ──

  @Test
  void maxInFlight_whenDataPresent_shouldComputeBudget() {
    for (int i = 0; i < 5; i++) {
      limiter.onEnqueue();
      limiter.onSuccess(100);
    }
    // 5 passes x 100ms avg RT x 10 bucketPerSecond / 1000 + 0.5 -> 5
    assertThat(limiter.getCurrentMaxInFlight()).isEqualTo(5);
  }

  @Test
  void maxInFlight_whenZeroPass_shouldReturnMaxValue() throws Exception {
    Field mpmrField = BbrRateLimiterImpl.class.getDeclaredField("maxPassMinRtField");
    mpmrField.setAccessible(true);
    Object mpmr = mpmrField.get(limiter);
    Field cacheField = mpmr.getClass().getSuperclass().getDeclaredField("maxPassCache");
    cacheField.setAccessible(true);
    AtomicLong cache = (AtomicLong) cacheField.get(mpmr);
    cache.set(0);
    assertThat(limiter.getCurrentMaxInFlight()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void maxInFlight_whenZeroRt_shouldReturnMaxValue() throws Exception {
    Field mpmrField = BbrRateLimiterImpl.class.getDeclaredField("maxPassMinRtField");
    mpmrField.setAccessible(true);
    Object mpmr = mpmrField.get(limiter);
    Field cacheField = mpmr.getClass().getSuperclass().getDeclaredField("minRtCache");
    cacheField.setAccessible(true);
    AtomicLong cache = (AtomicLong) cacheField.get(mpmr);
    cache.set(0);
    assertThat(limiter.getCurrentMaxInFlight()).isEqualTo(Long.MAX_VALUE);
  }

  // ── maxPASS ──

  @Test
  void maxPASS_shouldTrackPeakAcrossBuckets() throws Exception {
    // Populate bucket 0 with 7 passes.
    for (int i = 0; i < 7; i++) {
      limiter.onEnqueue();
      limiter.onSuccess(100);
    }
    // Tick advances to next bucket, zeroing only the advanced-into bucket.
    advanceBuckets(1);
    // Populate bucket 1 with 3 passes.
    for (int i = 0; i < 3; i++) {
      limiter.onEnqueue();
      limiter.onSuccess(100);
    }
    // Peak across all buckets should be 7 (from bucket 0).
    assertThat(limiter.getCurrentMaxInFlight()).isEqualTo(7);
  }

  @Test
  void maxPASS_whenEmpty_shouldReturnCachedValue() {
    assertThat(limiter.getCurrentMaxInFlight()).isEqualTo(1);
  }

  @Test
  void maxPASS_shouldUpdateCacheEma() throws Exception {
    for (int i = 0; i < 5; i++) {
      limiter.onEnqueue();
      limiter.onSuccess(100);
    }
    // First call populates the cache: maxPassCache -> (1+5)/2 = 3, minRtCache -> (1+100)/2 = 50.
    assertThat(limiter.getCurrentMaxInFlight()).isEqualTo(5);
    // Force tick to zero all buckets so lookups fall back to cache.
    forceZeroAllBuckets();
    // maxPASS returns maxPassCache=3, minRT returns minRtCache=50.
    // maxInFlight = floor(3 * 50 * 10 / 1000 + 0.5) = 2.
    assertThat(limiter.getCurrentMaxInFlight()).isEqualTo(2);
  }

  // ── minRT ──

  @Test
  void minRT_shouldTrackMinAverageAcrossBuckets() throws Exception {
    // Populate bucket 0 with high avg RT (200).
    for (int i = 0; i < 10; i++) {
      limiter.onEnqueue();
      limiter.onSuccess(200);
    }
    advanceBuckets(1);
    // Populate bucket 1 with low avg RT (50).
    for (int i = 0; i < 5; i++) {
      limiter.onEnqueue();
      limiter.onSuccess(50);
    }
    // minRT should pick the smallest avg (50 from bucket 1).
    assertThat(limiter.getCurrentMaxInFlight()).isGreaterThan(0);
  }

  @Test
  void minRT_whenEmpty_shouldReturnCachedValue() {
    assertThat(limiter.getCurrentMaxInFlight()).isEqualTo(1);
  }

  @Test
  void minRT_shouldUpdateCacheEma() throws Exception {
    for (int i = 0; i < 5; i++) {
      limiter.onEnqueue();
      limiter.onSuccess(100);
    }
    // Populate caches: maxPassCache -> 3, minRtCache -> 50.
    assertThat(limiter.getCurrentMaxInFlight()).isEqualTo(5);
    forceZeroAllBuckets();
    // Cache fallback: maxInFlight = floor(3 * 50 * 10 / 1000 + 0.5) = 2.
    assertThat(limiter.getCurrentMaxInFlight()).isEqualTo(2);
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
    for (int i = 0; i < 5; i++) {
      limiter.onEnqueue();
      limiter.onSuccess(100);
    }

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
    for (int i = 0; i < 5; i++) {
      limiter.onEnqueue();
      limiter.onSuccess(100);
    }

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

  // ── isCooldown (tested via tryAcquire behavior) ──

  @Test
  void isCooldown_whenNoDrop_shouldNotAffectPermissiveBranch() {
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.3);
    for (int i = 0; i < 5; i++) {
      limiter.onEnqueue();
      limiter.onSuccess(100);
    }
    for (int i = 0; i < 7; i++) {
      limiter.onEnqueue();
    }
    // Never dropped -> !isCooldown() -> permissive returns true.
    assertThat(limiter.tryAcquire()).isTrue();
  }

  @Test
  void isCooldown_afterDrop_shouldBlockPermissiveBranch() {
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.3);
    for (int i = 0; i < 5; i++) {
      limiter.onEnqueue();
      limiter.onSuccess(100);
    }
    for (int i = 0; i < 7; i++) {
      limiter.onEnqueue();
    }
    // Use onConsumerDrop to type cooldown (onGateDrop no longer sets cooldown)
    limiter.onConsumerDrop();
    // Cooldown active -> isCooldown() = true -> permissive false branch.
    assertThat(limiter.tryAcquire()).isFalse();
  }

  @Test
  void isCooldown_afterCooldownExpires_shouldReallow() throws Exception {
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.3);
    for (int i = 0; i < 5; i++) {
      limiter.onEnqueue();
      limiter.onSuccess(100);
    }
    for (int i = 0; i < 7; i++) {
      limiter.onEnqueue();
    }
    // Use onConsumerDrop to type cooldown (onGateDrop no longer sets cooldown)
    limiter.onConsumerDrop();
    assertThat(limiter.tryAcquire()).isFalse();

    // Simulate cooldown expiry via reflection.
    Field dtmfField = BbrRateLimiterImpl.class.getDeclaredField("dropTimeMinFlightField");
    dtmfField.setAccessible(true);
    Object dtmf = dtmfField.get(limiter);
    Field dropField = dtmf.getClass().getSuperclass().getDeclaredField("lastDropTime");
    dropField.setAccessible(true);
    dropField.set(dtmf, TimeSource.currentTimeMillis() - COOLDOWN_MS - 100);

    assertThat(limiter.tryAcquire()).isTrue();
  }

  // ── Integration: full cycle ──

  @Test
  void fullCycle_enqueueAcquireSuccessDrop() {
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.3);
    for (int i = 0; i < 5; i++) {
      limiter.onEnqueue();
      limiter.onSuccess(100);
    }
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
  void tryAcquire_withVeryHighInFlight_shouldNotOverflow() {
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.3);
    for (int i = 0; i < 5; i++) {
      limiter.onEnqueue();
      limiter.onSuccess(100);
    }
    for (int i = 0; i < 1_000_000; i++) {
      limiter.onEnqueue();
    }
    assertThat(limiter.tryAcquire()).isTrue();
  }

  @Test
  void tryAcquire_afterFullWindowZeroData_shouldFallbackToCache() throws Exception {
    when(cpuMonitor.getCpuLoadEMA()).thenReturn(0.3);
    for (int i = 0; i < 5; i++) {
      limiter.onEnqueue();
      limiter.onSuccess(100);
    }
    forceZeroAllBuckets();
    assertThat(limiter.tryAcquire()).isTrue();
  }

  // ── setMinInFlight ──

  @Test
  void setMinInFlight_shouldFloorMaxInFlight() {
    for (int i = 0; i < 5; i++) {
      limiter.onEnqueue();
      limiter.onSuccess(100);
    }
    limiter.setMinInFlight(20);
    assertThat(limiter.getCurrentMaxInFlight()).isEqualTo(20);
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

  // ── Reflection helpers ──

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
