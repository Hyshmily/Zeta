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
package io.github.hyshmily.zeta.sync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.github.hyshmily.zeta.sync.distributedlock.AutoReleaseLock;
import io.github.hyshmily.zeta.sync.distributedlock.impl.RedisLockProvider;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;

/**
 * Tests for {@link RedisLockProvider} covering lock acquisition (direct,
 * retry, inquiry), lock release (success, retry, expiry skip, not-owner),
 * and edge cases (blank key, negative counts, interruption, Redis exceptions).
 */
class RedisLockProviderTest {

  private static final String KEY = "testKey";
  private static final int LOCK_COUNT = 3;
  private static final int INQUIRY_COUNT = 1;
  private static final int UNLOCK_COUNT = 3;

  private StringRedisTemplate redisTemplate;
  private ValueOperations<String, String> valueOps;
  private ScheduledExecutorService scheduler;
  private RedisLockProvider provider;

  @BeforeEach
  void setUp() {
    redisTemplate = mock(StringRedisTemplate.class);
    valueOps = mock(ValueOperations.class);
    scheduler = mock(ScheduledExecutorService.class);
    when(redisTemplate.opsForValue()).thenReturn(valueOps);
    provider = new RedisLockProvider(redisTemplate, LOCK_COUNT, INQUIRY_COUNT, UNLOCK_COUNT, scheduler);
  }

  // ── Lock Acquisition ──────────────────────────────────────────

  @Test
  void tryLock_withFirstAttempt_shouldReturnLock() {
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(true);
    AutoReleaseLock lock = provider.tryLock(KEY, 10, TimeUnit.SECONDS);
    assertThat(lock).isNotNull();
  }

  @Test
  void tryLock_afterRetries_shouldReturnLock() {
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(false, false, true);
    AutoReleaseLock lock = provider.tryLock(KEY, 10, TimeUnit.SECONDS);
    assertThat(lock).isNotNull();
  }

  @Test
  void tryLock_whenAllRetriesExhausted_shouldReturnNull() {
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(false);
    AutoReleaseLock lock = provider.tryLock(KEY, 10, TimeUnit.SECONDS);
    assertThat(lock).isNull();
  }

  @Test
  void tryLock_withTransientFailure_shouldRecoverViaInquiry() {
    String lockKey = "zeta:lock:" + KEY;
    ArgumentCaptor<String> uuidCaptor = ArgumentCaptor.forClass(String.class);
    when(valueOps.setIfAbsent(eq(lockKey), uuidCaptor.capture(), any(Duration.class))).thenReturn(null);
    when(valueOps.get(lockKey)).thenAnswer(invocation -> uuidCaptor.getValue());
    AutoReleaseLock lock = provider.tryLock(KEY, 10, TimeUnit.SECONDS);
    assertThat(lock).isNotNull();
  }

  @Test
  void tryLock_withTransientFailure_whenOtherHolder_shouldReturnNull() {
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(null);
    when(valueOps.get(anyString())).thenReturn("other-owner-uuid");
    AutoReleaseLock lock = provider.tryLock(KEY, 10, TimeUnit.SECONDS);
    assertThat(lock).isNull();
  }

  @Test
  void tryLock_withTransientFailure_whenKeyMissing_shouldReturnNull() {
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(null);
    when(valueOps.get(anyString())).thenReturn(null);
    AutoReleaseLock lock = provider.tryLock(KEY, 10, TimeUnit.SECONDS);
    assertThat(lock).isNull();
  }

  @Test
  void tryLock_withBlankKey_shouldReturnNull() {
    AutoReleaseLock lock = provider.tryLock("", 10, TimeUnit.SECONDS);
    assertThat(lock).isNull();
  }

  @Test
  void tryLock_withNullKey_shouldReturnNull() {
    AutoReleaseLock lock = provider.tryLock(null, 10, TimeUnit.SECONDS);
    assertThat(lock).isNull();
  }

  @Test
  void tryLock_withZeroLockCount_shouldReturnNull() {
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(false);
    AutoReleaseLock lock = provider.tryLock(KEY, 10, TimeUnit.SECONDS, 0, 1, 1);
    assertThat(lock).isNull();
  }

  @Test
  void tryLock_withNegativeCounts_shouldFallbackToDefaults() {
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(false);
    AutoReleaseLock lock = provider.tryLock(KEY, 10, TimeUnit.SECONDS, -1, -1, -1);
    assertThat(lock).isNull();
  }

  @Test
  void tryLock_whenSetIfAbsentThrows_shouldPropagate() {
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenThrow(
      new RuntimeException("Redis connection refused")
    );
    assertThatThrownBy(() -> provider.tryLock(KEY, 10, TimeUnit.SECONDS)).isInstanceOf(RuntimeException.class);
  }

  // ── 6-arg Overload ────────────────────────────────────────────

  @Test
  void tryLock_withExplicitCounts_shouldUseThem() {
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(false, true); // lockCount=2 → 2 attempts
    AutoReleaseLock lock = provider.tryLock(KEY, 10, TimeUnit.SECONDS, 2, 1, 1);
    assertThat(lock).isNotNull();
  }

  @Test
  void tryLock_withExplicitCounts_whenExhausted_shouldReturnNull() {
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(false);
    AutoReleaseLock lock = provider.tryLock(KEY, 10, TimeUnit.SECONDS, 2, 1, 1);
    assertThat(lock).isNull();
  }

  // ── Lock Release ──────────────────────────────────────────────

  @Test
  void close_shouldReleaseViaLuaScript() {
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(true);
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString())).thenReturn(1L);
    AutoReleaseLock lock = provider.tryLock(KEY, 10, TimeUnit.SECONDS);
    assertThat(lock).isNotNull();
    lock.close();
    verify(redisTemplate).execute(any(DefaultRedisScript.class), anyList(), anyString());
  }

  @Test
  void close_whenScriptReturnsNull_shouldRetryThenSucceed() {
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(true);
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString())).thenReturn(null).thenReturn(1L);
    AutoReleaseLock lock = provider.tryLock(KEY, 10, TimeUnit.SECONDS);
    assertThat(lock).isNotNull();
    lock.close();
    verify(redisTemplate, times(2)).execute(any(DefaultRedisScript.class), anyList(), anyString());
  }

  @Test
  void close_whenScriptThrows_shouldRetryThenSucceed() {
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(true);
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString()))
      .thenThrow(new RedisSystemException("Redis down", new RuntimeException()))
      .thenReturn(1L);
    AutoReleaseLock lock = provider.tryLock(KEY, 10, TimeUnit.SECONDS);
    assertThat(lock).isNotNull();
    lock.close();
    verify(redisTemplate, times(2)).execute(any(DefaultRedisScript.class), anyList(), anyString());
  }

  @Test
  void close_whenAllRetriesFail_shouldNotThrow() {
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(true);
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString())).thenReturn(null);
    AutoReleaseLock lock = provider.tryLock(KEY, 10, TimeUnit.SECONDS);
    assertThat(lock).isNotNull();
    lock.close();
  }

  // ── Connection-level failures (RedisConnectionFailureException) ─────
  //
  // Regression guard: the catch used to name RedisSystemException only, and
  // RedisConnectionFailureException / QueryTimeoutException are SIBLINGS of it
  // under DataAccessException — not subclasses. The tests above (and the only
  // Redis-exception test that existed) stubbed RedisSystemException, i.e. exactly
  // the one variant the old catch handled, so the most common failure mode
  // (Redis unreachable) was never exercised.

  @Test
  void tryLock_whenRedisConnectionFails_shouldReturnNullInsteadOfThrowing() {
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class)))
      .thenThrow(new RedisConnectionFailureException("Connection refused"));
    assertThat(provider.tryLock(KEY, 10, TimeUnit.SECONDS)).isNull();
  }

  @Test
  void close_whenRedisConnectionFails_shouldNotThrow() {
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(true);
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString()))
      .thenThrow(new RedisConnectionFailureException("Connection refused"));
    AutoReleaseLock lock = provider.tryLock(KEY, 10, TimeUnit.SECONDS);
    assertThat(lock).isNotNull();
    lock.close();
    verify(redisTemplate, times(UNLOCK_COUNT)).execute(any(DefaultRedisScript.class), anyList(), anyString());
  }

  @Test
  void close_alwaysAttemptsUnlockRegardlessOfClock() {
    var handle = new RedisLockProvider.RedisLockHandle(redisTemplate, "zeta:lock:expired", "uuid", 3, null, 0);
    handle.close();
    verify(redisTemplate, times(3)).execute(any(DefaultRedisScript.class), anyList(), anyString());
  }

  @Test
  void close_whenNotOwner_shouldTryAllUnlockRetries() {
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(true);
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString())).thenReturn(0L); // wrong UUID or already released
    AutoReleaseLock lock = provider.tryLock(KEY, 10, TimeUnit.SECONDS);
    assertThat(lock).isNotNull();
    long start = System.currentTimeMillis();
    lock.close();
    assertThat(System.currentTimeMillis() - start).isLessThan(5000); // should not sleep long
  }

  // ── Defaults from 3-arg overload ──────────────────────────────

  @Test
  void tryLock_withDefaults_shouldUseConstructorDefaults() {
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(false, false, true); // 3rd attempt succeeds (default lockCount=3)
    AutoReleaseLock lock = provider.tryLock(KEY, 10, TimeUnit.SECONDS);
    assertThat(lock).isNotNull();
  }

  // ── Watchdog lifecycle ────────────────────────────────────────

  /**
   * Verifies that acquiring a lock schedules the watchdog renewal at the
   * documented cadence: {@code expireMs / 3}, floored at 1s (a 10s TTL →
   * 10000/3 = 3333ms). The watchdog re-arms the TTL for as long as the handle
   * lives — the lock does NOT expire unconditionally after {@code expire}.
   */
  @Test
  void watchdog_shouldScheduleRenewalAtDocumentedCadence() {
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(true);
    provider.tryLock(KEY, 10, TimeUnit.SECONDS);
    verify(scheduler).scheduleWithFixedDelay(any(Runnable.class), eq(3333L), eq(3333L), eq(TimeUnit.MILLISECONDS));
  }

  /**
   * Verifies the sub-second clamp: for TTLs below the 1s renewal floor the
   * interval is clamped down to {@code expireMs / 2} (500ms TTL → 250ms) so
   * the first renewal lands strictly before expiry.
   */
  @Test
  void watchdog_subSecondTtl_shouldClampIntervalToHalfTtl() {
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(true);
    provider.tryLock(KEY, 500, TimeUnit.MILLISECONDS);
    verify(scheduler).scheduleWithFixedDelay(any(Runnable.class), eq(250L), eq(250L), eq(TimeUnit.MILLISECONDS));
  }

  /**
   * Verifies that running the watchdog renewal task re-arms the TTL while the
   * lock is held: the conditional Lua renewal (GET + PEXPIRE on the caller's
   * token) executes with the handle's UUID and the full TTL.
   */
  @Test
  void watchdog_renewalTask_shouldReArmTtlWhileHeld() {
    RedisLockProvider.RedisLockHandle handle = new RedisLockProvider.RedisLockHandle(
      redisTemplate,
      "zeta:lock:" + KEY,
      "uuid-1",
      3,
      scheduler,
      10_000L
    );
    ArgumentCaptor<Runnable> renewalTask = ArgumentCaptor.forClass(Runnable.class);
    verify(scheduler).scheduleWithFixedDelay(renewalTask.capture(), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));

    renewalTask.getValue().run();

    verify(redisTemplate).execute(any(DefaultRedisScript.class), anyList(), eq("uuid-1"), eq("10000"));
    handle.close();
  }

  /**
   * Verifies that close() cancels the watchdog before releasing: after release
   * no further renewal is armed. This pins the leaked-handle consequence from
   * the other side — while the handle lives the watchdog renews forever, so
   * close() is the only thing that stops it.
   */
  @Test
  @SuppressWarnings("unchecked")
  void close_shouldCancelWatchdog() {
    ScheduledFuture<Void> future = mock(ScheduledFuture.class);
    doReturn(future)
      .when(scheduler)
      .scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
    RedisLockProvider.RedisLockHandle handle = new RedisLockProvider.RedisLockHandle(
      redisTemplate,
      "zeta:lock:" + KEY,
      "uuid-1",
      3,
      scheduler,
      10_000L
    );

    handle.close();

    verify(future).cancel(false);
    // The watchdog was armed exactly once (at construction) and never re-armed.
    verify(scheduler, times(1)).scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
  }

  // ── Lease loss detection ──────────────────────────────────────

  /**
   * The renewal script returns {@code 0} when the UUID in Redis no longer
   * matches ours — the lease lapsed (a pause longer than the TTL) or a peer
   * took the key. The watchdog must record that as loss so the holder stops
   * assuming mutual exclusion. This is the signal the implementation used to
   * discard outright, which made a lapsed lease indistinguishable from a held
   * one.
   */
  @Test
  @SuppressWarnings("unchecked")
  void watchdog_renewalReturningZero_shouldMarkLeaseLost() {
    ScheduledFuture<Void> future = mock(ScheduledFuture.class);
    doReturn(future)
      .when(scheduler)
      .scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), any(), any())).thenReturn(0L);
    var handle = new RedisLockProvider.RedisLockHandle(
      redisTemplate,
      "zeta:lock:" + KEY,
      "uuid-1",
      3,
      scheduler,
      10_000L
    );
    assertThat(handle.isHeld()).isTrue();
    assertThat(handle.lostAtMonoMs()).isEqualTo(-1L);

    runCapturedRenewalTask();

    assertThat(handle.isHeld()).isFalse();
    assertThat(handle.lostAtMonoMs()).isGreaterThanOrEqualTo(0L);
    // Nothing left to extend: the watchdog stops renewing a lock we do not own.
    verify(future).cancel(false);
  }

  /**
   * A renewal that succeeds must leave the handle intact — otherwise a
   * well-behaved lock would be reported lost and callers would abandon a
   * critical section they still own.
   */
  @Test
  @SuppressWarnings("unchecked")
  void watchdog_renewalReturningOne_shouldKeepLeaseHeld() {
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), any(), any())).thenReturn(1L);
    var handle = new RedisLockProvider.RedisLockHandle(
      redisTemplate,
      "zeta:lock:" + KEY,
      "uuid-1",
      3,
      scheduler,
      10_000L
    );

    runCapturedRenewalTask();

    assertThat(handle.isHeld()).isTrue();
    assertThat(handle.lostAtMonoMs()).isEqualTo(-1L);
  }

  /**
   * A renewal that throws is a transport failure, not proof of loss: the lease
   * may still be ticking in Redis. Reporting loss here would make callers
   * abandon a lock they hold, so a failure must not flip the flag — only the
   * script's {@code 0} verdict may.
   */
  @Test
  @SuppressWarnings("unchecked")
  void watchdog_renewalThrowing_shouldNotClaimLoss() {
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), any(), any()))
      .thenThrow(new RedisConnectionFailureException("redis down"));
    var handle = new RedisLockProvider.RedisLockHandle(
      redisTemplate,
      "zeta:lock:" + KEY,
      "uuid-1",
      3,
      scheduler,
      10_000L
    );

    runCapturedRenewalTask();

    assertThat(handle.isHeld()).isTrue();
    assertThat(handle.lostAtMonoMs()).isEqualTo(-1L);
  }

  /**
   * Repeated lost renewals must report one transition, not one per tick: a lease
   * that stays lost can fire the task again before cancellation takes effect.
   */
  @Test
  @SuppressWarnings("unchecked")
  void watchdog_repeatedLostRenewals_shouldReportSingleLossTimestamp() {
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), any(), any())).thenReturn(0L);
    var handle = new RedisLockProvider.RedisLockHandle(
      redisTemplate,
      "zeta:lock:" + KEY,
      "uuid-1",
      3,
      scheduler,
      10_000L
    );

    runCapturedRenewalTask();
    long firstLoss = handle.lostAtMonoMs();
    runCapturedRenewalTask();

    assertThat(handle.isHeld()).isFalse();
    assertThat(handle.lostAtMonoMs()).isEqualTo(firstLoss);
  }

  /**
   * With no watchdog wired (no scheduler), the renewal never runs — so the
   * release path is the only place loss can be observed. {@code close()} must
   * therefore treat a {@code 0} from the unlock script as loss rather than
   * returning the clean idempotent success it would otherwise report.
   */
  @Test
  @SuppressWarnings("unchecked")
  void close_unlockReturningZero_shouldMarkLeaseLost() {
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), any())).thenReturn(0L);
    var handle = new RedisLockProvider.RedisLockHandle(
      redisTemplate,
      "zeta:lock:" + KEY,
      "uuid-1",
      3,
      null,
      10_000L
    );
    assertThat(handle.isHeld()).isTrue();

    handle.close();

    assertThat(handle.isHeld()).isFalse();
  }

  /**
   * The default {@link AutoReleaseLock#isHeld()} must stay {@code true}: it is
   * the answer for providers that cannot observe lease loss, and flipping it to
   * {@code false} would make every such lock look broken.
   */
  @Test
  void autoReleaseLock_defaultIsHeld_shouldReportTrue() {
    AutoReleaseLock lock = () -> {};
    assertThat(lock.isHeld()).isTrue();
  }

  /** Run the single watchdog task the handle armed at construction. */
  private void runCapturedRenewalTask() {
    ArgumentCaptor<Runnable> renewalTask = ArgumentCaptor.forClass(Runnable.class);
    verify(scheduler, atLeastOnce())
      .scheduleWithFixedDelay(renewalTask.capture(), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));
    renewalTask.getValue().run();
  }
}
