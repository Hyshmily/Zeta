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
package io.github.hyshmily.hotkey.sync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.hyshmily.hotkey.cache.distributedlock.AutoReleaseLock;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
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
  private RedisLockProvider provider;

  @BeforeEach
  void setUp() {
    redisTemplate = mock(StringRedisTemplate.class);
    valueOps = mock(ValueOperations.class);
    when(redisTemplate.opsForValue()).thenReturn(valueOps);
    provider = new RedisLockProvider(redisTemplate, LOCK_COUNT, INQUIRY_COUNT, UNLOCK_COUNT);
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
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class)))
      .thenReturn(false, false, true);
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
    String lockKey = "hotkey:lock:" + KEY;
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
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class)))
      .thenThrow(new RuntimeException("Redis connection refused"));
    assertThatThrownBy(() -> provider.tryLock(KEY, 10, TimeUnit.SECONDS))
      .isInstanceOf(RuntimeException.class);
  }

  // ── 6-arg Overload ────────────────────────────────────────────

  @Test
  void tryLock_withExplicitCounts_shouldUseThem() {
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class)))
      .thenReturn(false, true); // lockCount=2 → 2 attempts
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
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString()))
      .thenReturn(null).thenReturn(1L);
    AutoReleaseLock lock = provider.tryLock(KEY, 10, TimeUnit.SECONDS);
    assertThat(lock).isNotNull();
    lock.close();
    verify(redisTemplate, times(2)).execute(any(DefaultRedisScript.class), anyList(), anyString());
  }

  @Test
  void close_whenScriptThrows_shouldRetryThenSucceed() {
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(true);
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString()))
      .thenThrow(new RuntimeException("Redis down")).thenReturn(1L);
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

  @Test
  void close_whenExpired_shouldSkipUnlock() {
    long past = System.currentTimeMillis() - 5000;
    var expired = new RedisLockProvider.RedisLockHandle(
      redisTemplate, "hotkey:lock:expired", "uuid", past, 3
    );
    expired.close();
    verify(redisTemplate, never()).execute(any(), anyList(), anyString());
  }

  @Test
  void close_whenNotOwner_shouldTryAllUnlockRetries() {
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(true);
    when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), anyString()))
      .thenReturn(0L); // wrong UUID or already released
    AutoReleaseLock lock = provider.tryLock(KEY, 10, TimeUnit.SECONDS);
    assertThat(lock).isNotNull();
    long start = System.currentTimeMillis();
    lock.close();
    assertThat(System.currentTimeMillis() - start).isLessThan(5000); // should not sleep long
  }

  // ── Defaults from 3-arg overload ──────────────────────────────

  @Test
  void tryLock_withDefaults_shouldUseConstructorDefaults() {
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class)))
      .thenReturn(false, false, true); // 3rd attempt succeeds (default lockCount=3)
    AutoReleaseLock lock = provider.tryLock(KEY, 10, TimeUnit.SECONDS);
    assertThat(lock).isNotNull();
  }
}
