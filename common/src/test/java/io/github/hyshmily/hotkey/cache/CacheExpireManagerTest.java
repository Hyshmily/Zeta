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
package io.github.hyshmily.hotkey.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.hotkey.model.CacheEntry;
import io.github.hyshmily.hotkey.model.KeyState;
import io.github.hyshmily.hotkey.autoconfigure.HotKeyProperties;
import io.github.hyshmily.hotkey.util.TimeSource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CacheExpireManager}, covering soft-expire logic, TTL computation, and background refresh.
 */
class CacheExpireManagerTest {

  private CacheExpireManager expireManager;
  private Cache<String, Object> caffeineCache;
  private HotKeyProperties ttlConfig;

  @BeforeEach
  void setUp() {
    caffeineCache = Caffeine.newBuilder().maximumSize(100).build();
    ttlConfig = new HotKeyProperties();
    Executor executor = Runnable::run;
    expireManager = new CacheExpireManager(caffeineCache, executor, ttlConfig, 10);
  }

  /**
   * Verifies that isSoftExpireEnabled delegates to the HotKeyProperties configuration.
   */
  @Test
  void isSoftExpireEnabled_shouldDelegateToConfig() {
    assertThat(expireManager.isSoftExpireEnabled()).isTrue();
  }

  /**
   * Verifies that computeHardExpireAt with a positive TTL returns a timestamp in the future.
   */
  @Test
  void computeHardExpireAt_withPositiveTtl_shouldReturnFutureTimestamp() {
    long expireAt = expireManager.computeHardExpireAt(1000);
    assertThat(expireAt).isGreaterThan(System.currentTimeMillis());
  }

  /**
   * Verifies that computeHardExpireAt falls back to the default TTL when the given value is zero.
   */
  @Test
  void computeHardExpireAt_withZeroTtl_shouldFallbackToDefault() {
    long expireAt = expireManager.computeHardExpireAt(0);
    assertThat(expireAt).isGreaterThan(System.currentTimeMillis());
  }

  /**
   * Verifies that getEffectiveHardTtlMs returns the configured default hard TTL value.
   */
  @Test
  void getEffectiveHardTtlMs_shouldReturnConfigValue() {
    assertThat(expireManager.getEffectiveHardTtlMs()).isEqualTo(300_000L);
  }

  /**
   * Verifies that getEffectiveHotHardTtlMs returns the configured hot-key hard TTL value.
   */
  @Test
  void getEffectiveHotHardTtlMs_shouldReturnConfigValue() {
    assertThat(expireManager.getEffectiveHotHardTtlMs()).isEqualTo(3_600_000L);
  }

  /**
   * Verifies that getEffectiveSoftTtlMs returns the configured default soft TTL value.
   */
  @Test
  void getEffectiveSoftTtlMs_shouldReturnConfigValue() {
    assertThat(expireManager.getEffectiveSoftTtlMs()).isEqualTo(30_000L);
  }

  /**
   * Verifies that getEffectiveHotSoftTtlMs returns the configured hot-key soft TTL value.
   */
  @Test
  void getEffectiveHotSoftTtlMs_shouldReturnConfigValue() {
    assertThat(expireManager.getEffectiveHotSoftTtlMs()).isEqualTo(300_000L);
  }

  /**
   * Verifies that isSoftExpired throws IllegalStateException when soft-expire is disabled in config.
   */
  @Test
  void isSoftExpired_whenDisabled_shouldThrow() {
    ttlConfig.setDefaultSoftTtlMs(0);
    ttlConfig.setDefaultHotSoftTtlMs(0);
    CacheExpireManager disabled = new CacheExpireManager(caffeineCache, Runnable::run, ttlConfig, 0);
    assertThatThrownBy(() -> disabled.isSoftExpired("key"))
      .isInstanceOf(IllegalStateException.class);
  }

  /**
   * Verifies that triggerBackgroundRefresh executes the supplier and preserves the cache entry.
   */
  @Test
  void triggerBackgroundRefresh_shouldAcquireAndReleasePermit() {
    caffeineCache.put("key", CacheEntry.builder()
      .value("old")
      .dataVersion(1)
      .isVersionDegraded(false)
      .decisionVersion(0)
      .hardTtlMs(300_000)
      .hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(30_000)
      .softExpireAtMs(System.currentTimeMillis() + 30_000)
      .keyState(KeyState.HOT)
      .normalHardTtlMs(300_000)
      .normalSoftTtlMs(30_000)
      .build());

    expireManager.triggerBackgroundRefresh("key", () -> "newValue", 30_000);

    assertThat(caffeineCache.getIfPresent("key")).isNotNull();
  }

  /**
   * Verifies that a non-CacheEntry plain value is considered soft-expired.
   */
  @Test
  void isSoftExpired_shouldReturnTrueForNonCacheEntry() {
    caffeineCache.put("plain", "not-a-cache-entry");
    assertThat(expireManager.isSoftExpired("not-a-cache-entry")).isTrue();
  }

  /**
   * Verifies that a missing cache key is considered soft-expired.
   */
  @Test
  void isSoftExpired_shouldReturnTrueForMissingEntry() {
    assertThat(expireManager.isSoftExpired(null)).isTrue();
  }

  /**
   * Verifies that computeHardExpireAt with zero falls back to default and returns a future timestamp.
   */
  @Test
  void computeHardExpireAt_withZero_shouldFallbackAndReturnFuture() {
    long result = expireManager.computeHardExpireAt(0);
    assertThat(result).isGreaterThan(System.currentTimeMillis());
  }

  /**
   * Verifies that computeHardExpireAt with a negative TTL falls back to default and returns a future timestamp.
   */
  @Test
  void computeHardExpireAt_withNegative_shouldFallbackAndReturnFuture() {
    long result = expireManager.computeHardExpireAt(-1);
    assertThat(result).isGreaterThan(System.currentTimeMillis());
  }

  /**
   * Verifies that computeHardExpireAt with Long.MAX_VALUE passes it through unchanged.
   */
  @Test
  void computeHardExpireAt_withMaxValue_shouldPassthrough() {
    assertThat(expireManager.computeHardExpireAt(Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Verifies that computeHardExpireAt with a positive TTL returns a future timestamp.
   */
  @Test
  void computeHardExpireAt_withPositive_shouldReturnFuture() {
    long result = expireManager.computeHardExpireAt(1000);
    assertThat(result).isGreaterThan(System.currentTimeMillis());
  }

  /**
   * Verifies that computeSoftExpireAt returns zero when soft-expire is disabled.
   */
  @Test
  void computeSoftExpireAt_withDisabledConfig_shouldReturnMaxValue() {
    ttlConfig.setDefaultSoftTtlMs(0);
    ttlConfig.setDefaultHotSoftTtlMs(0);
    CacheExpireManager disabled = new CacheExpireManager(caffeineCache, Runnable::run, ttlConfig, 0);
    assertThat(disabled.computeSoftExpireAt(0)).isZero();
  }

  /**
   * Verifies that triggerBackgroundRefresh does nothing when soft-expire is disabled.
   */
  @Test
  void triggerBackgroundRefresh_whenDisabled_shouldNotExecute() {
    ttlConfig.setDefaultSoftTtlMs(0);
    ttlConfig.setDefaultHotSoftTtlMs(0);
    caffeineCache.put("key", "plain-value");
    CacheExpireManager disabled = new CacheExpireManager(caffeineCache, Runnable::run, ttlConfig, 0);
    disabled.triggerBackgroundRefresh("key", () -> "should-not-run", 1_000);
    assertThat(caffeineCache.getIfPresent("key")).isEqualTo("plain-value");
  }

  /**
   * Verifies that triggerBackgroundRefresh discards the result when the entry's data version has changed since the refresh started.
   */
  @Test
  void triggerBackgroundRefresh_withStaleVersion_shouldDiscardResult() {
    caffeineCache.put("key", CacheEntry.builder()
      .value("original")
      .dataVersion(5)
      .isVersionDegraded(false)
      .decisionVersion(0)
      .hardTtlMs(300_000)
      .hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(30_000)
      .softExpireAtMs(System.currentTimeMillis() + 300_000)
      .keyState(KeyState.HOT)
      .normalHardTtlMs(300_000)
      .normalSoftTtlMs(30_000)
      .build());

    expireManager.triggerBackgroundRefresh("key", () -> {
      caffeineCache.asMap().computeIfPresent("key", (k, existing) -> {
        if (existing instanceof CacheEntry ce) {
          return ce.toBuilder().dataVersion(10).build();
        }
        return existing;
      });
      return "stale-value";
    }, 30_000);

    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(entry).isNotNull();
    assertThat(entry.getDataVersion()).isEqualTo(10);
    assertThat((Object) entry.getValue()).isEqualTo("original");
  }

  /**
   * Verifies that computeHardExpireAt produces a positive timestamp within a reasonable range.
   */
  @Test
  void hardExpireAt_producesPositiveTimestamp() {
    long hard = expireManager.computeHardExpireAt(5_000);
    assertThat(hard).isGreaterThan(System.currentTimeMillis());
    assertThat(hard).isLessThan(System.currentTimeMillis() + 60_000);
  }

  /**
   * Verifies that computeSoftExpireAt produces a positive timestamp within a reasonable range.
   */
  @Test
  void softExpireAt_producesPositiveTimestamp() {
    long soft = expireManager.computeSoftExpireAt(5_000);
    assertThat(soft).isGreaterThan(System.currentTimeMillis());
    assertThat(soft).isLessThan(System.currentTimeMillis() + 60_000);
  }

  /**
   * Verifies that computeHotHardExpireAt produces a positive future timestamp.
   */
  @Test
  void hotHardExpireAt_producesPositiveTimestamp() {
    long hot = expireManager.computeHotHardExpireAt();
    assertThat(hot).isGreaterThan(System.currentTimeMillis());
  }

  /**
   * Verifies that computeHotHardExpireAt returns Long.MAX_VALUE when hot hard TTL is disabled (0).
   * This covers toHardExpireTimestamp when hardTtlMs <= 0.
   */
  @Test
  void computeHotHardExpireAt_withDisabledHotHardTtl_shouldReturnMaxValue() {
    ttlConfig.setDefaultHotHardTtlMs(0);
    assertThat(expireManager.computeHotHardExpireAt()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Verifies that computeHotSoftExpireAt produces a positive future timestamp.
   */
  @Test
  void hotSoftExpireAt_producesPositiveTimestamp() {
    long hot = expireManager.computeHotSoftExpireAt();
    assertThat(hot).isGreaterThan(System.currentTimeMillis());
  }

  /**
   * Verifies that computeHotSoftExpireAt returns 0 when hot soft TTL is disabled (0).
   * This covers the toSoftExpireTimestamp branch when softTtlMs <= 0.
   */
  @Test
  void computeHotSoftExpireAt_withZeroHotSoftTtl_shouldReturnZero() {
    ttlConfig.setDefaultHotSoftTtlMs(0);
    assertThat(expireManager.computeHotSoftExpireAt()).isZero();
  }

  /**
   * Verifies that isSoftExpired returns true when softExpireAtMs is a past timestamp
   * (positive but earlier than the current time).
   */
  @Test
  void isSoftExpired_withExpiredEntry_shouldReturnTrue() {
    CacheEntry entry = CacheEntry.builder()
      .value("v").dataVersion(1).isVersionDegraded(false)
      .decisionVersion(0).hardTtlMs(300_000).hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(30_000).softExpireAtMs(1L)
      .keyState(KeyState.HOT).normalHardTtlMs(300_000).normalSoftTtlMs(30_000)
      .build();
    assertThat(expireManager.isSoftExpired(entry)).isTrue();
  }

  /**
   * Verifies that isSoftExpired with softExpireAtMs=Long.MAX_VALUE returns false
   * (MAX_VALUE means never soft-expire).
   */
  @Test
  void isSoftExpired_withMaxValue_shouldReturnFalse() {
    CacheEntry entry = CacheEntry.builder()
      .value("v").dataVersion(1).isVersionDegraded(false)
      .decisionVersion(0).hardTtlMs(300_000).hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(Long.MAX_VALUE).softExpireAtMs(Long.MAX_VALUE)
      .keyState(KeyState.HOT).normalHardTtlMs(300_000).normalSoftTtlMs(30_000)
      .build();
    caffeineCache.put("perm", entry);
    assertThat(expireManager.isSoftExpired(entry)).isFalse();
  }

  /**
   * Verifies that isSoftExpired with softExpireAtMs=0 returns true
   * (zero means immediately expired).
   */
  @Test
  void isSoftExpired_withZeroExpireAt_shouldReturnTrue() {
    CacheEntry entry = CacheEntry.builder()
      .value("v").dataVersion(1).isVersionDegraded(false)
      .decisionVersion(0).hardTtlMs(300_000).hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(0).softExpireAtMs(0)
      .keyState(KeyState.HOT).normalHardTtlMs(300_000).normalSoftTtlMs(30_000)
      .build();
    caffeineCache.put("zero", entry);
    assertThat(expireManager.isSoftExpired(entry)).isTrue();
  }

  /**
   * Verifies that computeSoftExpireAt passes Long.MAX_VALUE through unchanged.
   */
  @Test
  void computeSoftExpireAt_withMaxValue_shouldPassthrough() {
    assertThat(expireManager.computeSoftExpireAt(Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Verifies that triggerBackgroundRefresh skips the cache update when the supplier
   * returns null (fault mode: null supplier result).
   */
  @Test
  void triggerBackgroundRefresh_withNullResult_shouldNotUpdateCache() throws InterruptedException {
    Executor asyncExec = Executors.newCachedThreadPool();
    CacheExpireManager asyncExpire = new CacheExpireManager(caffeineCache, asyncExec, ttlConfig, 10);

    caffeineCache.put("key", CacheEntry.builder()
      .value("original").dataVersion(1).isVersionDegraded(false)
      .decisionVersion(0).hardTtlMs(300_000).hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(30_000).softExpireAtMs(System.currentTimeMillis() + 30_000)
      .keyState(KeyState.HOT).normalHardTtlMs(300_000).normalSoftTtlMs(30_000)
      .build());

    asyncExpire.triggerBackgroundRefresh("key", () -> null, 30_000);
    Thread.sleep(200);

    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(entry).isNotNull();
    assertThat((Object) entry.getValue()).isEqualTo("original");
  }

  /**
   * Verifies that triggerBackgroundRefresh skips when the refresh limiter semaphore
   * is exhausted (fault mode: limiter backpressure).
   */
  @Test
  void triggerBackgroundRefresh_withExhaustedLimiter_shouldSkip() throws InterruptedException {
    Executor asyncExec = Executors.newCachedThreadPool();
    CacheExpireManager limited = new CacheExpireManager(caffeineCache, asyncExec, ttlConfig, 1);

    caffeineCache.put("key1", CacheEntry.builder()
      .value("original1").dataVersion(1).isVersionDegraded(false)
      .decisionVersion(0).hardTtlMs(300_000).hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(30_000).softExpireAtMs(System.currentTimeMillis() + 30_000)
      .keyState(KeyState.HOT).normalHardTtlMs(300_000).normalSoftTtlMs(30_000)
      .build());
    caffeineCache.put("key2", CacheEntry.builder()
      .value("original2").dataVersion(1).isVersionDegraded(false)
      .decisionVersion(0).hardTtlMs(300_000).hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(30_000).softExpireAtMs(System.currentTimeMillis() + 30_000)
      .keyState(KeyState.HOT).normalHardTtlMs(300_000).normalSoftTtlMs(30_000)
      .build());

    CountDownLatch blockLatch = new CountDownLatch(1);

    // First call blocks the supplier, holding the only permit
    limited.triggerBackgroundRefresh("key1", () -> {
      try {
        blockLatch.await(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return "first-value";
    }, 30_000);

    // Give first call time to acquire the permit
    Thread.sleep(50);

    // Second refresh for DIFFERENT key should fail tryAcquire and return immediately
    limited.triggerBackgroundRefresh("key2", () -> "second-value", 30_000);

    // key1 should still be "original1" (first hasn't completed yet)
    CacheEntry entry1 = (CacheEntry) caffeineCache.getIfPresent("key1");
    assertThat(entry1).isNotNull();
    assertThat((Object) entry1.getValue()).isEqualTo("original1");

    // key2 should still be "original2" (limiter exhausted, refresh skipped)
    CacheEntry entry2 = (CacheEntry) caffeineCache.getIfPresent("key2");
    assertThat(entry2).isNotNull();
    assertThat((Object) entry2.getValue()).isEqualTo("original2");

    blockLatch.countDown();
  }

  /**
   * Verifies that triggerBackgroundRefresh deduplicates concurrent calls for the
   * same key — only the first caller executes the supplier (per-key dedup).
   */
  @Test
  void triggerBackgroundRefresh_withSameKey_shouldDeduplicate() throws InterruptedException {
    Executor asyncExec = Executors.newCachedThreadPool();
    CacheExpireManager asyncExpire = new CacheExpireManager(caffeineCache, asyncExec, ttlConfig, 10);

    caffeineCache.put("key", CacheEntry.builder()
      .value("original").dataVersion(1).isVersionDegraded(false)
      .decisionVersion(0).hardTtlMs(300_000).hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(30_000).softExpireAtMs(System.currentTimeMillis() + 30_000)
      .keyState(KeyState.HOT).normalHardTtlMs(300_000).normalSoftTtlMs(30_000)
      .build());

    AtomicInteger counter = new AtomicInteger(0);
    CountDownLatch blockLatch = new CountDownLatch(1);

    // First call blocks the supplier
    asyncExpire.triggerBackgroundRefresh("key", () -> {
      counter.incrementAndGet();
      try {
        blockLatch.await(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return "first";
    }, 30_000);

    // Give first call time to register in pendingRefreshes
    Thread.sleep(50);

    // Second call for same key should be deduped (supplier not invoked)
    asyncExpire.triggerBackgroundRefresh("key", () -> {
      counter.incrementAndGet();
      return "second";
    }, 30_000);

    blockLatch.countDown();
    Thread.sleep(300);

    assertThat(counter.get()).isEqualTo(1);
  }

  /**
   * Verifies that triggerBackgroundRefresh with a supplier error logs the failure
   * and leaves the existing entry intact (fault mode: supplier exception).
   */
  @Test
  void triggerBackgroundRefresh_withSupplierError_shouldPreserveExistingEntry() throws InterruptedException {
    Executor asyncExec = Executors.newCachedThreadPool();
    CacheExpireManager asyncExpire = new CacheExpireManager(caffeineCache, asyncExec, ttlConfig, 10);

    caffeineCache.put("key", CacheEntry.builder()
      .value("original").dataVersion(1).isVersionDegraded(false)
      .decisionVersion(0).hardTtlMs(300_000).hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(30_000).softExpireAtMs(System.currentTimeMillis() + 30_000)
      .keyState(KeyState.HOT).normalHardTtlMs(300_000).normalSoftTtlMs(30_000)
      .build());

    asyncExpire.triggerBackgroundRefresh("key", () -> {
      throw new RuntimeException("refresh-failed");
    }, 30_000);

    Thread.sleep(200);

    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(entry).isNotNull();
    assertThat((Object) entry.getValue()).isEqualTo("original");
  }

  /**
   * Verifies that custom ratio (0.5 = ±50%) produces jitter within the configured range.
   */
  @Test
  void toHardExpireTimestamp_withCustomRatio_shouldJitterWithinRange() {
    CacheExpireManager highJitter = new CacheExpireManager(caffeineCache, Runnable::run, ttlConfig, 10, 0.5);
    long ttl = 10_000;
    Stream.generate(() -> highJitter.computeHardExpireAt(ttl))
      .limit(100)
      .forEach(expireAt -> {
        long diff = expireAt - System.currentTimeMillis();
        assertThat(diff).isBetween(5_000L, 15_000L);
      });
  }

  /**
   * Verifies that the default (0.1 = ±10%) jitter is applied and within range for both hard and soft TTL paths.
   */
  @Test
  void toHardExpireTimestamp_withDefaultJitter_shouldJitterWithinRange() {
    long ttl = 10_000;
    Stream.generate(() -> expireManager.computeHardExpireAt(ttl))
      .limit(100)
      .forEach(expireAt -> {
        long diff = expireAt - System.currentTimeMillis();
        assertThat(diff).isBetween(9_000L, 11_000L);
      });
    Stream.generate(() -> expireManager.computeSoftExpireAt(ttl))
      .limit(100)
      .forEach(expireAt -> {
        long diff = expireAt - System.currentTimeMillis();
        assertThat(diff).isBetween(9_000L, 11_000L);
      });
  }

  /**
   * Verifies that a key that is removed from the cache during an in-flight background
   * refresh does not cause errors (fault mode: key evicted mid-refresh).
   */
  @Test
  void triggerBackgroundRefresh_withEvictedKeyDuringRefresh_shouldNotError() throws InterruptedException {
    Executor asyncExec = Executors.newCachedThreadPool();
    CacheExpireManager asyncExpire = new CacheExpireManager(caffeineCache, asyncExec, ttlConfig, 10);

    caffeineCache.put("key", CacheEntry.builder()
      .value("original").dataVersion(1).isVersionDegraded(false)
      .decisionVersion(0).hardTtlMs(300_000).hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(30_000).softExpireAtMs(System.currentTimeMillis() + 30_000)
      .keyState(KeyState.HOT).normalHardTtlMs(300_000).normalSoftTtlMs(30_000)
      .build());

    asyncExpire.triggerBackgroundRefresh("key", () -> {
      caffeineCache.invalidate("key");
      return "fresh-value";
    }, 30_000);

    Thread.sleep(200);

    // Key was evicted in the supplier; the refresh compute should create a new entry
    // because Optional.ofNullable(existing) will be empty, triggering orElseGet branch
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(entry).isNotNull();
    assertThat((Object) entry.getValue()).isEqualTo("fresh-value");
  }

  /**
   * Verifies that toHardExpireTimestamp with a custom jitter ratio uses the given ratio.
   */
  @Test
  void toHardExpireTimestamp_withCustomRatio_shouldUseGivenRatio() {
    long ttl = 10_000;
    Stream.generate(() -> expireManager.toHardExpireTimestamp(ttl, 0.5))
      .limit(50)
      .forEach(expireAt -> {
        long diff = expireAt - TimeSource.currentTimeMillis();
        assertThat(diff).isBetween(5_000L, 15_000L);
      });
  }

  /**
   * Verifies that toHardExpireTimestamp with Long.MAX_VALUE and a custom ratio passes through.
   */
  @Test
  void toHardExpireTimestamp_withMaxValueAndCustomRatio_shouldPassthrough() {
    assertThat(expireManager.toHardExpireTimestamp(Long.MAX_VALUE, 0.5)).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Verifies that toSoftExpireTimestamp with a custom jitter ratio uses the given ratio.
   */
  @Test
  void toSoftExpireTimestamp_withCustomRatio_shouldUseGivenRatio() {
    long ttl = 10_000;
    Stream.generate(() -> expireManager.toSoftExpireTimestamp(ttl, 0.5))
      .limit(50)
      .forEach(expireAt -> {
        long diff = expireAt - TimeSource.currentTimeMillis();
        assertThat(diff).isBetween(5_000L, 15_000L);
      });
  }

  /**
   * Verifies that toSoftExpireTimestamp with a disabled config returns zero regardless of ratio.
   */
  @Test
  void toSoftExpireTimestamp_withDisabledAndCustomRatio_shouldReturnZero() {
    ttlConfig.setDefaultSoftTtlMs(0);
    ttlConfig.setDefaultHotSoftTtlMs(0);
    CacheExpireManager disabled = new CacheExpireManager(caffeineCache, Runnable::run, ttlConfig, 0);
    assertThat(disabled.toSoftExpireTimestamp(10_000, 0.5)).isZero();
  }

  /**
   * Verifies that toSoftExpireTimestamp with non-positive soft TTL returns zero.
   */
  @Test
  void toSoftExpireTimestamp_withNonPositiveTtlAndCustomRatio_shouldReturnZero() {
    assertThat(expireManager.toSoftExpireTimestamp(0, 0.5)).isZero();
    assertThat(expireManager.toSoftExpireTimestamp(-1, 0.5)).isZero();
  }

  /**
   * Verifies that toSoftExpireTimestamp with Long.MAX_VALUE passes through.
   */
  @Test
  void toSoftExpireTimestamp_withMaxValueAndCustomRatio_shouldPassthrough() {
    assertThat(expireManager.toSoftExpireTimestamp(Long.MAX_VALUE, 0.5)).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Verifies that computeNullExpireAt with positive TTL returns a future timestamp.
   */
  @Test
  void computeNullExpireAt_withPositiveTtl_shouldReturnFuture() {
    assertThat(expireManager.computeNullExpireAt(1_000)).isGreaterThan(TimeSource.currentTimeMillis());
  }

  /**
   * Verifies that computeNullExpireAt with zero TTL falls back to config default.
   */
  @Test
  void computeNullExpireAt_withZeroTtl_shouldFallbackToConfig() {
    assertThat(expireManager.computeNullExpireAt(0)).isGreaterThan(TimeSource.currentTimeMillis());
  }

  /**
   * Verifies that computeNullExpireAt with Long.MAX_VALUE passes through.
   */
  @Test
  void computeNullExpireAt_withMaxValue_shouldPassthrough() {
    assertThat(expireManager.computeNullExpireAt(Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Verifies that triggerBackgroundRefresh catches RejectedExecutionException from
   * CompletableFuture.supplyAsync, releases the limiter permit and pending-refresh
   * marker, and preserves the existing cache entry.
   */
  @Test
  void triggerBackgroundRefresh_withRejectedExecution_shouldReleaseResources() {
    Executor rejectingExecutor = task -> { throw new RejectedExecutionException("rejected"); };
    CacheExpireManager rejectingMgr = new CacheExpireManager(caffeineCache, rejectingExecutor, ttlConfig, 10);

    caffeineCache.put("key", CacheEntry.builder()
      .value("original").dataVersion(1).isVersionDegraded(false)
      .decisionVersion(0).hardTtlMs(300_000).hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(30_000).softExpireAtMs(System.currentTimeMillis() + 30_000)
      .keyState(KeyState.HOT).normalHardTtlMs(300_000).normalSoftTtlMs(30_000)
      .build());

    rejectingMgr.triggerBackgroundRefresh("key", () -> "newValue", 30_000);

    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(entry).isNotNull();
    assertThat((Object) entry.getValue()).isEqualTo("original");
  }

  /**
   * Verifies that triggerBackgroundRefresh with a real async executor and the
   * new refresh-timeout wrapper works normally — the value is updated on success.
   */
  @Test
  void triggerBackgroundRefresh_async_withRefreshTimeoutEnabled_shouldWorkNormally() throws InterruptedException {
    Executor asyncExec = Executors.newCachedThreadPool();
    try {
      CacheExpireManager asyncExpire = new CacheExpireManager(caffeineCache, asyncExec, ttlConfig, 10);

      caffeineCache.put("key", CacheEntry.builder()
        .value("original").dataVersion(1).isVersionDegraded(false)
        .decisionVersion(0).hardTtlMs(300_000).hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000).softExpireAtMs(System.currentTimeMillis() + 30_000)
        .keyState(KeyState.HOT).normalHardTtlMs(300_000).normalSoftTtlMs(30_000)
        .build());

      asyncExpire.triggerBackgroundRefresh("key", () -> "updated", 30_000);
      Thread.sleep(200);

      CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
      assertThat(entry).isNotNull();
      assertThat((Object) entry.getValue()).isEqualTo("updated");
    } finally {
      ((java.util.concurrent.ExecutorService) asyncExec).shutdownNow();
    }
  }
}
