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
package io.github.hyshmily.zeta.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.BroadcastBuffer;
import io.github.hyshmily.zeta.cache.cachesupport.ExpireManager;
import io.github.hyshmily.zeta.cache.cachesupport.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.impl.ExpireManagerImpl;
import io.github.hyshmily.zeta.cache.codec.CacheCompressor;
import io.github.hyshmily.zeta.exception.ZetaBlockedException;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.model.ZetaCacheStats;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.rule.Rule.RuleAction;
import io.github.hyshmily.zeta.rule.impl.RuleMatcherImpl;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.util.version.impl.VersionControllerImpl;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link HotKeyCache}, covering peek, get, invalidate, and blacklist behaviors.
 */
class ZetaCacheTest {

  private HotKeyDetector hotKeyDetector;
  private Cache<String, Object> caffeineCache;
  private SingleFlight singleFlight;
  private ExpireManager expireManager;
  private Executor executor;
  private HotKeyCache hotKeyCache;
  private ScheduledExecutorService scheduler;

  @BeforeEach
  void setUp() {
    hotKeyDetector = mock(HotKeyDetector.class);
    when(hotKeyDetector.contains(anyString())).thenReturn(false);
    caffeineCache = Caffeine.newBuilder().maximumSize(100).build();
    singleFlight = mock(SingleFlight.class);
    executor = Runnable::run;
    ZetaProperties ttlConfig = new ZetaProperties();
    expireManager = new ExpireManagerImpl(caffeineCache, executor, ttlConfig, 10);
    scheduler = Executors.newSingleThreadScheduledExecutor();

    hotKeyCache = new HotKeyCache(
      hotKeyDetector,
      caffeineCache,
      singleFlight,
      expireManager,
      executor,
      new CentralDispatcher(
        Optional.empty(),
        Optional.empty(),
        new BroadcastBuffer(scheduler, Optional.empty()),
        hotKeyDetector
      ),
      new RuleMatcherImpl(Optional.empty(), Optional.empty()),
      new VersionControllerImpl(Optional.empty(), 60),
      ttlConfig,
      mock(HealthView.class),
      CacheCompressor.NONE
    );
  }

  /**
   * Verifies that peek returns a cached CacheEntry value.
   */
  @Test
  void peek_shouldReturnCachedValue() {
    caffeineCache.put(
      "key1",
      CacheEntry.builder()
        .value("stored")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(30_000)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );

    assertThat(hotKeyCache.peek("key1")).contains("stored");
  }

  /**
   * Verifies that peek returns empty for null or blank keys.
   */
  @Test
  void peek_shouldReturnEmptyForInvalidKey() {
    assertThat(hotKeyCache.peek(null)).isEmpty();
    assertThat(hotKeyCache.peek("")).isEmpty();
  }

  /**
   * Verifies that a cache entry with KeyState.HOT is identified as a local hot key.
   */
  @Test
  void isHotKey_shouldReturnTrueForHotEntry() {
    caffeineCache.put(
      "hotKey",
      CacheEntry.builder()
        .value("v")
        .dataVersion(0)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(3_600_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(300_000)
        .softExpireAtMs(System.currentTimeMillis() + 300_000)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );

    assertThat(hotKeyCache.isHot("hotKey")).isTrue();
  }

  /**
   * Verifies that a cache entry with KeyState.NORMAL is not identified as a local hot key.
   */
  @Test
  void isHot_shouldReturnFalseForNormalEntry() {
    caffeineCache.put(
      "normalKey",
      CacheEntry.builder()
        .value("v")
        .dataVersion(0)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() + 30_000)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );

    assertThat(hotKeyCache.isHot("normalKey")).isFalse();
  }

  /**
   * Verifies that isHot returns false for a key not present in the cache.
   */
  @Test
  void isZeta_shouldReturnFalseForMissing() {
    assertThat(hotKeyCache.isHot("missing")).isFalse();
  }

  /**
   * Verifies that isHot returns false for null keys.
   */
  @Test
  void isZeta_shouldReturnFalseForInvalid() {
    assertThat(hotKeyCache.isHot(null)).isFalse();
  }

  /**
   * Verifies that get returns a cached raw value without invoking the loader.
   */
  @Test
  void get_shouldReturnCachedValueOnHit() {
    caffeineCache.put("key1", "rawValue");
    assertThat(hotKeyCache.get("key1", () -> "loaded", 0L, 0L, true)).contains("rawValue");
  }

  /**
   * Verifies that get loads and caches a value on cache miss via SingleFlight.
   */
  @Test
  void get_shouldLoadAndCacheOnMiss() {
    when(singleFlight.load(anyString(), any())).thenReturn(Optional.of("loadedValue"));

    Optional<String> result = hotKeyCache.get("key1", () -> "loadedValue", 0L, 0L, true);
    assertThat(result).contains("loadedValue");
    assertThat(caffeineCache.getIfPresent("key1")).isNotNull();
  }

  /**
   * Verifies that get returns empty for null or blank keys without loading.
   */
  @Test
  void get_shouldReturnEmptyForInvalidKey() {
    assertThat(hotKeyCache.get(null, () -> "v", 0L, 0L, true)).isEmpty();
    assertThat(hotKeyCache.get("", () -> "v", 0L, 0L, true)).isEmpty();
  }

  /**
   * Verifies that invalidate removes a cached entry.
   */
  @Test
  void invalidate_All_shouldRemoveEntry() {
    caffeineCache.put("key1", "value");
    hotKeyCache.invalidate("key1", true);
    assertThat(caffeineCache.getIfPresent("key1")).isNull();
  }

  /**
   * Verifies that invalidate handles null and blank keys without throwing.
   */
  @Test
  void invalidate_All_shouldHandleInvalidKey() {
    hotKeyCache.invalidate((String) null, true);
    hotKeyCache.invalidate("", true);
  }

  /**
   * Verifies that invalidateAllLocal removes multiple cached entries.
   */
  @Test
  void invalidate_All_shouldRemoveEntries() {
    caffeineCache.put("k1", "v1");
    caffeineCache.put("k2", "v2");
    hotKeyCache.invalidate(List.of("k1", "k2"), true);
    assertThat(caffeineCache.getIfPresent("k1")).isNull();
    assertThat(caffeineCache.getIfPresent("k2")).isNull();
  }

  /**
   * Verifies that invalidateAllLocal skips null and blank keys in the input list.
   */
  @Test
  void invalidate_All_shouldSkipInvalidKeys() {
    caffeineCache.put("k1", "v1");
    hotKeyCache.invalidate(Arrays.asList("k1", null, ""), true);
    assertThat(caffeineCache.getIfPresent("k1")).isNull();
  }

  /**
   * Verifies that get throws ZetaBlockedException for a blacklisted key.
   */
  @Test
  void get_shouldThrowZeta() {
    hotKeyCache.addBlacklist("secret");
    assertThatThrownBy(() -> hotKeyCache.get("secret", () -> "db", 0L, 0L, true)).isInstanceOf(
      ZetaBlockedException.class
    );
  }

  /**
   * Verifies that getWithSoftExpire throws ZetaBlockedException for a blacklisted key.
   */
  @Test
  void getWithSoftExpire_shouldThrowZeta() {
    hotKeyCache.addBlacklist("secret");
    assertThatThrownBy(() -> hotKeyCache.getWithSoftExpire("secret", () -> "db", 0L, 0L, true)).isInstanceOf(
      ZetaBlockedException.class
    );
  }

  /**
   * Verifies that getWithSoftExpire falls back to get() when soft expire is disabled.
   */
  @Test
  void getWithSoftExpire_whenDisabled_shouldFallbackToGet() {
    ZetaProperties props = new ZetaProperties();
    props.setDefaultSoftTtlMs(0);
    props.setDefaultHotSoftTtlMs(0);
    ExpireManager noSoft = new ExpireManagerImpl(caffeineCache, executor, props, 10);

    when(singleFlight.load(anyString(), any())).thenReturn(Optional.of("loaded"));

    HotKeyCache cache = new HotKeyCache(
      hotKeyDetector,
      caffeineCache,
      singleFlight,
      noSoft,
      executor,
      new CentralDispatcher(
        Optional.empty(),
        Optional.empty(),
        new BroadcastBuffer(scheduler, Optional.empty()),
        hotKeyDetector
      ),
      new RuleMatcherImpl(Optional.empty(), Optional.empty()),
      new VersionControllerImpl(Optional.empty(), 60),
      props,
      mock(HealthView.class),
      CacheCompressor.NONE
    );

    assertThat(cache.getWithSoftExpire("key", () -> "loaded", 0L, 0L, true)).contains("loaded");
  }

  /**
   * Verifies that getWithSoftExpire returns the cached value when the soft TTL has not expired.
   */
  @Test
  void getWithSoftExpire_notExpired_shouldReturnCachedValue() {
    caffeineCache.put(
      "key",
      CacheEntry.builder()
        .value("cached")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() + 60_000)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );

    assertThat(hotKeyCache.getWithSoftExpire("key", () -> "should-not-load", 0L, 0L, true)).contains("cached");
  }

  /**
   * Verifies that getWithSoftExpire with an expired soft TTL returns the stale value and
   * schedules a background refresh (stale-while-revalidate).
   */
  @Test
  void getWithSoftExpire_expired_shouldReturnStaleAndTriggerRefresh() {
    caffeineCache.put(
      "key",
      CacheEntry.builder()
        .value("stale")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() - 1)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );

    // Should return stale value immediately (stale-while-revalidate)
    assertThat(hotKeyCache.getWithSoftExpire("key", () -> "fresh", 0L, 0L, true)).contains("stale");
  }

  /**
   * Verifies that peek throws ZetaBlockedException when the key matches a blacklist rule.
   */
  @Test
  void peek_withBlacklistedKey_shouldThrow() {
    hotKeyCache.addBlacklist("secret");
    assertThatThrownBy(() -> hotKeyCache.peek("secret")).isInstanceOf(ZetaBlockedException.class);
  }

  /**
   * Verifies that peek returns a raw (non-CacheEntry) value directly.
   */
  @Test
  void peek_shouldReturnRawValue() {
    caffeineCache.put("raw", "rawValue");
    assertThat(hotKeyCache.peek("raw")).contains("rawValue");
  }

  /**
   * Verifies that isHot returns false for a logically expired CacheEntry
   * even when the entry is still present in the Caffeine cache.
   */
  @Test
  void isHot_withExpiredEntry_shouldReturnFalse() {
    caffeineCache.put(
      "expired",
      CacheEntry.builder()
        .value("v")
        .dataVersion(0)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(1)
        .hardExpireAtMs(1)
        .softTtlMs(0)
        .softExpireAtMs(0)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );

    assertThat(hotKeyCache.isHot("expired")).isFalse();
  }

  /**
   * Verifies that putThrough caches the value and preserves it for subsequent reads.
   */
  @Test
  void putThrough_shouldWriteThroughAndCache() {
    hotKeyCache.putThrough("key1", "newValue", () -> {}, 0L, 0L, true);

    assertThat(hotKeyCache.peek("key1")).contains("newValue");
  }

  /**
   * Verifies that putThrough throws ZetaBlockedException when the key is blacklisted.
   */
  @Test
  void putThrough_withBlacklistedKey_shouldThrow() {
    hotKeyCache.addBlacklist("secret");
    assertThatThrownBy(() -> hotKeyCache.putThrough("secret", "value", () -> {}, 0L, 0L, true))
      .isInstanceOf(ZetaBlockedException.class)
      .hasFieldOrPropertyWithValue("cacheKey", "secret");
  }

  /**
   * Verifies that putThrough silently returns for invalid (null/blank) keys.
   */
  @Test
  void putThrough_withInvalidKey_shouldSkip() {
    hotKeyCache.putThrough(null, "value", () -> {}, 0L, 0L, true);
    hotKeyCache.putThrough("", "value", () -> {}, 0L, 0L, true);
    // No exception — silent skip
  }

  /**
   * Verifies that putThrough caches a null value using the NullValue sentinel,
   * which is transparently unwrapped to empty on peek.
   */
  @Test
  void putThrough_withNullValue_shouldUseNullValueSentinel() {
    hotKeyCache.putThrough("null-key", null, () -> {}, 0L, 0L, true);

    assertThat(hotKeyCache.peek("null-key")).isEmpty();
  }

  /**
   * Verifies that invalidateAfterPut with a failed mutation does NOT invalidate
   * the cache entry (fault mode: writer exception).
   */
  @Test
  void invalidateAfterPutAll() {
    caffeineCache.put("key1", "original");
    hotKeyCache.invalidateAfterPut(
      "key1",
      () -> {
        throw new RuntimeException("db-fail");
      },
      true
    );

    assertThat(hotKeyCache.peek("key1")).contains("original");
  }

  /**
   * Verifies that invalidateAfterPut throws ZetaBlockedException when the key is blacklisted.
   */
  @Test
  void invalidateAfterPut_All_withBlacklistedKey_shouldThrow() {
    hotKeyCache.addBlacklist("secret");
    assertThatThrownBy(() -> hotKeyCache.invalidateAfterPut("secret", () -> {}, true))
      .isInstanceOf(ZetaBlockedException.class)
      .hasFieldOrPropertyWithValue("cacheKey", "secret");
  }

  /**
   * Verifies that invalidateAfterPut silently returns for invalid (null/blank) keys.
   */
  @Test
  void invalidateAfterPut_All_withInvalidKey_shouldSkip() {
    caffeineCache.put("k", "v");
    hotKeyCache.invalidateAfterPut(null, () -> {}, true);
    hotKeyCache.invalidateAfterPut("", () -> {}, true);
    // Entry untouched
    assertThat(hotKeyCache.peek("k")).contains("v");
  }

  /**
   * Verifies that batch invalidateAfterPut runs all mutations, and a failing
   * mutation skips only its own key.
   */
  @Test
  void invalidateAfterPut_batch_shouldRunAllMutations() {
    caffeineCache.put("k1", "v1");
    caffeineCache.put("k2", "v2");
    caffeineCache.put("k3", "v3");

    hotKeyCache.invalidateAfterPut(
      Map.of(
        "k1",
        () -> {},
        "k2",
        () -> {
          throw new RuntimeException("fail");
        },
        "k3",
        () -> {}
      ),
      true
    );

    assertThat(caffeineCache.getIfPresent("k1")).isNull();
    assertThat(caffeineCache.getIfPresent("k3")).isNull();
    // k2 mutation failed, entry preserved
    assertThat(caffeineCache.getIfPresent("k2")).isEqualTo("v2");
  }

  /**
   * Verifies that addBlacklist with an invalid key is silently skipped.
   */
  @Test
  void addBlacklist_withInvalidKey_shouldSkip() {
    hotKeyCache.addBlacklist(null);
    hotKeyCache.addBlacklist("");
    assertThat(hotKeyCache.getAllRules()).isEmpty();
  }

  /**
   * Verifies that addWhitelist with an invalid key is silently skipped.
   */
  @Test
  void addWhitelist_withInvalidKey_shouldSkip() {
    hotKeyCache.addWhitelist(null);
    hotKeyCache.addWhitelist("");
    assertThat(hotKeyCache.getAllRules()).isEmpty();
  }

  /**
   * Verifies that isBlacklisted returns true for a blacklisted key.
   */
  @Test
  void isBlacklisted_shouldReturnTrue() {
    hotKeyCache.addBlacklist("secret");
    assertThat(hotKeyCache.isBlacklisted("secret")).isTrue();
    assertThat(hotKeyCache.isBlacklisted("other")).isFalse();
  }

  /**
   * Verifies that isWhitelisted returns true for a whitelisted key.
   */
  @Test
  void isWhitelisted_shouldReturnTrue() {
    hotKeyCache.addWhitelist("allowed");
    assertThat(hotKeyCache.isWhitelisted("allowed")).isTrue();
    assertThat(hotKeyCache.isWhitelisted("other")).isFalse();
  }

  /**
   * Verifies that unBlacklist removes a blacklist rule.
   */
  @Test
  void unBlacklist_shouldRemoveRule() {
    hotKeyCache.addBlacklist("secret");
    assertThat(hotKeyCache.isBlacklisted("secret")).isTrue();
    hotKeyCache.unBlacklist("secret");
    assertThat(hotKeyCache.isBlacklisted("secret")).isFalse();
  }

  /**
   * Verifies that unBlacklist with an invalid key is silently skipped.
   */
  @Test
  void unBlacklist_withInvalidKey_shouldSkip() {
    hotKeyCache.addBlacklist("secret");
    hotKeyCache.unBlacklist(null);
    hotKeyCache.unBlacklist("");
    assertThat(hotKeyCache.isBlacklisted("secret")).isTrue();
  }

  /**
   * Verifies that unWhitelist removes a whitelist rule.
   */
  @Test
  void unWhitelist_shouldRemoveRule() {
    hotKeyCache.addWhitelist("allowed");
    assertThat(hotKeyCache.isWhitelisted("allowed")).isTrue();
    hotKeyCache.unWhitelist("allowed");
    assertThat(hotKeyCache.isWhitelisted("allowed")).isFalse();
  }

  /**
   * Verifies that evaluateRule returns the expected action for blacklisted keys.
   */
  @Test
  void evaluateRule_shouldReturnBlockForBlacklistedKey() {
    hotKeyCache.addBlacklist("secret");
    assertThat(hotKeyCache.evaluateRule("secret")).isEqualTo(RuleAction.BLOCK);
    assertThat(hotKeyCache.evaluateRule("other")).isEqualTo(RuleAction.ALLOW);
  }

  /**
   * Verifies that getAllRules returns the current set of rules.
   */
  @Test
  void getAllRules_shouldReturnCurrentRules() {
    hotKeyCache.addBlacklist("key1");
    hotKeyCache.addBlacklist("key2");
    assertThat(hotKeyCache.getAllRules()).hasSize(2);
  }

  /**
   * Verifies that clearAllRules removes all rules.
   */
  @Test
  void clearAllRules_shouldRemoveAllRules() {
    hotKeyCache.addBlacklist("key1");
    hotKeyCache.addBlacklist("key2");
    assertThat(hotKeyCache.getAllRules()).hasSize(2);
    hotKeyCache.clearAllRules();
    assertThat(hotKeyCache.getAllRules()).isEmpty();
  }

  /**
   * Verifies that estimatedSizeOfKeysCount returns a positive count for cached entries.
   */
  @Test
  void estimatedSize_shouldReturnEstimate() {
    caffeineCache.put("k1", "v1");
    assertThat(hotKeyCache.estimatedSize()).isPositive();
  }

  /**
   * Verifies that invalidateAllLocal (no-arg) clears all cache entries (emergency flush).
   */
  @Test
  void invalidate_All_noArg_shouldClear() {
    caffeineCache.put("k1", "v1");
    caffeineCache.put("k2", "v2");
    assertThat(caffeineCache.estimatedSize()).isPositive();
    hotKeyCache.invalidateAllLocal();
    assertThat(caffeineCache.estimatedSize()).isZero();
  }

  /**
   * Verifies that getWithSoftExpire with an ALLOW_NO_REPORT rule does not report
   * and returns the cached value normally.
   */
  @Test
  void getWithSoftExpire_withNoReportRule_shouldReturnCached() {
    hotKeyCache.addWhitelist("no-report");
    caffeineCache.put(
      "no-report",
      CacheEntry.builder()
        .value("v")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() + 60_000)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );

    assertThat(hotKeyCache.getWithSoftExpire("no-report", () -> "fresh", 0L, 0L, true)).contains("v");
  }

  // ── get with logically expired entry ──

  @Test
  void get_withLogicallyExpiredEntry_shouldReload() {
    caffeineCache.put(
      "expired",
      CacheEntry.builder()
        .value("stale")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(1)
        .hardExpireAtMs(1)
        .softTtlMs(0)
        .softExpireAtMs(0)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );
    when(singleFlight.load(anyString(), any())).thenReturn(Optional.of("fresh"));

    assertThat(hotKeyCache.get("expired", () -> "fresh", 0L, 0L, true)).contains("fresh");
  }

  // ── getWithSoftExpire: invalid key ──

  @Test
  void getWithSoftExpire_withInvalidKey_shouldReturnEmpty() {
    assertThat(hotKeyCache.getWithSoftExpire(null, () -> "v", 0L, 0L, true)).isEmpty();
    assertThat(hotKeyCache.getWithSoftExpire("", () -> "v", 0L, 0L, true)).isEmpty();
  }

  // ── getWithSoftExpire: expired entry triggers reload ──

  @Test
  void getWithSoftExpire_withExpiredEntry_shouldReloadViaLoadAndCache() {
    caffeineCache.put(
      "expired",
      CacheEntry.builder()
        .value("stale")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(1)
        .hardExpireAtMs(1)
        .softTtlMs(0)
        .softExpireAtMs(0)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );
    when(singleFlight.load(anyString(), any())).thenReturn(Optional.of("fresh"));

    assertThat(hotKeyCache.getWithSoftExpire("expired", () -> "fresh", 0L, 0L, true)).contains("fresh");
  }

  // ── getWithSoftExpire: cache miss (no entry) triggers loadAndCache ──

  @Test
  void getWithSoftExpire_withCacheMiss_shouldLoad() {
    when(singleFlight.load(anyString(), any())).thenReturn(Optional.of("loaded"));

    assertThat(hotKeyCache.getWithSoftExpire("missing", () -> "loaded", 0L, 0L, true)).contains("loaded");
  }

  // ── unWhitelist with invalid key ──

  @Test
  void unWhitelist_withInvalidKey_shouldSkip() {
    hotKeyCache.addWhitelist("allowed");
    hotKeyCache.unWhitelist(null);
    hotKeyCache.unWhitelist("");
    assertThat(hotKeyCache.isWhitelisted("allowed")).isTrue();
  }

  // ── getWithSoftExpire with raw (non-CacheEntry) value in cache ──

  @Test
  void getWithSoftExpire_withNonCacheEntryRawValue_returnsRaw() {
    caffeineCache.put("raw", "bare-string");
    assertThat(hotKeyCache.getWithSoftExpire("raw", () -> "fresh", 0L, 0L, true)).contains("bare-string");
  }

  // ── getWithSoftExpire with NORMAL entry and soft expired ──

  @Test
  void getWithSoftExpire_withNormalEntrySoftExpired_shouldReturnStale() {
    caffeineCache.put(
      "normal",
      CacheEntry.builder()
        .value("stale")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() - 1000)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );

    Optional<String> result = hotKeyCache.getWithSoftExpire("normal", () -> "fresh", 0L, 0L, true);

    assertThat(result).contains("stale");
  }

  // ── putLocal ──

  @Test
  void putLocal_shouldCacheValue() {
    hotKeyCache.putLocal("k", "v", 0L, 0L);
    assertThat(hotKeyCache.peek("k")).contains("v");
  }

  @Test
  void putLocal_shouldCreateCacheEntry() {
    hotKeyCache.putLocal("k", "v", 0L, 0L);
    Object raw = caffeineCache.getIfPresent("k");
    assertThat(raw).isInstanceOf(CacheEntry.class);
    assertThat(((CacheEntry) raw).getValue()).isEqualTo("v");
  }

  @Test
  void putLocal_withTtl_shouldUseCustomTtl() {
    hotKeyCache.putLocal("k", "v", 10000L, 1000L);
    Object raw = caffeineCache.getIfPresent("k");
    assertThat(((CacheEntry) raw).getHardTtlMs()).isEqualTo(10000L);
    assertThat(((CacheEntry) raw).getSoftTtlMs()).isEqualTo(1000L);
  }

  @Test
  void putLocal_withBlacklistedKey_shouldThrow() {
    hotKeyCache.addBlacklist("secret");
    assertThatThrownBy(() -> hotKeyCache.putLocal("secret", "v", 0L, 0L)).isInstanceOf(ZetaBlockedException.class);
  }

  @Test
  void putLocal_withInvalidKey_shouldSkip() {
    hotKeyCache.putLocal(null, "v", 0L, 0L);
    hotKeyCache.putLocal("", "v", 0L, 0L);
    assertThat(hotKeyCache.estimatedSize()).isZero();
  }

  @Test
  void putLocal_shouldPreserveExistingMetadata() {
    caffeineCache.put(
      "k",
      CacheEntry.builder()
        .value("old")
        .dataVersion(42)
        .isVersionDegraded(false)
        .decisionVersion(7)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() + 60_000)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );

    hotKeyCache.putLocal("k", "new", 0L, 0L);

    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(entry.getValue()).isEqualTo("new");
    assertThat(entry.getDataVersion()).isEqualTo(42);
    assertThat(entry.getDecisionVersion()).isEqualTo(7);
    assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
  }

  // ── compareAndSet ──

  @Test
  void compareAndSet_shouldReplaceValueWhenMatch() {
    caffeineCache.put("k", CacheEntry.builder().value("old").build());
    assertThat(hotKeyCache.compareAndSet("k", "old", "new")).isTrue();
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(entry.getValue()).isEqualTo("new");
  }

  @Test
  void compareAndSet_shouldNotReplaceWhenMismatch() {
    caffeineCache.put("k", CacheEntry.builder().value("old").build());
    assertThat(hotKeyCache.compareAndSet("k", "wrong", "new")).isFalse();
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(entry.getValue()).isEqualTo("old");
  }

  @Test
  void compareAndSet_withAbsentKey_shouldReturnFalse() {
    assertThat(hotKeyCache.compareAndSet("absent", "old", "new")).isFalse();
  }

  @Test
  void compareAndSet_withBlacklistedKey_shouldThrow() {
    hotKeyCache.addBlacklist("block:*");
    assertThatThrownBy(() -> hotKeyCache.compareAndSet("block:k", "any", "v")).isInstanceOf(ZetaBlockedException.class);
  }

  @Test
  void compareAndSet_withInvalidKey_shouldReturnFalse() {
    assertThat(hotKeyCache.compareAndSet("", "old", "new")).isFalse();
    assertThat(hotKeyCache.compareAndSet(null, "old", "new")).isFalse();
  }

  @Test
  void compareAndSet_withNullExpected_shouldMatchNullValue() {
    caffeineCache.put("k", CacheEntry.builder().value(null).build());
    assertThat(hotKeyCache.compareAndSet("k", null, "replaced")).isTrue();
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(entry.getValue()).isEqualTo("replaced");
  }

  // ── compareAndInvalidate ──

  @Test
  void compareAndInvalidate_shouldRemoveWhenMatch() {
    caffeineCache.put("k", CacheEntry.builder().value("old").build());
    assertThat(hotKeyCache.compareAndInvalidate("k", "old")).isTrue();
    assertThat(caffeineCache.getIfPresent("k")).isNull();
  }

  @Test
  void compareAndInvalidate_shouldNotRemoveWhenMismatch() {
    caffeineCache.put("k", CacheEntry.builder().value("old").build());
    assertThat(hotKeyCache.compareAndInvalidate("k", "wrong")).isFalse();
    assertThat(caffeineCache.getIfPresent("k")).isNotNull();
  }

  @Test
  void compareAndInvalidate_withAbsentKey_shouldReturnFalse() {
    assertThat(hotKeyCache.compareAndInvalidate("absent", "old")).isFalse();
  }

  @Test
  void compareAndInvalidate_withBlacklistedKey_shouldThrow() {
    hotKeyCache.addBlacklist("block:*");
    caffeineCache.put("block:k", CacheEntry.builder().value("v").build());
    assertThatThrownBy(() -> hotKeyCache.compareAndInvalidate("block:k", "v")).isInstanceOf(ZetaBlockedException.class);
  }

  @Test
  void compareAndInvalidate_withInvalidKey_shouldReturnFalse() {
    assertThat(hotKeyCache.compareAndInvalidate("", "old")).isFalse();
    assertThat(hotKeyCache.compareAndInvalidate(null, "old")).isFalse();
  }

  // ── invalidateLocal ──

  @Test
  void invalidateLocal_shouldRemoveEntries() {
    caffeineCache.put("k1", "v1");
    caffeineCache.put("k2", "v2");
    hotKeyCache.invalidate(List.of("k1", "k2"), true);
    assertThat(caffeineCache.getIfPresent("k1")).isNull();
    assertThat(caffeineCache.getIfPresent("k2")).isNull();
  }

  @Test
  void invalidateLocal_withInvalidKeys_shouldSkipInvalid() {
    caffeineCache.put("k1", "v1");
    hotKeyCache.invalidate(Arrays.asList("k1", null, ""), true);
    assertThat(caffeineCache.getIfPresent("k1")).isNull();
  }

  @Test
  void invalidateLocal_withEmptyCollection_shouldSkip() {
    caffeineCache.put("k1", "v1");
    hotKeyCache.invalidate(List.of(), true);
    assertThat(caffeineCache.getIfPresent("k1")).isNotNull();
  }

  // ── stats ──

  @Test
  void stats_shouldReturnStats() {
    caffeineCache.put("k1", "v1");
    ZetaCacheStats stats = hotKeyCache.stats();
    assertThat(stats).isNotNull();
    assertThat(stats.estimatedSize()).isPositive();
  }

  // ── getLocalCache ──

  @Test
  void getLocalCache_shouldReturnUnderlyingCache() {
    assertThat(hotKeyCache.getLocalCache()).isSameAs(caffeineCache);
  }

  // ── invalidateAfterPut success path ──

  @Test
  void invalidateAfterPutAllOnSuccess() {
    caffeineCache.put("key1", "original");
    hotKeyCache.invalidateAfterPut("key1", () -> {}, true);
    assertThat(caffeineCache.getIfPresent("key1")).isNull();
  }

  // ── get with ALLOW_NO_REPORT ──

  @Test
  void get_withNoReportRule_shouldReturnCached() {
    hotKeyCache.addWhitelist("no-report");
    caffeineCache.put("no-report", "v");
    assertThat(hotKeyCache.get("no-report", () -> "fresh", 0L, 0L, true)).contains("v");
  }

  // ── getWithSoftExpire with TTL override ──

  @Test
  void getWithSoftExpire_withSoftTtlOverride_shouldReturnCached() {
    caffeineCache.put(
      "key",
      CacheEntry.builder()
        .value("cached")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() + 60_000)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );
    assertThat(hotKeyCache.getWithSoftExpire("key", () -> "fresh", 0L, 500L, true)).contains("cached");
  }

  // ── putThrough with TTL overrides ──

  @Test
  void putThrough_withTtlOverrides_shouldCacheWithCustomTtl() {
    hotKeyCache.putThrough("key1", "v", () -> {}, 50000L, 5000L, true);
    Object raw = caffeineCache.getIfPresent("key1");
    assertThat(raw).isInstanceOf(CacheEntry.class);
    assertThat(((CacheEntry) raw).getHardTtlMs()).isEqualTo(50000L);
    assertThat(((CacheEntry) raw).getSoftTtlMs()).isEqualTo(5000L);
  }

  // ── invalidateAllLocal(Collection) with all-invalid keys ──

  @Test
  void invalidate_All_collection_whenInvalid_shouldSkip() {
    caffeineCache.put("k1", "v1");
    hotKeyCache.invalidate(Arrays.asList(null, ""), true);
    assertThat(caffeineCache.getIfPresent("k1")).isNotNull();
  }

  // ── getWithSoftExpire with hard/soft TTL overrides ──

  @Test
  void getWithSoftExpire_withBothTtlOverrides_shouldReturnCached() {
    caffeineCache.put(
      "key",
      CacheEntry.builder()
        .value("cached")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() + 60_000)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );
    assertThat(hotKeyCache.getWithSoftExpire("key", () -> "fresh", 10000L, 500L, true)).contains("cached");
  }

  // ── broadcastAllLocalRulesManually (no publisher) ──

  @Test
  @DisplayName("broadcastAllLocalRulesManually should not throw with no publisher")
  void broadcastAllLocalRulesManually_shouldNotThrow() {
    hotKeyCache.broadcastAllLocalRulesManually();
  }

  // ── invalidateAfterPut with no existing entry ──

  @Test
  @DisplayName("invalidateAfterPut on clean key should not throw")
  void invalidateAfterPut_All_withNoExistingEntry_shouldWork() {
    hotKeyCache.invalidateAfterPut("key1", () -> {}, true);
    assertThat(caffeineCache.getIfPresent("key1")).isNull();
  }

  // ── Hot path detection and promotion ──

  @Nested
  @DisplayName("Hot path detection and promotion")
  class HotPathTest {

    private HotKeyDetector hotKeyDetector;
    private Cache<String, Object> caffeineCache;
    private SingleFlight singleFlight;
    private ExpireManager expireManager;
    private Executor executor;
    private HotKeyCache hotKeyCache;
    private CacheSyncPublisher publisher;
    private BroadcastBuffer broadcastBuffer;
    private HealthView healthView;

    @BeforeEach
    void setUp() {
      hotKeyDetector = mock(HotKeyDetector.class);
      caffeineCache = Caffeine.newBuilder().maximumSize(100).build();
      singleFlight = mock(SingleFlight.class);
      executor = Runnable::run;
      ZetaProperties ttlConfig = new ZetaProperties();
      expireManager = new ExpireManagerImpl(caffeineCache, executor, ttlConfig, 10);
      publisher = mock(CacheSyncPublisher.class);
      broadcastBuffer = new BroadcastBuffer(
        Executors.newSingleThreadScheduledExecutor(r -> {
          Thread t = new Thread(r, "zeta-send-flusher");
          t.setDaemon(true);
          return t;
        }),
        Optional.of(publisher)
      );
      healthView = mock(HealthView.class);
      KeyReporter reporter = mock(KeyReporter.class);
      hotKeyCache = new HotKeyCache(
        hotKeyDetector,
        caffeineCache,
        singleFlight,
        expireManager,
        executor,
        new CentralDispatcher(Optional.of(reporter), Optional.of(publisher), broadcastBuffer, hotKeyDetector),
        new RuleMatcherImpl(Optional.empty(), Optional.empty()),
        new VersionControllerImpl(Optional.empty(), 60),
        ttlConfig,
        healthView,
        CacheCompressor.NONE
      );
    }

    @Test
    @DisplayName("loadAndCache should promote key to HOT when detected as hot")
    void loadAndCache_shouldPromoteHotKey() {
      when(hotKeyDetector.contains("key1")).thenReturn(true);
      when(singleFlight.load(eq("key1"), any())).thenReturn(Optional.of("value"));

      Optional<String> result = hotKeyCache.get("key1", () -> "value", 0L, 0L, true);

      assertThat(result).contains("value");
      Object raw = caffeineCache.getIfPresent("key1");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      CacheEntry entry = (CacheEntry) raw;
      assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
      assertThat(entry.getValue()).isEqualTo("value");
    }

    @Test
    @DisplayName("loadAndCache should preserve Worker-managed HOT entry")
    void loadAndCache_shouldPreserveWorkerManagedEntry() {
      when(hotKeyDetector.contains("key1")).thenReturn(false);
      when(singleFlight.load(eq("key1"), any())).thenAnswer(invocation -> {
        Supplier<String> reader = invocation.getArgument(1);
        caffeineCache.put(
          "key1",
          CacheEntry.builder()
            .value("workerValue")
            .dataVersion(100)
            .isVersionDegraded(false)
            .decisionVersion(42)
            .hardTtlMs(3_600_000)
            .hardExpireAtMs(Long.MAX_VALUE)
            .softTtlMs(300_000)
            .softExpireAtMs(Long.MAX_VALUE)
            .keyState(KeyState.HOT)
            .normalHardTtlMs(300_000)
            .normalSoftTtlMs(30_000)
            .build()
        );
        return Optional.ofNullable(reader.get());
      });

      Optional<String> result = hotKeyCache.get("key1", () -> "newValue", 0L, 0L, true);

      assertThat(result).contains("newValue");
      Object raw = caffeineCache.getIfPresent("key1");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      CacheEntry entry = (CacheEntry) raw;
      assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
      assertThat(entry.getValue()).isEqualTo("workerValue");
    }

    @Test
    @DisplayName("getWithSoftExpire uses normalSoftTtlMs for COOL entry")
    void getWithSoftExpire_shouldUseCoolNormalSoftTtl() {
      caffeineCache.put(
        "key",
        CacheEntry.builder()
          .value("stale")
          .dataVersion(1)
          .isVersionDegraded(false)
          .decisionVersion(5)
          .hardTtlMs(300_000)
          .hardExpireAtMs(Long.MAX_VALUE)
          .softTtlMs(30_000)
          .softExpireAtMs(System.currentTimeMillis() - 1000)
          .keyState(KeyState.COOL)
          .normalHardTtlMs(300_000)
          .normalSoftTtlMs(5000)
          .build()
      );

      hotKeyCache.getWithSoftExpire("key", () -> "fresh", 0L, 0L, true);

      CacheEntry after = (CacheEntry) caffeineCache.getIfPresent("key");
      assertThat(after.getValue()).isEqualTo("fresh");
      assertThat(after.getSoftTtlMs()).isEqualTo(5000L);
    }

    @Test
    @DisplayName("getWithSoftExpire uses hot soft TTL override for HOT entry")
    void getWithSoftExpire_shouldUseHotSoftTtlOverride() {
      caffeineCache.put(
        "key",
        CacheEntry.builder()
          .value("stale")
          .dataVersion(1)
          .isVersionDegraded(false)
          .decisionVersion(5)
          .hardTtlMs(3_600_000)
          .hardExpireAtMs(Long.MAX_VALUE)
          .softTtlMs(300_000)
          .softExpireAtMs(System.currentTimeMillis() - 1000)
          .keyState(KeyState.HOT)
          .normalHardTtlMs(300_000)
          .normalSoftTtlMs(30_000)
          .build()
      );

      hotKeyCache.getWithSoftExpire("key", () -> "fresh", 0L, 9999L, true);

      CacheEntry after = (CacheEntry) caffeineCache.getIfPresent("key");
      assertThat(after.getValue()).isEqualTo("fresh");
      assertThat(after.getSoftTtlMs()).isEqualTo(9999L);
    }

    @Test
    @DisplayName("promoteLocalHotkeyIfNeeded should promote NORMAL to HOT")
    void promoteLocalHotkeyIfNeeded_shouldPromoteNormalToHot() {
      caffeineCache.put(
        "key1",
        CacheEntry.builder()
          .value("v")
          .dataVersion(1)
          .isVersionDegraded(false)
          .decisionVersion(0)
          .hardTtlMs(300_000)
          .hardExpireAtMs(Long.MAX_VALUE)
          .softTtlMs(30_000)
          .softExpireAtMs(System.currentTimeMillis() + 60_000)
          .keyState(KeyState.NORMAL)
          .normalHardTtlMs(300_000)
          .normalSoftTtlMs(30_000)
          .build()
      );
      when(hotKeyDetector.contains("key1")).thenReturn(true);

      hotKeyCache.get("key1", () -> "loaded", 0L, 0L, true);

      Object raw = caffeineCache.getIfPresent("key1");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      CacheEntry entry = (CacheEntry) raw;
      assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
    }

    @Test
    @DisplayName("promoteLocalHotkeyIfNeeded should promote COOL to HOT when cluster unhealthy")
    void promoteLocalHotkeyIfNeeded_shouldPromoteCoolWhenClusterUnhealthy() {
      caffeineCache.put(
        "key1",
        CacheEntry.builder()
          .value("v")
          .dataVersion(1)
          .isVersionDegraded(false)
          .decisionVersion(5)
          .hardTtlMs(300_000)
          .hardExpireAtMs(Long.MAX_VALUE)
          .softTtlMs(30_000)
          .softExpireAtMs(System.currentTimeMillis() + 60_000)
          .keyState(KeyState.COOL)
          .normalHardTtlMs(300_000)
          .normalSoftTtlMs(30_000)
          .build()
      );
      when(hotKeyDetector.contains("key1")).thenReturn(true);
      when(healthView.isClusterHealthy()).thenReturn(false);

      hotKeyCache.get("key1", () -> "loaded", 0L, 0L, true);

      Object raw = caffeineCache.getIfPresent("key1");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      CacheEntry entry = (CacheEntry) raw;
      assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
    }

    @Test
    @DisplayName("promoteLocalHotkeyIfNeeded should NOT promote COOL when cluster healthy")
    void promoteLocalHotkeyIfNeeded_shouldNotPromoteCoolWhenClusterHealthy() {
      caffeineCache.put(
        "key1",
        CacheEntry.builder()
          .value("v")
          .dataVersion(1)
          .isVersionDegraded(false)
          .decisionVersion(5)
          .hardTtlMs(300_000)
          .hardExpireAtMs(Long.MAX_VALUE)
          .softTtlMs(30_000)
          .softExpireAtMs(System.currentTimeMillis() + 60_000)
          .keyState(KeyState.COOL)
          .normalHardTtlMs(300_000)
          .normalSoftTtlMs(30_000)
          .build()
      );
      when(hotKeyDetector.contains("key1")).thenReturn(true);
      when(healthView.isClusterHealthy()).thenReturn(true);

      hotKeyCache.get("key1", () -> "loaded", 0L, 0L, true);

      Object raw = caffeineCache.getIfPresent("key1");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      CacheEntry entry = (CacheEntry) raw;
      assertThat(entry.getKeyState()).isEqualTo(KeyState.COOL);
    }

    @Test
    @DisplayName("putThrough preserves Worker-managed HOT state")
    void buildPutThroughEntry_shouldPreserveWorkerManagedState() {
      caffeineCache.put(
        "key1",
        CacheEntry.builder()
          .value("old")
          .dataVersion(1)
          .isVersionDegraded(false)
          .decisionVersion(5)
          .hardTtlMs(3_600_000)
          .hardExpireAtMs(Long.MAX_VALUE)
          .softTtlMs(300_000)
          .softExpireAtMs(Long.MAX_VALUE)
          .keyState(KeyState.HOT)
          .normalHardTtlMs(300_000)
          .normalSoftTtlMs(30_000)
          .build()
      );

      hotKeyCache.putThrough("key1", "newValue", () -> {}, 0L, 0L, true);

      Object raw = caffeineCache.getIfPresent("key1");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      CacheEntry entry = (CacheEntry) raw;
      assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
      // putThrough with degraded version (no Redis) does not overwrite healthy Worker-managed entry
      assertThat(entry.getValue()).isEqualTo("old");
    }

    @Test
    @DisplayName("invalidate should send when publisher present")
    void invalidate_shouldBroadcastWhenPublisherPresent() {
      hotKeyCache.invalidate("key1", true);

      verify(publisher).broadcastLocalInvalidate(eq("key1"), anyLong(), eq(true));
    }

    @Test
    @DisplayName("invalidateAllLocal should send when publisher present")
    void invalidateAll_shouldBroadcastWhenPublisherPresent() {
      hotKeyCache.invalidate(List.of("key1", "key2"), true);

      verify(publisher).broadcastLocalInvalidateAll(eq(List.of("key1", "key2")));
    }

    @Test
    @DisplayName("putThrough should send refresh when publisher present")
    void putThrough_shouldBroadcastWhenPublisherPresent() {
      hotKeyCache.putThrough("key1", "value", () -> {}, 0L, 0L, true);

      broadcastBuffer.flush();
      verify(publisher).broadcastRefresh(eq("key1"), anyLong(), eq(true));
    }

    @Test
    @DisplayName("invalidateAfterPut should send when publisher present")
    void putBeforeInvalidate_shouldBroadcastWhenPublisherPresent() {
      hotKeyCache.invalidateAfterPut("key1", () -> {}, true);

      verify(publisher).broadcastLocalInvalidate(eq("key1"), anyLong(), eq(true));
    }

    @Test
    @DisplayName("broadcastAllLocalRulesManually should delegate without throwing")
    void broadcastAllLocalRulesManually_shouldDelegate() {
      hotKeyCache.broadcastAllLocalRulesManually();
    }

    @Test
    @DisplayName("get with TTL overrides should use them in loadAndCache")
    void get_shouldUseTtlOverrides() {
      when(singleFlight.load(anyString(), any())).thenReturn(Optional.of("value"));
      when(hotKeyDetector.contains("key1")).thenReturn(false);

      hotKeyCache.get("key1", () -> "value", 50000L, 5000L, true);

      Object raw = caffeineCache.getIfPresent("key1");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      CacheEntry entry = (CacheEntry) raw;
      assertThat(entry.getHardTtlMs()).isEqualTo(50000L);
      assertThat(entry.getSoftTtlMs()).isEqualTo(5000L);
      assertThat(entry.getKeyState()).isEqualTo(KeyState.NORMAL);
    }

    @Test
    @DisplayName("getWithSoftExpire with COOL expired entry triggers background refresh")
    void getWithSoftExpire_coolEntry_shouldTriggerRefresh() {
      caffeineCache.put(
        "key",
        CacheEntry.builder()
          .value("stale")
          .dataVersion(1)
          .isVersionDegraded(false)
          .decisionVersion(5)
          .hardTtlMs(300_000)
          .hardExpireAtMs(Long.MAX_VALUE)
          .softTtlMs(30_000)
          .softExpireAtMs(System.currentTimeMillis() - 1000)
          .keyState(KeyState.COOL)
          .normalHardTtlMs(300_000)
          .normalSoftTtlMs(30_000)
          .build()
      );

      hotKeyCache.getWithSoftExpire("key", () -> "fresh", 0L, 0L, true);

      CacheEntry after = (CacheEntry) caffeineCache.getIfPresent("key");
      assertThat(after.getValue()).isEqualTo("fresh");
      assertThat(after.getSoftExpireAtMs()).isGreaterThan(System.currentTimeMillis() - 500);
    }

    @Test
    @DisplayName("getWithSoftExpire with COOL entry and default soft uses effective soft TTL")
    void getWithSoftExpire_coolEntryWithZeroNormalSoft_usesEffectiveSoft() {
      caffeineCache.put(
        "key",
        CacheEntry.builder()
          .value("stale")
          .dataVersion(1)
          .isVersionDegraded(false)
          .decisionVersion(5)
          .hardTtlMs(300_000)
          .hardExpireAtMs(Long.MAX_VALUE)
          .softTtlMs(0)
          .softExpireAtMs(System.currentTimeMillis() - 1000)
          .keyState(KeyState.COOL)
          .normalHardTtlMs(300_000)
          .normalSoftTtlMs(0)
          .build()
      );
      when(singleFlight.load(anyString(), any())).thenReturn(Optional.of("fresh"));

      hotKeyCache.getWithSoftExpire("key", () -> "fresh", 0L, 0L, true);

      CacheEntry after = (CacheEntry) caffeineCache.getIfPresent("key");
      assertThat(after.getValue()).isEqualTo("fresh");
    }

    @Test
    @DisplayName("get with TTL overrides and hot detection uses TTL overrides in hot path")
    void get_withTtlOverridesAndHotDetection_usesTtlOverrides() {
      when(singleFlight.load(anyString(), any())).thenReturn(Optional.of("hot-value"));
      when(hotKeyDetector.contains("key1")).thenReturn(true);

      hotKeyCache.get("key1", () -> "hot-value", 80000L, 8000L, true);

      Object raw = caffeineCache.getIfPresent("key1");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      CacheEntry entry = (CacheEntry) raw;
      assertThat(entry.getHardTtlMs()).isEqualTo(80000L);
      assertThat(entry.getSoftTtlMs()).isEqualTo(8000L);
      assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
    }

    @Test
    @DisplayName("get with ALLOW_NO_REPORT and hot detection skips report")
    void get_withAllowNoReportAndHotDetection_skipsReport() {
      hotKeyCache.addWhitelist("noreport-hot");
      when(singleFlight.load(eq("noreport-hot"), any())).thenReturn(Optional.of("value"));
      when(hotKeyDetector.contains("noreport-hot")).thenReturn(true);

      hotKeyCache.get("noreport-hot", () -> "value", 0L, 0L, true);

      Object raw = caffeineCache.getIfPresent("noreport-hot");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      assertThat(((CacheEntry) raw).getValue()).isEqualTo("value");
    }

    @Test
    @DisplayName("get with ALLOW_NO_REPORT and no hot detection skips report")
    void get_withAllowNoReportAndNoHotDetection_skipsReport() {
      hotKeyCache.addWhitelist("noreport-normal");
      when(singleFlight.load(eq("noreport-normal"), any())).thenReturn(Optional.of("value"));
      when(hotKeyDetector.contains("noreport-normal")).thenReturn(false);

      hotKeyCache.get("noreport-normal", () -> "value", 0L, 0L, true);

      Object raw = caffeineCache.getIfPresent("noreport-normal");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      assertThat(((CacheEntry) raw).getKeyState()).isEqualTo(KeyState.NORMAL);
    }

    @Test
    @DisplayName("processLocalHotkeyIfNeeded should extend HOT entry expiry when past half TTL")
    void processLocalHotkeyIfNeeded_shouldExtendHotExpiry() {
      long originalExpireAt = System.currentTimeMillis() + 5_000;
      caffeineCache.put(
        "key1",
        CacheEntry.builder()
          .value("v")
          .dataVersion(1)
          .isVersionDegraded(false)
          .decisionVersion(5)
          .hardTtlMs(60_000)
          .hardExpireAtMs(originalExpireAt)
          .softTtlMs(30_000)
          .softExpireAtMs(System.currentTimeMillis() + 60_000)
          .keyState(KeyState.HOT)
          .normalHardTtlMs(300_000)
          .normalSoftTtlMs(30_000)
          .build()
      );
      when(hotKeyDetector.contains("key1")).thenReturn(true);

      hotKeyCache.get("key1", () -> "loaded", 0L, 0L, true);

      CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key1");
      assertThat(entry.getHardExpireAtMs()).isGreaterThan(originalExpireAt);
    }

    @Test
    @DisplayName("processLocalHotkeyIfNeeded should NOT extend HOT entry when within first half")
    void processLocalHotkeyIfNeeded_shouldNotExtendWithinFirstHalf() {
      long futureExpireAt = System.currentTimeMillis() + 120_000;
      caffeineCache.put(
        "key1",
        CacheEntry.builder()
          .value("v")
          .dataVersion(1)
          .isVersionDegraded(false)
          .decisionVersion(5)
          .hardTtlMs(120_000)
          .hardExpireAtMs(futureExpireAt)
          .softTtlMs(30_000)
          .softExpireAtMs(System.currentTimeMillis() + 60_000)
          .keyState(KeyState.HOT)
          .normalHardTtlMs(300_000)
          .normalSoftTtlMs(30_000)
          .build()
      );
      when(hotKeyDetector.contains("key1")).thenReturn(true);

      hotKeyCache.get("key1", () -> "loaded", 0L, 0L, true);

      CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key1");
      assertThat(entry.getHardExpireAtMs()).isEqualTo(futureExpireAt);
    }

    @Test
    @DisplayName("processLocalHotkeyIfNeeded should NOT extend HOT entry with MAX_VALUE hardExpireAt")
    void processLocalHotkeyIfNeeded_shouldNotExtendMaxValueExpiry() {
      caffeineCache.put(
        "key1",
        CacheEntry.builder()
          .value("v")
          .dataVersion(1)
          .isVersionDegraded(false)
          .decisionVersion(5)
          .hardTtlMs(60_000)
          .hardExpireAtMs(Long.MAX_VALUE)
          .softTtlMs(30_000)
          .softExpireAtMs(System.currentTimeMillis() + 60_000)
          .keyState(KeyState.HOT)
          .normalHardTtlMs(300_000)
          .normalSoftTtlMs(30_000)
          .build()
      );
      when(hotKeyDetector.contains("key1")).thenReturn(true);

      hotKeyCache.get("key1", () -> "loaded", 0L, 0L, true);

      CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key1");
      assertThat(entry.getHardExpireAtMs()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    @DisplayName("loadAndCache preserves Worker-managed entry in hot path")
    void loadAndCache_withWorkerManagedEntryInHotPath_preservesIt() {
      when(hotKeyDetector.contains("key1")).thenReturn(true);
      when(singleFlight.load(eq("key1"), any())).thenAnswer(invocation -> {
        Supplier<String> reader = invocation.getArgument(1);
        caffeineCache.put(
          "key1",
          CacheEntry.builder()
            .value("workerValue")
            .dataVersion(100)
            .isVersionDegraded(false)
            .decisionVersion(42)
            .hardTtlMs(3_600_000)
            .hardExpireAtMs(Long.MAX_VALUE)
            .softTtlMs(300_000)
            .softExpireAtMs(Long.MAX_VALUE)
            .keyState(KeyState.HOT)
            .normalHardTtlMs(300_000)
            .normalSoftTtlMs(30_000)
            .build()
        );
        return Optional.ofNullable(reader.get());
      });

      Optional<String> result = hotKeyCache.get("key1", () -> "newValue", 0L, 0L, true);

      assertThat(result).contains("newValue");
      Object raw = caffeineCache.getIfPresent("key1");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      CacheEntry entry = (CacheEntry) raw;
      assertThat(entry.getValue()).isEqualTo("workerValue");
    }
  }

  // ── putThrough with existing CacheEntry: buildPutThroughEntry deeper branches ──

  @Test
  void putThrough_withExistingEntry_shouldPreserveDecisionMetadata() {
    caffeineCache.put(
      "key1",
      CacheEntry.builder()
        .value("oldValue")
        .dataVersion(Long.MIN_VALUE)
        .isVersionDegraded(true)
        .decisionVersion(42)
        .decisionNodeId("worker-1")
        .decisionEpoch(7)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() + 60_000)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );

    hotKeyCache.putThrough("key1", "newValue", () -> {}, 0L, 0L, true);

    Object raw = caffeineCache.getIfPresent("key1");
    assertThat(raw).isInstanceOf(CacheEntry.class);
    CacheEntry entry = (CacheEntry) raw;
    assertThat(entry.getValue()).isEqualTo("newValue");
    assertThat(entry.getDecisionVersion()).isEqualTo(42);
    assertThat(entry.getDecisionNodeId()).isEqualTo("worker-1");
    assertThat(entry.getDecisionEpoch()).isEqualTo(7);
  }

  @Test
  void putThrough_withWorkerManagedEntry_shouldPreserveNormalTtls() {
    caffeineCache.put(
      "key1",
      CacheEntry.builder()
        .value("old")
        .dataVersion(Long.MIN_VALUE)
        .isVersionDegraded(true)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() + 60_000)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(777_777)
        .normalSoftTtlMs(77_777)
        .build()
    );

    hotKeyCache.putThrough("key1", "newValue", () -> {}, 50000L, 5000L, true);

    Object raw = caffeineCache.getIfPresent("key1");
    assertThat(raw).isInstanceOf(CacheEntry.class);
    CacheEntry entry = (CacheEntry) raw;
    assertThat(entry.getNormalHardTtlMs()).isEqualTo(777_777);
    assertThat(entry.getNormalSoftTtlMs()).isEqualTo(77_777);
  }

  @Test
  void putThrough_withExistingEntryAndHigherDataVersion_shouldSkip() {
    caffeineCache.put(
      "key1",
      CacheEntry.builder()
        .value("original")
        .dataVersion(Long.MAX_VALUE)
        .isVersionDegraded(true)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() + 60_000)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );

    hotKeyCache.putThrough("key1", "newValue", () -> {}, 0L, 0L, true);

    Object raw = caffeineCache.getIfPresent("key1");
    assertThat(raw).isInstanceOf(CacheEntry.class);
    CacheEntry entry = (CacheEntry) raw;
    assertThat(entry.getValue()).isEqualTo("original");
  }
}
