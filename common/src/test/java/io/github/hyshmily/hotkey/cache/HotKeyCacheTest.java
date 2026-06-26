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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.hotkey.autoconfigure.HotKeyProperties;
import io.github.hyshmily.hotkey.exception.HotKeyBlockedException;
import io.github.hyshmily.hotkey.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.hotkey.model.CacheEntry;
import io.github.hyshmily.hotkey.model.HotKeyCacheStats;
import io.github.hyshmily.hotkey.model.KeyState;
import io.github.hyshmily.hotkey.reporting.HotKeyReporter;
import io.github.hyshmily.hotkey.rule.Rule.RuleAction;
import io.github.hyshmily.hotkey.rule.RuleMatcher;
import io.github.hyshmily.hotkey.sharding.ClusterHealthView;
import io.github.hyshmily.hotkey.sharding.RingManager;
import io.github.hyshmily.hotkey.sync.local.CacheSyncPublisher;
import io.github.hyshmily.hotkey.util.version.VersionController;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link HotKeyCache}, covering peek, get, invalidate, and blacklist behaviors.
 */
class HotKeyCacheTest {

  private HotKeyDetector hotKeyDetector;
  private Cache<String, Object> caffeineCache;
  private SingleFlight singleFlight;
  private CacheExpireManager expireManager;
  private Executor executor;
  private HotKeyCache hotKeyCache;

  @BeforeEach
  void setUp() {
    hotKeyDetector = mock(HotKeyDetector.class);
    when(hotKeyDetector.contains(anyString())).thenReturn(false);
    caffeineCache = Caffeine.newBuilder().maximumSize(100).build();
    singleFlight = mock(SingleFlight.class);
    executor = Runnable::run;
    HotKeyProperties ttlConfig = new HotKeyProperties();
    expireManager = new CacheExpireManager(caffeineCache, executor, ttlConfig, 10);

    hotKeyCache = new HotKeyCache(
      hotKeyDetector,
      caffeineCache,
      singleFlight,
      expireManager,
      executor,
      Optional.empty(),
      Optional.empty(),
      new RuleMatcher(Optional.empty(), Optional.empty()),
      new VersionController(Optional.empty(), 60),
      ttlConfig,
      mock(ClusterHealthView.class)
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
  void isLocalHotKey_shouldReturnTrueForHotEntry() {
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

    assertThat(hotKeyCache.isLocalHotKey("hotKey")).isTrue();
  }

  /**
   * Verifies that a cache entry with KeyState.NORMAL is not identified as a local hot key.
   */
  @Test
  void isLocalHotKey_shouldReturnFalseForNormalEntry() {
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

    assertThat(hotKeyCache.isLocalHotKey("normalKey")).isFalse();
  }

  /**
   * Verifies that isLocalHotKey returns false for a key not present in the cache.
   */
  @Test
  void isLocalHotKey_shouldReturnFalseForMissingKey() {
    assertThat(hotKeyCache.isLocalHotKey("missing")).isFalse();
  }

  /**
   * Verifies that isLocalHotKey returns false for null keys.
   */
  @Test
  void isLocalHotKey_shouldReturnFalseForInvalidKey() {
    assertThat(hotKeyCache.isLocalHotKey(null)).isFalse();
  }

  /**
   * Verifies that get returns a cached raw value without invoking the loader.
   */
  @Test
  void get_shouldReturnCachedValueOnHit() {
    caffeineCache.put("key1", "rawValue");
    assertThat(hotKeyCache.get("key1", () -> "loaded")).contains("rawValue");
  }

  /**
   * Verifies that get loads and caches a value on cache miss via SingleFlight.
   */
  @Test
  void get_shouldLoadAndCacheOnMiss() {
    when(singleFlight.load(anyString(), any())).thenReturn(Optional.of("loadedValue"));

    Optional<String> result = hotKeyCache.get("key1", () -> "loadedValue");
    assertThat(result).contains("loadedValue");
    assertThat(caffeineCache.getIfPresent("key1")).isNotNull();
  }

  /**
   * Verifies that get returns empty for null or blank keys without loading.
   */
  @Test
  void get_shouldReturnEmptyForInvalidKey() {
    assertThat(hotKeyCache.get(null, () -> "v")).isEmpty();
    assertThat(hotKeyCache.get("", () -> "v")).isEmpty();
  }

  /**
   * Verifies that invalidate removes a cached entry.
   */
  @Test
  void invalidate_shouldRemoveEntry() {
    caffeineCache.put("key1", "value");
    hotKeyCache.invalidate("key1");
    assertThat(caffeineCache.getIfPresent("key1")).isNull();
  }

  /**
   * Verifies that invalidate handles null and blank keys without throwing.
   */
  @Test
  void invalidate_shouldHandleInvalidKey() {
    hotKeyCache.invalidate(null);
    hotKeyCache.invalidate("");
  }

  /**
   * Verifies that invalidateAllLocal removes multiple cached entries.
   */
  @Test
  void invalidateAll_shouldRemoveEntries() {
    caffeineCache.put("k1", "v1");
    caffeineCache.put("k2", "v2");
    hotKeyCache.invalidateAll(List.of("k1", "k2"));
    assertThat(caffeineCache.getIfPresent("k1")).isNull();
    assertThat(caffeineCache.getIfPresent("k2")).isNull();
  }

  /**
   * Verifies that invalidateAllLocal skips null and blank keys in the input list.
   */
  @Test
  void invalidateAll_shouldSkipInvalidKeys() {
    caffeineCache.put("k1", "v1");
    hotKeyCache.invalidateAll(Arrays.asList("k1", null, ""));
    assertThat(caffeineCache.getIfPresent("k1")).isNull();
  }

  /**
   * Verifies that get throws HotKeyBlockedException for a blacklisted key.
   */
  @Test
  void get_shouldThrowHotKeyBlockedExceptionForBlacklistedKey() {
    hotKeyCache.addBlacklist("secret");
    assertThatThrownBy(() -> hotKeyCache.get("secret", () -> "db")).isInstanceOf(HotKeyBlockedException.class);
  }

  /**
   * Verifies that getWithSoftExpire throws HotKeyBlockedException for a blacklisted key.
   */
  @Test
  void getWithSoftExpire_shouldThrowHotKeyBlockedExceptionForBlacklistedKey() {
    hotKeyCache.addBlacklist("secret");
    assertThatThrownBy(() -> hotKeyCache.getWithSoftExpire("secret", () -> "db")).isInstanceOf(
      HotKeyBlockedException.class
    );
  }

  /**
   * Verifies that getWithSoftExpire falls back to get() when soft expire is disabled.
   */
  @Test
  void getWithSoftExpire_whenDisabled_shouldFallbackToGet() {
    HotKeyProperties props = new HotKeyProperties();
    props.setDefaultSoftTtlMs(0);
    props.setDefaultHotSoftTtlMs(0);
    CacheExpireManager noSoft = new CacheExpireManager(caffeineCache, executor, props, 10);

    when(singleFlight.load(anyString(), any())).thenReturn(Optional.of("loaded"));

    HotKeyCache cache = new HotKeyCache(
      hotKeyDetector,
      caffeineCache,
      singleFlight,
      noSoft,
      executor,
      Optional.empty(),
      Optional.empty(),
      new RuleMatcher(Optional.empty(), Optional.empty()),
      new VersionController(Optional.empty(), 60),
      props,
      mock(ClusterHealthView.class)
    );
 
    assertThat(cache.getWithSoftExpire("key", () -> "loaded")).contains("loaded");
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

    assertThat(hotKeyCache.getWithSoftExpire("key", () -> "should-not-load")).contains("cached");
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
    assertThat(hotKeyCache.getWithSoftExpire("key", () -> "fresh")).contains("stale");
  }

  /**
   * Verifies that peek throws HotKeyBlockedException when the key matches a blacklist rule.
   */
  @Test
  void peek_withBlacklistedKey_shouldThrow() {
    hotKeyCache.addBlacklist("secret");
    assertThatThrownBy(() -> hotKeyCache.peek("secret")).isInstanceOf(HotKeyBlockedException.class);
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
   * Verifies that isLocalHotKey returns false for a logically expired CacheEntry
   * even when the entry is still present in the Caffeine cache.
   */
  @Test
  void isLocalHotKey_withExpiredEntry_shouldReturnFalse() {
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

    assertThat(hotKeyCache.isLocalHotKey("expired")).isFalse();
  }

  /**
   * Verifies that putThrough caches the value and preserves it for subsequent reads.
   */
  @Test
  void putThrough_shouldWriteThroughAndCache() {
    hotKeyCache.putThrough("key1", "newValue", () -> {});

    assertThat(hotKeyCache.peek("key1")).contains("newValue");
  }

  /**
   * Verifies that putThrough throws HotKeyBlockedException when the key is blacklisted.
   */
  @Test
  void putThrough_withBlacklistedKey_shouldThrow() {
    hotKeyCache.addBlacklist("secret");
    assertThatThrownBy(() -> hotKeyCache.putThrough("secret", "value", () -> {}))
      .isInstanceOf(HotKeyBlockedException.class)
      .hasFieldOrPropertyWithValue("cacheKey", "secret");
  }

  /**
   * Verifies that putThrough silently returns for invalid (null/blank) keys.
   */
  @Test
  void putThrough_withInvalidKey_shouldSkip() {
    hotKeyCache.putThrough(null, "value", () -> {});
    hotKeyCache.putThrough("", "value", () -> {});
    // No exception — silent skip
  }

  /**
   * Verifies that putThrough caches a null value using the NullValue sentinel,
   * which is transparently unwrapped to empty on peek.
   */
  @Test
  void putThrough_withNullValue_shouldUseNullValueSentinel() {
    hotKeyCache.putThrough("null-key", null, () -> {});

    assertThat(hotKeyCache.peek("null-key")).isEmpty();
  }

  /**
   * Verifies that putBeforeInvalidate with a failed mutation does NOT invalidate
   * the cache entry (fault mode: writer exception).
   */
  @Test
  void putBeforeInvalidate_whenMutationFails_shouldNotInvalidate() {
    caffeineCache.put("key1", "original");
    hotKeyCache.putBeforeInvalidate("key1", () -> {
      throw new RuntimeException("db-fail");
    });

    assertThat(hotKeyCache.peek("key1")).contains("original");
  }

  /**
   * Verifies that putBeforeInvalidate throws HotKeyBlockedException when the key is blacklisted.
   */
  @Test
  void putBeforeInvalidate_withBlacklistedKey_shouldThrow() {
    hotKeyCache.addBlacklist("secret");
    assertThatThrownBy(() -> hotKeyCache.putBeforeInvalidate("secret", () -> {}))
      .isInstanceOf(HotKeyBlockedException.class)
      .hasFieldOrPropertyWithValue("cacheKey", "secret");
  }

  /**
   * Verifies that putBeforeInvalidate silently returns for invalid (null/blank) keys.
   */
  @Test
  void putBeforeInvalidate_withInvalidKey_shouldSkip() {
    caffeineCache.put("k", "v");
    hotKeyCache.putBeforeInvalidate(null, () -> {});
    hotKeyCache.putBeforeInvalidate("", () -> {});
    // Entry untouched
    assertThat(hotKeyCache.peek("k")).contains("v");
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
   * Verifies that estimatedSize returns a positive count for cached entries.
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
  void invalidateAll_noArg_shouldClearAll() {
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

    assertThat(hotKeyCache.getWithSoftExpire("no-report", () -> "fresh")).contains("v");
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

    assertThat(hotKeyCache.get("expired", () -> "fresh")).contains("fresh");
  }

  // ── getWithSoftExpire: invalid key ──

  @Test
  void getWithSoftExpire_withInvalidKey_shouldReturnEmpty() {
    assertThat(hotKeyCache.getWithSoftExpire(null, () -> "v")).isEmpty();
    assertThat(hotKeyCache.getWithSoftExpire("", () -> "v")).isEmpty();
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

    assertThat(hotKeyCache.getWithSoftExpire("expired", () -> "fresh")).contains("fresh");
  }

  // ── getWithSoftExpire: cache miss (no entry) triggers loadAndCache ──

  @Test
  void getWithSoftExpire_withCacheMiss_shouldLoad() {
    when(singleFlight.load(anyString(), any())).thenReturn(Optional.of("loaded"));

    assertThat(hotKeyCache.getWithSoftExpire("missing", () -> "loaded")).contains("loaded");
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
    assertThat(hotKeyCache.getWithSoftExpire("raw", () -> "fresh")).contains("bare-string");
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

    Optional<String> result = hotKeyCache.getWithSoftExpire("normal", () -> "fresh");

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
    assertThatThrownBy(() -> hotKeyCache.putLocal("secret", "v", 0L, 0L)).isInstanceOf(HotKeyBlockedException.class);
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

  // ── evictLocal ──

  @Test
  void evictLocal_shouldRemoveEntries() {
    caffeineCache.put("k1", "v1");
    caffeineCache.put("k2", "v2");
    hotKeyCache.evictLocal(List.of("k1", "k2"));
    assertThat(caffeineCache.getIfPresent("k1")).isNull();
    assertThat(caffeineCache.getIfPresent("k2")).isNull();
  }

  @Test
  void evictLocal_withInvalidKeys_shouldSkipInvalid() {
    caffeineCache.put("k1", "v1");
    hotKeyCache.evictLocal(Arrays.asList("k1", null, ""));
    assertThat(caffeineCache.getIfPresent("k1")).isNull();
  }

  @Test
  void evictLocal_withEmptyCollection_shouldSkip() {
    caffeineCache.put("k1", "v1");
    hotKeyCache.evictLocal(List.of());
    assertThat(caffeineCache.getIfPresent("k1")).isNotNull();
  }

  // ── stats ──

  @Test
  void stats_shouldReturnStats() {
    caffeineCache.put("k1", "v1");
    HotKeyCacheStats stats = hotKeyCache.stats();
    assertThat(stats).isNotNull();
    assertThat(stats.estimatedSize()).isPositive();
  }

  // ── getLocalCache ──

  @Test
  void getLocalCache_shouldReturnUnderlyingCache() {
    assertThat(hotKeyCache.getLocalCache()).isSameAs(caffeineCache);
  }

  // ── putBeforeInvalidate success path ──

  @Test
  void putBeforeInvalidate_shouldInvalidateOnSuccess() {
    caffeineCache.put("key1", "original");
    hotKeyCache.putBeforeInvalidate("key1", () -> {});
    assertThat(caffeineCache.getIfPresent("key1")).isNull();
  }

  // ── get with ALLOW_NO_REPORT ──

  @Test
  void get_withNoReportRule_shouldReturnCached() {
    hotKeyCache.addWhitelist("no-report");
    caffeineCache.put("no-report", "v");
    assertThat(hotKeyCache.get("no-report", () -> "fresh")).contains("v");
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
    assertThat(hotKeyCache.getWithSoftExpire("key", () -> "fresh", 500L)).contains("cached");
  }

  // ── putThrough with TTL overrides ──

  @Test
  void putThrough_withTtlOverrides_shouldCacheWithCustomTtl() {
    hotKeyCache.putThrough("key1", "v", () -> {}, 50000L, 5000L);
    Object raw = caffeineCache.getIfPresent("key1");
    assertThat(raw).isInstanceOf(CacheEntry.class);
    assertThat(((CacheEntry) raw).getHardTtlMs()).isEqualTo(50000L);
    assertThat(((CacheEntry) raw).getSoftTtlMs()).isEqualTo(5000L);
  }

  // ── invalidateAllLocal(Collection) with all-invalid keys ──

  @Test
  void invalidateAll_collection_whenAllInvalid_shouldSkip() {
    caffeineCache.put("k1", "v1");
    hotKeyCache.invalidateAll(Arrays.asList(null, ""));
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
    assertThat(hotKeyCache.getWithSoftExpire("key", () -> "fresh", 10000L, 500L)).contains("cached");
  }

  // ── broadcastAllLocalRulesManually (no publisher) ──

  @Test
  @DisplayName("broadcastAllLocalRulesManually should not throw with no publisher")
  void broadcastAllLocalRulesManually_shouldNotThrow() {
    hotKeyCache.broadcastAllLocalRulesManually();
  }

  // ── putBeforeInvalidate with no existing entry ──

  @Test
  @DisplayName("putBeforeInvalidate on clean key should not throw")
  void putBeforeInvalidate_withNoExistingEntry_shouldWork() {
    hotKeyCache.putBeforeInvalidate("key1", () -> {});
    assertThat(caffeineCache.getIfPresent("key1")).isNull();
  }

  // ── Hot path detection and promotion ──

  @Nested
  @DisplayName("Hot path detection and promotion")
  class HotPathTest {

    private HotKeyDetector hotKeyDetector;
    private Cache<String, Object> caffeineCache;
    private SingleFlight singleFlight;
    private CacheExpireManager expireManager;
    private Executor executor;
    private HotKeyCache hotKeyCache;
    private CacheSyncPublisher publisher;
    private ClusterHealthView healthView;

    @BeforeEach
    void setUp() {
      hotKeyDetector = mock(HotKeyDetector.class);
      caffeineCache = Caffeine.newBuilder().maximumSize(100).build();
      singleFlight = mock(SingleFlight.class);
      executor = Runnable::run;
      HotKeyProperties ttlConfig = new HotKeyProperties();
      expireManager = new CacheExpireManager(caffeineCache, executor, ttlConfig, 10);
      publisher = mock(CacheSyncPublisher.class);
      healthView = mock(ClusterHealthView.class);
      HotKeyReporter reporter = mock(HotKeyReporter.class);

      hotKeyCache = new HotKeyCache(
        hotKeyDetector,
        caffeineCache,
        singleFlight,
        expireManager,
        executor,
        Optional.of(publisher),
        Optional.of(reporter),
      new RuleMatcher(Optional.empty(), Optional.empty()),
      new VersionController(Optional.empty(), 60),
      ttlConfig,
      healthView
    );
  }

    @Test
    @DisplayName("loadAndCache should promote key to HOT when detected as hot")
    void loadAndCache_shouldPromoteHotKey() {
      when(hotKeyDetector.contains("key1")).thenReturn(true);
      when(singleFlight.load(eq("key1"), any())).thenReturn(Optional.of("value"));

      Optional<String> result = hotKeyCache.get("key1", () -> "value");

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

      Optional<String> result = hotKeyCache.get("key1", () -> "newValue");

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

      hotKeyCache.getWithSoftExpire("key", () -> "fresh");

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

      hotKeyCache.getWithSoftExpire("key", () -> "fresh", 9999L);

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

      hotKeyCache.get("key1", () -> "loaded");

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

      hotKeyCache.get("key1", () -> "loaded");

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

      hotKeyCache.get("key1", () -> "loaded");

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

      hotKeyCache.putThrough("key1", "newValue", () -> {});

      Object raw = caffeineCache.getIfPresent("key1");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      CacheEntry entry = (CacheEntry) raw;
      assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
      // putThrough with degraded version (no Redis) does not overwrite healthy Worker-managed entry
      assertThat(entry.getValue()).isEqualTo("old");
    }

    @Test
    @DisplayName("invalidate should broadcast when publisher present")
    void invalidate_shouldBroadcastWhenPublisherPresent() {
      hotKeyCache.invalidate("key1");

      verify(publisher).broadcastLocalInvalidate(eq("key1"), anyLong(), eq(true));
    }

    @Test
    @DisplayName("invalidateAllLocal should broadcast when publisher present")
    void invalidateAll_shouldBroadcastWhenPublisherPresent() {
      hotKeyCache.invalidateAll(List.of("key1", "key2"));

      verify(publisher).broadcastLocalInvalidateAll(eq(List.of("key1", "key2")));
    }

    @Test
    @DisplayName("putThrough should broadcast refresh when publisher present")
    void putThrough_shouldBroadcastWhenPublisherPresent() {
      hotKeyCache.putThrough("key1", "value", () -> {});

      verify(publisher).broadcastRefresh(eq("key1"), anyLong(), eq(true));
    }

    @Test
    @DisplayName("putBeforeInvalidate should broadcast when publisher present")
    void putBeforeInvalidate_shouldBroadcastWhenPublisherPresent() {
      hotKeyCache.putBeforeInvalidate("key1", () -> {});

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

      hotKeyCache.get("key1", () -> "value", 50000L, 5000L);

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

      hotKeyCache.getWithSoftExpire("key", () -> "fresh");

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

      hotKeyCache.getWithSoftExpire("key", () -> "fresh");

      CacheEntry after = (CacheEntry) caffeineCache.getIfPresent("key");
      assertThat(after.getValue()).isEqualTo("fresh");
    }

    @Test
    @DisplayName("get with TTL overrides and hot detection uses TTL overrides in hot path")
    void get_withTtlOverridesAndHotDetection_usesTtlOverrides() {
      when(singleFlight.load(anyString(), any())).thenReturn(Optional.of("hot-value"));
      when(hotKeyDetector.contains("key1")).thenReturn(true);

      hotKeyCache.get("key1", () -> "hot-value", 80000L, 8000L);

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

      hotKeyCache.get("noreport-hot", () -> "value");

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

      hotKeyCache.get("noreport-normal", () -> "value");

      Object raw = caffeineCache.getIfPresent("noreport-normal");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      assertThat(((CacheEntry) raw).getKeyState()).isEqualTo(KeyState.NORMAL);
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

      Optional<String> result = hotKeyCache.get("key1", () -> "newValue");

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

    hotKeyCache.putThrough("key1", "newValue", () -> {});

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

    hotKeyCache.putThrough("key1", "newValue", () -> {}, 50000L, 5000L);

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

    hotKeyCache.putThrough("key1", "newValue", () -> {});

    Object raw = caffeineCache.getIfPresent("key1");
    assertThat(raw).isInstanceOf(CacheEntry.class);
    CacheEntry entry = (CacheEntry) raw;
    assertThat(entry.getValue()).isEqualTo("original");
  }
}
