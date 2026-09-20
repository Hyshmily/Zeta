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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.annotation.annotationsupporter.NullValue;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.BroadcastBuffer;
import io.github.hyshmily.zeta.cache.cachesupport.CircuitBreaker;
import io.github.hyshmily.zeta.cache.cachesupport.ExpireManager;
import io.github.hyshmily.zeta.cache.cachesupport.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.impl.ExpireManagerImpl;
import io.github.hyshmily.zeta.cache.cachesupport.impl.SingleFlightImpl;
import io.github.hyshmily.zeta.cache.codec.CacheCompressor;
import io.github.hyshmily.zeta.exception.ZetaBlockedException;
import io.github.hyshmily.zeta.exception.ZetaExceptionHandler;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.CachePolicy;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.model.StalePolicy;
import io.github.hyshmily.zeta.model.VersionedValue;
import io.github.hyshmily.zeta.model.ZetaCacheStats;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.rule.Rule.RuleAction;
import io.github.hyshmily.zeta.rule.impl.RuleMatcherImpl;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import io.github.hyshmily.zeta.util.version.VersionController;
import io.github.hyshmily.zeta.util.version.impl.VersionControllerImpl;
import io.github.hyshmily.zeta.model.EntryDraft;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link HotKeyCache}, covering peek, get, invalidate, and blacklist behaviors.
 */
class ZetaCacheTest {

  private final SnowflakeIdGenerator snowflakeIdGenerator = new SnowflakeIdGenerator(0, 1);

  private HotKeyDetector hotKeyDetector;
  private Cache<String, Object> caffeineCache;
  private SingleFlight singleFlight;
  private ExpireManager expireManager;
  private Executor executor;
  private HotKeyCache hotKeyCache;
  private ScheduledExecutorService scheduler;
  private HealthView healthView;
  private ZetaProperties ttlConfig;

  @BeforeEach
  void setUp() {
    hotKeyDetector = mock(HotKeyDetector.class);
    when(hotKeyDetector.contains(anyString())).thenReturn(false);
    caffeineCache = Caffeine.newBuilder().maximumSize(100).build();
    singleFlight = mock(SingleFlight.class);
    executor = Runnable::run;
    ttlConfig = new ZetaProperties();
    healthView = mock(HealthView.class);
    expireManager = new ExpireManagerImpl(caffeineCache, executor, ttlConfig, 10, CacheCompressor.NONE, healthView);
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
      new VersionControllerImpl(Optional.empty(), 60, snowflakeIdGenerator),
      ttlConfig,
      healthView,
      CacheCompressor.NONE
    );
  }

  /**
   * SingleFlight stub carrier for the ADR-0033 composite-load contract: the
   * supplier handed to {@code singleFlight.load} returns a {@link VersionedValue}.
   * Unstamped (probe withheld) so tests keep exercising the legacy version
   * handling. Typed as {@code Optional<Object>} so the {@code any()} matcher's
   * {@code T=Object} inference accepts it.
   */
  @SuppressWarnings("all")
  private static Optional<Object> vv(String value) {
    return (Optional) Optional.of(new VersionedValue(value, 0L, false));
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
   * Decision-Validity demotion end-to-end (ADR-0035): a HOT entry stamped by a
   * dead Worker is served on the first read, and the entry is reverted in place
   * to NORMAL with the decision stamp cleared and normal TTLs — no reload, no
   * miss, no source dependency.
   */
  @Test
  void get_shouldDemoteOrphanedWorkerHotEntryInPlace() {
    when(healthView.isAlive("w1")).thenReturn(false);
    caffeineCache.put("key1", workerHotEntry("w1", 5L));

    Optional<String> result = hotKeyCache.get(
      "key1",
      CachePolicy.of(() -> "loaded", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH)
    );

    assertThat(result).contains("stored");
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key1");
    assertThat(entry.getKeyState()).isEqualTo(KeyState.NORMAL);
    assertThat(entry.getDecisionNodeId()).isNull();
    assertThat(entry.getDecisionVersion()).isZero();
    assertThat(entry.getDecisionEpoch()).isZero();
    assertThat(entry.getHardTtlMs()).isEqualTo(60_000);
    assertThat(entry.getValue()).isEqualTo("stored");
  }

  /**
   * Decision-Validity control (ADR-0035): a HOT entry stamped by a live Worker
   * with a matching epoch is untouched by the read path.
   */
  @Test
  void get_shouldKeepLiveWorkerHotEntryUnchanged() {
    when(healthView.isAlive("w1")).thenReturn(true);
    when(healthView.epochOf("w1")).thenReturn(5L);
    caffeineCache.put("key1", workerHotEntry("w1", 5L));

    Optional<String> result = hotKeyCache.get(
      "key1",
      CachePolicy.of(() -> "loaded", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH)
    );

    assertThat(result).contains("stored");
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key1");
    assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(entry.getDecisionNodeId()).isEqualTo("w1");
    assertThat(entry.getHardTtlMs()).isEqualTo(3_600_000);
  }

  /** Build a Worker-stamped HOT entry with a 1h HOT TTL and 60s/15s normal baseline. */
  private static CacheEntry workerHotEntry(String nodeId, long epoch) {
    return CacheEntry.builder()
      .value("stored")
      .dataVersion(1)
      .isVersionDegraded(false)
      .decisionVersion(42)
      .decisionNodeId(nodeId)
      .decisionEpoch(epoch)
      .hardTtlMs(3_600_000)
      .hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(300_000)
      .softExpireAtMs(Long.MAX_VALUE)
      .keyState(KeyState.HOT)
      .normalHardTtlMs(60_000)
      .normalSoftTtlMs(15_000)
      .build();
  }

  /**
   * Decision-Validity demotion on the atomic read-through path (ADR-0035):
   * {@code computeIfAbsent} serves an orphaned Worker-HOT entry and reverts it
   * in place to NORMAL — the compute-in-lock hit branch must not leave the
   * orphaned stamp behind.
   */
  @Test
  void computeIfAbsent_shouldDemoteOrphanedWorkerHotEntryInPlace() {
    when(healthView.isAlive("w1")).thenReturn(false);
    caffeineCache.put("key1", workerHotEntry("w1", 5L));

    Optional<String> result = hotKeyCache.computeIfAbsent(
      "key1",
      CachePolicy.of(() -> "loaded", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH)
    );

    assertThat(result).contains("stored");
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key1");
    assertThat(entry.getKeyState()).isEqualTo(KeyState.NORMAL);
    assertThat(entry.getDecisionNodeId()).isNull();
    assertThat(entry.getDecisionVersion()).isZero();
    assertThat(entry.getHardTtlMs()).isEqualTo(60_000);
  }

  /**
   * Decision-Validity control on the atomic read-through path: a live Worker's
   * HOT entry is untouched by {@code computeIfAbsent}.
   */
  @Test
  void computeIfAbsent_shouldKeepLiveWorkerHotEntryUnchanged() {
    when(healthView.isAlive("w1")).thenReturn(true);
    when(healthView.epochOf("w1")).thenReturn(5L);
    caffeineCache.put("key1", workerHotEntry("w1", 5L));

    Optional<String> result = hotKeyCache.computeIfAbsent(
      "key1",
      CachePolicy.of(() -> "loaded", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH)
    );

    assertThat(result).contains("stored");
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key1");
    assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(entry.getDecisionNodeId()).isEqualTo("w1");
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
    assertThat(
      hotKeyCache.get("key1", CachePolicy.of(() -> "loaded", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))
    ).contains("rawValue");
  }

  /**
   * Verifies that get loads and caches a value on cache miss via SingleFlight.
   */
  @Test
  void get_shouldLoadAndCacheOnMiss() {
    when(singleFlight.load(anyString(), any())).thenReturn(vv("loadedValue"));

    Optional<String> result = hotKeyCache.get(
      "key1",
      CachePolicy.of(() -> "loadedValue", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH)
    );
    assertThat(result).contains("loadedValue");
    assertThat(caffeineCache.getIfPresent("key1")).isNotNull();
  }

  /**
   * Lazy TTL contract (ADR-0030 / {@link CachePolicy}): a plain NORMAL-entry
   * hit must never evaluate the TTL suppliers. The previous eager resolution
   * at the {@code get} entry ran the (possibly SpEL-backed) suppliers on every
   * hit; the read path now resolves them only when an entry is created,
   * promoted, or refreshed.
   */
  @Test
  void get_shouldNotEvaluateTtlSuppliersOnPlainNORMALHit() {
    AtomicInteger hardEvals = new AtomicInteger();
    AtomicInteger softEvals = new AtomicInteger();
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
        .softExpireAtMs(Long.MAX_VALUE)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );
    when(hotKeyDetector.contains("key1")).thenReturn(false);

    Optional<String> result = hotKeyCache.get(
      "key1",
      new CachePolicy(
        hardEvals::incrementAndGet,
        softEvals::incrementAndGet,
        true,
        false,
        StalePolicy.SOFT_REFRESH,
        () -> "loaded",
        true,
        false
      )
    );

    assertThat(result).contains("stored");
    assertThat(hardEvals.get()).isZero();
    assertThat(softEvals.get()).isZero();
  }

  /**
   * At-most-once TTL evaluation on the load path (the {@link CachePolicy}
   * contract): a miss that builds an entry consumes both overrides exactly
   * once, even though the effective and hot-TTL resolutions derive from the
   * same raw override.
   */
  @Test
  void get_shouldEvaluateTtlSuppliersAtMostOnceOnMiss() {
    when(singleFlight.load(anyString(), any())).thenReturn(vv("loadedValue"));
    AtomicInteger hardEvals = new AtomicInteger();
    AtomicInteger softEvals = new AtomicInteger();

    Optional<String> result = hotKeyCache.get(
      "key1",
      new CachePolicy(
        hardEvals::incrementAndGet,
        softEvals::incrementAndGet,
        true,
        false,
        StalePolicy.SOFT_REFRESH,
        () -> "loadedValue",
        true,
        false
      )
    );

    assertThat(result).contains("loadedValue");
    assertThat(hardEvals.get()).isEqualTo(1);
    assertThat(softEvals.get()).isEqualTo(1);
  }

  /**
   * At-most-once TTL evaluation across the SOFT_REFRESH composition of
   * {@code getWithSoftExpire}: one call triggers both the background refresh
   * (consuming the soft override) and the local promotion probe (consuming
   * both overrides), yet each supplier is evaluated exactly once thanks to the
   * per-call memoized read context. A naive policy-threading refactor without
   * the shared memo would evaluate the soft supplier twice here.
   */
  @Test
  void getWithSoftExpire_softRefreshPlusPromotion_shouldEvaluateTtlSuppliersAtMostOnce() {
    when(hotKeyDetector.contains("key1")).thenReturn(true);
    when(healthView.isClusterHealthy()).thenReturn(false);
    caffeineCache.put(
      "key1",
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
    AtomicInteger hardEvals = new AtomicInteger();
    AtomicInteger softEvals = new AtomicInteger();

    Optional<String> result = hotKeyCache.getWithSoftExpire(
      "key1",
      new CachePolicy(
        hardEvals::incrementAndGet,
        softEvals::incrementAndGet,
        true,
        false,
        StalePolicy.SOFT_REFRESH,
        () -> "fresh",
        true,
        false
      )
    );

    assertThat(result).contains("stale");
    assertThat(hardEvals.get()).isEqualTo(1);
    assertThat(softEvals.get()).isEqualTo(1);
  }

  /**
   * Verifies that get returns empty for null or blank keys without loading.
   */
  @Test
  void get_shouldReturnEmptyForInvalidKey() {
    assertThat(
      hotKeyCache.get(null, CachePolicy.of(() -> "v", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))
    ).isEmpty();
    assertThat(hotKeyCache.get("", CachePolicy.of(() -> "v", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))).isEmpty();
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
    assertThatThrownBy(() ->
      hotKeyCache.get("secret", CachePolicy.of(() -> "db", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))
    ).isInstanceOf(ZetaBlockedException.class);
  }

  /**
   * Verifies that getWithSoftExpire throws ZetaBlockedException for a blacklisted key.
   */
  @Test
  void getWithSoftExpire_shouldThrowZeta() {
    hotKeyCache.addBlacklist("secret");
    assertThatThrownBy(() ->
      hotKeyCache.getWithSoftExpire("secret", CachePolicy.of(() -> "db", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))
    ).isInstanceOf(ZetaBlockedException.class);
  }

  @Test
  void execute_shouldRouteSwallowedExceptionToConfiguredHandler() {
    when(singleFlight.load(anyString(), any())).thenThrow(new IllegalStateException("boom"));
    AtomicInteger calls = new AtomicInteger();
    ZetaExceptionHandler.setDefaultExceptionHandler(t -> calls.incrementAndGet());
    try {
      assertThat(
        hotKeyCache.get("key1", CachePolicy.of(() -> "loaded", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))
      ).isEmpty();
      assertThat(calls.get()).isEqualTo(1);
    } finally {
      ZetaExceptionHandler.setDefaultExceptionHandler(null);
    }
  }

  // ── failOnError: distinguishing a failing data source from a missing key ──

  @Test
  void get_withFailOnError_shouldPropagateReaderFailure() {
    when(singleFlight.load(anyString(), any())).thenThrow(new IllegalStateException("boom"));

    assertThatThrownBy(() -> hotKeyCache.get("key1", CachePolicy.of(() -> "loaded").withFailOnError()))
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("boom");
    assertThat(caffeineCache.getIfPresent("key1")).isNull();
  }

  @Test
  void get_default_shouldSwallowReaderFailureAsMiss() {
    when(singleFlight.load(anyString(), any())).thenThrow(new IllegalStateException("boom"));

    assertThat(hotKeyCache.get("key1", CachePolicy.of(() -> "loaded"))).isEmpty();
    assertThat(caffeineCache.getIfPresent("key1")).isNull();
  }

  @Test
  void get_withFailOnError_nullResult_shouldStillCacheNullValue() {
    when(singleFlight.load(anyString(), any())).thenReturn(Optional.empty());

    assertThat(hotKeyCache.get("key1", CachePolicy.of(() -> null).withFailOnError())).isEmpty();
    Object raw = caffeineCache.getIfPresent("key1");
    assertThat(raw).isNotNull();
    assertThat(raw).isInstanceOf(CacheEntry.class);
    assertThat(((CacheEntry) raw).getValue()).isEqualTo(NullValue.INSTANCE);
  }

  @Test
  void computeIfAbsent_withFailOnError_shouldPropagateAndPreserveExistingEntry() {
    caffeineCache.put(
      "key1",
      CacheEntry.builder()
        .value("old")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(System.currentTimeMillis() + 300_000)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() - 1000)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );
    when(singleFlight.load(anyString(), any())).thenThrow(new IllegalStateException("boom"));

    assertThatThrownBy(() ->
      hotKeyCache.computeIfAbsent(
        "key1",
        CachePolicy.of(
          () -> {
            throw new IllegalStateException("boom");
          },
          0L,
          0L,
          true,
          true,
          StalePolicy.REVALIDATE
        ).withFailOnError()
      )
    )
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("boom");
    assertThat(caffeineCache.getIfPresent("key1")).isNotNull();
  }

  @Test
  void computeIfAbsent_miss_shouldLoadViaSingleFlightAndCacheNormalEntry() {
    when(singleFlight.load(eq("key1"), any())).thenReturn(vv("fresh"));

    assertThat(hotKeyCache.computeIfAbsent("key1", CachePolicy.of(() -> "fresh"))).contains("fresh");

    Object raw = caffeineCache.getIfPresent("key1");
    assertThat(raw).isInstanceOf(CacheEntry.class);
    assertThat(((CacheEntry) raw).getValue()).isEqualTo("fresh");
    assertThat(((CacheEntry) raw).getKeyState()).isEqualTo(KeyState.NORMAL);
  }

  @Test
  void computeIfAbsent_nullReader_shouldCacheNullSentinelAndReturnEmpty() {
    when(singleFlight.load(anyString(), any())).thenReturn(Optional.empty());

    assertThat(hotKeyCache.computeIfAbsent("key1", CachePolicy.of(() -> null))).isEmpty();

    Object raw = caffeineCache.getIfPresent("key1");
    assertThat(raw).isInstanceOf(CacheEntry.class);
    assertThat(((CacheEntry) raw).getValue()).isEqualTo(NullValue.INSTANCE);
  }

  @Test
  void computeIfAbsent_nullReader_noNullCaching_shouldLeaveNoEntry() {
    when(singleFlight.load(anyString(), any())).thenReturn(Optional.empty());

    assertThat(
      hotKeyCache.computeIfAbsent("key1", CachePolicy.of(() -> null, 0L, 0L, false, true, StalePolicy.SOFT_REFRESH))
    ).isEmpty();
    assertThat(caffeineCache.getIfPresent("key1")).isNull();
  }

  @Test
  void computeIfAbsent_revalidate_readerFailure_shouldKeepEntryAndReturnEmpty() {
    caffeineCache.put(
      "key1",
      CacheEntry.builder()
        .value("stale")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(System.currentTimeMillis() + 300_000)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() - 1000)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );
    when(singleFlight.load(anyString(), any())).thenThrow(new IllegalStateException("boom"));

    assertThat(
      hotKeyCache.computeIfAbsent("key1", CachePolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.REVALIDATE))
    ).isEmpty();

    // The soft-expired entry is kept in L1: it powers the circuit-breaker
    // fallback and lets the next call retry after the failed load.
    Object raw = caffeineCache.getIfPresent("key1");
    assertThat(raw).isInstanceOf(CacheEntry.class);
    assertThat(((CacheEntry) raw).getValue()).isEqualTo("stale");
  }

  @Test
  void computeIfAbsent_revalidate_circuitBreakerOpen_shouldServeStaleEntry() {
    caffeineCache.put(
      "key1",
      CacheEntry.builder()
        .value("stale")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(System.currentTimeMillis() + 300_000)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() - 1000)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );
    when(singleFlight.isBreakerOpen()).thenReturn(true);
    when(singleFlight.load(anyString(), any())).thenReturn(Optional.empty());

    assertThat(
      hotKeyCache.computeIfAbsent("key1", CachePolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.REVALIDATE))
    ).contains("stale");
  }

  /**
   * Regression (issue 25): when a soft-expired REVALIDATE entry crosses its hard TTL while
   * the load is in flight (source failing), the CB-open stale fallback must refuse to serve
   * it — the hard TTL contract is not bypassed. The kept entry must also NOT be overwritten
   * with a fabricated NullValue sentinel (the source never answered null).
   */
  @Test
  void computeIfAbsent_revalidate_circuitBreakerOpen_hardExpiredDuringLoad_shouldReturnEmpty() throws Exception {
    caffeineCache.put(
      "key1",
      CacheEntry.builder()
        .value("old")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(System.currentTimeMillis() + 300)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() - 1000)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );
    CountDownLatch releaseLoad = new CountDownLatch(1);
    when(singleFlight.isBreakerOpen()).thenReturn(true);
    when(singleFlight.load(anyString(), any())).thenAnswer(inv -> {
      releaseLoad.await();
      return Optional.empty();
    });

    CompletableFuture<Optional<String>> future = CompletableFuture.supplyAsync(() ->
      hotKeyCache.computeIfAbsent("key1", CachePolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.REVALIDATE))
    );

    Thread.sleep(500); // let the kept entry cross its hard expiry while the load is blocked
    releaseLoad.countDown();
    Optional<String> result = future.get(5, TimeUnit.SECONDS);

    assertThat(result).isEmpty();
    Object raw = caffeineCache.getIfPresent("key1");
    assertThat(raw).isInstanceOf(CacheEntry.class);
    assertThat(((CacheEntry) raw).getValue()).isEqualTo("old");
  }

  /**
   * Regression (issue 25): same as the sibling test, but for the load-throws path
   * (computeInLock catch block) — a kept entry that crosses its hard TTL during a
   * failing load must not be served as stale either.
   */
  @Test
  void computeIfAbsent_revalidate_circuitBreakerOpen_hardExpiredDuringThrowingLoad_shouldReturnEmpty()
    throws Exception {
    caffeineCache.put(
      "key1",
      CacheEntry.builder()
        .value("old")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(System.currentTimeMillis() + 300)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() - 1000)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );
    CountDownLatch releaseLoad = new CountDownLatch(1);
    when(singleFlight.isBreakerOpen()).thenReturn(true);
    when(singleFlight.load(anyString(), any())).thenAnswer(inv -> {
      releaseLoad.await();
      throw new IllegalStateException("boom");
    });

    CompletableFuture<Optional<String>> future = CompletableFuture.supplyAsync(() ->
      hotKeyCache.computeIfAbsent("key1", CachePolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.REVALIDATE))
    );

    Thread.sleep(500); // let the kept entry cross its hard expiry while the load is blocked
    releaseLoad.countDown();
    Optional<String> result = future.get(5, TimeUnit.SECONDS);

    assertThat(result).isEmpty();
    Object raw = caffeineCache.getIfPresent("key1");
    assertThat(raw).isInstanceOf(CacheEntry.class);
    assertThat(((CacheEntry) raw).getValue()).isEqualTo("old");
  }

  @Test
  void computeIfAbsent_hardExpired_readerFailure_shouldRemoveEntryAndReturnEmpty() {
    caffeineCache.put(
      "key1",
      CacheEntry.builder()
        .value("old")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(System.currentTimeMillis() - 1000)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() - 1000)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );
    when(singleFlight.load(anyString(), any())).thenThrow(new IllegalStateException("boom"));

    // Aligned with the get() path: hard-expired entries are removed on a
    // failed reload — they are never served as stale.
    assertThat(hotKeyCache.computeIfAbsent("key1", CachePolicy.of(() -> "fresh"))).isEmpty();
    assertThat(caffeineCache.getIfPresent("key1")).isNull();
  }

  @Test
  void computeIfAbsent_lazyTtl_shouldNotEvaluateTtlSuppliersOnPlainNormalHit() {
    AtomicInteger hardEvals = new AtomicInteger();
    AtomicInteger softEvals = new AtomicInteger();

    CachePolicy policy = new CachePolicy(
      () -> {
        hardEvals.incrementAndGet();
        return 100_000L;
      },
      () -> {
        softEvals.incrementAndGet();
        return 10_000L;
      },
      true,
      false,
      StalePolicy.SOFT_REFRESH,
      () -> "fresh",
      true,
      false
    );

    when(singleFlight.load(anyString(), any())).thenReturn(vv("fresh"));

    // Miss: TTL suppliers are evaluated exactly once (entry creation).
    assertThat(hotKeyCache.computeIfAbsent("key1", policy)).contains("fresh");
    assertThat(hardEvals.get()).isEqualTo(1);
    assertThat(softEvals.get()).isEqualTo(1);

    // Plain NORMAL hit: suppliers must NOT be evaluated again (ADR-0023).
    assertThat(hotKeyCache.computeIfAbsent("key1", policy)).contains("fresh");
    assertThat(hardEvals.get()).isEqualTo(1);
    assertThat(softEvals.get()).isEqualTo(1);
  }

  @Test
  void computeIfAbsent_softRefresh_shouldServeStaleAndTriggerBackgroundRefresh() {
    caffeineCache.put(
      "key1",
      CacheEntry.builder()
        .value("stale")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        // 10s in the past: comfortably beyond TimeSource's ~5ms cached-clock
        // staleness so the soft-expire branch is taken deterministically.
        .softExpireAtMs(System.currentTimeMillis() - 10_000)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );

    // Stale-while-revalidate: the caller receives the stale value, and the
    // background refresh runs on the (synchronous test) executor, replacing
    // the entry with the fresh value before the call returns.
    assertThat(
      hotKeyCache.computeIfAbsent("key1", CachePolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))
    ).contains("stale");

    Object raw = caffeineCache.getIfPresent("key1");
    assertThat(raw).isInstanceOf(CacheEntry.class);
    assertThat(((CacheEntry) raw).getValue()).isEqualTo("fresh");
  }

  @Test
  void computeIfAbsent_softRefresh_shouldRefreshCoolEntryAndDowngradeToNormal() {
    caffeineCache.put(
      "key1",
      CacheEntry.builder()
        .value("stale")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(5)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() - 10_000)
        .keyState(KeyState.COOL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(5000)
        .build()
    );

    // COOL entries are proactively refreshed on the annotation path too; the
    // successful refresh downgrades the entry to NORMAL.
    assertThat(
      hotKeyCache.computeIfAbsent("key1", CachePolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))
    ).contains("stale");

    CacheEntry after = (CacheEntry) caffeineCache.getIfPresent("key1");
    assertThat(after.getValue()).isEqualTo("fresh");
    assertThat(after.getKeyState()).isEqualTo(KeyState.NORMAL);
  }

  @Test
  void computeIfAbsent_softRefresh_coolEntryWithNullReader_shouldNotRefreshAndReturnValue() {
    caffeineCache.put(
      "key1",
      CacheEntry.builder()
        .value("stale")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(5)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() - 10_000)
        .keyState(KeyState.COOL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(5000)
        .build()
    );

    // Convenience policy without a reader: the background refresh must be
    // skipped (no NPE on supplyAsync(null)), the stale value served, and the
    // entry left untouched.
    assertThat(hotKeyCache.computeIfAbsent("key1", CachePolicy.of(0L, 0L, true, false))).contains("stale");

    CacheEntry after = (CacheEntry) caffeineCache.getIfPresent("key1");
    assertThat(after.getValue()).isEqualTo("stale");
    assertThat(after.getKeyState()).isEqualTo(KeyState.COOL);
  }

  @Test
  void getWithSoftExpire_softRefresh_coolEntryWithNullReader_shouldNotRefreshAndReturnValue() {
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
        .softExpireAtMs(System.currentTimeMillis() - 10_000)
        .keyState(KeyState.COOL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(5000)
        .build()
    );

    assertThat(hotKeyCache.getWithSoftExpire("key", CachePolicy.of(0L, 0L, true, false))).contains("stale");

    CacheEntry after = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(after.getValue()).isEqualTo("stale");
    assertThat(after.getKeyState()).isEqualTo(KeyState.COOL);
  }

  @Test
  void computeIfAbsent_nullSentinelHit_shouldReturnEmptyWithoutInvokingReader() {
    caffeineCache.put(
      "key1",
      CacheEntry.builder()
        .value(NullValue.INSTANCE)
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(10_000)
        .hardExpireAtMs(System.currentTimeMillis() + 10_000)
        .softTtlMs(0L)
        .softExpireAtMs(0L)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(10_000)
        .normalSoftTtlMs(0L)
        .build()
    );

    assertThat(hotKeyCache.computeIfAbsent("key1", CachePolicy.of(() -> "should-not-load"))).isEmpty();
    verify(singleFlight, never()).load(anyString(), any());
  }

  /**
   * Executor-aware sentinel regression: a {@code NullValue} sentinel hit in
   * {@code computeIfAbsent} must neither arm a background refresh nor promote
   * the sentinel to HOT. Sentinels carry {@code softExpireAtMs = 0} which
   * {@code isSoftExpired} reports as expired, so without the in-compute guard
   * the SOFT_REFRESH branch would re-invoke the loader on every sentinel hit,
   * and the promotion branch would extend the null block to the hot hard TTL.
   * The setUp executor is synchronous ({@code Runnable::run}), so an armed
   * refresh would have run the reader and replaced the entry before the call
   * returned — the reader counter and the stored entry are the observables.
   */
  @Test
  void computeIfAbsent_nullSentinelHit_shouldNotArmRefreshOrPromoteSentinel() {
    caffeineCache.put(
      "key1",
      CacheEntry.builder()
        .value(NullValue.INSTANCE)
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(10_000)
        .hardExpireAtMs(System.currentTimeMillis() + 10_000)
        .softTtlMs(0L)
        .softExpireAtMs(0L)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(10_000)
        .normalSoftTtlMs(0L)
        .build()
    );
    // TopK membership: makes the promotion branch reachable for this entry.
    when(hotKeyDetector.contains("key1")).thenReturn(true);
    AtomicInteger readerCalls = new AtomicInteger();

    assertThat(
      hotKeyCache.computeIfAbsent(
        "key1",
        CachePolicy.of(
          () -> {
            readerCalls.incrementAndGet();
            return "should-not-load";
          },
          0L,
          0L,
          true,
          true,
          StalePolicy.SOFT_REFRESH
        )
      )
    ).isEmpty();

    assertThat(readerCalls.get()).isZero();
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key1");
    assertThat((Object) entry.getValue()).isEqualTo(NullValue.INSTANCE);
    assertThat(entry.getKeyState()).isEqualTo(KeyState.NORMAL);
  }

  @Test
  void getAll_withFailOnError_shouldPropagateReaderFailure() {
    when(singleFlight.load(any(Iterable.class), any(), anyBoolean())).thenThrow(new IllegalStateException("boom"));

    assertThatThrownBy(() -> hotKeyCache.get(List.of("a", "b"), k -> "v", 0L, 0L, true, true))
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("boom");
  }

  @Test
  void getAll_default_shouldSwallowPerKeyFailure() {
    when(singleFlight.load(any(Iterable.class), any(), anyBoolean())).thenReturn(
      Map.of("a", Optional.of("v"), "b", Optional.<String>empty())
    );

    Map<String, Optional<String>> result = hotKeyCache.get(List.of("a", "b"), k -> "v", 0L, 0L, true, false);

    assertThat(result).containsEntry("a", Optional.of("v")).containsEntry("b", Optional.empty());
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

    when(singleFlight.load(anyString(), any())).thenReturn(vv("loaded"));

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
      new VersionControllerImpl(Optional.empty(), 60, snowflakeIdGenerator),
      props,
      mock(HealthView.class),
      CacheCompressor.NONE
    );

    assertThat(
      cache.getWithSoftExpire("key", CachePolicy.of(() -> "loaded", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))
    ).contains("loaded");
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

    assertThat(
      hotKeyCache.getWithSoftExpire(
        "key",
        CachePolicy.of(() -> "should-not-load", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH)
      )
    ).contains("cached");
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
    assertThat(
      hotKeyCache.getWithSoftExpire("key", CachePolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))
    ).contains("stale");
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
   * Verifies that when the shared scheduler rejects the REFRESH flush scheduling, putThrough still
   * updates L1 and delivers the refresh via the BroadcastBuffer synchronous-flush fallback, instead
   * of stranding peers on the stale value.
   */
  @Test
  void putThrough_whenFlushSchedulingRejected_shouldUpdateL1AndStillBroadcast() {
    ScheduledExecutorService rejecting = mock(ScheduledExecutorService.class);
    when(rejecting.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenThrow(
      new RejectedExecutionException("saturated")
    );

    CacheSyncPublisher publisher = mock(CacheSyncPublisher.class);
    HotKeyCache cache = new HotKeyCache(
      hotKeyDetector,
      caffeineCache,
      singleFlight,
      expireManager,
      executor,
      new CentralDispatcher(
        Optional.empty(),
        Optional.of(publisher),
        new BroadcastBuffer(rejecting, Optional.of(publisher)),
        hotKeyDetector
      ),
      new RuleMatcherImpl(Optional.empty(), Optional.empty()),
      new VersionControllerImpl(Optional.empty(), 60, snowflakeIdGenerator),
      new ZetaProperties(),
      mock(HealthView.class),
      CacheCompressor.NONE
    );

    assertThatCode(() -> cache.putThrough("key1", "newValue", () -> {}, 0L, 0L, true)).doesNotThrowAnyException();
    assertThat(cache.peek("key1")).contains("newValue");
    verify(publisher).broadcastRefresh(eq("key1"), anyLong(), anyBoolean());
  }

  /**
   * Degraded-fallback path (version controller fails after the writer ran):
   * the L1 is updated with a degraded version, and a caller that opted OUT of
   * broadcasting gets no REFRESH broadcast — the same opt-out contract as the
   * success path.
   */
  @Test
  void putThrough_degradedFallback_optOut_shouldNotBroadcast() {
    VersionController failing = mock(VersionController.class);
    when(failing.nextVersion("key1")).thenThrow(new IllegalStateException("redis down"));
    when(failing.fallbackVersion()).thenReturn(new VersionController.VersionResult(-100L, true));

    CacheSyncPublisher publisher = mock(CacheSyncPublisher.class);
    BroadcastBuffer buffer = new BroadcastBuffer(scheduler, Optional.of(publisher));
    HotKeyCache cache = new HotKeyCache(
      hotKeyDetector,
      caffeineCache,
      singleFlight,
      expireManager,
      executor,
      new CentralDispatcher(Optional.empty(), Optional.of(publisher), buffer, hotKeyDetector),
      new RuleMatcherImpl(Optional.empty(), Optional.empty()),
      failing,
      ttlConfig,
      mock(HealthView.class),
      CacheCompressor.NONE
    );

    cache.putThrough("key1", "degradedValue", () -> {}, 0L, 0L, false);

    // The degraded local update still lands (coherence with the mutation).
    assertThat(cache.peek("key1")).contains("degradedValue");
    buffer.flush();
    verify(publisher, never()).broadcastRefresh(anyString(), anyLong(), anyBoolean());
  }

  /** Control: the same degraded path with broadcast opted in sends the degraded REFRESH. */
  @Test
  void putThrough_degradedFallback_optIn_shouldBroadcastDegradedVersion() {
    VersionController failing = mock(VersionController.class);
    when(failing.nextVersion("key1")).thenThrow(new IllegalStateException("redis down"));
    when(failing.fallbackVersion()).thenReturn(new VersionController.VersionResult(-100L, true));

    CacheSyncPublisher publisher = mock(CacheSyncPublisher.class);
    BroadcastBuffer buffer = new BroadcastBuffer(scheduler, Optional.of(publisher));
    HotKeyCache cache = new HotKeyCache(
      hotKeyDetector,
      caffeineCache,
      singleFlight,
      expireManager,
      executor,
      new CentralDispatcher(Optional.empty(), Optional.of(publisher), buffer, hotKeyDetector),
      new RuleMatcherImpl(Optional.empty(), Optional.empty()),
      failing,
      ttlConfig,
      mock(HealthView.class),
      CacheCompressor.NONE
    );

    cache.putThrough("key1", "degradedValue", () -> {}, 0L, 0L, true);

    assertThat(cache.peek("key1")).contains("degradedValue");
    buffer.flush();
    verify(publisher).broadcastRefresh(eq("key1"), eq(-100L), eq(true));
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
   * Verifies that getWithSoftExpire with an ALLOW_NO_REPORT rule does not reportToWorker
   * and returns the cached value normally.
   */
  @Test
  void getWithSoftExpire_withNoReportRule_shouldReturnCached() {
    hotKeyCache.addWhitelist("no-reportToWorker");
    caffeineCache.put(
      "no-reportToWorker",
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

    assertThat(
      hotKeyCache.getWithSoftExpire(
        "no-reportToWorker",
        CachePolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH)
      )
    ).contains("v");
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
    when(singleFlight.load(anyString(), any())).thenReturn(vv("fresh"));

    assertThat(
      hotKeyCache.get("expired", CachePolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))
    ).contains("fresh");
  }

  // ── get with StalePolicy.REVALIDATE: caller policy wired through the hit path ──

  /**
   * A caller-supplied {@link StalePolicy#REVALIDATE} on a plain {@code get} is
   * honored like on {@code getWithSoftExpire}: a soft-expired entry is
   * dropped-and-reloaded and the fresh value serves the caller. The old
   * behavior silently treated REVALIDATE as RETURN (stale served, loader
   * never invoked).
   */
  @Test
  void get_revalidate_softExpiredEntry_shouldReloadViaLoader() {
    caffeineCache.put(
      "key1",
      CacheEntry.builder()
        .value("stale")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(System.currentTimeMillis() + 300_000)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() - 10_000)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );
    when(singleFlight.load(eq("key1"), any())).thenReturn(vv("fresh"));

    assertThat(hotKeyCache.get("key1", CachePolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.REVALIDATE)))
      .contains("fresh");
    verify(singleFlight, times(1)).load(eq("key1"), any());
    assertThat(((CacheEntry) caffeineCache.getIfPresent("key1")).getValue()).isEqualTo("fresh");
  }

  /** Control: {@link StalePolicy#RETURN} on the same entry serves the stale value without a reload. */
  @Test
  void get_return_softExpiredEntry_shouldServeStaleWithoutReload() {
    caffeineCache.put(
      "key1",
      CacheEntry.builder()
        .value("stale")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(System.currentTimeMillis() + 300_000)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() - 10_000)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );

    assertThat(hotKeyCache.get("key1", CachePolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.RETURN)))
      .contains("stale");
    verify(singleFlight, never()).load(anyString(), any());
    assertThat(((CacheEntry) caffeineCache.getIfPresent("key1")).getValue()).isEqualTo("stale");
  }

  /**
   * REVALIDATE must keep the sentinel's penetration protection: sentinels
   * carry {@code softExpireAtMs = 0} (always reported soft-expired) and must
   * not be dropped-and-reloaded by the REVALIDATE branch.
   */
  @Test
  void get_revalidate_nullSentinelHit_shouldKeepPenetrationProtection() {
    caffeineCache.put(
      "key1",
      CacheEntry.builder()
        .value(NullValue.INSTANCE)
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(10_000)
        .hardExpireAtMs(System.currentTimeMillis() + 10_000)
        .softTtlMs(0L)
        .softExpireAtMs(0L)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(10_000)
        .normalSoftTtlMs(0L)
        .build()
    );
    when(singleFlight.load(anyString(), any())).thenReturn(vv("loaded"));

    assertThat(hotKeyCache.get("key1", CachePolicy.of(() -> "loaded", 0L, 0L, true, true, StalePolicy.REVALIDATE)))
      .isEmpty();
    verify(singleFlight, never()).load(anyString(), any());
    assertThat(((CacheEntry) caffeineCache.getIfPresent("key1")).getValue()).isEqualTo(NullValue.INSTANCE);
  }

  // ── getWithSoftExpire: invalid key ──

  @Test
  void getWithSoftExpire_withInvalidKey_shouldReturnEmpty() {
    assertThat(
      hotKeyCache.getWithSoftExpire(null, CachePolicy.of(() -> "v", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))
    ).isEmpty();
    assertThat(
      hotKeyCache.getWithSoftExpire("", CachePolicy.of(() -> "v", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))
    ).isEmpty();
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
    when(singleFlight.load(anyString(), any())).thenReturn(vv("fresh"));

    assertThat(
      hotKeyCache.getWithSoftExpire(
        "expired",
        CachePolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH)
      )
    ).contains("fresh");
  }

  // ── getWithSoftExpire: cache miss (no entry) triggers loadAndCache ──

  @Test
  void getWithSoftExpire_withCacheMiss_shouldLoad() {
    when(singleFlight.load(anyString(), any())).thenReturn(vv("loaded"));

    assertThat(
      hotKeyCache.getWithSoftExpire(
        "missing",
        CachePolicy.of(() -> "loaded", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH)
      )
    ).contains("loaded");
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
    assertThat(
      hotKeyCache.getWithSoftExpire("raw", CachePolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))
    ).contains("bare-string");
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

    Optional<String> result = hotKeyCache.getWithSoftExpire(
      "normal",
      CachePolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH)
    );

    assertThat(result).contains("stale");
  }

  // ── getWithSoftExpire REVALIDATE: worker-managed keep-and-renew ──

  /** Build a Worker-stamped COOL entry whose soft TTL has expired. */
  private static CacheEntry workerCoolSoftExpiredEntry() {
    return CacheEntry.builder()
      .value("stored")
      .dataVersion(1)
      .isVersionDegraded(false)
      .decisionVersion(5)
      .decisionNodeId("w1")
      .decisionEpoch(7)
      .hardTtlMs(300_000)
      .hardExpireAtMs(System.currentTimeMillis() + 300_000)
      .softTtlMs(30_000)
      .softExpireAtMs(System.currentTimeMillis() - 10_000)
      .keyState(KeyState.COOL)
      .normalHardTtlMs(300_000)
      .normalSoftTtlMs(30_000)
      .build();
  }

  /**
   * REVALIDATE keep-and-renew (loadCacheEntry's worker-managed branch): a
   * soft-expired COOL entry is kept (decision stamp intact) instead of being
   * overwritten, and its soft expiry is renewed to the entry's own soft
   * cadence — clamped by the hard TTL — so subsequent reads do NOT re-invoke
   * the loader at read rate (COOL entries are never rebroadcast by a Worker).
   */
  @Test
  void getWithSoftExpire_revalidate_workerManagedCoolEntry_shouldKeepAndRenewWithoutReloadRate() {
    caffeineCache.put("key1", workerCoolSoftExpiredEntry());
    when(singleFlight.load(eq("key1"), any())).thenReturn(vv("fresh"));

    // First read: the kept entry's reload re-invokes the loader once; the
    // fresh value serves this caller while the Worker-managed entry survives.
    assertThat(
      hotKeyCache.getWithSoftExpire("key1", CachePolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.REVALIDATE))
    ).contains("fresh");

    CacheEntry after = (CacheEntry) caffeineCache.getIfPresent("key1");
    assertThat((Object) after.getValue()).isEqualTo("stored");
    assertThat(after.getKeyState()).isEqualTo(KeyState.COOL);
    assertThat(after.getDecisionNodeId()).isEqualTo("w1");
    assertThat(after.getDecisionVersion()).isEqualTo(5L);
    // Soft expiry renewed within the hard TTL: no longer soft-expired.
    assertThat(expireManager.ttlPolicy().isSoftExpired(after)).isFalse();
    assertThat(after.getSoftExpireAtMs()).isLessThanOrEqualTo(after.getHardExpireAtMs());

    // Subsequent reads serve the kept entry — the loader is NOT re-invoked.
    assertThat(
      hotKeyCache.getWithSoftExpire("key1", CachePolicy.of(() -> "fresh2", 0L, 0L, true, true, StalePolicy.REVALIDATE))
    ).contains("stored");
    verify(singleFlight, times(1)).load(anyString(), any());
  }

  /**
   * A null load must not erase a Worker decision: the {@code NullValue}
   * sentinel written over the kept entry carries the existing entry's decision
   * stamp so {@code decisionVersion} stays comparable for later broadcasts.
   */
  @Test
  void get_revalidate_workerManagedEntry_nullLoad_shouldPreserveDecisionStampOnSentinel() {
    caffeineCache.put("key1", workerCoolSoftExpiredEntry());
    when(singleFlight.load(eq("key1"), any())).thenReturn(Optional.empty());

    assertThat(hotKeyCache.get("key1", CachePolicy.of(() -> null, 0L, 0L, true, true, StalePolicy.REVALIDATE)))
      .isEmpty();

    CacheEntry sentinel = (CacheEntry) caffeineCache.getIfPresent("key1");
    assertThat((Object) sentinel.getValue()).isEqualTo(NullValue.INSTANCE);
    assertThat(sentinel.getDecisionNodeId()).isEqualTo("w1");
    assertThat(sentinel.getDecisionVersion()).isEqualTo(5L);
    assertThat(sentinel.getDecisionEpoch()).isEqualTo(7L);
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

  // ── compareAndSet / compareAndInvalidate: latched concurrent writer ──

  /**
   * Latched race wiring for the CAS family: a real Caffeine cache backs a
   * delegating mock whose {@code getIfPresent} parks after capturing the
   * snapshot, handing the writer the exact race window between the snapshot
   * read and the in-lock re-judgment. The suite previously covered only the
   * single-threaded branches — that is how the inverted
   * {@code compareAndInvalidate} guard survived.
   */
  @SuppressWarnings("unchecked")
  private HotKeyCache newLatchedCasCache(
    Cache<String, Object> realCache,
    CountDownLatch snapshotTaken,
    CountDownLatch writerCommitted
  ) {
    AtomicBoolean gated = new AtomicBoolean(true);
    Cache<String, Object> gatedCache = mock(
      Cache.class,
      withSettings().defaultAnswer(delegatesTo(realCache))
    );
    doAnswer(inv -> {
      Object snapshot = realCache.getIfPresent(inv.getArgument(0));
      if (gated.compareAndSet(true, false)) {
        snapshotTaken.countDown();
        writerCommitted.await(5, TimeUnit.SECONDS);
      }
      return snapshot;
    }).when(gatedCache).getIfPresent(anyString());

    return new HotKeyCache(
      hotKeyDetector,
      gatedCache,
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
      new VersionControllerImpl(Optional.empty(), 60, snowflakeIdGenerator),
      ttlConfig,
      healthView,
      CacheCompressor.NONE
    );
  }

  /**
   * Latched race: the writer commits a DIFFERENT value while the
   * {@code compareAndSet} is in flight. The in-lock re-judgment must refuse
   * the replacement (no lost update) — the newer entry survives.
   */
  @Test
  void compareAndSet_concurrentWriterSwapsValue_shouldNotOverwriteNewerEntry() throws Exception {
    Cache<String, Object> real = Caffeine.newBuilder().maximumSize(100).build();
    CountDownLatch snapshotTaken = new CountDownLatch(1);
    CountDownLatch writerCommitted = new CountDownLatch(1);
    HotKeyCache cache = newLatchedCasCache(real, snapshotTaken, writerCommitted);
    real.put("k", CacheEntry.builder().value("old").build());

    CompletableFuture<Boolean> cas =
      CompletableFuture.supplyAsync(() -> cache.compareAndSet("k", "old", "new"));
    assertThat(snapshotTaken.await(5, TimeUnit.SECONDS)).isTrue();

    // Writer commits a newer value while the CAS call is in flight.
    real.put("k", CacheEntry.builder().value("swapped").build());
    writerCommitted.countDown();

    assertThat(cas.get(5, TimeUnit.SECONDS)).isFalse();
    assertThat(((CacheEntry) real.getIfPresent("k")).getValue()).isEqualTo("swapped");
  }

  /**
   * Latched race, matching direction: the writer replaces the entry with a
   * NEW instance carrying the SAME user value. The instance-identity fast
   * path misses, the re-judgment matches, and the replacement proceeds.
   */
  @Test
  void compareAndSet_concurrentWriterSwapsToEqualValue_shouldStillReplace() throws Exception {
    Cache<String, Object> real = Caffeine.newBuilder().maximumSize(100).build();
    CountDownLatch snapshotTaken = new CountDownLatch(1);
    CountDownLatch writerCommitted = new CountDownLatch(1);
    HotKeyCache cache = newLatchedCasCache(real, snapshotTaken, writerCommitted);
    real.put("k", CacheEntry.builder().value("old").build());

    CompletableFuture<Boolean> cas =
      CompletableFuture.supplyAsync(() -> cache.compareAndSet("k", "old", "new"));
    assertThat(snapshotTaken.await(5, TimeUnit.SECONDS)).isTrue();

    real.put("k", CacheEntry.builder().value("old").dataVersion(9).build());
    writerCommitted.countDown();

    assertThat(cas.get(5, TimeUnit.SECONDS)).isTrue();
    assertThat(((CacheEntry) real.getIfPresent("k")).getValue()).isEqualTo("new");
  }

  /** Latched race on {@code compareAndInvalidate}: a newer non-matching value must NOT be invalidated. */
  @Test
  void compareAndInvalidate_concurrentWriterSwapsValue_shouldNotInvalidateNewerEntry() throws Exception {
    Cache<String, Object> real = Caffeine.newBuilder().maximumSize(100).build();
    CountDownLatch snapshotTaken = new CountDownLatch(1);
    CountDownLatch writerCommitted = new CountDownLatch(1);
    HotKeyCache cache = newLatchedCasCache(real, snapshotTaken, writerCommitted);
    real.put("k", CacheEntry.builder().value("old").build());

    CompletableFuture<Boolean> invalidate =
      CompletableFuture.supplyAsync(() -> cache.compareAndInvalidate("k", "old"));
    assertThat(snapshotTaken.await(5, TimeUnit.SECONDS)).isTrue();

    real.put("k", CacheEntry.builder().value("swapped").build());
    writerCommitted.countDown();

    assertThat(invalidate.get(5, TimeUnit.SECONDS)).isFalse();
    assertThat(((CacheEntry) real.getIfPresent("k")).getValue()).isEqualTo("swapped");
  }

  /** Latched race on {@code compareAndInvalidate}: a newer instance with the matching value IS invalidated. */
  @Test
  void compareAndInvalidate_concurrentWriterSwapsToEqualValue_shouldStillInvalidate() throws Exception {
    Cache<String, Object> real = Caffeine.newBuilder().maximumSize(100).build();
    CountDownLatch snapshotTaken = new CountDownLatch(1);
    CountDownLatch writerCommitted = new CountDownLatch(1);
    HotKeyCache cache = newLatchedCasCache(real, snapshotTaken, writerCommitted);
    real.put("k", CacheEntry.builder().value("old").build());

    CompletableFuture<Boolean> invalidate =
      CompletableFuture.supplyAsync(() -> cache.compareAndInvalidate("k", "old"));
    assertThat(snapshotTaken.await(5, TimeUnit.SECONDS)).isTrue();

    real.put("k", CacheEntry.builder().value("old").dataVersion(9).build());
    writerCommitted.countDown();

    assertThat(invalidate.get(5, TimeUnit.SECONDS)).isTrue();
    assertThat(real.getIfPresent("k")).isNull();
  }

  // ── putIfAbsent ──

  @Test
  void putIfAbsent_withAbsentKey_shouldInsertAndReturnTrue() {
    assertThat(hotKeyCache.putIfAbsent("k", "v", 0L, 0L)).isTrue();
    assertThat(hotKeyCache.peek("k")).contains("v");
  }

  @Test
  void putIfAbsent_withExistingKey_shouldReturnFalseAndNotOverwrite() {
    assertThat(hotKeyCache.putIfAbsent("k", "first", 0L, 0L)).isTrue();
    assertThat(hotKeyCache.putIfAbsent("k", "second", 0L, 0L)).isFalse();
    assertThat(hotKeyCache.peek("k")).contains("first");
  }

  @Test
  void putIfAbsent_withExistingKeyAndDifferentTtl_shouldReturnFalseAndNotChangeTtl() {
    assertThat(hotKeyCache.putIfAbsent("k", "v", 10000L, 1000L)).isTrue();
    assertThat(hotKeyCache.putIfAbsent("k", "v", 50000L, 5000L)).isFalse();
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(entry.getHardTtlMs()).isEqualTo(10000L);
    assertThat(entry.getSoftTtlMs()).isEqualTo(1000L);
  }

  @Test
  void putIfAbsent_withExistingNullValueSentinel_shouldReturnFalseAndNotOverwrite() {
    caffeineCache.put(
      "k",
      CacheEntry.builder()
        .value(NullValue.INSTANCE)
        .dataVersion(0)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(10_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(0)
        .softExpireAtMs(0)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(10_000)
        .normalSoftTtlMs(0)
        .build()
    );
    assertThat(hotKeyCache.putIfAbsent("k", "v", 0L, 0L)).isFalse();
    assertThat(hotKeyCache.peek("k")).isEmpty();
  }

  @Test
  void putIfAbsent_withExistingWorkerManagedHotEntry_shouldPreserveHotState() {
    caffeineCache.put(
      "k",
      CacheEntry.builder()
        .value("workerVal")
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
    assertThat(hotKeyCache.putIfAbsent("k", "localVal", 0L, 0L)).isFalse();
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(entry.getValue()).isEqualTo("workerVal");
    assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
  }

  @Test
  void putIfAbsent_whenInserted_shouldInvalidateSingleFlightDedup() {
    // ADR-0067: the committed write obsoletes every load result recorded before
    // it, so a later miss must re-invoke the reader instead of replaying a dedup
    // future that carries the pre-write value. putIfAbsent writes with
    // VERSION_DEFAULT and does not bump the version, so loadCacheEntry's
    // VersionGuard cannot reject the stale replay — the dedup invalidation is the
    // only thing standing between this write and a silent read-your-old-value.
    assertThat(hotKeyCache.putIfAbsent("k", "v", 0L, 0L)).isTrue();
    verify(singleFlight).invalidate("k");
  }

  @Test
  void putIfAbsent_whenKeyAlreadyPresent_shouldNotInvalidateSingleFlightDedup() {
    assertThat(hotKeyCache.putIfAbsent("k", "first", 0L, 0L)).isTrue();
    assertThat(hotKeyCache.putIfAbsent("k", "second", 0L, 0L)).isFalse();
    // The second call is a no-op, so it must not discard a warm dedup entry.
    verify(singleFlight, times(1)).invalidate("k");
  }

  @Test
  void putIfAbsent_withExistingWorkerManagedCoolEntry_shouldPreserveCoolState() {
    caffeineCache.put(
      "k",
      CacheEntry.builder()
        .value("workerCool")
        .dataVersion(200)
        .isVersionDegraded(false)
        .decisionVersion(10)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(Long.MAX_VALUE)
        .keyState(KeyState.COOL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );
    assertThat(hotKeyCache.putIfAbsent("k", "override", 0L, 0L)).isFalse();
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(entry.getKeyState()).isEqualTo(KeyState.COOL);
  }

  @Test
  void putIfAbsent_withBlacklistedKey_shouldThrow() {
    hotKeyCache.addBlacklist("block:*");
    assertThatThrownBy(() -> hotKeyCache.putIfAbsent("block:k", "v", 0L, 0L)).isInstanceOf(ZetaBlockedException.class);
  }

  @Test
  void putIfAbsent_withInvalidKey_shouldReturnFalse() {
    assertThat(hotKeyCache.putIfAbsent(null, "v", 0L, 0L)).isFalse();
    assertThat(hotKeyCache.putIfAbsent("", "v", 0L, 0L)).isFalse();
  }

  @Test
  void putIfAbsent_withCustomTtl_shouldUseCustomTtl() {
    assertThat(hotKeyCache.putIfAbsent("k", "v", 50000L, 5000L)).isTrue();
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(entry.getHardTtlMs()).isEqualTo(50000L);
    assertThat(entry.getSoftTtlMs()).isEqualTo(5000L);
  }

  @Test
  void putIfAbsent_withExistingBareObject_shouldNotOverwrite() {
    caffeineCache.put("k", "bareValue");
    assertThat(hotKeyCache.putIfAbsent("k", "newValue", 0L, 0L)).isFalse();
    assertThat(caffeineCache.getIfPresent("k")).isEqualTo("bareValue");
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
    hotKeyCache.addWhitelist("no-reportToWorker");
    caffeineCache.put("no-reportToWorker", "v");
    assertThat(
      hotKeyCache.get("no-reportToWorker", CachePolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))
    ).contains("v");
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
    assertThat(
      hotKeyCache.getWithSoftExpire(
        "key",
        CachePolicy.of(() -> "fresh", 0L, 500L, true, true, StalePolicy.SOFT_REFRESH)
      )
    ).contains("cached");
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
    assertThat(
      hotKeyCache.getWithSoftExpire(
        "key",
        CachePolicy.of(() -> "fresh", 10000L, 500L, true, true, StalePolicy.SOFT_REFRESH)
      )
    ).contains("cached");
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

    private final SnowflakeIdGenerator snowflakeIdGenerator = new SnowflakeIdGenerator(0, 1);

    private HotKeyDetector hotKeyDetector;
    private Cache<String, Object> caffeineCache;
    private SingleFlight singleFlight;
    private ExpireManager expireManager;
    private Executor executor;
    private HotKeyCache hotKeyCache;
    private CacheSyncPublisher publisher;
    private BroadcastBuffer broadcastBuffer;
    private HealthView healthView;
    private KeyReporter reporter;

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
      reporter = mock(KeyReporter.class);
      hotKeyCache = new HotKeyCache(
        hotKeyDetector,
        caffeineCache,
        singleFlight,
        expireManager,
        executor,
        new CentralDispatcher(Optional.of(reporter), Optional.of(publisher), broadcastBuffer, hotKeyDetector),
        new RuleMatcherImpl(Optional.empty(), Optional.empty()),
        new VersionControllerImpl(Optional.empty(), 60, snowflakeIdGenerator),
        ttlConfig,
        healthView,
        CacheCompressor.NONE
      );
    }

    @Test
    @DisplayName("loadAndCache should promote key to HOT when detected as hot")
    void loadAndCache_shouldPromoteHotKey() {
      when(hotKeyDetector.contains("key1")).thenReturn(true);
      when(singleFlight.load(eq("key1"), any())).thenReturn(vv("value"));

      Optional<String> result = hotKeyCache.get(
        "key1",
        CachePolicy.of(() -> "value", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH)
      );

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
        Supplier<?> reader = invocation.getArgument(1);
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

      Optional<String> result = hotKeyCache.get(
        "key1",
        CachePolicy.of(() -> "newValue", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH)
      );

      assertThat(result).contains("newValue");
      Object raw = caffeineCache.getIfPresent("key1");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      CacheEntry entry = (CacheEntry) raw;
      assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
      assertThat(entry.getValue()).isEqualTo("workerValue");
    }

    @Test
    @DisplayName("getWithSoftExpire refreshes COOL entry and downgrades it to NORMAL")
    void getWithSoftExpire_shouldRefreshCoolEntryAndDowngradeToNormal() {
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

      hotKeyCache.getWithSoftExpire("key", CachePolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

      // COOL entries are refreshed; the successful refresh downgrades the
      // entry to NORMAL (local activity regains the ordinary lifecycle).
      CacheEntry after = (CacheEntry) caffeineCache.getIfPresent("key");
      assertThat(after.getValue()).isEqualTo("fresh");
      assertThat(after.getSoftTtlMs()).isEqualTo(5000L);
      assertThat(after.getKeyState()).isEqualTo(KeyState.NORMAL);
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

      hotKeyCache.getWithSoftExpire(
        "key",
        CachePolicy.of(() -> "fresh", 0L, 9999L, true, true, StalePolicy.SOFT_REFRESH)
      );

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

      hotKeyCache.get("key1", CachePolicy.of(() -> "loaded", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

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

      hotKeyCache.get("key1", CachePolicy.of(() -> "loaded", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

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

      hotKeyCache.get("key1", CachePolicy.of(() -> "loaded", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

      Object raw = caffeineCache.getIfPresent("key1");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      CacheEntry entry = (CacheEntry) raw;
      assertThat(entry.getKeyState()).isEqualTo(KeyState.COOL);
    }

    @Test
    @DisplayName("processLocalHotkeyIfNeeded should skip promote for non-member NORMAL entry via lock-free pre-check")
    void processLocalHotkeyIfNeeded_shouldSkipPromoteForNonMemberNormal() {
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
      when(hotKeyDetector.contains("key1")).thenReturn(false);

      assertThat(
        hotKeyCache.get("key1", CachePolicy.of(() -> "loaded", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))
      ).contains("v");

      CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key1");
      assertThat(entry.getKeyState()).isEqualTo(KeyState.NORMAL);
      assertThat(entry.getHardTtlMs()).isEqualTo(300_000L);
    }

    @Test
    @DisplayName("promote should NOT promote when TopK membership revoked between pre-check and compute (TOCTOU guard)")
    void promote_shouldNotPromoteWhenMembershipRevokedBetweenPreCheckAndCompute() {
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
      // First contains() = lock-free pre-check (passes), second = TOCTOU guard inside compute (fails)
      when(hotKeyDetector.contains("key1")).thenReturn(true, false);

      hotKeyCache.get("key1", CachePolicy.of(() -> "loaded", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

      CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key1");
      assertThat(entry.getKeyState()).isEqualTo(KeyState.NORMAL);
    }

    @Test
    @DisplayName("promote should preserve a concurrently-replaced bare value instead of deleting it")
    void promote_shouldPreserveBareValueReplacedDuringProcessing() {
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
      // Simulate a concurrent raw write replacing the CacheEntry with a bare value
      // between the lock-free pre-check and the promote compute.
      when(hotKeyDetector.contains(anyString())).thenAnswer(invocation -> {
        caffeineCache.put("key1", "bareValue");
        return true;
      });

      assertThat(
        hotKeyCache.get("key1", CachePolicy.of(() -> "loaded", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))
      ).contains("v");

      assertThat(caffeineCache.getIfPresent("key1")).isEqualTo("bareValue");
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
      // Normal path (non-Redis fallback within nextVersion) still respects version guard;
      // the degraded fallback rewrite keeps the same guard (forceUpdate=false, ADR-0066).
      assertThat(entry.getValue()).isEqualTo("old");
    }

    @Test
    @DisplayName("invalidate should send when publisher present")
    void invalidate_shouldBroadcastWhenPublisherPresent() {
      hotKeyCache.invalidate("key1", true);

      verify(publisher).broadcastLocalInvalidate(eq("key1"), anyLong(), eq(true));
    }

    @Test
    @DisplayName("invalidateAllLocal should send per-key versioned invalidates when publisher present")
    void invalidateAll_shouldBroadcastWhenPublisherPresent() {
      hotKeyCache.invalidate(List.of("key1", "key2"), true);

      // ADR-0066: the batch path now sends the same per-key versioned INVALIDATE
      // messages as the single-key path (the unversioned INVALIDATE_ALL broadcast
      // could not be ordered against a newer REFRESH on the same key).
      verify(publisher).broadcastLocalInvalidate(eq("key1"), anyLong(), eq(true));
      verify(publisher).broadcastLocalInvalidate(eq("key2"), anyLong(), eq(true));
      verify(publisher, never()).broadcastLocalInvalidateAll(anyList());
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
    void invalidateAfterPut_shouldBroadcastWhenPublisherPresent() {
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
      when(singleFlight.load(anyString(), any())).thenReturn(vv("value"));
      when(hotKeyDetector.contains("key1")).thenReturn(false);

      hotKeyCache.get("key1", CachePolicy.of(() -> "value", 50000L, 5000L, true, true, StalePolicy.SOFT_REFRESH));

      Object raw = caffeineCache.getIfPresent("key1");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      CacheEntry entry = (CacheEntry) raw;
      assertThat(entry.getHardTtlMs()).isEqualTo(50000L);
      assertThat(entry.getSoftTtlMs()).isEqualTo(5000L);
      assertThat(entry.getKeyState()).isEqualTo(KeyState.NORMAL);
    }

    @Test
    @DisplayName("getWithSoftExpire with COOL expired entry refreshes and downgrades to NORMAL")
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

      hotKeyCache.getWithSoftExpire("key", CachePolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

      // COOL entries are proactively refreshed; the success downgrades the
      // entry to NORMAL (value and soft-expiry timestamp updated).
      CacheEntry after = (CacheEntry) caffeineCache.getIfPresent("key");
      assertThat(after.getValue()).isEqualTo("fresh");
      assertThat(after.getSoftExpireAtMs()).isGreaterThan(System.currentTimeMillis() - 500);
      assertThat(after.getKeyState()).isEqualTo(KeyState.NORMAL);
    }

    @Test
    @DisplayName("getWithSoftExpire with COOL entry and zero soft TTL refreshes")
    void getWithSoftExpire_coolEntryWithZeroSoft_shouldRefresh() {
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
      when(singleFlight.load(anyString(), any())).thenReturn(vv("fresh"));

      hotKeyCache.getWithSoftExpire("key", CachePolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

      CacheEntry after = (CacheEntry) caffeineCache.getIfPresent("key");
      assertThat(after.getValue()).isEqualTo("fresh");
      assertThat(after.getKeyState()).isEqualTo(KeyState.NORMAL);
    }

    @Test
    @DisplayName("get with TTL overrides and hot detection uses TTL overrides in hot path")
    void get_withTtlOverridesAndHotDetection_usesTtlOverrides() {
      when(singleFlight.load(anyString(), any())).thenReturn(vv("hot-value"));
      when(hotKeyDetector.contains("key1")).thenReturn(true);

      hotKeyCache.get("key1", CachePolicy.of(() -> "hot-value", 80000L, 8000L, true, true, StalePolicy.SOFT_REFRESH));

      Object raw = caffeineCache.getIfPresent("key1");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      CacheEntry entry = (CacheEntry) raw;
      assertThat(entry.getHardTtlMs()).isEqualTo(3_600_000L);
      assertThat(entry.getSoftTtlMs()).isEqualTo(300_000L);
      assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
    }

    @Test
    @DisplayName("get with ALLOW_NO_REPORT and hot detection skips reportToWorker")
    void get_withAllowNoReportAndHotDetection_skipsReport() {
      hotKeyCache.addWhitelist("noreport-hot");
      when(singleFlight.load(eq("noreport-hot"), any())).thenReturn(vv("value"));
      when(hotKeyDetector.contains("noreport-hot")).thenReturn(true);

      hotKeyCache.get("noreport-hot", CachePolicy.of(() -> "value", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

      Object raw = caffeineCache.getIfPresent("noreport-hot");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      assertThat(((CacheEntry) raw).getValue()).isEqualTo("value");
    }

    @Test
    @DisplayName("get with ALLOW_NO_REPORT and no hot detection skips reportToWorker")
    void get_withAllowNoReportAndNoHotDetection_skipsReport() {
      hotKeyCache.addWhitelist("noreport-normal");
      when(singleFlight.load(eq("noreport-normal"), any())).thenReturn(vv("value"));
      when(hotKeyDetector.contains("noreport-normal")).thenReturn(false);

      hotKeyCache.get("noreport-normal", CachePolicy.of(() -> "value", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

      Object raw = caffeineCache.getIfPresent("noreport-normal");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      assertThat(((CacheEntry) raw).getKeyState()).isEqualTo(KeyState.NORMAL);
    }

    @Test
    @DisplayName("processLocalHotkeyIfNeeded should NOT extend HOT entry past 75% TTL — hard TTL is the bound")
    void processLocalHotkeyIfNeeded_shouldNotExtendHotExpiry() {
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

      hotKeyCache.get("key1", CachePolicy.of(() -> "loaded", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

      CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key1");
      assertThat(entry.getHardExpireAtMs()).isEqualTo(originalExpireAt);
    }

    @Test
    @DisplayName("processLocalHotkeyIfNeeded should NOT extend HOT entry when within first 25%")
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

      hotKeyCache.get("key1", CachePolicy.of(() -> "loaded", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

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

      hotKeyCache.get("key1", CachePolicy.of(() -> "loaded", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

      CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key1");
      assertThat(entry.getHardExpireAtMs()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    @DisplayName("loadAndCache preserves Worker-managed entry in hot path")
    void loadAndCache_withWorkerManagedEntryInHotPath_preservesIt() {
      when(hotKeyDetector.contains("key1")).thenReturn(true);
      when(singleFlight.load(eq("key1"), any())).thenAnswer(invocation -> {
        Supplier<?> reader = invocation.getArgument(1);
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

      Optional<String> result = hotKeyCache.get(
        "key1",
        CachePolicy.of(() -> "newValue", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH)
      );

      assertThat(result).contains("newValue");
      Object raw = caffeineCache.getIfPresent("key1");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      CacheEntry entry = (CacheEntry) raw;
      assertThat(entry.getValue()).isEqualTo("workerValue");
    }

    /**
     * peekAndTag on a whitelisted key keeps the detection half (the whitelist
     * semantics are "detect without reporting", never "ignore") while the
     * Worker report is skipped — the exact flag combination
     * {@code tag(key, false, true)} would produce.
     */
    @Test
    @DisplayName("peekAndTag on a whitelisted key detects without reporting")
    void peekAndTag_whitelistedKey_detectsWithoutReporting() {
      hotKeyCache.addWhitelist("wl::key");
      caffeineCache.put(
        "wl::key",
        CacheEntry.builder()
          .value("stored")
          .dataVersion(1)
          .isVersionDegraded(false)
          .decisionVersion(0)
          .hardTtlMs(300_000)
          .hardExpireAtMs(Long.MAX_VALUE)
          .softTtlMs(30_000)
          .softExpireAtMs(Long.MAX_VALUE)
          .keyState(KeyState.HOT)
          .build()
      );

      assertThat(hotKeyCache.peekAndTag("wl::key")).isEqualTo("stored");

      verify(hotKeyDetector).add("wl::key");
      verify(reporter, never()).reportToWorker("wl::key");
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

    hotKeyCache.putThrough("key1", "newValue", () -> {}, 0L, 0L, true);

    Object raw = caffeineCache.getIfPresent("key1");
    assertThat(raw).isInstanceOf(CacheEntry.class);
    CacheEntry entry = (CacheEntry) raw;
    assertThat(entry.getValue()).isEqualTo("original");
  }

  // ── getAndSet ──

  @Test
  void getAndSet_shouldReplaceValueAndReturnOld() {
    caffeineCache.put("k", CacheEntry.builder().value("old").build());
    Optional<String> result = hotKeyCache.getAndSet("k", "new", 0L, 0L);
    assertThat(result).contains("old");
    assertThat(((CacheEntry) caffeineCache.getIfPresent("k")).getValue()).isEqualTo("new");
  }

  @Test
  void getAndSet_withAbsentKey_shouldReturnEmpty() {
    assertThat(hotKeyCache.getAndSet("k", "new", 0L, 0L)).isEmpty();
    assertThat(((CacheEntry) caffeineCache.getIfPresent("k")).getValue()).isEqualTo("new");
  }

  @Test
  void getAndSet_shouldPreserveExistingMetadata() {
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

    hotKeyCache.getAndSet("k", "new", 0L, 0L);

    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(entry.getValue()).isEqualTo("new");
    assertThat(entry.getDataVersion()).isEqualTo(42);
    assertThat(entry.getDecisionVersion()).isEqualTo(7);
    assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(entry.getHardTtlMs()).isEqualTo(300_000);
    assertThat(entry.getSoftTtlMs()).isEqualTo(30_000);
  }

  @Test
  void getAndSet_withBlacklistedKey_shouldThrow() {
    hotKeyCache.addBlacklist("block:*");
    assertThatThrownBy(() -> hotKeyCache.getAndSet("block:k", "v", 0L, 0L)).isInstanceOf(ZetaBlockedException.class);
  }

  @Test
  void getAndSet_withInvalidKey_shouldReturnEmpty() {
    assertThat(hotKeyCache.getAndSet(null, "v", 0L, 0L)).isEmpty();
    assertThat(hotKeyCache.getAndSet("", "v", 0L, 0L)).isEmpty();
  }

  @Test
  void getAndSet_withNullNewValue_shouldStoreNullViaSentinel() {
    caffeineCache.put("k", CacheEntry.builder().value("old").build());
    Optional<String> result = hotKeyCache.getAndSet("k", null, 0L, 0L);
    assertThat(result).contains("old");
    assertThat(hotKeyCache.peek("k")).isEmpty();
  }

  @Test
  void getAndSet_withExistingNullValue_shouldReturnEmpty() {
    caffeineCache.put("k", CacheEntry.builder().value(null).build());
    assertThat(hotKeyCache.getAndSet("k", "new", 0L, 0L)).isEmpty();
    assertThat(((CacheEntry) caffeineCache.getIfPresent("k")).getValue()).isEqualTo("new");
  }

  @Test
  void getAndSet_withCustomTtlOnAbsent_shouldUseCustomTtl() {
    hotKeyCache.getAndSet("k", "v", 50000L, 5000L);
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(entry.getHardTtlMs()).isEqualTo(50000L);
    assertThat(entry.getSoftTtlMs()).isEqualTo(5000L);
  }

  @Test
  void getAndSet_withBareObject_shouldReturnBareAndWrapInEntry() {
    caffeineCache.put("k", "bare");
    Optional<String> result = hotKeyCache.getAndSet("k", "new", 0L, 0L);
    assertThat(result).contains("bare");
    assertThat(caffeineCache.getIfPresent("k")).isInstanceOf(CacheEntry.class);
    assertThat(((CacheEntry) caffeineCache.getIfPresent("k")).getValue()).isEqualTo("new");
  }

  @Test
  void getAndSet_shouldNotBumpDataVersionOrBroadcast() {
    caffeineCache.put(
      "k",
      CacheEntry.builder()
        .value("old")
        .dataVersion(100)
        .decisionVersion(50)
        .hardTtlMs(300_000)
        .softTtlMs(30_000)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );

    Optional<String> result = hotKeyCache.getAndSet("k", "new", 0L, 0L);
    assertThat(result).contains("old");

    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(entry.getDataVersion()).isEqualTo(100);
    assertThat(entry.getDecisionVersion()).isEqualTo(50);
    assertThat(entry.getValue()).isEqualTo("new");
  }

  @Test
  void getAndSet_shouldInvalidateSingleFlightDedup() {
    // Same ADR-0067 contract as putIfAbsent: getAndSet replaces the stored value
    // on every path (its compute never returns null) while writing
    // VERSION_DEFAULT, so the dedup invalidation is what prevents an in-flight
    // pre-write load result from being replayed over this write.
    hotKeyCache.getAndSet("k", "new", 0L, 0L);
    verify(singleFlight).invalidate("k");
  }

  @Test
  void getAndSet_whenKeyAbsent_shouldStillInvalidateSingleFlightDedup() {
    // The absent-key path creates a fresh VERSION_DEFAULT entry, which is exactly
    // the case VersionGuard cannot reject on replay — so it must invalidate too.
    assertThat(hotKeyCache.getAndSet("absentKey", "v", 0L, 0L)).isEmpty();
    verify(singleFlight).invalidate("absentKey");
  }

  // ── Batch null-sentinel penetration protection (single-key parity) ──

  /**
   * Batch null-sentinel parity (cache-penetration protection): a batch
   * {@code get} over a key holding a valid {@link NullValue} sentinel serves an
   * empty value <b>without</b> invoking the reader or SingleFlight — the batch
   * loop used to treat the sentinel as a miss and reload it on every call,
   * unlike the single-key {@code get} contract.
   */
  @Test
  void getAll_sentinelHit_shouldServeEmptyWithoutReload() {
    AtomicInteger readerCalls = new AtomicInteger();
    caffeineCache.put("sentinel", nullSentinelEntry());
    caffeineCache.put("hit", "rawHit");

    Map<String, Optional<String>> results = hotKeyCache.get(
      List.of("sentinel", "hit"),
      key -> {
        readerCalls.incrementAndGet();
        return "loaded";
      },
      0L,
      0L,
      false,
      false
    );

    assertThat(results.get("sentinel")).isEmpty();
    assertThat(results.get("hit")).contains("rawHit");
    assertThat(readerCalls.get()).isZero();
    verifyNoInteractions(singleFlight);
  }

  /**
   * Batch null-sentinel parity for {@code getWithSoftExpire}: same contract as
   * the batch {@code get} — a sentinel hit is served empty without a reload.
   */
  @Test
  void getAllWithSoftExpire_sentinelHit_shouldServeEmptyWithoutReload() {
    AtomicInteger readerCalls = new AtomicInteger();
    caffeineCache.put("sentinel", nullSentinelEntry());
    caffeineCache.put("hit", "rawHit");

    Map<String, Optional<String>> results = hotKeyCache.getWithSoftExpire(
      List.of("sentinel", "hit"),
      key -> {
        readerCalls.incrementAndGet();
        return "loaded";
      },
      0L,
      0L,
      false,
      false
    );

    assertThat(results.get("sentinel")).isEmpty();
    assertThat(results.get("hit")).contains("rawHit");
    assertThat(readerCalls.get()).isZero();
    verifyNoInteractions(singleFlight);
  }

  /**
   * A valid {@link NullValue} sentinel entry within its hard TTL (shape shared
   * with {@code computeIfAbsent_nullSentinelHit_shouldReturnEmptyWithoutInvokingReader}).
   */
  private static CacheEntry nullSentinelEntry() {
    return CacheEntry.builder()
      .value(NullValue.INSTANCE)
      .dataVersion(1)
      .isVersionDegraded(false)
      .decisionVersion(0)
      .hardTtlMs(10_000)
      .hardExpireAtMs(System.currentTimeMillis() + 10_000)
      .softTtlMs(0L)
      .softExpireAtMs(0L)
      .keyState(KeyState.NORMAL)
      .normalHardTtlMs(10_000)
      .normalSoftTtlMs(0L)
      .build();
  }

  /**
   * Contract parity with {@code get}: a {@link ZetaBlockedException} raised on
   * the load path (a blocked reader surfaced through SingleFlight, or the
   * post-load BLOCK re-check) propagates out of {@code computeIfAbsent} even
   * with {@code failOnError = false} — the computeInLock catch used to swallow
   * it into an empty result.
   */
  @Test
  void computeIfAbsent_blockedKeyOnLoad_shouldPropagateZetaBlockedException() {
    when(singleFlight.load(anyString(), any())).thenThrow(new ZetaBlockedException("test", "key1"));

    assertThatThrownBy(() ->
      hotKeyCache.computeIfAbsent("key1", CachePolicy.of(() -> "loaded", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))
    ).isInstanceOf(ZetaBlockedException.class);
  }

  /**
   * {@code tag} normalizes like every other entry point: with query stripping
   * enabled, tagging {@code "a?x=1"} counts the same key identity ({@code "a"})
   * that {@code get("a")} sees — previously the un-normalized key reached the
   * detector and split the count.
   */
  @Test
  void tag_shouldNormalizeKeyWhenStripQueryEnabled() {
    ttlConfig.getCacheKey().setStripQuery(true);

    hotKeyCache.tag("a?x=1");
    hotKeyCache.tag("b?y=2", false, false);

    verify(hotKeyDetector).add("a");
    verify(hotKeyDetector).add("b");
    verify(hotKeyDetector, never()).add("a?x=1");
    verify(hotKeyDetector, never()).add("b?y=2");
  }

  /**
   * REVALIDATE on a soft-expired Worker-managed entry keeps the entry (and its
   * decision stamp) in L1 while still serving a freshly loaded value — the
   * former drop-and-reload rebuilt the entry locally without Worker metadata,
   * diverging from computeInLock's REVALIDATE and loadCacheEntry's
   * Worker-managed guard. Keep-and-renew: the kept entry's soft expiry is
   * renewed (within the untouched hard TTL) so the loader is not re-invoked
   * at read rate — COOL entries never rebroadcast, and HOT entries only get
   * renewal from the Worker on the ADR-0024 cadence.
   */
  @Test
  void getWithSoftExpire_revalidateOnSoftExpiredWorkerHotEntry_shouldKeepEntryAndDecisionStamp() {
    when(healthView.isAlive("w1")).thenReturn(true);
    when(healthView.epochOf("w1")).thenReturn(5L);
    CacheEntry hotEntry = softExpiredWorkerHotEntry("w1", 5L);
    caffeineCache.put("key1", hotEntry);
    when(singleFlight.load(anyString(), any())).thenReturn(vv("loadedValue"));

    Optional<String> result = hotKeyCache.getWithSoftExpire(
      "key1",
      CachePolicy.of(() -> "loadedValue", 0L, 0L, true, true, StalePolicy.REVALIDATE)
    );

    assertThat(result).contains("loadedValue");
    CacheEntry kept = (CacheEntry) caffeineCache.getIfPresent("key1");
    assertThat(kept.getDecisionNodeId()).isEqualTo("w1");
    assertThat(kept.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(kept.getHardExpireAtMs()).isEqualTo(Long.MAX_VALUE);
    // Keep-and-renew: the soft expiry moved past the read (clamped by the
    // hard TTL — Long.MAX_VALUE here), so the loader is not re-invoked at
    // read rate and the entry instance was rewritten by the renewal.
    assertThat(expireManager.ttlPolicy().isSoftExpired(kept)).isFalse();
    assertThat(kept).isNotSameAs(hotEntry);
  }

  // ── peekAndTag: the annotation-path peek (single lookup + single rule evaluation) ──

  /**
   * A served hit (value) tags the detector — the annotation path's "every read
   * triggers detection" invariant delivered in the merged peekAndTag form.
   */
  @Test
  void peekAndTag_hit_returnsValueAndTagsDetector() {
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
        .softExpireAtMs(Long.MAX_VALUE)
        .keyState(KeyState.HOT)
        .build()
    );

    assertThat(hotKeyCache.peekAndTag("key1")).isEqualTo("stored");
    verify(hotKeyDetector).add("key1");
  }

  /** A raw (non-CacheEntry) L1 value is served as-is and still counted. */
  @Test
  void peekAndTag_rawValue_servedAndTagged() {
    caffeineCache.put("raw1", "plain");

    assertThat(hotKeyCache.peekAndTag("raw1")).isEqualTo("plain");
    verify(hotKeyDetector).add("raw1");
  }

  /** A true miss does neither detection nor reporting. */
  @Test
  void peekAndTag_miss_doesNotTag() {
    assertThat(hotKeyCache.peekAndTag("absent")).isNull();
    verify(hotKeyDetector, never()).add("absent");
  }

  /** A cached-null sentinel hit returns the sentinel itself and is still counted. */
  @Test
  void peekAndTag_nullSentinel_returnsSentinelAndTags() {
    caffeineCache.put(
      "nk",
      CacheEntry.builder()
        .value(NullValue.INSTANCE)
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(Long.MAX_VALUE)
        .keyState(KeyState.NORMAL)
        .build()
    );

    assertThat(hotKeyCache.peekAndTag("nk")).isSameAs(NullValue.INSTANCE);
    verify(hotKeyDetector).add("nk");
  }

  /** A blocked key throws via preGuard — before any lookup or tagging. */
  @Test
  void peekAndTag_blockedKey_throwsAndDoesNotTag() {
    hotKeyCache.addBlacklist("blk");
    caffeineCache.put("blk", "v");

    assertThatThrownBy(() -> hotKeyCache.peekAndTag("blk")).isInstanceOf(ZetaBlockedException.class);
    verify(hotKeyDetector, never()).add("blk");
  }

  /** A Worker-stamped HOT entry whose soft TTL has passed while the hard TTL is still valid. */
  private static CacheEntry softExpiredWorkerHotEntry(String nodeId, long epoch) {
    return CacheEntry.builder()
      .value("stored")
      .dataVersion(1)
      .isVersionDegraded(false)
      .decisionVersion(42)
      .decisionNodeId(nodeId)
      .decisionEpoch(epoch)
      .hardTtlMs(3_600_000)
      .hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(300_000)
      .softExpireAtMs(1L)
      .keyState(KeyState.HOT)
      .normalHardTtlMs(60_000)
      .normalSoftTtlMs(15_000)
      .build();
  }

  /**
   * Write/invalidation → dedup-cache coherence and load-path version guards
   * (ADR-0067). Uses a REAL {@link SingleFlightImpl} so the completed-future
   * reuse window (ADR-0002) is exercised end-to-end against the invalidation
   * and write paths.
   */
  @Nested
  @DisplayName("Dedup coherence and load guards (ADR-0067)")
  class DedupCoherenceTest {

    private HotKeyCache realFlightCache;
    private Cache<String, Object> realFlightL1;
    private VersionController versionController;

    @BeforeEach
    void setUp() {
      realFlightL1 = Caffeine.newBuilder().maximumSize(100).build();
      CircuitBreaker breaker = mock(CircuitBreaker.class);
      when(breaker.isOpen()).thenReturn(false);
      when(breaker.allowRequest()).thenReturn(true);
      SingleFlight realSingleFlight = new SingleFlightImpl(1000, 5, 5, Runnable::run, breaker);
      versionController = mock(VersionController.class);
      // Default probe: withheld (unstamped). Tests re-stub per scenario.
      when(versionController.currentVersion(anyString())).thenReturn(Optional.empty());
      when(versionController.nextVersion(anyString())).thenReturn(new VersionController.VersionResult(4L, false));

      realFlightCache = new HotKeyCache(
        hotKeyDetector,
        realFlightL1,
        realSingleFlight,
        new ExpireManagerImpl(realFlightL1, Runnable::run, ttlConfig, 10, CacheCompressor.NONE, healthView),
        Runnable::run,
        new CentralDispatcher(
          Optional.empty(),
          Optional.empty(),
          new BroadcastBuffer(scheduler, Optional.empty()),
          hotKeyDetector
        ),
        new RuleMatcherImpl(Optional.empty(), Optional.empty()),
        versionController,
        ttlConfig,
        healthView,
        CacheCompressor.NONE
      );
    }

    /**
     * The AUDIT-2 regression: a completed dedup future carrying the pre-write
     * value must not be replayed onto a post-write miss — putThrough
     * invalidates the dedup entry, so the next miss re-invokes the reader.
     */
    @Test
    void putThrough_invalidatesDedupEntry_postWriteMissRereadsSource() {
      AtomicInteger readerCalls = new AtomicInteger();
      assertThat(realFlightCache.get("k", CachePolicy.of(() -> {
        readerCalls.incrementAndGet();
        return "A";
      }, 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))).contains("A");

      realFlightCache.putThrough("k", "B", () -> {}, 0, 0, true);
      assertThat(realFlightCache.peek("k")).contains("B");

      // Simulate the L1 loss the dedup replay would ride on: an eviction, a
      // peer INVALIDATE, or (pre-ADR-0067) the sender's own REFRESH drop.
      realFlightL1.invalidate("k");

      Optional<String> after = realFlightCache.get("k", CachePolicy.of(() -> {
        readerCalls.incrementAndGet();
        return "B-fresh";
      }, 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

      assertThat(after).contains("B-fresh");
      assertThat(readerCalls.get()).as("the post-write miss must re-invoke the reader").isEqualTo(2);
    }

    /** Same dedup contract for {@code invalidate}: the next miss re-reads. */
    @Test
    void invalidate_invalidatesDedupEntry_nextMissRereadsSource() {
      AtomicInteger readerCalls = new AtomicInteger();
      assertThat(realFlightCache.get("k", CachePolicy.of(() -> {
        readerCalls.incrementAndGet();
        return "A";
      }, 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))).contains("A");

      realFlightCache.invalidate("k", false);

      Optional<String> after = realFlightCache.get("k", CachePolicy.of(() -> {
        readerCalls.incrementAndGet();
        return "fresh";
      }, 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

      assertThat(after).contains("fresh");
      assertThat(readerCalls.get()).isEqualTo(2);
    }

    /**
     * The load-path version guard: a load whose reader raced a concurrent
     * putThrough (read pre-commit, ADR-0033 probe stamped post-INCR at the
     * same version) must not regress L1 — the write's value survives.
     */
    @Test
    void stampedLoad_notNewerThanExisting_refusesToRegressConcurrentWrite() {
      when(versionController.currentVersion("k")).thenReturn(Optional.of(4L));

      Optional<String> served = realFlightCache.get("k", CachePolicy.of(() -> {
        // Concurrent write landing mid-load, before the load's own probe.
        realFlightCache.putThrough("k", "B", () -> {}, 0, 0, false);
        return "A-old";
      }, 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

      // The caller still sees what its reader loaded (the read raced the
      // commit), but L1 must hold the write's value, not the stale reload.
      assertThat(served).contains("A-old");
      CacheEntry kept = (CacheEntry) realFlightL1.getIfPresent("k");
      assertThat(kept.getDataVersion()).isEqualTo(4L);
      assertThat(kept.getValue()).isEqualTo("B");
    }

    /**
     * A null load replaces a Worker-managed entry that landed mid-load — the
     * source's null answer is authoritative for the VALUE — but the decision
     * stamp is carried into the sentinel (the pinned stamp-preservation
     * semantics): state NORMAL, short null TTL, stamp intact for later
     * decisionVersion ordering.
     */
    @Test
    void nullLoad_replacesWorkerManagedEntry_preservingDecisionStamp() {
      when(versionController.currentVersion("k")).thenReturn(Optional.of(6L));

      Optional<String> served = realFlightCache.get("k", CachePolicy.of(() -> {
        realFlightL1.put("k", workerHotEntry("w1", 1L));
        return null;
      }, 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

      assertThat(served).isEmpty();
      CacheEntry sentinel = (CacheEntry) realFlightL1.getIfPresent("k");
      assertThat(sentinel.getValue()).isEqualTo(NullValue.INSTANCE);
      assertThat(sentinel.getKeyState()).isEqualTo(KeyState.NORMAL);
      assertThat(sentinel.getDecisionNodeId()).isEqualTo("w1");
      assertThat(sentinel.getDecisionEpoch()).isEqualTo(1L);
      assertThat(sentinel.getDataVersion()).isEqualTo(6L);
    }

    /** A null load at the same version must not clobber an existing normal entry. */
    @Test
    void nullLoad_doesNotClobberSameVersionEntry() {
      when(versionController.currentVersion("k")).thenReturn(Optional.of(4L));

      realFlightCache.get("k", CachePolicy.of(() -> {
        realFlightL1.put("k", normalEntry(4L));
        return null;
      }, 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

      CacheEntry kept = (CacheEntry) realFlightL1.getIfPresent("k");
      assertThat(kept.getValue()).isEqualTo("v");
      assertThat(kept.getDataVersion()).isEqualTo(4L);
    }

    /** An unstamped (fail-open) null load carries no version authority: it never replaces an entry. */
    @Test
    void unstampedNullLoad_doesNotReplaceExistingEntry() {
      realFlightCache.get("k", CachePolicy.of(() -> {
        realFlightL1.put("k", normalEntry(3L));
        return null;
      }, 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

      CacheEntry kept = (CacheEntry) realFlightL1.getIfPresent("k");
      assertThat(kept.getValue()).isEqualTo("v");
      assertThat(kept.getDataVersion()).isEqualTo(3L);
    }

    /** A null load probed strictly newer than the entry replaces it with a stamped sentinel. */
    @Test
    void nullLoad_probedNewer_replacesOlderEntryWithSentinel() {
      when(versionController.currentVersion("k")).thenReturn(Optional.of(4L));

      realFlightCache.get("k", CachePolicy.of(() -> {
        realFlightL1.put("k", normalEntry(3L));
        return null;
      }, 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

      CacheEntry sentinel = (CacheEntry) realFlightL1.getIfPresent("k");
      assertThat(sentinel.getValue()).isEqualTo(NullValue.INSTANCE);
      assertThat(sentinel.getDataVersion()).isEqualTo(4L);
      assertThat(sentinel.getKeyState()).isEqualTo(KeyState.NORMAL);
    }

    /**
     * Same dedup contract for {@code compareAndInvalidate}: the removed key's
     * completed dedup future must not be replayed onto the next miss — the
     * replay would re-cache the value the caller just invalidated (the
     * ADR-0067 compound repro shape on a second removal path).
     */
    @Test
    void compareAndInvalidate_invalidatesDedupEntry_nextMissRereadsSource() {
      AtomicInteger readerCalls = new AtomicInteger();
      assertThat(realFlightCache.get("k", CachePolicy.of(() -> {
        readerCalls.incrementAndGet();
        return "A";
      }, 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))).contains("A");

      assertThat(realFlightCache.compareAndInvalidate("k", "A")).isTrue();
      assertThat(realFlightL1.getIfPresent("k")).isNull();

      Optional<String> after = realFlightCache.get("k", CachePolicy.of(() -> {
        readerCalls.incrementAndGet();
        return "fresh";
      }, 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

      assertThat(after).contains("fresh");
      assertThat(readerCalls.get()).as("the post-invalidation miss must re-invoke the reader").isEqualTo(2);
    }

    /** A mismatching compareAndInvalidate removes nothing and must not touch the dedup cache. */
    @Test
    void compareAndInvalidate_mismatch_keepsEntryAndDedupFuture() {
      AtomicInteger readerCalls = new AtomicInteger();
      assertThat(realFlightCache.get("k", CachePolicy.of(() -> {
        readerCalls.incrementAndGet();
        return "A";
      }, 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))).contains("A");

      assertThat(realFlightCache.compareAndInvalidate("k", "other")).isFalse();
      assertThat(realFlightCache.peek("k")).contains("A");

      // The entry survived, so its still-valid dedup future may serve the next hit.
      assertThat(readerCalls.get()).isEqualTo(1);
    }

    /**
     * Same dedup contract for {@code invalidateAllLocal}: the emergency flush
     * must also clear the dedup cache — otherwise every flushed key with a
     * completed future is re-cached from the replay within the dedup TTL.
     */
    @Test
    void invalidateAllLocal_clearsDedupCache_nextMissRereadsSource() {
      AtomicInteger readerCalls = new AtomicInteger();
      assertThat(realFlightCache.get("k", CachePolicy.of(() -> {
        readerCalls.incrementAndGet();
        return "A";
      }, 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))).contains("A");

      realFlightCache.invalidateAllLocal();
      assertThat(realFlightL1.getIfPresent("k")).isNull();

      Optional<String> after = realFlightCache.get("k", CachePolicy.of(() -> {
        readerCalls.incrementAndGet();
        return "fresh";
      }, 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

      assertThat(after).contains("fresh");
      assertThat(readerCalls.get()).as("the post-flush miss must re-invoke the reader").isEqualTo(2);
    }

    /**
     * REVALIDATE drop-and-reload of a soft-expired NORMAL entry must re-invoke
     * the loader: the drop removes the L1 entry, so the ADR-0067 contract
     * requires dropping the dedup entry too — without it, the reload replays
     * the completed future (the pre-drop value) instead of the fresh load.
     */
    @Test
    void revalidateDropOfSoftExpiredEntry_reinvokesLoader() {
      AtomicInteger readerCalls = new AtomicInteger();
      assertThat(realFlightCache.get("k", CachePolicy.of(() -> {
        readerCalls.incrementAndGet();
        return "A";
      }, 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))).contains("A");

      // Age the entry past its soft TTL (hard TTL stays valid) so the next
      // REVALIDATE read takes the drop-and-reload branch.
      // Age the entry past its soft expiry with an explicit timestamp: the
      // REVALIDATE branch only consults softExpireAtMs, so the duration stays as stored.
      CacheEntry stale = EntryDraft.of((CacheEntry) realFlightL1.getIfPresent("k")).softExpiryAt(1L).build();
      realFlightL1.put("k", stale);

      Optional<String> after = realFlightCache.get("k", CachePolicy.of(() -> {
        readerCalls.incrementAndGet();
        return "fresh";
      }, 0L, 0L, true, true, StalePolicy.REVALIDATE));

      assertThat(after).contains("fresh");
      assertThat(readerCalls.get()).as("the drop-and-reload must re-invoke the loader").isEqualTo(2);
      assertThat(realFlightL1.getIfPresent("k")).isInstanceOf(CacheEntry.class);
      assertThat(((CacheEntry) realFlightL1.getIfPresent("k")).getValue()).isEqualTo("fresh");
    }

    private static CacheEntry normalEntry(long dataVersion) {
      return CacheEntry.builder()
        .value("v")
        .dataVersion(dataVersion)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(0)
        .softExpireAtMs(0)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(0)
        .build();
    }
  }
}
