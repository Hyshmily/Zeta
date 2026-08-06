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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.annotation.annotationsupporter.NullValue;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.BroadcastBuffer;
import io.github.hyshmily.zeta.cache.cachesupport.ExpireManager;
import io.github.hyshmily.zeta.cache.cachesupport.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.impl.ExpireManagerImpl;
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
import io.github.hyshmily.zeta.util.version.impl.VersionControllerImpl;
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

  @BeforeEach
  void setUp() {
    hotKeyDetector = mock(HotKeyDetector.class);
    when(hotKeyDetector.contains(anyString())).thenReturn(false);
    caffeineCache = Caffeine.newBuilder().maximumSize(100).build();
    singleFlight = mock(SingleFlight.class);
    executor = Runnable::run;
    ZetaProperties ttlConfig = new ZetaProperties();
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
      // only the Exception catch block bypasses it via forceUpdate=true.
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
}
