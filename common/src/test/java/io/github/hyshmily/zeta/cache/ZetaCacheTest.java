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
import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.circuitbreaker.CircuitBreaker;
import io.github.hyshmily.zeta.cache.cachesupport.circuitbreaker.CircuitBreakerRegistry;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycleImpl;
import io.github.hyshmily.zeta.cache.cachesupport.loader.LoadStatus;
import io.github.hyshmily.zeta.cache.cachesupport.loader.LoaderOutcome;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlightImpl;
import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.InvalidatePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.StalePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.cache.cachesupport.raw.VersionedValue;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.model.ZetaCacheStats;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.scheduler.BackgroundRefresher;
import io.github.hyshmily.zeta.scheduler.DefaultBackgroundRefresher;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.util.clock.MonotonicStampClock;
import io.github.hyshmily.zeta.util.clock.StampClock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Tests for {@link HotKeyCache}, covering peek, get, invalidate, and TTL behaviors.
 */
class ZetaCacheTest {

  private AccessCounter accessCounter;

  private HotKeyDetector hotKeyDetector;
  private Cache<String, CacheEntry> caffeineCache;
  private SingleFlight singleFlight;
  private EntryLifecycle entryLifecycle;
  private BackgroundRefresher backgroundRefresher;
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
    entryLifecycle = new EntryLifecycleImpl(caffeineCache, ttlConfig, healthView);
    backgroundRefresher = new DefaultBackgroundRefresher(caffeineCache, executor, entryLifecycle, 10);
    scheduler = Executors.newSingleThreadScheduledExecutor();
    accessCounter = mock(AccessCounter.class);

    hotKeyCache = new HotKeyCache(
      hotKeyDetector,
      caffeineCache,
      singleFlight,
      entryLifecycle,
      backgroundRefresher,
      accessCounter,
      new BroadcastBuffer(scheduler, null, 500, 2_000, null),
      new MonotonicStampClock(1L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS),
      healthView
    );
  }

  /**
   * Explicit-outcome stub carrier for the composite-load contract (design decisions: version stamps):
   * the supplier handed to {@code singleFlight.loadOutcome} returns a
   * {@link VersionedValue}. The stamp is a real (positive) pre-read stamp:
   * production loads always allocate one, and an unknown stamp would be
   * refused by any resident entry.
   */
  @SuppressWarnings("unchecked")
  private static LoaderOutcome<Object> oc(String value) {
    return LoaderOutcome.loaded(new VersionedValue(value, 50L));
  }

  /** Caller-affine read used by the annotation path: the loader keeps the caller's transaction. */
  private static GetPolicy callerRuns() {
    GetPolicy base = GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH);
    return new GetPolicy(base.hardTtlMs(), base.softTtlMs(), base.stalePolicy(), base.report(), true);
  }

  @Test
  @DisplayName("putAll stores every key with one contiguous version block")
  void putAll_storesEveryKeyWithContiguousVersions() {
    Map<String, Object> entries = new LinkedHashMap<>();
    entries.put("a", "1");
    entries.put("b", "2");

    hotKeyCache.putAll(entries, 0L, 0L, false);

    CacheEntry first = caffeineCache.getIfPresent("a");
    CacheEntry second = caffeineCache.getIfPresent("b");
    assertThat(first).isNotNull();
    assertThat(second).isNotNull();
    assertThat(first.getValue()).isEqualTo("1");
    assertThat(second.getValue()).isEqualTo("2");
    assertThat(second.getDataVersion()).isEqualTo(first.getDataVersion() + 1);
  }

  @Test
  @DisplayName("putAll inside a transaction stores the batch only after commit")
  void putAll_insideTransactionStoresAfterCommit() {
    Map<String, Object> entries = Map.of("a", "1", "b", "2");

    TransactionSynchronizationManager.initSynchronization();
    try {
      hotKeyCache.putAll(entries, 0L, 0L, false);
      assertThat(caffeineCache.getIfPresent("a")).isNull();
      assertThat(caffeineCache.getIfPresent("b")).isNull();

      TransactionSynchronizationManager.getSynchronizations().forEach(TransactionSynchronization::afterCommit);

      assertThat(caffeineCache.getIfPresent("a")).isNotNull();
      assertThat(caffeineCache.getIfPresent("b")).isNotNull();
    } finally {
      TransactionSynchronizationManager.clear();
    }
  }

  @Test
  @DisplayName("a configured permanent hard TTL resolves to the sync-rejection sentinel")
  void resolveSyncableHardTtl_mapsConfiguredPermanentToSentinel() {
    ttlConfig.setHardTtlMs(0L);
    assertThat(hotKeyCache.resolveSyncableHardTtl(0L)).isEqualTo(Long.MAX_VALUE);

    ttlConfig.setHardTtlMs(300_000L);
    assertThat(hotKeyCache.resolveSyncableHardTtl(0L)).isEqualTo(300_000L);
    assertThat(hotKeyCache.resolveSyncableHardTtl(45_000L)).isEqualTo(45_000L);
  }

  @Test
  @DisplayName("a caller-affine load inside a transaction is isolated and does not populate L1")
  void loadInsideTransaction_isIsolatedAndDoesNotPopulateL1() {
    when(singleFlight.loadIsolated(anyString(), any(), any())).thenAnswer(invocation -> {
      Supplier<VersionedValue> reader = invocation.getArgument(1);
      VersionedValue value = reader.get();
      return value == null ? LoaderOutcome.sourceEmpty() : LoaderOutcome.loaded(value);
    });

    TransactionSynchronizationManager.initSynchronization();
    try {
      String value = hotKeyCache.get("tx-key", () -> "loaded", callerRuns());

      assertThat(value).isEqualTo("loaded");
      assertThat(caffeineCache.getIfPresent("tx-key"))
        .as("a transaction may read its own uncommitted writes; the value must not be published")
        .isNull();

      TransactionSynchronizationManager.getSynchronizations().forEach(TransactionSynchronization::afterCommit);

      assertThat(caffeineCache.getIfPresent("tx-key")).as("the isolated read registers no deferred store").isNull();
    } finally {
      TransactionSynchronizationManager.clear();
    }
  }

  /**
   * Verifies that peek returns a cached CacheEntry value.
   */
  @Test
  void peek_shouldReturnCachedValue() {
    caffeineCache.put(
      "key1",
      EntryDraft.blank(null)
        .value("stored")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(Long.MAX_VALUE, 30_000)
        .keyState(KeyState.HOT)
        .build()
    );

    assertThat((Object) hotKeyCache.peek("key1")).isEqualTo("stored");
  }

  /**
   * Decision-Validity demotion end-to-end (design decisions: hot state): a HOT entry stamped by a
   * dead Worker is served on the first read, and the entry is reverted in place
   * to NORMAL with the decision stamp cleared and normal TTLs — no reload, no
   * miss, no source dependency.
   */
  @Test
  void get_shouldDemoteOrphanedWorkerHotEntryInPlace() {
    when(healthView.isAlive("w1")).thenReturn(false);
    caffeineCache.put("key1", workerHotEntry("w1", 5L));

    String result = hotKeyCache.get("key1", () -> "loaded", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH));

    assertThat(result).isEqualTo("stored");
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key1");
    assertThat(entry.getKeyState()).isEqualTo(KeyState.NORMAL);
    assertThat(entry.getDecisionNodeId()).isNull();
    assertThat(entry.getDecisionVersion()).isZero();
    assertThat(entry.getDecisionEpoch()).isZero();
    assertThat(entry.getHardTtlMs()).isEqualTo(60_000);
    assertThat(entry.getValue()).isEqualTo("stored");
  }

  /**
   * A soft-expired Worker-HOT entry under REVALIDATE must be demoted before the blocking reload
   * writes: otherwise the reload preserves the dead Worker's HOT regime and lease.
   */
  @Test
  void get_revalidateDemotesOrphanedWorkerHotEntryBeforeTheReload() {
    when(healthView.isAlive("w1")).thenReturn(false);
    when(singleFlight.load(anyString(), any(), anyBoolean(), any())).thenAnswer(invocation -> {
      Supplier<VersionedValue> reader = invocation.getArgument(1);
      @SuppressWarnings("unchecked")
      Consumer<VersionedValue> commit = invocation.getArgument(3);
      VersionedValue value = reader.get();
      if (value != null) {
        commit.accept(value);
        return LoaderOutcome.loaded(value);
      }
      return LoaderOutcome.sourceEmpty();
    });
    caffeineCache.put("key1", softExpiredWorkerHotEntry("w1", 5L));

    String result = hotKeyCache.get("key1", () -> "fresh", GetPolicy.of(0L, 0L, true, StalePolicy.REVALIDATE));

    assertThat(result).isEqualTo("fresh");
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key1");
    assertThat(entry.getKeyState()).isEqualTo(KeyState.NORMAL);
    assertThat(entry.getDecisionNodeId()).isNull();
    assertThat(entry.getDecisionVersion()).isZero();
    assertThat(entry.getValue()).isEqualTo("fresh");
  }

  /**
   * Decision-Validity control (design decisions: hot state): a HOT entry stamped by a live Worker
   * with a matching epoch is untouched by the read path.
   */
  @Test
  void get_shouldKeepLiveWorkerHotEntryUnchanged() {
    when(healthView.isAlive("w1")).thenReturn(true);
    when(healthView.epochOf("w1")).thenReturn(5L);
    when(healthView.isCurrentIncarnation("w1", 5L)).thenReturn(true);
    caffeineCache.put("key1", workerHotEntry("w1", 5L));

    String result = hotKeyCache.get("key1", () -> "loaded", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH));

    assertThat(result).isEqualTo("stored");
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key1");
    assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(entry.getDecisionNodeId()).isEqualTo("w1");
    assertThat(entry.getHardTtlMs()).isEqualTo(3_600_000);
  }

  /**
   * Minimal NORMAL entry with pure logical expiry (F1 L1 type closure:
   * tests seed L1 with entries, never bare values).
   */
  private static CacheEntry plainEntry(Object value) {
    return EntryDraft.blank(null)
      .value(value)
      .version(0)
      .decision(DecisionStamp.hot(0, null, 0L))
      .ttl(300_000, 0L, 300_000, 0L)
      .expiryAt(Long.MAX_VALUE, 0L)
      .keyState(KeyState.NORMAL)
      .build();
  }

  /**
   * A NORMAL entry whose soft window has lapsed while its hard TTL is still
   * valid — the "stale but servable" shape every breaker-open fallback and
   * stale-while-revalidate test needs.
   */
  private static CacheEntry staleHitEntry(Object value) {
    long now = System.currentTimeMillis();
    return EntryDraft.blank(null)
      .value(value)
      .version(0)
      .decision(DecisionStamp.hot(0, null, 0L))
      .ttl(300_000, 30_000, 300_000, 30_000)
      .expiryAt(now + 300_000, now - 1_000)
      .keyState(KeyState.NORMAL)
      .build();
  }

  /** Build a Worker-stamped HOT entry with a 1h HOT TTL and 60s/15s normal baseline. */
  private static CacheEntry workerHotEntry(String nodeId, long epoch) {
    return EntryDraft.blank(null)
      .value("stored")
      .version(1)
      .decision(DecisionStamp.hot(42, nodeId, epoch))
      .ttl(3_600_000, 300_000, 60_000, 15_000)
      .expiryAt(Long.MAX_VALUE, Long.MAX_VALUE)
      .keyState(KeyState.HOT)
      .build();
  }

  /**
   * Decision-Validity demotion on the atomic read-through path (design decisions: hot state):
   * {@code computeIfAbsent} serves an orphaned Worker-HOT entry and reverts it
   * in place to NORMAL — the compute-in-lock hit branch must not leave the
   * orphaned stamp behind.
   */
  @Test
  void computeIfAbsent_shouldDemoteOrphanedWorkerHotEntryInPlace() {
    when(healthView.isAlive("w1")).thenReturn(false);
    caffeineCache.put("key1", workerHotEntry("w1", 5L));

    String result = hotKeyCache.get("key1", () -> "loaded", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH));

    assertThat(result).isEqualTo("stored");
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
    when(healthView.isCurrentIncarnation("w1", 5L)).thenReturn(true);
    caffeineCache.put("key1", workerHotEntry("w1", 5L));

    String result = hotKeyCache.get("key1", () -> "loaded", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH));

    assertThat(result).isEqualTo("stored");
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
      EntryDraft.blank(null)
        .value("v")
        .version(0)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(3_600_000, 300_000, 300_000, 30_000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 300_000)
        .keyState(KeyState.HOT)
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
      EntryDraft.blank(null)
        .value("v")
        .version(0)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 30_000)
        .keyState(KeyState.NORMAL)
        .build()
    );

    assertThat(hotKeyCache.isHot("normalKey")).isFalse();
  }

  /**
   * Verifies that isHot returns false for a key not present in the cache.
   */
  @Test
  void isHot_shouldReturnFalseForMissing() {
    assertThat(hotKeyCache.isHot("missing")).isFalse();
  }

  /**
   * Verifies that get returns a cached entry value without invoking the loader.
   */
  @Test
  void get_shouldReturnCachedValueOnHit() {
    caffeineCache.put("key1", plainEntry("rawValue"));
    assertThat(
      (Object) hotKeyCache.get("key1", () -> "loaded", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH))
    ).isEqualTo("rawValue");
  }

  /**
   * Verifies that get loads and caches a value on cache miss via SingleFlight.
   */
  @Test
  void get_shouldLoadAndCacheOnMiss() {
    SingleFlightStubs.stubLoad(singleFlight, oc("loadedValue"));

    String result = hotKeyCache.get("key1", () -> "loadedValue", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH));
    assertThat(result).isEqualTo("loadedValue");
    assertThat(caffeineCache.getIfPresent("key1")).isNotNull();
  }

  /**
   * Lazy TTL contract (design decisions: version stamps / {@link GetPolicy}): a plain NORMAL-entry
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
      EntryDraft.blank(null)
        .value("stored")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(Long.MAX_VALUE, Long.MAX_VALUE)
        .keyState(KeyState.NORMAL)
        .build()
    );
    when(hotKeyDetector.contains("key1")).thenReturn(false);

    String result = hotKeyCache.get(
      "key1",
      () -> "loaded",
      new GetPolicy(hardEvals::incrementAndGet, softEvals::incrementAndGet, StalePolicy.SOFT_REFRESH, true, false)
    );

    assertThat(result).isEqualTo("stored");
    assertThat(hardEvals.get()).isZero();
    assertThat(softEvals.get()).isZero();
  }

  /**
   * At-most-once TTL evaluation on the load path (the {@link GetPolicy}
   * contract): a miss that builds an entry consumes both overrides exactly
   * once, even though the effective and hot-TTL resolutions derive from the
   * same raw override.
   */
  @Test
  void get_shouldEvaluateTtlSuppliersAtMostOnceOnMiss() {
    SingleFlightStubs.stubLoad(singleFlight, oc("loadedValue"));
    AtomicInteger hardEvals = new AtomicInteger();
    AtomicInteger softEvals = new AtomicInteger();

    String result = hotKeyCache.get(
      "key1",
      () -> "loadedValue",
      new GetPolicy(hardEvals::incrementAndGet, softEvals::incrementAndGet, StalePolicy.SOFT_REFRESH, true, false)
    );

    assertThat(result).isEqualTo("loadedValue");
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
      EntryDraft.blank(null)
        .value("stale")
        .version(1)
        .decision(DecisionStamp.hot(5, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() - 1000)
        .keyState(KeyState.COOL)
        .build()
    );
    AtomicInteger hardEvals = new AtomicInteger();
    AtomicInteger softEvals = new AtomicInteger();

    String result = hotKeyCache.get(
      "key1",
      () -> "fresh",
      new GetPolicy(hardEvals::incrementAndGet, softEvals::incrementAndGet, StalePolicy.SOFT_REFRESH, true, false)
    );

    assertThat(result).isEqualTo("stale");
    assertThat(hardEvals.get()).isEqualTo(1);
    assertThat(softEvals.get()).isEqualTo(1);
  }

  /**
   * Verifies that invalidate removes a cached entry.
   */
  @Test
  void invalidate_All_shouldRemoveEntry() {
    caffeineCache.put("key1", plainEntry("value"));
    hotKeyCache.invalidate("key1", new InvalidatePolicy(InvalidatePolicy.Scope.BOTH, false));
    assertThat(caffeineCache.getIfPresent("key1")).isNull();
  }

  /**
   * Verifies that invalidateAllLocal removes multiple cached entries.
   */
  @Test
  void invalidate_All_shouldRemoveEntries() {
    caffeineCache.put("k1", plainEntry("v1"));
    caffeineCache.put("k2", plainEntry("v2"));
    hotKeyCache.invalidate(List.of("k1", "k2"), new InvalidatePolicy(InvalidatePolicy.Scope.BOTH, false));
    assertThat(caffeineCache.getIfPresent("k1")).isNull();
    assertThat(caffeineCache.getIfPresent("k2")).isNull();
  }

  @Test
  void execute_shouldPropagateReaderFailureWithoutWritingAnEntry() {
    SingleFlightStubs.stubLoad(singleFlight, LoaderOutcome.failed(new IllegalStateException("boom")));

    assertThatThrownBy(() ->
      hotKeyCache.get("key1", () -> "loaded", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH))
    )
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("boom");
    assertThat(caffeineCache.getIfPresent("key1")).isNull();
  }

  // ── A failing source is never a miss: the cause reaches the caller ──

  @Test
  void get_readerFailure_shouldPropagate() {
    SingleFlightStubs.stubLoad(singleFlight, LoaderOutcome.failed(new IllegalStateException("boom")));

    assertThatThrownBy(() -> hotKeyCache.get("key1", () -> "loaded", GetPolicy.defaults()))
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("boom");
    assertThat(caffeineCache.getIfPresent("key1")).isNull();
  }

  // ── Suppression is not a null answer: no sentinel may be pinned ──

  /**
   * A suppressed load (breaker / timeout / executor saturation) means the
   * source was never asked, so there is no authority to record "this key is
   * absent". Writing the negative sentinel here pins a fabricated miss for the
   * whole {@code null-value-ttl}: the caller sees {@code empty} on every read
   * in that window without a single source call. The correct outcome is a bare
   * miss — the next read retries.
   */
  @Test
  void get_suppressedLoad_shouldThrowAndNotWriteNullSentinel() {
    SingleFlightStubs.stubLoad(singleFlight, LoaderOutcome.suppressed(LoadStatus.SUPPRESSED_TIMEOUT));
    when(singleFlight.isBreakerOpen(anyString())).thenReturn(false);

    assertThatThrownBy(() -> hotKeyCache.get("key1", () -> "loaded", GetPolicy.defaults())).isInstanceOf(
      io.github.hyshmily.zeta.exception.ZetaSuppressedException.class
    );
    assertThat(caffeineCache.getIfPresent("key1")).as("a suppressed load must not pin a negative sentinel").isNull();
  }

  /** Every suppression reason with a closed breaker surfaces instead of masquerading as a miss. */
  @Test
  void get_allSuppressionReasons_shouldThrowAndNotWriteNullSentinel() {
    for (LoadStatus status : List.of(
      LoadStatus.SUPPRESSED_BREAKER,
      LoadStatus.SUPPRESSED_TIMEOUT,
      LoadStatus.SUPPRESSED_EXECUTOR
    )) {
      caffeineCache.invalidateAll();
      SingleFlightStubs.stubLoad(singleFlight, LoaderOutcome.suppressed(status));
      when(singleFlight.isBreakerOpen(anyString())).thenReturn(false);

      assertThatThrownBy(() -> hotKeyCache.get("key1", () -> "loaded", GetPolicy.defaults()))
        .as("status %s", status)
        .isInstanceOf(io.github.hyshmily.zeta.exception.ZetaSuppressedException.class);
      assertThat(caffeineCache.getIfPresent("key1")).as("status %s must not write a sentinel", status).isNull();
    }
  }

  /**
   * Control for the above: a suppressed load under an OPEN breaker with a
   * hard-valid entry still serves that stale value — the graceful-degradation
   * path is intact, it is only the sentinel write that is gone.
   */
  @Test
  void get_suppressedLoad_withBreakerOpenAndStaleEntry_shouldServeStaleNotSentinel() {
    caffeineCache.put("key1", staleHitEntry("stale"));
    SingleFlightStubs.stubLoad(singleFlight, LoaderOutcome.suppressed(LoadStatus.SUPPRESSED_BREAKER));
    when(singleFlight.isBreakerOpen(anyString())).thenReturn(true);

    assertThat(
      (Object) hotKeyCache.get("key1", () -> "loaded", GetPolicy.of(0L, 0L, true, StalePolicy.RETURN))
    ).isEqualTo("stale");
    assertThat(((CacheEntry) caffeineCache.getIfPresent("key1")).getValue())
      .as("the surviving entry must remain the stale value, not a NullValue sentinel")
      .isEqualTo("stale");
  }

  /**
   * The contrast case: the source really answered "absent" — under the v2
   * null contract nothing is written (no sentinel exists), so the miss is a
   * bare miss and the next read retries the source.
   */
  @Test
  void get_sourceEmpty_shouldLeaveNoEntry() {
    SingleFlightStubs.stubLoad(singleFlight, LoaderOutcome.sourceEmpty());

    assertThat((Object) hotKeyCache.get("key1", () -> null, GetPolicy.defaults())).isNull();
    assertThat(caffeineCache.getIfPresent("key1")).isNull();
  }

  @Test
  void get_emptySource_shouldLeaveNoEntry() {
    SingleFlightStubs.stubLoad(singleFlight, LoaderOutcome.sourceEmpty());

    assertThat((Object) hotKeyCache.get("key1", () -> null, GetPolicy.defaults())).isNull();
    assertThat(caffeineCache.getIfPresent("key1")).isNull();
  }

  @Test
  void computeIfAbsent_readerFailure_shouldPropagateAndPreserveExistingEntry() {
    caffeineCache.put(
      "key1",
      EntryDraft.blank(null)
        .value("old")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(System.currentTimeMillis() + 300_000, System.currentTimeMillis() - 1000)
        .keyState(KeyState.NORMAL)
        .build()
    );
    SingleFlightStubs.stubLoad(singleFlight, LoaderOutcome.failed(new IllegalStateException("boom")));

    assertThatThrownBy(() ->
      hotKeyCache.get(
        "key1",
        () -> {
          throw new IllegalStateException("boom");
        },
        GetPolicy.of(0L, 0L, true, StalePolicy.REVALIDATE)
      )
    )
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("boom");
    assertThat(caffeineCache.getIfPresent("key1")).isNotNull();
  }

  @Test
  void computeIfAbsent_miss_shouldLoadViaSingleFlightAndCacheNormalEntry() {
    SingleFlightStubs.stubLoad(singleFlight, oc("fresh"));

    assertThat((Object) hotKeyCache.get("key1", () -> "fresh", GetPolicy.defaults())).isEqualTo("fresh");

    Object raw = caffeineCache.getIfPresent("key1");
    assertThat(raw).isInstanceOf(CacheEntry.class);
    assertThat(((CacheEntry) raw).getValue()).isEqualTo("fresh");
    assertThat(((CacheEntry) raw).getKeyState()).isEqualTo(KeyState.NORMAL);
  }

  /**
   * A genuinely absent reader ({@code null} supplier) resolves a miss to the
   * documented null answer without entering SingleFlight and without writing an
   * entry — the pre-load guard that keeps an NPE out of the read path.
   */
  @Test
  void get_nullReaderOnMiss_shouldLeaveNoEntryAndReturnNull() {
    assertThat((Object) hotKeyCache.get("key1", (Supplier<String>) null, GetPolicy.defaults())).isNull();
    assertThat(caffeineCache.getIfPresent("key1")).isNull();
    verify(singleFlight, never()).load(anyString(), any(), anyBoolean(), any());
  }

  @Test
  void get_nullAnswer_noNullCaching_shouldLeaveNoEntry() {
    SingleFlightStubs.stubLoad(singleFlight, LoaderOutcome.sourceEmpty());

    assertThat(
      (Object) hotKeyCache.get("key1", () -> null, GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH))
    ).isNull();
    assertThat(caffeineCache.getIfPresent("key1")).isNull();
  }

  @Test
  void computeIfAbsent_revalidate_readerFailure_shouldKeepEntryAndPropagate() {
    caffeineCache.put(
      "key1",
      EntryDraft.blank(null)
        .value("stale")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(System.currentTimeMillis() + 300_000, System.currentTimeMillis() - 1000)
        .keyState(KeyState.NORMAL)
        .build()
    );
    SingleFlightStubs.stubLoad(singleFlight, LoaderOutcome.failed(new IllegalStateException("boom")));

    assertThatThrownBy(() -> hotKeyCache.get("key1", () -> "fresh", GetPolicy.of(0L, 0L, true, StalePolicy.REVALIDATE)))
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("boom");

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
      EntryDraft.blank(null)
        .value("stale")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(System.currentTimeMillis() + 300_000, System.currentTimeMillis() - 1000)
        .keyState(KeyState.NORMAL)
        .build()
    );
    when(singleFlight.isBreakerOpen(anyString())).thenReturn(true);
    SingleFlightStubs.stubLoad(singleFlight, LoaderOutcome.sourceEmpty());

    assertThat(
      (Object) hotKeyCache.get("key1", () -> "fresh", GetPolicy.of(0L, 0L, true, StalePolicy.REVALIDATE))
    ).isEqualTo("stale");
  }

  /**
   * A soft-expired REVALIDATE entry that crosses its hard TTL while the load is in flight
   * (source failing) is never served by the CB-open stale fallback — the hard TTL contract
   * is not bypassed. The kept entry is also not overwritten with a fabricated NullValue
   * sentinel (the source never answered null).
   *
   * <p>The refused entry is evicted on the spot — and its dedup slot forgotten with it
   * (design decisions: dedup lifecycle) — instead of lingering until the next read of that key.
   * The stale fallback is the one path that inspects the entry with no reader behind it, so
   * it is also the natural cleanup point; leaving it resident let a retained completed future
   * replay the expired value onto the next miss and re-cache it under fresh TTLs.
   */
  @Test
  void computeIfAbsent_revalidate_circuitBreakerOpen_hardExpiredDuringLoad_shouldReturnEmpty() throws Exception {
    caffeineCache.put(
      "key1",
      EntryDraft.blank(null)
        .value("old")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(System.currentTimeMillis() + 300, System.currentTimeMillis() - 1000)
        .keyState(KeyState.NORMAL)
        .build()
    );
    CountDownLatch releaseLoad = new CountDownLatch(1);
    when(singleFlight.isBreakerOpen(anyString())).thenReturn(true);
    when(singleFlight.load(anyString(), any(), anyBoolean(), any())).thenAnswer(inv -> {
      releaseLoad.await();
      return LoaderOutcome.sourceEmpty();
    });

    CompletableFuture<String> future = CompletableFuture.supplyAsync(() ->
      hotKeyCache.get("key1", () -> "fresh", GetPolicy.of(0L, 0L, true, StalePolicy.REVALIDATE))
    );

    Thread.sleep(500); // let the kept entry cross its hard expiry while the load is blocked
    releaseLoad.countDown();
    String result = future.get(5, TimeUnit.SECONDS);

    assertThat(result).isNull();
    assertThat(caffeineCache.getIfPresent("key1"))
      .as("the hard-expired entry is evicted — neither served nor sentinelized, nor left to linger")
      .isNull();
  }

  /**
   * The same hard-TTL property on the load-throws path (computeInLock catch block):
   * a kept entry that crosses its hard TTL during a failing load is not served as
   * stale either, and the refused entry is evicted on the spot (see the sibling test
   * for the rationale).
   */
  @Test
  void computeIfAbsent_revalidate_circuitBreakerOpen_hardExpiredDuringThrowingLoad_shouldReturnEmpty()
    throws Exception {
    caffeineCache.put(
      "key1",
      EntryDraft.blank(null)
        .value("old")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(System.currentTimeMillis() + 300, System.currentTimeMillis() - 1000)
        .keyState(KeyState.NORMAL)
        .build()
    );
    CountDownLatch releaseLoad = new CountDownLatch(1);
    when(singleFlight.isBreakerOpen(anyString())).thenReturn(true);
    when(singleFlight.load(anyString(), any(), anyBoolean(), any())).thenAnswer(inv -> {
      releaseLoad.await();
      return LoaderOutcome.failed(new IllegalStateException("boom"));
    });

    CompletableFuture<String> future = CompletableFuture.supplyAsync(() ->
      hotKeyCache.get("key1", () -> "fresh", GetPolicy.of(0L, 0L, true, StalePolicy.REVALIDATE))
    );

    Thread.sleep(500); // let the kept entry cross its hard expiry while the load is blocked
    releaseLoad.countDown();
    String result = future.get(5, TimeUnit.SECONDS);

    assertThat(result).isNull();
    assertThat(caffeineCache.getIfPresent("key1"))
      .as("the hard-expired entry is evicted — neither served nor sentinelized, nor left to linger")
      .isNull();
  }

  @Test
  void computeIfAbsent_hardExpired_readerFailure_shouldRemoveEntryAndPropagate() {
    caffeineCache.put(
      "key1",
      EntryDraft.blank(null)
        .value("old")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(System.currentTimeMillis() - 1000, System.currentTimeMillis() - 1000)
        .keyState(KeyState.NORMAL)
        .build()
    );
    SingleFlightStubs.stubLoad(singleFlight, LoaderOutcome.failed(new IllegalStateException("boom")));

    // Aligned with the get() path: hard-expired entries are removed on a
    // failed reload — they are never served as stale — and the failure propagates.
    assertThatThrownBy(() -> hotKeyCache.get("key1", () -> "fresh", GetPolicy.defaults()))
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("boom");
    assertThat(caffeineCache.getIfPresent("key1")).isNull();
  }

  @Test
  void computeIfAbsent_lazyTtl_shouldNotEvaluateTtlSuppliersOnPlainNormalHit() {
    AtomicInteger hardEvals = new AtomicInteger();
    AtomicInteger softEvals = new AtomicInteger();

    Supplier<String> policyReader = () -> "fresh";
    GetPolicy policy = new GetPolicy(
      () -> {
        hardEvals.incrementAndGet();
        return 100_000L;
      },
      () -> {
        softEvals.incrementAndGet();
        return 10_000L;
      },
      StalePolicy.SOFT_REFRESH,
      true,
      false
    );

    SingleFlightStubs.stubLoad(singleFlight, oc("fresh"));

    // Miss: TTL suppliers are evaluated exactly once (entry creation).
    assertThat((Object) hotKeyCache.get("key1", policyReader, policy)).isEqualTo("fresh");
    assertThat(hardEvals.get()).isEqualTo(1);
    assertThat(softEvals.get()).isEqualTo(1);

    // Plain NORMAL hit: suppliers must NOT be evaluated again (design decisions: spring integration).
    assertThat((Object) hotKeyCache.get("key1", policyReader, policy)).isEqualTo("fresh");
    assertThat(hardEvals.get()).isEqualTo(1);
    assertThat(softEvals.get()).isEqualTo(1);
  }

  @Test
  void computeIfAbsent_softRefresh_shouldServeStaleAndTriggerBackgroundRefresh() {
    caffeineCache.put(
      "key1",
      EntryDraft.blank(null)
        .value("stale")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() - 10_000)
        .keyState(KeyState.HOT)
        .build()
    );

    // Stale-while-revalidate: the caller receives the stale value, and the
    // background refresh runs on the (synchronous test) executor, replacing
    // the entry with the fresh value before the call returns.
    assertThat(
      (Object) hotKeyCache.get("key1", () -> "fresh", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH))
    ).isEqualTo("stale");

    Object raw = caffeineCache.getIfPresent("key1");
    assertThat(raw).isInstanceOf(CacheEntry.class);
    assertThat(((CacheEntry) raw).getValue()).isEqualTo("fresh");
  }

  @Test
  void computeIfAbsent_softRefresh_shouldRefreshCoolEntryAndDowngradeToNormal() {
    caffeineCache.put(
      "key1",
      EntryDraft.blank(null)
        .value("stale")
        .version(1)
        .decision(DecisionStamp.hot(5, null, 0L))
        .ttl(300_000, 30_000, 300_000, 5000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() - 10_000)
        .keyState(KeyState.COOL)
        .build()
    );

    // A soft-refresh reload of a COOL entry also downgrades it to NORMAL.
    assertThat(
      (Object) hotKeyCache.get("key1", () -> "fresh", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH))
    ).isEqualTo("stale");

    CacheEntry after = (CacheEntry) caffeineCache.getIfPresent("key1");
    assertThat(after.getValue()).isEqualTo("fresh");
    assertThat(after.getKeyState()).isEqualTo(KeyState.NORMAL);
  }

  @Test
  void computeIfAbsent_softRefresh_coolEntryWithNullReader_shouldNotRefreshAndReturnValue() {
    caffeineCache.put(
      "key1",
      EntryDraft.blank(null)
        .value("stale")
        .version(1)
        .decision(DecisionStamp.hot(5, null, 0L))
        .ttl(300_000, 30_000, 300_000, 5000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() - 10_000)
        .keyState(KeyState.COOL)
        .build()
    );

    // Convenience policy without a reader: the background refresh must be
    // skipped (no NPE on supplyAsync(null)), the stale value served, and the
    // entry left untouched.
    assertThat(
      (Object) hotKeyCache.get("key1", (Supplier<String>) null, GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH))
    ).isEqualTo("stale");

    CacheEntry after = (CacheEntry) caffeineCache.getIfPresent("key1");
    assertThat(after.getValue()).isEqualTo("stale");
    assertThat(after.getKeyState()).isEqualTo(KeyState.COOL);
  }

  @Test
  void getWithSoftExpire_softRefresh_coolEntryWithNullReader_shouldNotRefreshAndReturnValue() {
    caffeineCache.put(
      "key",
      EntryDraft.blank(null)
        .value("stale")
        .version(1)
        .decision(DecisionStamp.hot(5, null, 0L))
        .ttl(300_000, 30_000, 300_000, 5000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() - 10_000)
        .keyState(KeyState.COOL)
        .build()
    );

    assertThat(
      (Object) hotKeyCache.get("key", (Supplier<String>) null, GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH))
    ).isEqualTo("stale");

    CacheEntry after = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(after.getValue()).isEqualTo("stale");
    assertThat(after.getKeyState()).isEqualTo(KeyState.COOL);
  }

  @Test
  void getAll_perKeyFailure_shouldPropagateAfterProcessingTheBatch() {
    SingleFlightStubs.stubBatchLoad(
      singleFlight,
      Map.of("a", LoaderOutcome.failed(new IllegalStateException("boom")), "b", LoaderOutcome.loaded((Object) "v"))
    );

    assertThatThrownBy(() ->
      hotKeyCache.get(List.of("a", "b"), k -> "v", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH))
    )
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("boom");
  }

  @Test
  void getAll_perKeyFailure_shouldThrowWithoutReturningPartialResults() {
    SingleFlightStubs.stubBatchLoad(
      singleFlight,
      Map.of(
        "a",
        LoaderOutcome.loaded((Object) new VersionedValue("v", 1L)),
        "b",
        LoaderOutcome.failed(new IllegalStateException("boom"))
      )
    );

    assertThatThrownBy(() ->
      hotKeyCache.get(List.of("a", "b"), k -> "v", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH))
    )
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("boom");
  }

  /**
   * Verifies that getWithSoftExpire falls back to get() when soft expire is disabled.
   */
  @Test
  void getWithSoftExpire_whenDisabled_shouldFallbackToGet() {
    ZetaProperties props = new ZetaProperties();
    props.setSoftTtlMs(0);
    props.setHotSoftTtlMs(0);
    EntryLifecycleImpl noSoft = new EntryLifecycleImpl(caffeineCache, props, null);
    BackgroundRefresher noSoftRefresh = new DefaultBackgroundRefresher(caffeineCache, executor, noSoft, 10);

    SingleFlightStubs.stubLoad(singleFlight, oc("loaded"));

    HotKeyCache cache = new HotKeyCache(
      hotKeyDetector,
      caffeineCache,
      singleFlight,
      noSoft,
      noSoftRefresh,
      new AccessCounter(hotKeyDetector, null, scheduler, 50L, AccessCounter.DEFAULT_DETECTOR_INTERVAL_MS),
      new BroadcastBuffer(scheduler, null, 500, 2_000, null),
      new MonotonicStampClock(1L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS),
      mock(HealthView.class)
    );

    assertThat(
      (Object) cache.get("key", () -> "loaded", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH))
    ).isEqualTo("loaded");
  }

  /**
   * Verifies that getWithSoftExpire returns the cached value when the soft TTL has not expired.
   */
  @Test
  void getWithSoftExpire_notExpired_shouldReturnCachedValue() {
    caffeineCache.put(
      "key",
      EntryDraft.blank(null)
        .value("cached")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 60_000)
        .keyState(KeyState.HOT)
        .build()
    );

    assertThat(
      (Object) hotKeyCache.get("key", () -> "should-not-load", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH))
    ).isEqualTo("cached");
  }

  /**
   * Verifies that getWithSoftExpire with an expired soft TTL returns the stale value and
   * schedules a background refresh (stale-while-revalidate).
   */
  @Test
  void getWithSoftExpire_expired_shouldReturnStaleAndTriggerRefresh() {
    caffeineCache.put(
      "key",
      EntryDraft.blank(null)
        .value("stale")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() - 1)
        .keyState(KeyState.HOT)
        .build()
    );

    // Should return stale value immediately (stale-while-revalidate)
    assertThat(
      (Object) hotKeyCache.get("key", () -> "fresh", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH))
    ).isEqualTo("stale");
  }

  /**
   * Verifies that peek returns a cached entry value directly.
   */
  @Test
  void peek_shouldReturnCachedEntryValue() {
    caffeineCache.put("raw", plainEntry("rawValue"));
    assertThat((Object) hotKeyCache.peek("raw")).isEqualTo("rawValue");
  }

  /**
   * Verifies that isHot returns false for a logically expired CacheEntry
   * even when the entry is still present in the Caffeine cache.
   */
  @Test
  void isHot_withExpiredEntry_shouldReturnFalse() {
    caffeineCache.put(
      "expired",
      EntryDraft.blank(null)
        .value("v")
        .version(0)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(1, 0, 300_000, 30_000)
        .expiryAt(1, 0)
        .keyState(KeyState.HOT)
        .build()
    );

    assertThat(hotKeyCache.isHot("expired")).isFalse();
  }

  /**
   * Verifies that putThrough caches the value and preserves it for subsequent reads.
   */
  @Test
  void putThrough_shouldWriteThroughAndCache() {
    hotKeyCache.putThrough("key1", "newValue", 0L, 0L);

    assertThat((Object) hotKeyCache.peek("key1")).isEqualTo("newValue");
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
      entryLifecycle,
      backgroundRefresher,
      new AccessCounter(hotKeyDetector, null, scheduler, 50L, AccessCounter.DEFAULT_DETECTOR_INTERVAL_MS),
      new BroadcastBuffer(rejecting, publisher, 500, 2_000, null),
      new MonotonicStampClock(1L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS),
      mock(HealthView.class)
    );

    assertThatCode(() -> cache.putThrough("key1", "newValue", 0L, 0L)).doesNotThrowAnyException();
    assertThat((Object) cache.peek("key1")).isEqualTo("newValue");
    verify(publisher).broadcastRefresh(eq("key1"), anyLong());
  }

  /** Verifies that a local immediate clear — {@code clearAll(Scope.LOCAL, immediate)} — clears every entry. */
  @Test
  void clear_localImmediate_shouldClearEveryEntry() {
    caffeineCache.put("k1", plainEntry("v1"));
    caffeineCache.put("k2", plainEntry("v2"));
    assertThat(caffeineCache.estimatedSize()).isPositive();
    hotKeyCache.clear(new InvalidatePolicy(InvalidatePolicy.Scope.LOCAL, true));
    assertThat(caffeineCache.estimatedSize()).isZero();
  }

  // ── get with logically expired entry ──

  @Test
  void get_withLogicallyExpiredEntry_shouldReload() {
    caffeineCache.put(
      "expired",
      EntryDraft.blank(null)
        .value("stale")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(1, 0, 300_000, 30_000)
        .expiryAt(1, 0)
        .keyState(KeyState.NORMAL)
        .build()
    );
    SingleFlightStubs.stubLoad(singleFlight, oc("fresh"));

    assertThat(
      (Object) hotKeyCache.get("expired", () -> "fresh", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH))
    ).isEqualTo("fresh");
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
      EntryDraft.blank(null)
        .value("stale")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(System.currentTimeMillis() + 300_000, System.currentTimeMillis() - 10_000)
        .keyState(KeyState.NORMAL)
        .build()
    );
    SingleFlightStubs.stubLoad(singleFlight, oc("fresh"));

    assertThat(
      (Object) hotKeyCache.get("key1", () -> "fresh", GetPolicy.of(0L, 0L, true, StalePolicy.REVALIDATE))
    ).isEqualTo("fresh");
    verify(singleFlight, times(1)).load(eq("key1"), any(), anyBoolean(), any());
    assertThat(((CacheEntry) caffeineCache.getIfPresent("key1")).getValue()).isEqualTo("fresh");
  }

  /** Control: {@link StalePolicy#RETURN} on the same entry serves the stale value without a reload. */
  @Test
  void get_return_softExpiredEntry_shouldServeStaleWithoutReload() {
    caffeineCache.put(
      "key1",
      EntryDraft.blank(null)
        .value("stale")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(System.currentTimeMillis() + 300_000, System.currentTimeMillis() - 10_000)
        .keyState(KeyState.NORMAL)
        .build()
    );

    assertThat(
      (Object) hotKeyCache.get("key1", () -> "fresh", GetPolicy.of(0L, 0L, true, StalePolicy.RETURN))
    ).isEqualTo("stale");
    verify(singleFlight, never()).load(anyString(), any(), anyBoolean(), any());
    assertThat(((CacheEntry) caffeineCache.getIfPresent("key1")).getValue()).isEqualTo("stale");
  }

  // ── getWithSoftExpire: expired entry triggers reload ──

  @Test
  void getWithSoftExpire_withExpiredEntry_shouldReloadViaLoadAndCache() {
    caffeineCache.put(
      "expired",
      EntryDraft.blank(null)
        .value("stale")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(1, 0, 300_000, 30_000)
        .expiryAt(1, 0)
        .keyState(KeyState.NORMAL)
        .build()
    );
    SingleFlightStubs.stubLoad(singleFlight, oc("fresh"));

    assertThat(
      (Object) hotKeyCache.get("expired", () -> "fresh", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH))
    ).isEqualTo("fresh");
  }

  // ── getWithSoftExpire: cache miss (no entry) triggers loadAndCache ──

  @Test
  void getWithSoftExpire_withCacheMiss_shouldLoad() {
    SingleFlightStubs.stubLoad(singleFlight, oc("loaded"));

    assertThat(
      (Object) hotKeyCache.get("missing", () -> "loaded", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH))
    ).isEqualTo("loaded");
  }

  // ── getWithSoftExpire with NORMAL entry (no soft TTL) ──

  @Test
  void getWithSoftExpire_withNormalEntryNoSoftTtl_returnsCached() {
    caffeineCache.put("raw", plainEntry("bare-string"));
    assertThat(
      (Object) hotKeyCache.get("raw", () -> "fresh", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH))
    ).isEqualTo("bare-string");
  }

  // ── getWithSoftExpire with NORMAL entry and soft expired ──

  @Test
  void getWithSoftExpire_withNormalEntrySoftExpired_shouldReturnStale() {
    caffeineCache.put(
      "normal",
      EntryDraft.blank(null)
        .value("stale")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() - 1000)
        .keyState(KeyState.NORMAL)
        .build()
    );

    String result = hotKeyCache.get("normal", () -> "fresh", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH));

    assertThat(result).isEqualTo("stale");
  }

  // ── getWithSoftExpire REVALIDATE: worker-managed keep-and-renew ──

  /** Build a Worker-stamped COOL entry whose soft TTL has expired. */
  private static CacheEntry workerCoolSoftExpiredEntry() {
    return EntryDraft.blank(null)
      .value("stored")
      .version(1)
      .decision(DecisionStamp.hot(5, "w1", 7))
      .ttl(300_000, 30_000, 300_000, 30_000)
      .expiryAt(System.currentTimeMillis() + 300_000, System.currentTimeMillis() - 10_000)
      .keyState(KeyState.COOL)
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
    SingleFlightStubs.stubLoad(singleFlight, oc("fresh"));

    // First read: the kept entry's reload re-invokes the loader once; the
    // fresh value serves this caller while the Worker-managed entry survives.
    assertThat(
      (Object) hotKeyCache.get("key1", () -> "fresh", GetPolicy.of(0L, 0L, true, StalePolicy.REVALIDATE))
    ).isEqualTo("fresh");

    CacheEntry after = (CacheEntry) caffeineCache.getIfPresent("key1");
    // The reload's value replaces the stale one; the Worker's TTL regime and
    // decision stamp survive (P0-1).
    assertThat((Object) after.getValue()).isEqualTo("fresh");
    assertThat(after.getKeyState()).isEqualTo(KeyState.COOL);
    assertThat(after.getDecisionNodeId()).isEqualTo("w1");
    assertThat(after.getDecisionVersion()).isEqualTo(5L);
    // Soft expiry renewed within the hard TTL: no longer soft-expired.
    assertThat(entryLifecycle.ttlPolicy().isSoftExpired(after)).isFalse();
    assertThat(after.getSoftExpireAtMs()).isLessThanOrEqualTo(after.getHardExpireAtMs());

    // Subsequent reads serve the renewed entry — the loader is NOT re-invoked.
    assertThat(
      (Object) hotKeyCache.get("key1", () -> "fresh2", GetPolicy.of(0L, 0L, true, StalePolicy.REVALIDATE))
    ).isEqualTo("fresh");
    verify(singleFlight, times(1)).load(anyString(), any(), anyBoolean(), any());
  }

  /**
   * A null load must not erase a Worker decision: under the v2 null contract
   * nothing is written over the kept entry, so the entry (and its decision
   * stamp) survives untouched for later decisionVersion ordering.
   */
  @Test
  void get_revalidate_workerManagedEntry_nullLoad_shouldPreserveKeptEntry() {
    caffeineCache.put("key1", workerCoolSoftExpiredEntry());
    SingleFlightStubs.stubLoad(singleFlight, LoaderOutcome.sourceEmpty());

    assertThat(
      (Object) hotKeyCache.get("key1", () -> null, GetPolicy.of(0L, 0L, true, StalePolicy.REVALIDATE))
    ).isNull();

    CacheEntry kept = (CacheEntry) caffeineCache.getIfPresent("key1");
    assertThat((Object) kept.getValue()).isEqualTo("stored");
    assertThat(kept.getDecisionNodeId()).isEqualTo("w1");
    assertThat(kept.getDecisionVersion()).isEqualTo(5L);
    assertThat(kept.getDecisionEpoch()).isEqualTo(7L);
  }

  // ── putLocal ──

  @Test
  void putLocal_shouldCacheValue() {
    hotKeyCache.putLocal("k", "v", 0L, 0L);
    assertThat((Object) hotKeyCache.peek("k")).isEqualTo("v");
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
  void putLocal_shouldPreserveExistingMetadata() {
    caffeineCache.put(
      "k",
      EntryDraft.blank(null)
        .value("old")
        .version(42)
        .decision(DecisionStamp.hot(7, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 60_000)
        .keyState(KeyState.HOT)
        .build()
    );

    hotKeyCache.putLocal("k", "new", 0L, 0L);

    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(entry.getValue()).isEqualTo("new");
    // A local write carries a fresh stamp (design decisions: version stamps I4); the rest of the
    // metadata is preserved.
    assertThat(entry.getDataVersion()).isGreaterThan(42L);
    assertThat(entry.getDecisionVersion()).isEqualTo(7);
    assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
  }

  // ── invalidateLocal ──

  @Test
  void invalidateLocal_shouldRemoveEntries() {
    caffeineCache.put("k1", plainEntry("v1"));
    caffeineCache.put("k2", plainEntry("v2"));
    hotKeyCache.invalidate(List.of("k1", "k2"), new InvalidatePolicy(InvalidatePolicy.Scope.BOTH, false));
    assertThat(caffeineCache.getIfPresent("k1")).isNull();
    assertThat(caffeineCache.getIfPresent("k2")).isNull();
  }

  @Test
  void invalidateLocal_withEmptyCollection_shouldSkip() {
    caffeineCache.put("k1", plainEntry("v1"));
    hotKeyCache.invalidate(List.of(), new InvalidatePolicy(InvalidatePolicy.Scope.BOTH, false));
    assertThat(caffeineCache.getIfPresent("k1")).isNotNull();
  }

  // ── stats ──

  @Test
  void stats_shouldReturnStats() {
    caffeineCache.put("k1", plainEntry("v1"));
    ZetaCacheStats stats = hotKeyCache.stats();
    assertThat(stats).isNotNull();
    assertThat(stats.estimatedSize()).isPositive();
  }

  // ── get with ALLOW_NO_REPORT ──

  // ── getWithSoftExpire with TTL override ──

  @Test
  void getWithSoftExpire_withSoftTtlOverride_shouldReturnCached() {
    caffeineCache.put(
      "key",
      EntryDraft.blank(null)
        .value("cached")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 60_000)
        .keyState(KeyState.HOT)
        .build()
    );
    assertThat(
      (Object) hotKeyCache.get("key", () -> "fresh", GetPolicy.of(0L, 500L, true, StalePolicy.SOFT_REFRESH))
    ).isEqualTo("cached");
  }

  // ── putThrough with TTL overrides ──

  @Test
  void putThrough_withTtlOverrides_shouldCacheWithCustomTtl() {
    hotKeyCache.putThrough("key1", "v", 50000L, 5000L);
    CacheEntry raw = caffeineCache.getIfPresent("key1");
    assertThat(raw).isNotNull();
    assertThat(raw.getHardTtlMs()).isEqualTo(50000L);
    assertThat(raw.getSoftTtlMs()).isEqualTo(5000L);
  }

  // ── getWithSoftExpire with hard/soft TTL overrides ──

  @Test
  void getWithSoftExpire_withBothTtlOverrides_shouldReturnCached() {
    caffeineCache.put(
      "key",
      EntryDraft.blank(null)
        .value("cached")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 60_000)
        .keyState(KeyState.HOT)
        .build()
    );
    assertThat(
      (Object) hotKeyCache.get("key", () -> "fresh", GetPolicy.of(10000L, 500L, true, StalePolicy.SOFT_REFRESH))
    ).isEqualTo("cached");
  }

  // ── broadcastAllLocalRulesManually (no publisher) ──

  // ── Hot path detection and promotion ──

  @Nested
  @DisplayName("Hot path detection and promotion")
  class HotPathTest {

    private AccessCounter accessCounter;

    private HotKeyDetector hotKeyDetector;
    private Cache<String, CacheEntry> caffeineCache;
    private SingleFlight singleFlight;
    private EntryLifecycle entryLifecycle;
    private BackgroundRefresher backgroundRefresher;
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
      entryLifecycle = new EntryLifecycleImpl(caffeineCache, ttlConfig, null);
      backgroundRefresher = new DefaultBackgroundRefresher(caffeineCache, executor, entryLifecycle, 10);
      publisher = mock(CacheSyncPublisher.class);
      broadcastBuffer = new BroadcastBuffer(
        Executors.newSingleThreadScheduledExecutor(r -> {
          Thread t = new Thread(r, "zeta-send-flusher");
          t.setDaemon(true);
          return t;
        }),
        publisher,
        500,
        2_000,
        null
      );
      healthView = mock(HealthView.class);
      reporter = mock(KeyReporter.class);
      hotKeyCache = new HotKeyCache(
        hotKeyDetector,
        caffeineCache,
        singleFlight,
        entryLifecycle,
        backgroundRefresher,
        new AccessCounter(hotKeyDetector, reporter, scheduler, 50L, AccessCounter.DEFAULT_DETECTOR_INTERVAL_MS),
        broadcastBuffer,
        new MonotonicStampClock(1L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS),
        healthView
      );
    }

    /**
     * The admission sink the engine registered on the detector — production's
     * TopK admission event. NORMAL promotion left the read path (design
     * decisions: hot state), so a test that wants a HOT entry fires one event
     * here and then runs the maintenance tick, exactly like
     * {@code ZetaSchedulingConfiguration} does every 500 ms.
     *
     * @return the sink the engine handed to the sketch
     */
    private Consumer<String> admissionSink() {
      ArgumentCaptor<Consumer<String>> sink = ArgumentCaptor.forClass(Consumer.class);
      verify(hotKeyDetector, atLeastOnce()).setAdmittedSink(sink.capture());
      return sink.getValue();
    }

    @Test
    @DisplayName("loadAndCache should promote key to HOT when detected as hot")
    void loadAndCache_shouldPromoteHotKey() {
      when(hotKeyDetector.contains("key1")).thenReturn(true);
      SingleFlightStubs.stubLoad(singleFlight, oc("value"));

      String result = hotKeyCache.get("key1", () -> "value", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH));

      assertThat(result).isEqualTo("value");
      Object raw = caffeineCache.getIfPresent("key1");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      CacheEntry entry = (CacheEntry) raw;
      assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
    }

    @Test
    @DisplayName("a steady-state HOT hit should skip the TopK sketch probe")
    void hotHit_shouldSkipSketchProbeForSteadyStateHot() {
      caffeineCache.put(
        "key1",
        EntryDraft.blank(null)
          .value("v")
          .version(1)
          .decision(DecisionStamp.hot(0, null, 0L))
          .ttl(300_000, 30_000, 300_000, 30_000)
          .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 60_000)
          .keyState(KeyState.HOT)
          .build()
      );

      assertThat(
        (Object) hotKeyCache.get("key1", () -> "loaded", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH))
      ).isEqualTo("v");

      // Guard order: the entry-state check runs before the sketch probe, so a
      // HOT entry (never promotable) pays no probe on its hottest path.
      verify(hotKeyDetector, never()).contains(anyString());
      CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key1");
      assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
    }

    @Test
    @DisplayName("a NORMAL hit pays no sketch probe: promotion is not a read-path concern")
    void hotHit_shouldStillProbeNormalEntries() {
      caffeineCache.put(
        "key1",
        EntryDraft.blank(null)
          .value("v")
          .version(1)
          .decision(DecisionStamp.hot(0, null, 0L))
          .ttl(300_000, 30_000, 300_000, 30_000)
          .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 60_000)
          .keyState(KeyState.NORMAL)
          .build()
      );
      when(hotKeyDetector.contains("key1")).thenReturn(true);

      hotKeyCache.get("key1", () -> "loaded", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH));

      // The read path only re-verifies Worker demotion and the degraded-COOL
      // branch: a NORMAL hit must not pay a sketch probe there (design
      // decisions: hot state — promotion is event-driven, off the read path).
      verify(hotKeyDetector, never()).contains("key1");
      CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key1");
      assertThat(entry.getKeyState()).isEqualTo(KeyState.NORMAL);
    }

    @Test
    @DisplayName("loadAndCache should preserve Worker-managed HOT entry")
    void loadAndCache_shouldPreserveWorkerManagedEntry() {
      when(hotKeyDetector.contains("key1")).thenReturn(false);
      when(singleFlight.load(eq("key1"), any(), anyBoolean(), any())).thenAnswer(invocation -> {
        Supplier<?> reader = invocation.getArgument(1);
        caffeineCache.put(
          "key1",
          EntryDraft.blank(null)
            .value("workerValue")
            .version(100)
            .decision(DecisionStamp.hot(42, null, 0L))
            .ttl(3_600_000, 300_000, 300_000, 30_000)
            .expiryAt(Long.MAX_VALUE, Long.MAX_VALUE)
            .keyState(KeyState.HOT)
            .build()
        );
        Object v = reader.get();
        if (v != null) {
          java.util.function.Consumer<Object> commit = invocation.getArgument(3);
          commit.accept(v);
        }
        return v == null ? LoaderOutcome.sourceEmpty() : LoaderOutcome.loaded(v);
      });

      String result = hotKeyCache.get("key1", () -> "newValue", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH));

      assertThat(result).isEqualTo("newValue");
      Object raw = caffeineCache.getIfPresent("key1");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      CacheEntry entry = (CacheEntry) raw;
      assertThat(entry.getKeyState()).as("the Worker's HOT regime survives the load").isEqualTo(KeyState.HOT);
      assertThat(entry.getHardTtlMs()).as("the Worker-granted TTL survives the load").isEqualTo(3_600_000);
      // P0-1: a load that wins the resident guard replaces the value — keeping the
      // HOT TTL but dropping the freshly loaded value pinned stale data for the
      // whole Worker-granted lease.
      assertThat(entry.getValue()).isEqualTo("newValue");
    }

    @Test
    @DisplayName("getWithSoftExpire refreshes COOL entry and downgrades it to NORMAL")
    void getWithSoftExpire_shouldRefreshCoolEntryAndDowngradeToNormal() {
      caffeineCache.put(
        "key",
        EntryDraft.blank(null)
          .value("stale")
          .version(1)
          .decision(DecisionStamp.hot(5, null, 0L))
          .ttl(300_000, 30_000, 300_000, 5000)
          .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() - 1000)
          .keyState(KeyState.COOL)
          .build()
      );

      hotKeyCache.get("key", () -> "fresh", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH));

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
        EntryDraft.blank(null)
          .value("stale")
          .version(1)
          .decision(DecisionStamp.hot(5, null, 0L))
          .ttl(3_600_000, 300_000, 300_000, 30_000)
          .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() - 1000)
          .keyState(KeyState.HOT)
          .build()
      );

      hotKeyCache.get("key", () -> "fresh", GetPolicy.of(0L, 9999L, true, StalePolicy.SOFT_REFRESH));

      CacheEntry after = (CacheEntry) caffeineCache.getIfPresent("key");
      assertThat(after.getValue()).isEqualTo("fresh");
      assertThat(after.getSoftTtlMs()).isEqualTo(9999L);
    }

    @Test
    @DisplayName("an admission event promotes the resident NORMAL entry on the maintenance tick")
    void promoteLocalHotkeyIfNeeded_shouldPromoteNormalToHot() {
      caffeineCache.put(
        "key1",
        EntryDraft.blank(null)
          .value("v")
          .version(1)
          .decision(DecisionStamp.hot(0, null, 0L))
          .ttl(300_000, 30_000, 300_000, 30_000)
          .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 60_000)
          .keyState(KeyState.NORMAL)
          .build()
      );
      when(hotKeyDetector.contains("key1")).thenReturn(true);

      // NORMAL promotion left the read path: the admission event queues the key
      // and the maintenance tick performs the rewrite (design decisions: hot state).
      admissionSink().accept("key1");
      assertThat(hotKeyCache.promoteAdmitted()).as("one entry transitioned to HOT").isEqualTo(1);

      Object raw = caffeineCache.getIfPresent("key1");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      CacheEntry entry = (CacheEntry) raw;
      assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
      assertThat(entry.getHardTtlMs()).isEqualTo(entryLifecycle.ttlPolicy().effectiveHotHardTtlMs());
    }

    @Test
    @DisplayName("an admission tick removes a logically expired resident instead of promoting it")
    void promoteAdmitted_shouldRemoveExpiredResident() {
      caffeineCache.put(
        "key1",
        EntryDraft.blank(null)
          .value("v")
          .version(1)
          .decision(DecisionStamp.hot(0, null, 0L))
          .ttl(300_000, 30_000, 300_000, 30_000)
          .expiryAt(System.currentTimeMillis() - 60_000, 0L)
          .keyState(KeyState.NORMAL)
          .build()
      );
      when(hotKeyDetector.contains("key1")).thenReturn(true);

      admissionSink().accept("key1");

      assertThat(hotKeyCache.promoteAdmitted()).as("an expired entry is not a promotion").isZero();
      assertThat(caffeineCache.getIfPresent("key1")).as("the expired resident is removed").isNull();
    }

    @Test
    @DisplayName("the lock-free pre-check still lets an expired HOT resident reach the removal")
    void promoteAdmitted_shouldRemoveExpiredHotResident() {
      caffeineCache.put(
        "key1",
        EntryDraft.blank(null)
          .value("v")
          .version(1)
          .decision(DecisionStamp.hot(5, "w1", 0L))
          .ttl(3_600_000, 300_000, 300_000, 30_000)
          .expiryAt(System.currentTimeMillis() - 60_000, 0L)
          .keyState(KeyState.HOT)
          .build()
      );
      when(hotKeyDetector.contains("key1")).thenReturn(true);

      admissionSink().accept("key1");

      assertThat(hotKeyCache.promoteAdmitted()).as("an expired entry is not a promotion").isZero();
      assertThat(caffeineCache.getIfPresent("key1"))
        .as("a non-promotable state must not bypass the expired-resident removal")
        .isNull();
    }

    @Test
    @DisplayName("an admission tick keeps a resident's longer normal TTL baseline")
    void promoteAdmitted_shouldKeepLongerNormalTtlBaseline() {
      caffeineCache.put(
        "key1",
        EntryDraft.blank(null)
          .value("v")
          .version(1)
          .decision(DecisionStamp.hot(0, null, 0L))
          .ttl(7_200_000, 600_000, 7_200_000, 600_000)
          .keyState(KeyState.NORMAL)
          .build()
      );
      when(hotKeyDetector.contains("key1")).thenReturn(true);

      admissionSink().accept("key1");
      assertThat(hotKeyCache.promoteAdmitted()).as("one entry transitioned to HOT").isEqualTo(1);

      CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key1");
      assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
      assertThat(entry.getHardTtlMs()).as("the explicit 2h baseline outlives the 1h hot default").isEqualTo(7_200_000L);
      assertThat(entry.getSoftTtlMs()).isEqualTo(600_000L);
    }

    @Test
    @DisplayName("promoteLocalHotkeyIfNeeded should promote COOL to HOT when cluster unhealthy")
    void promoteLocalHotkeyIfNeeded_shouldPromoteCoolWhenClusterUnhealthy() {
      caffeineCache.put(
        "key1",
        EntryDraft.blank(null)
          .value("v")
          .version(1)
          .decision(DecisionStamp.hot(5, null, 0L))
          .ttl(300_000, 30_000, 300_000, 30_000)
          .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 60_000)
          .keyState(KeyState.COOL)
          .build()
      );
      when(hotKeyDetector.contains("key1")).thenReturn(true);
      when(healthView.isClusterHealthy()).thenReturn(false);

      hotKeyCache.get("key1", () -> "loaded", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH));

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
        EntryDraft.blank(null)
          .value("v")
          .version(1)
          .decision(DecisionStamp.hot(5, null, 0L))
          .ttl(300_000, 30_000, 300_000, 30_000)
          .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 60_000)
          .keyState(KeyState.COOL)
          .build()
      );
      when(hotKeyDetector.contains("key1")).thenReturn(true);
      when(healthView.isClusterHealthy()).thenReturn(true);

      hotKeyCache.get("key1", () -> "loaded", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH));

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
        EntryDraft.blank(null)
          .value("v")
          .version(1)
          .decision(DecisionStamp.hot(0, null, 0L))
          .ttl(300_000, 30_000, 300_000, 30_000)
          .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 60_000)
          .keyState(KeyState.NORMAL)
          .build()
      );
      when(hotKeyDetector.contains("key1")).thenReturn(false);

      assertThat(
        (Object) hotKeyCache.get("key1", () -> "loaded", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH))
      ).isEqualTo("v");

      CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key1");
      assertThat(entry.getKeyState()).isEqualTo(KeyState.NORMAL);
      assertThat(entry.getHardTtlMs()).isEqualTo(300_000L);
    }

    @Test
    @DisplayName("an admission event for a key no longer in the TopK does not promote")
    void promoteAdmitted_shouldSkipNonMember() {
      caffeineCache.put(
        "key1",
        EntryDraft.blank(null)
          .value("v")
          .version(1)
          .decision(DecisionStamp.hot(0, null, 0L))
          .ttl(300_000, 30_000, 300_000, 30_000)
          .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 60_000)
          .keyState(KeyState.NORMAL)
          .build()
      );
      // The drain samples TopK membership before the atomic rewrite; a key
      // expelled between admission and this tick is not promoted.
      when(hotKeyDetector.contains("key1")).thenReturn(false);

      admissionSink().accept("key1");
      assertThat(hotKeyCache.promoteAdmitted()).isZero();

      CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key1");
      assertThat(entry.getKeyState()).isEqualTo(KeyState.NORMAL);
    }

    @Test
    @DisplayName("promote must preserve a Worker entry that replaced the resident one before the tick")
    void promote_shouldPreserveEntryReplacedDuringProcessing() {
      caffeineCache.put(
        "key1",
        EntryDraft.blank(null)
          .value("v")
          .version(1)
          .decision(DecisionStamp.hot(0, null, 0L))
          .ttl(300_000, 30_000, 300_000, 30_000)
          .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 60_000)
          .keyState(KeyState.NORMAL)
          .build()
      );
      // A Worker broadcast lands between the admission event and the maintenance
      // tick: the drain's membership probe installs it, and the lock-free
      // promotable-state pre-check must then leave it alone — its TTL regime is
      // the Worker's.
      when(hotKeyDetector.contains("key1")).thenAnswer(invocation -> {
        caffeineCache.put("key1", workerHotEntry("w1", 3));
        return true;
      });

      admissionSink().accept("key1");
      assertThat(hotKeyCache.promoteAdmitted()).as("a Worker-managed entry is never locally promoted").isZero();

      CacheEntry kept = (CacheEntry) caffeineCache.getIfPresent("key1");
      assertThat(kept.getValue()).as("the replacement survives the promotion attempt").isEqualTo("stored");
      assertThat(kept.getKeyState()).isEqualTo(KeyState.HOT);
      assertThat(kept.getDecisionNodeId()).isEqualTo("w1");
    }

    @Test
    @DisplayName("putThrough preserves Worker-managed HOT state")
    void buildPutThroughEntry_shouldPreserveWorkerManagedState() {
      caffeineCache.put(
        "key1",
        EntryDraft.blank(null)
          .value("old")
          .version(1)
          .decision(DecisionStamp.hot(5, null, 0L))
          .ttl(3_600_000, 300_000, 300_000, 30_000)
          .expiryAt(Long.MAX_VALUE, Long.MAX_VALUE)
          .keyState(KeyState.HOT)
          .build()
      );

      hotKeyCache.putThrough("key1", "newValue", 0L, 0L);

      CacheEntry entry = caffeineCache.getIfPresent("key1");
      assertThat(entry).isNotNull();
      assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
      // A local write at a strictly newer stamp is authoritative; the Worker
      // decision metadata and TTL regime survive.
      assertThat(entry.getValue()).isEqualTo("newValue");
    }

    @Test
    @DisplayName("invalidate should send when publisher present")
    void invalidate_shouldBroadcastWhenPublisherPresent() {
      hotKeyCache.invalidate("key1", new InvalidatePolicy(InvalidatePolicy.Scope.BOTH, false));

      verify(publisher).broadcastInvalidate(eq("key1"), anyLong());
    }

    @Test
    @DisplayName("invalidateAllLocal should send one batched INVALIDATE carrying per-key versions")
    void invalidateAll_shouldBroadcastWhenPublisherPresent() {
      hotKeyCache.invalidate(List.of("key1", "key2"), new InvalidatePolicy(InvalidatePolicy.Scope.BOTH, false));

      // design decisions: invalidation: the batch path sends one INVALIDATE_BATCH chunk carrying the
      // per-key versioned entries (design decisions: version comparison's per-key ordering preserved).
      org.mockito.ArgumentCaptor<List<SyncMessage.Entry>> batch = org.mockito.ArgumentCaptor.forClass(
        (Class) List.class
      );
      verify(publisher).broadcastInvalidateBatch(batch.capture());
      assertThat(batch.getValue()).extracting(SyncMessage.Entry::key).containsExactlyInAnyOrder("key1", "key2");
      assertThat(batch.getValue()).allMatch(e -> e.version() > 0);
    }

    @Test
    @DisplayName("putThrough should send refresh when publisher present")
    void putThrough_shouldBroadcastWhenPublisherPresent() {
      hotKeyCache.putThrough("key1", "value", 0L, 0L);

      broadcastBuffer.flush();
      verify(publisher).broadcastRefresh(eq("key1"), anyLong());
    }

    /**
     * A synced refresh is a version-stamped write on the sync plane: its loaded
     * value travels in a buffered value-carrying PUT (design decisions: sync messages), the same
     * message path as {@code putThrough} — not the pre-0098 value-less REFRESH.
     * The PUT carries the refresher's value so peers apply it without reloading.
     */
    @Test
    @DisplayName("refresh(sync=true) buffers a value-carrying PUT with the loaded value")
    void refresh_syncTrue_buffersValueCarryingPut() {
      byte[] payload = "refreshed-payload".getBytes(java.nio.charset.StandardCharsets.UTF_8);
      when(publisher.serializePutPayload("refreshed")).thenReturn(payload);

      hotKeyCache.refresh("key1", () -> "refreshed", 0L, 0L, true);

      assertThat((Object) hotKeyCache.peek("key1")).isEqualTo("refreshed");
      verify(publisher).serializePutPayload("refreshed");
      broadcastBuffer.flush();
      verify(publisher).broadcastPut(eq("key1"), anyLong(), same(payload));
      verify(publisher, never()).broadcastRefresh(anyString(), anyLong());
    }

    /**
     * With {@code sync = false} a refresh is a pure local apply: it still bumps
     * its own version and loads, but never reaches the dispatcher/publisher.
     */
    @Test
    @DisplayName("refresh(sync=false) applies locally without any broadcast")
    void refresh_syncFalse_appliesLocallyWithoutBroadcast() {
      hotKeyCache.refresh("key2", () -> "local-only", 0L, 0L, false);

      assertThat((Object) hotKeyCache.peek("key2")).isEqualTo("local-only");
      verify(publisher, never()).serializePutPayload(any());
      verify(publisher, never()).broadcastPut(anyString(), anyLong(), any());
      verify(publisher, never()).broadcastRefresh(anyString(), anyLong());
    }

    @Test
    @DisplayName("get with TTL overrides should use them in loadAndCache")
    void get_shouldUseTtlOverrides() {
      SingleFlightStubs.stubLoad(singleFlight, oc("value"));
      when(hotKeyDetector.contains("key1")).thenReturn(false);

      hotKeyCache.get("key1", () -> "value", GetPolicy.of(50000L, 5000L, true, StalePolicy.SOFT_REFRESH));

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
        EntryDraft.blank(null)
          .value("stale")
          .version(1)
          .decision(DecisionStamp.hot(5, null, 0L))
          .ttl(300_000, 30_000, 300_000, 30_000)
          .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() - 1000)
          .keyState(KeyState.COOL)
          .build()
      );

      hotKeyCache.get("key", () -> "fresh", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH));

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
        EntryDraft.blank(null)
          .value("stale")
          .version(1)
          .decision(DecisionStamp.hot(5, null, 0L))
          .ttl(300_000, 0, 300_000, 0)
          .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() - 1000)
          .keyState(KeyState.COOL)
          .build()
      );
      SingleFlightStubs.stubLoad(singleFlight, oc("fresh"));

      hotKeyCache.get("key", () -> "fresh", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH));

      CacheEntry after = (CacheEntry) caffeineCache.getIfPresent("key");
      assertThat(after.getValue()).isEqualTo("fresh");
      assertThat(after.getKeyState()).isEqualTo(KeyState.NORMAL);
    }

    @Test
    @DisplayName("get with TTL overrides and hot detection uses TTL overrides in hot path")
    void get_withTtlOverridesAndHotDetection_usesTtlOverrides() {
      SingleFlightStubs.stubLoad(singleFlight, oc("hot-value"));
      when(hotKeyDetector.contains("key1")).thenReturn(true);

      hotKeyCache.get("key1", () -> "hot-value", GetPolicy.of(80000L, 8000L, true, StalePolicy.SOFT_REFRESH));

      Object raw = caffeineCache.getIfPresent("key1");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      CacheEntry entry = (CacheEntry) raw;
      assertThat(entry.getHardTtlMs()).isEqualTo(3_600_000L);
      assertThat(entry.getSoftTtlMs()).isEqualTo(300_000L);
      assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
    }

    @Test
    @DisplayName("processLocalHotkeyIfNeeded should NOT extend HOT entry past 75% TTL — hard TTL is the bound")
    void processLocalHotkeyIfNeeded_shouldNotExtendHotExpiry() {
      long originalExpireAt = System.currentTimeMillis() + 5_000;
      caffeineCache.put(
        "key1",
        EntryDraft.blank(null)
          .value("v")
          .version(1)
          .decision(DecisionStamp.hot(5, null, 0L))
          .ttl(60_000, 30_000, 300_000, 30_000)
          .expiryAt(originalExpireAt, System.currentTimeMillis() + 60_000)
          .keyState(KeyState.HOT)
          .build()
      );
      when(hotKeyDetector.contains("key1")).thenReturn(true);

      hotKeyCache.get("key1", () -> "loaded", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH));

      CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key1");
      assertThat(entry.getHardExpireAtMs()).isEqualTo(originalExpireAt);
    }

    @Test
    @DisplayName("processLocalHotkeyIfNeeded should NOT extend HOT entry when within first 25%")
    void processLocalHotkeyIfNeeded_shouldNotExtendWithinFirst25Percent() {
      long futureExpireAt = System.currentTimeMillis() + 120_000;
      caffeineCache.put(
        "key1",
        EntryDraft.blank(null)
          .value("v")
          .version(1)
          .decision(DecisionStamp.hot(5, null, 0L))
          .ttl(120_000, 30_000, 300_000, 30_000)
          .expiryAt(futureExpireAt, System.currentTimeMillis() + 60_000)
          .keyState(KeyState.HOT)
          .build()
      );
      when(hotKeyDetector.contains("key1")).thenReturn(true);

      hotKeyCache.get("key1", () -> "loaded", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH));

      CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key1");
      assertThat(entry.getHardExpireAtMs()).isEqualTo(futureExpireAt);
    }

    @Test
    @DisplayName("processLocalHotkeyIfNeeded should NOT extend HOT entry with MAX_VALUE hardExpireAt")
    void processLocalHotkeyIfNeeded_shouldNotExtendMaxValueExpiry() {
      caffeineCache.put(
        "key1",
        EntryDraft.blank(null)
          .value("v")
          .version(1)
          .decision(DecisionStamp.hot(5, null, 0L))
          .ttl(60_000, 30_000, 300_000, 30_000)
          .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 60_000)
          .keyState(KeyState.HOT)
          .build()
      );
      when(hotKeyDetector.contains("key1")).thenReturn(true);

      hotKeyCache.get("key1", () -> "loaded", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH));

      CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key1");
      assertThat(entry.getHardExpireAtMs()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    @DisplayName("loadAndCache preserves Worker-managed entry in hot path")
    void loadAndCache_withWorkerManagedEntryInHotPath_preservesIt() {
      when(hotKeyDetector.contains("key1")).thenReturn(true);
      when(singleFlight.load(eq("key1"), any(), anyBoolean(), any())).thenAnswer(invocation -> {
        Supplier<?> reader = invocation.getArgument(1);
        caffeineCache.put(
          "key1",
          EntryDraft.blank(null)
            .value("workerValue")
            .version(100)
            .decision(DecisionStamp.hot(42, null, 0L))
            .ttl(3_600_000, 300_000, 300_000, 30_000)
            .expiryAt(Long.MAX_VALUE, Long.MAX_VALUE)
            .keyState(KeyState.HOT)
            .build()
        );
        Object v = reader.get();
        if (v != null) {
          java.util.function.Consumer<Object> commit = invocation.getArgument(3);
          commit.accept(v);
        }
        return v == null ? LoaderOutcome.sourceEmpty() : LoaderOutcome.loaded(v);
      });

      String result = hotKeyCache.get("key1", () -> "newValue", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH));

      assertThat(result).isEqualTo("newValue");
      Object raw = caffeineCache.getIfPresent("key1");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      CacheEntry entry = (CacheEntry) raw;
      assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
      assertThat(entry.getValue()).as("the freshly loaded value wins, the HOT regime stays").isEqualTo("newValue");
    }
  }

  // ── putThrough with existing CacheEntry: buildPutThroughEntry deeper branches ──

  @Test
  void putThrough_withExistingEntry_shouldPreserveDecisionMetadata() {
    caffeineCache.put(
      "key1",
      EntryDraft.blank(null)
        .value("oldValue")
        .version(Long.MIN_VALUE)
        .decision(DecisionStamp.hot(42, "worker-1", 7))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 60_000)
        .keyState(KeyState.HOT)
        .build()
    );

    hotKeyCache.putThrough("key1", "newValue", 0L, 0L);

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
      EntryDraft.blank(null)
        .value("old")
        .version(Long.MIN_VALUE)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 777_777, 77_777)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 60_000)
        .keyState(KeyState.HOT)
        .build()
    );

    hotKeyCache.putThrough("key1", "newValue", 50000L, 5000L);

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
      EntryDraft.blank(null)
        .value("original")
        .version(Long.MAX_VALUE)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 60_000)
        .keyState(KeyState.HOT)
        .build()
    );

    hotKeyCache.putThrough("key1", "newValue", 0L, 0L);

    Object raw = caffeineCache.getIfPresent("key1");
    assertThat(raw).isInstanceOf(CacheEntry.class);
    CacheEntry entry = (CacheEntry) raw;
    assertThat(entry.getValue()).isEqualTo("original");
  }

  /**
   * Keys are used as passed by the caller (key即传即用): a key containing
   * {@code ?} is a distinct physical key and is never truncated.
   */
  @Test
  void tag_shouldUseKeyAsIs_queryStringIsDistinctKey() {
    hotKeyCache.recordAccess("a?x=1");
    hotKeyCache.recordAccess("b?y=2", Zeta.TagMode.RECORD);

    // design decisions: access counting: one merged add per read into the shared counter.
    verify(accessCounter).record("a?x=1", Zeta.TagMode.RECORD);
    verify(accessCounter).record("b?y=2", Zeta.TagMode.RECORD);
    verify(accessCounter, never()).add("a");
    verify(accessCounter, never()).add("b");
  }

  /**
   * REVALIDATE on a soft-expired Worker-managed entry keeps the entry (and its
   * decision stamp) in L1 while still serving a freshly loaded value; rebuilding
   * the entry locally would drop the Worker metadata and diverge from
   * computeInLock's REVALIDATE and loadCacheEntry's Worker-managed guard.
   * Keep-and-renew: the kept entry's soft expiry is
   * renewed (within the untouched hard TTL) so the loader is not re-invoked
   * at read rate — COOL entries never rebroadcast, and HOT entries only get
   * renewal from the Worker on the cadence (design decisions: state machine).
   */
  @Test
  void getWithSoftExpire_revalidateOnSoftExpiredWorkerHotEntry_shouldKeepEntryAndDecisionStamp() {
    when(healthView.isAlive("w1")).thenReturn(true);
    when(healthView.epochOf("w1")).thenReturn(5L);
    when(healthView.isCurrentIncarnation("w1", 5L)).thenReturn(true);
    CacheEntry hotEntry = softExpiredWorkerHotEntry("w1", 5L);
    caffeineCache.put("key1", hotEntry);
    SingleFlightStubs.stubLoad(singleFlight, oc("loadedValue"));

    String result = hotKeyCache.get("key1", () -> "loadedValue", GetPolicy.of(0L, 0L, true, StalePolicy.REVALIDATE));

    assertThat(result).isEqualTo("loadedValue");
    CacheEntry kept = (CacheEntry) caffeineCache.getIfPresent("key1");
    assertThat(kept.getDecisionNodeId()).isEqualTo("w1");
    assertThat(kept.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(kept.getHardExpireAtMs()).isEqualTo(Long.MAX_VALUE);
    // Keep-and-renew: the soft expiry moved past the read (clamped by the
    // hard TTL — Long.MAX_VALUE here), so the loader is not re-invoked at
    // read rate and the entry instance was rewritten by the renewal.
    assertThat(entryLifecycle.ttlPolicy().isSoftExpired(kept)).isFalse();
    assertThat(kept).isNotSameAs(hotEntry);
  }

  /** A Worker-stamped HOT entry whose soft TTL has passed while the hard TTL is still valid. */
  private static CacheEntry softExpiredWorkerHotEntry(String nodeId, long epoch) {
    return EntryDraft.blank(null)
      .value("stored")
      .version(1)
      .decision(DecisionStamp.hot(42, nodeId, epoch))
      .ttl(3_600_000, 300_000, 60_000, 15_000)
      .expiryAt(Long.MAX_VALUE, 1L)
      .keyState(KeyState.HOT)
      .build();
  }

  /**
   * Write/invalidation → dedup-cache coherence and load-path version guards
   * (design decisions: dedup lifecycle). Uses a REAL {@link SingleFlightImpl} so the completed-future
   * reuse window (design decisions: dedup lifecycle) is exercised end-to-end against the invalidation
   * and write paths.
   */
  @Nested
  @DisplayName("Dedup coherence and load guards (design decisions: dedup lifecycle)")
  class DedupCoherenceTest {

    private HotKeyCache realFlightCache;
    private Cache<String, CacheEntry> realFlightL1;
    private StampClock stampClock;

    @BeforeEach
    void setUp() {
      realFlightL1 = Caffeine.newBuilder().maximumSize(100).build();
      CircuitBreaker breaker = mock(CircuitBreaker.class);
      when(breaker.isOpen()).thenReturn(false);
      when(breaker.tryAcquireStamp()).thenReturn(CircuitBreaker.STAMP_CLOSED);
      // Registry-based construction path (see ZetaAutoConfiguration): every key's shard is the mock.
      CircuitBreakerRegistry breakerRegistry = mock(CircuitBreakerRegistry.class);
      when(breakerRegistry.breakerFor(anyString())).thenReturn(breaker);
      SingleFlight realSingleFlight = new SingleFlightImpl(1000, 5, 5, Runnable::run, breakerRegistry);
      stampClock = mock(StampClock.class);
      // Monotone per call: a load that began first holds the lower stamp.
      AtomicLong stampSequence = new AtomicLong(3L);
      when(stampClock.nextStamp()).thenAnswer(inv -> stampSequence.incrementAndGet());
      when(stampClock.nextStamps(org.mockito.ArgumentMatchers.anyInt())).thenAnswer(inv -> {
        int n = inv.getArgument(0);
        return stampSequence.addAndGet(n) - n + 1;
      });

      realFlightCache = new HotKeyCache(
        hotKeyDetector,
        realFlightL1,
        realSingleFlight,
        new EntryLifecycleImpl(realFlightL1, ttlConfig, healthView),
        new DefaultBackgroundRefresher(
          realFlightL1,
          Runnable::run,
          new EntryLifecycleImpl(realFlightL1, ttlConfig, healthView),
          10
        ),
        new AccessCounter(hotKeyDetector, null, scheduler, 50L, AccessCounter.DEFAULT_DETECTOR_INTERVAL_MS),
        new BroadcastBuffer(scheduler, null, 500, 2_000, null),
        stampClock,
        healthView
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
      assertThat(
        (Object) realFlightCache.get(
          "k",
          () -> {
            readerCalls.incrementAndGet();
            return "A";
          },
          GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH)
        )
      ).isEqualTo("A");

      realFlightCache.putThrough("k", "B", 0, 0);
      assertThat((Object) realFlightCache.peek("k")).isEqualTo("B");

      // Simulate the L1 loss the dedup replay would ride on: an eviction, a
      // peer INVALIDATE, or (pre-design decisions: dedup lifecycle) the sender's own REFRESH drop.
      realFlightL1.invalidate("k");

      String after = realFlightCache.get(
        "k",
        () -> {
          readerCalls.incrementAndGet();
          return "B-fresh";
        },
        GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH)
      );

      assertThat(after).isEqualTo("B-fresh");
      assertThat(readerCalls.get()).as("the post-write miss must re-invoke the reader").isEqualTo(2);
    }

    /** Same dedup contract for {@code invalidate}: the next miss re-reads. */
    @Test
    void invalidate_invalidatesDedupEntry_nextMissRereadsSource() {
      AtomicInteger readerCalls = new AtomicInteger();
      assertThat(
        (Object) realFlightCache.get(
          "k",
          () -> {
            readerCalls.incrementAndGet();
            return "A";
          },
          GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH)
        )
      ).isEqualTo("A");

      realFlightCache.invalidate("k", new InvalidatePolicy(InvalidatePolicy.Scope.LOCAL, false));

      String after = realFlightCache.get(
        "k",
        () -> {
          readerCalls.incrementAndGet();
          return "fresh";
        },
        GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH)
      );

      assertThat(after).isEqualTo("fresh");
      assertThat(readerCalls.get()).isEqualTo(2);
    }

    /**
     * The load-path version guard: a load whose reader raced a concurrent
     * putThrough (read pre-commit, design decisions: version stamps probe stamped post-INCR at the
     * same version) must not regress L1 — the write's value survives.
     */
    @Test
    void stampedLoad_notNewerThanExisting_refusesToRegressConcurrentWrite() {
      String served = realFlightCache.get(
        "k",
        () -> {
          // Concurrent write landing mid-load, before the load's own probe.
          realFlightCache.putThrough("k", "B", 0, 0);
          return "A-old";
        },
        GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH)
      );

      // The caller still sees what its reader loaded (the read raced the
      // commit), but L1 must hold the write's value, not the stale reload.
      assertThat(served).isEqualTo("A-old");
      CacheEntry kept = (CacheEntry) realFlightL1.getIfPresent("k");
      assertThat(kept.getDataVersion()).isEqualTo(5L);
      assertThat(kept.getValue()).isEqualTo("B");
    }

    /**
     * A null load must leave a Worker-managed entry that landed mid-load
     * completely untouched: the v2 null contract writes nothing (no sentinel
     * exists), so the Worker's value, decision stamp and dataVersion survive.
     */
    @Test
    void nullLoad_shouldLeaveWorkerManagedEntryUntouched() {
      String served = realFlightCache.get(
        "k",
        () -> {
          realFlightL1.put("k", workerHotEntry("w1", 1L));
          return null;
        },
        GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH)
      );

      assertThat(served).isNull();
      // Null answer writes nothing: the Worker-managed entry survives intact.
      CacheEntry kept = (CacheEntry) realFlightL1.getIfPresent("k");
      assertThat(kept.getValue()).isEqualTo("stored");
      assertThat(kept.getKeyState()).isEqualTo(KeyState.HOT);
      assertThat(kept.getDecisionNodeId()).isEqualTo("w1");
      assertThat(kept.getDecisionEpoch()).isEqualTo(1L);
      // v2 null contract: the null answer wrote nothing — the Worker entry's
      // own dataVersion (1) is untouched, and the probed version (6) is not
      // stamped onto it by a load that produced no value.
      assertThat(kept.getDataVersion()).isEqualTo(1L);
    }

    /** A null load at the same version must not clobber an existing normal entry. */
    @Test
    void nullLoad_doesNotClobberSameVersionEntry() {
      realFlightCache.get(
        "k",
        () -> {
          realFlightL1.put("k", normalEntry(4L));
          return null;
        },
        GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH)
      );

      CacheEntry kept = (CacheEntry) realFlightL1.getIfPresent("k");
      assertThat(kept.getValue()).isEqualTo("v");
      assertThat(kept.getDataVersion()).isEqualTo(4L);
    }

    /** An unstamped (fail-open) null load carries no version authority: it never replaces an entry. */
    @Test
    void unstampedNullLoad_doesNotReplaceExistingEntry() {
      realFlightCache.get(
        "k",
        () -> {
          realFlightL1.put("k", normalEntry(3L));
          return null;
        },
        GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH)
      );

      CacheEntry kept = (CacheEntry) realFlightL1.getIfPresent("k");
      assertThat(kept.getValue()).isEqualTo("v");
      assertThat(kept.getDataVersion()).isEqualTo(3L);
    }

    /**
     * A null load whose probe is strictly newer than the resident entry still
     * writes nothing (no sentinel exists in v2): the older normal entry stays.
     */
    @Test
    void nullLoad_probedNewer_shouldLeaveEntryUntouched() {
      realFlightCache.get(
        "k",
        () -> {
          realFlightL1.put("k", normalEntry(3L));
          return null;
        },
        GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH)
      );

      // Null answer writes nothing (no sentinel exists): the older entry stays.
      CacheEntry kept = (CacheEntry) realFlightL1.getIfPresent("k");
      assertThat(kept.getValue()).isEqualTo("v");
      assertThat(kept.getDataVersion()).isEqualTo(3L);
      assertThat(kept.getKeyState()).isEqualTo(KeyState.NORMAL);
    }

    private static CacheEntry normalEntry(long dataVersion) {
      return EntryDraft.blank(null)
        .value("v")
        .version(dataVersion)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 0, 300_000, 0)
        .expiryAt(Long.MAX_VALUE, 0)
        .keyState(KeyState.NORMAL)
        .build();
    }
  }
}
