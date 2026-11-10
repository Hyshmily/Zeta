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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
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
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.scheduler.BackgroundRefresher;
import io.github.hyshmily.zeta.scheduler.DefaultBackgroundRefresher;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.clock.StampClock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for the write-path guard gaps found while auditing
 * {@link HotKeyCache} against the load/populate paths
 * (design decisions: invalidation, write path, dedup lifecycle, version stamps):
 *
 * <ul>
 *   <li><b>putThrough/refresh resurrection:</b> the write-through apply shared no
 *       anti-resurrection barrier with the other L1 write paths, so an
 *       INVALIDATE landing between the stamp allocation and the compute was
 *       undone by the apply.</li>
 *   <li><b>Refused writes broadcast:</b> a write the local guard refused (newer
 *       resident, or tombstoned) still shipped its stale (value, stamp) to
 *       peers.</li>
 *   <li><b>Worker-regime clobber:</b> the write-through rebuild reset a COOL/HOT
 *       entry's active TTL timestamps to the configured defaults while keeping
 *       the Worker state, and dropped the decision lease with them.</li>
 *   <li><b>Batch duplicate keys:</b> a repeated key was probed, counted and
 *       access-recorded twice, and misfired the batched load's in-flight guard
 *       against its own released ticket.</li>
 *   <li><b>Stale serve is silent:</b> a breaker-open stale fallback recorded no
 *       access, erasing the key's hot-key history for the length of the
 *       outage.</li>
 * </ul>
 */
class HotKeyCacheWriteGuardRegressionTest {

  private HotKeyDetector hotKeyDetector;
  private AccessCounter accessCounter;
  private Cache<String, CacheEntry> caffeineCache;
  private CircuitBreaker circuitBreaker;
  private CircuitBreakerRegistry breakerRegistry;
  private Executor executor;
  private EntryLifecycle entryLifecycle;
  private BackgroundRefresher backgroundRefresher;
  private ScheduledExecutorService scheduler;
  private HealthView healthView;
  private ZetaProperties ttlConfig;
  private StampClock stampClock;

  @BeforeEach
  void setUp() {
    hotKeyDetector = mock(HotKeyDetector.class);
    when(hotKeyDetector.contains(anyString())).thenReturn(false);
    accessCounter = mock(AccessCounter.class);
    caffeineCache = Caffeine.newBuilder().maximumSize(100).build();
    circuitBreaker = mock(CircuitBreaker.class);
    when(circuitBreaker.tryAcquireStamp()).thenReturn(CircuitBreaker.STAMP_CLOSED);
    when(circuitBreaker.isOpen()).thenReturn(false);
    // Registry-based construction path (see ZetaAutoConfiguration): every key's shard is the mock.
    breakerRegistry = mock(CircuitBreakerRegistry.class);
    when(breakerRegistry.breakerFor(anyString())).thenReturn(circuitBreaker);
    executor = Runnable::run;
    ttlConfig = new ZetaProperties();
    healthView = mock(HealthView.class);
    entryLifecycle = new EntryLifecycleImpl(caffeineCache, ttlConfig, healthView);
    backgroundRefresher = new DefaultBackgroundRefresher(caffeineCache, executor, entryLifecycle, 10);
    scheduler = Executors.newSingleThreadScheduledExecutor();
    stampClock = mock(StampClock.class);
  }

  @AfterEach
  void tearDown() {
    scheduler.shutdownNow();
  }

  private HotKeyCache newCache() {
    return newCache(null, new TestSingleFlight(10_000, 5, 5, executor, breakerRegistry));
  }

  private HotKeyCache newCache(CacheSyncPublisher publisher) {
    return newCache(publisher, new TestSingleFlight(10_000, 5, 5, executor, breakerRegistry));
  }

  private HotKeyCache newCache(SingleFlight singleFlight) {
    return newCache(null, singleFlight);
  }

  private HotKeyCache newCache(CacheSyncPublisher publisher, SingleFlight singleFlight) {
    return new HotKeyCache(
      hotKeyDetector,
      caffeineCache,
      singleFlight,
      entryLifecycle,
      backgroundRefresher,
      accessCounter,
      new BroadcastBuffer(scheduler, publisher, 500, 2_000, null),
      stampClock,
      healthView
    );
  }

  // ── Tombstone barrier on the write-through apply ──

  /**
   * The refresh path bumps, then loads, then applies — so an INVALIDATE that
   * lands while the loader runs allocates a newer stamp and removes the entry.
   * The apply must consult the tombstone like every other L1 write path
   * (loadCacheEntry, putLocal): on the absent entry the
   * resident guard has nothing to compare against, and without the barrier the
   * removed value is re-written at the refresh's older stamp.
   */
  @Test
  @DisplayName("refresh apply is refused when an invalidate landed during the load")
  void refresh_applyRefusedAfterInvalidateLandedDuringLoad() {
    when(stampClock.nextStamp()).thenReturn(10L, 11L);
    HotKeyCache cache = newCache();

    cache.refresh(
      "k",
      () -> {
        // Lands after the refresh sampled its stamp (10): the invalidation
        // allocates 11 and records the tombstone.
        cache.invalidate("k", new InvalidatePolicy(InvalidatePolicy.Scope.LOCAL, false));
        return "refreshed";
      },
      0L,
      0L,
      false
    );

    assertThat(caffeineCache.getIfPresent("k")).as("the invalidated key must not be resurrected").isNull();
  }

  // ── A null refresh never reaches the cache ──

  /**
   * A refresh whose loader answers {@code null} must leave the resident entry
   * alone on <b>every</b> call.
   */
  @Test
  @DisplayName("a null refresh loader keeps the resident entry on every call")
  void refresh_nullLoader_neverReplacesTheResidentEntry() {
    when(stampClock.nextStamp()).thenReturn(10L, 11L, 12L);
    HotKeyCache cache = newCache();
    caffeineCache.put("k", normalEntry("v1", 5L));

    for (int i = 0; i < 3; i++) {
      cache.refresh("k", () -> null, 0L, 0L, false);
    }

    CacheEntry entry = caffeineCache.getIfPresent("k");
    assertThat(entry).as("the resident entry must survive repeated null refreshes").isNotNull();
    assertThat(entry.getValue()).isEqualTo("v1");
  }

  // ── A refused write broadcasts nothing ──
  /**
   * A write the local guard refused — here a strictly newer resident — must not
   * ship its (value, stamp) to peers: a peer without the newer entry would
   * apply it and diverge, and after a tombstone the broadcast would re-warm on
   * peers exactly what this node just invalidated. The applied/refused
   * distinction is the same one the load path makes before touching the dedup
   * entry.
   */
  @Test
  @DisplayName("putThrough refused by a newer resident broadcasts nothing")
  void putThrough_refusedWrite_broadcastsNothing() {
    CacheSyncPublisher publisher = mock(CacheSyncPublisher.class);
    HotKeyCache cache = newCache(publisher);
    when(stampClock.nextStamp()).thenReturn(5L);
    caffeineCache.put("k", normalEntry("fresh", 9L));

    cache.putThrough("k", "stale-write", 0L, 0L);

    assertThat(((CacheEntry) caffeineCache.getIfPresent("k")).getValue()).isEqualTo("fresh");
    verify(publisher, never()).serializePutPayload(any());
  }

  /** Control: an applied write does broadcast (the gate must not mute the normal path). */
  @Test
  @DisplayName("putThrough applied write broadcasts the value-carrying PUT")
  void putThrough_appliedWrite_broadcasts() {
    CacheSyncPublisher publisher = mock(CacheSyncPublisher.class);
    HotKeyCache cache = newCache(publisher);
    when(stampClock.nextStamp()).thenReturn(5L);

    cache.putThrough("k", "newValue", 0L, 0L);

    assertThat((Object) cache.peek("k")).isEqualTo("newValue");
    verify(publisher).serializePutPayload("newValue");
  }

  // ── Worker-managed TTL regime survives a write-through ──

  /**
   * The Worker owns the TTL regime of a COOL entry: a putThrough/refresh is a
   * value write, not a regime change. The former rebuild through
   * {@code newEntry} reset the active hard/soft timestamps to the configured
   * defaults while keeping the COOL state — extending the cooling window past
   * the Worker's bound — and silently dropped the decision lease, the demotion
   * anchor for orphaned HOT entries.
   */
  @Test
  @DisplayName("refresh on a COOL entry keeps the Worker's TTL regime, state, stamp and lease")
  void refresh_workerManagedEntry_preservesRegime() {
    when(stampClock.nextStamp()).thenReturn(10L);
    HotKeyCache cache = newCache();
    CacheEntry cool = workerCoolEntry("w1", 5L);
    caffeineCache.put("k", cool);

    cache.refresh("k", () -> "v2", 0L, 0L, false);

    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(entry.getValue()).isEqualTo("v2");
    assertThat(entry.getKeyState()).isEqualTo(KeyState.COOL);
    assertThat(entry.getDecisionNodeId()).isEqualTo("w1");
    assertThat(entry.getDecisionVersion()).isEqualTo(42L);
    // The Worker's hard bound survives verbatim — the configured default (5 min)
    // must not replace the Worker's 10-minute cooling window.
    assertThat(entry.getHardExpireAtMs()).isEqualTo(cool.getHardExpireAtMs());
    assertThat(entry.getHardTtlMs()).isEqualTo(600_000L);
    // The soft window is re-armed for the fresh value, capped by the hard bound.
    assertThat(entry.getSoftExpireAtMs()).isBetween(
      cool.getSoftExpireAtMs() - 5_000L,
      cool.getSoftExpireAtMs() + 5_000L
    );
    assertThat(entry.getDataVersion()).isEqualTo(10L);
  }

  /** Same regime contract on the putThrough shape of the write-through apply. */
  @Test
  @DisplayName("putThrough on a COOL entry keeps the Worker's TTL regime")
  void putThrough_workerManagedEntry_preservesRegime() {
    when(stampClock.nextStamp()).thenReturn(10L);
    HotKeyCache cache = newCache();
    CacheEntry cool = workerCoolEntry("w1", 5L);
    caffeineCache.put("k", cool);

    cache.putThrough("k", "v2", 0L, 0L);

    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(entry.getValue()).isEqualTo("v2");
    assertThat(entry.getKeyState()).isEqualTo(KeyState.COOL);
    assertThat(entry.getHardExpireAtMs()).isEqualTo(cool.getHardExpireAtMs());
    assertThat(entry.getNormalHardTtlMs()).isEqualTo(cool.getNormalHardTtlMs());
  }

  // ── Batch reads dedup their keys ──

  /**
   * A repeated key ("k", "k") used to be probed, counted and access-recorded
   * twice, and the batched load then misfired its own in-flight guard: the
   * first iteration released the key's dedup ticket, so the second saw
   * {@code !isCurrent} and treated its own release as a concurrent
   * invalidation — skipping the store and adding a second access record for
   * one logical read.
   */
  @Test
  @DisplayName("batch get probes, counts and loads each unique key once")
  void batchGet_duplicateKeys_countedAndLoadedOnce() {
    HotKeyCache cache = newCache();
    AtomicInteger readerCalls = new AtomicInteger();

    Map<String, String> result = cache.get(
      List.of("k", "k"),
      key -> {
        readerCalls.incrementAndGet();
        return "v";
      },
      GetPolicy.of(0L, 0L, true, StalePolicy.RETURN)
    );

    assertThat(result).containsOnlyKeys("k").containsEntry("k", "v");
    assertThat(readerCalls.get()).isEqualTo(1);
    assertThat(cache.stats().missCount()).isEqualTo(1);
    assertThat(cache.stats().hitCount()).isZero();
    verify(accessCounter, times(1)).record("k", true);
  }

  /** Control for the duplicate-key fix: a repeated HIT counts once as well. */
  @Test
  @DisplayName("batch get counts a repeated hit once")
  void batchGet_duplicateHitKeys_countedOnce() {
    HotKeyCache cache = newCache();
    caffeineCache.put("k", normalEntry("v", 3L));

    Map<String, String> result = cache.get(List.of("k", "k"), key -> "v", GetPolicy.defaults());

    assertThat(result).containsOnlyKeys("k").containsEntry("k", "v");
    assertThat(cache.stats().hitCount()).isEqualTo(1);
    assertThat(cache.stats().missCount()).isZero();
    verify(accessCounter, times(1)).record("k", true);
  }

  // ── A stale serve is a served read ──

  /**
   * Under an open breaker the suppressed load degrades to a hard-valid stale
   * entry. That serve used to record no access, so a key hot enough to be read
   * throughout the outage stopped looking hot exactly when the Worker needed
   * to know — the access history of the outage window was silently erased.
   */
  @Test
  @DisplayName("breaker-open stale serve records the access")
  void staleServe_underOpenBreaker_recordsAccess() {
    SingleFlight flight = mock(SingleFlight.class);
    when(flight.isBreakerOpen("k")).thenReturn(true);
    SingleFlightStubs.stubLoad(flight, LoaderOutcome.suppressed(LoadStatus.SUPPRESSED_BREAKER));
    HotKeyCache cache = newCache(flight);
    // Soft-expired but hard-valid: a REVALIDATE read blocks on the loader, the
    // load is suppressed by the open breaker, and the read degrades to the
    // stale fallback.
    long now = TimeSource.currentTimeMillis();
    caffeineCache.put(
      "k",
      EntryDraft.blank(null)
        .value("stale")
        .version(3)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(now + 300_000, now - 1_000)
        .keyState(KeyState.NORMAL)
        .build()
    );

    Object served = cache.get("k", () -> "fresh", GetPolicy.of(0L, 0L, true, StalePolicy.REVALIDATE));

    assertThat(served).isEqualTo("stale");
    verify(accessCounter, times(1)).record("k", true);
  }

  // ── fixtures ──

  /** A plain NORMAL entry stamped with the given data version. */
  private static CacheEntry normalEntry(String value, long dataVersion) {
    return EntryDraft.blank(null)
      .value(value)
      .version(dataVersion)
      .decision(DecisionStamp.hot(0, null, 0L))
      .ttl(300_000, 30_000, 300_000, 30_000)
      .expiryAt(Long.MAX_VALUE, Long.MAX_VALUE)
      .keyState(KeyState.NORMAL)
      .build();
  }

  /**
   * A Worker-stamped COOL entry with a 10-minute cooling window (60s/15s
   * normal baseline) — the regime a local write must not touch.
   */
  private static CacheEntry workerCoolEntry(String nodeId, long epoch) {
    long now = TimeSource.currentTimeMillis();
    return EntryDraft.blank(null)
      .value("stored")
      .version(1)
      .decision(DecisionStamp.hot(42, nodeId, epoch))
      .ttl(600_000, 60_000, 60_000, 15_000)
      .expiryAt(now + 600_000, now + 60_000)
      .keyState(KeyState.COOL)
      .build();
  }
}
