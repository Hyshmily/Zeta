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
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.scheduler.BackgroundRefresher;
import io.github.hyshmily.zeta.scheduler.DefaultBackgroundRefresher;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.clock.StampClock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for call-chain conflicts found while tracing the public API
 * end to end (read / write / invalidate / fluent paths).
 *
 * <ul>
 *   <li><b>Writer-side equal-version guard:</b> {@code putThrough}'s local apply
 *       must not skip on an EQUAL version — the reader's design decisions: version stamps probe can stamp
 *       pre-write data with the writer's own post-INCR version, and the sender's
 *       self-REFRESH is dropped (design decisions: dedup lifecycle), so an equal-version skip pins the stale
 *       value on the writer instance until TTL.</li>
 *   <li><b>putLocal dedup invariant (design decisions: dedup lifecycle):</b> a completed SingleFlight result
 *       must not outlive the L1 write that obsoletes it — {@code putLocal} is a
 *       local write with no version bump, so the only protection is dedup
 *       invalidation.</li>
 *   <li><b>putLocal on a Worker-managed entry:</b> the Worker owns the TTL regime
 *       of a HOT/COOL entry; a local value write must keep it (the Worker cannot
 *       repair clobbered TTLs — the same-decisionVersion guard skips the rebroadcast).</li>
 *   <li><b>NORMAL-entry soft refresh:</b> {@code getWithSoftExpire} must trigger the
 *       background refresh for a soft-expired NORMAL entry exactly like
 *       {@code computeIfAbsent} does — {@code registerRefresh}'s "every cycle
 *       triggers an async refresh" contract depends on it.</li>
 * </ul>
 */
class HotKeyCacheConflictTest {

  private HotKeyDetector hotKeyDetector;
  private KeyReporter reporter;
  private AccessCounter accessCounter;
  private Cache<String, CacheEntry> caffeineCache;
  private CircuitBreaker circuitBreaker;
  private CircuitBreakerRegistry breakerRegistry;
  private Executor executor;
  private EntryLifecycle entryLifecycle;
  private BackgroundRefresher backgroundRefresher;
  private java.util.concurrent.ScheduledExecutorService scheduler;
  private HealthView healthView;
  private ZetaProperties ttlConfig;
  private StampClock stampClock;
  private RecordingDispatcher dispatcher;

  @BeforeEach
  void setUp() {
    hotKeyDetector = mock(HotKeyDetector.class);
    when(hotKeyDetector.contains(anyString())).thenReturn(false);
    reporter = mock(KeyReporter.class);
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
    accessCounter = mock(AccessCounter.class);
    stampClock = mock(StampClock.class);
    dispatcher = new RecordingDispatcher();
  }

  @AfterEach
  void tearDown() {
    scheduler.shutdownNow();
  }

  /** Build a cache with a REAL SingleFlight (dedup exercised) and the given version controller. */
  private HotKeyCache newCache() {
    return newCache(new TestSingleFlight(10_000, 5, 5, executor, breakerRegistry));
  }

  private HotKeyCache newCache(SingleFlight singleFlight) {
    return new HotKeyCache(
      hotKeyDetector,
      caffeineCache,
      singleFlight,
      entryLifecycle,
      backgroundRefresher,
      dispatcher,
      stampClock,
      healthView
    );
  }

  /** The INVALIDATE_BATCH wire entries the cache broadcast (with a null publisher the plain dispatcher drops them). */
  private final class RecordingDispatcher extends CentralDispatcher {

    private final List<SyncMessage.Entry> invalidatedBatches = new ArrayList<>();

    private RecordingDispatcher() {
      super(accessCounter, null, new BroadcastBuffer(scheduler, null, 500, 2_000, null), null);
    }

    @Override
    public void broadcastInvalidateBatch(List<SyncMessage.Entry> entries) {
      invalidatedBatches.addAll(entries);
    }
  }

  // ── Writer-side equal-version guard (putThrough vs over-stamped read load) ──

  /**
   * The reader's probe-after-read (design decisions: version stamps) can absorb the writer's own INCR:
   * the load stores pre-write data stamped with the version putThrough is about
   * to allocate. The writer's local apply must overwrite an EQUAL version (its
   * value is authoritative at that version) — an equal-version skip pins the
   * stale value on the writer instance, where the self-REFRESH heal is dropped
   * by the origin filter (design decisions: dedup lifecycle).
   */
  @Test
  @DisplayName("putThrough applies its own write over an equal-version over-stamped load")
  void putThrough_equalVersionOverStamp_appliesOwnWrite() {
    // The load's pre-read stamp and the writer's stamp are both 5.
    when(stampClock.nextStamp()).thenReturn(5L);
    HotKeyCache cache = newCache();

    // The raced load: pre-write value stamped with the writer's version.
    cache.get("k", () -> "old", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH));
    assertThat(((CacheEntry) caffeineCache.getIfPresent("k")).getValue()).isEqualTo("old");

    cache.putThrough("k", "new", 0L, 0L);

    assertThat(((CacheEntry) caffeineCache.getIfPresent("k")).getValue()).isEqualTo("new");
  }

  /** Control: a genuinely newer concurrent write still wins — the guard must skip OLDER writes. */
  @Test
  @DisplayName("putThrough does not overwrite a strictly newer existing entry")
  void putThrough_olderWrite_skipsOnNewerExisting() {
    when(stampClock.nextStamp()).thenReturn(5L);
    HotKeyCache cache = newCache();
    caffeineCache.put("k", normalEntry("fresh", 9L));

    cache.putThrough("k", "old-write", 0L, 0L);

    assertThat(((CacheEntry) caffeineCache.getIfPresent("k")).getValue()).isEqualTo("fresh");
  }

  // ── bump-before-drop ordering (design decisions: invalidation) ──

  /**
   * The version must be allocated BEFORE the L1 removal on the single-key path
   * too — not only on the batch path.
   *
   * <p>Bumping after the drop reopens the window design decisions: invalidation closed: a reader that
   * misses after the drop reloads and stamps the value with the still-old version
   * {@code v}, then the INVALIDATE carrying {@code v+1} is skipped by the plain
   * {@code >=} matrix — pinning the stale value for its whole TTL with no
   * watermark to unpin it.
   *
   * <p>Pinned by observing the entry's presence at the moment the bump runs: an
   * implementation that drops first and bumps later yields the same final version
   * numbers, so end-state assertions cannot see the difference. If the bump ran
   * first the entry is still in L1; if it ran after the drop, the entry is gone.
   * The removal itself is asserted as the control for that observation.
   */
  @Test
  @DisplayName("single-key invalidate allocates its version before dropping the entry")
  void invalidate_singleKey_bumpsVersionBeforeDropping() {
    AtomicBoolean residentAtBump = new AtomicBoolean(false);
    when(stampClock.nextStamp()).thenAnswer(inv -> {
      residentAtBump.set(caffeineCache.getIfPresent("k") != null);
      return 21L;
    });
    HotKeyCache cache = newCache();
    caffeineCache.put("k", normalEntry("v", 20L));

    cache.invalidate("k", new InvalidatePolicy(InvalidatePolicy.Scope.BOTH, false));

    assertThat(residentAtBump).as("the entry must still be resident when the version is bumped").isTrue();
    assertThat(caffeineCache.getIfPresent("k")).as("the removal must actually happen").isNull();
    verify(stampClock).nextStamp();
  }

  /**
   * The batch path must allocate every version before dropping any of them. Interleaving
   * allocation and drop per key would leave the already-dropped keys on the old version,
   * re-opening the window for exactly those keys — so both keys must still be
   * resident when the (single, block) reservation runs.
   */
  @Test
  @DisplayName("batch invalidate allocates every version before dropping any entry")
  void invalidate_batch_bumpsAllKeysBeforeDroppingAny() {
    AtomicBoolean bothKeysResidentAtReservation = new AtomicBoolean(false);
    when(stampClock.nextStamps(org.mockito.ArgumentMatchers.anyInt())).thenAnswer(inv -> {
      bothKeysResidentAtReservation.set(
        caffeineCache.getIfPresent("a") != null && caffeineCache.getIfPresent("b") != null
      );
      return 21L;
    });
    HotKeyCache cache = newCache();
    caffeineCache.put("a", normalEntry("va", 20L));
    caffeineCache.put("b", normalEntry("vb", 20L));

    cache.invalidate(List.of("a", "b"), new InvalidatePolicy(InvalidatePolicy.Scope.BOTH, false));

    assertThat(bothKeysResidentAtReservation)
      .as("both keys must still be resident when the block is reserved")
      .isTrue();
    // One block reservation covers the whole wire chunk; the per-key path is gone.
    verify(stampClock, times(1)).nextStamps(2);
    verify(stampClock, never()).nextStamp();
    assertThat(caffeineCache.getIfPresent("a")).isNull();
    assertThat(caffeineCache.getIfPresent("b")).isNull();
  }

  /**
   * A batch invalidation allocates one distinct stamp per key before any drop,
   * reserved as a single contiguous clock block.
   *
   * <p>The stamps come from the node-local clock (design decisions: version stamps), so every key gets
   * a real, strictly increasing stamp; no key is left without an ordering
   * value for its INVALIDATE broadcast.
   */
  @Test
  @DisplayName("batch invalidate allocates a distinct stamp per key")
  void invalidate_batch_allocatesAStampPerKey() {
    long[] seq = { 30L };
    when(stampClock.nextStamps(org.mockito.ArgumentMatchers.anyInt())).thenAnswer(inv -> {
      int n = inv.getArgument(0);
      long base = seq[0];
      seq[0] += n;
      return base;
    });
    HotKeyCache cache = newCache();
    List<String> keys = List.of("a", "b", "c", "d", "e");

    cache.invalidate(keys, new InvalidatePolicy(InvalidatePolicy.Scope.BOTH, false));

    // One contiguous-block reservation for a chunk that fits the wire batch.
    verify(stampClock, times(1)).nextStamps(keys.size());
    // The block went out on the wire as one INVALIDATE_BATCH — base 30, one
    // entry per key in reservation order.
    assertThat(dispatcher.invalidatedBatches).hasSize(keys.size());
    for (int i = 0; i < keys.size(); i++) {
      assertThat(dispatcher.invalidatedBatches.get(i).key()).isEqualTo(keys.get(i));
      assertThat(dispatcher.invalidatedBatches.get(i).version()).isEqualTo(30L + i);
    }
  }

  /**
   * With broadcasting disabled nothing reaches a peer, but the local drop still
   * happens — the local-only invalidation contract
   * ({@code InvalidatePolicy.localOnly()}).
   *
   * <p>A stamp <em>is</em> allocated: it is the local tombstone that stops an
   * in-flight read from filling the value back (design decisions: invalidation).
   * It never reaches a peer.
   */
  @Test
  @DisplayName("local-only invalidate drops the entry and tombstones it without broadcasting")
  void invalidate_localOnly_dropsWithoutBumpOrBroadcast() {
    HotKeyCache cache = newCache();
    caffeineCache.put("k", normalEntry("v", 20L));

    cache.invalidate("k", new InvalidatePolicy(InvalidatePolicy.Scope.LOCAL, false));

    assertThat(caffeineCache.getIfPresent("k")).isNull();
    verify(stampClock).nextStamp();
    assertThat(dispatcher.invalidatedBatches).as("a local-only invalidation never reaches a peer").isEmpty();
  }

  // ── putLocal dedup invariant (design decisions: dedup lifecycle) ──

  /**
   * A completed SingleFlight result must not outlive the L1 write that obsoletes
   * it. putLocal carries no version bump, so a pre-write dedup result replayed
   * onto the next miss would re-cache the pre-write value over the local write.
   */
  @Test
  @DisplayName("putLocal invalidates the dedup result recorded before it")
  void putLocal_invalidatesDedupResultRecordedBeforeIt() {
    when(stampClock.nextStamp()).thenReturn(5L);
    HotKeyCache cache = newCache();

    // Load and cache — the dedup cache now holds "stale"@5 for the SingleFlight TTL.
    cache.get("k", () -> "stale", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH));

    // L1-only eviction: the dedup result survives (this is the window putLocal must close).
    caffeineCache.invalidate("k");

    // The local write (a local-only put, e.g. @ZetaPut(sync = false)) obsoletes the recorded load.
    cache.putLocal("k", "fallback", 0L, 0L);

    // The putLocal entry itself is gone (short TTL / eviction) while the dedup TTL lives.
    caffeineCache.invalidate("k");

    String result = cache.get("k", () -> "fresh", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH));

    // The reader must be re-invoked — the pre-write "stale" result was invalidated.
    assertThat(result).isEqualTo("fresh");
  }

  // ── putLocal on a Worker-managed entry ──

  /**
   * The Worker owns the TTL regime of a HOT entry: a local value write updates
   * the value but must keep the hot TTLs and the decision stamp — the Worker
   * cannot repair clobbered TTLs because its rebroadcast with the same
   * decisionVersion is skipped by the decision guard.
   */
  @Test
  @DisplayName("putLocal on a HOT entry keeps the Worker's TTL regime")
  void putLocal_workerManagedEntry_preservesTtlRegime() {
    when(stampClock.nextStamp()).thenReturn(5L);
    HotKeyCache cache = newCache();
    CacheEntry hot = workerHotEntry("w1", 5L);
    caffeineCache.put("k", hot);

    cache.putLocal("k", "new", 10_000L, 1_000L);

    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(entry.getValue()).isEqualTo("new");
    assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(entry.getDecisionNodeId()).isEqualTo("w1");
    assertThat(entry.getDecisionVersion()).isEqualTo(42L);
    // The Worker's TTL regime survives the local write.
    assertThat(entry.getHardTtlMs()).isEqualTo(3_600_000L);
    assertThat(entry.getSoftTtlMs()).isEqualTo(300_000L);
    assertThat(entry.getHardExpireAtMs()).isEqualTo(hot.getHardExpireAtMs());
    // A value write re-arms the soft window (capped by the hard bound), so a
    // previously soft-expired HOT entry does not keep forcing background reloads.
    assertThat(entry.getSoftExpireAtMs()).isLessThanOrEqualTo(entry.getHardExpireAtMs());
    assertThat(entry.getSoftExpireAtMs()).isGreaterThan(TimeSource.currentTimeMillis() + 250_000L);
  }

  // ── NORMAL-entry soft refresh (getWithSoftExpire ≡ computeIfAbsent) ──

  /**
   * A soft-expired NORMAL entry with a reader must trigger the background
   * refresh on {@code getWithSoftExpire} exactly like {@code computeIfAbsent}
   * does. {@code registerRefresh} schedules {@code getWithSoftExpire} ticks and
   * documents "every cycle triggers an async refresh" — silently refusing
   * NORMAL entries breaks that contract and the stale-while-revalidate semantics
   * of {@code zeta.local.soft-ttl-ms}.
   */
  @Test
  @DisplayName("getWithSoftExpire triggers the background refresh for a soft-expired NORMAL entry")
  void getWithSoftExpire_normalSoftExpiredEntry_triggersBackgroundRefresh() {
    HotKeyCache cache = newCache();
    // A soft-expired NORMAL entry (soft window lapsed, hard TTL still valid).
    caffeineCache.put("k", softExpiredNormalEntry("v1"));

    // The stale value is still served, but the refresh must re-arm and apply.
    String served = cache.get("k", () -> "v2", GetPolicy.of(60_000L, 30_000L, true, StalePolicy.SOFT_REFRESH));

    assertThat(served).isEqualTo("v1");
    // The inline refresh executor applies the fresh value before the call returns.
    assertThat(((CacheEntry) caffeineCache.getIfPresent("k")).getValue()).isEqualTo("v2");
  }

  /**
   * A plain {@code get} whose policy is {@code SOFT_REFRESH} must arm the
   * background refresh exactly like {@code getWithSoftExpire}. The read path
   * used to hard-code "no soft refresh" for the plain overload, so a spec
   * registered with {@code softTtl(...)} never refreshed: the caller kept
   * reading an ever-older value with no reload until the hard TTL expired.
   */
  @Test
  @DisplayName("plain get arms the background refresh when the policy is SOFT_REFRESH")
  void get_plainOverloadWithSoftRefreshPolicy_triggersBackgroundRefresh() {
    HotKeyCache cache = newCache();
    caffeineCache.put("k", softExpiredNormalEntry("v1"));

    String served = cache.get("k", () -> "v2", GetPolicy.of(60_000L, 30_000L, true, StalePolicy.SOFT_REFRESH));

    assertThat(served).isEqualTo("v1");
    // The inline refresh executor applies the fresh value before the call returns.
    assertThat(((CacheEntry) caffeineCache.getIfPresent("k")).getValue()).isEqualTo("v2");
  }

  /**
   * Control: {@code RETURN} is honoured as "serve stale, never refresh" — the
   * fix must not turn every policy into a refresh.
   */
  @Test
  @DisplayName("plain get with RETURN serves the stale value and refreshes nothing")
  void get_plainOverloadWithReturnPolicy_doesNotRefresh() {
    HotKeyCache cache = newCache();
    caffeineCache.put("k", softExpiredNormalEntry("v1"));

    String served = cache.get("k", () -> "v2", GetPolicy.of(60_000L, 30_000L, true, StalePolicy.RETURN));

    assertThat(served).isEqualTo("v1");
    assertThat(((CacheEntry) caffeineCache.getIfPresent("k")).getValue()).isEqualTo("v1");
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

  /** Build a plain NORMAL entry stamped with the given data version. */
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

  /** Build a soft-expired NORMAL entry: the soft window lapsed, the hard TTL is still valid. */
  private static CacheEntry softExpiredNormalEntry(String value) {
    return EntryDraft.blank(null)
      .value(value)
      .version(5)
      .decision(DecisionStamp.hot(0, null, 0L))
      .ttl(300_000, 30_000, 300_000, 30_000)
      .expiryAt(TimeSource.currentTimeMillis() + 300_000, TimeSource.currentTimeMillis() - 1)
      .keyState(KeyState.NORMAL)
      .build();
  }

  /**
   * design decisions: dedup lifecycle across an in-flight load: invalidating a key while its reader is
   * still running must serve the already-waiting callers but must not let the
   * pre-invalidation value take L1 residency again. The dedup future's removal
   * is the generation signal; a later read reloads.
   */
  @Test
  @DisplayName("invalidate during an in-flight load serves but does not repopulate L1")
  void invalidateDuringLoad_doesNotRepopulateL1() throws Exception {
    when(stampClock.nextStamp()).thenReturn(10L, 11L);
    ExecutorService loadPool = Executors.newSingleThreadExecutor();
    ExecutorService caller = Executors.newSingleThreadExecutor();
    try {
      HotKeyCache cache = newCache(new TestSingleFlight(1_000, 5, 5, loadPool, breakerRegistry));
      CountDownLatch entered = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      Future<String> inFlight = caller.submit(() ->
        cache.get(
          "k",
          () -> {
            entered.countDown();
            try {
              release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            return "old";
          },
          GetPolicy.defaults()
        )
      );
      assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

      cache.invalidate("k", new InvalidatePolicy(InvalidatePolicy.Scope.LOCAL, false));
      release.countDown();
      assertThat(inFlight.get(5, TimeUnit.SECONDS)).isEqualTo("old");

      Object servedAfterInvalidate = cache.peek("k");
      assertThat(servedAfterInvalidate).isNull();
      AtomicInteger reloads = new AtomicInteger();
      assertThat(
        cache.get(
          "k",
          () -> {
            reloads.incrementAndGet();
            return "new";
          },
          GetPolicy.defaults()
        )
      ).isEqualTo("new");
      assertThat(reloads.get()).isEqualTo(1);
    } finally {
      caller.shutdownNow();
      loadPool.shutdownNow();
    }
  }

  /**
   * The batch read submits and releases its pinned dedup futures under the same
   * key. A mismatch leaves the completed future replayable for its whole dedup
   * TTL — a post-eviction miss would re-cache the old value without invoking
   * the reader.
   */
  @Test
  @DisplayName("batch get unpins its dedup future under the submitted key")
  void batchGet_releasesTicketUnderTheSubmittedKey() {
    when(stampClock.nextStamp()).thenReturn(1L);
    TestSingleFlight flight = new TestSingleFlight(1_000, 5, 5, executor, breakerRegistry);
    HotKeyCache named = new HotKeyCache(
      hotKeyDetector,
      caffeineCache,
      flight,
      entryLifecycle,
      backgroundRefresher,
      new CentralDispatcher(accessCounter, null, new BroadcastBuffer(scheduler, null, 500, 2_000, null), null),
      stampClock,
      healthView
    );

    named.get(List.of("k"), key -> "v", GetPolicy.defaults());
    assertThat(caffeineCache.getIfPresent("k")).isNotNull();

    // Evict L1, then ask SingleFlight again: a released ticket must re-run the
    // supplier; a leaked one replays the completed "v" future with no call.
    caffeineCache.invalidate("k");
    AtomicInteger loads = new AtomicInteger();
    flight.loadOutcome("k", () -> {
      loads.incrementAndGet();
      return "v2";
    });
    assertThat(loads.get()).isEqualTo(1);
  }

  /**
   * A namespace clear is version-guarded on the sender too, not an
   * unconditional {@code invalidateAll}: a write allocated after the clear
   * stamp must survive locally exactly as it survives on every peer.
   */
  @Test
  @DisplayName("clear removes entries at or below the clear stamp and keeps newer writes")
  void clear_isVersionGuarded() {
    when(stampClock.nextStamp()).thenReturn(5L);
    HotKeyCache cache = newCache();
    caffeineCache.put("newer", normalEntry("new", 6L));
    caffeineCache.put("older", normalEntry("old", 4L));

    cache.clear(new InvalidatePolicy(InvalidatePolicy.Scope.BOTH, true));

    assertThat(caffeineCache.getIfPresent("newer")).isNotNull();
    assertThat(caffeineCache.getIfPresent("older")).isNull();
  }

  /**
   * A local-only clear uses the same stamp ceiling as the synced form: a write
   * allocated after the clear stamp must survive locally, exactly as it survives
   * on every peer. An unconditional local sweep erased a concurrent write the
   * cluster kept.
   */
  @Test
  @DisplayName("a local-only clear also keeps writes newer than its stamp")
  void localClear_isVersionGuarded() {
    when(stampClock.nextStamp()).thenReturn(5L);
    HotKeyCache cache = newCache();
    caffeineCache.put("newer", normalEntry("new", 6L));
    caffeineCache.put("older", normalEntry("old", 4L));

    cache.clear(new InvalidatePolicy(InvalidatePolicy.Scope.LOCAL, true));

    assertThat(caffeineCache.getIfPresent("newer")).isNotNull();
    assertThat(caffeineCache.getIfPresent("older")).isNull();
  }

  /** A whole-table local clear is JVM-wide: it must drop every dedup slot,
   * including the default namespace's unprefixed keys. */
  @Test
  @DisplayName("a local immediate clear drops every dedup future")
  void clear_localImmediate_dropsAllDedupFutures() {
    SingleFlight flight = mock(SingleFlight.class);
    HotKeyCache cache = newCache(flight);

    cache.clear(new InvalidatePolicy(InvalidatePolicy.Scope.LOCAL, true));

    verify(flight).invalidateAll();
  }

  /** A scoped clear kills only the flights under its prefix: a read that starts
   * after the clear must not join a pre-clear load, while another namespace's
   * in-flight load keeps running. */
  @Test
  @DisplayName("a prefix clear drops only matching dedup futures")
  void clearPrefix_dropsOnlyMatchingDedupFutures() {
    SingleFlight flight = mock(SingleFlight.class);
    HotKeyCache cache = newCache(flight);

    cache.clearPrefix("scope:", new InvalidatePolicy(InvalidatePolicy.Scope.LOCAL, true));

    verify(flight).invalidatePrefix("scope:");
    verify(flight, never()).invalidateAll();
  }
}
