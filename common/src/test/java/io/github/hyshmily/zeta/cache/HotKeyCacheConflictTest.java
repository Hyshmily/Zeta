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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.BroadcastBuffer;
import io.github.hyshmily.zeta.cache.cachesupport.CircuitBreaker;
import io.github.hyshmily.zeta.cache.cachesupport.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.impl.EntryLifecycleImpl;
import io.github.hyshmily.zeta.scheduler.BackgroundRefresher;
import io.github.hyshmily.zeta.scheduler.DefaultBackgroundRefresher;
import io.github.hyshmily.zeta.cache.cachesupport.impl.SingleFlightImpl;
import io.github.hyshmily.zeta.cache.codec.CacheCompressor;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.ReadPolicy;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.model.StalePolicy;
import io.github.hyshmily.zeta.rule.impl.RuleMatcherImpl;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import io.github.hyshmily.zeta.util.version.VersionController;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for call-chain conflicts found while tracing the public API
 * end to end (read / write / invalidate / fluent / annotation paths).
 *
 * <ul>
 *   <li><b>Writer-side equal-version guard:</b> {@code putThrough}'s local apply
 *       must not skip on an EQUAL version — the reader's ADR-0033 probe can stamp
 *       pre-write data with the writer's own post-INCR version, and the sender's
 *       self-REFRESH is dropped (ADR-0067), so an equal-version skip pins the stale
 *       value on the writer instance until TTL.</li>
 *   <li><b>putLocal dedup invariant (ADR-0067):</b> a completed SingleFlight result
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

  private final SnowflakeIdGenerator snowflakeIdGenerator = new SnowflakeIdGenerator(0, 1, 5L, false);

  private HotKeyDetector hotKeyDetector;
  private Cache<String, CacheEntry> caffeineCache;
  private CircuitBreaker circuitBreaker;
  private Executor executor;
  private EntryLifecycle entryLifecycle;
  private BackgroundRefresher backgroundRefresher;
  private java.util.concurrent.ScheduledExecutorService scheduler;
  private HealthView healthView;
  private ZetaProperties ttlConfig;
  private VersionController versionController;

  @BeforeEach
  void setUp() {
    hotKeyDetector = mock(HotKeyDetector.class);
    when(hotKeyDetector.contains(anyString())).thenReturn(false);
    caffeineCache = Caffeine.newBuilder().maximumSize(100).build();
    circuitBreaker = mock(CircuitBreaker.class);
    when(circuitBreaker.allowRequest()).thenReturn(true);
    when(circuitBreaker.isOpen()).thenReturn(false);
    executor = Runnable::run;
    ttlConfig = new ZetaProperties();
    healthView = mock(HealthView.class);
    entryLifecycle = new EntryLifecycleImpl(caffeineCache, ttlConfig, CacheCompressor.NONE, healthView);
    backgroundRefresher = new DefaultBackgroundRefresher(caffeineCache, executor, entryLifecycle.ttlPolicy(), CacheCompressor.NONE, 10);
    scheduler = Executors.newSingleThreadScheduledExecutor();
    versionController = mock(VersionController.class);
    when(versionController.isRedisConfigured()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    scheduler.shutdownNow();
  }

  /** Build a cache with a REAL SingleFlight (dedup exercised) and the given version controller. */
  private HotKeyCache newCache() {
    SingleFlight singleFlight = new SingleFlightImpl(10_000, 5, 5, executor, circuitBreaker);
    return new HotKeyCache(
        hotKeyDetector,
        caffeineCache,
        singleFlight,
        entryLifecycle,
        backgroundRefresher,
        executor,
        new CentralDispatcher(
            Optional.empty(),
            Optional.empty(),
            new BroadcastBuffer(scheduler, Optional.empty(), 500, 2_000, null),
            hotKeyDetector),
        new RuleMatcherImpl(Optional.empty(), Optional.empty()),
        versionController,
        ttlConfig,
        healthView,
        CacheCompressor.NONE,
        null);
  }

  // ── Writer-side equal-version guard (putThrough vs over-stamped read load) ──

  /**
   * The reader's probe-after-read (ADR-0033) can absorb the writer's own INCR:
   * the load stores pre-write data stamped with the version putThrough is about
   * to allocate. The writer's local apply must overwrite an EQUAL version (its
   * value is authoritative at that version) — an equal-version skip pins the
   * stale value on the writer instance, where the self-REFRESH heal is dropped
   * by the origin filter (ADR-0067).
   */
  @Test
  @DisplayName("putThrough applies its own write over an equal-version over-stamped load")
  void putThrough_equalVersionOverStamp_appliesOwnWrite() {
    when(versionController.currentVersion("k")).thenReturn(Optional.of(5L));
    // The writer's INCR produced 5; the racing reader's probe observed the same 5.
    when(versionController.nextVersion("k")).thenReturn(new VersionController.VersionResult(5L, false));
    HotKeyCache cache = newCache();

    // The raced load: pre-write value stamped with the writer's version.
    cache.get("k", ReadPolicy.of(() -> "old", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));
    assertThat(((CacheEntry) caffeineCache.getIfPresent("k")).getValue()).isEqualTo("old");

    cache.putThrough("k", "new", () -> {}, 0L, 0L, true);

    assertThat(((CacheEntry) caffeineCache.getIfPresent("k")).getValue()).isEqualTo("new");
  }

  /** Control: a genuinely newer concurrent write still wins — the guard must skip OLDER writes. */
  @Test
  @DisplayName("putThrough does not overwrite a strictly newer existing entry")
  void putThrough_olderWrite_skipsOnNewerExisting() {
    when(versionController.nextVersion("k")).thenReturn(new VersionController.VersionResult(5L, false));
    HotKeyCache cache = newCache();
    caffeineCache.put("k", normalEntry("fresh", 9L));

    cache.putThrough("k", "old-write", () -> {}, 0L, 0L, true);

    assertThat(((CacheEntry) caffeineCache.getIfPresent("k")).getValue()).isEqualTo("fresh");
  }

  // ── bump-before-drop ordering (ADR-0086) ──

  /**
   * The version must be allocated BEFORE the L1 removal on every invalidation
   * path, not just the single-key one.
   *
   * <p>Bumping after the drop reopens the window ADR-0086 closed: a reader that
   * misses after the drop reloads and stamps the value with the still-old version
   * {@code v}, then the INVALIDATE carrying {@code v+1} is skipped by the plain
   * {@code >=} matrix — pinning the stale value for its whole TTL with no
   * watermark to unpin it.
   *
   * <p>Pinned by observing the entry's presence at the moment the bump runs: an
   * implementation that drops first and bumps later yields the same final version
   * numbers, so end-state assertions cannot see the difference. If the bump ran
   * first the entry is still in L1; if it ran after the drop, the entry is gone.
   */
  @Test
  @DisplayName("invalidateAfterPut allocates the version before removing L1")
  void invalidateAfterPut_singleKey_bumpsBeforeDrop() {
    AtomicBoolean entryPresentAtBump = new AtomicBoolean(false);
    when(versionController.nextVersion("k")).thenAnswer(inv -> {
      entryPresentAtBump.set(caffeineCache.getIfPresent("k") != null);
      return new VersionController.VersionResult(11L, false);
    });
    HotKeyCache cache = newCache();
    caffeineCache.put("k", normalEntry("v", 10L));

    cache.invalidateAfterPut("k", () -> {}, true);

    assertThat(entryPresentAtBump).as("entry must still be in L1 when the version is allocated").isTrue();
    verify(versionController).nextVersion("k");
    assertThat(caffeineCache.getIfPresent("k")).isNull();
  }

  /** Control for the test above: the removal must actually happen. */
  @Test
  @DisplayName("invalidateAfterPut still removes the entry")
  void invalidateAfterPut_singleKey_removesEntry() {
    when(versionController.nextVersion("k")).thenReturn(new VersionController.VersionResult(11L, false));
    HotKeyCache cache = newCache();
    caffeineCache.put("k", normalEntry("v", 10L));

    cache.invalidateAfterPut("k", () -> {}, true);

    assertThat(caffeineCache.getIfPresent("k")).isNull();
  }

  /**
   * The batch path must bump every key before dropping any of them. Interleaving
   * bump-and-drop per key would leave the already-dropped keys on the old version,
   * re-opening the window for exactly those keys — so both keys must still be
   * resident at the moment the first bump runs.
   */
  @Test
  @DisplayName("batch invalidate allocates every version before dropping any entry")
  void invalidate_batch_bumpsAllKeysBeforeDroppingAny() {
    AtomicBoolean bothKeysResidentAtFirstBump = new AtomicBoolean(false);
    AtomicBoolean firstBumpSeen = new AtomicBoolean(false);
    when(versionController.nextVersion(anyString())).thenAnswer(inv -> {
      if (firstBumpSeen.compareAndSet(false, true)) {
        bothKeysResidentAtFirstBump.set(
          caffeineCache.getIfPresent("a") != null && caffeineCache.getIfPresent("b") != null
        );
      }
      return new VersionController.VersionResult(21L, false);
    });
    HotKeyCache cache = newCache();
    caffeineCache.put("a", normalEntry("va", 20L));
    caffeineCache.put("b", normalEntry("vb", 20L));

    cache.invalidate(List.of("a", "b"), true);

    assertThat(bothKeysResidentAtFirstBump).as("both keys must still be resident at the first bump").isTrue();
    verify(versionController).nextVersion("a");
    verify(versionController).nextVersion("b");
    assertThat(caffeineCache.getIfPresent("a")).isNull();
    assertThat(caffeineCache.getIfPresent("b")).isNull();
  }

  /** The map variant must bump after its mutation commits but before its own removal. */
  @Test
  @DisplayName("invalidateAfterPut(map) allocates each key's version before removing it")
  void invalidateAfterPut_map_bumpsBeforeDropPerKey() {
    AtomicBoolean entryPresentAtBump = new AtomicBoolean(false);
    AtomicBoolean mutationRanBeforeBump = new AtomicBoolean(false);
    when(versionController.nextVersion("k")).thenAnswer(inv -> {
      entryPresentAtBump.set(caffeineCache.getIfPresent("k") != null);
      return new VersionController.VersionResult(31L, false);
    });
    HotKeyCache cache = newCache();
    caffeineCache.put("k", normalEntry("v", 30L));

    cache.invalidateAfterPut(Map.of("k", (Runnable) () -> mutationRanBeforeBump.set(true)), true);

    assertThat(mutationRanBeforeBump).as("the mutation must commit before the version is allocated").isTrue();
    assertThat(entryPresentAtBump).as("entry must still be in L1 when the version is allocated").isTrue();
    verify(versionController).nextVersion("k");
    assertThat(caffeineCache.getIfPresent("k")).isNull();
  }

  /**
   * A failed mutation must skip the invalidation entirely — no bump, no removal.
   * This is the pre-existing contract the bump-in-place refactor must preserve.
   */
  @Test
  @DisplayName("invalidateAfterPut skips the bump when the mutation fails")
  void invalidateAfterPut_mutationFailure_skipsBumpAndDrop() {
    HotKeyCache cache = newCache();
    caffeineCache.put("k", normalEntry("v", 5L));

    assertThatThrownBy(() -> cache.invalidateAfterPut("k", () -> {
      throw new IllegalStateException("mutation failed");
    }, true)).isInstanceOf(IllegalStateException.class);

    verify(versionController, never()).nextVersion("k");
    assertThat(caffeineCache.getIfPresent("k")).isNotNull();
  }

  /** With broadcasting disabled there is no bump and no send, but the drop still happens. */
  @Test
  @DisplayName("invalidateAfterPut without peer broadcast still removes the entry")
  void invalidateAfterPut_noBroadcast_skipsBumpButDrops() {
    HotKeyCache cache = newCache();
    caffeineCache.put("k", normalEntry("v", 5L));

    cache.invalidateAfterPut("k", () -> {}, false);

    verify(versionController, never()).nextVersion("k");
    assertThat(caffeineCache.getIfPresent("k")).isNull();
  }

  /**
   * The batch form returns the keys it could not invalidate. It used to be
   * {@code void}, so a partially applied batch was indistinguishable from a
   * fully applied one — the caller's only signal was a log line it never read.
   */
  @Test
  @DisplayName("invalidateAfterPut(map) reports the keys it failed to invalidate")
  void invalidateAfterPut_map_reportsFailedKeys() {
    HotKeyCache cache = newCache();
    caffeineCache.put("ok", normalEntry("v", 5L));
    caffeineCache.put("boom", normalEntry("v", 5L));

    Set<String> failed = cache.invalidateAfterPut(
      Map.of(
        "ok", (Runnable) () -> {},
        "boom", (Runnable) () -> {
          throw new IllegalStateException("mutation failed");
        },
        " ", (Runnable) () -> {}
      ),
      false
    );

    // Order-independent: rejected keys are collected up front, mutation
    // failures while the batch runs.
    assertThat(failed).containsExactlyInAnyOrder("boom", " ");
    assertThat(caffeineCache.getIfPresent("ok")).isNull();
    assertThat(caffeineCache.getIfPresent("boom")).isNotNull();
  }

  /** A fully applied batch reports nothing. */
  @Test
  @DisplayName("invalidateAfterPut(map) returns an empty set when every key applies")
  void invalidateAfterPut_map_allApplied_returnsEmpty() {
    HotKeyCache cache = newCache();
    caffeineCache.put("a", normalEntry("va", 5L));
    caffeineCache.put("b", normalEntry("vb", 5L));

    assertThat(cache.invalidateAfterPut(Map.of("a", (Runnable) () -> {}, "b", (Runnable) () -> {}), false)).isEmpty();
    assertThat(caffeineCache.getIfPresent("a")).isNull();
    assertThat(caffeineCache.getIfPresent("b")).isNull();
  }

  // ── putLocal dedup invariant (ADR-0067) ──


  /**
   * A completed SingleFlight result must not outlive the L1 write that obsoletes
   * it. putLocal carries no version bump, so a pre-write dedup result replayed
   * onto the next miss would re-cache the pre-write value over the local write.
   */
  @Test
  @DisplayName("putLocal invalidates the dedup result recorded before it")
  void putLocal_invalidatesDedupResultRecordedBeforeIt() {
    when(versionController.currentVersion("k")).thenReturn(Optional.of(5L));
    HotKeyCache cache = newCache();

    // Load and cache — the dedup cache now holds "stale"@5 for the SingleFlight TTL.
    cache.get("k", ReadPolicy.of(() -> "stale", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

    // L1-only eviction: the dedup result survives (this is the window putLocal must close).
    caffeineCache.invalidate("k");

    // The local write (e.g. @SkipBroadcast @CachePut) obsoletes the recorded load.
    cache.putLocal("k", "fallback", 0L, 0L);

    // The putLocal entry itself is gone (short TTL / eviction) while the dedup TTL lives.
    caffeineCache.invalidate("k");

    Optional<String> result =
        cache.get("k", ReadPolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

    // The reader must be re-invoked — the pre-write "stale" result was invalidated.
    assertThat(result).contains("fresh");
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
    when(versionController.nextVersion("k")).thenReturn(new VersionController.VersionResult(5L, false));
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
    assertThat(entry.getSoftExpireAtMs()).isEqualTo(hot.getSoftExpireAtMs());
  }

  // ── NORMAL-entry soft refresh (getWithSoftExpire ≡ computeIfAbsent) ──

  /**
   * A soft-expired NORMAL entry with a reader must trigger the background
   * refresh on {@code getWithSoftExpire} exactly like {@code computeIfAbsent}
   * does. {@code registerRefresh} schedules {@code getWithSoftExpire} ticks and
   * documents "every cycle triggers an async refresh" — silently refusing
   * NORMAL entries breaks that contract and the stale-while-revalidate semantics
   * of {@code zeta.local.default-soft-ttl-ms}.
   */
  @Test
  @DisplayName("getWithSoftExpire triggers the background refresh for a soft-expired NORMAL entry")
  void getWithSoftExpire_normalSoftExpiredEntry_triggersBackgroundRefresh() {
    HotKeyCache cache = newCache();
    // A soft-expired NORMAL entry (soft window lapsed, hard TTL still valid).
    caffeineCache.put("k", softExpiredNormalEntry("v1"));

    // The stale value is still served, but the refresh must re-arm and apply.
    Optional<String> served =
        cache.getWithSoftExpire("k", ReadPolicy.of(() -> "v2", 60_000L, 30_000L, true, true, StalePolicy.SOFT_REFRESH));

    assertThat(served).contains("v1");
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

    Optional<String> served =
        cache.get("k", ReadPolicy.of(() -> "v2", 60_000L, 30_000L, true, true, StalePolicy.SOFT_REFRESH));

    assertThat(served).contains("v1");
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

    Optional<String> served =
        cache.get("k", ReadPolicy.of(() -> "v2", 60_000L, 30_000L, true, true, StalePolicy.RETURN));

    assertThat(served).contains("v1");
    assertThat(((CacheEntry) caffeineCache.getIfPresent("k")).getValue()).isEqualTo("v1");
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

  /** Build a plain NORMAL entry stamped with the given data version. */
  private static CacheEntry normalEntry(String value, long dataVersion) {
    return CacheEntry.builder()
        .value(value)
        .dataVersion(dataVersion)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(Long.MAX_VALUE)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build();
  }

  /** Build a soft-expired NORMAL entry: the soft window lapsed, the hard TTL is still valid. */
  private static CacheEntry softExpiredNormalEntry(String value) {
    return CacheEntry.builder()
        .value(value)
        .dataVersion(5)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(TimeSource.currentTimeMillis() + 300_000)
        .softTtlMs(30_000)
        .softExpireAtMs(TimeSource.currentTimeMillis() - 1)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build();
  }
}
