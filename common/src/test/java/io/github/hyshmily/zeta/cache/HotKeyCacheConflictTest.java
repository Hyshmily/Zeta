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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.BroadcastBuffer;
import io.github.hyshmily.zeta.cache.cachesupport.CircuitBreaker;
import io.github.hyshmily.zeta.cache.cachesupport.ExpireManager;
import io.github.hyshmily.zeta.cache.cachesupport.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.impl.ExpireManagerImpl;
import io.github.hyshmily.zeta.cache.cachesupport.impl.SingleFlightImpl;
import io.github.hyshmily.zeta.cache.codec.CacheCompressor;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.CachePolicy;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.model.StalePolicy;
import io.github.hyshmily.zeta.rule.impl.RuleMatcherImpl;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import io.github.hyshmily.zeta.util.version.VersionController;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
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

  private final SnowflakeIdGenerator snowflakeIdGenerator = new SnowflakeIdGenerator(0, 1);

  private HotKeyDetector hotKeyDetector;
  private Cache<String, Object> caffeineCache;
  private CircuitBreaker circuitBreaker;
  private Executor executor;
  private ExpireManager expireManager;
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
    expireManager = new ExpireManagerImpl(caffeineCache, executor, ttlConfig, 10, CacheCompressor.NONE, healthView);
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
        expireManager,
        executor,
        new CentralDispatcher(
            Optional.empty(),
            Optional.empty(),
            new BroadcastBuffer(scheduler, Optional.empty()),
            hotKeyDetector),
        new RuleMatcherImpl(Optional.empty(), Optional.empty()),
        versionController,
        ttlConfig,
        healthView,
        CacheCompressor.NONE);
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
    cache.get("k", CachePolicy.of(() -> "old", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));
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
    cache.get("k", CachePolicy.of(() -> "stale", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

    // L1-only eviction: the dedup result survives (this is the window putLocal must close).
    caffeineCache.invalidate("k");

    // The local write (e.g. @SkipBroadcast @CachePut) obsoletes the recorded load.
    cache.putLocal("k", "fallback", 0L, 0L);

    // The putLocal entry itself is gone (short TTL / eviction) while the dedup TTL lives.
    caffeineCache.invalidate("k");

    Optional<String> result =
        cache.get("k", CachePolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

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
        cache.getWithSoftExpire("k", CachePolicy.of(() -> "v2", 60_000L, 30_000L, true, true, StalePolicy.SOFT_REFRESH));

    assertThat(served).contains("v1");
    // The inline refresh executor applies the fresh value before the call returns.
    assertThat(((CacheEntry) caffeineCache.getIfPresent("k")).getValue()).isEqualTo("v2");
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
