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

import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.rabbitmq.client.Channel;
import io.github.hyshmily.zeta.annotation.annotationsupporter.NullValue;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.BroadcastBuffer;
import io.github.hyshmily.zeta.cache.cachesupport.ExpireManager;
import io.github.hyshmily.zeta.cache.cachesupport.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.impl.ExpireManagerImpl;
import io.github.hyshmily.zeta.cache.codec.CacheCompressor;
import io.github.hyshmily.zeta.cache.loader.CacheLoader;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.CachePolicy;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.model.StalePolicy;
import io.github.hyshmily.zeta.rule.RuleMatcher;
import io.github.hyshmily.zeta.rule.impl.RuleMatcherImpl;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.local.CacheSyncListener;
import io.github.hyshmily.zeta.sync.local.CacheSyncProperties;
import io.github.hyshmily.zeta.sync.local.DefaultSyncDecisionHandler;
import io.github.hyshmily.zeta.sync.local.SyncDecisionHandler;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import io.github.hyshmily.zeta.util.version.VersionController;
import io.github.hyshmily.zeta.util.version.impl.VersionControllerImpl;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

/**
 * Tests for ADR-0033 Read-Path Version Stamping.
 *
 * <p>Covers: miss loads stamped with the probed {@code dataVersion} (so late
 * stale broadcasts are rejected), fail-open probe behavior, stamped
 * {@code NullValue} sentinels, the refresh 4-case apply guard (including the
 * degraded-entry self-heal), batch probe pairing, and the pipelined
 * {@code currentVersions} MGET.
 */
@Timeout(30)
class VersionStampedLoadTest {

  private final SnowflakeIdGenerator snowflakeIdGenerator = new SnowflakeIdGenerator(0, 1);

  private HotKeyDetector hotKeyDetector;
  private Cache<String, Object> cache;
  private SingleFlight singleFlight;
  private ExpireManager expireManager;
  private Executor executor;
  private VersionController versionController;
  private HotKeyCache hotKeyCache;
  private ScheduledExecutorService scheduler;
  private com.rabbitmq.client.Channel channel;

  @BeforeEach
  void setUp() {
    hotKeyDetector = mock(HotKeyDetector.class);
    when(hotKeyDetector.contains(anyString())).thenReturn(false);
    cache = Caffeine.newBuilder().maximumSize(10_000).build();
    singleFlight = mock(SingleFlight.class);
    // Run the composite (value read + version probe) supplier inline.
    when(singleFlight.load(anyString(), any())).thenAnswer(inv -> {
      @SuppressWarnings("unchecked")
      Supplier<Object> supplier = inv.getArgument(1);
      return Optional.ofNullable(supplier.get());
    });
    when(singleFlight.load(anyIterable(), any(), anyBoolean())).thenAnswer(inv -> {
      Iterable<String> keys = inv.getArgument(0);
      @SuppressWarnings("unchecked")
      Function<String, Object> fn = inv.getArgument(1);
      Map<String, Optional<Object>> out = new LinkedHashMap<>();
      for (String key : keys) {
        out.put(key, Optional.ofNullable(fn.apply(key)));
      }
      return out;
    });
    executor = Runnable::run;
    ZetaProperties ttlConfig = new ZetaProperties();
    expireManager = new ExpireManagerImpl(cache, executor, ttlConfig, 10);
    versionController = mock(VersionController.class);
    when(versionController.currentVersion(anyString())).thenReturn(Optional.empty());
    when(versionController.currentVersions(anyIterable())).thenReturn(new LinkedHashMap<>());
    scheduler = Executors.newSingleThreadScheduledExecutor();
    channel = mock(Channel.class);

    hotKeyCache = new HotKeyCache(
      hotKeyDetector,
      cache,
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
      versionController,
      ttlConfig,
      mock(HealthView.class),
      CacheCompressor.NONE
    );
  }

  @AfterEach
  void tearDown() {
    scheduler.shutdownNow();
  }

  private void awaitScheduler() throws InterruptedException {
    CountDownLatch phase1 = new CountDownLatch(1);
    CountDownLatch phase2 = new CountDownLatch(1);
    scheduler.execute(phase1::countDown);
    assertThat(phase1.await(10, TimeUnit.SECONDS)).isTrue();
    scheduler.execute(phase2::countDown);
    assertThat(phase2.await(10, TimeUnit.SECONDS)).isTrue();
  }

  private static Message syncMessage(String key, String type, long version, boolean degraded) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, type);
    props.setHeader(HEADER_VERSION, version);
    props.setHeader(HEADER_IS_VERSION_DEGRADED, degraded);
    return new Message(key.getBytes(StandardCharsets.UTF_8), props);
  }

  private CacheSyncListener createListener(CacheLoader loader) {
    CacheSyncProperties props = new CacheSyncProperties();
    props.setWarmupJitterMs(0);
    SyncDecisionHandler handler = new DefaultSyncDecisionHandler(
      cache,
      loader,
      expireManager,
      mock(RuleMatcher.class),
      List.of()
    );
    CacheSyncListener l = new CacheSyncListener(props, scheduler, handler);
    l.init();
    return l;
  }

  private static CacheEntry entry(
    Object value,
    long dataVersion,
    boolean degraded,
    long softExpireAtMs,
    KeyState keyState
  ) {
    return CacheEntry.builder()
      .value(value)
      .dataVersion(dataVersion)
      .isVersionDegraded(degraded)
      .decisionVersion(0)
      .hardTtlMs(300_000)
      .hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(30_000)
      .softExpireAtMs(softExpireAtMs)
      .keyState(keyState)
      .normalHardTtlMs(300_000)
      .normalSoftTtlMs(30_000)
      .build();
  }

  private static CacheEntry softExpiredEntry(Object value, long dataVersion, boolean degraded, KeyState keyState) {
    return entry(value, dataVersion, degraded, TimeSource.currentTimeMillis() - 1_000, keyState);
  }

  // ═══════════════════════════════════════════════════════════════
  // Miss-load stamping + late-broadcast rejection (gap B regression)
  // ═══════════════════════════════════════════════════════════════

  @Test
  @DisplayName("miss load stamps the probed version; a late stale REFRESH broadcast is rejected")
  void missLoad_stampsProbedVersion_lateStaleBroadcastRejected() throws Exception {
    when(versionController.currentVersion("k")).thenReturn(Optional.of(42L));
    CacheSyncListener listener = createListener(key -> "fresh");

    assertThat(hotKeyCache.get("k", CachePolicy.of(() -> "v", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))).contains(
      "v"
    );

    CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
    assertThat(ce.getDataVersion()).isEqualTo(42L);
    assertThat(ce.isVersionDegraded()).isFalse();

    // Late stale broadcast (version 10 < probed 42) — must be rejected.
    listener.handleSyncMessage(channel, syncMessage("k", SyncMessage.TYPE_REFRESH, 10L, false));
    awaitScheduler();
    ce = (CacheEntry) cache.getIfPresent("k");
    assertThat(ce.getDataVersion()).isEqualTo(42L);
    assertThat(ce.getValue()).isEqualTo("v");

    // A genuinely newer broadcast (version 50 > 42) still applies.
    listener.handleSyncMessage(channel, syncMessage("k", SyncMessage.TYPE_REFRESH, 50L, false));
    awaitScheduler();
    ce = (CacheEntry) cache.getIfPresent("k");
    assertThat(ce.getDataVersion()).isEqualTo(50L);
    assertThat(ce.getValue()).isEqualTo("fresh");
  }

  @Test
  @DisplayName("probe failure withholds the stamp (fail-open): legacy acceptance preserved")
  void probeFailure_failOpen_legacyAcceptancePreserved() throws Exception {
    // currentVersion returns empty → unstamped entry at VERSION_DEFAULT.
    when(versionController.currentVersion("k")).thenReturn(Optional.empty());
    CacheSyncListener listener = createListener(key -> "fresh");

    assertThat(hotKeyCache.get("k", CachePolicy.of(() -> "v", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))).contains(
      "v"
    );

    CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
    assertThat(ce.getDataVersion()).isZero();

    // A broadcast with m > 0 is accepted exactly as before the stamping change.
    listener.handleSyncMessage(channel, syncMessage("k", SyncMessage.TYPE_REFRESH, 5L, false));
    awaitScheduler();
    ce = (CacheEntry) cache.getIfPresent("k");
    assertThat(ce.getDataVersion()).isEqualTo(5L);
  }

  @Test
  @DisplayName("fail-open reload preserves the existing entry's version identity")
  void failOpenReload_preservesExistingVersionIdentity() {
    cache.put("k", entry("old", 7L, false, TimeSource.currentTimeMillis() - 1_000, KeyState.NORMAL));
    when(versionController.currentVersion("k")).thenReturn(Optional.empty());

    // computeIfAbsentWithSoftExpire REVALIDATE keeps the stale entry in L1
    // during the load, so the fail-open reload must preserve its version.
    assertThat(
      hotKeyCache.computeIfAbsentWithSoftExpire(
        "k",
        CachePolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.REVALIDATE)
      )
    ).contains("fresh");

    CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
    assertThat(ce.getDataVersion()).isEqualTo(7L);
    assertThat(ce.getValue()).isEqualTo("fresh");
  }

  // ═══════════════════════════════════════════════════════════════
  // NullValue sentinel stamping
  // ═══════════════════════════════════════════════════════════════

  @Test
  @DisplayName("null sentinel is stamped; stale broadcast cannot overwrite it, newer one can")
  void nullSentinel_stamped_staleBroadcastRejected() throws Exception {
    when(versionController.currentVersion("k")).thenReturn(Optional.of(100L));
    CacheSyncListener listener = createListener(key -> "fresh");

    assertThat(
      hotKeyCache.get("k", CachePolicy.of(() -> null, 0L, 0L, true, true, StalePolicy.SOFT_REFRESH))
    ).isEmpty();

    CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
    assertThat(ce.getValue()).isInstanceOf(NullValue.class);
    assertThat(ce.getDataVersion()).isEqualTo(100L);

    // Stale broadcast (50 < 100): the null marker must survive.
    listener.handleSyncMessage(channel, syncMessage("k", SyncMessage.TYPE_REFRESH, 50L, false));
    awaitScheduler();
    ce = (CacheEntry) cache.getIfPresent("k");
    assertThat(ce.getValue()).isInstanceOf(NullValue.class);
    assertThat(ce.getDataVersion()).isEqualTo(100L);

    // Newer broadcast (150 > 100): replaces the sentinel with real data.
    listener.handleSyncMessage(channel, syncMessage("k", SyncMessage.TYPE_REFRESH, 150L, false));
    awaitScheduler();
    ce = (CacheEntry) cache.getIfPresent("k");
    assertThat(ce.getDataVersion()).isEqualTo(150L);
    assertThat(ce.getValue()).isEqualTo("fresh");
  }

  // ═══════════════════════════════════════════════════════════════
  // Refresh apply guard (4-case, stamped)
  // ═══════════════════════════════════════════════════════════════

  @Test
  @DisplayName("soft refresh stamps the probed version on the refreshed entry")
  void refreshTask_stampsProbedVersion() {
    cache.put("k", softExpiredEntry("old", 5L, false, KeyState.HOT));
    when(versionController.currentVersion("k")).thenReturn(Optional.of(42L));

    hotKeyCache.getWithSoftExpire("k", CachePolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

    CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
    assertThat(ce.getValue()).isEqualTo("fresh");
    assertThat(ce.getDataVersion()).isEqualTo(42L);
    assertThat(ce.isVersionDegraded()).isFalse();
  }

  @Test
  @DisplayName("refresh result is discarded when L1 advanced beyond the probed version")
  void refreshTask_discardedWhenEntryAdvancedBeyondProbe() {
    cache.put("k", softExpiredEntry("old", 60L, false, KeyState.HOT));
    // Redis already at 50: the entry (60) is newer than the probe.
    when(versionController.currentVersion("k")).thenReturn(Optional.of(50L));

    hotKeyCache.getWithSoftExpire("k", CachePolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

    CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
    assertThat(ce.getValue()).isEqualTo("old");
    assertThat(ce.getDataVersion()).isEqualTo(60L);
  }

  @Test
  @DisplayName("degraded entry is healed by a normal probe (4-case, not numeric)")
  void degradedEntry_healedByNormalProbe() {
    cache.put("k", softExpiredEntry("old", Long.MIN_VALUE | snowflakeIdGenerator.nextId(), true, KeyState.HOT));
    when(versionController.currentVersion("k")).thenReturn(Optional.of(42L));

    hotKeyCache.getWithSoftExpire("k", CachePolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

    CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
    assertThat(ce.getValue()).isEqualTo("fresh");
    assertThat(ce.getDataVersion()).isEqualTo(42L);
    assertThat(ce.isVersionDegraded()).isFalse();
  }

  @Test
  @DisplayName("unstamped refresh (probe withheld) keeps the legacy L1-internal guard")
  void refreshTask_unstamped_legacyGuardApplied() {
    cache.put("k", softExpiredEntry("old", 60L, false, KeyState.HOT));
    when(versionController.currentVersion("k")).thenReturn(Optional.empty());

    hotKeyCache.getWithSoftExpire("k", CachePolicy.of(() -> "fresh", 0L, 0L, true, true, StalePolicy.SOFT_REFRESH));

    CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
    assertThat(ce.getValue()).isEqualTo("fresh");
    assertThat(ce.getDataVersion()).isEqualTo(60L);
  }

  // ═══════════════════════════════════════════════════════════════
  // Batch load probe pairing
  // ═══════════════════════════════════════════════════════════════

  @Test
  @DisplayName("batch load pairs versions from currentVersions, fail-open per key")
  void batchLoad_stampsVersions_failOpenPerKey() {
    when(versionController.currentVersions(anyIterable())).thenAnswer(inv -> {
      Iterable<String> keys = inv.getArgument(0);
      Map<String, Optional<Long>> out = new LinkedHashMap<>();
      for (String key : keys) {
        out.put(key, "b".equals(key) ? Optional.empty() : Optional.of("a".equals(key) ? 7L : 9L));
      }
      return out;
    });

    Map<String, Optional<String>> result = hotKeyCache.get(
      List.of("a", "b", "c"),
      key -> "val-" + key,
      0L,
      0L,
      true,
      false
    );

    assertThat(result.get("a")).contains("val-a");
    assertThat(result.get("b")).contains("val-b");
    assertThat(result.get("c")).contains("val-c");

    assertThat(((CacheEntry) cache.getIfPresent("a")).getDataVersion()).isEqualTo(7L);
    assertThat(((CacheEntry) cache.getIfPresent("b")).getDataVersion()).isZero();
    assertThat(((CacheEntry) cache.getIfPresent("c")).getDataVersion()).isEqualTo(9L);
  }

  // ═══════════════════════════════════════════════════════════════
  // VersionControllerImpl.currentVersions — pipelined MGET
  // ═══════════════════════════════════════════════════════════════

  @Test
  @DisplayName("currentVersions reads all keys in one MGET with per-key fail-open")
  void currentVersions_mget_mapsAndFailOpen() {
    StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    ValueOperations<String, String> ops = mock(ValueOperations.class);
    when(redisTemplate.opsForValue()).thenReturn(ops);
    when(ops.multiGet(anyList())).thenReturn(Arrays.asList("10", null, "abc"));

    VersionController vc = new VersionControllerImpl(Optional.of(redisTemplate), 60, snowflakeIdGenerator);
    Map<String, Optional<Long>> versions = vc.currentVersions(List.of("k1", "k2", "k3"));

    assertThat(versions.get("k1")).contains(10L);
    assertThat(versions.get("k2")).isEmpty();
    assertThat(versions.get("k3")).isEmpty();

    verify(ops).multiGet(List.of("zeta:ver:k1", "zeta:ver:k2", "zeta:ver:k3"));
  }

  @Test
  @DisplayName("currentVersions maps every key to empty on a batch Redis failure")
  void currentVersions_batchFailure_allEmpty() {
    StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    ValueOperations<String, String> ops = mock(ValueOperations.class);
    when(redisTemplate.opsForValue()).thenReturn(ops);
    when(ops.multiGet(anyList())).thenThrow(new RuntimeException("boom"));

    VersionController vc = new VersionControllerImpl(Optional.of(redisTemplate), 60, snowflakeIdGenerator);
    Map<String, Optional<Long>> versions = vc.currentVersions(List.of("k1", "k2"));

    assertThat(versions.get("k1")).isEmpty();
    assertThat(versions.get("k2")).isEmpty();
  }
}
