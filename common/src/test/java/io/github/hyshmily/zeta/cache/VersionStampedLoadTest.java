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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.rabbitmq.client.Channel;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycleImpl;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.loader.LoadStatus;
import io.github.hyshmily.zeta.cache.cachesupport.loader.LoaderOutcome;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.StalePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.cache.cachesupport.raw.VersionedValue;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.scheduler.BackgroundRefresher;
import io.github.hyshmily.zeta.scheduler.DefaultBackgroundRefresher;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.local.CacheSyncListener;
import io.github.hyshmily.zeta.sync.local.CacheSyncProperties;
import io.github.hyshmily.zeta.sync.local.DefaultSyncContext;
import io.github.hyshmily.zeta.sync.local.SyncContext;
import io.github.hyshmily.zeta.sync.local.SyncPolicy;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.sync.local.SyncType;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.clock.StampClock;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * Tests for design decisions: version stamps pre-read stamping.
 *
 * <p>Covers: loads stamped before the value read (so late stale broadcasts are
 * rejected), the refresh snapshot guard, the v2 null-answer no-entry contract,
 * and batch stamp allocation.
 */
@Timeout(30)
class VersionStampedLoadTest {

  private final AtomicLong stampSequence = new AtomicLong();

  private HotKeyDetector hotKeyDetector;
  private Cache<String, CacheEntry> cache;
  private SingleFlight singleFlight;
  private EntryLifecycle entryLifecycle;
  private BackgroundRefresher backgroundRefresher;
  private Executor executor;
  private StampClock stampClock;
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
    when(singleFlight.load(anyString(), any(), anyBoolean(), any())).thenAnswer(inv -> {
      @SuppressWarnings("unchecked")
      Supplier<Object> supplier = inv.getArgument(1);
      @SuppressWarnings("unchecked")
      Consumer<Object> commit = inv.getArgument(3);
      Object v = supplier.get();
      if (v != null) {
        commit.accept(v);
        return LoaderOutcome.loaded(v);
      }
      return LoaderOutcome.sourceEmpty();
    });
    when(singleFlight.load(anyIterable(), any(), any())).thenAnswer(inv -> {
      Iterable<String> keys = inv.getArgument(0);
      @SuppressWarnings("unchecked")
      Function<String, Object> fn = inv.getArgument(1);
      @SuppressWarnings("unchecked")
      BiConsumer<String, Object> commit = inv.getArgument(2);
      Map<String, LoaderOutcome<Object>> out = new LinkedHashMap<>();
      for (String key : keys) {
        Object v = fn.apply(key);
        if (v != null) {
          commit.accept(key, v);
        }
        out.put(key, v == null ? LoaderOutcome.sourceEmpty() : LoaderOutcome.loaded(v));
      }
      return out;
    });
    executor = Runnable::run;
    ZetaProperties ttlConfig = new ZetaProperties();
    entryLifecycle = new EntryLifecycleImpl(cache, ttlConfig, null);
    backgroundRefresher = new DefaultBackgroundRefresher(cache, executor, entryLifecycle, 10);
    stampClock = mock(StampClock.class);
    stampSequence.set(0L);
    when(stampClock.nextStamp()).thenAnswer(inv -> stampSequence.incrementAndGet());
    when(stampClock.nextStamps(org.mockito.ArgumentMatchers.anyInt())).thenAnswer(inv -> {
      int n = inv.getArgument(0);
      return stampSequence.addAndGet(n) - n + 1;
    });
    scheduler = Executors.newSingleThreadScheduledExecutor();
    channel = mock(Channel.class);

    hotKeyCache = new HotKeyCache(
      hotKeyDetector,
      cache,
      singleFlight,
      entryLifecycle,
      backgroundRefresher,
      new AccessCounter(hotKeyDetector, null, scheduler, 50L, AccessCounter.DEFAULT_DETECTOR_INTERVAL_MS),
      new BroadcastBuffer(scheduler, null, 500, 2_000, null),
      stampClock,
      mock(HealthView.class)
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

  private static Message syncMessage(String key, String type, long version) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, type);
    props.setHeader(HEADER_VERSION, version);
    return new Message(key.getBytes(StandardCharsets.UTF_8), props);
  }

  private CacheSyncListener createListener(CacheLoader loader) {
    CacheSyncProperties props = new CacheSyncProperties();
    props.setJitterMs(0);
    SyncPolicy handler = new SyncPolicy() {};
    SyncContext context = new DefaultSyncContext(cache, loader, entryLifecycle, List.of(), null, null);
    CacheSyncListener l = new CacheSyncListener(props, scheduler, handler, context, null, null);
    return l;
  }

  private static CacheEntry entry(Object value, long dataVersion, long softExpireAtMs, KeyState keyState) {
    return EntryDraft.blank(null)
      .value(value)
      .version(dataVersion)
      .decision(DecisionStamp.hot(0, null, 0L))
      .ttl(300_000, 30_000, 300_000, 30_000)
      .expiryAt(Long.MAX_VALUE, softExpireAtMs)
      .keyState(keyState)
      .build();
  }

  private static CacheEntry softExpiredEntry(Object value, long dataVersion, KeyState keyState) {
    return entry(value, dataVersion, TimeSource.currentTimeMillis() - 1_000, keyState);
  }

  /** An entry past its hard TTL — still physically resident, logically expired. */
  private static CacheEntry hardExpiredEntry(Object value, long dataVersion) {
    return EntryDraft.blank(null)
      .value(value)
      .version(dataVersion)
      .decision(DecisionStamp.hot(0, null, 0L))
      .ttl(300_000, 30_000, 300_000, 30_000)
      .expiryAt(TimeSource.currentTimeMillis() - 1_000, Long.MAX_VALUE)
      .keyState(KeyState.NORMAL)
      .build();
  }

  // ═══════════════════════════════════════════════════════════════
  // Pre-read stamping + late-broadcast rejection
  // ═══════════════════════════════════════════════════════════════

  @Test
  @DisplayName("miss load stamps the pre-read version; a late stale REFRESH broadcast is rejected")
  void missLoad_stampsPreReadVersion_lateStaleBroadcastRejected() throws Exception {
    when(stampClock.nextStamp()).thenReturn(42L);
    CacheSyncListener listener = createListener(key -> "fresh");

    assertThat(
      (Object) hotKeyCache.get("k", () -> "v", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH))
    ).isEqualTo("v");

    CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
    assertThat(ce.getDataVersion()).isEqualTo(42L);

    // Late stale broadcast (version 10 < the load's 42) — must be rejected.
    listener.onMessage(channel, syncMessage("k", SyncType.REFRESH.code(), 10L));
    awaitScheduler();
    ce = (CacheEntry) cache.getIfPresent("k");
    assertThat(ce.getDataVersion()).isEqualTo(42L);
    assertThat(ce.getValue()).isEqualTo("v");

    // A genuinely newer broadcast (version 50 > 42) still applies.
    listener.onMessage(channel, syncMessage("k", SyncType.REFRESH.code(), 50L));
    awaitScheduler();
    ce = (CacheEntry) cache.getIfPresent("k");
    assertThat(ce.getDataVersion()).isEqualTo(50L);
    assertThat(ce.getValue()).isEqualTo("fresh");
  }

  @Test
  @DisplayName("REVALIDATE reload carries the pre-read stamp instead of the resident identity")
  void revalidateReload_stampsThePreReadVersion() {
    cache.put("k", entry("old", 7L, TimeSource.currentTimeMillis() - 1_000, KeyState.NORMAL));
    when(stampClock.nextStamp()).thenReturn(42L);

    // computeIfAbsentWithSoftExpire REVALIDATE keeps the stale entry in L1
    // during the load; the reload result carries the pre-read stamp.
    assertThat(
      (Object) hotKeyCache.get("k", () -> "fresh", GetPolicy.of(0L, 0L, true, StalePolicy.REVALIDATE))
    ).isEqualTo("fresh");

    CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
    assertThat(ce.getDataVersion()).isEqualTo(42L);
    assertThat(ce.getValue()).isEqualTo("fresh");
  }

  // ═══════════════════════════════════════════════════════════════
  // Null-answer contract (v2): null is never cached
  // ═══════════════════════════════════════════════════════════════

  @Test
  @DisplayName("null loader result writes no entry; a later REFRESH populates it")
  void nullAnswer_noEntry_laterBroadcastApplies() throws Exception {
    CacheSyncListener listener = createListener(key -> "fresh");

    assertThat(
      (Object) hotKeyCache.get("k", () -> null, GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH))
    ).isNull();

    // No sentinel, no entry: the next read re-invokes the loader.
    assertThat(cache.getIfPresent("k")).isNull();

    // A versioned REFRESH broadcast still applies normally.
    listener.onMessage(channel, syncMessage("k", SyncType.REFRESH.code(), 150L));
    awaitScheduler();
    CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
    assertThat(ce).isNotNull();
    assertThat(ce.getDataVersion()).isEqualTo(150L);
    assertThat(ce.getValue()).isEqualTo("fresh");
  }

  // ═══════════════════════════════════════════════════════════════
  // Refresh snapshot guard
  // ═══════════════════════════════════════════════════════════════

  @Test
  @DisplayName("soft refresh stamps the pre-read version on the refreshed entry")
  void refreshTask_stampsPreReadVersion() {
    cache.put("k", softExpiredEntry("old", 5L, KeyState.HOT));
    when(stampClock.nextStamp()).thenReturn(42L);

    hotKeyCache.get("k", () -> "fresh", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH));

    CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
    assertThat(ce.getValue()).isEqualTo("fresh");
    assertThat(ce.getDataVersion()).isEqualTo(42L);
  }

  @Test
  @DisplayName("refresh result is discarded when L1 advanced beyond the trigger-time snapshot")
  void refreshTask_discardedWhenEntryAdvancedBeyondSnapshot() {
    cache.put("k", softExpiredEntry("old", 60L, KeyState.HOT));
    // The reader simulates a write landing while the refresh is in flight,
    // advancing L1 beyond the snapshot taken at refresh-creation time.
    when(stampClock.nextStamp()).thenReturn(50L);

    hotKeyCache.get(
      "k",
      () -> {
        cache.put("k", softExpiredEntry("newer", 70L, KeyState.HOT));
        return "fresh";
      },
      GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH)
    );

    CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
    assertThat(ce.getValue()).isEqualTo("newer");
    assertThat(ce.getDataVersion()).isEqualTo(70L);
  }

  @Test
  @DisplayName("refresh applies with max(entry, pre-read stamp) when no write landed in flight")
  void refreshTask_appliesWithMaxStampWhenNoConcurrentWrite() {
    cache.put("k", softExpiredEntry("old", 60L, KeyState.HOT));
    // Older incoming stamp: the fresh value and extended TTLs still apply, but
    // the entry's own stamp wins so the L1 watermark never regresses.
    when(stampClock.nextStamp()).thenReturn(50L);

    hotKeyCache.get("k", () -> "fresh", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH));

    CacheEntry ce = (CacheEntry) cache.getIfPresent("k");
    assertThat(ce.getValue()).isEqualTo("fresh");
    assertThat(ce.getDataVersion()).isEqualTo(60L);
  }

  // ═══════════════════════════════════════════════════════════════
  // Batch load: dedup-ticket lifecycle + probe scope
  // ═══════════════════════════════════════════════════════════════

  /**
   * Every key that reaches L1 carries a real stamp, and each key gets its own
   * value (design decisions: version stamps I4). A key whose source answered {@code null} writes no
   * entry and therefore exposes no stamp.
   */
  @Test
  @DisplayName("batch load stamps only the keys that were actually stored")
  void batchLoad_stampsOnlyStoredKeys() {
    when(singleFlight.load(anyIterable(), any(), any())).thenAnswer(inv -> {
      Iterable<String> keys = inv.getArgument(0);
      @SuppressWarnings("unchecked")
      Function<String, Object> reader = inv.getArgument(1);
      @SuppressWarnings("unchecked")
      BiConsumer<String, Object> commit = inv.getArgument(2);
      Map<String, LoaderOutcome<Object>> out = new LinkedHashMap<>();
      for (String key : keys) {
        switch (key) {
          case "empty" -> out.put(key, LoaderOutcome.sourceEmpty());
          case "failed" -> out.put(key, LoaderOutcome.failed(new IllegalStateException("boom")));
          case "suppressed" -> out.put(key, LoaderOutcome.suppressed(LoadStatus.SUPPRESSED_BREAKER));
          default -> {
            Object value = new VersionedValue("v-" + key, 1L);
            commit.accept(key, value);
            out.put(key, LoaderOutcome.loaded(value));
          }
        }
      }
      return out;
    });
    long[] sequence = { 40L };
    when(stampClock.nextStamp()).thenAnswer(inv -> sequence[0] += 10L);

    hotKeyCache.get(
      List.of("stored1", "stored2", "empty"),
      key -> "empty".equals(key) ? null : "v-" + key,
      GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH)
    );

    assertThat(((CacheEntry) cache.getIfPresent("stored1")).getDataVersion()).isPositive();
    assertThat(((CacheEntry) cache.getIfPresent("stored2")).getDataVersion()).isPositive();
    assertThat(cache.getIfPresent("empty")).isNull();

    // A failed or suppressed load writes no entry and now propagates instead of
    // masquerading as a missing key.
    assertThatThrownBy(() ->
      hotKeyCache.get(List.of("failed"), key -> "v-failed", GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH))
    )
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("boom");
    assertThatThrownBy(() ->
      hotKeyCache.get(
        List.of("suppressed"),
        key -> "v-suppressed",
        GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH)
      )
    ).isInstanceOf(io.github.hyshmily.zeta.exception.ZetaSuppressedException.class);
    assertThat(cache.getIfPresent("failed")).isNull();
    assertThat(cache.getIfPresent("suppressed")).isNull();
  }

  // ═══════════════════════════════════════════════════════════════
  // Batch load stamp allocation
  // ═══════════════════════════════════════════════════════════════

  @Test
  @DisplayName("batch load assigns a distinct pre-read stamp per key")
  void batchLoad_assignsDistinctStampsPerKey() {
    long[] sequence = { 6L };
    when(stampClock.nextStamp()).thenAnswer(inv -> sequence[0] += 3L);

    Map<String, String> result = hotKeyCache.get(
      List.of("a", "b", "c"),
      key -> "val-" + key,
      GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH)
    );

    assertThat(result.get("a")).isEqualTo("val-a");
    assertThat(result.get("b")).isEqualTo("val-b");
    assertThat(result.get("c")).isEqualTo("val-c");

    long va = ((CacheEntry) cache.getIfPresent("a")).getDataVersion();
    long vb = ((CacheEntry) cache.getIfPresent("b")).getDataVersion();
    long vc = ((CacheEntry) cache.getIfPresent("c")).getDataVersion();
    assertThat(va).isPositive();
    assertThat(vb).isGreaterThan(va);
    assertThat(vc).isGreaterThan(vb);
  }
}
