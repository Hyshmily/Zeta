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
package io.github.hyshmily.zeta;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.policy.*;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.exception.ZetaModeException;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.model.ZetaCacheStats;
import io.github.hyshmily.zeta.scheduler.TimedRefreshCoordinator;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ZetaTest {

  private HotKeyCache hotKeyCache;
  private TopK appDetector;
  private Zeta zeta;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    hotKeyCache = mock(HotKeyCache.class);
    appDetector = mock(TopK.class);
    // Mirror the real TtlPolicy behaviour (softTtlMs > 0 is used as-is). Without this stub the
    // mock default 0 would collapse every refresh interval to 1ms, turning destroy_shouldNotThrow
    // into a race between the scheduled refresh and the cancellation.
    when(hotKeyCache.resolveEffectiveSoftTtl(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));
    // Mirror the real TTL resolution so a lazily supplied Long.MAX_VALUE, and a
    // configuration that resolves to permanent, both reach requireSyncable.
    when(hotKeyCache.resolveSyncableHardTtl(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));
    zeta = new DefaultZeta(hotKeyCache, appDetector);
  }

  @Test
  void isLocalZeta_shouldDelegateToCache() {
    when(hotKeyCache.isHot("key1")).thenReturn(true);
    assertThat(zeta.detector().isLocalHotKey("key1")).isTrue();
    verify(hotKeyCache).isHot("key1");
  }

  @Test
  void peek_shouldDelegateToCache() {
    when(hotKeyCache.peek("key1")).thenReturn("value");
    String value = zeta.peek("key1");
    assertThat(value).isEqualTo("value");
    verify(hotKeyCache).peek("key1");
  }

  @Test
  void peek_shouldReturnNullOnMiss() {
    when(hotKeyCache.peek("key1")).thenReturn(null);
    String value = zeta.peek("key1");
    assertThat(value).isNull();
  }

  @Test
  void get_withReader_shouldDelegateToCache() {
    when(hotKeyCache.get(anyString(), any(Supplier.class), any(GetPolicy.class))).thenReturn("value");
    assertThat(zeta.get("key1", () -> "loaded", GetPolicy.defaults())).isEqualTo("value");
    verify(hotKeyCache).get(
      eq("key1"),
      argThat((Supplier<String> r) -> r != null && "loaded".equals(r.get())),
      eq(GetPolicy.defaults())
    );
  }

  @Test
  void get_withTtl_shouldDelegateToCache() {
    when(hotKeyCache.get(anyString(), any(Supplier.class), any(GetPolicy.class))).thenReturn("v");
    assertThat(zeta.get("key1", () -> "loaded", GetPolicy.of(1000L, 100L, true, StalePolicy.SOFT_REFRESH))).isEqualTo(
      "v"
    );
    verify(hotKeyCache).get(anyString(), any(Supplier.class), any(GetPolicy.class));
  }

  @Test
  void get_nullOptions_shouldDefault() {
    when(hotKeyCache.get(anyString(), any(Supplier.class), any(GetPolicy.class))).thenReturn("v");
    assertThat(zeta.get("key1", () -> "loaded", null)).isEqualTo("v");
    verify(hotKeyCache).get(
      eq("key1"),
      any(Supplier.class),
      argThat(o -> o.report() && o.stalePolicy() == StalePolicy.SOFT_REFRESH)
    );
  }

  @Test
  void get_withStalePolicy_shouldCarryIt() {
    when(hotKeyCache.get(anyString(), any(Supplier.class), any(GetPolicy.class))).thenReturn("v");
    zeta.get("key1", () -> "loaded", GetPolicy.of(0L, 0L, true, StalePolicy.RETURN));
    verify(hotKeyCache).get(eq("key1"), any(Supplier.class), argThat(o -> o.stalePolicy() == StalePolicy.RETURN));
  }

  @Test
  void get_withLoader_shouldPassLoaderThrough() {
    when(hotKeyCache.get(anyString(), any(CacheLoader.class), any(GetPolicy.class))).thenReturn("v");
    CacheLoader<String, String> loader = mock(CacheLoader.class);
    when(loader.load("key1")).thenReturn("loaded");

    assertThat(zeta.get("key1", loader, GetPolicy.defaults())).isEqualTo("v");
    // The loader crosses the facade untouched — the engine derives the
    // admission filter from its isCacheable.
    verify(hotKeyCache).get(eq("key1"), eq(loader), eq(GetPolicy.defaults()));
  }

  @Test
  void get_withLambdaLoader_shouldPassThrough() {
    when(hotKeyCache.get(anyString(), any(CacheLoader.class), any(GetPolicy.class))).thenReturn("v");
    zeta.get("key1", k -> "loaded", GetPolicy.defaults());
    // Plain lambda loader: isCacheable defaults to true.
    verify(hotKeyCache).get(eq("key1"), any(CacheLoader.class), eq(GetPolicy.defaults()));
  }

  // ── Batch get pass-through ──

  @Test
  @SuppressWarnings("unchecked")
  void getBatch_shouldPassThroughEngineResult() {
    when(hotKeyCache.get(anyList(), any(Function.class), any(GetPolicy.class))).thenAnswer(inv -> {
      List<String> keys = (List<String>) inv.getArgument(0);
      Function<String, String> reader = (Function<String, String>) inv.getArgument(1);
      Map<String, String> out = new LinkedHashMap<>();
      for (String k : keys) {
        String v = reader.apply(k);
        if (v != null) {
          out.put(k, v);
        }
      }
      return out;
    });

    Map<String, String> result = zeta.getAll(List.of("a", "b"), k -> "a".equals(k) ? null : "v:" + k, null);

    // Missing key = miss: the map holds only the keys that produced a value.
    assertThat(result).containsExactly(Map.entry("b", "v:b"));
  }

  @Test
  void invalidate_shouldDelegateToCache() {
    zeta.invalidate("key1", null);
    verify(hotKeyCache).invalidate("key1", InvalidatePolicy.defaults());
  }

  @Test
  void invalidate_varargs_shouldDelegateToCache() {
    zeta.invalidate(List.of("k1", "k2"), InvalidatePolicy.defaults());
    verify(hotKeyCache).invalidate(List.of("k1", "k2"), InvalidatePolicy.defaults());
  }

  @Test
  void invalidate_varargs_shouldDropInvalidKeysBeforeDelegating() {
    // The collection gate is the facade's: the engine never sees a null/blank
    // element, so its batch paths can assume valid keys.
    zeta.invalidate(Arrays.asList("k1", null, " ", "k2"), InvalidatePolicy.defaults());
    verify(hotKeyCache).invalidate(List.of("k1", "k2"), InvalidatePolicy.defaults());
  }

  @Test
  void invalidate_varargs_allInvalid_shouldNotReachTheEngine() {
    zeta.invalidate(Arrays.asList(null, ""), InvalidatePolicy.defaults());
    verify(hotKeyCache, never()).invalidate(anyList(), any(InvalidatePolicy.class));
  }

  @Test
  void invalidate_varargs_remoteScope_shouldDropInvalidKeysBeforeBroadcast() {
    zeta.invalidate(Arrays.asList("k1", null), new InvalidatePolicy(InvalidatePolicy.Scope.REMOTE, false));
    verify(hotKeyCache).invalidate(List.of("k1"), new InvalidatePolicy(InvalidatePolicy.Scope.REMOTE, false));
  }

  @Test
  @SuppressWarnings("unchecked")
  void getAll_shouldDropInvalidKeysBeforeDelegating() {
    when(hotKeyCache.get(anyList(), any(Function.class), any(GetPolicy.class))).thenReturn(Map.of());

    zeta.getAll(Arrays.asList("k1", null, " "), k -> "v", GetPolicy.defaults());

    verify(hotKeyCache).get(eq(List.of("k1")), any(Function.class), any(GetPolicy.class));
  }

  @Test
  void invalidate_localOnly_shouldSuppressSync() {
    zeta.invalidate("key1", InvalidatePolicy.localOnly());
    verify(hotKeyCache).invalidate("key1", InvalidatePolicy.localOnly());
  }

  // ── put ──

  @Test
  void put_shouldDefaultToLocalWrite() {
    zeta.put("key1", "value", null);
    verify(hotKeyCache).putLocal("key1", "value", 0L, 0L);
    verify(hotKeyCache, never()).putThrough(anyString(), any(), anyLong(), anyLong());
  }

  @Test
  void put_sync_shouldDelegateToVersionedWrite() {
    zeta.put("key1", "value", PutPolicy.synced());
    verify(hotKeyCache).putThrough("key1", "value", 0L, 0L);
    verify(hotKeyCache, never()).putLocal(anyString(), any(), anyLong(), anyLong());
  }

  @Test
  void put_withTtl_shouldDelegateWithTtl() {
    zeta.put("key1", "value", PutPolicy.of(2000L, 200L, false));
    verify(hotKeyCache).putLocal("key1", "value", 2000L, 200L);
  }

  @Test
  void put_shouldRejectNullValue() {
    assertThatThrownBy(() -> zeta.put("k", null, PutPolicy.defaults())).isInstanceOf(NullPointerException.class);
  }

  @Test
  void put_shouldRejectNegativeHardTtl() {
    assertThatThrownBy(() -> zeta.put("k", "v", PutPolicy.of(-1L, 0L, false))).isInstanceOf(
      IllegalArgumentException.class
    );
  }

  @Test
  void put_syncWithLazilySuppliedPermanentTtl_shouldBeRejectedAtResolution() {
    // The canonical record constructor takes suppliers, so this bypasses the
    // static factory's check; the resolved value must be rejected before the
    // write reaches the cache.
    PutPolicy options = new PutPolicy(() -> Long.MAX_VALUE, null, true);

    assertThatThrownBy(() -> zeta.put("k", "v", options))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("permanent entry cannot be synced");
  }

  @Test
  void put_syncWithConfiguredPermanentTtl_shouldBeRejectedBeforeTheWrite() {
    when(hotKeyCache.resolveSyncableHardTtl(anyLong())).thenReturn(Long.MAX_VALUE);

    assertThatThrownBy(() -> zeta.put("k", "v", PutPolicy.synced()))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("permanent entry cannot be synced");
    verify(hotKeyCache, never()).putThrough(anyString(), any(), anyLong(), anyLong());
  }

  @Test
  void refresh_syncWithConfiguredPermanentTtl_shouldBeRejectedBeforeTheLoad() {
    when(hotKeyCache.resolveSyncableHardTtl(anyLong())).thenReturn(Long.MAX_VALUE);

    assertThatThrownBy(() -> zeta.refresh("k", () -> "v", PutPolicy.synced()))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("permanent entry cannot be synced");
    verify(hotKeyCache, never()).refresh(anyString(), any(Supplier.class), anyLong(), anyLong(), anyBoolean());
  }

  @Test
  void refreshAll_shouldRejectAConfiguredPermanentDefault() {
    when(hotKeyCache.resolveSyncableHardTtl(anyLong())).thenReturn(Long.MAX_VALUE);

    assertThatThrownBy(() -> zeta.refreshAll(Map.of("k", (Supplier<?>) () -> "v")))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("permanent entry cannot be synced");
  }

  @Test
  void put_shouldThrowInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null);
    assertThatThrownBy(() -> workerOnly.put("k", "v", PutPolicy.defaults())).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void putAll_empty_shouldStillThrowInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null);
    assertThatThrownBy(() -> workerOnly.putAll(Map.of(), PutPolicy.defaults())).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void refreshAll_empty_shouldStillThrowInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null);
    assertThatThrownBy(() -> workerOnly.refreshAll(Map.of())).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void putAll_shouldDefaultToLocalBatchWrite() {
    Map<String, Object> entries = Map.of("k1", "v1", "k2", "v2");

    zeta.putAll(entries);

    verify(hotKeyCache).putAll(entries, 0L, 0L, false);
    verify(hotKeyCache, never()).putAll(entries, 0L, 0L, true);
  }

  @Test
  void putAll_sync_shouldDelegateToVersionedBatchWrite() {
    Map<String, Object> entries = Map.of("k1", "v1");

    zeta.putAll(entries, PutPolicy.synced());

    verify(hotKeyCache).putAll(entries, 0L, 0L, true);
  }

  @Test
  void putAll_shouldRejectTheWholeBatchBeforeWriting() {
    Map<String, Object> entries = new LinkedHashMap<>();
    entries.put("k1", "v1");
    entries.put(" ", "v2");

    assertThatThrownBy(() -> zeta.putAll(entries, PutPolicy.defaults())).isInstanceOf(IllegalArgumentException.class);
    verify(hotKeyCache, never()).putAll(anyMap(), anyLong(), anyLong(), anyBoolean());
  }

  @Test
  void putAll_shouldRejectANullValueBeforeWriting() {
    Map<String, Object> entries = new LinkedHashMap<>();
    entries.put("k1", "v1");
    entries.put("k2", null);

    assertThatThrownBy(() -> zeta.putAll(entries, PutPolicy.defaults())).isInstanceOf(NullPointerException.class);
    verify(hotKeyCache, never()).putAll(anyMap(), anyLong(), anyLong(), anyBoolean());
  }

  @Test
  void putAll_syncWithConfiguredPermanentTtl_shouldBeRejectedBeforeTheBatch() {
    when(hotKeyCache.resolveSyncableHardTtl(anyLong())).thenReturn(Long.MAX_VALUE);

    assertThatThrownBy(() -> zeta.putAll(Map.of("k", "v"), PutPolicy.synced()))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("permanent entry cannot be synced");
    verify(hotKeyCache, never()).putAll(anyMap(), anyLong(), anyLong(), anyBoolean());
  }

  @Test
  void putAll_empty_shouldBeANoOp() {
    zeta.putAll(Map.of(), PutPolicy.synced());

    verify(hotKeyCache, never()).putAll(anyMap(), anyLong(), anyLong(), anyBoolean());
  }

  @Test
  void unsafe_raw_shouldDelegateToEngineTable() {
    @SuppressWarnings("unchecked")
    Cache<String, CacheEntry> table = mock(Cache.class);
    when(hotKeyCache.table()).thenReturn(table);
    assertThat(zeta.unsafe().raw()).isSameAs(table);
    verify(hotKeyCache).table();
  }

  @Test
  void unsafe_raw_shouldThrowInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null);
    assertThatThrownBy(() -> workerOnly.unsafe().raw()).isInstanceOf(ZetaModeException.class);
  }

  // ── detector admin ──

  // ── detector feeds (tag family) ──

  @Test
  void tag_shouldDelegateToCache() {
    zeta.tag("my-key");
    verify(hotKeyCache).recordAccess("my-key");
  }

  @Test
  void tag_withMode_shouldDelegateToCache() {
    zeta.tag("my-key", Zeta.TagMode.DETECT_ONLY);
    verify(hotKeyCache).recordAccess("my-key", Zeta.TagMode.DETECT_ONLY);
  }

  @Test
  void tag_nullKey_shouldReject() {
    assertThatThrownBy(() -> zeta.tag(null)).isInstanceOf(IllegalArgumentException.class);
    verify(hotKeyCache, never()).recordAccess(anyString());
  }

  @Test
  void tag_nonPositiveCount_shouldReject() {
    assertThatThrownBy(() -> zeta.tag("k", 0)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> zeta.tag("k", -1)).isInstanceOf(IllegalArgumentException.class);
    verify(hotKeyCache, never()).recordAccess(anyString(), anyLong());
  }

  @Test
  void returnLocalTopNHotKeys_shouldDelegateToAppDetector() {
    List<Item> items = List.of(new Item("k", 10));
    when(appDetector.listTopN(3)).thenReturn(items);
    assertThat(zeta.detector().localTopKeys(3)).containsExactly(new Item("k", 10));
    verify(appDetector).listTopN(3);
  }

  @Test
  void returnLocalTopNHotKeys_shouldReturnEmptyWhenDetectorNull() {
    Zeta hk = new DefaultZeta(hotKeyCache, null);
    assertThat(hk.detector().localTopKeys(5)).isEmpty();
  }

  // ── clearAll (LOCAL + immediate) ──

  @Test
  void clearAll_localImmediate_shouldDelegateToALocalImmediateClear() {
    InvalidatePolicy policy = new InvalidatePolicy(InvalidatePolicy.Scope.LOCAL, true);
    zeta.clearAll(policy);
    verify(hotKeyCache).clear(policy);
  }

  @Test
  void forgetDedup_shouldDelegateToCache() {
    zeta.unsafe().forgetDedup("k");
    verify(hotKeyCache).forgetDedup("k");
  }

  @Test
  void forgetDedup_shouldRejectBlankKey() {
    // Same fail-fast contract as the single-key data verbs; the engine's dedup
    // loop assumes valid keys.
    assertThatThrownBy(() -> zeta.unsafe().forgetDedup("k1", " ")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void cacheMethods_shouldThrowInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null);
    assertThatThrownBy(() -> workerOnly.get("k", () -> "v", GetPolicy.defaults())).isInstanceOf(
      ZetaModeException.class
    );
    assertThatThrownBy(() -> workerOnly.detector().isLocalHotKey("k")).isInstanceOf(ZetaModeException.class);
    assertThatThrownBy(() -> workerOnly.peek("k")).isInstanceOf(ZetaModeException.class);
    assertThatThrownBy(() -> workerOnly.invalidate("k", null)).isInstanceOf(ZetaModeException.class);
    assertThatThrownBy(() -> workerOnly.put("k", "v", null)).isInstanceOf(ZetaModeException.class);
    assertThatThrownBy(() -> workerOnly.clearAll(InvalidatePolicy.defaults())).isInstanceOf(ZetaModeException.class);
    assertThatThrownBy(() -> workerOnly.unsafe().forgetDedup("k")).isInstanceOf(ZetaModeException.class);
  }

  // ── stats ──

  @Test
  void stats_shouldDelegateToCache() {
    ZetaCacheStats stats = mock(ZetaCacheStats.class);
    when(hotKeyCache.stats()).thenReturn(stats);
    assertThat(zeta.stats()).isSameAs(stats);
    verify(hotKeyCache).stats();
  }

  @Test
  void stats_shouldThrowInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null);
    assertThatThrownBy(workerOnly::stats).isInstanceOf(ZetaModeException.class);
  }

  // ── refresh ──

  @Test
  void refresh_shouldDelegateToGuardedRefreshWithSyncOn() {
    zeta.refresh("k1", () -> "v");
    verify(hotKeyCache).refresh(eq("k1"), any(), eq(0L), eq(0L), eq(true));
  }

  @Test
  void refresh_localOnly_shouldDelegateWithoutBroadcast() {
    zeta.refresh("k1", () -> "v", PutPolicy.defaults());
    verify(hotKeyCache).refresh(eq("k1"), any(), eq(0L), eq(0L), eq(false));
  }

  @Test
  void refresh_withTtl_shouldDelegateWithTtl() {
    zeta.refresh("k1", () -> "v", PutPolicy.of(5000L, 500L, false));
    verify(hotKeyCache).refresh(eq("k1"), any(), eq(5000L), eq(500L), eq(false));
  }

  @Test
  void refresh_shouldThrowInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null);
    assertThatThrownBy(() -> workerOnly.refresh("k", () -> "v")).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void refresh_partialWiring_cacheStillServes() {
    // The cache engine owns its detector by construction, so a facade wired
    // with a cache but no detector reference still serves the whole data plane.
    Zeta partial = new DefaultZeta(hotKeyCache, null);
    assertThat(partial.isCacheAvailable()).isTrue();
    when(hotKeyCache.peek("k")).thenReturn("v");
    String peeked = partial.peek("k");
    assertThat(peeked).isEqualTo("v");
    verify(hotKeyCache).peek("k");
  }

  @Test
  void refreshAll_shouldRefreshEachKey() {
    zeta.refreshAll(Map.of("k1", (Supplier<?>) () -> "v1", "k2", (Supplier<?>) () -> "v2"));
    verify(hotKeyCache, times(2)).refresh(anyString(), any(), anyLong(), anyLong(), anyBoolean());
  }

  // ── registerRefresh / unregisterRefresh ────────────────────────

  @Test
  void unregisterRefresh_shouldNotThrow() throws InterruptedException {
    CountDownLatch firstCallLatch = new CountDownLatch(1);
    when(hotKeyCache.get(eq("cancel-key"), any(Supplier.class), any(GetPolicy.class))).thenAnswer(invocation -> {
      firstCallLatch.countDown();
      return "v";
    });

    zeta
      .refresh()
      .registerRefresh("cancel-key", () -> "v", GetPolicy.of(300_000L, 10L, true, StalePolicy.SOFT_REFRESH));
    assertThat(firstCallLatch.await(5, TimeUnit.SECONDS)).as("first scheduled refresh occurred").isTrue();

    zeta.refresh().unregisterRefresh("cancel-key");

    verify(hotKeyCache, atLeastOnce()).get(eq("cancel-key"), any(Supplier.class), any(GetPolicy.class));
  }

  @Test
  void registerRefresh_replacesExistingRegistration() throws InterruptedException {
    AtomicInteger callCount = new AtomicInteger(0);
    when(hotKeyCache.get(anyString(), any(Supplier.class), any(GetPolicy.class))).thenAnswer(invocation -> {
      callCount.incrementAndGet();
      return "v";
    });

    zeta.refresh().registerRefresh("dup-key", () -> "v1", GetPolicy.of(300_000L, 5L, true, StalePolicy.SOFT_REFRESH));
    Thread.sleep(20);
    zeta.refresh().registerRefresh("dup-key", () -> "v2", GetPolicy.of(300_000L, 5L, true, StalePolicy.SOFT_REFRESH));

    int countBefore = callCount.get();
    Thread.sleep(30);
    int countAfter = callCount.get();

    assertThat(countAfter).isGreaterThan(countBefore);
  }

  @Test
  void registerRefresh_invokesFacadeGet() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    when(hotKeyCache.get(eq("k1"), any(Supplier.class), any(GetPolicy.class))).thenAnswer(invocation -> {
      latch.countDown();
      return "v";
    });

    zeta.refresh().registerRefresh("k1", () -> "v", GetPolicy.of(300_000L, 5L, true, StalePolicy.SOFT_REFRESH));
    assertThat(latch.await(5, TimeUnit.SECONDS)).as("facade get was invoked by scheduled refresh").isTrue();

    zeta.refresh().unregisterRefresh("k1");
  }

  /**
   * The replace-and-cancel race: two concurrent registrations for the same key
   * must leave exactly one ALIVE future registered. A put-then-cancel sequence
   * lets T1 and T2 cancel each other's new future (T1's prev read is F2, T2's
   * is F1), silently killing the timed refresh.
   */
  @Test
  void registerRefresh_concurrentRegistrations_leaveExactlyOneAliveFuture() throws Exception {
    int threads = 8;
    int iterations = 25;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<?>> jobs = new java.util.ArrayList<>();
    for (int t = 0; t < threads; t++) {
      jobs.add(
        pool.submit(() -> {
          start.await();
          for (int i = 0; i < iterations; i++) {
            zeta
              .refresh()
              .registerRefresh("race-key", () -> "v", GetPolicy.of(300_000L, 5_000L, true, StalePolicy.SOFT_REFRESH));
          }
          return null;
        })
      );
    }
    start.countDown();
    for (Future<?> job : jobs) {
      job.get(30, TimeUnit.SECONDS);
    }
    pool.shutdown();

    // The per-key future map moved into the TimedRefreshCoordinator when the
    // scheduling infrastructure was extracted from the facade; reach it through
    // the facade's coordinator field.
    java.lang.reflect.Field coordinatorField = DefaultZeta.class.getDeclaredField("refreshCoordinator");
    coordinatorField.setAccessible(true);
    Object coordinator = coordinatorField.get(zeta);
    java.lang.reflect.Field field = coordinator.getClass().getDeclaredField("refreshFutures");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.ScheduledFuture<?>> futures =
      (java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.ScheduledFuture<?>>) field.get(coordinator);

    Object surviving = futures.get("race-key");
    assertThat(surviving).as("a registration must survive the race").isNotNull();
    assertThat(futures.get("race-key").isCancelled()).as("the surviving future must not be cancelled").isFalse();

    zeta.refresh().unregisterRefresh("race-key");
  }

  @Test
  void destroy_shouldNotThrow() {
    zeta.refresh().registerRefresh("k1", () -> "v", GetPolicy.of(300_000L, 10_000L, true, StalePolicy.SOFT_REFRESH));
    ((DefaultZeta) zeta).destroy();
    verify(hotKeyCache, never()).get(eq("k1"), any(Supplier.class), any(GetPolicy.class));
  }

  // ── Parameter validation ──

  @Test
  void get_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.get(null, () -> "v", GetPolicy.defaults())).isInstanceOf(
      IllegalArgumentException.class
    );
  }

  @Test
  void getAll_shouldRejectNullKeys() {
    assertThatThrownBy(() -> zeta.getAll((Collection<String>) null, k -> "v", GetPolicy.defaults())).isInstanceOf(
      NullPointerException.class
    );
  }

  @Test
  void put_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.put(null, "v", PutPolicy.defaults())).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void invalidate_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.invalidate((String) null, null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void invalidate_shouldRejectEmptyKey() {
    assertThatThrownBy(() -> zeta.invalidate("", null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void batchInvalidate_shouldRejectNullKeys() {
    assertThatThrownBy(() -> zeta.invalidate((Collection<String>) null, null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void peek_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.peek(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void isLocalHotKey_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.detector().isLocalHotKey(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void registerRefresh_shouldRejectNullKey() {
    assertThatThrownBy(() ->
      zeta.refresh().registerRefresh(null, () -> "v", GetPolicy.of(1000L, 100L, true, StalePolicy.SOFT_REFRESH))
    ).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void registerRefresh_shouldRejectNullSupplier() {
    assertThatThrownBy(() ->
      zeta.refresh().registerRefresh("k", null, GetPolicy.of(1000L, 100L, true, StalePolicy.SOFT_REFRESH))
    ).isInstanceOf(NullPointerException.class);
  }

  @Test
  void registerRefresh_intervalIsSoftTtlTimesOnePointOne() {
    // The documented cadence contract: interval = resolved soft TTL × 1.1 so
    // the entry is stale at every tick even under the ±5% TTL jitter.
    assertThat(TimedRefreshCoordinator.intervalFor(100L)).isEqualTo(110L);
    assertThat(TimedRefreshCoordinator.intervalFor(1_000L)).isEqualTo(1_100L);
    assertThat(TimedRefreshCoordinator.intervalFor(5_000L)).isEqualTo(5_500L);
    // Rounding: 7ms × 1.1 = 7.7 → ceil to 8 so the interval never undershoots.
    assertThat(TimedRefreshCoordinator.intervalFor(7L)).isEqualTo(8L);
    // Non-positive TTLs fail fast instead of degrading to a 1ms hot loop.
    assertThatThrownBy(() -> TimedRefreshCoordinator.intervalFor(0L)).isInstanceOf(IllegalArgumentException.class);
    assertThat(TimedRefreshCoordinator.intervalFor(1L)).isEqualTo(2L);
  }

  @Test
  void unregisterRefresh_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.refresh().unregisterRefresh(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void refresh_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.refresh(null, () -> "v")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void refresh_shouldRejectNullLoader() {
    assertThatThrownBy(() -> zeta.refresh("k", null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void workerMode_shouldStillRejectNullKey() {
    Zeta workerOnly = new DefaultZeta(null, null);
    assertThatThrownBy(() -> workerOnly.get(null, () -> "v", GetPolicy.defaults())).isInstanceOf(
      IllegalArgumentException.class
    );
  }
}
