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
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
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
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
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
  @SuppressWarnings("unchecked")
  void getAll_negativeTtl_shouldFailFastEvenForAnEmptyCollection() {
    GetPolicy negative = new GetPolicy(() -> 0L, () -> -1L, StalePolicy.SOFT_REFRESH, true, false);

    assertThatThrownBy(() -> zeta.getAll(List.of(), k -> "v", negative))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("softTtlMs");
    verify(hotKeyCache, never()).get(anyList(), any(Function.class), any(GetPolicy.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void getAll_shouldResolveTheLazyTtlsExactlyOnce() {
    when(hotKeyCache.get(anyList(), any(Function.class), any(GetPolicy.class))).thenReturn(Map.of());
    AtomicInteger evaluations = new AtomicInteger();
    GetPolicy lazy = new GetPolicy(
      () -> {
        evaluations.incrementAndGet();
        return 1_000L;
      },
      () -> {
        evaluations.incrementAndGet();
        return 100L;
      },
      StalePolicy.SOFT_REFRESH,
      true,
      false
    );

    zeta.getAll(List.of("k1", "k2"), k -> "v", lazy);

    assertThat(evaluations).hasValue(2);
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

    assertThatThrownBy(() -> zeta.refresh("k", () -> "v", true))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("permanent entry cannot be synced");
    verify(hotKeyCache, never()).refresh(anyString(), any(Supplier.class), anyBoolean());
  }

  @Test
  void refreshAll_shouldRejectAConfiguredPermanentDefault() {
    when(hotKeyCache.resolveSyncableHardTtl(anyLong())).thenReturn(Long.MAX_VALUE);

    assertThatThrownBy(() -> zeta.refreshAll(Map.of("k", (Supplier<?>) () -> "v"), true))
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
  void putAll_validatesAndSubmitsOneSnapshot() {
    Map<String, Object> entries = new AbstractMap<>() {
      private int entrySetCalls;

      @Override
      public Set<Map.Entry<String, Object>> entrySet() {
        entrySetCalls++;
        return entrySetCalls == 1
          ? Map.<String, Object>of("good", "v").entrySet()
          : Map.<String, Object>of(" ", "v").entrySet();
      }

      @Override
      public int size() {
        return 1;
      }
    };

    zeta.putAll(entries, PutPolicy.defaults());

    // The engine receives the validated copy, not the caller's map: a second
    // traversal of the live map would expose the blank key injected above.
    verify(hotKeyCache).putAll(Map.of("good", "v"), 0L, 0L, false);
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
  void putAll_emptyWithANegativeDynamicTtl_shouldStillReject() {
    PutPolicy negative = new PutPolicy(() -> -1L, () -> 0L, false);

    assertThatThrownBy(() -> zeta.putAll(Map.of(), negative)).isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * A duration above the packed-entry maximum but below the permanent sentinel is not encodable.
   * Validating it here fails before a transaction's after-commit hook, not after the business
   * write committed.
   */
  @Test
  void putAll_emptyWithAnUnencodableDynamicTtl_shouldStillReject() {
    PutPolicy unencodable = new PutPolicy(() -> 3_000_000_000_000L, () -> 0L, false);

    assertThatThrownBy(() -> zeta.putAll(Map.of(), unencodable))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("out of range");
  }

  @Test
  void putAll_emptyWithAnUnsyncablePermanentDefault_shouldStillReject() {
    when(hotKeyCache.resolveSyncableHardTtl(anyLong())).thenReturn(Long.MAX_VALUE);

    assertThatThrownBy(() -> zeta.putAll(Map.of(), PutPolicy.synced()))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("permanent entry cannot be synced");
  }

  @Test
  void refreshAll_emptyWithAnUnsyncablePermanentDefault_shouldStillReject() {
    when(hotKeyCache.resolveSyncableHardTtl(anyLong())).thenReturn(Long.MAX_VALUE);

    assertThatThrownBy(() -> zeta.refreshAll(Map.<String, Supplier<?>>of(), true))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("permanent entry cannot be synced");
  }

  @Test
  void refreshAll_validatesAndSubmitsOneSnapshot() {
    SingleSnapshotMap loaders = new SingleSnapshotMap();

    zeta.refreshAll(loaders);

    assertThat(loaders.entrySetCalls).as("one validation pass, one submission source").isEqualTo(1);
    verify(hotKeyCache).refresh(eq("good"), any(Supplier.class), eq(false));
  }

  /** A map whose second traversal would expose an entry absent from the validated snapshot. */
  private static final class SingleSnapshotMap extends AbstractMap<String, Supplier<?>> {

    private int entrySetCalls;

    @Override
    public Set<Map.Entry<String, Supplier<?>>> entrySet() {
      entrySetCalls++;
      if (entrySetCalls == 1) {
        return Map.<String, Supplier<?>>of("good", () -> "v").entrySet();
      }
      return Map.<String, Supplier<?>>of(" ", () -> "v").entrySet();
    }

    @Override
    public int size() {
      return 1;
    }
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
  void tag_shouldRouteEveryOverloadThroughTheCanonicalPrimitive() {
    zeta.tag("a");
    zeta.tag("b", Zeta.TagMode.DETECT_ONLY);
    zeta.tag("c", 5L);
    zeta.tag("d", 7L, Zeta.TagMode.REPORT_ONLY);

    verify(hotKeyCache).recordAccess("a", 1L, Zeta.TagMode.RECORD);
    verify(hotKeyCache).recordAccess("b", 1L, Zeta.TagMode.DETECT_ONLY);
    verify(hotKeyCache).recordAccess("c", 5L, Zeta.TagMode.RECORD);
    verify(hotKeyCache).recordAccess("d", 7L, Zeta.TagMode.REPORT_ONLY);
  }

  @Test
  void tag_nullKey_shouldReject() {
    assertThatThrownBy(() -> zeta.tag(null, 1L, Zeta.TagMode.RECORD)).isInstanceOf(IllegalArgumentException.class);
    verify(hotKeyCache, never()).recordAccess(anyString(), anyLong(), any());
  }

  @Test
  void tag_nonPositiveCount_shouldReject() {
    assertThatThrownBy(() -> zeta.tag("k", 0, Zeta.TagMode.RECORD)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> zeta.tag("k", -1, Zeta.TagMode.RECORD)).isInstanceOf(IllegalArgumentException.class);
    verify(hotKeyCache, never()).recordAccess(anyString(), anyLong(), any());
  }

  @Test
  void tag_nullMode_shouldReject() {
    assertThatThrownBy(() -> zeta.tag("k", 1L, null)).isInstanceOf(NullPointerException.class);
    verify(hotKeyCache, never()).recordAccess(anyString(), anyLong(), any());
  }

  @Test
  void tag_silent_shouldValidateBeforeNoOp() {
    assertThatThrownBy(() -> zeta.tag(" ", 1L, Zeta.TagMode.SILENT)).isInstanceOf(IllegalArgumentException.class);
    verify(hotKeyCache, never()).recordAccess(anyString(), anyLong(), any());
  }

  @Test
  void tagAll_shouldDelegateTheValidatedSnapshot() {
    Map<String, Long> counts = new LinkedHashMap<>();
    counts.put("a", 2L);
    counts.put("b", 3L);

    zeta.tagAll(counts, Zeta.TagMode.DETECT_ONLY);

    verify(hotKeyCache).recordAccess(eq(Map.of("a", 2L, "b", 3L)), eq(Zeta.TagMode.DETECT_ONLY));
  }

  @Test
  void tagAll_empty_shouldStillCheckModeAndDeployment() {
    assertThatThrownBy(() -> zeta.tagAll(Map.of(), null)).isInstanceOf(NullPointerException.class);
    Zeta workerOnly = new DefaultZeta(null, null);
    assertThatThrownBy(() -> workerOnly.tagAll(Map.of(), Zeta.TagMode.RECORD)).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void tagAll_invalidEntry_shouldRejectTheWholeBatch() {
    assertThatThrownBy(() -> zeta.tagAll(Map.of("ok", 1L, " ", 2L), Zeta.TagMode.RECORD)).isInstanceOf(
      IllegalArgumentException.class
    );
    assertThatThrownBy(() -> zeta.tagAll(Map.of("ok", 1L, "bad", 0L), Zeta.TagMode.RECORD)).isInstanceOf(
      IllegalArgumentException.class
    );

    Map<String, Long> nullCount = new LinkedHashMap<>();
    nullCount.put("k", null);
    assertThatThrownBy(() -> zeta.tagAll(nullCount, Zeta.TagMode.RECORD)).isInstanceOf(NullPointerException.class);
    verify(hotKeyCache, never()).recordAccess(anyMap(), any());
  }

  @Test
  void tagAll_collection_shouldSumDuplicateKeys() {
    zeta.tagAll(List.of("a", "a", "b"), Zeta.TagMode.DETECT_ONLY);
    verify(hotKeyCache).recordAccess(eq(Map.of("a", 2L, "b", 1L)), eq(Zeta.TagMode.DETECT_ONLY));
  }

  @Test
  void tagAll_collection_invalidElement_shouldReject() {
    assertThatThrownBy(() -> zeta.tagAll(Arrays.asList("a", null), Zeta.TagMode.RECORD)).isInstanceOf(
      IllegalArgumentException.class
    );
    verify(hotKeyCache, never()).recordAccess(anyMap(), any());
  }

  @Test
  void tagAll_silent_shouldValidateBeforeNoOp() {
    assertThatThrownBy(() -> zeta.tagAll(Map.of("k", 0L), Zeta.TagMode.SILENT)).isInstanceOf(
      IllegalArgumentException.class
    );
    verify(hotKeyCache, never()).recordAccess(anyMap(), any());
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
  void forgetDedup_emptyArray_shouldNotReachTheEngine() {
    zeta.unsafe().forgetDedup();
    verify(hotKeyCache, never()).forgetDedup(any(String[].class));
  }

  @Test
  void forgetDedup_emptyArray_shouldStillThrowInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null);
    assertThatThrownBy(() -> workerOnly.unsafe().forgetDedup()).isInstanceOf(ZetaModeException.class);
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
  void refresh_shouldDefaultToLocalWithoutBroadcast() {
    zeta.refresh("k1", () -> "v");
    verify(hotKeyCache).refresh(eq("k1"), any(Supplier.class), eq(false));
  }

  @Test
  void refresh_synced_shouldBroadcast() {
    zeta.refresh("k1", () -> "v", true);
    verify(hotKeyCache).refresh(eq("k1"), any(Supplier.class), eq(true));
  }

  @Test
  void refresh_withCacheLoader_shouldDefaultToLocalWithoutBroadcast() {
    CacheLoader<String, String> loader = key -> "v";

    zeta.refresh("k1", loader);

    verify(hotKeyCache).refresh(eq("k1"), same(loader), eq(false));
  }

  @Test
  void refresh_withCacheLoaderAndSync_shouldBroadcast() {
    CacheLoader<String, String> loader = key -> "v";

    zeta.refresh("k1", loader, true);

    verify(hotKeyCache).refresh(eq("k1"), same(loader), eq(true));
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
    verify(hotKeyCache, times(2)).refresh(anyString(), any(Supplier.class), eq(false));
  }

  @Test
  void refreshAll_sync_shouldBroadcastEachKey() {
    zeta.refreshAll(Map.of("k1", (Supplier<?>) () -> "v1", "k2", (Supplier<?>) () -> "v2"), true);

    verify(hotKeyCache).refresh(eq("k1"), any(Supplier.class), eq(true));
    verify(hotKeyCache).refresh(eq("k2"), any(Supplier.class), eq(true));
  }

  @Test
  void refreshAll_singleEntry_shouldRunInlineWithoutTheExecutor() {
    DefaultZeta facade = new DefaultZeta(hotKeyCache, appDetector, task -> {
      throw new AssertionError("a single-entry refresh must not enter the fan-out");
    });

    facade.refreshAll(Map.of("only", (Supplier<?>) () -> "v"));

    verify(hotKeyCache).refresh(eq("only"), any(Supplier.class), eq(false));
  }

  @Test
  void refreshAll_submissionError_shouldJoinAcceptedTasksBeforeRethrowing() {
    AtomicBoolean refreshed = new AtomicBoolean();
    doAnswer(invocation -> {
      Thread.sleep(150L);
      refreshed.set(true);
      return null;
    })
      .when(hotKeyCache)
      .refresh(anyString(), any(Supplier.class), anyBoolean());
    AtomicInteger submissions = new AtomicInteger();
    DefaultZeta facade = new DefaultZeta(hotKeyCache, appDetector, task -> {
      if (submissions.getAndIncrement() == 0) {
        Thread worker = new Thread(task, "refresh-all-submission-error");
        worker.setDaemon(true);
        worker.start();
      } else {
        throw new AssertionError("submission failed");
      }
    });

    assertThatThrownBy(() -> facade.refreshAll(Map.of("k1", (Supplier<?>) () -> "v1", "k2", (Supplier<?>) () -> "v2")))
      .isInstanceOf(AssertionError.class)
      .hasMessage("submission failed");
    assertThat(refreshed).as("the accepted task completed before refreshAll returned").isTrue();
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
  void refresh_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.refresh(null, () -> "v")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void refresh_shouldRejectNullReader() {
    assertThatThrownBy(() -> zeta.refresh("k", (Supplier<String>) null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void refresh_shouldRejectNullLoader() {
    assertThatThrownBy(() -> zeta.refresh("k", (CacheLoader<String, String>) null)).isInstanceOf(
      NullPointerException.class
    );
  }

  @Test
  void workerMode_shouldStillRejectNullKey() {
    Zeta workerOnly = new DefaultZeta(null, null);
    assertThatThrownBy(() -> workerOnly.get(null, () -> "v", GetPolicy.defaults())).isInstanceOf(
      IllegalArgumentException.class
    );
  }
}
