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
package io.github.hyshmily.hotkey;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.cache.HotKeyCache;
import io.github.hyshmily.hotkey.exception.HotKeyBlockedException;
import io.github.hyshmily.hotkey.exception.HotKeyModeException;
import io.github.hyshmily.hotkey.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.hotkey.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.hotkey.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.hotkey.model.HotKeyCacheStats;
import io.github.hyshmily.hotkey.rule.Rule;
import io.github.hyshmily.hotkey.rule.Rule.RuleAction;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HotKeyTest {

  private HotKeyCache hotKeyCache;
  private HotKeyDetector appDetector;
  private TopK workerTopK;
  private HotKey hotKey;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    hotKeyCache = mock(HotKeyCache.class);
    appDetector = mock(HotKeyDetector.class);
    workerTopK = mock(TopK.class);
    hotKey = new HotKey(hotKeyCache, appDetector, workerTopK);
  }

  @Test
  void isLocalHotKey_shouldDelegateToCache() {
    when(hotKeyCache.isHot("key1")).thenReturn(true);
    assertThat(hotKey.isLocalHotKey("key1")).isTrue();
    verify(hotKeyCache).isHot("key1");
  }

  @Test
  void peek_shouldDelegateToCache() {
    when(hotKeyCache.peek("key1")).thenReturn(Optional.of("value"));
    assertThat(hotKey.peek("key1")).contains("value");
    verify(hotKeyCache).peek("key1");
  }

  @Test
  void get_shouldDelegateToCache() {
    when(hotKeyCache.get(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(Optional.of("value"));
    assertThat(hotKey.get("key1", () -> "loaded")).contains("value");
    verify(hotKeyCache).get(anyString(), any(), anyLong(), anyLong(), anyBoolean());
  }

  @Test
  void get_withTtl_shouldDelegateToCache() {
    when(hotKeyCache.get(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(Optional.of("v"));
    assertThat(hotKey.get("key1", () -> "loaded", 1000L, 100L)).contains("v");
    verify(hotKeyCache).get(anyString(), any(), anyLong(), anyLong(), anyBoolean());
  }

  @Test
  void getWithSoftExpire_shouldDelegateToCache() {
    when(hotKeyCache.getWithSoftExpire(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(
      Optional.of("v")
    );
    assertThat(hotKey.getWithSoftExpire("key1", () -> "v")).contains("v");
    verify(hotKeyCache).getWithSoftExpire(anyString(), any(), anyLong(), anyLong(), anyBoolean());
  }

  @Test
  void invalidate_shouldDelegateToCache() {
    hotKey.invalidate("key1");
    verify(hotKeyCache).invalidate("key1", true);
  }

  @Test
  void invalidate_varargs_shouldDelegateToCache() {
    hotKey.invalidate(List.of("k1", "k2"));
    verify(hotKeyCache).invalidate(List.of("k1", "k2"), true);
  }

  @Test
  void putThrough_shouldDelegateToCache() {
    hotKey.putThrough("key1", "value", () -> {});
    verify(hotKeyCache).putThrough(anyString(), any(), any(), anyLong(), anyLong(), anyBoolean());
  }

  @Test
  void putThrough_withTtl_shouldDelegateToCache() {
    hotKey.putThrough("key1", "value", () -> {}, 2000L, 200L, true);
    verify(hotKeyCache).putThrough(anyString(), any(), any(), anyLong(), anyLong(), anyBoolean());
  }

  @Test
  void invalidateAfterPut_single_shouldDelegateToCache() {
    hotKey.invalidateAfterPut("key1", () -> {});
    verify(hotKeyCache).invalidateAfterPut(anyString(), any(), anyBoolean());
  }

  @Test
  void returnHotKeys_shouldReturnLocalFromTopK() {
    when(appDetector.list()).thenReturn(List.of(new Item("k1", 10)));
    assertThat(hotKey.returnLocalHotKeys()).hasSize(1);
  }

  @Test
  void returnHotKeys_shouldReturnLocalEmptyWhenTopKNull() {
    HotKey hk = new HotKey(hotKeyCache, null, null);
    assertThat(hk.returnLocalHotKeys()).isEmpty();
  }

  @Test
  void returnWorkerHotKeys_shouldReturnFromWorkerTopK() {
    when(workerTopK.list()).thenReturn(List.of(new Item("k1", 5)));
    assertThat(hotKey.returnWorkerHotKeys()).hasSize(1);
  }

  @Test
  void returnWorkerHotKeys_shouldReturnEmptyWhenWorkerTopKNull() {
    assertThat(new HotKey(hotKeyCache, appDetector, null).returnWorkerHotKeys()).isEmpty();
  }

  @Test
  void returnTotalDataStreams_shouldReturnLocalFromTopK() {
    when(appDetector.total()).thenReturn(100L);
    assertThat(hotKey.returnLocalTotalDataStreams()).isEqualTo(100L);
  }

  @Test
  void returnTotalDataStreams_shouldReturnLocalZeroWhenTopKNull() {
    assertThat(new HotKey(hotKeyCache, null, null).returnLocalTotalDataStreams()).isZero();
  }

  @Test
  void returnExpelledHotKeys_shouldReturnLocalFromTopK() {
    LinkedBlockingQueue<Item> queue = new LinkedBlockingQueue<>();
    queue.add(new Item("k1", 5));
    when(appDetector.expelled()).thenReturn(queue);
    assertThat(hotKey.returnLocalExpelledHotKeys()).hasSize(1);
  }

  @Test
  void cacheMethods_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.get("k", () -> "v")).isInstanceOf(HotKeyModeException.class);
    assertThatThrownBy(() -> workerOnly.isLocalHotKey("k")).isInstanceOf(HotKeyModeException.class);
    assertThatThrownBy(() -> workerOnly.peek("k")).isInstanceOf(HotKeyModeException.class);
    assertThatThrownBy(() -> workerOnly.invalidate("k")).isInstanceOf(HotKeyModeException.class);
    assertThatThrownBy(() -> workerOnly.putThrough("k", "v", () -> {})).isInstanceOf(HotKeyModeException.class);
    assertThatThrownBy(() -> workerOnly.invalidateAfterPut("k", () -> {})).isInstanceOf(HotKeyModeException.class);
  }

  @Test
  void get_shouldPropagateHotKeyBlockedException() {
    when(hotKeyCache.get(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenThrow(
      new HotKeyBlockedException("HotKeyCache", "secret")
    );
    assertThatThrownBy(() -> hotKey.get("secret", () -> "v")).isInstanceOf(HotKeyBlockedException.class);
  }

  // ── Additional getWithSoftExpire overloads ──

  @Test
  void getWithSoftExpire_withSoftTtl_shouldDelegateToCache() {
    when(hotKeyCache.getWithSoftExpire(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(
      Optional.of("v")
    );
    assertThat(hotKey.getWithSoftExpire("key1", () -> "v", 200L)).contains("v");
    verify(hotKeyCache).getWithSoftExpire(anyString(), any(), anyLong(), anyLong(), anyBoolean());
  }

  @Test
  void getWithSoftExpire_withHardSoftTtl_shouldDelegateToCache() {
    when(hotKeyCache.getWithSoftExpire(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(
      Optional.of("v")
    );
    assertThat(hotKey.getWithSoftExpire("key1", () -> "v", 5000L, 500L, true)).contains("v");
    verify(hotKeyCache).getWithSoftExpire(anyString(), any(), anyLong(), anyLong(), anyBoolean());
  }

  // ── isWorkerHotKey ──

  @Test
  void isWorkerHotKey_shouldDelegateToWorkerTopK() {
    when(workerTopK.contains("hot-key")).thenReturn(true);
    assertThat(hotKey.isWorkerHotKey("hot-key")).isTrue();
    verify(workerTopK).contains("hot-key");
  }

  @Test
  void isWorkerHotKey_shouldReturnFalseWhenNotInWorkerTopK() {
    when(workerTopK.contains("cold-key")).thenReturn(false);
    assertThat(hotKey.isWorkerHotKey("cold-key")).isFalse();
  }

  @Test
  void isWorkerHotKey_shouldReturnFalseWhenWorkerTopKNull() {
    HotKey hk = new HotKey(hotKeyCache, appDetector, null);
    assertThat(hk.isWorkerHotKey("any")).isFalse();
  }

  @Test
  void isWorkerHotKey_shouldReturnFalseWhenNullKey() {
    assertThat(hotKey.isWorkerHotKey(null)).isFalse();
    verifyNoInteractions(workerTopK);
  }

  // ── returnWorkerExpelledHotKeys ──

  @Test
  void returnWorkerExpelledHotKeys_shouldReturnFromWorkerTopK() {
    LinkedBlockingQueue<Item> queue = new LinkedBlockingQueue<>();
    queue.add(new Item("wk1", 3));
    when(workerTopK.expelled()).thenReturn(queue);
    assertThat(hotKey.returnWorkerExpelledHotKeys()).hasSize(1);
  }

  @Test
  void returnWorkerExpelledHotKeys_shouldReturnEmptyQueueWhenWorkerTopKNull() {
    HotKey hk = new HotKey(hotKeyCache, appDetector, null);
    assertThat(hk.returnWorkerExpelledHotKeys()).isEmpty();
  }

  // ── returnWorkerTotalDataStreams ──

  @Test
  void returnWorkerTotalDataStreams_shouldReturnFromWorkerTopK() {
    when(workerTopK.total()).thenReturn(999L);
    assertThat(hotKey.returnWorkerTotalDataStreams()).isEqualTo(999L);
  }

  @Test
  void returnWorkerTotalDataStreams_shouldReturnZeroWhenWorkerTopKNull() {
    HotKey hk = new HotKey(hotKeyCache, appDetector, null);
    assertThat(hk.returnWorkerTotalDataStreams()).isZero();
  }

  // ── returnLocalExpelledHotKeys null guard ──

  @Test
  void returnExpelledHotKeys_shouldReturnEmptyQueueWhenTopKNull() {
    HotKey hk = new HotKey(hotKeyCache, null, null);
    assertThat(hk.returnLocalExpelledHotKeys()).isEmpty();
  }

  // ── returnLocalTotalDataStreams null guard (full 3-arg ctor) ──

  @Test
  void returnTotalDataStreams_shouldReturnLocalZeroWhenTopKNullThreeArg() {
    HotKey hk = new HotKey(hotKeyCache, null, workerTopK);
    assertThat(hk.returnLocalTotalDataStreams()).isZero();
  }

  // ── getLocalCache ──

  @SuppressWarnings("unchecked")
  @Test
  void getLocalCache_shouldDelegateToCache() {
    Cache<String, Object> caffeine = mock(Cache.class);
    when(hotKeyCache.getLocalCache()).thenReturn(caffeine);
    assertThat(hotKey.getLocalCache()).isSameAs(caffeine);
    verify(hotKeyCache).getLocalCache();
  }

  @Test
  void getLocalCache_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThatThrownBy(workerOnly::getLocalCache).isInstanceOf(HotKeyModeException.class);
  }

  // ── addBlacklist / removeBlacklist / addWhitelist / removeWhitelist ──

  @Test
  void addBlacklist_shouldDelegateToCache() {
    hotKey.addBlacklist("secret-*");
    verify(hotKeyCache).addBlacklist("secret-*");
  }

  @Test
  void addBlacklist_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.addBlacklist("x")).isInstanceOf(HotKeyModeException.class);
  }

  @Test
  void removeBlacklist_shouldDelegateToCache() {
    hotKey.removeBlacklist("old-rule");
    verify(hotKeyCache).unBlacklist("old-rule");
  }

  @Test
  void removeBlacklist_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.removeBlacklist("x")).isInstanceOf(HotKeyModeException.class);
  }

  @Test
  void addWhitelist_shouldDelegateToCache() {
    hotKey.addWhitelist("health-*");
    verify(hotKeyCache).addWhitelist("health-*");
  }

  @Test
  void addWhitelist_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.addWhitelist("x")).isInstanceOf(HotKeyModeException.class);
  }

  @Test
  void removeWhitelist_shouldDelegateToCache() {
    hotKey.removeWhitelist("health-*");
    verify(hotKeyCache).unWhitelist("health-*");
  }

  @Test
  void removeWhitelist_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.removeWhitelist("x")).isInstanceOf(HotKeyModeException.class);
  }

  // ── getAllRules ──

  @Test
  void getAllRules_shouldDelegateToCache() {
    Rule rule = new Rule(Rule.RuleType.EXACT, "blocked", RuleAction.BLOCK);
    when(hotKeyCache.getAllRules()).thenReturn(List.of(rule));
    assertThat(hotKey.getAllRules()).containsExactly(rule);
    verify(hotKeyCache).getAllRules();
  }

  @Test
  void getAllRules_shouldReturnEmptyWhenCacheNull() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThat(workerOnly.getAllRules()).isEmpty();
  }

  // ── evaluateRule ──

  @Test
  void evaluateRule_shouldDelegateToCache() {
    when(hotKeyCache.evaluateRule("secret")).thenReturn(RuleAction.BLOCK);
    assertThat(hotKey.evaluateRule("secret")).isEqualTo(RuleAction.BLOCK);
    verify(hotKeyCache).evaluateRule("secret");
  }

  @Test
  void evaluateRule_shouldReturnAllowWhenCacheNull() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThat(workerOnly.evaluateRule("any")).isEqualTo(RuleAction.ALLOW);
  }

  // ── clearAllRules ──

  @Test
  void clearAllRules_shouldDelegateToCache() {
    hotKey.clearAllRules();
    verify(hotKeyCache).clearAllRules();
  }

  @Test
  void clearAllRules_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThatThrownBy(workerOnly::clearAllRules).isInstanceOf(HotKeyModeException.class);
  }

  // ── broadcastAllLocalRulesManually ──

  @Test
  void broadcastAllLocalRulesManually_shouldDelegateToCache() {
    hotKey.broadcastAllLocalRulesManually();
    verify(hotKeyCache).broadcastAllLocalRulesManually();
  }

  @Test
  void broadcastAllLocalRulesManually_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThatThrownBy(workerOnly::broadcastAllLocalRulesManually).isInstanceOf(HotKeyModeException.class);
  }

  // ── notifyLocalDetector ──

  @Test
  void notifyLocalDetector_shouldDelegateToTopK() {
    hotKey.notifyLocalDetector("my-key");
    verify(appDetector).add("my-key");
  }

  @Test
  void notifyLocalDetector_shouldIgnoreNullKey() {
    hotKey.notifyLocalDetector((String) null);
    verify(appDetector, never()).add(anyString());
  }

  // ── read / write factories ──

  @Test
  void read_shouldReturnHotKeyReadQuery() {
    when(hotKeyCache.evaluateRule("k")).thenReturn(RuleAction.ALLOW);
    when(hotKeyCache.get(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(Optional.of("v"));
    assertThat(
      hotKey
        .read("k")
        .withPrimary(() -> "db")
        .execute()
    ).contains("v");
  }

  @Test
  void write_shouldReturnHotKeyWriteCommand() {
    hotKey.write("k").invalidate();
    verify(hotKeyCache).invalidate("k", true);
  }

  // ── computeIfAbsent single-key ──

  @Test
  void computeIfAbsent_shouldReturnValue() {
    when(hotKeyCache.computeIfAbsent(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(
      Optional.of("v")
    );
    assertThat(hotKey.computeIfAbsent("k", () -> "loaded")).isEqualTo("v");
    verify(hotKeyCache).computeIfAbsent(eq("k"), any(), anyLong(), anyLong(), anyBoolean());
  }

  @Test
  void computeIfAbsent_shouldReturnNullWhenLoaderNull() {
    when(hotKeyCache.computeIfAbsent(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(
      Optional.empty()
    );
    assertThat((Object) hotKey.computeIfAbsent("k", () -> null)).isNull();
  }

  @Test
  void computeIfAbsent_withHardTtl_shouldDelegate() {
    when(hotKeyCache.computeIfAbsent(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(
      Optional.of("v")
    );
    assertThat(hotKey.computeIfAbsent("k", () -> "db", 5000L)).isEqualTo("v");
    verify(hotKeyCache).computeIfAbsent(eq("k"), any(), eq(5000L), eq(0L), anyBoolean());
  }

  @Test
  void computeIfAbsent_withHardSoftTtl_shouldDelegate() {
    when(hotKeyCache.computeIfAbsent(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(
      Optional.of("v")
    );
    assertThat(hotKey.computeIfAbsent("k", () -> "db", 5000L, 500L)).isEqualTo("v");
    verify(hotKeyCache).computeIfAbsent(eq("k"), any(), eq(5000L), eq(500L), anyBoolean());
  }

  @Test
  void computeIfAbsent_withNonDefaultReport_shouldDelegate() {
    when(hotKeyCache.computeIfAbsent(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(
      Optional.of("v")
    );
    assertThat(hotKey.computeIfAbsent("k", () -> "db", 0L, 0L, false)).isEqualTo("v");
    verify(hotKeyCache).computeIfAbsent(eq("k"), any(), eq(0L), eq(0L), eq(false));
  }

  @Test
  void computeIfAbsentWithSoftExpire_SoftTtl_shouldDelegate() {
    when(hotKeyCache.computeIfAbsentWithSoftExpire(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(
      Optional.of("v")
    );
    assertThat(hotKey.computeIfAbsentWithSoftExpire("k", () -> "db", 500L)).isEqualTo("v");
    verify(hotKeyCache).computeIfAbsentWithSoftExpire(eq("k"), any(), eq(0L), eq(500L), anyBoolean());
  }

  @Test
  void computeIfAbsentWithSoftExpire_collectionSoftTtl_shouldDelegate() {
    when(hotKeyCache.computeIfAbsentWithSoftExpire(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(
      Optional.of("v")
    );
    assertThat(hotKey.computeIfAbsentWithSoftExpire("k", () -> "db", 500L)).isEqualTo("v");
    verify(hotKeyCache).computeIfAbsentWithSoftExpire(eq("k"), any(), eq(0L), eq(500L), anyBoolean());
  }

  @Test
  void computeIfAbsentWithSoftExpire_BothTtls_shouldDelegate() {
    when(hotKeyCache.computeIfAbsentWithSoftExpire(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(
      Optional.of("v")
    );
    assertThat(hotKey.computeIfAbsentWithSoftExpire("k", () -> "db", 5000L, 500L)).isEqualTo("v");
    verify(hotKeyCache).computeIfAbsentWithSoftExpire(eq("k"), any(), eq(5000L), eq(500L), anyBoolean());
  }

  @Test
  void computeIfAbsentWithSoftExpire_BothTtlsAndReport_shouldDelegate() {
    when(hotKeyCache.computeIfAbsentWithSoftExpire(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(
      Optional.of("v")
    );
    assertThat(hotKey.computeIfAbsentWithSoftExpire("k", () -> "db", 5000L, 500L, true)).isEqualTo("v");
    verify(hotKeyCache).computeIfAbsentWithSoftExpire(eq("k"), any(), eq(5000L), eq(500L), eq(true));
  }

  // ── invalidateAllLocal no-arg ──

  @Test
  void invalidateAllLocal_shouldDelegateToCache() {
    hotKey.invalidateAllLocal();
    verify(hotKeyCache).invalidateAllLocal();
  }

  @Test
  void invalidateAllLocal_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThatThrownBy(workerOnly::invalidateAllLocal).isInstanceOf(HotKeyModeException.class);
  }

  // ── invalidate Collection ──

  @Test
  void invalidate_collection_shouldDelegateToCache() {
    hotKey.invalidate(List.of("k1", "k2"));
    verify(hotKeyCache).invalidate(List.of("k1", "k2"), true);
  }

  // ── putLocal ──

  @Test
  void putLocal_shouldDelegateToCache() {
    hotKey.putLocal("k", "v");
    verify(hotKeyCache).putLocal("k", "v", 0L, 0L);
  }

  @Test
  void putLocal_withTtl_shouldDelegateToCache() {
    hotKey.putLocal("k", "v", 5000L, 500L);
    verify(hotKeyCache).putLocal("k", "v", 5000L, 500L);
  }

  // ── compareAndSet / compareAndInvalidate ──

  @Test
  void compareAndSet_shouldDelegateToCache() {
    when(hotKeyCache.compareAndSet(eq("k"), eq("old"), eq("new"))).thenReturn(true);
    assertThat(hotKey.compareAndSet("k", "old", "new")).isTrue();
    verify(hotKeyCache).compareAndSet("k", "old", "new");
  }

  @Test
  void compareAndSet_shouldReturnFalseOnMismatch() {
    when(hotKeyCache.compareAndSet(eq("k"), eq("wrong"), eq("new"))).thenReturn(false);
    assertThat(hotKey.compareAndSet("k", "wrong", "new")).isFalse();
    verify(hotKeyCache).compareAndSet("k", "wrong", "new");
  }

  @Test
  void compareAndSet_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.compareAndSet("k", "old", "new")).isInstanceOf(HotKeyModeException.class);
  }

  @Test
  void compareAndInvalidate_shouldDelegateToCache() {
    when(hotKeyCache.compareAndInvalidate(eq("k"), eq("old"))).thenReturn(true);
    assertThat(hotKey.compareAndInvalidate("k", "old")).isTrue();
    verify(hotKeyCache).compareAndInvalidate("k", "old");
  }

  @Test
  void compareAndInvalidate_shouldReturnFalseOnMismatch() {
    when(hotKeyCache.compareAndInvalidate(eq("k"), eq("wrong"))).thenReturn(false);
    assertThat(hotKey.compareAndInvalidate("k", "wrong")).isFalse();
    verify(hotKeyCache).compareAndInvalidate("k", "wrong");
  }

  @Test
  void compareAndInvalidate_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.compareAndInvalidate("k", "old")).isInstanceOf(HotKeyModeException.class);
  }

  // ── estimatedSizeOfKeysCount ──

  @Test
  void estimatedSize_shouldDelegateToCache() {
    when(hotKeyCache.estimatedSize()).thenReturn(42L);
    assertThat(hotKey.estimatedSize()).isEqualTo(42L);
    verify(hotKeyCache).estimatedSize();
  }

  @Test
  void estimatedSize_shouldReturnZeroInWorkerMode() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThat(workerOnly.estimatedSize()).isZero();
  }

  // ── stats ──

  @Test
  void stats_shouldDelegateToCache() {
    HotKeyCacheStats stats = mock(HotKeyCacheStats.class);
    when(hotKeyCache.stats()).thenReturn(stats);
    assertThat(hotKey.stats()).isSameAs(stats);
    verify(hotKeyCache).stats();
  }

  @Test
  void stats_shouldReturnNullInWorkerMode() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThat(workerOnly.stats()).isNull();
  }

  // ── notifyLocalDetectorDirect ──

  @Test
  void notifyLocalDetectorDirect_shouldDelegateToAppDetector() {
    hotKey.notifyLocalDetectorDirect("k", 5);
    verify(appDetector).addDirect("k", 5);
  }

  @Test
  void notifyLocalDetectorDirect_map_shouldDelegateToAppDetector() {
    Map<String, Long> map = Map.of("k", 3L);
    hotKey.notifyLocalDetectorDirect(map);
    verify(appDetector).addDirect(map);
  }

  // ── notifyLocalDetector(String, long) and Map ──

  @Test
  void notifyLocalDetector_withDelta_shouldDelegateToAppDetector() {
    hotKey.notifyLocalDetector("k", 7);
    verify(appDetector).add("k", 7);
  }

  @Test
  void notifyLocalDetector_map_shouldDelegateToAppDetector() {
    Map<String, Long> map = Map.of("k", 3L);
    hotKey.notifyLocalDetector(map);
    verify(appDetector).add(map);
  }

  // ── returnLocalTopNHotKeys ──

  @Test
  void returnLocalTopNHotKeys_shouldDelegateToAppDetector() {
    List<Item> items = List.of(new Item("k", 10));
    when(appDetector.listTopN(3)).thenReturn(items);
    assertThat(hotKey.returnLocalTopNHotKeys(3)).isSameAs(items);
    verify(appDetector).listTopN(3);
  }

  @Test
  void returnLocalTopNHotKeys_shouldReturnEmptyWhenDetectorNull() {
    HotKey hk = new HotKey(hotKeyCache, null, workerTopK);
    assertThat(hk.returnLocalTopNHotKeys(5)).isEmpty();
  }

  // ── isBlacklisted ──

  @Test
  void isBlacklisted_shouldDelegateToCache() {
    when(hotKeyCache.isBlacklisted("secret")).thenReturn(true);
    assertThat(hotKey.isBlacklisted("secret")).isTrue();
    verify(hotKeyCache).isBlacklisted("secret");
  }

  @Test
  void isBlacklisted_shouldReturnFalseWhenCacheNull() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThat(workerOnly.isBlacklisted("x")).isFalse();
  }

  // ── isWhitelisted ──

  @Test
  void isWhitelisted_shouldDelegateToCache() {
    when(hotKeyCache.isWhitelisted("health")).thenReturn(true);
    assertThat(hotKey.isWhitelisted("health")).isTrue();
    verify(hotKeyCache).isWhitelisted("health");
  }

  @Test
  void isWhitelisted_shouldReturnFalseWhenCacheNull() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThat(workerOnly.isWhitelisted("x")).isFalse();
  }

  // ── peek ──

  @Test
  void peek_shouldReturnMapOfPresentValues() {
    when(hotKeyCache.peek("k1")).thenReturn(Optional.of("v1"));
    when(hotKeyCache.peek("k2")).thenReturn(Optional.empty());
    assertThat(hotKey.peekAll(List.of("k1", "k2"))).containsEntry("k1", "v1").doesNotContainKey("k2");
  }

  @Test
  void peek_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.peekAll(List.of("k"))).isInstanceOf(HotKeyModeException.class);
  }

  // ── invalidateLocal ──

  @Test
  void invalidateLocal_shouldDelegateToCache() {
    hotKey.invalidate("key1", false);
    verify(hotKeyCache).invalidate("key1", false);
  }

  @Test
  void invalidateLocal_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.invalidate("k", false)).isInstanceOf(HotKeyModeException.class);
  }

  // ── areLocalHotKeys ──

  @Test
  void areLocalHotKeys_shouldDelegateToCache() {
    when(hotKeyCache.isHot("k1")).thenReturn(true);
    when(hotKeyCache.isHot("k2")).thenReturn(false);
    assertThat(hotKey.areLocalHotKeys(List.of("k1", "k2"))).containsEntry("k1", true).containsEntry("k2", false);
  }

  @Test
  void areLocalHotKeys_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.areLocalHotKeys(List.of("k"))).isInstanceOf(HotKeyModeException.class);
  }

  // ── areWorkerHotKeys ──

  @Test
  void areWorkerHotKeys_shouldReturnMap() {
    when(workerTopK.contains("k1")).thenReturn(true);
    when(workerTopK.contains("k2")).thenReturn(false);
    assertThat(hotKey.areWorkerHotKeys(List.of("k1", "k2"))).containsEntry("k1", true).containsEntry("k2", false);
  }

  @Test
  void areWorkerHotKeys_whenWorkerTopKNull_shouldReturnAllFalse() {
    HotKey hk = new HotKey(hotKeyCache, appDetector, null);
    assertThat(hk.areWorkerHotKeys(List.of("k1"))).containsEntry("k1", false);
  }

  // ── refresh ──

  @Test
  void refresh_shouldEvictAndPutThrough() {
    hotKey.refresh("k1", () -> "v");
    verify(hotKeyCache).invalidate("k1", false);
    verify(hotKeyCache).putThrough(eq("k1"), eq("v"), any(), anyLong(), anyLong(), anyBoolean());
  }

  @Test
  void refresh_withTtl_shouldEvictAndPutThroughWithTtl() {
    hotKey.refresh("k1", () -> "v", 5000L, 500L);
    verify(hotKeyCache).invalidate("k1", false);
    verify(hotKeyCache).putThrough(eq("k1"), eq("v"), any(), eq(5000L), eq(500L), anyBoolean());
  }

  @Test
  void refresh_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.refresh("k", () -> "v")).isInstanceOf(HotKeyModeException.class);
  }

  // ── refreshAll ──

  @Test
  void refreshAll_shouldRefreshEachKey() {
    hotKey.refreshAll(Map.of("k1", (Supplier<?>) () -> "v1", "k2", (Supplier<?>) () -> "v2"));
    verify(hotKeyCache, times(2)).invalidate(anyString(), eq(false));
    verify(hotKeyCache, times(2)).putThrough(anyString(), any(), any(), anyLong(), anyLong(), anyBoolean());
  }

  // ── invalidateAfterPut ──

  @Test
  void invalidateAfterPut_shouldDelegateToCache() {
    hotKey.invalidateAfterPut(Map.of("k1", () -> {}, "k2", () -> {}));
    verify(hotKeyCache).invalidateAfterPut(anyMap(), anyBoolean());
  }

  @Test
  void invalidateAfterPut_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.invalidateAfterPut(Map.of("k", () -> {}))).isInstanceOf(
      HotKeyModeException.class
    );
  }

  // ── addBlacklist(Collection) / removeBlacklist(Collection) ──

  @Test
  void addBlacklist_collection_shouldDelegateToCache() {
    hotKey.addBlacklist(List.of("a", "b"));
    verify(hotKeyCache).addBlacklist("a");
    verify(hotKeyCache).addBlacklist("b");
  }

  @Test
  void addBlacklist_collection_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.addBlacklist(List.of("x"))).isInstanceOf(HotKeyModeException.class);
  }

  @Test
  void removeBlacklist_collection_shouldDelegateToCache() {
    hotKey.removeBlacklist(List.of("a", "b"));
    verify(hotKeyCache).unBlacklist("a");
    verify(hotKeyCache).unBlacklist("b");
  }

  @Test
  void removeBlacklist_collection_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.removeBlacklist(List.of("x"))).isInstanceOf(HotKeyModeException.class);
  }

  // ── addWhitelist(Collection) / removeWhitelist(Collection) ──

  @Test
  void addWhitelist_collection_shouldDelegateToCache() {
    hotKey.addWhitelist(List.of("a", "b"));
    verify(hotKeyCache).addWhitelist("a");
    verify(hotKeyCache).addWhitelist("b");
  }

  @Test
  void addWhitelist_collection_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.addWhitelist(List.of("x"))).isInstanceOf(HotKeyModeException.class);
  }

  @Test
  void removeWhitelist_collection_shouldDelegateToCache() {
    hotKey.removeWhitelist(List.of("a", "b"));
    verify(hotKeyCache).unWhitelist("a");
    verify(hotKeyCache).unWhitelist("b");
  }

  @Test
  void removeWhitelist_collection_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.removeWhitelist(List.of("x"))).isInstanceOf(HotKeyModeException.class);
  }

  // ── evaluateRules(Collection) ──

  @Test
  void evaluateRules_shouldReturnMap() {
    when(hotKeyCache.evaluateRule("k1")).thenReturn(RuleAction.BLOCK);
    when(hotKeyCache.evaluateRule("k2")).thenReturn(RuleAction.ALLOW);
    assertThat(hotKey.evaluateRules(List.of("k1", "k2")))
      .containsEntry("k1", RuleAction.BLOCK)
      .containsEntry("k2", RuleAction.ALLOW);
  }

  @Test
  void evaluateRules_whenCacheNull_shouldReturnAllAllow() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThat(workerOnly.evaluateRules(List.of("k1", "k2")))
      .containsEntry("k1", RuleAction.ALLOW)
      .containsEntry("k2", RuleAction.ALLOW);
  }

  // ── isBlacklisted(Collection) / isWhitelisted(Collection) ──

  @Test
  void isBlacklisted_collection_shouldReturnMap() {
    when(hotKeyCache.isBlacklisted("k1")).thenReturn(true);
    when(hotKeyCache.isBlacklisted("k2")).thenReturn(false);
    assertThat(hotKey.isBlacklisted(List.of("k1", "k2"))).containsEntry("k1", true).containsEntry("k2", false);
  }

  @Test
  void isBlacklisted_collection_whenCacheNull_shouldReturnAllFalse() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThat(workerOnly.isBlacklisted(List.of("k1"))).containsEntry("k1", false);
  }

  @Test
  void isWhitelisted_collection_shouldReturnMap() {
    when(hotKeyCache.isWhitelisted("k1")).thenReturn(true);
    when(hotKeyCache.isWhitelisted("k2")).thenReturn(false);
    assertThat(hotKey.isWhitelisted(List.of("k1", "k2"))).containsEntry("k1", true).containsEntry("k2", false);
  }

  @Test
  void isWhitelisted_collection_whenCacheNull_shouldReturnAllFalse() {
    HotKey workerOnly = new HotKey(null, null, workerTopK);
    assertThat(workerOnly.isWhitelisted(List.of("k1"))).containsEntry("k1", false);
  }

  // ── registerRefresh / unregisterRefresh ────────────────────────

  @Test
  void unregisterRefresh_shouldNotThrow() throws InterruptedException {
    CountDownLatch firstCallLatch = new CountDownLatch(1);
    when(hotKeyCache.getWithSoftExpire(eq("cancel-key"), any(), anyLong(), anyLong(), anyBoolean())).thenAnswer(
      invocation -> {
        firstCallLatch.countDown();
        return Optional.of("v");
      }
    );

    hotKey.registerRefresh("cancel-key", () -> "v", 300_000L, 10L);
    assertThat(firstCallLatch.await(5, TimeUnit.SECONDS)).as("first scheduled refresh occurred").isTrue();

    hotKey.unregisterRefresh("cancel-key");

    verify(hotKeyCache, atLeastOnce()).getWithSoftExpire(eq("cancel-key"), any(), eq(300_000L), eq(10L), anyBoolean());
  }

  @Test
  void registerRefresh_replacesExistingRegistration() throws InterruptedException {
    AtomicInteger callCount = new AtomicInteger(0);
    when(hotKeyCache.getWithSoftExpire(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenAnswer(
      invocation -> {
        callCount.incrementAndGet();
        return Optional.of("v");
      }
    );

    hotKey.registerRefresh("dup-key", () -> "v1", 300_000L, 5L);
    Thread.sleep(20);
    hotKey.registerRefresh("dup-key", () -> "v2", 300_000L, 5L);

    int countBefore = callCount.get();
    Thread.sleep(30);
    int countAfter = callCount.get();

    assertThat(countAfter).isGreaterThan(countBefore);
  }

  @Test
  void registerRefresh_invokesGetWithSoftExpire() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    when(hotKeyCache.getWithSoftExpire(eq("k1"), any(), anyLong(), anyLong(), anyBoolean())).thenAnswer(invocation -> {
      latch.countDown();
      return Optional.of("v");
    });

    hotKey.registerRefresh("k1", () -> "v", 300_000L, 5L);
    assertThat(latch.await(5, TimeUnit.SECONDS)).as("getWithSoftExpire was invoked by scheduled refresh").isTrue();

    hotKey.unregisterRefresh("k1");
  }

  @Test
  void destroy_shouldNotThrow() {
    hotKey.registerRefresh("k1", () -> "v", 300_000L, 10_000L);
    hotKey.destroy();
    verify(hotKeyCache, never()).getWithSoftExpire(eq("k1"), any(), anyLong(), anyLong(), anyBoolean());
  }
}
