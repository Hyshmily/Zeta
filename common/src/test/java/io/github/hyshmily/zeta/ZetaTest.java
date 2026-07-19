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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.exception.ZetaBlockedException;
import io.github.hyshmily.zeta.exception.ZetaModeException;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.model.ZetaCacheStats;
import io.github.hyshmily.zeta.rule.Rule;
import io.github.hyshmily.zeta.rule.Rule.RuleAction;
import java.util.Collection;
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

class ZetaTest {

  private HotKeyCache hotKeyCache;
  private HotKeyDetector appDetector;
  private TopK workerTopK;
  private Zeta zeta;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    hotKeyCache = mock(HotKeyCache.class);
    appDetector = mock(HotKeyDetector.class);
    workerTopK = mock(TopK.class);
    zeta = new Zeta(hotKeyCache, appDetector, workerTopK);
  }

  @Test
  void isLocalZeta_shouldDelegateToCache() {
    when(hotKeyCache.isHot("key1")).thenReturn(true);
    assertThat(zeta.isLocalHotKey("key1")).isTrue();
    verify(hotKeyCache).isHot("key1");
  }

  @Test
  void peek_shouldDelegateToCache() {
    when(hotKeyCache.peek("key1")).thenReturn(Optional.of("value"));
    assertThat(zeta.peek("key1")).contains("value");
    verify(hotKeyCache).peek("key1");
  }

  @Test
  void get_shouldDelegateToCache() {
    when(hotKeyCache.get(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(Optional.of("value"));
    assertThat(zeta.get("key1", () -> "loaded")).contains("value");
    verify(hotKeyCache).get(anyString(), any(), anyLong(), anyLong(), anyBoolean());
  }

  @Test
  void get_withTtl_shouldDelegateToCache() {
    when(hotKeyCache.get(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(Optional.of("v"));
    assertThat(zeta.get("key1", () -> "loaded", 1000L, 100L)).contains("v");
    verify(hotKeyCache).get(anyString(), any(), anyLong(), anyLong(), anyBoolean());
  }

  @Test
  void getWithSoftExpire_shouldDelegateToCache() {
    when(hotKeyCache.getWithSoftExpire(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(
      Optional.of("v")
    );
    assertThat(zeta.getWithSoftExpire("key1", () -> "v")).contains("v");
    verify(hotKeyCache).getWithSoftExpire(anyString(), any(), anyLong(), anyLong(), anyBoolean());
  }

  @Test
  void invalidate_shouldDelegateToCache() {
    zeta.invalidate("key1");
    verify(hotKeyCache).invalidate("key1", true);
  }

  @Test
  void invalidate_varargs_shouldDelegateToCache() {
    zeta.invalidate(List.of("k1", "k2"));
    verify(hotKeyCache).invalidate(List.of("k1", "k2"), true);
  }

  @Test
  void putThrough_shouldDelegateToCache() {
    zeta.putThrough("key1", "value", () -> {});
    verify(hotKeyCache).putThrough(anyString(), any(), any(), anyLong(), anyLong(), anyBoolean());
  }

  @Test
  void putThrough_withTtl_shouldDelegateToCache() {
    zeta.putThrough("key1", "value", () -> {}, 2000L, 200L, true);
    verify(hotKeyCache).putThrough(anyString(), any(), any(), anyLong(), anyLong(), anyBoolean());
  }

  @Test
  void invalidateAfterPut_single_shouldDelegateToCache() {
    zeta.invalidateAfterPut("key1", () -> {});
    verify(hotKeyCache).invalidateAfterPut(anyString(), any(), anyBoolean());
  }

  @Test
  void returnHotKeys_shouldReturnLocalFromTopK() {
    when(appDetector.list()).thenReturn(List.of(new Item("k1", 10)));
    assertThat(zeta.returnLocalHotKeys()).hasSize(1);
  }

  @Test
  void returnHotKeys_shouldReturnLocalEmptyWhenTopKNull() {
    Zeta hk = new Zeta(hotKeyCache, null, null);
    assertThat(hk.returnLocalHotKeys()).isEmpty();
  }

  @Test
  void returnWorkerHotKeys_shouldReturnFromWorkerTopK() {
    when(workerTopK.list()).thenReturn(List.of(new Item("k1", 5)));
    assertThat(zeta.returnWorkerHotKeys()).hasSize(1);
  }

  @Test
  void returnWorkerHotKeys_shouldReturnEmptyWhenWorkerTopKNull() {
    assertThat(new Zeta(hotKeyCache, appDetector, null).returnWorkerHotKeys()).isEmpty();
  }

  @Test
  void returnTotalDataStreams_shouldReturnLocalFromTopK() {
    when(appDetector.total()).thenReturn(100L);
    assertThat(zeta.returnLocalTotalDataStreams()).isEqualTo(100L);
  }

  @Test
  void returnTotalDataStreams_shouldReturnLocalZeroWhenTopKNull() {
    assertThat(new Zeta(hotKeyCache, null, null).returnLocalTotalDataStreams()).isZero();
  }

  @Test
  void returnExpelledHotKeys_shouldReturnLocalFromTopK() {
    LinkedBlockingQueue<Item> queue = new LinkedBlockingQueue<>();
    queue.add(new Item("k1", 5));
    when(appDetector.expelled()).thenReturn(queue);
    assertThat(zeta.returnLocalExpelledHotKeys()).hasSize(1);
  }

  @Test
  void cacheMethods_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.get("k", () -> "v")).isInstanceOf(ZetaModeException.class);
    assertThatThrownBy(() -> workerOnly.isLocalHotKey("k")).isInstanceOf(ZetaModeException.class);
    assertThatThrownBy(() -> workerOnly.peek("k")).isInstanceOf(ZetaModeException.class);
    assertThatThrownBy(() -> workerOnly.invalidate("k")).isInstanceOf(ZetaModeException.class);
    assertThatThrownBy(() -> workerOnly.putThrough("k", "v", () -> {})).isInstanceOf(ZetaModeException.class);
    assertThatThrownBy(() -> workerOnly.invalidateAfterPut("k", () -> {})).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void get_shouldPropagateZetaBlockedException() {
    when(hotKeyCache.get(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenThrow(
      new ZetaBlockedException("HotKeyCache", "secret")
    );
    assertThatThrownBy(() -> zeta.get("secret", () -> "v")).isInstanceOf(ZetaBlockedException.class);
  }

  // ── Additional getWithSoftExpire overloads ──

  @Test
  void getWithSoftExpire_withSoftTtl_shouldDelegateToCache() {
    when(hotKeyCache.getWithSoftExpire(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(
      Optional.of("v")
    );
    assertThat(zeta.getWithSoftExpire("key1", () -> "v", 200L)).contains("v");
    verify(hotKeyCache).getWithSoftExpire(anyString(), any(), anyLong(), anyLong(), anyBoolean());
  }

  @Test
  void getWithSoftExpire_withHardSoftTtl_shouldDelegateToCache() {
    when(hotKeyCache.getWithSoftExpire(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(
      Optional.of("v")
    );
    assertThat(zeta.getWithSoftExpire("key1", () -> "v", 5000L, 500L, true)).contains("v");
    verify(hotKeyCache).getWithSoftExpire(anyString(), any(), anyLong(), anyLong(), anyBoolean());
  }

  // ── isWorkerHotKey ──

  @Test
  void isWorkerZeta_shouldDelegateToWorkerTopK() {
    when(workerTopK.contains("hot-key")).thenReturn(true);
    assertThat(zeta.isWorkerHotKey("hot-key")).isTrue();
    verify(workerTopK).contains("hot-key");
  }

  @Test
  void isWorkerZeta_shouldReturnFalseWhenNotInWorkerTopK() {
    when(workerTopK.contains("cold-key")).thenReturn(false);
    assertThat(zeta.isWorkerHotKey("cold-key")).isFalse();
  }

  @Test
  void isWorkerZeta_shouldReturnFalseWhenWorkerTopKNull() {
    Zeta hk = new Zeta(hotKeyCache, appDetector, null);
    assertThat(hk.isWorkerHotKey("any")).isFalse();
  }

  @Test
  void isWorkerZeta() {
    assertThat(zeta.isWorkerHotKey(null)).isFalse();
    verifyNoInteractions(workerTopK);
  }

  // ── returnWorkerExpelledHotKeys ──

  @Test
  void returnWorkerExpelledHotKeys_shouldReturnFromWorkerTopK() {
    LinkedBlockingQueue<Item> queue = new LinkedBlockingQueue<>();
    queue.add(new Item("wk1", 3));
    when(workerTopK.expelled()).thenReturn(queue);
    assertThat(zeta.returnWorkerExpelledHotKeys()).hasSize(1);
  }

  @Test
  void returnWorkerExpelledHotKeys_shouldReturnEmptyQueueWhenWorkerTopKNull() {
    Zeta hk = new Zeta(hotKeyCache, appDetector, null);
    assertThat(hk.returnWorkerExpelledHotKeys()).isEmpty();
  }

  // ── returnWorkerTotalDataStreams ──

  @Test
  void returnWorkerTotalDataStreams_shouldReturnFromWorkerTopK() {
    when(workerTopK.total()).thenReturn(999L);
    assertThat(zeta.returnWorkerTotalDataStreams()).isEqualTo(999L);
  }

  @Test
  void returnWorkerTotalDataStreams_shouldReturnZeroWhenWorkerTopKNull() {
    Zeta hk = new Zeta(hotKeyCache, appDetector, null);
    assertThat(hk.returnWorkerTotalDataStreams()).isZero();
  }

  // ── returnLocalExpelledHotKeys null guard ──

  @Test
  void returnExpelledHotKeys_shouldReturnEmptyQueueWhenTopKNull() {
    Zeta hk = new Zeta(hotKeyCache, null, null);
    assertThat(hk.returnLocalExpelledHotKeys()).isEmpty();
  }

  // ── returnLocalTotalDataStreams null guard (full 3-arg ctor) ──

  @Test
  void returnTotalDataStreams_shouldReturnLocalZeroWhenTopKNullThreeArg() {
    Zeta hk = new Zeta(hotKeyCache, null, workerTopK);
    assertThat(hk.returnLocalTotalDataStreams()).isZero();
  }

  // ── getLocalCache ──

  @SuppressWarnings("unchecked")
  @Test
  void getLocalCache_shouldDelegateToCache() {
    Cache<String, Object> caffeine = mock(Cache.class);
    when(hotKeyCache.getLocalCache()).thenReturn(caffeine);
    assertThat(zeta.getLocalCache()).isSameAs(caffeine);
    verify(hotKeyCache).getLocalCache();
  }

  @Test
  void getLocalCache_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThatThrownBy(workerOnly::getLocalCache).isInstanceOf(ZetaModeException.class);
  }

  // ── addBlacklist / removeBlacklist / addWhitelist / removeWhitelist ──

  @Test
  void addBlacklist_shouldDelegateToCache() {
    zeta.addBlacklist("secret-*");
    verify(hotKeyCache).addBlacklist("secret-*");
  }

  @Test
  void addBlacklist_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.addBlacklist("x")).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void removeBlacklist_shouldDelegateToCache() {
    zeta.removeBlacklist("old-rule");
    verify(hotKeyCache).unBlacklist("old-rule");
  }

  @Test
  void removeBlacklist_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.removeBlacklist("x")).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void addWhitelist_shouldDelegateToCache() {
    zeta.addWhitelist("health-*");
    verify(hotKeyCache).addWhitelist("health-*");
  }

  @Test
  void addWhitelist_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.addWhitelist("x")).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void removeWhitelist_shouldDelegateToCache() {
    zeta.removeWhitelist("health-*");
    verify(hotKeyCache).unWhitelist("health-*");
  }

  @Test
  void removeWhitelist_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.removeWhitelist("x")).isInstanceOf(ZetaModeException.class);
  }

  // ── getAllRules ──

  @Test
  void getAllRules_shouldDelegateToCache() {
    Rule rule = new Rule(Rule.RuleType.EXACT, "blocked", RuleAction.BLOCK);
    when(hotKeyCache.getAllRules()).thenReturn(List.of(rule));
    assertThat(zeta.getAllRules()).containsExactly(rule);
    verify(hotKeyCache).getAllRules();
  }

  @Test
  void getAllRules_shouldReturnEmptyWhenCacheNull() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThat(workerOnly.getAllRules()).isEmpty();
  }

  // ── evaluateRule ──

  @Test
  void evaluateRule_shouldDelegateToCache() {
    when(hotKeyCache.evaluateRule("secret")).thenReturn(RuleAction.BLOCK);
    assertThat(zeta.evaluateRule("secret")).isEqualTo(RuleAction.BLOCK);
    verify(hotKeyCache).evaluateRule("secret");
  }

  @Test
  void evaluateRule_shouldReturnAllowWhenCacheNull() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThat(workerOnly.evaluateRule("any")).isEqualTo(RuleAction.ALLOW);
  }

  // ── clearAllRules ──

  @Test
  void clearAllRules_shouldDelegateToCache() {
    zeta.clearAllRules();
    verify(hotKeyCache).clearAllRules();
  }

  @Test
  void clearAllRules_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThatThrownBy(workerOnly::clearAllRules).isInstanceOf(ZetaModeException.class);
  }

  // ── broadcastAllLocalRulesManually ──

  @Test
  void broadcastAllLocalRulesManually_shouldDelegateToCache() {
    zeta.broadcastAllLocalRulesManually();
    verify(hotKeyCache).broadcastAllLocalRulesManually();
  }

  @Test
  void broadcastAllLocalRulesManually_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThatThrownBy(workerOnly::broadcastAllLocalRulesManually).isInstanceOf(ZetaModeException.class);
  }

  // ── notifyLocalDetector ──

  @Test
  void notifyLocalDetector_shouldDelegateToTopK() {
    zeta.notifyLocalDetector("my-key");
    verify(appDetector).add("my-key");
  }

  @Test
  void notifyLocalDetector_shouldIgnoreNullKey() {
    zeta.notifyLocalDetector((String) null);
    verify(appDetector, never()).add(anyString());
  }

  // ── read / write factories ──

  @Test
  void read_shouldReturnZetaReadQuery() {
    when(hotKeyCache.evaluateRule("k")).thenReturn(RuleAction.ALLOW);
    when(hotKeyCache.get(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(Optional.of("v"));
    assertThat(
      zeta
        .read("k")
        .withPrimary(() -> "db")
        .execute()
    ).contains("v");
  }

  @Test
  void write_shouldReturnZetaWriteCommand() {
    zeta.write("k").invalidate();
    verify(hotKeyCache).invalidate("k", true);
  }

  // ── computeIfAbsent single-key ──

  @Test
  void computeIfAbsent_shouldReturnValue() {
    when(hotKeyCache.computeIfAbsent(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(
      Optional.of("v")
    );
    assertThat(zeta.computeIfAbsent("k", () -> "loaded")).isEqualTo("v");
    verify(hotKeyCache).computeIfAbsent(eq("k"), any(), anyLong(), anyLong(), anyBoolean());
  }

  @Test
  void computeIfAbsent_shouldReturnNullWhenLoaderNull() {
    when(hotKeyCache.computeIfAbsent(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(
      Optional.empty()
    );
    assertThat((Object) zeta.computeIfAbsent("k", () -> null)).isNull();
  }

  @Test
  void computeIfAbsent_withHardTtl_shouldDelegate() {
    when(hotKeyCache.computeIfAbsent(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(
      Optional.of("v")
    );
    assertThat(zeta.computeIfAbsent("k", () -> "db", 5000L)).isEqualTo("v");
    verify(hotKeyCache).computeIfAbsent(eq("k"), any(), eq(5000L), eq(0L), anyBoolean());
  }

  @Test
  void computeIfAbsent_withHardSoftTtl_shouldDelegate() {
    when(hotKeyCache.computeIfAbsent(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(
      Optional.of("v")
    );
    assertThat(zeta.computeIfAbsent("k", () -> "db", 5000L, 500L)).isEqualTo("v");
    verify(hotKeyCache).computeIfAbsent(eq("k"), any(), eq(5000L), eq(500L), anyBoolean());
  }

  @Test
  void computeIfAbsent_withNonDefaultReport_shouldDelegate() {
    when(hotKeyCache.computeIfAbsent(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(
      Optional.of("v")
    );
    assertThat(zeta.computeIfAbsent("k", () -> "db", 0L, 0L, false)).isEqualTo("v");
    verify(hotKeyCache).computeIfAbsent(eq("k"), any(), eq(0L), eq(0L), eq(false));
  }

  @Test
  void computeIfAbsentWithSoftExpire_SoftTtl_shouldDelegate() {
    when(hotKeyCache.computeIfAbsentWithSoftExpire(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(
      Optional.of("v")
    );
    assertThat(zeta.computeIfAbsentWithSoftExpire("k", () -> "db", 500L)).isEqualTo("v");
    verify(hotKeyCache).computeIfAbsentWithSoftExpire(eq("k"), any(), eq(0L), eq(500L), anyBoolean());
  }

  @Test
  void computeIfAbsentWithSoftExpire_collectionSoftTtl_shouldDelegate() {
    when(hotKeyCache.computeIfAbsentWithSoftExpire(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(
      Optional.of("v")
    );
    assertThat(zeta.computeIfAbsentWithSoftExpire("k", () -> "db", 500L)).isEqualTo("v");
    verify(hotKeyCache).computeIfAbsentWithSoftExpire(eq("k"), any(), eq(0L), eq(500L), anyBoolean());
  }

  @Test
  void computeIfAbsentWithSoftExpire_BothTtls_shouldDelegate() {
    when(hotKeyCache.computeIfAbsentWithSoftExpire(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(
      Optional.of("v")
    );
    assertThat(zeta.computeIfAbsentWithSoftExpire("k", () -> "db", 5000L, 500L)).isEqualTo("v");
    verify(hotKeyCache).computeIfAbsentWithSoftExpire(eq("k"), any(), eq(5000L), eq(500L), anyBoolean());
  }

  @Test
  void computeIfAbsentWithSoftExpire_BothTtlsAndReport_shouldDelegate() {
    when(hotKeyCache.computeIfAbsentWithSoftExpire(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(
      Optional.of("v")
    );
    assertThat(zeta.computeIfAbsentWithSoftExpire("k", () -> "db", 5000L, 500L, true)).isEqualTo("v");
    verify(hotKeyCache).computeIfAbsentWithSoftExpire(eq("k"), any(), eq(5000L), eq(500L), eq(true));
  }

  // ── invalidateAllLocal no-arg ──

  @Test
  void invalidateAllLocal_shouldDelegateToCache() {
    zeta.invalidateAllLocal();
    verify(hotKeyCache).invalidateAllLocal();
  }

  @Test
  void invalidateAllLocal_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThatThrownBy(workerOnly::invalidateAllLocal).isInstanceOf(ZetaModeException.class);
  }

  // ── invalidate Collection ──

  @Test
  void invalidate_collection_shouldDelegateToCache() {
    zeta.invalidate(List.of("k1", "k2"));
    verify(hotKeyCache).invalidate(List.of("k1", "k2"), true);
  }

  // ── putLocal ──

  @Test
  void putLocal_shouldDelegateToCache() {
    zeta.putLocal("k", "v");
    verify(hotKeyCache).putLocal("k", "v", 0L, 0L);
  }

  @Test
  void putLocal_withTtl_shouldDelegateToCache() {
    zeta.putLocal("k", "v", 5000L, 500L);
    verify(hotKeyCache).putLocal("k", "v", 5000L, 500L);
  }

  // ── compareAndSet / compareAndInvalidate ──

  @Test
  void compareAndSet_shouldDelegateToCache() {
    when(hotKeyCache.compareAndSet(eq("k"), eq("old"), eq("new"))).thenReturn(true);
    assertThat(zeta.compareAndSet("k", "old", "new")).isTrue();
    verify(hotKeyCache).compareAndSet("k", "old", "new");
  }

  @Test
  void compareAndSet_shouldReturnFalseOnMismatch() {
    when(hotKeyCache.compareAndSet(eq("k"), eq("wrong"), eq("new"))).thenReturn(false);
    assertThat(zeta.compareAndSet("k", "wrong", "new")).isFalse();
    verify(hotKeyCache).compareAndSet("k", "wrong", "new");
  }

  @Test
  void compareAndSet_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.compareAndSet("k", "old", "new")).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void compareAndInvalidate_shouldDelegateToCache() {
    when(hotKeyCache.compareAndInvalidate(eq("k"), eq("old"))).thenReturn(true);
    assertThat(zeta.compareAndInvalidate("k", "old")).isTrue();
    verify(hotKeyCache).compareAndInvalidate("k", "old");
  }

  @Test
  void compareAndInvalidate_shouldReturnFalseOnMismatch() {
    when(hotKeyCache.compareAndInvalidate(eq("k"), eq("wrong"))).thenReturn(false);
    assertThat(zeta.compareAndInvalidate("k", "wrong")).isFalse();
    verify(hotKeyCache).compareAndInvalidate("k", "wrong");
  }

  @Test
  void compareAndInvalidate_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.compareAndInvalidate("k", "old")).isInstanceOf(ZetaModeException.class);
  }

  // ── getAndSet / putIfAbsent ──

  @Test
  void getAndSet_shouldDelegateToCache() {
    @SuppressWarnings("unchecked")
    Optional<Object> old = (Optional) Optional.of("old");
    when(hotKeyCache.getAndSet(eq("k"), eq("new"), eq(0L), eq(0L))).thenReturn(old);
    assertThat(zeta.getAndSet("k", "new", 0L, 0L)).isSameAs(old);
    verify(hotKeyCache).getAndSet("k", "new", 0L, 0L);
  }

  @Test
  void getAndSet_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.getAndSet("k", "v", 0L, 0L)).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void putIfAbsent_shouldDelegateToCache() {
    when(hotKeyCache.putIfAbsent(eq("k"), eq("v"), eq(0L), eq(0L))).thenReturn(true);
    assertThat(zeta.putIfAbsent("k", "v", 0L, 0L)).isTrue();
    verify(hotKeyCache).putIfAbsent("k", "v", 0L, 0L);
  }

  @Test
  void putIfAbsent_shouldReturnFalseWhenPresent() {
    when(hotKeyCache.putIfAbsent(eq("k"), eq("v"), eq(0L), eq(0L))).thenReturn(false);
    assertThat(zeta.putIfAbsent("k", "v", 0L, 0L)).isFalse();
    verify(hotKeyCache).putIfAbsent("k", "v", 0L, 0L);
  }

  @Test
  void putIfAbsent_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.putIfAbsent("k", "v", 0L, 0L)).isInstanceOf(ZetaModeException.class);
  }

  // ── estimatedSizeOfKeysCount ──

  @Test
  void estimatedSize_shouldDelegateToCache() {
    when(hotKeyCache.estimatedSize()).thenReturn(42L);
    assertThat(zeta.estimatedSize()).isEqualTo(42L);
    verify(hotKeyCache).estimatedSize();
  }

  @Test
  void estimatedSize_shouldReturnZeroInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThat(workerOnly.estimatedSize()).isZero();
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
  void stats_shouldReturnNullInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThat(workerOnly.stats()).isNull();
  }

  // ── notifyLocalDetectorDirect ──

  @Test
  void notifyLocalDetectorDirect_shouldDelegateToAppDetector() {
    zeta.notifyLocalDetectorDirect("k", 5);
    verify(appDetector).addDirect("k", 5);
  }

  @Test
  void notifyLocalDetectorDirect_map_shouldDelegateToAppDetector() {
    Map<String, Long> map = Map.of("k", 3L);
    zeta.notifyLocalDetectorDirect(map);
    verify(appDetector).addDirect(map);
  }

  // ── notifyLocalDetector(String, long) and Map ──

  @Test
  void notifyLocalDetector_withDelta_shouldDelegateToAppDetector() {
    zeta.notifyLocalDetector("k", 7);
    verify(appDetector).add("k", 7);
  }

  @Test
  void notifyLocalDetector_map_shouldDelegateToAppDetector() {
    Map<String, Long> map = Map.of("k", 3L);
    zeta.notifyLocalDetector(map);
    verify(appDetector).add(map);
  }

  // ── returnLocalTopNHotKeys ──

  @Test
  void returnLocalTopNHotKeys_shouldDelegateToAppDetector() {
    List<Item> items = List.of(new Item("k", 10));
    when(appDetector.listTopN(3)).thenReturn(items);
    assertThat(zeta.returnLocalTopNHotKeys(3)).isSameAs(items);
    verify(appDetector).listTopN(3);
  }

  @Test
  void returnLocalTopNHotKeys_shouldReturnEmptyWhenDetectorNull() {
    Zeta hk = new Zeta(hotKeyCache, null, workerTopK);
    assertThat(hk.returnLocalTopNHotKeys(5)).isEmpty();
  }

  // ── isBlacklisted ──

  @Test
  void isBlacklisted_shouldDelegateToCache() {
    when(hotKeyCache.isBlacklisted("secret")).thenReturn(true);
    assertThat(zeta.isBlacklisted("secret")).isTrue();
    verify(hotKeyCache).isBlacklisted("secret");
  }

  @Test
  void isBlacklisted_shouldReturnFalseWhenCacheNull() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThat(workerOnly.isBlacklisted("x")).isFalse();
  }

  // ── isWhitelisted ──

  @Test
  void isWhitelisted_shouldDelegateToCache() {
    when(hotKeyCache.isWhitelisted("health")).thenReturn(true);
    assertThat(zeta.isWhitelisted("health")).isTrue();
    verify(hotKeyCache).isWhitelisted("health");
  }

  @Test
  void isWhitelisted_shouldReturnFalseWhenCacheNull() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThat(workerOnly.isWhitelisted("x")).isFalse();
  }

  // ── peek ──

  @Test
  void peek_shouldReturnMapOfPresentValues() {
    when(hotKeyCache.peek("k1")).thenReturn(Optional.of("v1"));
    when(hotKeyCache.peek("k2")).thenReturn(Optional.empty());
    assertThat(zeta.peekAll(List.of("k1", "k2"))).containsEntry("k1", "v1").doesNotContainKey("k2");
  }

  @Test
  void peek_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.peekAll(List.of("k"))).isInstanceOf(ZetaModeException.class);
  }

  // ── invalidateLocal ──

  @Test
  void invalidateLocal_shouldDelegateToCache() {
    zeta.invalidate("key1", false);
    verify(hotKeyCache).invalidate("key1", false);
  }

  @Test
  void invalidateLocal_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.invalidate("k", false)).isInstanceOf(ZetaModeException.class);
  }

  // ── areLocalHotKeys ──

  @Test
  void areLocalHotKeys_shouldDelegateToCache() {
    when(hotKeyCache.isHot("k1")).thenReturn(true);
    when(hotKeyCache.isHot("k2")).thenReturn(false);
    assertThat(zeta.areLocalHotKeys(List.of("k1", "k2"))).containsEntry("k1", true).containsEntry("k2", false);
  }

  @Test
  void areLocalHotKeys_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.areLocalHotKeys(List.of("k"))).isInstanceOf(ZetaModeException.class);
  }

  // ── areWorkerHotKeys ──

  @Test
  void areWorkerHotKeys_shouldReturnMap() {
    when(workerTopK.contains("k1")).thenReturn(true);
    when(workerTopK.contains("k2")).thenReturn(false);
    assertThat(zeta.areWorkerHotKeys(List.of("k1", "k2"))).containsEntry("k1", true).containsEntry("k2", false);
  }

  @Test
  void areWorkerHotKeys_whenWorkerTopKNull_shouldReturnAllFalse() {
    Zeta hk = new Zeta(hotKeyCache, appDetector, null);
    assertThat(hk.areWorkerHotKeys(List.of("k1"))).containsEntry("k1", false);
  }

  // ── refresh ──

  @Test
  void refresh_shouldEvictAndPutThrough() {
    zeta.refresh("k1", () -> "v");
    verify(hotKeyCache).invalidate("k1", false);
    verify(hotKeyCache).putThrough(eq("k1"), eq("v"), any(), anyLong(), anyLong(), anyBoolean());
  }

  @Test
  void refresh_withTtl_shouldEvictAndPutThroughWithTtl() {
    zeta.refresh("k1", () -> "v", 5000L, 500L);
    verify(hotKeyCache).invalidate("k1", false);
    verify(hotKeyCache).putThrough(eq("k1"), eq("v"), any(), eq(5000L), eq(500L), anyBoolean());
  }

  @Test
  void refresh_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.refresh("k", () -> "v")).isInstanceOf(ZetaModeException.class);
  }

  // ── refreshAll ──

  @Test
  void refreshAll_shouldRefreshEachKey() {
    zeta.refreshAll(Map.of("k1", (Supplier<?>) () -> "v1", "k2", (Supplier<?>) () -> "v2"));
    verify(hotKeyCache, times(2)).invalidate(anyString(), eq(false));
    verify(hotKeyCache, times(2)).putThrough(anyString(), any(), any(), anyLong(), anyLong(), anyBoolean());
  }

  // ── invalidateAfterPut ──

  @Test
  void invalidateAfterPut_shouldDelegateToCache() {
    zeta.invalidateAfterPut(Map.of("k1", () -> {}, "k2", () -> {}));
    verify(hotKeyCache).invalidateAfterPut(anyMap(), anyBoolean());
  }

  @Test
  void invalidateAfterPut_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.invalidateAfterPut(Map.of("k", () -> {}))).isInstanceOf(
      ZetaModeException.class
    );
  }

  // ── addBlacklist(Collection) / removeBlacklist(Collection) ──

  @Test
  void addBlacklist_collection_shouldDelegateToCache() {
    zeta.addBlacklist(List.of("a", "b"));
    verify(hotKeyCache).addBlacklist("a");
    verify(hotKeyCache).addBlacklist("b");
  }

  @Test
  void addBlacklist_collection_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.addBlacklist(List.of("x"))).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void removeBlacklist_collection_shouldDelegateToCache() {
    zeta.removeBlacklist(List.of("a", "b"));
    verify(hotKeyCache).unBlacklist("a");
    verify(hotKeyCache).unBlacklist("b");
  }

  @Test
  void removeBlacklist_collection_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.removeBlacklist(List.of("x"))).isInstanceOf(ZetaModeException.class);
  }

  // ── addWhitelist(Collection) / removeWhitelist(Collection) ──

  @Test
  void addWhitelist_collection_shouldDelegateToCache() {
    zeta.addWhitelist(List.of("a", "b"));
    verify(hotKeyCache).addWhitelist("a");
    verify(hotKeyCache).addWhitelist("b");
  }

  @Test
  void addWhitelist_collection_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.addWhitelist(List.of("x"))).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void removeWhitelist_collection_shouldDelegateToCache() {
    zeta.removeWhitelist(List.of("a", "b"));
    verify(hotKeyCache).unWhitelist("a");
    verify(hotKeyCache).unWhitelist("b");
  }

  @Test
  void removeWhitelist_collection_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.removeWhitelist(List.of("x"))).isInstanceOf(ZetaModeException.class);
  }

  // ── evaluateRules(Collection) ──

  @Test
  void evaluateRules_shouldReturnMap() {
    when(hotKeyCache.evaluateRule("k1")).thenReturn(RuleAction.BLOCK);
    when(hotKeyCache.evaluateRule("k2")).thenReturn(RuleAction.ALLOW);
    assertThat(zeta.evaluateRules(List.of("k1", "k2")))
      .containsEntry("k1", RuleAction.BLOCK)
      .containsEntry("k2", RuleAction.ALLOW);
  }

  @Test
  void evaluateRules_whenCacheNull_shouldReturnAllAllow() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThat(workerOnly.evaluateRules(List.of("k1", "k2")))
      .containsEntry("k1", RuleAction.ALLOW)
      .containsEntry("k2", RuleAction.ALLOW);
  }

  // ── isBlacklisted(Collection) / isWhitelisted(Collection) ──

  @Test
  void isBlacklisted_collection_shouldReturnMap() {
    when(hotKeyCache.isBlacklisted("k1")).thenReturn(true);
    when(hotKeyCache.isBlacklisted("k2")).thenReturn(false);
    assertThat(zeta.isBlacklisted(List.of("k1", "k2"))).containsEntry("k1", true).containsEntry("k2", false);
  }

  @Test
  void isBlacklisted_collection_whenCacheNull_shouldReturnAllFalse() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThat(workerOnly.isBlacklisted(List.of("k1"))).containsEntry("k1", false);
  }

  @Test
  void isWhitelisted_collection_shouldReturnMap() {
    when(hotKeyCache.isWhitelisted("k1")).thenReturn(true);
    when(hotKeyCache.isWhitelisted("k2")).thenReturn(false);
    assertThat(zeta.isWhitelisted(List.of("k1", "k2"))).containsEntry("k1", true).containsEntry("k2", false);
  }

  @Test
  void isWhitelisted_collection_whenCacheNull_shouldReturnAllFalse() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
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

    zeta.registerRefresh("cancel-key", () -> "v", 300_000L, 10L);
    assertThat(firstCallLatch.await(5, TimeUnit.SECONDS)).as("first scheduled refresh occurred").isTrue();

    zeta.unregisterRefresh("cancel-key");

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

    zeta.registerRefresh("dup-key", () -> "v1", 300_000L, 5L);
    Thread.sleep(20);
    zeta.registerRefresh("dup-key", () -> "v2", 300_000L, 5L);

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

    zeta.registerRefresh("k1", () -> "v", 300_000L, 5L);
    assertThat(latch.await(5, TimeUnit.SECONDS)).as("getWithSoftExpire was invoked by scheduled refresh").isTrue();

    zeta.unregisterRefresh("k1");
  }

  @Test
  void destroy_shouldNotThrow() {
    zeta.registerRefresh("k1", () -> "v", 300_000L, 10_000L);
    zeta.destroy();
    verify(hotKeyCache, never()).getWithSoftExpire(eq("k1"), any(), anyLong(), anyLong(), anyBoolean());
  }

  // ── Parameter validation ──

  @Test
  void read_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.read(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void read_shouldRejectEmptyKey() {
    assertThatThrownBy(() -> zeta.read("")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void write_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.write(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void computeIfAbsent_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.computeIfAbsent(null, () -> "v")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void computeIfAbsent_shouldRejectNullLoader() {
    assertThatThrownBy(() -> zeta.computeIfAbsent("k", null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void computeIfAbsent_shouldRejectNegativeHardTtl() {
    assertThatThrownBy(() -> zeta.computeIfAbsent("k", () -> "v", -1L, 0L, true)).isInstanceOf(
      IllegalArgumentException.class
    );
  }

  @Test
  void computeIfAbsent_shouldRejectNegativeSoftTtl() {
    assertThatThrownBy(() -> zeta.computeIfAbsent("k", () -> "v", 0L, -1L, true)).isInstanceOf(
      IllegalArgumentException.class
    );
  }

  @Test
  void computeIfAbsentWithSoftExpire_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.computeIfAbsentWithSoftExpire(null, () -> "v", 0L, 0L, true)).isInstanceOf(
      IllegalArgumentException.class
    );
  }

  @Test
  void get_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.get(null, () -> "v")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void get_shouldRejectNullReader() {
    assertThatThrownBy(() -> zeta.get("k", null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void getWithSoftExpire_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.getWithSoftExpire(null, () -> "v")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void batchGet_shouldRejectNullKeys() {
    assertThatThrownBy(() -> zeta.get((Iterable<String>) null, k -> "v")).isInstanceOf(NullPointerException.class);
  }

  @Test
  void putThrough_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.putThrough(null, "v", () -> {})).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void putThrough_shouldRejectNullValue() {
    assertThatThrownBy(() -> zeta.putThrough("k", null, () -> {})).isInstanceOf(NullPointerException.class);
  }

  @Test
  void putThrough_shouldRejectNullWriter() {
    assertThatThrownBy(() -> zeta.putThrough("k", "v", null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void putLocal_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.putLocal(null, "v")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void putLocal_shouldRejectNullValue() {
    assertThatThrownBy(() -> zeta.putLocal("k", null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void invalidate_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.invalidate((String) null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void invalidate_shouldRejectEmptyKey() {
    assertThatThrownBy(() -> zeta.invalidate("")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void batchInvalidate_shouldRejectNullKeys() {
    assertThatThrownBy(() -> zeta.invalidate((Collection<String>) null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void invalidateAfterPut_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.invalidateAfterPut(null, () -> {})).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void invalidateAfterPut_shouldRejectNullMutation() {
    assertThatThrownBy(() -> zeta.invalidateAfterPut("k", null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void peek_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.peek(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void peekAll_shouldRejectNullKeys() {
    assertThatThrownBy(() -> zeta.peekAll(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void compareAndSet_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.compareAndSet(null, "old", "new")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void compareAndInvalidate_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.compareAndInvalidate(null, "old")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void tryLock_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.tryLock(null, 10, TimeUnit.SECONDS)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void tryLock_shouldRejectNullUnit() {
    assertThatThrownBy(() -> zeta.tryLock("k", 10, null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void tryLock_shouldRejectNegativeExpire() {
    assertThatThrownBy(() -> zeta.tryLock("k", -1, TimeUnit.SECONDS)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void isLocalHotKey_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.isLocalHotKey(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void registerRefresh_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.registerRefresh(null, () -> "v", 1000L, 100L)).isInstanceOf(
      IllegalArgumentException.class
    );
  }

  @Test
  void registerRefresh_shouldRejectNullSupplier() {
    assertThatThrownBy(() -> zeta.registerRefresh("k", null, 1000L, 100L)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void unregisterRefresh_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.unregisterRefresh(null)).isInstanceOf(IllegalArgumentException.class);
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
  void addBlacklist_shouldRejectNullPattern() {
    assertThatThrownBy(() -> zeta.addBlacklist((String) null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void addBlacklist_shouldRejectEmptyPattern() {
    assertThatThrownBy(() -> zeta.addBlacklist("")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void evaluateRule_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.evaluateRule(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void isBlacklisted_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.isBlacklisted((String) null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void isWhitelisted_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.isWhitelisted((String) null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void areLocalHotKeys_shouldRejectNullKeys() {
    assertThatThrownBy(() -> zeta.areLocalHotKeys(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void areWorkerHotKeys_shouldRejectNullKeys() {
    assertThatThrownBy(() -> zeta.areWorkerHotKeys(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void workerMode_shouldStillRejectNullKey() {
    Zeta workerOnly = new Zeta(null, null, workerTopK);
    assertThatThrownBy(() -> workerOnly.get(null, () -> "v")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void putLocal_shouldRejectNegativeHardTtl() {
    assertThatThrownBy(() -> zeta.putLocal("k", "v", -1L, 0L)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void putThrough_shouldRejectNegativeHardTtl() {
    assertThatThrownBy(() -> zeta.putThrough("k", "v", () -> {}, -1L, 0L, true)).isInstanceOf(
      IllegalArgumentException.class
    );
  }
}
