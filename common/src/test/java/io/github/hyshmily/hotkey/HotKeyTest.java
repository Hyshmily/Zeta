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

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.cache.HotKeyCache;
import io.github.hyshmily.hotkey.exception.HotKeyBlockedException;
import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.Item;
import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.TopK;
import io.github.hyshmily.hotkey.rule.Rule;
import io.github.hyshmily.hotkey.rule.Rule.RuleAction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class HotKeyTest {

  private HotKeyCache hotKeyCache;
  private TopK topK;
  private TopK workerTopK;
  private HotKey hotKey;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    hotKeyCache = mock(HotKeyCache.class);
    topK = mock(TopK.class);
    workerTopK = mock(TopK.class);
    hotKey = new HotKey(hotKeyCache, topK, workerTopK);
  }

  @Test
  void isLocalHotKey_shouldDelegateToCache() {
    when(hotKeyCache.isLocalHotKey("key1")).thenReturn(true);
    assertThat(hotKey.isLocalHotKey("key1")).isTrue();
    verify(hotKeyCache).isLocalHotKey("key1");
  }

  @Test
  void peek_shouldDelegateToCache() {
    when(hotKeyCache.peek("key1")).thenReturn(Optional.of("value"));
    assertThat(hotKey.peek("key1")).contains("value");
    verify(hotKeyCache).peek("key1");
  }

  @Test
  void get_shouldDelegateToCache() {
    when(hotKeyCache.get(anyString(), any())).thenReturn(Optional.of("value"));
    assertThat(hotKey.get("key1", () -> "loaded")).contains("value");
    verify(hotKeyCache).get(anyString(), any());
  }

  @Test
  void get_withTtl_shouldDelegateToCache() {
    when(hotKeyCache.get(anyString(), any(), anyLong(), anyLong())).thenReturn(Optional.of("v"));
    assertThat(hotKey.get("key1", () -> "loaded", 1000L, 100L)).contains("v");
    verify(hotKeyCache).get(anyString(), any(), anyLong(), anyLong());
  }

  @Test
  void getWithSoftExpire_shouldDelegateToCache() {
    when(hotKeyCache.getWithSoftExpire(anyString(), any())).thenReturn(Optional.of("v"));
    assertThat(hotKey.getWithSoftExpire("key1", () -> "v")).contains("v");
    verify(hotKeyCache).getWithSoftExpire(anyString(), any());
  }

  @Test
  void invalidate_shouldDelegateToCache() {
    hotKey.invalidate("key1");
    verify(hotKeyCache).invalidate("key1");
  }

  @Test
  void invalidateAll_varargs_shouldDelegateToCache() {
    hotKey.invalidateAll("k1", "k2");
    verify(hotKeyCache).invalidateAll(List.of("k1", "k2"));
  }

  @Test
  void putThrough_shouldDelegateToCache() {
    hotKey.putThrough("key1", "value", () -> {});
    verify(hotKeyCache).putThrough(anyString(), any(), any());
  }

  @Test
  void putThrough_withTtl_shouldDelegateToCache() {
    hotKey.putThrough("key1", "value", () -> {}, 2000L, 200L);
    verify(hotKeyCache).putThrough(anyString(), any(), any(), anyLong(), anyLong());
  }

  @Test
  void putBeforeInvalidate_shouldDelegateToCache() {
    hotKey.putBeforeInvalidate("key1", () -> {});
    verify(hotKeyCache).putBeforeInvalidate(anyString(), any());
  }

  @Test
  void returnHotKeys_shouldReturnLocalFromTopK() {
    when(topK.list()).thenReturn(List.of(new Item("k1", 10)));
    assertThat(hotKey.returnLocalHotKeys()).hasSize(1);
  }

  @Test
  void returnHotKeys_shouldReturnLocalEmptyWhenTopKNull() {
    HotKey hk = new HotKey(hotKeyCache, null);
    assertThat(hk.returnLocalHotKeys()).isEmpty();
  }

  @Test
  void returnWorkerHotKeys_shouldReturnFromWorkerTopK() {
    when(workerTopK.list()).thenReturn(List.of(new Item("k1", 5)));
    assertThat(hotKey.returnWorkerHotKeys()).hasSize(1);
  }

  @Test
  void returnWorkerHotKeys_shouldReturnEmptyWhenWorkerTopKNull() {
    assertThat(new HotKey(hotKeyCache, topK, null).returnWorkerHotKeys()).isEmpty();
  }

  @Test
  void returnTotalDataStreams_shouldReturnLocalFromTopK() {
    when(topK.total()).thenReturn(100L);
    assertThat(hotKey.returnLocalTotalDataStreams()).isEqualTo(100L);
  }

  @Test
  void returnTotalDataStreams_shouldReturnLocalZeroWhenTopKNull() {
    assertThat(new HotKey(hotKeyCache, null).returnLocalTotalDataStreams()).isZero();
  }

  @Test
  void returnExpelledHotKeys_shouldReturnLocalFromTopK() {
    LinkedBlockingQueue<Item> queue = new LinkedBlockingQueue<>();
    queue.add(new Item("k1", 5));
    when(topK.expelled()).thenReturn(queue);
    assertThat(hotKey.returnLocalExpelledHotKeys()).hasSize(1);
  }

  @Test
  void cacheMethods_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, topK);
    assertThatThrownBy(() -> workerOnly.get("k", () -> "v"))
      .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> workerOnly.isLocalHotKey("k"))
      .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> workerOnly.peek("k"))
      .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> workerOnly.invalidate("k"))
      .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> workerOnly.putThrough("k", "v", () -> {}))
      .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> workerOnly.putBeforeInvalidate("k", () -> {}))
      .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void get_shouldPropagateHotKeyBlockedException() {
    when(hotKeyCache.get(anyString(), any()))
      .thenThrow(new HotKeyBlockedException("HotKeyCache", "secret"));
    assertThatThrownBy(() -> hotKey.get("secret", () -> "v"))
      .isInstanceOf(HotKeyBlockedException.class);
  }

  // ── Additional getWithSoftExpire overloads ──

  @Test
  void getWithSoftExpire_withSoftTtl_shouldDelegateToCache() {
    when(hotKeyCache.getWithSoftExpire(anyString(), any(), anyLong())).thenReturn(Optional.of("v"));
    assertThat(hotKey.getWithSoftExpire("key1", () -> "v", 200L)).contains("v");
    verify(hotKeyCache).getWithSoftExpire(anyString(), any(), anyLong());
  }

  @Test
  void getWithSoftExpire_withHardSoftTtl_shouldDelegateToCache() {
    when(hotKeyCache.getWithSoftExpire(anyString(), any(), anyLong(), anyLong())).thenReturn(Optional.of("v"));
    assertThat(hotKey.getWithSoftExpire("key1", () -> "v", 5000L, 500L)).contains("v");
    verify(hotKeyCache).getWithSoftExpire(anyString(), any(), anyLong(), anyLong());
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
    HotKey hk = new HotKey(hotKeyCache, topK, null);
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
    HotKey hk = new HotKey(hotKeyCache, topK, null);
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
    HotKey hk = new HotKey(hotKeyCache, topK, null);
    assertThat(hk.returnWorkerTotalDataStreams()).isZero();
  }

  // ── returnLocalExpelledHotKeys null guard ──

  @Test
  void returnExpelledHotKeys_shouldReturnEmptyQueueWhenTopKNull() {
    HotKey hk = new HotKey(hotKeyCache, null);
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
    HotKey workerOnly = new HotKey(null, topK);
    assertThatThrownBy(workerOnly::getLocalCache)
      .isInstanceOf(UnsupportedOperationException.class);
  }

  // ── addBlacklist / removeBlacklist / addWhitelist / removeWhitelist ──

  @Test
  void addBlacklist_shouldDelegateToCache() {
    hotKey.addBlacklist("secret-*");
    verify(hotKeyCache).addBlacklist("secret-*");
  }

  @Test
  void addBlacklist_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, topK);
    assertThatThrownBy(() -> workerOnly.addBlacklist("x"))
      .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void removeBlacklist_shouldDelegateToCache() {
    hotKey.removeBlacklist("old-rule");
    verify(hotKeyCache).unBlacklist("old-rule");
  }

  @Test
  void removeBlacklist_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, topK);
    assertThatThrownBy(() -> workerOnly.removeBlacklist("x"))
      .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void addWhitelist_shouldDelegateToCache() {
    hotKey.addWhitelist("health-*");
    verify(hotKeyCache).addWhitelist("health-*");
  }

  @Test
  void addWhitelist_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, topK);
    assertThatThrownBy(() -> workerOnly.addWhitelist("x"))
      .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void removeWhitelist_shouldDelegateToCache() {
    hotKey.removeWhitelist("health-*");
    verify(hotKeyCache).unWhitelist("health-*");
  }

  @Test
  void removeWhitelist_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, topK);
    assertThatThrownBy(() -> workerOnly.removeWhitelist("x"))
      .isInstanceOf(UnsupportedOperationException.class);
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
    HotKey workerOnly = new HotKey(null, topK);
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
    HotKey workerOnly = new HotKey(null, topK);
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
    HotKey workerOnly = new HotKey(null, topK);
    assertThatThrownBy(workerOnly::clearAllRules)
      .isInstanceOf(UnsupportedOperationException.class);
  }

  // ── broadcastAllLocalRulesManually ──

  @Test
  void broadcastAllLocalRulesManually_shouldDelegateToCache() {
    hotKey.broadcastAllLocalRulesManually();
    verify(hotKeyCache).broadcastAllLocalRulesManually();
  }

  @Test
  void broadcastAllLocalRulesManually_shouldThrowInWorkerMode() {
    HotKey workerOnly = new HotKey(null, topK);
    assertThatThrownBy(workerOnly::broadcastAllLocalRulesManually)
      .isInstanceOf(UnsupportedOperationException.class);
  }

  // ── notifyLocalDetector ──

  @Test
  void notifyLocalDetector_shouldDelegateToTopK() {
    hotKey.notifyLocalDetector("my-key");
    verify(topK).addDirect("my-key", 1);
  }

  @Test
  void notifyLocalDetector_shouldIgnoreNullKey() {
    hotKey.notifyLocalDetector(null);
    verifyNoInteractions(topK);
  }
}
