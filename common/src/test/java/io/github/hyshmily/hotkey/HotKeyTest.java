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

import io.github.hyshmily.hotkey.cache.HotKeyCache;
import io.github.hyshmily.hotkey.exception.HotKeyBlockedException;
import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.Item;
import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.TopK;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the {@link HotKey} facade verifying delegation to HotKeyCache, TopK, and worker mode.
 */
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

  /**
   * Verifies that isLocalHotKey delegates to HotKeyCache and returns the expected result.
   */
  @Test
  void isLocalHotKey_shouldDelegateToCache() {
    when(hotKeyCache.isLocalHotKey("key1")).thenReturn(true);
    assertThat(hotKey.isLocalHotKey("key1")).isTrue();
    verify(hotKeyCache).isLocalHotKey("key1");
  }

  /**
   * Verifies that peek delegates to HotKeyCache and returns the cached value if present.
   */
  @Test
  void peek_shouldDelegateToCache() {
    when(hotKeyCache.peek("key1")).thenReturn(Optional.of("value"));
    assertThat(hotKey.peek("key1")).contains("value");
    verify(hotKeyCache).peek("key1");
  }

  /**
   * Verifies that get(key, supplier) delegates to HotKeyCache.
   */
  @Test
  void get_shouldDelegateToCache() {
    when(hotKeyCache.get(anyString(), any())).thenReturn(Optional.of("value"));
    assertThat(hotKey.get("key1", () -> "loaded")).contains("value");
    verify(hotKeyCache).get(anyString(), any());
  }

  /**
   * Verifies that get(key, supplier, hardTtlMs, softTtlMs) delegates to HotKeyCache with TTL parameters.
   */
  @Test
  void get_withTtl_shouldDelegateToCache() {
    when(hotKeyCache.get(anyString(), any(), anyLong(), anyLong())).thenReturn(Optional.of("v"));
    assertThat(hotKey.get("key1", () -> "loaded", 1000L, 100L)).contains("v");
    verify(hotKeyCache).get(anyString(), any(), anyLong(), anyLong());
  }

  /**
   * Verifies that getWithSoftExpire delegates to HotKeyCache.
   */
  @Test
  void getWithSoftExpire_shouldDelegateToCache() {
    when(hotKeyCache.getWithSoftExpire(anyString(), any())).thenReturn(Optional.of("v"));
    assertThat(hotKey.getWithSoftExpire("key1", () -> "v")).contains("v");
    verify(hotKeyCache).getWithSoftExpire(anyString(), any());
  }

  /**
   * Verifies that invalidate delegates to HotKeyCache to remove the key.
   */
  @Test
  void invalidate_shouldDelegateToCache() {
    hotKey.invalidate("key1");
    verify(hotKeyCache).invalidate("key1");
  }

  /**
   * Verifies that invalidateAll with varargs delegates to HotKeyCache with the key list.
   */
  @Test
  void invalidateAll_varargs_shouldDelegateToCache() {
    hotKey.invalidateAll("k1", "k2");
    verify(hotKeyCache).invalidateAll(List.of("k1", "k2"));
  }

  /**
   * Verifies that putThrough delegates to HotKeyCache.
   */
  @Test
  void putThrough_shouldDelegateToCache() {
    hotKey.putThrough("key1", "value", () -> {});
    verify(hotKeyCache).putThrough(anyString(), any(), any());
  }

  /**
   * Verifies that putBeforeInvalidate delegates to HotKeyCache.
   */
  @Test
  void putBeforeInvalidate_shouldDelegateToCache() {
    hotKey.putBeforeInvalidate("key1", () -> {});
    verify(hotKeyCache).putBeforeInvalidate(anyString(), any());
  }

  /**
   * Verifies that returnLocalHotKeys returns items from the local TopK instance.
   */
  @Test
  void returnHotKeys_shouldReturnLocalFromTopK() {
    when(topK.list()).thenReturn(List.of(new Item("k1", 10)));
    assertThat(hotKey.returnLocalHotKeys()).hasSize(1);
  }

  /**
   * Verifies that returnLocalHotKeys returns an empty list when the local TopK is null.
   */
  @Test
  void returnHotKeys_shouldReturnLocalEmptyWhenTopKNull() {
    HotKey hk = new HotKey(hotKeyCache, null);
    assertThat(hk.returnLocalHotKeys()).isEmpty();
  }

  /**
   * Verifies that returnWorkerHotKeys returns items from the worker TopK instance.
   */
  @Test
  void returnWorkerHotKeys_shouldReturnFromWorkerTopK() {
    when(workerTopK.list()).thenReturn(List.of(new Item("k1", 5)));
    assertThat(hotKey.returnWorkerHotKeys()).hasSize(1);
  }

  /**
   * Verifies that returnWorkerHotKeys returns an empty list when the worker TopK is null.
   */
  @Test
  void returnWorkerHotKeys_shouldReturnEmptyWhenWorkerTopKNull() {
    assertThat(new HotKey(hotKeyCache, topK, null).returnWorkerHotKeys()).isEmpty();
  }

  /**
   * Verifies that returnLocalTotalDataStreams returns the total count from the local TopK.
   */
  @Test
  void returnTotalDataStreams_shouldReturnLocalFromTopK() {
    when(topK.total()).thenReturn(100L);
    assertThat(hotKey.returnLocalTotalDataStreams()).isEqualTo(100L);
  }

  /**
   * Verifies that returnLocalTotalDataStreams returns zero when the local TopK is null.
   */
  @Test
  void returnTotalDataStreams_shouldReturnLocalZeroWhenTopKNull() {
    assertThat(new HotKey(hotKeyCache, null).returnLocalTotalDataStreams()).isZero();
  }

  /**
   * Verifies that returnLocalExpelledHotKeys returns expelled items from the local TopK.
   */
  @Test
  void returnExpelledHotKeys_shouldReturnLocalFromTopK() {
    LinkedBlockingQueue<Item> queue = new LinkedBlockingQueue<>();
    queue.add(new Item("k1", 5));
    when(topK.expelled()).thenReturn(queue);
    assertThat(hotKey.returnLocalExpelledHotKeys()).hasSize(1);
  }

  /**
   * Verifies that cache methods throw UnsupportedOperationException in worker-only mode (null HotKeyCache).
   */
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

  /**
   * Verifies that HotKeyBlockedException from HotKeyCache propagates through the facade.
   */
  @Test
  void get_shouldPropagateHotKeyBlockedException() {
    when(hotKeyCache.get(anyString(), any()))
      .thenThrow(new HotKeyBlockedException("secret"));
    assertThatThrownBy(() -> hotKey.get("secret", () -> "v"))
      .isInstanceOf(HotKeyBlockedException.class);
  }
}
