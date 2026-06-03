package io.github.hyshmily.hotkey;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.hyshmily.hotkey.algorithm.Item;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.exception.HotKeyBlockedException;
import io.github.hyshmily.hotkey.hotkeycache.HotKeyCache;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
      .thenThrow(new HotKeyBlockedException("secret"));
    assertThatThrownBy(() -> hotKey.get("secret", () -> "v"))
      .isInstanceOf(HotKeyBlockedException.class);
  }
}
