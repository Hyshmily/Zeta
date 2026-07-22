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
package io.github.hyshmily.zeta.hotkeydetector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.AddResult;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.HeavyKeeper;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ZetaDetectorTest {

  private final AddResult DUMMY = new AddResult(null, false, "k");

  private HeavyKeeper heavyKeeper;
  private ScheduledExecutorService scheduler;
  private HotKeyDetector detector;

  @BeforeEach
  void setUp() {
    heavyKeeper = mock(HeavyKeeper.class);
    scheduler = mock(ScheduledExecutorService.class);
    detector = new HotKeyDetector(heavyKeeper, scheduler);
  }

  // ── Constructors ──

  @Test
  void constructor_withScheduler_shouldNotFail() {
    assertThat(detector).isNotNull();
  }

  @Test
  void constructor_withoutScheduler_shouldNotFail() {
    assertThat(new HotKeyDetector(heavyKeeper)).isNotNull();
  }

  // ── Lifecycle ──

  @Test
  void afterPropertiesSet_shouldNotThrow() {
    detector.afterPropertiesSet();
  }

  @Test
  void destroy_shouldNotThrow() {
    detector.destroy();
  }

  // ── addDirect(String, int) ──

  @Test
  void addDirect_shouldDelegateToHeavyKeeper() {
    AddResult result = new AddResult(null, false, "k");
    when(heavyKeeper.addDirect("k", 1)).thenReturn(result);
    assertThat(detector.addDirect("k", 1)).isSameAs(result);
    verify(heavyKeeper).addDirect("k", 1);
  }

  @Test
  void addDirect_shouldReturnColdWhenKeyNull() {
    assertThat(detector.addDirect(null, 1)).isSameAs(AddResult.cold());
    verify(heavyKeeper, never()).addDirect(anyString(), anyInt());
  }

  @Test
  void addDirect_shouldReturnColdWhenKeyBlank() {
    assertThat(detector.addDirect("   ", 1)).isSameAs(AddResult.cold());
    verify(heavyKeeper, never()).addDirect(anyString(), anyInt());
  }

  // ── addDirect(Map) ──

  @Test
  void addDirect_batch_shouldDelegateToHeavyKeeper() {
    Map<String, Long> map = new HashMap<>();
    map.put("k1", 3L);
    map.put("k2", 5L);
    List<AddResult> results = List.of(new AddResult(null, false, "k1"));
    when(heavyKeeper.addDirect(map)).thenReturn(results);
    assertThat(detector.addDirect(map)).isSameAs(results);
    verify(heavyKeeper).addDirect(map);
  }

  @Test
  void addDirect_batch_shouldFilterNullKey() {
    Map<String, Long> map = new HashMap<>();
    map.put(null, 1L);
    map.put("k", 2L);
    detector.addDirect(map);
    verify(heavyKeeper).addDirect(argThat(m -> m.size() == 1 && !m.containsKey(null)));
  }

  @Test
  void addDirect_batch_shouldFilterBlankKey() {
    Map<String, Long> map = new HashMap<>();
    map.put("", 1L);
    map.put("k", 2L);
    detector.addDirect(map);
    verify(heavyKeeper).addDirect(argThat(m -> m.size() == 1 && !m.containsKey("")));
  }

  @Test
  void addDirect_batch_shouldHandleEmptyMap() {
    Map<String, Long> map = new HashMap<>();
    detector.addDirect(map);
    verify(heavyKeeper).addDirect(map);
  }

  // ── add(String) ──

  @Test
  void add_shouldAcceptValidKey() {
    detector.add("valid-key");
    // BufferedCounter doesn't flush synchronously; just verify no throw
  }

  @Test
  void add_shouldSkipNullKey() {
    detector.add((String) null);
    verifyNoInteractions(heavyKeeper);
  }

  @Test
  void add_shouldSkipBlankKey() {
    detector.add("  ");
    verifyNoInteractions(heavyKeeper);
  }

  // ── add(String, long) ──

  @Test
  void add_withDelta_shouldAcceptValidKey() {
    detector.add("valid", 5);
  }

  @Test
  void add_withDelta_shouldSkipNullKey() {
    detector.add(null, 5);
    verifyNoInteractions(heavyKeeper);
  }

  @Test
  void add_withDelta_shouldSkipBlankKey() {
    detector.add("  ", 5);
    verifyNoInteractions(heavyKeeper);
  }

  // ── add(Map) ──

  @Test
  void add_map_shouldAcceptValidKeys() {
    Map<String, Long> map = new HashMap<>();
    map.put("k1", 3L);
    map.put("k2", 4L);
    detector.add(map);
  }

  @Test
  void add_map_shouldSkipInvalidKeys() {
    Map<String, Long> map = new HashMap<>();
    map.put(null, 1L);
    map.put("", 2L);
    map.put("valid", 3L);
    detector.add(map);
  }

  @Test
  void add_map_shouldHandleEmptyMap() {
    detector.add(new HashMap<>());
  }

  // ── list ──

  @Test
  void list_shouldDelegate() {
    List<Item> items = List.of(new Item("k", 10));
    when(heavyKeeper.list()).thenReturn(items);
    assertThat(detector.list()).isSameAs(items);
  }

  // ── listTopN ──

  @Test
  void listTopN_shouldDelegate() {
    List<Item> items = List.of(new Item("k", 10));
    when(heavyKeeper.listTopN(5)).thenReturn(items);
    assertThat(detector.listTopN(5)).isSameAs(items);
  }

  // ── contains ──

  @Test
  void contains_shouldDelegate() {
    when(heavyKeeper.contains("hot")).thenReturn(true);
    assertThat(detector.contains("hot")).isTrue();
    verify(heavyKeeper).contains("hot");
  }

  @Test
  void contains_shouldReturnFalseWhenKeyNull() {
    assertThat(detector.contains(null)).isFalse();
    verify(heavyKeeper, never()).contains(anyString());
  }

  @Test
  void contains_shouldReturnFalseWhenKeyBlank() {
    assertThat(detector.contains("  ")).isFalse();
    verify(heavyKeeper, never()).contains(anyString());
  }

  // ── total ──

  @Test
  void total_shouldDelegate() {
    when(heavyKeeper.total()).thenReturn(42L);
    assertThat(detector.total()).isEqualTo(42L);
  }

  // ── expelled ──

  @Test
  void expelled_shouldDelegate() {
    BlockingQueue<Item> queue = new LinkedBlockingQueue<>();
    when(heavyKeeper.expelled()).thenReturn(queue);
    assertThat(detector.expelled()).isSameAs(queue);
  }

  // ── fading ──

  @Test
  void fading_shouldDelegate() {
    detector.fading();
    verify(heavyKeeper).fading();
  }

  @Test
  void add_buffered_shouldEventuallyFlushToHeavyKeeper() throws Exception {
    HeavyKeeper realKeeper = new HeavyKeeper(3, 1000, 4, 0.9, 1);
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    HotKeyDetector realDetector = new HotKeyDetector(realKeeper, scheduler);
    realDetector.afterPropertiesSet();
    realDetector.add("testKey");
    Thread.sleep(600);
    realDetector.destroy();
    scheduler.shutdown();
    assertThat(realKeeper.contains("testKey")).isTrue();
  }
}
