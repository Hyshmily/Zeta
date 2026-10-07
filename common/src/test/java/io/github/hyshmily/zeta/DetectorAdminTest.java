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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.exception.ZetaModeException;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.zeta.model.HotKey;
import io.github.hyshmily.zeta.rule.RuleService;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link Zeta.DetectorAdmin} view: app-mode parity with the
 * former direct methods (which now delegate to it) and Worker-only-mode
 * degradation (feeds throw, pure queries answer empty).
 */
class DetectorAdminTest {

  private HotKeyCache hotKeyCache;
  private HotKeyDetector appDetector;
  private Zeta zeta;

  @BeforeEach
  void setUp() {
    hotKeyCache = mock(HotKeyCache.class);
    appDetector = mock(HotKeyDetector.class);
    zeta = new DefaultZeta(hotKeyCache, appDetector, null, null, mock(RuleService.class));
  }

  @Test
  void detector_isLocalHotKey_shouldDelegateToCache() {
    when(hotKeyCache.isHot("key1")).thenReturn(true);
    assertThat(zeta.detector().isLocalHotKey("key1")).isTrue();
    verify(hotKeyCache).isHot("key1");
  }

  @Test
  void detector_notifyFeeds_shouldReachDetector() {
    zeta.detector().notifyLocalDetector("k");
    verify(appDetector).add("k");
    zeta.detector().notifyLocalDetector("k", 3L);
    verify(appDetector).add("k", 3L);
    zeta.detector().notifyLocalDetector(Map.of("k", 2L));
    verify(appDetector).add(Map.of("k", 2L));
    zeta.detector().notifyLocalDetectorDirect("k", 5L);
    verify(appDetector).addDirect("k", 5L);
    zeta.detector().notifyLocalDetectorDirect(Map.of("k", 7L));
    verify(appDetector).addDirect(Map.of("k", 7L));
  }

  @Test
  void detector_localTopKeys_shouldMapItemsToHotKeys() {
    Item item = mock(Item.class);
    when(item.key()).thenReturn("k");
    when(item.count()).thenReturn(42L);
    when(appDetector.list()).thenReturn(List.of(item));
    when(appDetector.listTopN(3)).thenReturn(List.of(item));
    assertThat(zeta.detector().localTopKeys()).containsExactly(new HotKey("k", 42));
    assertThat(zeta.detector().localTopKeys(3)).containsExactly(new HotKey("k", 42));
  }

  @Test
  void detector_areLocalHotKeys_shouldFanOut() {
    when(hotKeyCache.isHot("a")).thenReturn(true);
    when(hotKeyCache.isHot("b")).thenReturn(false);
    assertThat(zeta.detector().areLocalHotKeys(List.of("a", "b")))
      .isEqualTo(Map.of("a", true, "b", false));
  }

  @Test
  void detector_expelledAndTotal_shouldDelegate() {
    assertThat(zeta.detector().returnLocalTotalDataStreams()).isZero();
    verify(appDetector).total();
    zeta.detector().returnLocalExpelledHotKeys();
    verify(appDetector).expelled();
  }

  @Test
  void workerOnly_detectorFeeds_shouldThrow() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThatThrownBy(() -> workerOnly.detector().notifyLocalDetector("k"))
      .isInstanceOf(ZetaModeException.class);
    assertThatThrownBy(() -> workerOnly.detector().notifyLocalDetectorDirect("k", 1L))
      .isInstanceOf(ZetaModeException.class);
    assertThatThrownBy(() -> workerOnly.detector().isLocalHotKey("k"))
      .isInstanceOf(ZetaModeException.class);
  }

  @Test
  void workerOnly_detectorQueries_shouldAnswerEmpty() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThat(workerOnly.detector().localTopKeys()).isEmpty();
    assertThat(workerOnly.detector().localTopKeys(5)).isEmpty();
    assertThat(workerOnly.detector().returnLocalTotalDataStreams()).isZero();
    assertThat(workerOnly.detector().returnLocalExpelledHotKeys()).isEmpty();
  }
}
