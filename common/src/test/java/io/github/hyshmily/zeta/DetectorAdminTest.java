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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.exception.ZetaModeException;
import io.github.hyshmily.zeta.heat.HeatView;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link Zeta.DetectorAdmin} view (pure queries) and the
 * {@link Zeta#tag} feed family: app-mode parity and Worker-only-mode
 * degradation (tag/isLocalHotKey throw, pure queries answer empty).
 */
class DetectorAdminTest {

  private HotKeyCache hotKeyCache;
  private TopK appDetector;
  private HeatView heatView;
  private Zeta zeta;

  @BeforeEach
  void setUp() {
    hotKeyCache = mock(HotKeyCache.class);
    appDetector = mock(TopK.class);
    heatView = mock(HeatView.class);
    zeta = new DefaultZeta(hotKeyCache, appDetector, null, null, null, heatView);
  }

  @Test
  void detector_isLocalHotKey_shouldDelegateToHeatView() {
    when(heatView.isLocalHot("key1")).thenReturn(true);
    assertThat(zeta.detector().isLocalHotKey("key1")).isTrue();
    verify(heatView).isLocalHot("key1");
  }

  @Test
  void detector_feeds_shouldReachEngine() {
    zeta.tag("k", Zeta.TagMode.DETECT_ONLY);
    verify(hotKeyCache).recordAccess("k", 1L, Zeta.TagMode.DETECT_ONLY);
  }

  @Test
  void detector_localTopKeys_shouldReturnDetectorItems() {
    when(appDetector.listTopN(3)).thenReturn(List.of(new Item("k", 42)));
    assertThat(zeta.detector().localTopKeys(3)).containsExactly(new Item("k", 42));
  }

  @Test
  void detector_localTopKeys_shouldRejectANegativeLimitInEveryMode() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null, null);

    assertThatThrownBy(() -> workerOnly.detector().localTopKeys(-1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> zeta.detector().localTopKeys(-1)).isInstanceOf(IllegalArgumentException.class);
    verify(appDetector, never()).listTopN(anyInt());
  }

  @Test
  void detector_localTopKeys_zero_shouldAnswerEmptyInEveryMode() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null, null);

    assertThat(zeta.detector().localTopKeys(0)).isEmpty();
    assertThat(workerOnly.detector().localTopKeys(0)).isEmpty();
    verify(appDetector, never()).listTopN(anyInt());
  }

  @Test
  void workerOnly_detectorFeeds_shouldThrow() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null, null);
    assertThatThrownBy(() -> workerOnly.tag("k", Zeta.TagMode.DETECT_ONLY)).isInstanceOf(ZetaModeException.class);
    assertThatThrownBy(() -> workerOnly.detector().isLocalHotKey("k")).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void workerOnly_detectorQueries_shouldAnswerEmpty() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null, null);
    assertThat(workerOnly.detector().localTopKeys(5)).isEmpty();
  }
}
