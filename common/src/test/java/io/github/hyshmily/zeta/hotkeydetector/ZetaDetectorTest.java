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
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Write-side tests for {@link HotKeyDetector}. The detector is the cache-side
 * edge of the sketch: it owns the lenient key contract and forwards writes;
 * every query goes to the {@code TopK} bean directly, so this class must never
 * be one itself.
 */
class ZetaDetectorTest {

  private HeavyKeeper heavyKeeper;
  private HotKeyDetector detector;

  @BeforeEach
  void setUp() {
    heavyKeeper = mock(HeavyKeeper.class);
    detector = new HotKeyDetector(heavyKeeper);
    // The constructor registers the TopK admission listener on the sketch — a
    // wiring interaction, not detector behaviour. Start every test from a clean
    // invocation history so "no interactions" means "no detector call".
    clearInvocations(heavyKeeper);
  }

  @Test
  void detectorIsNotATopKBean() {
    // Two TopK beans made every List<TopK> injection decay the same sketch
    // twice; the detector must stay out of that type.
    assertThat(TopK.class.isAssignableFrom(HotKeyDetector.class)).isFalse();
  }

  // ── addDirect(String, long) ──

  @Test
  void addDirect_shouldDelegateToHeavyKeeper() {
    AddResult result = new AddResult(null, false, "k");
    when(heavyKeeper.addDirect("k", 1L)).thenReturn(result);
    assertThat(detector.addDirect("k", 1L)).isSameAs(result);
    verify(heavyKeeper).addDirect("k", 1L);
  }

  @Test
  void addDirect_shouldReturnColdWhenKeyNull() {
    assertThat(detector.addDirect(null, 1)).isSameAs(AddResult.cold());
    verify(heavyKeeper, never()).addDirect(anyString(), anyLong());
  }

  @Test
  void addDirect_shouldReturnColdWhenKeyBlank() {
    assertThat(detector.addDirect("   ", 1)).isSameAs(AddResult.cold());
    verify(heavyKeeper, never()).addDirect(anyString(), anyLong());
  }

  // ── ingest(Map) — the shared counter's tide entry ──

  @Test
  void ingest_shouldDelegateToHeavyKeeperIngestBatch() {
    Map<String, Long> batch = new HashMap<>();
    batch.put("k1", 3L);
    batch.put("k2", 5L);
    detector.ingest(batch);
    verify(heavyKeeper).ingestBatch(batch);
  }

  @Test
  void ingest_shouldForwardWithoutPreScanning() {
    Map<String, Long> batch = new HashMap<>();
    batch.put(null, 1L);
    batch.put("valid", 3L);
    detector.ingest(batch);
    // Key validity is the kernel's contract now: one call, no filtered copy.
    verify(heavyKeeper).ingestBatch(batch);
    verify(heavyKeeper, never()).addDirect(anyString(), anyLong());
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
}
