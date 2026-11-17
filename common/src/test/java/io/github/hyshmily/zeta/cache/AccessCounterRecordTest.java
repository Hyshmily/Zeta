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
package io.github.hyshmily.zeta.cache;

import static io.github.hyshmily.zeta.constants.ZetaConstants.TOPK_INCR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.Test;

/** Tag-mode routing: each mode must run exactly its slice of the counting pipeline. */
class AccessCounterRecordTest {

  private final HotKeyDetector detector = mock(HotKeyDetector.class);
  private final KeyReporter reporter = mock(KeyReporter.class);
  private final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);

  private AccessCounter counter() {
    return new AccessCounter(detector, reporter, scheduler, 50L, 500L);
  }

  @Test
  void readPathReportFeedsTheMergedReservoir() {
    AccessCounter counter = counter();

    counter.record("k", true);

    assertThat(counter.pendingKeyCount()).isPositive();
    verify(detector, never()).addDirect(anyString(), anyLong());
    verify(reporter, never()).publishDirect(anyMap());
  }

  @Test
  void readPathNoReportFeedsTheSketchDirectly() {
    AccessCounter counter = counter();

    counter.record("k", false);

    verify(detector).addDirect("k", TOPK_INCR);
    assertThat(counter.pendingKeyCount()).isZero();
    verify(reporter, never()).publishDirect(anyMap());
  }

  @Test
  void recordUsesTheMergedReservoir() {
    AccessCounter counter = counter();

    counter.record("k", 5L, Zeta.TagMode.RECORD);

    assertThat(counter.pendingKeyCount()).isPositive();
    verify(detector, never()).addDirect(anyString(), anyLong());
    verify(reporter, never()).publishDirect(anyMap());
  }

  @Test
  void detectOnlyUsesTheExplicitCount() {
    AccessCounter counter = counter();

    counter.record("k", 5L, Zeta.TagMode.DETECT_ONLY);

    verify(detector).addDirect("k", 5L);
    assertThat(counter.pendingKeyCount()).isZero();
    verify(reporter, never()).publishDirect(anyMap());
  }

  @Test
  void reportOnlyUsesTheExplicitCount() {
    AccessCounter counter = counter();

    counter.record("k", 5L, Zeta.TagMode.REPORT_ONLY);

    verify(reporter).publishDirect(Map.of("k", 5L));
    assertThat(counter.pendingKeyCount()).isZero();
    verify(detector, never()).addDirect(anyString(), anyLong());
  }

  @Test
  void silentRunsNothing() {
    AccessCounter counter = counter();

    counter.record("k", 5L, Zeta.TagMode.SILENT);

    assertThat(counter.pendingKeyCount()).isZero();
    verify(detector, never()).addDirect(anyString(), anyLong());
    verify(reporter, never()).publishDirect(anyMap());
  }

  @Test
  void recordAllRecordFeedsTheMergedReservoir() {
    AccessCounter counter = counter();

    counter.recordAll(Map.of("a", 2L, "b", 3L), Zeta.TagMode.RECORD);

    assertThat(counter.pendingKeyCount()).isPositive();
    verify(detector, never()).addDirect(anyString(), anyLong());
    verify(reporter, never()).publishDirect(anyMap());
  }

  @Test
  void recordAllDetectOnlyIngestsTheBatch() {
    AccessCounter counter = counter();
    Map<String, Long> batch = Map.of("a", 2L, "b", 3L);

    counter.recordAll(batch, Zeta.TagMode.DETECT_ONLY);

    verify(detector).ingest(batch);
    assertThat(counter.pendingKeyCount()).isZero();
    verify(reporter, never()).publishDirect(anyMap());
  }

  @Test
  void recordAllReportOnlyPublishesTheBatch() {
    AccessCounter counter = counter();
    Map<String, Long> batch = Map.of("a", 2L, "b", 3L);

    counter.recordAll(batch, Zeta.TagMode.REPORT_ONLY);

    verify(reporter).publishDirect(batch);
    assertThat(counter.pendingKeyCount()).isZero();
    verify(detector, never()).addDirect(anyString(), anyLong());
  }

  @Test
  void recordAllSilentRunsNothing() {
    AccessCounter counter = counter();

    counter.recordAll(Map.of("a", 2L), Zeta.TagMode.SILENT);

    verify(detector, never()).ingest(anyMap());
    verify(detector, never()).addDirect(anyString(), anyLong());
    verify(reporter, never()).publishDirect(anyMap());
  }
}
