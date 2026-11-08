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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.Test;

/** Tag-mode routing: each mode must run exactly its slice of the counting pipeline. */
class AccessCounterRecordTest {

  private final HotKeyDetector detector = mock(HotKeyDetector.class);
  private final KeyReporter reporter = mock(KeyReporter.class);
  private final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);

  private AccessCounter counter() {
    AccessCounter counter = spy(new AccessCounter(detector, reporter, scheduler, 50L, 500L));
    doNothing().when(counter).add(anyString());
    doNothing().when(counter).addDetection(anyString());
    doNothing().when(counter).addReportOnly(anyString());
    return counter;
  }

  @Test
  void recordWithReportingUsesTheMergedPath() {
    AccessCounter counter = counter();

    counter.record("k", true);

    verify(counter).add("k");
    verify(counter, never()).addDetection(anyString());
    verify(counter, never()).addReportOnly(anyString());
  }

  @Test
  void recordWithoutReportingUsesDetectionOnly() {
    AccessCounter counter = counter();

    counter.record("k", false);

    verify(counter).addDetection("k");
    verify(counter, never()).add(anyString());
    verify(counter, never()).addReportOnly(anyString());
  }

  @Test
  void tagModeRecordUsesTheMergedPath() {
    AccessCounter counter = counter();

    counter.record("k", Zeta.TagMode.RECORD);

    verify(counter).add("k");
    verify(counter, never()).addDetection(anyString());
    verify(counter, never()).addReportOnly(anyString());
  }

  @Test
  void tagModeDetectOnlySkipsReporting() {
    AccessCounter counter = counter();

    counter.record("k", Zeta.TagMode.DETECT_ONLY);

    verify(counter).addDetection("k");
    verify(counter, never()).add(anyString());
    verify(counter, never()).addReportOnly(anyString());
  }

  @Test
  void tagModeReportOnlySkipsLocalCounting() {
    AccessCounter counter = counter();

    counter.record("k", Zeta.TagMode.REPORT_ONLY);

    verify(counter).addReportOnly("k");
    verify(counter, never()).add(anyString());
    verify(counter, never()).addDetection(anyString());
  }

  @Test
  void tagModeSilentRunsNothing() {
    AccessCounter counter = counter();

    counter.record("k", Zeta.TagMode.SILENT);

    verify(counter, never()).add(anyString());
    verify(counter, never()).addDetection(anyString());
    verify(counter, never()).addReportOnly(anyString());
  }
}
