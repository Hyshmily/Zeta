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
package io.github.hyshmily.zeta.worker.ingest;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.reporting.ReportMessage;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.worker.detection.DetectionPipeline;
import io.github.hyshmily.zeta.worker.dispatch.ingest.ReportConsumer;
import io.github.hyshmily.zeta.worker.metrics.WorkerDetectionMetrics;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** Transport concerns only: fence, staleness, malformed entries, and forwarding. */
@ExtendWith(MockitoExtension.class)
class ReportConsumerTest {

  private static final long EPOCH = 2L;

  @Mock
  private DetectionPipeline pipeline;

  @BeforeEach
  void enablePipeline() {
    lenient().when(pipeline.enabled()).thenReturn(true);
  }

  private ReportConsumer consumer(long stalenessMs) {
    return new ReportConsumer(pipeline, EPOCH, stalenessMs, new WorkerDetectionMetrics(null));
  }

  private static ReportMessage message(long epoch, Map<String, Long> counts) {
    return new ReportMessage(1L, "app", TimeSource.currentTimeMillis(), epoch, counts);
  }

  @Test
  void dropsReportsAddressedToAnotherIncarnation() {
    consumer(0).onReport(message(EPOCH - 1, Map.of("k", 5L)));
    verify(pipeline).enabled();
    verifyNoMoreInteractions(pipeline);
  }

  @Test
  void forwardsEveryWellFormedEntry() {
    consumer(0).onReport(message(EPOCH, Map.of("a", 1L, "b", 2L)));
    verify(pipeline).onReport(eq("a"), eq(1L), anyLong(), anyLong());
    verify(pipeline).onReport(eq("b"), eq(2L), anyLong(), anyLong());
  }

  @Test
  void disabledDetectionDropsTheWholeBatchWithoutIteratingIt() {
    when(pipeline.enabled()).thenReturn(false);

    consumer(0).onReport(message(EPOCH, Map.of("a", 1L, "b", 2L)));

    verify(pipeline).enabled();
    verifyNoMoreInteractions(pipeline);
  }

  @Test
  void dropsNullAndNonPositiveEntriesBeforeThePipeline() {
    Map<String, Long> counts = new HashMap<>();
    counts.put("ok", 3L);
    counts.put("zero", 0L);
    counts.put("negative", -1L);
    counts.put("null", null);

    consumer(0).onReport(message(EPOCH, counts));

    verify(pipeline).onReport(eq("ok"), eq(3L), anyLong(), anyLong());
    verify(pipeline, never()).onReport(eq("zero"), eq(0L), anyLong(), anyLong());
    verify(pipeline, never()).onReport(eq("negative"), eq(-1L), anyLong(), anyLong());
    verify(pipeline, never()).onReport(eq("null"), eq(0L), anyLong(), anyLong());
  }

  @Test
  void dropsReportsOlderThanTheStalenessThreshold() {
    long staleTimestamp = TimeSource.currentTimeMillis() - 5_000L;
    consumer(1_000L).onReport(new ReportMessage(1L, "app", staleTimestamp, EPOCH, Map.of("k", 5L)));
    verify(pipeline).enabled();
    verifyNoMoreInteractions(pipeline);
  }

  @Test
  void acceptsAFutureTimestampBecauseClockSkewIsNotStaleness() {
    long future = TimeSource.currentTimeMillis() + 5_000L;
    consumer(1_000L).onReport(new ReportMessage(1L, "app", future, EPOCH, Map.of("k", 5L)));
    verify(pipeline).onReport(eq("k"), eq(5L), anyLong(), anyLong());
  }

  @Test
  void ignoresAMalformedBodyWithoutTouchingThePipeline() {
    consumer(0).onReport(new ReportMessage(1L, "app", TimeSource.currentTimeMillis(), EPOCH, null));
    verify(pipeline).enabled();
    verifyNoMoreInteractions(pipeline);
  }

  @Test
  void swallowsAPipelineFailureAndKeepsConsumingTheBatch() {
    lenient()
      .doThrow(new IllegalStateException("boom"))
      .when(pipeline)
      .onReport(eq("bad"), eq(1L), anyLong(), anyLong());
    consumer(0).onReport(message(EPOCH, Map.of("bad", 1L, "good", 2L)));

    verify(pipeline).onReport(eq("good"), eq(2L), anyLong(), anyLong());
    verify(pipeline, atLeastOnce()).onReport(anyString(), anyLong(), anyLong(), anyLong());
  }
}
