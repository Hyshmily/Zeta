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

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.github.hyshmily.zeta.detection.ZetaBayesianSM;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.reporting.ReportMessage;
import io.github.hyshmily.zeta.worker.detection.Evaluator;
import io.github.hyshmily.zeta.worker.detection.GlobalQpsEstimator;
import io.github.hyshmily.zeta.worker.dispatch.WorkerBroadcaster;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ReportConsumerTest {

  @Mock
  private Evaluator keyEvaluator;

  @Mock
  private WorkerBroadcaster broadcaster;

  @Mock
  private TopK workerTopK;

  @Mock
  private GlobalQpsEstimator globalQpsEstimator;

  @Mock
  private ZetaBayesianSM stateMachine;

  private ReportConsumer consumer;

  @BeforeEach
  void setUp() {
    consumer = new ReportConsumer(keyEvaluator, broadcaster, workerTopK, globalQpsEstimator, stateMachine);
  }

  @Test
  void shouldProcessEntriesAndFeedAllComponents() {
    ReportMessage message = new ReportMessage(
      0L,
      "testApp",
      System.currentTimeMillis(),
      Map.of("key1", 5L, "key2", 3L)
    );
    when(keyEvaluator.evaluate(anyString(), anyLong())).thenReturn(ZetaDecision.none("key", null));

    consumer.onReport(message);

    verify(workerTopK).addDirect("key1", 5L);
    verify(workerTopK).addDirect("key2", 3L);
    verify(keyEvaluator).evaluate("key1", 5L);
    verify(keyEvaluator).evaluate("key2", 3L);
    verify(globalQpsEstimator).addTotal(8L);
  }

  @Test
  void shouldBroadcastHotWhenStateMachineReturnsHot() {
    ReportMessage message = new ReportMessage(0L, "testApp", System.currentTimeMillis(), Map.of("hotKey", 100L));
    when(keyEvaluator.evaluate("hotKey", 100L)).thenReturn(ZetaDecision.hot("hotKey", null));

    consumer.onReport(message);

    verify(broadcaster).broadcastHot("hotKey");
  }

  @Test
  void shouldSkipStaleMessages() {
    ReportMessage message = new ReportMessage(0L, "testApp", System.currentTimeMillis() - 10_000, Map.of("key", 1L));
    consumer.onReport(message);

    verify(workerTopK, never()).addDirect(anyString(), anyLong());
    verify(keyEvaluator, never()).evaluate(anyString(), anyLong());
  }

  @Test
  void shouldHandleExceptionsGracefully() {
    ReportMessage message = new ReportMessage(0L, "testApp", System.currentTimeMillis(), Map.of("badKey", 1L));
    when(keyEvaluator.evaluate("badKey", 1L)).thenThrow(new RuntimeException("test error"));

    consumer.onReport(message);

    verify(globalQpsEstimator).addTotal(1L);
  }

  @Test
  void shouldHandleEmptyCountsMap() {
    ReportMessage message = new ReportMessage(0L, "testApp", System.currentTimeMillis(), Map.of());
    consumer.onReport(message);
    verify(globalQpsEstimator, never()).addTotal(anyLong());
    verify(workerTopK, never()).addDirect(anyString(), anyLong());
  }

  @Test
  void shouldHandleCountAtMaxIntegerBoundary() {
    ReportMessage message = new ReportMessage(
      0L,
      "testApp",
      System.currentTimeMillis(),
      Map.of("bigKey", (long) Integer.MAX_VALUE)
    );
    when(keyEvaluator.evaluate("bigKey", (long) Integer.MAX_VALUE)).thenReturn(ZetaDecision.none("bigKey", null));

    consumer.onReport(message);

    verify(workerTopK).addDirect("bigKey", (long) Integer.MAX_VALUE);
    verify(globalQpsEstimator).addTotal((long) Integer.MAX_VALUE);
  }

  @Test
  void shouldClampCountToMaxIntForTopK() {
    ReportMessage message = new ReportMessage(
      0L,
      "testApp",
      System.currentTimeMillis(),
      Map.of("hugeKey", (long) Integer.MAX_VALUE + 1)
    );
    when(keyEvaluator.evaluate("hugeKey", (long) Integer.MAX_VALUE + 1)).thenReturn(ZetaDecision.none("hugeKey", null));

    consumer.onReport(message);

    verify(workerTopK).addDirect("hugeKey", (long) Integer.MAX_VALUE + 1);
  }

  @Test
  void shouldHandleNegativeCounts() {
    ReportMessage message = new ReportMessage(0L, "testApp", System.currentTimeMillis(), Map.of("negKey", -5L));
    when(keyEvaluator.evaluate("negKey", -5L)).thenReturn(ZetaDecision.none("negKey", null));

    consumer.onReport(message);

    verify(keyEvaluator).evaluate("negKey", -5L);
    verify(globalQpsEstimator).addTotal(-5L);
  }

  @Test
  void shouldProcessMessageUnderStalenessBoundary() {
    consumer.stalenessThresholdMs = 100_000L;
    long now = System.currentTimeMillis();
    ReportMessage message = new ReportMessage(0L, "testApp", now - 1, Map.of("key", 1L));
    when(keyEvaluator.evaluate("key", 1L)).thenReturn(ZetaDecision.none("key", null));

    consumer.onReport(message);

    verify(keyEvaluator).evaluate("key", 1L);
  }

  @Test
  void shouldNotBroadcastWhenDecisionIsNone() {
    ReportMessage message = new ReportMessage(0L, "testApp", System.currentTimeMillis(), Map.of("key", 1L));
    when(keyEvaluator.evaluate("key", 1L)).thenReturn(ZetaDecision.none("key", null));

    consumer.onReport(message);

    verify(broadcaster, never()).broadcastHot(anyString());
    verify(broadcaster, never()).broadcastCool(anyString());
  }

  @Test
  void shouldBroadcastCoolWhenStateMachineReturnsCool() {
    ReportMessage message = new ReportMessage(0L, "testApp", System.currentTimeMillis(), Map.of("coolKey", 1L));
    when(keyEvaluator.evaluate("coolKey", 1L)).thenReturn(ZetaDecision.cool("coolKey", null));

    consumer.onReport(message);

    verify(broadcaster).broadcastCool("coolKey");
  }

  @Test
  void shouldContinueProcessingAfterEvaluationError() {
    ReportMessage message = new ReportMessage(
      0L,
      "testApp",
      System.currentTimeMillis(),
      Map.of("goodKey", 1L, "badKey", 2L)
    );
    when(keyEvaluator.evaluate("goodKey", 1L)).thenReturn(ZetaDecision.none("goodKey", null));
    when(keyEvaluator.evaluate("badKey", 2L)).thenThrow(new RuntimeException("evaluation error"));

    consumer.onReport(message);

    verify(keyEvaluator).evaluate("goodKey", 1L);
    verify(globalQpsEstimator).addTotal(3L);
  }

  @Test
  void broadcastHotSuccess_shouldNotRollback() {
    ReportMessage message = new ReportMessage(0L, "testApp", System.currentTimeMillis(), Map.of("hotKey", 100L));
    when(keyEvaluator.evaluate("hotKey", 100L)).thenReturn(ZetaDecision.hot("hotKey", null));

    consumer.onReport(message);

    verify(broadcaster).broadcastHot("hotKey");
  }

  @Test
  void broadcastCoolSuccess_shouldNotRollback() {
    ReportMessage message = new ReportMessage(0L, "testApp", System.currentTimeMillis(), Map.of("coolKey", 1L));
    when(keyEvaluator.evaluate("coolKey", 1L)).thenReturn(ZetaDecision.cool("coolKey", null));

    consumer.onReport(message);

    verify(broadcaster).broadcastCool("coolKey");
  }
}
