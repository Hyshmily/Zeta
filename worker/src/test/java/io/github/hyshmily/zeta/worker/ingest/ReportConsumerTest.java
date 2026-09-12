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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.github.hyshmily.zeta.detection.ZetaBayesianSM;
import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.reporting.ReportMessage;
import io.github.hyshmily.zeta.worker.detection.Evaluator;
import io.github.hyshmily.zeta.worker.detection.GlobalQpsEstimator;
import io.github.hyshmily.zeta.worker.dispatch.WorkerBroadcastBuffer;
import io.github.hyshmily.zeta.worker.dispatch.WorkerBroadcaster;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ReportConsumerTest {

  @Mock
  private Evaluator keyEvaluator;

  @Mock
  private WorkerBroadcaster broadcaster;

  @Mock
  private GlobalQpsEstimator globalQpsEstimator;

  @Mock
  private ZetaBayesianSM stateMachine;

  private ReportConsumer consumer;

  @BeforeEach
  void setUp() {
    consumer = new ReportConsumer(keyEvaluator, broadcaster, globalQpsEstimator, stateMachine, 0L);
  }

  @Test
  void shouldProcessEntriesAndFeedAllComponents() {
    ReportMessage message = new ReportMessage(
      0L,
      "testApp",
      System.currentTimeMillis(),
      Map.of("key1", 5L, "key2", 3L)
    );
    when(keyEvaluator.evaluate(anyString(), anyLong(), anyDouble())).thenReturn(ZetaDecision.none("key", null));

    consumer.onReport(message);

    verify(keyEvaluator).evaluate(eq("key1"), eq(5L), anyDouble());
    verify(keyEvaluator).evaluate(eq("key2"), eq(3L), anyDouble());
    verify(globalQpsEstimator).addTotal(8L);
  }

  @Test
  void shouldBroadcastHotWhenStateMachineReturnsHot() {
    ReportMessage message = new ReportMessage(0L, "testApp", System.currentTimeMillis(), Map.of("hotKey", 100L));
    when(keyEvaluator.evaluate(eq("hotKey"), eq(100L), anyDouble())).thenReturn(ZetaDecision.hot("hotKey", null));

    consumer.onReport(message);

    verify(broadcaster).broadcastHot("hotKey");
  }

  @Test
  void shouldSkipStaleMessagesWhenFilterEnabled() {
    consumer = new ReportConsumer(keyEvaluator, broadcaster, globalQpsEstimator, stateMachine, 5000L);
    ReportMessage message = new ReportMessage(0L, "testApp", System.currentTimeMillis() - 10_000, Map.of("key", 1L));
    consumer.onReport(message);

    verify(keyEvaluator, never()).evaluate(anyString(), anyLong(), anyDouble());
  }

  @Test
  void shouldProcessOldMessageWhenFilterDisabledByDefault() {
    ReportMessage message = new ReportMessage(0L, "testApp", System.currentTimeMillis() - 10_000, Map.of("key", 1L));
    when(keyEvaluator.evaluate(eq("key"), eq(1L), anyDouble())).thenReturn(ZetaDecision.none("key", null));

    consumer.onReport(message);

    verify(keyEvaluator).evaluate(eq("key"), eq(1L), anyDouble());
    verify(globalQpsEstimator).addTotal(1L);
  }

  @Test
  void shouldProcessMessageWhenReporterClockAhead() {
    consumer = new ReportConsumer(keyEvaluator, broadcaster, globalQpsEstimator, stateMachine, 5000L);
    ReportMessage message = new ReportMessage(0L, "testApp", System.currentTimeMillis() + 10_000, Map.of("key", 1L));
    when(keyEvaluator.evaluate(eq("key"), eq(1L), anyDouble())).thenReturn(ZetaDecision.none("key", null));

    consumer.onReport(message);

    verify(keyEvaluator).evaluate(eq("key"), eq(1L), anyDouble());
    verify(globalQpsEstimator).addTotal(1L);
  }

  @Test
  void shouldHandleExceptionsGracefully() {
    ReportMessage message = new ReportMessage(0L, "testApp", System.currentTimeMillis(), Map.of("badKey", 1L));
    when(keyEvaluator.evaluate(eq("badKey"), eq(1L), anyDouble())).thenThrow(new RuntimeException("test error"));

    consumer.onReport(message);

    verify(globalQpsEstimator).addTotal(1L);
  }

  @Test
  void shouldHandleEmptyCountsMap() {
    ReportMessage message = new ReportMessage(0L, "testApp", System.currentTimeMillis(), Map.of());
    consumer.onReport(message);
    verify(globalQpsEstimator, never()).addTotal(anyLong());
  }

  @Test
  void shouldHandleCountAtMaxIntegerBoundary() {
    ReportMessage message = new ReportMessage(
      0L,
      "testApp",
      System.currentTimeMillis(),
      Map.of("bigKey", (long) Integer.MAX_VALUE)
    );
    when(keyEvaluator.evaluate(eq("bigKey"), eq((long) Integer.MAX_VALUE), anyDouble())).thenReturn(
      ZetaDecision.none("bigKey", null)
    );

    consumer.onReport(message);

    verify(globalQpsEstimator).addTotal((long) Integer.MAX_VALUE);
  }

  @Test
  void shouldHandleNegativeCounts() {
    ReportMessage message = new ReportMessage(0L, "testApp", System.currentTimeMillis(), Map.of("negKey", -5L));
    when(keyEvaluator.evaluate(eq("negKey"), eq(-5L), anyDouble())).thenReturn(ZetaDecision.none("negKey", null));

    consumer.onReport(message);

    verify(keyEvaluator).evaluate(eq("negKey"), eq(-5L), anyDouble());
    verify(globalQpsEstimator).addTotal(-5L);
  }

  @Test
  void shouldProcessMessageUnderStalenessBoundary() {
    consumer = new ReportConsumer(keyEvaluator, broadcaster, globalQpsEstimator, stateMachine, 100_000L);
    long now = System.currentTimeMillis();
    ReportMessage message = new ReportMessage(0L, "testApp", now - 1, Map.of("key", 1L));
    when(keyEvaluator.evaluate(eq("key"), eq(1L), anyDouble())).thenReturn(ZetaDecision.none("key", null));

    consumer.onReport(message);

    verify(keyEvaluator).evaluate(eq("key"), eq(1L), anyDouble());
  }

  @Test
  void shouldNotBroadcastWhenDecisionIsNone() {
    ReportMessage message = new ReportMessage(0L, "testApp", System.currentTimeMillis(), Map.of("key", 1L));
    when(keyEvaluator.evaluate(eq("key"), eq(1L), anyDouble())).thenReturn(ZetaDecision.none("key", null));

    consumer.onReport(message);

    verify(broadcaster, never()).broadcastHot(anyString());
    verify(broadcaster, never()).broadcastCool(anyString());
  }

  @Test
  void shouldBroadcastCoolWhenStateMachineReturnsCool() {
    ReportMessage message = new ReportMessage(0L, "testApp", System.currentTimeMillis(), Map.of("coolKey", 1L));
    when(keyEvaluator.evaluate(eq("coolKey"), eq(1L), anyDouble())).thenReturn(ZetaDecision.cool("coolKey", null));

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
    when(keyEvaluator.evaluate(eq("goodKey"), eq(1L), anyDouble())).thenReturn(ZetaDecision.none("goodKey", null));
    when(keyEvaluator.evaluate(eq("badKey"), eq(2L), anyDouble())).thenThrow(new RuntimeException("evaluation error"));

    consumer.onReport(message);

    verify(keyEvaluator).evaluate(eq("goodKey"), eq(1L), anyDouble());
    verify(globalQpsEstimator).addTotal(3L);
  }

  @Test
  void broadcastHotSuccess_shouldNotRollback() {
    ReportMessage message = new ReportMessage(0L, "testApp", System.currentTimeMillis(), Map.of("hotKey", 100L));
    when(keyEvaluator.evaluate(eq("hotKey"), eq(100L), anyDouble())).thenReturn(ZetaDecision.hot("hotKey", null));

    consumer.onReport(message);

    verify(broadcaster).broadcastHot("hotKey");
  }

  @Test
  void broadcastFailure_withNullSnapshot_shouldResetKeyNotThrow() {
    ReportMessage message = new ReportMessage(0L, "testApp", System.currentTimeMillis(), Map.of("newKey", 100L));
    when(keyEvaluator.evaluate(eq("newKey"), eq(100L), anyDouble())).thenReturn(ZetaDecision.hot("newKey", null));
    when(broadcaster.broadcastHot("newKey")).thenReturn(false);

    consumer.onReport(message);

    verify(stateMachine).rollbackToPreviousState(eq("newKey"), isNull());
    verify(broadcaster).broadcastHot("newKey");
  }

  @Test
  void broadcastCoolSuccess_shouldNotRollback() {
    ReportMessage message = new ReportMessage(0L, "testApp", System.currentTimeMillis(), Map.of("coolKey", 1L));
    when(keyEvaluator.evaluate(eq("coolKey"), eq(1L), anyDouble())).thenReturn(ZetaDecision.cool("coolKey", null));

    consumer.onReport(message);

    verify(broadcaster).broadcastCool("coolKey");
  }

  /**
   * Verifies the global window total is sampled exactly once per batch (not
   * once per key) and that every key of the batch sees the same ratio.
   */
  @Test
  void shouldSampleGlobalTotalOncePerBatchAndPassSameRatioToAllKeys() {
    ReportMessage message = new ReportMessage(
      0L,
      "testApp",
      System.currentTimeMillis(),
      Map.of("k1", 1L, "k2", 2L, "k3", 3L)
    );
    when(keyEvaluator.evaluate(anyString(), anyLong(), anyDouble())).thenReturn(ZetaDecision.none("k", null));
    when(globalQpsEstimator.getWindowTotal()).thenReturn(0L);

    consumer.onReport(message);

    verify(globalQpsEstimator, times(1)).getWindowTotal();
    ArgumentCaptor<Double> ratios = ArgumentCaptor.forClass(Double.class);
    verify(keyEvaluator, times(3)).evaluate(anyString(), anyLong(), ratios.capture());
    assertThat(ratios.getAllValues()).containsExactly(1.0, 1.0, 1.0);
  }

  /**
   * Verifies the batch ratio semantics: the first processed batch (no previous
   * sample) and a zero sampled total both yield the neutral ratio 1.0 (never a
   * division by zero and never the 0.1-clamp 10x trend inflation); a
   * meaningful sample compares against the previous batch's sample.
   */
  @Test
  void shouldComputeBatchRatioAgainstPreviousBatchSample() {
    when(keyEvaluator.evaluate(anyString(), anyLong(), anyDouble())).thenReturn(ZetaDecision.none("k", null));
    when(globalQpsEstimator.getWindowTotal()).thenReturn(0L, 1000L, 2000L);

    consumer.onReport(batch("k1", 1L)); // total 0            -> neutral 1.0
    consumer.onReport(batch("k2", 1L)); // 1000 vs prev 0    -> neutral 1.0
    consumer.onReport(batch("k3", 1L)); // 2000 vs prev 1000 -> 2.0

    ArgumentCaptor<Double> ratios = ArgumentCaptor.forClass(Double.class);
    verify(keyEvaluator, times(3)).evaluate(anyString(), anyLong(), ratios.capture());
    assertThat(ratios.getAllValues()).containsExactly(1.0, 1.0, 2.0);
  }

  /**
   * Verifies the broadcast-failure WARN is rate-limited to one log per 10s
   * window (ADR-0037 convention): a second failure inside the window is
   * logged silently (rollback still applied), a failure after the window
   * logs again.
   */
  @Test
  void broadcastFailureWarnIsRateLimitedToTenSecondWindow() {
    ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(
      ReportConsumer.class
    );
    ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender =
      new ch.qos.logback.core.read.ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    try {
      when(keyEvaluator.evaluate(anyString(), anyLong(), anyDouble())).thenReturn(ZetaDecision.hot("k", null));
      when(broadcaster.broadcastHot("k")).thenReturn(false);

      consumer.onReport(batch("k", 1L));
      consumer.onReport(batch("k", 1L)); // inside the 10s window -> suppressed

      assertThat(warnCount(appender, "Failed to broadcast")).isEqualTo(1);

      io.github.hyshmily.zeta.util.TimeSource.setTimeOffsetForTest(0, 11_000);
      try {
        consumer.onReport(batch("k", 1L)); // window elapsed -> logs again
        assertThat(warnCount(appender, "Failed to broadcast")).isEqualTo(2);
      } finally {
        io.github.hyshmily.zeta.util.TimeSource.setTimeOffsetForTest(0, 0);
      }
    } finally {
      logger.detachAppender(appender);
    }
  }

  private long warnCount(
    ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender,
    String fragment
  ) {
    return appender.list
      .stream()
      .filter(e -> e.getLevel() == ch.qos.logback.classic.Level.WARN)
      .filter(e -> e.getFormattedMessage().contains(fragment))
      .count();
  }

  private ReportMessage batch(String key, long count) {
    return new ReportMessage(0L, "testApp", System.currentTimeMillis(), Map.of(key, count));
  }

  // ── Buffered broadcast path (ADR-0061) ──

  /**
   * Verifies the buffered path: decisions are handed to the
   * {@link WorkerBroadcastBuffer} and the broadcast still happens (on the
   * drain thread), with the legacy synchronous drain preserved when no buffer
   * is wired.
   */
  @Test
  void bufferedConsumer_shouldBroadcastHotThroughBuffer() throws Exception {
    io.github.hyshmily.zeta.worker.dispatch.WorkerBroadcastBuffer buffer =
      new io.github.hyshmily.zeta.worker.dispatch.WorkerBroadcastBuffer(100);
    consumer = new ReportConsumer(keyEvaluator, broadcaster, globalQpsEstimator, stateMachine, 0L, buffer);
    try {
      ReportMessage message = new ReportMessage(0L, "testApp", System.currentTimeMillis(), Map.of("hotKey", 100L));
      when(keyEvaluator.evaluate(eq("hotKey"), eq(100L), anyDouble())).thenReturn(ZetaDecision.hot("hotKey", null));
      when(broadcaster.broadcastHot("hotKey")).thenReturn(true);

      consumer.onReport(message);

      org.mockito.Mockito.verify(broadcaster, org.mockito.Mockito.timeout(2000)).broadcastHot("hotKey");
      org.mockito.Mockito.verify(stateMachine, org.mockito.Mockito.timeout(2000).times(0)).rollbackToPreviousState(
        anyString(),
        any()
      );
    } finally {
      buffer.shutdown();
    }
  }

  /**
   * Verifies the buffered failure semantics: a send failure surfaced through
   * the buffer applies the state rollback on the drain thread.
   */
  @Test
  void bufferedConsumer_failedSend_shouldRollBackState() throws Exception {
    io.github.hyshmily.zeta.worker.dispatch.WorkerBroadcastBuffer buffer =
      new io.github.hyshmily.zeta.worker.dispatch.WorkerBroadcastBuffer(100);
    consumer = new ReportConsumer(keyEvaluator, broadcaster, globalQpsEstimator, stateMachine, 0L, buffer);
    try {
      ReportMessage message = new ReportMessage(0L, "testApp", System.currentTimeMillis(), Map.of("newKey", 100L));
      when(keyEvaluator.evaluate(eq("newKey"), eq(100L), anyDouble())).thenReturn(ZetaDecision.hot("newKey", null));
      when(broadcaster.broadcastHot("newKey")).thenReturn(false);

      consumer.onReport(message);

      org.mockito.Mockito.verify(stateMachine, org.mockito.Mockito.timeout(2000)).rollbackToPreviousState(
        eq("newKey"),
        isNull()
      );
    } finally {
      buffer.shutdown();
    }
  }
}
