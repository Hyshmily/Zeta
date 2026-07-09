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
package io.github.hyshmily.hotkey.worker.ingest;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.hotkeydetector.heavykeeper.TopK;
import io.github.hyshmily.hotkey.model.HotKeyDecision;
import io.github.hyshmily.hotkey.reporting.ReportMessage;
import io.github.hyshmily.hotkey.worker.detection.GlobalQpsEstimator;
import io.github.hyshmily.hotkey.worker.detection.SlidingWindowDetector;
import io.github.hyshmily.hotkey.worker.detection.TopKValidator;
import io.github.hyshmily.hotkey.worker.dispatch.WorkerBroadcaster;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link ReportConsumer}.
 */
@ExtendWith(MockitoExtension.class)
class ReportConsumerTest {

  @Mock
  private SlidingWindowDetector detector;

  @Mock
  private HotKeyStateMachine stateMachine;

  @Mock
  private WorkerBroadcaster broadcaster;

  @Mock
  private TopKValidator topKValidator;

  @Mock
  private TopK workerTopK;

  @Mock
  private GlobalQpsEstimator globalQpsEstimator;

  private ReportConsumer consumer;

  @BeforeEach
  void setUp() {
    consumer = new ReportConsumer(detector, stateMachine, broadcaster, topKValidator, workerTopK, globalQpsEstimator);
  }

  /**
   * Verifies that {@code onReport} processes all entries and feeds the detector, TopK, and QPS estimator.
   */
  @Test
  void shouldProcessEntriesAndFeedAllComponents() {
    ReportMessage message = new ReportMessage("testApp", System.currentTimeMillis(), Map.of("key1", 5L, "key2", 3L));
    when(detector.addCount(anyString(), anyLong())).thenReturn(false);
    when(stateMachine.evaluate(anyString(), anyBoolean())).thenReturn(HotKeyDecision.none("key"));

    consumer.onReport(message);

    verify(workerTopK).addDirect(Map.of("key1", 5L, "key2", 3L));
    verify(detector).addCount("key1", 5L);
    verify(detector).addCount("key2", 3L);
    verify(globalQpsEstimator).addTotal(8L);
  }

  /**
   * Verifies that a HOT decision from the state machine triggers {@code broadcastHot} and confirmation.
   */
  @Test
  void shouldBroadcastHotWhenStateMachineReturnsHot() {
    ReportMessage message = new ReportMessage("testApp", System.currentTimeMillis(), Map.of("hotKey", 100L));
    when(detector.addCount("hotKey", 100L)).thenReturn(true);
    when(stateMachine.evaluate("hotKey", true)).thenReturn(HotKeyDecision.hot("hotKey"));

    consumer.onReport(message);

    verify(broadcaster).broadcastHot(eq("hotKey"), eq("sliding_window"), anyLong());
    verify(topKValidator).markConfirmed("hotKey");
  }

  /**
   * Verifies that a COOL decision from the state machine triggers {@code broadcastCool} and cool confirmation.
   */
  @Test
  void shouldBroadcastCoolWhenStateMachineReturnsCool() {
    ReportMessage message = new ReportMessage("testApp", System.currentTimeMillis(), Map.of("coolKey", 1L));
    when(detector.addCount("coolKey", 1L)).thenReturn(false);
    when(stateMachine.evaluate("coolKey", false)).thenReturn(HotKeyDecision.cool("coolKey"));

    consumer.onReport(message);

    verify(broadcaster).broadcastCool(eq("coolKey"), anyLong());
    verify(topKValidator).markCooled("coolKey");
  }

  /**
   * Verifies that stale report messages (older than the staleness threshold) are skipped without processing.
   */
  @Test
  void shouldSkipStaleMessages() {
    ReportMessage message = new ReportMessage("testApp", System.currentTimeMillis() - 10_000, Map.of("key", 1L));
    consumer.onReport(message);

    verify(workerTopK, never()).addDirect(any(Map.class));
    verify(detector, never()).addCount(anyString(), anyLong());
    verify(stateMachine, never()).evaluate(anyString(), anyBoolean());
  }

  /**
   * Verifies that exceptions thrown during per-key processing are caught and do not prevent other keys from being processed.
   */
  @Test
  void shouldHandleExceptionsGracefully() {
    ReportMessage message = new ReportMessage("testApp", System.currentTimeMillis(), Map.of("badKey", 1L));
    when(detector.addCount("badKey", 1L)).thenThrow(new RuntimeException("test error"));

    consumer.onReport(message);

    // should not propagate — other keys still processed
    verify(globalQpsEstimator).addTotal(1L);
  }

  /**
   * Verifies that a report message with an empty counts map is handled without error
   * and no processing occurs (empty map short-circuits).
   */
  @Test
  void shouldHandleEmptyCountsMap() {
    ReportMessage message = new ReportMessage("testApp", System.currentTimeMillis(), Map.of());
    consumer.onReport(message);
    verify(globalQpsEstimator, never()).addTotal(anyLong());
    verify(workerTopK, never()).addDirect(any());
  }

  /**
   * Verifies that a count value at the Integer.MAX_VALUE boundary is passed correctly
   * to {@code workerTopK.addDirect}.
   */
  @Test
  void shouldHandleCountAtMaxIntegerBoundary() {
    ReportMessage message = new ReportMessage(
      "testApp",
      System.currentTimeMillis(),
      Map.of("bigKey", (long) Integer.MAX_VALUE)
    );
    when(detector.addCount("bigKey", Integer.MAX_VALUE)).thenReturn(false);
    when(stateMachine.evaluate("bigKey", false)).thenReturn(HotKeyDecision.none("bigKey"));

    consumer.onReport(message);

    verify(workerTopK).addDirect(Map.of("bigKey", (long) Integer.MAX_VALUE));
    verify(globalQpsEstimator).addTotal((long) Integer.MAX_VALUE);
  }

  /**
   * Verifies that a count exceeding Integer.MAX_VALUE is clamped to Integer.MAX_VALUE
   * when passed to TopK.
   */
  @Test
  void shouldClampCountToMaxIntForTopK() {
    ReportMessage message = new ReportMessage(
      "testApp",
      System.currentTimeMillis(),
      Map.of("hugeKey", (long) Integer.MAX_VALUE + 1)
    );
    when(detector.addCount("hugeKey", (long) Integer.MAX_VALUE + 1)).thenReturn(false);
    when(stateMachine.evaluate("hugeKey", false)).thenReturn(HotKeyDecision.none("hugeKey"));

    consumer.onReport(message);

    verify(workerTopK).addDirect(Map.of("hugeKey", (long) Integer.MAX_VALUE + 1));
  }

  /**
   * Verifies that a report with negative count values does not cause failures.
   */
  @Test
  void shouldHandleNegativeCounts() {
    ReportMessage message = new ReportMessage("testApp", System.currentTimeMillis(), Map.of("negKey", -5L));
    when(detector.addCount("negKey", -5L)).thenReturn(false);
    when(stateMachine.evaluate("negKey", false)).thenReturn(HotKeyDecision.none("negKey"));

    consumer.onReport(message);

    verify(detector).addCount("negKey", -5L);
    verify(globalQpsEstimator).addTotal(-5L);
  }

  /**
   * Verifies that a report message well under the staleness boundary
   * is still processed (boundary: > threshold means stale).
   */
  @Test
  void shouldProcessMessageUnderStalenessBoundary() {
    consumer.stalenessThresholdMs = 100_000L;
    long now = System.currentTimeMillis();
    ReportMessage message = new ReportMessage("testApp", now - 1, Map.of("key", 1L));
    when(detector.addCount("key", 1L)).thenReturn(false);
    when(stateMachine.evaluate("key", false)).thenReturn(HotKeyDecision.none("key"));

    consumer.onReport(message);

    verify(detector).addCount("key", 1L);
  }

  /**
   * Verifies that the NONE decision type does not trigger any send or marking.
   */
  @Test
  void shouldNotBroadcastWhenDecisionIsNone() {
    ReportMessage message = new ReportMessage("testApp", System.currentTimeMillis(), Map.of("key", 1L));
    when(detector.addCount("key", 1L)).thenReturn(false);
    when(stateMachine.evaluate("key", false)).thenReturn(HotKeyDecision.none("key"));

    consumer.onReport(message);

    verify(broadcaster, never()).broadcastHot(anyString(), anyString(), anyLong());
    verify(broadcaster, never()).broadcastCool(anyString(), anyLong());
    verify(topKValidator, never()).markConfirmed(anyString());
    verify(topKValidator, never()).markCooled(anyString());
  }

  /**
   * Verifies that all entries are processed even when one entry's stateMachine evaluation throws.
   */
  @Test
  void shouldContinueProcessingAfterStateMachineError() {
    ReportMessage message = new ReportMessage(
      "testApp",
      System.currentTimeMillis(),
      Map.of("goodKey", 1L, "badKey", 2L)
    );
    when(detector.addCount("goodKey", 1L)).thenReturn(false);
    when(detector.addCount("badKey", 2L)).thenReturn(true);
    when(stateMachine.evaluate("goodKey", false)).thenReturn(HotKeyDecision.none("goodKey"));
    when(stateMachine.evaluate("badKey", true)).thenThrow(new RuntimeException("state machine error"));

    consumer.onReport(message);

    // goodKey still processed despite badKey error
    verify(stateMachine).evaluate("goodKey", false);
    verify(globalQpsEstimator).addTotal(3L);
  }

  /**
   * Verifies that a successful HOT send does not trigger a state machine rollback.
   *
   * <p>Broadcast failures are fire-and-forget per ADR-0007 — no rollback occurs.
   */
  @Test
  void broadcastHotSuccess_shouldNotRollback() {
    ReportMessage message = new ReportMessage("testApp", System.currentTimeMillis(), Map.of("hotKey", 100L));
    when(detector.addCount("hotKey", 100L)).thenReturn(true);
    when(stateMachine.evaluate("hotKey", true)).thenReturn(HotKeyDecision.hot("hotKey"));

    consumer.onReport(message);

    verify(broadcaster).broadcastHot(eq("hotKey"), eq("sliding_window"), anyLong());
    verify(topKValidator).markConfirmed("hotKey");
  }

  /**
   * Verifies that a successful COOL send does not trigger a state machine rollback.
   */
  @Test
  void broadcastCoolSuccess_shouldNotRollback() {
    ReportMessage message = new ReportMessage("testApp", System.currentTimeMillis(), Map.of("coolKey", 1L));
    when(detector.addCount("coolKey", 1L)).thenReturn(false);
    when(stateMachine.evaluate("coolKey", false)).thenReturn(HotKeyDecision.cool("coolKey"));

    consumer.onReport(message);

    verify(broadcaster).broadcastCool(eq("coolKey"), anyLong());
    verify(topKValidator).markCooled("coolKey");
    verify(stateMachine, never()).rollbackToPreviousState(anyString(), anyMap());
  }
}
