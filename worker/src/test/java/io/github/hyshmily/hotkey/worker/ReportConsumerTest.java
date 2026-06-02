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
package io.github.hyshmily.hotkey.worker;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.entity.HotKeyDecision;
import io.github.hyshmily.hotkey.report.ReportMessage;
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

  @Test
  void shouldProcessEntriesAndFeedAllComponents() {
    ReportMessage message = new ReportMessage("testApp", System.currentTimeMillis(), Map.of("key1", 5L, "key2", 3L));
    when(detector.addCount(anyString(), anyLong())).thenReturn(false);
    when(stateMachine.evaluate(anyString(), anyBoolean())).thenReturn(HotKeyDecision.none("key"));

    consumer.onReport(message);

    verify(workerTopK).add("key1", 5);
    verify(workerTopK).add("key2", 3);
    verify(detector).addCount("key1", 5L);
    verify(detector).addCount("key2", 3L);
    verify(globalQpsEstimator).addTotal(8L);
  }

  @Test
  void shouldBroadcastHotWhenStateMachineReturnsHot() {
    ReportMessage message = new ReportMessage("testApp", System.currentTimeMillis(), Map.of("hotKey", 100L));
    when(detector.addCount("hotKey", 100L)).thenReturn(true);
    when(stateMachine.evaluate("hotKey", true)).thenReturn(HotKeyDecision.hot("hotKey"));

    consumer.onReport(message);

    verify(broadcaster).broadcastHot("hotKey", "sliding_window");
    verify(topKValidator).markConfirmed("hotKey");
  }

  @Test
  void shouldBroadcastCoolWhenStateMachineReturnsCool() {
    ReportMessage message = new ReportMessage("testApp", System.currentTimeMillis(), Map.of("coolKey", 1L));
    when(detector.addCount("coolKey", 1L)).thenReturn(false);
    when(stateMachine.evaluate("coolKey", false)).thenReturn(HotKeyDecision.cool("coolKey"));

    consumer.onReport(message);

    verify(broadcaster).broadcastCool("coolKey");
    verify(topKValidator).markCooled("coolKey");
  }

  @Test
  void shouldSkipStaleMessages() {
    ReportMessage message = new ReportMessage("testApp", System.currentTimeMillis() - 10_000, Map.of("key", 1L));
    consumer.onReport(message);

    verify(workerTopK, never()).add(anyString(), anyInt());
    verify(detector, never()).addCount(anyString(), anyLong());
    verify(stateMachine, never()).evaluate(anyString(), anyBoolean());
  }

  @Test
  void shouldHandleExceptionsGracefully() {
    ReportMessage message = new ReportMessage("testApp", System.currentTimeMillis(), Map.of("badKey", 1L));
    when(detector.addCount("badKey", 1L)).thenThrow(new RuntimeException("test error"));

    consumer.onReport(message);

    // should not propagate — other keys still processed
    verify(globalQpsEstimator).addTotal(1L);
  }
}
