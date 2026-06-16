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
package io.github.hyshmily.hotkey.worker.dispatch;

import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.sync.WorkerMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.concurrent.atomic.AtomicLong;

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link WorkerBroadcaster}.
 */
@ExtendWith(MockitoExtension.class)
class WorkerBroadcasterTest {

  @Mock
  private RabbitTemplate rabbitTemplate;

  @Mock
  private HotKeyStateMachine stateMachine;

  @Captor
  private ArgumentCaptor<Message> messageCaptor;

  private WorkerBroadcaster broadcaster;
  private final AtomicLong configTimestampCounter = new AtomicLong(0);

  @BeforeEach
  void setUp() {
    broadcaster = new WorkerBroadcaster(rabbitTemplate, "hotkey.broadcast.exchange", "testApp", stateMachine, configTimestampCounter);
  }

  /**
   * Verifies that {@code broadcastHot} sends a message with the correct routing key and AMQP headers.
   */
  @Test
  void shouldSendHotWithCorrectRoutingKeyAndHeaders() {
    broadcaster.broadcastHot("myKey", "test_source");
    verify(rabbitTemplate).send(eq("hotkey.broadcast.exchange"), eq(ROUTING_KEY_BROADCAST + "testApp"), messageCaptor.capture());
    Message sent = messageCaptor.getValue();
    assertThat(new String(sent.getBody())).isEqualTo("myKey");
    assertThat(sent.getMessageProperties().getHeaders().get(AMQP_HEADER_TYPE)).isEqualTo(WorkerMessage.TYPE_HOT);
    assertThat(sent.getMessageProperties().getHeaders().get(AMQP_HEADER_IS_VERSION_DEGRADED)).isEqualTo(false);
    assertThat((Long) sent.getMessageProperties().getHeaders().get(AMQP_HEADER_VERSION)).isPositive();
  }

  /**
   * Verifies that {@code broadcastCool} sends a message with the correct routing key and AMQP headers.
   */
  @Test
  void shouldSendCoolWithCorrectRoutingKeyAndHeaders() {
    broadcaster.broadcastCool("myKey");
    verify(rabbitTemplate).send(eq("hotkey.broadcast.exchange"), eq(ROUTING_KEY_BROADCAST + "testApp"), messageCaptor.capture());
    Message sent = messageCaptor.getValue();
    assertThat(new String(sent.getBody())).isEqualTo("myKey");
    assertThat(sent.getMessageProperties().getHeaders().get(AMQP_HEADER_TYPE)).isEqualTo(WorkerMessage.TYPE_COOL);
    assertThat(sent.getMessageProperties().getHeaders().get(AMQP_HEADER_IS_VERSION_DEGRADED)).isEqualTo(false);
    assertThat((Long) sent.getMessageProperties().getHeaders().get(AMQP_HEADER_VERSION)).isPositive();
  }

  /**
   * Verifies that the decision version counter increments on each successive broadcast.
   */
  @Test
  void decisionVersionShouldIncrementOnEachBroadcast() {
    broadcaster.broadcastCool("k0");
    broadcaster.broadcastHot("k1", "s1");
    broadcaster.broadcastHot("k2", "s1");
    broadcaster.broadcastHot("k3", "s1");

    verify(rabbitTemplate, times(4)).send(any(), eq(ROUTING_KEY_BROADCAST + "testApp"), messageCaptor.capture());
    long v0 = (Long) messageCaptor.getAllValues().get(0).getMessageProperties().getHeaders().get(AMQP_HEADER_VERSION);
    long v3 = (Long) messageCaptor.getAllValues().get(3).getMessageProperties().getHeaders().get(AMQP_HEADER_VERSION);
    assertThat(v3).isGreaterThan(v0);
  }

  /**
   * Verifies that broadcasting with an empty key string does not fail.
   */
  @Test
  void shouldHandleEmptyKey() {
    broadcaster.broadcastHot("", "source");
    verify(rabbitTemplate).send(any(), any(), messageCaptor.capture());
    assertThat(new String(messageCaptor.getValue().getBody())).isEmpty();
  }

  /**
   * Verifies that {@code getCurrentDecisionVersion} returns the current value
   * without incrementing it.
   */
  @Test
  void getCurrentDecisionVersionShouldNotIncrement() {
    long v1 = broadcaster.getCurrentDecisionVersion();
    long v2 = broadcaster.getCurrentDecisionVersion();
    assertThat(v2).isEqualTo(v1);
  }

  /**
   * Verifies that broadcastHot and broadcastCool both increment the decision version.
   */
  @Test
  void bothHotAndCoolShouldIncrementDecisionVersion() {
    broadcaster.broadcastHot("k1", "s1");
    broadcaster.broadcastCool("k2");
    broadcaster.broadcastHot("k3", "s1");

    verify(rabbitTemplate, times(3)).send(any(), any(), messageCaptor.capture());
    long v0 = (Long) messageCaptor.getAllValues().get(0).getMessageProperties().getHeaders().get(AMQP_HEADER_VERSION);
    long v1 = (Long) messageCaptor.getAllValues().get(1).getMessageProperties().getHeaders().get(AMQP_HEADER_VERSION);
    long v2 = (Long) messageCaptor.getAllValues().get(2).getMessageProperties().getHeaders().get(AMQP_HEADER_VERSION);
    assertThat(v1).isGreaterThan(v0);
    assertThat(v2).isGreaterThan(v1);
  }
}
