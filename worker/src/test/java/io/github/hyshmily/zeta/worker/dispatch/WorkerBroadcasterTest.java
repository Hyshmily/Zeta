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
package io.github.hyshmily.zeta.worker.dispatch;

import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.*;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Exchange.*;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Routing.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.sync.worker.WorkerMessage;
import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * Tests for {@link WorkerBroadcaster}.
 */
@ExtendWith(MockitoExtension.class)
class WorkerBroadcasterTest {

  @Mock
  private RabbitTemplate rabbitTemplate;

  @Captor
  private ArgumentCaptor<Message> messageCaptor;

  private WorkerBroadcaster broadcaster;
  private final AtomicLong epochCounter = new AtomicLong(0);

  @BeforeEach
  void setUp() {
    broadcaster = new WorkerBroadcaster(rabbitTemplate, BROADCAST, "testApp", "test-node", epochCounter, mock(SnowflakeIdGenerator.class));
  }

  /**
   * Verifies that {@code broadcastHot} sends a message with the correct routing key and AMQP headers.
   */
  @Test
  void shouldSendHotWithCorrectRoutingKeyAndHeaders() {
    broadcaster.broadcastHot("myKey");
    verify(rabbitTemplate).send(eq(BROADCAST), eq(KEY_BROADCAST + "testApp"), messageCaptor.capture());
    Message sent = messageCaptor.getValue();
    assertThat(new String(sent.getBody())).isEqualTo("myKey");
    assertThat(sent.getMessageProperties().getHeaders().get(HEADER_TYPE)).isEqualTo(WorkerMessage.TYPE_HOT);
    assertThat(sent.getMessageProperties().getHeaders().get(HEADER_IS_VERSION_DEGRADED)).isEqualTo(false);
    assertThat((Long) sent.getMessageProperties().getHeaders().get(HEADER_VERSION)).isPositive();
  }

  /**
   * Verifies that {@code broadcastCool} sends a message with the correct routing key and AMQP headers.
   */
  @Test
  void shouldSendCoolWithCorrectRoutingKeyAndHeaders() {
    broadcaster.broadcastCool("myKey");
    verify(rabbitTemplate).send(eq(BROADCAST), eq(KEY_BROADCAST + "testApp"), messageCaptor.capture());
    Message sent = messageCaptor.getValue();
    assertThat(new String(sent.getBody())).isEqualTo("myKey");
    assertThat(sent.getMessageProperties().getHeaders().get(HEADER_TYPE)).isEqualTo(WorkerMessage.TYPE_COOL);
    assertThat(sent.getMessageProperties().getHeaders().get(HEADER_IS_VERSION_DEGRADED)).isEqualTo(false);
    assertThat((Long) sent.getMessageProperties().getHeaders().get(HEADER_VERSION)).isPositive();
  }

  /**
   * Verifies that the decision version counter increments on each successive send.
   */
  @Test
  void decisionVersionShouldIncrementOnEachBroadcast() {
    broadcaster.broadcastCool("k0");
    broadcaster.broadcastHot("k1");
    broadcaster.broadcastHot("k2");
    broadcaster.broadcastHot("k3");

    verify(rabbitTemplate, times(4)).send(any(), eq(KEY_BROADCAST + "testApp"), messageCaptor.capture());
    long v0 = (Long) messageCaptor.getAllValues().get(0).getMessageProperties().getHeaders().get(HEADER_VERSION);
    long v3 = (Long) messageCaptor.getAllValues().get(3).getMessageProperties().getHeaders().get(HEADER_VERSION);
    assertThat(v3).isGreaterThan(v0);
  }

  /**
   * Verifies that broadcasting with an empty key string does not fail.
   */
  @Test
  void shouldHandleEmptyKey() {
    broadcaster.broadcastHot("");
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
    broadcaster.broadcastHot("k1");
    broadcaster.broadcastCool("k2");
    broadcaster.broadcastHot("k3");

    verify(rabbitTemplate, times(3)).send(any(), any(), messageCaptor.capture());
    long v0 = (Long) messageCaptor.getAllValues().get(0).getMessageProperties().getHeaders().get(HEADER_VERSION);
    long v1 = (Long) messageCaptor.getAllValues().get(1).getMessageProperties().getHeaders().get(HEADER_VERSION);
    long v2 = (Long) messageCaptor.getAllValues().get(2).getMessageProperties().getHeaders().get(HEADER_VERSION);
    assertThat(v1).isGreaterThan(v0);
    assertThat(v2).isGreaterThan(v1);
  }

  /**
   * Verifies that {@code broadcastHot} sets the {@link ZetaConstants.Amqp#HEADER_NODE_ID}
   * header on the sent message.
   */
  @Test
  void sendBroadcast_shouldIncludeNodeIdHeader() {
    broadcaster.broadcastHot("key");
    verify(rabbitTemplate).send(any(), any(), messageCaptor.capture());
    assertThat(messageCaptor.getValue().getMessageProperties().getHeaders()).containsEntry(HEADER_NODE_ID, "test-node");
  }

  /**
   * Verifies that the epoch header reflects the current epoch counter and increments
   * across successive broadcasts.
   */
  @Test
  void sendBroadcast_shouldIncludeEpochHeader() {
    broadcaster.broadcastHot("key");
    epochCounter.incrementAndGet();
    broadcaster.broadcastHot("key2");

    verify(rabbitTemplate, times(2)).send(any(), any(), messageCaptor.capture());
    assertThat(messageCaptor.getAllValues()).hasSize(2);
    assertThat(
      (Long) messageCaptor.getAllValues().get(0).getMessageProperties().getHeaders().get(HEADER_EPOCH)
    ).isEqualTo(0L);
    assertThat(
      (Long) messageCaptor.getAllValues().get(1).getMessageProperties().getHeaders().get(HEADER_EPOCH)
    ).isEqualTo(1L);
  }

  /**
   * Verifies that when {@link RabbitTemplate#send} throws, the error is silently
   * logged and does NOT propagate (fire-and-forget per ADR-0007).
   */
  @Test
  void sendBroadcast_whenRabbitTemplateThrows_shouldSwallow() {
    doThrow(new RuntimeException("connection lost"))
      .when(rabbitTemplate)
      .send(any(String.class), any(String.class), any(Message.class));

    // Should not throw — ADR-0007 fire-and-forget
    broadcaster.broadcastHot("key");
  }

  /**
   * Verifies that broadcastCool also swallows AMQP send failures.
   */
  @Test
  void broadcastCool_whenRabbitTemplateThrows_shouldSwallow() {
    doThrow(new RuntimeException("connection lost"))
      .when(rabbitTemplate)
      .send(any(String.class), any(String.class), any(Message.class));

    broadcaster.broadcastCool("key");
  }
}
