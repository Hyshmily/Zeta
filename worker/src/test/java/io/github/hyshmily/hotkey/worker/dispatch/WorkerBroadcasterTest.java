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

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.AMQP_HEADER_IS_VERSION_DEGRADED;
import static io.github.hyshmily.hotkey.constants.HotKeyConstants.AMQP_HEADER_TYPE;
import static io.github.hyshmily.hotkey.constants.HotKeyConstants.AMQP_HEADER_VERSION;
import static io.github.hyshmily.hotkey.constants.HotKeyConstants.ROUTING_KEY_BROADCAST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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

  @BeforeEach
  void setUp() {
    broadcaster = new WorkerBroadcaster(rabbitTemplate, "hotkey.broadcast.exchange", "testApp");
  }

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
}
