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
package io.github.hyshmily.zeta.util.amqp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * Contract tests for the shared AMQP consume envelope.
 */
class AmqpConsumeTest {

  @Test
  void aMessageWithoutPropertiesIsDroppedWithoutAckOrProcessing() throws Exception {
    Channel channel = mock(Channel.class);
    Message message = mock(Message.class);
    when(message.getMessageProperties()).thenReturn(null);
    AtomicBoolean processed = new AtomicBoolean();

    AmqpConsume.ackOrNack(channel, message, mock(Logger.class), "test", () -> processed.set(true));

    assertThat(processed).isFalse();
    verifyNoInteractions(channel);
  }

  @Test
  void error_shouldSurviveAFailingNack() throws Exception {
    Channel channel = mock(Channel.class);
    doThrow(new IOException("channel closed")).when(channel).basicNack(7L, false, false);
    Message message = new Message(new byte[] { 1 }, new MessageProperties());
    message.getMessageProperties().setDeliveryTag(7L);
    AssertionError failure = new AssertionError("body failed");

    assertThatThrownBy(() ->
      AmqpConsume.ackOrNack(channel, message, mock(Logger.class), "test", () -> {
        throw failure;
      })
    ).isSameAs(failure);
    verify(channel).basicNack(7L, false, false);
    assertThat(failure.getSuppressed()).hasSize(1);
    assertThat(failure.getSuppressed()[0]).isInstanceOf(IOException.class);
  }
}
