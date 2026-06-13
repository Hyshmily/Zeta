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

import lombok.AllArgsConstructor;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.*;

/**
 * Worker-side verification request handler — receives PING from App, replies PONG.
 *
 * <p>Queue: hotkey.verify.ping.{workerId}
 * Uses Direct reply-to (amq.rabbitmq.reply-to) for the response.
 */
@AllArgsConstructor
public class VerifyConsumer {

  private final RabbitTemplate rabbitTemplate;
  private final String workerId;

  /**
   * Handles an incoming PING request from an App instance.
   *
   * <p>Extracts the {@code reply-to} header from the incoming message and
   * sends a PONG response via Direct reply-to ({@code amq.rabbitmq.reply-to}).
   * If no {@code reply-to} is present the message is silently dropped.
   *
   * @param ping the incoming PING message
   */
  public void handlePing(Message ping) {
    String replyTo = ping.getMessageProperties().getReplyTo();
    if (replyTo == null) {
      return;
    }

    MessageProperties pongProps = new MessageProperties();
    pongProps.setHeader(AMQP_HEADER_VERIFY_TYPE, AMQP_HEADER_VERIFY_PONG);
    pongProps.setHeader(AMQP_HEADER_VERIFY_WORKER_ID, workerId);

    Message pong = new Message(new byte[0], pongProps);
    rabbitTemplate.send("", replyTo, pong);
  }
}
