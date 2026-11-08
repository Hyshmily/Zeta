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

import com.rabbitmq.client.Channel;
import io.github.hyshmily.zeta.util.Internal;
import java.io.IOException;
import org.slf4j.Logger;
import org.springframework.amqp.core.Message;

/**
 * The shared AMQP consume envelope of the sync and worker listeners: run the
 * plane's processing body, then ack; on any processing exception report it at
 * ERROR and nack with {@code requeue=false} so a poison message cannot loop —
 * the next application-level write re-sends the operation.
 *
 * <p><b>Ack outside the processing try.</b> RabbitMQ's basicAck can also fail
 * (IOException, or a RuntimeException such as AlreadyClosedException when the
 * channel is gone); keeping it inside the try meant an ack failure on an
 * already-processed message was logged as a processing failure and then
 * followed by basicNack on the same broken channel. An ack failure propagates
 * to the container, which owns channel recovery.
 */
@Internal
public final class AmqpConsume {

  private AmqpConsume() {}

  /**
   * Run {@code body} for one delivery, then ack it. On a processing failure:
   * one ERROR with the truncated body, then nack ({@code requeue=false}) — no
   * ack for that delivery.
   *
   * @param channel       the AMQP channel used for ack/nack
   * @param msg           the raw AMQP message (delivery tag + body)
   * @param log           the plane's logger for the failure line
   * @param failurePrefix the ERROR prefix naming the plane ("CacheSync",
   *                      "Worker message")
   * @param body          the plane's processing body; exceptions are reported,
   *                      never rethrown
   * @throws IOException when the final ack itself fails (propagates to the container)
   */
  public static void ackOrNack(
    Channel channel,
    Message msg,
    Logger log,
    String failurePrefix,
    Runnable body
  ) throws IOException {
    long tag = msg.getMessageProperties().getDeliveryTag();
    try {
      body.run();
    } catch (Exception e) {
      // Body is truncated: a PUT payload can be tens of KB and must not be echoed whole.
      log.error(
        "{} processing failed: body={}",
        failurePrefix,
        AmqpMessageReader.abbreviateBody(msg.getBody(), 256),
        e
      );
      channel.basicNack(tag, false, false);
      return;
    }
    channel.basicAck(tag, false);
  }
}
