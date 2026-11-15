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
import org.springframework.amqp.core.MessageProperties;

/**
 * The shared AMQP consume envelope of the sync and worker listeners: run the
 * plane's processing body, then ack; on any processing exception report it at
 * ERROR and nack with {@code requeue=false} so a poison message cannot loop —
 * the next application-level write re-sends the operation.
 *
 * <p>The ack runs outside the processing try: an ack failure on an
 * already-processed delivery propagates to the container, which owns channel
 * recovery, rather than being reported as a processing failure and followed by
 * a nack on the same broken channel (design decisions: failure behaviour).
 */
@Internal
public final class AmqpConsume {

  private AmqpConsume() {}

  /**
   * Run {@code body} for one delivery, then ack it. On a processing failure:
   * one ERROR with the truncated body, then nack ({@code requeue=false}) — no
   * ack for that delivery. An {@link Error} is nacked and then rethrown: an
   * escaping error would otherwise leave the delivery unsettled and RabbitMQ
   * would redeliver the same poison message after the channel recovers. A nack
   * that fails while that Error propagates is attached as suppressed, so the
   * channel failure never replaces the original Error.
   *
   * @param channel       the AMQP channel used for ack/nack
   * @param msg           the raw AMQP message (delivery tag + body)
   * @param log           the plane's logger for the failure line
   * @param failurePrefix the ERROR prefix naming the plane ("CacheSync",
   *                      "Worker message")
   * @param body          the plane's processing body; a failure is reported and
   *                      nacked, and an {@link Error} is rethrown after the nack
   * @throws IOException when the final ack, or the nack of a non-Error failure,
   *                     itself fails (propagates to the container)
   */
  public static void ackOrNack(Channel channel, Message msg, Logger log, String failurePrefix, Runnable body)
    throws IOException {
    // Without properties there is no delivery tag to ack or nack; drop the malformed delivery
    // at the one boundary that owns the envelope.
    MessageProperties props = msg.getMessageProperties();
    if (props == null) {
      log.error("{} message has no properties; dropping it without ack", failurePrefix);
      return;
    }
    long tag = props.getDeliveryTag();
    try {
      body.run();
    } catch (Throwable t) {
      // Body is truncated: a PUT payload can be tens of KB and must not be echoed whole.
      log.error(
        "{} processing failed: body={}",
        failurePrefix,
        AmqpMessageReader.abbreviateBody(msg.getBody(), 256),
        t
      );
      if (t instanceof Error error) {
        try {
          channel.basicNack(tag, false, false);
        } catch (Throwable nackFailure) {
          error.addSuppressed(nackFailure);
        }
        throw error;
      }
      channel.basicNack(tag, false, false);
      return;
    }
    channel.basicAck(tag, false);
  }
}
