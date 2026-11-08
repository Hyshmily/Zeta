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
package io.github.hyshmily.zeta.reporting;

import static io.github.hyshmily.zeta.constants.ZetaConstants.Routing.KEY_REPORT;

import io.github.hyshmily.zeta.util.Internal;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * Publishes batched access-add reports to the RabbitMQ report exchange.
 * Each batch is routed to a specific Worker queue via the
 * {@code reportToWorker.<appName>.<nodeId>} routing key.
 */
@RequiredArgsConstructor
@Slf4j
@Internal
public class ReportPublisher {

  /** RabbitMQ template for message publishing. */
  private final RabbitTemplate rabbitTemplate;
  /** Name of the RabbitMQ exchange to which reports are sent. */
  private final String reportExchange;
  /** Application name; prefixes the per-Worker routing key. */
  private final String appName;

  /**
   * Publish a report message for the given target.
   *
   * @param target  the Worker nodeId
   * @param message the report data
   */
  public void publish(String target, ReportMessage message) {
    String routingKey = KEY_REPORT + appName + "." + target;
    rabbitTemplate.convertAndSend(reportExchange, routingKey, message);
    // Only oversized batches are traced: the common small batch is not worth a
    // per-flush debug call, while a >=100-key batch is where serialization and
    // message size actually matter.
    if (message.counts().size() >= 100) {
      log.debug("Published report: target={}, routingKey={}, keys={}", target, routingKey, message.counts().size());
    }
  }
}
