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
package io.github.hyshmily.hotkey.reporting;
import lombok.extern.slf4j.Slf4j;

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.ROUTING_KEY_REPORT;

import lombok.RequiredArgsConstructor;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * Publishes access-count reports to the RabbitMQ report exchange.
 * Each report is routed to the specific Worker queue via the
 * {@code report.<appName>.<nodeId>} routing key.
 */
@RequiredArgsConstructor
@Slf4j
public class ReportPublisher {


  /** RabbitMQ template for message publishing. */
  private final RabbitTemplate rabbitTemplate;
  /** Name of the RabbitMQ exchange to which reports are sent. */
  private final String reportExchange;
  /** Application name; included in the routing key and message payload. */
  private final String appName;

  /**
   * Publish a report message for the given target.
   *
   * @param target  the Worker nodeId
   * @param message the report data
   */
  public void publish(String target, ReportMessage message) {
    String routingKey = ROUTING_KEY_REPORT + appName + "." + target;

    try {
      rabbitTemplate.convertAndSend(reportExchange, routingKey, message);
    } catch (AmqpException e) {
      log.error(
        "Failed to publish report: target={}, keys={}, error={}",
        target,
        message.counts().size(),
        e.getMessage()
      );
    }
    log.debug("Published report: target={}, keys={}", target, message.counts().size());
  }
}
