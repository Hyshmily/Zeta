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

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.ROUTING_KEY_REPORT;

import lombok.RequiredArgsConstructor;
import io.github.hyshmily.hotkey.logging.DefaultLogger;
import io.github.hyshmily.hotkey.logging.HotKeyLogger;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * Publishes access-count reports to the RabbitMQ report exchange.
 * Each report is routed to the shard-specific queue via the
 * {@code report.<appName>.<shardIndex>} routing key.
 */
@RequiredArgsConstructor
public class ReportPublisher {

  private static final HotKeyLogger log = new DefaultLogger(ReportPublisher.class);

  private final RabbitTemplate rabbitTemplate;
  private final String reportExchange;
  private final String appName;

  /**
   * Publish a report message for the given target.
   *
   * @param target  legacy: String.valueOf(shardIndex), CH mode: nodeId
   * @param message the report data
   */
  public void publish(String target, ReportMessage message) {
    String routingKey = ROUTING_KEY_REPORT + appName + "." + target;

    rabbitTemplate.convertAndSend(reportExchange, routingKey, message);
    log.debug("Published report: target={}, keys={}", target, message.counts().size());
  }
}
