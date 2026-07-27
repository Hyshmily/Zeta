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
package io.github.hyshmily.zeta.worker.rule;

import static io.github.hyshmily.zeta.constants.ZetaConstants.Routing.KEY_FASTLANE_RULES;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * Publishes the full fast-lane rule set to peer Workers (ADR-0025).
 *
 * <p>Broadcasts on the heartbeat exchange with routing key
 * {@code fastlane.rules} over the control-plane (heartbeat-dedicated)
 * connection. Two triggers:
 *
 * <ul>
 *   <li><b>Immediate</b> — invoked by
 *       {@link io.github.hyshmily.zeta.worker.endpoint.FastLaneEndpoint} after
 *       every successful local mutation, so rule changes propagate within
 *       milliseconds.</li>
 *   <li><b>Periodic</b> — scheduled via the shared {@code hotKeyScheduler}
 *       (wired in
 *       {@link io.github.hyshmily.zeta.worker.config.WorkerAutoConfiguration}),
 *       so a fresh or partitioned Worker converges within one gossip interval
 *       even if an immediate broadcast was lost (fire-and-forget, ADR-0007).</li>
 * </ul>
 *
 * <p>Send failures are logged and swallowed — the next periodic broadcast
 * self-heals.
 */
@Internal
@Slf4j
@RequiredArgsConstructor
public class FastLaneRulesBroadcaster {

  /** Control-plane RabbitMQ template (heartbeat-dedicated connection). */
  private final RabbitTemplate rabbitTemplate;
  /** Target topic exchange shared with heartbeats. */
  private final String heartbeatExchange;
  /** Unique identity of this Worker node. */
  private final String nodeId;
  /** Source of the current rule set and its version. */
  private final FastLaneRuleManager ruleManager;
  /** Snowflake generator for the message trace ID. */
  private final SnowflakeIdGenerator snowflakeIdGenerator;

  /**
   * Broadcasts the current full rule set with its version to all peer Workers.
   * Fire-and-forget: errors are logged and swallowed.
   */
  public void broadcastNow() {
    try {
      FastLaneRulesMessage msg = new FastLaneRulesMessage(
        snowflakeIdGenerator.nextId(),
        nodeId,
        ruleManager.getRulesVersion(),
        ruleManager.getRules()
      );
      rabbitTemplate.send(heartbeatExchange, KEY_FASTLANE_RULES, msg.toMessage());
      log.debug("Fast-lane rules gossip sent: version={}, rules={}", msg.rulesVersion(), msg.rules().size());
    } catch (Exception e) {
      log.warn("Failed to broadcast fast-lane rules gossip; next periodic cycle will retry", e);
    }
  }
}
