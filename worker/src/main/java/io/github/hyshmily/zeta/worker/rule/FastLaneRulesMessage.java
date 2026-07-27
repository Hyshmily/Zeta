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

import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.worker.rule.FastLaneRuleManager.FastLaneRule;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * Worker-to-Worker gossip message carrying the full fast-lane rule set
 * (ADR-0025).
 *
 * <p>Published on the heartbeat exchange with routing key
 * {@code fastlane.rules} over the control-plane connection. Deliberately
 * outside {@code heartbeat.*} so the message is not fanned out to App-side
 * heartbeat queues — fast-lane rules are Worker-internal state.
 *
 * <p><b>Wire format:</b> metadata rides in AMQP headers
 * ({@code type}, {@code nodeId}, {@code messageId}, {@code fastlaneRulesVersion});
 * the body carries the complete rule set as JSON
 * ({@code [{"keyPattern":"product:*","threshold":500}, ...]}). Full-set
 * transmission is idempotent and self-healing; rule sets are small (tens of
 * entries), so delta sync would add fragility for no measurable gain.
 *
 * @param id           snowflake message ID for end-to-end tracing
 * @param nodeId       unique identifier of the originating Worker node
 * @param rulesVersion wall-clock version of the rule set (ms since epoch);
 *                     last-writer-wins, ties broken by nodeId comparison
 * @param rules        the complete fast-lane rule set (never {@code null},
 *                     may be empty to propagate a "clear all" operation)
 */
@Internal
@Slf4j
public record FastLaneRulesMessage(long id, String nodeId, long rulesVersion, List<FastLaneRule> rules) {
  /** Message type discriminator for fast-lane rules gossip ({@value}). */
  public static final String TYPE = "FASTLANE_RULES";

  /** Shared mapper for rule-set (de)serialization. Thread-safe after configuration. */
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final TypeReference<List<FastLaneRule>> RULE_LIST_TYPE = new TypeReference<>() {};

  /**
   * Converts this message to an AMQP {@link Message} with metadata in headers
   * and the JSON rule set as the body.
   *
   * @return the constructed AMQP message
   * @throws IllegalStateException if the rule set fails to serialize
   */
  public Message toMessage() {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, TYPE);
    props.setHeader(HEADER_NODE_ID, nodeId);
    props.setHeader(HEADER_MESSAGE_ID, id);
    props.setHeader(HEADER_FASTLANE_RULES_VERSION, rulesVersion);

    try {
      return new Message(MAPPER.writeValueAsBytes(rules), props);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to serialize fast-lane rules for gossip", e);
    }
  }

  /**
   * Deserializes a {@link FastLaneRulesMessage} from an AMQP {@link Message}.
   *
   * <p>Returns {@code null} when the message is not of type {@value #TYPE},
   * has no properties, or has an unparseable body — malformed gossip is
   * silently dropped (fire-and-forget, next periodic broadcast self-heals).
   *
   * @param msg the incoming AMQP message; may be null (returns null)
   * @return the deserialized message, or {@code null} if not applicable or malformed
   */
  public static FastLaneRulesMessage from(Message msg) {
    if (msg == null || msg.getMessageProperties() == null) {
      return null;
    }
    var h = msg.getMessageProperties();
    if (!TYPE.equals(h.getHeader(HEADER_TYPE))) {
      return null;
    }
    try {
      List<FastLaneRule> rules = MAPPER.readValue(msg.getBody(), RULE_LIST_TYPE);
      return new FastLaneRulesMessage(
        h.getHeader(HEADER_MESSAGE_ID) instanceof Number n ? n.longValue() : 0,
        h.getHeader(HEADER_NODE_ID) instanceof String s ? s : "",
        h.getHeader(HEADER_FASTLANE_RULES_VERSION) instanceof Number n ? n.longValue() : 0,
        rules == null ? List.of() : rules
      );
    } catch (Exception e) {
      log.warn("Malformed fast-lane rules gossip message, dropping", e);
      return null;
    }
  }
}
