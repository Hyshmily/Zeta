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
package io.github.hyshmily.hotkey.sync;

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.*;

import java.nio.charset.StandardCharsets;
import org.springframework.amqp.core.Message;

/**
 * Message from Worker to app instances carrying hot/cool decisions or heartbeats.
 * Travels via {@code hotkey.worker.exchange} (FanoutExchange).
 *
 * <p>No {@code isVersionDegraded} — Worker skips broadcast if Redis GET fails.
 *
 * @param cacheKey        the affected cache key
 * @param type            the decision ({@link #TYPE_HOT}, {@link #TYPE_COOL}, or {@link #TYPE_PING})
 * @param decisionVersion the Worker‑local decision version (monotonically increasing)
 * @param shardIndex      the Worker shard index (for PING; 0 for HOT/COOL)
 * @param timestamp       the heartbeat timestamp (for PING; 0 for HOT/COOL)
 * @param nodeId          the Worker node identifier (for PING; null for HOT/COOL)
 */
public record WorkerMessage(String cacheKey, String type, long decisionVersion, int shardIndex, long timestamp, String nodeId) {
  public static final String TYPE_HOT = "HOT";
  public static final String TYPE_COOL = "COOL";
  public static final String TYPE_PING = "PING";

  /**
   * Deserialize a {@code WorkerMessage} from an AMQP message body and headers.
   * Returns {@code null} when the body is empty.
   *
   * @param msg the incoming AMQP message
   * @return a parsed {@link WorkerMessage}, or {@code null}
   */
  public static WorkerMessage from(Message msg) {
    byte[] body = msg.getBody();
    if (body == null || body.length == 0) {
      return null;
    }

    String cacheKey = new String(body, StandardCharsets.UTF_8);
    String type = msg.getMessageProperties().getHeader(AMQP_HEADER_TYPE);

    long decisionVersion =
      msg.getMessageProperties().getHeader(AMQP_HEADER_VERSION) instanceof Number n ? n.longValue() : VERSION_DEFAULT;
    int shardIndex =
      msg.getMessageProperties().getHeader(AMQP_HEADER_SHARD_INDEX) instanceof Number n ? n.intValue() : 0;
    long timestamp =
      msg.getMessageProperties().getHeader(AMQP_HEADER_TIMESTAMP) instanceof Number n ? n.longValue() : 0L;
    String nodeId =
      msg.getMessageProperties().getHeader(AMQP_HEADER_NODE_ID) instanceof String s ? s : null;
    return new WorkerMessage(cacheKey, type, decisionVersion, shardIndex, timestamp, nodeId);
  }
}
