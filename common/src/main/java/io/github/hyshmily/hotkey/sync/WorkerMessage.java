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
 * Message from Worker to app instances carrying hot/cool decisions for a single cache key.
 * Travels via the {@code hotkey.worker.exchange} FanoutExchange, ensuring every connected
 * app instance receives each decision exactly once (subject to at-most-once delivery).
 *
 * <p>Each {@code WorkerMessage} carries the Worker's monotonically increasing
 * {@code decisionVersion}, which enables receivers to apply idempotent ordering:
 * stale decisions (lower version than the local entry's {@code decisionVersion}) are
 * silently skipped. This version space is orthogonal to the application-level
 * {@code dataVersion} — see ADR-0008 (Dual Version Space).
 *
 * <p>Unlike {@link SyncMessage}, this record has no {@code isVersionDegraded} field.
 * The Worker simply skips broadcast entirely if {@code Redis GET} fails during a HOT
 * decision, preventing degraded decision propagation.
 *
 * @param cacheKey        the affected cache key; must not be null
 * @param type            the decision type, one of {@link #TYPE_HOT} or {@link #TYPE_COOL}
 * @param decisionVersion the Worker-local decision version (monotonically increasing
 *                        {@code AtomicLong}); used for idempotent ordering on the receiver
 * @param timestamp       reserved for future use; always {@code 0L} for HOT/COOL decisions
 * @param nodeId          the originating Worker's node identity; used by receivers for
 *                        per-Worker version partitioning (see {@link VersionGuard}) — ADR-0010
 * @param epoch           the originating Worker's restart generation counter; receivers
 *                        unconditionally accept decisions from a higher epoch (see ADR-0010)
 */
public record WorkerMessage(
  String cacheKey,
  String type,
  long decisionVersion,
  long timestamp,
  String nodeId,
  long epoch
) {
  /** Promotes a cache key to hot state — extended TTL, soft expiration enabled. */
  public static final String TYPE_HOT = "HOT";

  /** Downgrades a cache key to cool state — normal TTL, soft expiration disabled. */
  public static final String TYPE_COOL = "COOL";

  /**
   * Deserializes a {@code WorkerMessage} from an AMQP message body and headers.
   * <p>
   * The cache key is read from the message body (UTF-8 decoded). The type,
   * decision version, timestamp, and node ID are read from message headers.
   * Missing or non-numeric version headers default to {@code VERSION_DEFAULT} (0L).
   * Missing or non-String node ID defaults to {@code null}.
   *
   * @param msg the incoming AMQP message; must not be null
   * @return a parsed {@link WorkerMessage}, or {@code null} if the body is empty
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
    long timestamp =
      msg.getMessageProperties().getHeader(AMQP_HEADER_TIMESTAMP) instanceof Number n ? n.longValue() : 0L;
    String nodeId = msg.getMessageProperties().getHeader(AMQP_HEADER_NODE_ID) instanceof String s ? s : null;
    long epoch = msg.getMessageProperties().getHeader(AMQP_HEADER_EPOCH) instanceof Number n ? n.longValue() : 0L;
    return new WorkerMessage(cacheKey, type, decisionVersion, timestamp, nodeId, epoch);
  }
}
