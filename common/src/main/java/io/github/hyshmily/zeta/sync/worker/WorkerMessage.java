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
package io.github.hyshmily.zeta.sync.worker;

import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.*;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Version.*;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;

/**
 * Message from Worker to app instances carrying hot/cool decisions for a single cache key.
 * Travels via the {@code zeta.worker.exchange} FanoutExchange, ensuring every connected
 * app instance receives each decision exactly once (subject to at-most-once delivery).
 *
 * <p>Each {@code WorkerMessage} carries the Worker's monotonically increasing
 * {@code decisionVersion}, which enables receivers to apply idempotent ordering:
 * stale decisions (lower version than the local entry's {@code decisionVersion}) are
 * silently skipped. This version space is orthogonal to the application-level
 * {@code dataVersion} — see ADR-0008 (Dual Version Space).
 *
 * <p>Unlike {@link SyncMessage}, this reportToWorker has no {@code isVersionDegraded} field.
 * The Worker simply skips send entirely if {@code Redis GET} fails during a HOT
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
@Slf4j
@Internal
public record WorkerMessage(
  long id,
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

  private static final AtomicBoolean WARNED_VERSION_TYPE = new AtomicBoolean(false);
  private static final AtomicBoolean WARNED_EPOCH_TYPE = new AtomicBoolean(false);
  private static final AtomicBoolean WARNED_ID_TYPE = new AtomicBoolean(false);

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
    String type = msg.getMessageProperties().getHeader(HEADER_TYPE);

    long decisionVersion = toLong(msg, HEADER_VERSION, VERSION_DEFAULT, WARNED_VERSION_TYPE);
    long timestamp = toLong(msg, HEADER_TIMESTAMP, 0L, null);
    String nodeId = msg.getMessageProperties().getHeader(HEADER_NODE_ID) instanceof String s ? s : null;
    long epoch = toLong(msg, HEADER_EPOCH, 0L, WARNED_EPOCH_TYPE);
    long id = toLong(msg, HEADER_MESSAGE_ID, 0L, WARNED_ID_TYPE);

    return new WorkerMessage(id, cacheKey, type, decisionVersion, timestamp, nodeId, epoch);
  }

  /**
   * Reads a numeric AMQP message header, returning its {@code long} value.
   *
   * <p>If the header value is not a {@link Number} (e.g. a String was sent by
   * a different version of the sender), the method logs a one-time warning
   * via the {@code warnFlag} and returns the {@code defaultValue}. The
   * one-time flag prevents log flooding when every message has the wrong type.
   *
   * <p>When {@code warnFlag} is {@code null}, the warning is silently skipped
   * (used for best-effort headers like {@code timestamp}).
   *
   * @param msg          the AMQP message containing the header
   * @param header       the header name to read
   * @param defaultValue the fallback value when the header is missing or non-numeric
   * @param warnFlag     one-shot warning gate; null to suppress warnings
   * @return the header's numeric value, or {@code defaultValue}
   */
  private static long toLong(Message msg, String header, long defaultValue, AtomicBoolean warnFlag) {
    Object value = msg.getMessageProperties().getHeader(header);
    if (value instanceof Number n) {
      return n.longValue();
    }
    if (value != null && warnFlag != null && warnFlag.compareAndSet(false, true)) {
      log.warn(
        "Non-numeric header '{}' (type {}), defaulting to {}. Value: {}",
        header,
        value.getClass().getName(),
        defaultValue,
        value
      );
    }
    return defaultValue;
  }
}
