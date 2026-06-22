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
package io.github.hyshmily.hotkey.sync.worker;

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.*;

import io.github.hyshmily.hotkey.sharding.ClusterHealthView;
import java.nio.charset.StandardCharsets;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * Structured heartbeat message broadcast periodically by the Worker to all
 * application instances for cluster health monitoring and gossip-based config
 * synchronization.
 *
 * <p>Broadcast on the dedicated {@code hotkey.heartbeat.exchange} (Topic exchange).
 * Each App instance consumes all heartbeats from all Workers and updates its local
 * {@link ClusterHealthView} accordingly.
 *
 * <p><b>Fields:</b>
 * <ul>
 *   <li>{@code epoch} — Monotonically increasing Worker restart counter. Handles
 *       the case where a Worker dies and its {@code AtomicLong} decision version
 *       resets, allowing App instances to detect restarts.</li>
 *   <li>{@code decisionVersionHwm} — Watermark (high-water mark) of the Worker's
 *       current decision version, used for idempotent decision ordering on the receiver.</li>
 *   <li>{@code loadFactor} — Normalized scheduling hint (0.0–1.0) reflecting the
 *       Worker's current processing load.</li>
 *   <li>{@code readyToServe} — Cold-start guard; {@code false} during Worker
 *       initialization before the first full detection cycle completes.</li>
 *   <li>{@code configFingerprint}, {@code configConfirmCount}, etc. — Gossip-based
 *       state machine configuration fields for decentralized config sync (see ADR-0003).</li>
 * </ul>
 *
 * <p>Serialization is header-based: the body carries only the {@code workerId} as
 * UTF-8 bytes; all structured fields are stored as AMQP message headers for efficient
 * routing and filtering.
 *
 * @param workerId           unique identifier for the originating Worker node
 * @param epoch              Worker restart counter; increases monotonically on each Worker start
 * @param timestamp          wall-clock time when this heartbeat was generated (millis since epoch)
 * @param decisionVersionHwm high-water mark of the Worker's current decision version
 * @param loadFactor         normalized load factor (0.0–1.0) for scheduling hints
 * @param readyToServe       {@code false} during Worker cold-start initialization;
 *                           App instances should not rely on this Worker's decisions yet
 * @param configFingerprint  hash/fingerprint of the Worker's current state machine configuration
 * @param configConfirmCount gossip: number of peers that have confirmed the current config
 * @param configCoolCount    gossip: cool-down period count in the state machine
 * @param configGraceCount   gossip: grace-period count in the state machine
 * @param configTimestamp    gossip: timestamp of the last config change
 *
 * @see ClusterHealthView
 * @see WorkerHeartbeatVerifier
 */
public record WorkerHeartbeatMessage(
  String workerId,
  long epoch,
  long timestamp,
  long decisionVersionHwm,
  double loadFactor,
  boolean readyToServe,
  int configFingerprint,
  int configConfirmCount,
  int configCoolCount,
  int configGraceCount,
  long configTimestamp
) {
  /** Message type discriminator for heartbeat messages ({@value}). */
  public static final String TYPE = "WORKER_HB";

  /**
   * Converts this heartbeat record to an AMQP {@link Message} with all fields
   * set as message headers.
   *
   * <p>The body carries the {@code workerId} as UTF-8 bytes. Headers include
   * type discriminator ({@value #TYPE}), epoch, decision version watermark,
   * load factor, readiness flag, config gossip fields, and the originating
   * node identifier &amp; timestamp.
   *
   * @return the constructed AMQP message
   */
  public Message toMessage() {
    MessageProperties props = new MessageProperties();

    props.setHeader(AMQP_HEADER_TYPE, TYPE);
    props.setHeader(AMQP_HEADER_HEARTBEAT_EPOCH, epoch);
    props.setHeader(AMQP_HEADER_HEARTBEAT_DV_HWM, decisionVersionHwm);
    props.setHeader(AMQP_HEADER_HEARTBEAT_LOAD, loadFactor);
    props.setHeader(AMQP_HEADER_HEARTBEAT_READY, readyToServe);
    props.setHeader(AMQP_HEADER_HEARTBEAT_CONFIG_FP, configFingerprint);
    props.setHeader(AMQP_HEADER_NODE_ID, workerId);
    props.setHeader(AMQP_HEADER_TIMESTAMP, timestamp);
    props.setHeader(AMQP_HEADER_HEARTBEAT_CONFIG_CONFIRM, configConfirmCount);
    props.setHeader(AMQP_HEADER_HEARTBEAT_CONFIG_COOL, configCoolCount);
    props.setHeader(AMQP_HEADER_HEARTBEAT_CONFIG_GRACE, configGraceCount);
    props.setHeader(AMQP_HEADER_HEARTBEAT_CONFIG_TIMESTAMP, configTimestamp);

    return new Message(workerId.getBytes(StandardCharsets.UTF_8), props);
  }

  /**
   * Deserializes a {@link WorkerHeartbeatMessage} from an AMQP {@link Message}.
   *
   * <p>All fields are extracted from message headers. Returns {@code null} if the
   * message type header does not match {@value #TYPE} or if the message properties
   * are null.
   * <p>
   * Missing or malformed header values are silently defaulted (0, 0.0, false, or
   * empty string as appropriate) rather than causing a parse failure.
   *
   * @param msg the incoming AMQP message; may be null (returns null)
   * @return the deserialized heartbeat, or {@code null} if the type is wrong or
   *         properties are null
   */
  public static WorkerHeartbeatMessage from(Message msg) {
    var h = msg.getMessageProperties();
    if (h == null) {
      return null;
    }
    if (!TYPE.equals(h.getHeader(AMQP_HEADER_TYPE))) {
      return null;
    }

    return new WorkerHeartbeatMessage(
      h.getHeader(AMQP_HEADER_NODE_ID) instanceof String s ? s : "",
      h.getHeader(AMQP_HEADER_HEARTBEAT_EPOCH) instanceof Number n ? n.longValue() : 0,
      h.getHeader(AMQP_HEADER_TIMESTAMP) instanceof Number n ? n.longValue() : 0,
      h.getHeader(AMQP_HEADER_HEARTBEAT_DV_HWM) instanceof Number n ? n.longValue() : 0,
      h.getHeader(AMQP_HEADER_HEARTBEAT_LOAD) instanceof Number n ? n.doubleValue() : 0.0,
      Boolean.TRUE.equals(h.getHeader(AMQP_HEADER_HEARTBEAT_READY)),
      h.getHeader(AMQP_HEADER_HEARTBEAT_CONFIG_FP) instanceof Integer i ? i : 0,
      h.getHeader(AMQP_HEADER_HEARTBEAT_CONFIG_CONFIRM) instanceof Number n ? n.intValue() : 0,
      h.getHeader(AMQP_HEADER_HEARTBEAT_CONFIG_COOL) instanceof Number n ? n.intValue() : 0,
      h.getHeader(AMQP_HEADER_HEARTBEAT_CONFIG_GRACE) instanceof Number n ? n.intValue() : 0,
      h.getHeader(AMQP_HEADER_HEARTBEAT_CONFIG_TIMESTAMP) instanceof Number n ? n.longValue() : 0
    );
  }
}
