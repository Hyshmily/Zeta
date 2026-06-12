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

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

import java.nio.charset.StandardCharsets;

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.*;

/**
 * Enhanced heartbeat message broadcast by the Worker to all App instances.
 *
 * <p>Replaces the old PING-only heartbeat with structured fields including
 * epoch (Worker restart detection), decisionVersionHwm (version watermark),
 * loadFactor (scheduling hint), readyToServe (cold-start guard), and
 * state machine configuration fields for gossip-based config sync.
 *
 * <p>Broadcast on the dedicated {@code hotkey.heartbeat.exchange} (Topic).
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
    long configTimestamp) {

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
    props.setHeader("hbConfigConfirm", configConfirmCount);
    props.setHeader("hbConfigCool", configCoolCount);
    props.setHeader("hbConfigGrace", configGraceCount);
    props.setHeader("hbConfigTs", configTimestamp);

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
        h.getHeader("hbConfigConfirm") instanceof Number n ? n.intValue() : 0,
        h.getHeader("hbConfigCool") instanceof Number n ? n.intValue() : 0,
        h.getHeader("hbConfigGrace") instanceof Number n ? n.intValue() : 0,
        h.getHeader("hbConfigTs") instanceof Number n ? n.longValue() : 0
    );
  }
}
