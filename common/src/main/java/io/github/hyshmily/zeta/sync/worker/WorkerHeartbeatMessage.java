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

import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.amqp.AmqpMessageReader;
import java.nio.charset.StandardCharsets;
import org.jspecify.annotations.Nullable;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * Structured heartbeat message sent periodically by the Worker.
 *
 * <p>Broadcast on the dedicated heartbeat exchange (topic). Every App instance consumes each
 * heartbeat and matches it against its own app name before recording it in the local
 * {@link io.github.hyshmily.zeta.sharding.HealthView}.
 *
 * <p><b>Fields:</b>
 * <ul>
 *   <li>{@code epoch} — Worker restart counter. Handles the case where a Worker dies and its
 *       in-memory version counters reset, letting App instances detect restarts.</li>
 *   <li>{@code readyToServe} — cold-start guard; {@code false} before the grace period elapses
 *       with an empty HOT set.</li>
 * </ul>
 *
 * <p>Serialization is header-based: the body carries only the {@code workerId} as UTF-8 bytes.
 *
 * @param id           time-sortable message trace ID; {@code 0} when the header was
 *                     missing or non-numeric
 * @param workerId     unique identifier of the originating Worker node
 * @param epoch        Worker restart counter
 * @param readyToServe {@code false} during cold start
 * @param appName      the sending application name, or {@code null} when the sender
 *                     declares none
 */
@Internal
public record WorkerHeartbeatMessage(
  long id,
  String workerId,
  long epoch,
  boolean readyToServe,
  @Nullable String appName
) {
  /** Message type discriminator for heartbeat messages ({@value}). */
  public static final String TYPE = "WORKER_HB";

  /**
   * Converts this heartbeat to an AMQP {@link Message}.
   *
   * @return the constructed AMQP message
   */
  public Message toMessage() {
    MessageProperties props = new MessageProperties();

    props.setHeader(HEADER_TYPE, TYPE);
    props.setHeader(HEADER_HEARTBEAT_EPOCH, epoch);
    props.setHeader(HEADER_HEARTBEAT_READY, readyToServe);
    props.setHeader(HEADER_NODE_ID, workerId);
    props.setHeader(HEADER_MESSAGE_ID, id);
    if (appName != null && !appName.isBlank()) {
      props.setHeader(HEADER_APP_NAME, appName);
    }

    return new Message(workerId.getBytes(StandardCharsets.UTF_8), props);
  }

  /**
   * Deserializes a heartbeat from an AMQP {@link Message}.
   *
   * <p>Missing or malformed headers are defaulted rather than failing the parse: a heartbeat is a
   * liveness signal, and dropping one because a field is absent would make a healthy Worker look
   * dead. The one exception is the Worker identity: a heartbeat without one cannot be recorded
   * (an empty id would enter the health table and the hash ring), so the node-id header falls
   * back to the body and a blank result drops the message.
   *
   * @param msg the incoming message
   * @return the heartbeat, or {@code null} when the type header does not match or the
   *         Worker identity is missing
   * @throws NullPointerException if {@code msg} is {@code null}
   */
  public static WorkerHeartbeatMessage from(Message msg) {
    var props = msg.getMessageProperties();
    if (props == null) {
      return null;
    }
    if (!TYPE.equals(props.getHeader(HEADER_TYPE))) {
      return null;
    }

    String workerId =
      props.getHeader(HEADER_NODE_ID) instanceof String s && !s.isBlank() ? s : bodyWorkerId(msg.getBody());
    if (workerId == null || workerId.isBlank()) {
      return null;
    }

    String appName = AmqpMessageReader.readStringHeader(props, HEADER_APP_NAME);
    return new WorkerHeartbeatMessage(
      AmqpMessageReader.readLongHeader(props, HEADER_MESSAGE_ID, 0L),
      workerId,
      AmqpMessageReader.readLongHeader(props, HEADER_HEARTBEAT_EPOCH, 0L),
      Boolean.TRUE.equals(props.getHeader(HEADER_HEARTBEAT_READY)),
      appName == null || appName.isBlank() ? null : appName
    );
  }

  private static String bodyWorkerId(byte[] body) {
    return body == null || body.length == 0 ? null : new String(body, StandardCharsets.UTF_8);
  }
}
