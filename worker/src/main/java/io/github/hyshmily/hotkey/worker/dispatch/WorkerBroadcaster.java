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
package io.github.hyshmily.hotkey.worker.dispatch;

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.*;

import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import io.github.hyshmily.hotkey.sync.WorkerMessage;
import io.github.hyshmily.hotkey.logging.DefaultLogger;
import io.github.hyshmily.hotkey.logging.HotKeyLogger;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * Publishes HOT and COOL decisions to all application instances via the
 * configured RabbitMQ {@code broadcastExchange}.
 *
 * <p>Each broadcast message carries a monotonically increasing
 * <b>decision version</b> (worker‑local counter) that is used by receivers to
 * discard stale or out‑of‑order messages.
 *
 * <p>Messages are delivered to every instance's dedicated queue through a
 * fanout exchange ({@code hotkey.broadcast.exchange}) — the receiver
 * differentiates message type via the {@code AMQP_HEADER_TYPE} header.
 */
@RequiredArgsConstructor
public class WorkerBroadcaster {

  private static final HotKeyLogger log = new DefaultLogger(WorkerBroadcaster.class);

  /** RabbitMQ template used to publish HOT/COOL decisions and heartbeats. */
  private final RabbitTemplate rabbitTemplate;
  /** Target exchange name for all broadcast messages (typically {@code hotkey.broadcast.exchange}). */
  private final String broadcastExchange;
  /** Application name used in the broadcast routing key. */
  private final String appName;
  /** State machine providing current confirm/cool/grace counts for heartbeat messages. */
  private final HotKeyStateMachine stateMachine;
  /** Shared monotonic counter for config-change timestamps embedded in heartbeats. */
  private final AtomicLong configTimestampCounter;

  /**
   * Worker‑local decision version counter.
   * Incremented atomically for every broadcast to provide strict ordering
   * for this worker.  Combined with consistent‑hash sharding (one key → one
   * worker) this guarantees a total order of decisions per key.
   */
  private final AtomicLong decisionVersionCounter = new AtomicLong(System.currentTimeMillis());

  /**
   * Broadcasts a HOT decision for the given key.
   *
   * @param cacheKey the key that has been confirmed as hot
   * @param source   a label describing the detection source (e.g. "sliding_window")
   */
  public void broadcastHot(String cacheKey, String source) {
    long dv = decisionVersionCounter.incrementAndGet();
    sendBroadcast(cacheKey, WorkerMessage.TYPE_HOT, dv);
    log.debug("Broadcast HOT: key={}, dv={}, source={}", cacheKey, dv, source);
  }

  /**
   * Broadcasts a COOL decision for the given key.
   *
   * @param cacheKey the key that has been confirmed as fully cooled
   */
  public void broadcastCool(String cacheKey) {
    long dv = decisionVersionCounter.incrementAndGet();
    sendBroadcast(cacheKey, WorkerMessage.TYPE_COOL, dv);
    log.debug("Broadcast COOL: key={}, dv={}", cacheKey, dv);
  }

  /**
   * Common send helper for HOT/COOL decisions.
   *
   * <p>Builds {@link MessageProperties} with type, version, and degraded flag,
   * creates a {@link Message}, and publishes via {@link #rabbitTemplate}.
   *
   * @param cacheKey the key being broadcast
   * @param type     the message type ({@link WorkerMessage#TYPE_HOT} or {@link WorkerMessage#TYPE_COOL})
   * @param version  the monotonically increasing decision version for ordering
   */
  private void sendBroadcast(String cacheKey, String type, long version) {
    MessageProperties props = new MessageProperties();
    props.setHeader(AMQP_HEADER_TYPE, type);
    props.setHeader(AMQP_HEADER_VERSION, version);
    props.setHeader(AMQP_HEADER_IS_VERSION_DEGRADED, false);

    Message msg = new Message(cacheKey.getBytes(StandardCharsets.UTF_8), props);
    rabbitTemplate.send(broadcastExchange, ROUTING_KEY_BROADCAST + appName, msg);
  }

  /**
   * Broadcasts a heartbeat (ping) message to all application instances.
   *
   * @param nodeId the unique identifier of this worker node
   */
  public void broadcastHeartbeat(String nodeId) {
    MessageProperties props = new MessageProperties();
    props.setHeader(AMQP_HEADER_TYPE, WorkerMessage.TYPE_PING);
    props.setHeader(AMQP_HEADER_NODE_ID, nodeId);
    props.setHeader(AMQP_HEADER_TIMESTAMP, System.currentTimeMillis());
    props.setHeader(AMQP_HEADER_CONFIG_CONFIRM_COUNT, stateMachine.getConfirmCount());
    props.setHeader(AMQP_HEADER_CONFIG_COOL_COUNT, stateMachine.getCoolCount());
    props.setHeader(AMQP_HEADER_CONFIG_GRACE_COUNT, stateMachine.getPreCoolGraceCount());
    props.setHeader(AMQP_HEADER_CONFIG_TIMESTAMP, configTimestampCounter.get());

    Message msg = new Message(AMQP_MESSAGE_PING.getBytes(StandardCharsets.UTF_8), props);
    rabbitTemplate.send(broadcastExchange, ROUTING_KEY_BROADCAST + appName, msg);
  }
}
