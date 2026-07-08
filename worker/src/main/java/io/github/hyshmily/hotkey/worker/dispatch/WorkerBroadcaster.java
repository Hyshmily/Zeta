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

import io.github.hyshmily.hotkey.sync.worker.WorkerMessage;
import io.github.hyshmily.hotkey.util.version.VersionGuard;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/*
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
/** Default constructor. */
@RequiredArgsConstructor
@Slf4j
public class WorkerBroadcaster {

  /** RabbitMQ template used to publish HOT/COOL decisions and heartbeats. */
  private final RabbitTemplate rabbitTemplate;
  /** Target exchange name for all broadcast messages (typically {@code hotkey.broadcast.exchange}). */
  private final String broadcastExchange;
  /** Application name used in the broadcast routing key. */
  private final String appName;

  /** Originating Worker's node identity, set on every broadcast header for cross-Worker
   * version tracking in {@link VersionGuard}. */
  private final String nodeId;

  /** Monotonically increasing epoch counter for this Worker instance, incremented on every
   * restart. Transmitted in broadcast headers so receivers detect Worker restarts and
   * unconditionally accept decisions from a higher epoch (see ADR-0010). */
  private final AtomicLong epochCounter;

  /**
   * Worker‑local decision version counter.
   * Incremented atomically for every broadcast to provide strict ordering
   * for this worker.  Combined with consistent‑hash sharding (one key → one
   * worker) this guarantees a total order of decisions per key.
   */
  private final AtomicLong decisionVersionCounter = new AtomicLong(System.currentTimeMillis());

  /**
   * Atomically allocates the next decision version without sending.
   * Called from within {@link io.github.hyshmily.hotkey.worker.ingest.ReportConsumer#doOnReport}
   * to pre-allocate a version before enqueuing an async broadcast.
   *
   * @return the next monotonically increasing decision version
   */
  public long nextDecisionVersion() {
    return decisionVersionCounter.incrementAndGet();
  }

  /**
   * Broadcasts a HOT decision for the given key.
   *
   * @param cacheKey the key that has been confirmed as hot
   * @param source   a label describing the detection source (e.g. "sliding_window")
   */
  public void broadcastHot(String cacheKey, String source) {
    long dv = decisionVersionCounter.incrementAndGet();
    sendBroadcast(cacheKey, WorkerMessage.TYPE_HOT, dv);
    log.debug(
      "Broadcast HOT: key={}, dv={}, source={}, nodeId={}, epoch={}",
      cacheKey,
      dv,
      source,
      nodeId,
      epochCounter.get()
    );
  }

  /**
   * Broadcasts a HOT decision with a pre-allocated version number.
   *
   * @param cacheKey the key that has been confirmed as hot
   * @param source   a label describing the detection source
   * @param dv       pre-allocated decision version (from {@link #nextDecisionVersion()})
   */
  public void broadcastHot(String cacheKey, String source, long dv) {
    sendBroadcast(cacheKey, WorkerMessage.TYPE_HOT, dv);
    log.debug(
      "Broadcast HOT: key={}, dv={}, source={}, nodeId={}, epoch={}",
      cacheKey,
      dv,
      source,
      nodeId,
      epochCounter.get()
    );
  }

  /**
   * Broadcasts a COOL decision for the given key.
   *
   * @param cacheKey the key that has been confirmed as fully cooled
   */
  public void broadcastCool(String cacheKey) {
    long dv = decisionVersionCounter.incrementAndGet();
    sendBroadcast(cacheKey, WorkerMessage.TYPE_COOL, dv);
  }

  /**
   * Broadcasts a COOL decision with a pre-allocated version number.
   *
   * @param cacheKey the key that has been confirmed as fully cooled
   * @param dv       pre-allocated decision version (from {@link #nextDecisionVersion()})
   */
  public void broadcastCool(String cacheKey, long dv) {
    sendBroadcast(cacheKey, WorkerMessage.TYPE_COOL, dv);
    log.debug("Broadcast COOL: key={}, dv={}, nodeId={}, epoch={}", cacheKey, dv, nodeId, epochCounter.get());
  }

  /**
   * Returns the current decision version without incrementing.
   *
   * <p>Used by {@link WorkerHeartbeatProducer} for the heartbeat's decisionVersionHwm field.
   *
   * @return the current decision version counter value
   */
  public long getCurrentDecisionVersion() {
    return decisionVersionCounter.get();
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
    try {
      MessageProperties props = new MessageProperties();
      props.setHeader(AMQP_HEADER_TYPE, type);
      props.setHeader(AMQP_HEADER_VERSION, version);
      props.setHeader(AMQP_HEADER_IS_VERSION_DEGRADED, false);
      props.setHeader(AMQP_HEADER_NODE_ID, nodeId);
      props.setHeader(AMQP_HEADER_EPOCH, epochCounter.get());

      Message msg = new Message(cacheKey.getBytes(StandardCharsets.UTF_8), props);
      rabbitTemplate.send(broadcastExchange, ROUTING_KEY_BROADCAST + appName, msg);
    } catch (Exception e) {
      log.error("Failed to broadcast {} for key={}, dv={}: {}", type, cacheKey, version, e.getMessage());
      // no throw — ADR-0007 fire-and-forget
    }
  }
}
