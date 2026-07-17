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
package io.github.hyshmily.zeta.worker.dispatch;

import static io.github.hyshmily.zeta.constants.ZetaConstants.*;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.sync.worker.WorkerMessage;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * Publishes HOT and COOL decisions to all application instances via the
 * configured RabbitMQ {@code broadcastExchange}.
 *
 * <p>Each send message carries a monotonically increasing
 * <b>decision version</b> (worker‑local counter) that is used by receivers to
 * discard stale or out‑of‑order messages.
 *
 * <p>Messages are delivered to every instance's dedicated queue through a
 * fanout exchange ({@code zeta.send.exchange}) — the receiver
 * differentiates message type via the {@code AMQP_HEADER_TYPE} header.
 */
@RequiredArgsConstructor
@Slf4j
public class WorkerBroadcaster {

  /** RabbitMQ template used to publish HOT/COOL decisions and heartbeats. */
  private final RabbitTemplate rabbitTemplate;
  /** Target exchange name for all send messages (typically {@code zeta.send.exchange}). */
  private final String broadcastExchange;
  /** Application name used in the send routing key. */
  private final String appName;

  /** Originating Worker's node identity, set on every send header for cross-Worker
   * version tracking in {@link VersionGuard}. */
  private final String nodeId;

  /** Monotonically increasing epoch counter for this Worker instance, incremented on every
   * restart. Transmitted in send headers so receivers detect Worker restarts and
   * unconditionally accept decisions from a higher epoch (see ADR-0010). */
  private final AtomicLong epochCounter;

  /**
   * Worker‑local decision version counter.
   * Incremented atomically for every send to provide strict ordering
   * for this worker.  Combined with consistent‑hash sharding (one key → one
   * worker) this guarantees a total order of decisions per key.
   *
   * <p>Initialised at zero — the epoch counter handles cross‑restart ordering,
   * so we neither need nor want a wall‑clock seed (see ADR-0010).
   */
  private final AtomicLong decisionVersionCounter = new AtomicLong(0L);

  /**
   * Short‑lived deduplication cache that prevents redundant AMQP broadcasts
   * when two Workers transiently evaluate the same key during ring convergence
   * (see ADR-0013). A (key + type) pair is retained for 100 ms so the second
   * Worker's broadcast is silently elided.
   */
  private final Cache<String, Boolean> broadcastDedupCache = Caffeine.newBuilder()
    .expireAfterWrite(100, TimeUnit.MILLISECONDS)
    .maximumSize(1_000)
    .build();

  /**
   * Atomically allocates the next decision version without sending.
   * Called from within {@link io.github.hyshmily.zeta.worker.ingest.ReportConsumer#doOnReport}
   * to pre-allocate a version before enqueuing an async send.
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
   */
  public boolean broadcastHot(String cacheKey) {
    String dedupKey = cacheKey + ":" + WorkerMessage.TYPE_HOT;
    if (broadcastDedupCache.getIfPresent(dedupKey) != null) {
      return true;
    }
    long dv = nextDecisionVersion();
    try {
      sendBroadcast(cacheKey, WorkerMessage.TYPE_HOT, dv);
      broadcastDedupCache.put(dedupKey, Boolean.TRUE);
    } catch (Exception e) {
      log.error("Failed to broadcast HOT decision for key {}: {}", cacheKey, e.getMessage());
      return false;
    }
    return true;
  }

  /**
   * Broadcasts a COOL decision for the given key.
   *
   * @param cacheKey the key that has been confirmed as fully cooled
   */
  public boolean broadcastCool(String cacheKey) {
    String dedupKey = cacheKey + ":" + WorkerMessage.TYPE_COOL;
    if (broadcastDedupCache.getIfPresent(dedupKey) != null) {
      return true;
    }
    long dv = nextDecisionVersion();
    try {
      sendBroadcast(cacheKey, WorkerMessage.TYPE_COOL, dv);
      broadcastDedupCache.put(dedupKey, Boolean.TRUE);
    } catch (Exception e) {
      log.error("Failed to broadcast COOL decision for key {}: {}", cacheKey, e.getMessage());
      return false;
      // no throw — ADR-0007 fire-and-forget
    }
    return true;
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
   * @param cacheKey the key being send
   * @param type     the message type ({@link WorkerMessage#TYPE_HOT} or {@link WorkerMessage#TYPE_COOL})
   * @param version  the monotonically increasing decision version for ordering
   */
  private void sendBroadcast(String cacheKey, String type, long version) {
    MessageProperties props = new MessageProperties();
    props.setHeader(AMQP_HEADER_TYPE, type);
    props.setHeader(AMQP_HEADER_VERSION, version);
    props.setHeader(AMQP_HEADER_IS_VERSION_DEGRADED, false);
    props.setHeader(AMQP_HEADER_NODE_ID, nodeId);
    props.setHeader(AMQP_HEADER_EPOCH, epochCounter.get());

    Message msg = new Message(cacheKey.getBytes(StandardCharsets.UTF_8), props);
    rabbitTemplate.send(broadcastExchange, ROUTING_KEY_BROADCAST + appName, msg);
  }
}
