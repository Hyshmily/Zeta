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
package io.github.hyshmily.hotkey.worker;

import static io.github.hyshmily.hotkey.constant.HotKeyConstants.*;

import io.github.hyshmily.hotkey.broadcast.WorkerMessage;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import io.github.hyshmily.hotkey.log.DefaultLogger;
import io.github.hyshmily.hotkey.log.HotKeyLogger;

/**
 * Publishes HOT and COOL decisions to all application instances via the
 * configured RabbitMQ {@code broadcastExchange}.
 *
 * <p>Each broadcast message carries a monotonically increasing
 * <b>decision version</b> (worker‑local counter) that is used by receivers to
 * discard stale or out‑of‑order messages.
 *
 * <p>Messages are delivered to every instance's dedicated queue through a
 * fanout exchange ({@code hotkey.broadcast.exchange}).  Routing keys follow
 * the patterns {@code hot.<appName>} and {@code cool.<appName>} but are
 * ignored by the fanout exchange — the receiver differentiates message type
 * via the {@code AMQP_HEADER_TYPE} header.
 */
@RequiredArgsConstructor
public class WorkerBroadcaster {
  private static final HotKeyLogger log = new DefaultLogger(WorkerBroadcaster.class);

  private final RabbitTemplate rabbitTemplate;
  private final String broadcastExchange;
  private final String appName;

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

    MessageProperties props = new MessageProperties();
    props.setHeader(AMQP_HEADER_TYPE, WorkerMessage.TYPE_HOT);
    props.setHeader(AMQP_HEADER_VERSION, dv);
    props.setHeader(AMQP_HEADER_IS_VERSION_DEGRADED, false);

    Message msg = new Message(cacheKey.getBytes(StandardCharsets.UTF_8), props);
    rabbitTemplate.send(broadcastExchange, ROUTING_KEY_HOT + appName, msg);

    log.debug("Broadcast HOT: key={}, dv={}, source={}", cacheKey, dv, source);
  }

  /**
   * Broadcasts a COOL decision for the given key.
   *
   * @param cacheKey the key that has been confirmed as fully cooled
   */
  public void broadcastCool(String cacheKey) {
    long dv = decisionVersionCounter.incrementAndGet();

    MessageProperties props = new MessageProperties();
    props.setHeader(AMQP_HEADER_TYPE, WorkerMessage.TYPE_COOL);
    props.setHeader(AMQP_HEADER_VERSION, dv);
    props.setHeader(AMQP_HEADER_IS_VERSION_DEGRADED, false);

    Message msg = new Message(cacheKey.getBytes(StandardCharsets.UTF_8), props);
    rabbitTemplate.send(broadcastExchange, ROUTING_KEY_COOL + appName, msg);

    log.debug("Broadcast COOL: key={}, dv={}", cacheKey, dv);
  }
}
