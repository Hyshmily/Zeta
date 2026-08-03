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
package io.github.hyshmily.zeta.sync.local;

import static io.github.hyshmily.zeta.sync.local.SyncMessage.*;

import com.rabbitmq.client.Channel;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.sync.dispatcher.PerKeyOrderedDispatcher;
import io.github.hyshmily.zeta.sync.worker.WorkerListener;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;

/**
 * Listens for cache synchronization messages (INVALIDATE / REFRESH / RULES_SYNC) from
 * peer application instances via the {@code zeta.sync.exchange} FanoutExchange and
 * processes them via {@link SyncDecisionHandler}.
 *
 * <p>This is the inbound half of the instance-to-instance cache coherence protocol.
 * The outbound half is {@link CacheSyncPublisher}. Together they ensure that a data
 * mutation on one instance is propagated to all peers.
 *
 * <p><b>Thread safety:</b> All cache mutations use
 * {@link com.github.benmanes.caffeine.cache.Cache#asMap()}{@code .compute()} for
 * atomic per-key updates. The AMQP ack is sent before the cache mutation
 * (ack-before-update pattern, see ADR-0004); the mutation is scheduled with random
 * jitter to spread Redis load across instances.
 *
 * @see CacheSyncPublisher
 * @see SyncMessage
 * @see SyncDecisionHandler
 * @see WorkerListener
 */
@Slf4j
@Internal
public class CacheSyncListener {

  /** Configuration for sync exchange name, jitter settings, and consumer concurrency. */
  private final CacheSyncProperties properties;

  /** Scheduler for running jitter-delayed cache update tasks, spreading Redis load.
   * Supplied externally to allow shared-pool reuse across listeners. */
  private final ScheduledExecutorService scheduler;

  /** Strategy that processes REFRESH, INVALIDATE, INVALIDATE_ALL, and RULES_SYNC messages. */
  private final SyncDecisionHandler decisionHandler;

  /** Per-key FIFO dispatcher for ordered cache mutation execution. */
  private PerKeyOrderedDispatcher dispatcher;

  public CacheSyncListener(
    CacheSyncProperties properties,
    ScheduledExecutorService scheduler,
    SyncDecisionHandler decisionHandler
  ) {
    this.properties = properties;
    this.scheduler = scheduler;
    this.decisionHandler = decisionHandler;
  }

  @PostConstruct
  public void init() {
    this.dispatcher = new PerKeyOrderedDispatcher(
      scheduler,
      "cache-sync",
      PerKeyOrderedDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
      PerKeyOrderedDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      properties.getMaxPendingUnits()
    );
  }

  @PreDestroy
  public void destroy() {
    if (dispatcher != null) {
      dispatcher.close();
    }
  }

  /**
   * RabbitMQ message callback for incoming sync messages. Acknowledges the message
   * immediately after parsing (ack-before-update), then schedules the actual cache
   * mutation asynchronously with a random jitter to spread Redis load across peers.
   *
   * <p>On success, the message is acknowledged via {@link Channel#basicAck}. On any
   * processing exception (parse failure, routing failure), the message is negatively
   * acknowledged with {@code requeue=false} to prevent poison-message loops. The next
   * application-level write will re-send the operation.
   *
   * @param channel the AMQP channel used for ack/nack operations
   * @param msg     the raw AMQP message whose body and headers carry the sync payload;
   *                must not be null
   * @throws IOException if the channel's basicAck or basicNack call fails
   */
  public void handleSyncMessage(Channel channel, Message msg) throws IOException {
    long tag = msg.getMessageProperties().getDeliveryTag();
    try {
      processSync(msg);
      channel.basicAck(tag, false);
    } catch (Exception e) {
      log.error("CacheSync processing failed: body={}", new String(msg.getBody()), e);
      channel.basicNack(tag, false, false);
    }
  }

  /**
   * Decodes the raw AMQP message into a {@link SyncMessage} and schedules the
   * appropriate handler to run after a random delay within
   * {@link CacheSyncProperties#getWarmupJitterMs()}. The jitter spreads Redis
   * reads when multiple peers process the same sync send simultaneously.
   *
   * @param msg the raw AMQP message; if the body is null, empty, or cannot be
   *            parsed into a valid {@link SyncMessage}, the message is silently
   *            dropped without scheduling any task
   */
  private void processSync(Message msg) {
    SyncMessage sm = SyncMessage.from(msg);
    if (sm == null) {
      log.debug("Received sync message with empty or invalid body");
      return;
    }

    Runnable task = () -> {
      try {
        syncMessageRouter(sm);
      } catch (Exception e) {
        log.error("Async sync task failed: type={}, key={}, version={}", sm.type(), sm.cacheKey(), sm.version(), e);
      }
    };

    // Weight the task against the dispatcher's global pending budget by payload size
    // (~1 unit per KB): batch payloads (INVALIDATE_ALL body = JSON key list) are far
    // heavier than single-key messages, so the budget must track bytes, not message count.
    byte[] body = msg.getBody();
    int weight = 1 + (body == null ? 0 : body.length >> 10);

    long jitterMs = properties.getWarmupJitterMs();
    long delay = jitterMs > 0 ? ThreadLocalRandom.current().nextLong(jitterMs) : 0L;
    dispatcher.submitWithWeight(sm.cacheKey(), task, weight, delay);
  }

  /**
   * Routes the deserialized {@link SyncMessage} to the appropriate handler based
   * on its type field. Delegates to {@link SyncDecisionHandler#handleLocalInvalidate},
   * {@link SyncDecisionHandler#handleLocalInvalidateAll},
   * {@link SyncDecisionHandler#handleRefresh}, or
   * {@link SyncDecisionHandler#handleRulesSync} accordingly.
   *
   * @param msg the deserialized sync message to route; must not be null
   */
  private void syncMessageRouter(SyncMessage msg) {
    if (msg.type() == null) {
      log.debug("Received sync with null type, skipping");
      return;
    }
    switch (msg.type()) {
      case TYPE_INVALIDATE -> decisionHandler.handleLocalInvalidate(msg);
      case TYPE_INVALIDATE_ALL -> decisionHandler.handleLocalInvalidateAll(msg);
      case TYPE_REFRESH -> decisionHandler.handleRefresh(msg);
      case TYPE_RULES_SYNC -> decisionHandler.handleRulesSync(msg);
      default -> log.warn("Unknown sync type: {}, cacheKey: {}", msg.type(), msg.cacheKey());
    }
  }
}
