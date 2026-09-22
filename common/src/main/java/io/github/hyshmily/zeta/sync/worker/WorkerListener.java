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

import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.HEADER_APP_NAME;
import static io.github.hyshmily.zeta.sync.worker.WorkerMessage.TYPE_COOL;
import static io.github.hyshmily.zeta.sync.worker.WorkerMessage.TYPE_HOT;

import com.rabbitmq.client.Channel;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.sync.dispatcher.DispatcherStats;
import io.github.hyshmily.zeta.sync.dispatcher.PerKeyOrderedDispatcher;
import io.github.hyshmily.zeta.sync.local.CacheSyncListener;
import io.github.hyshmily.zeta.util.AmqpMessageReader;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;

/**
 * Listens for Worker hot/cool decisions via the {@code zeta.worker.exchange}
 * FanoutExchange and applies them to the local Caffeine L1 cache via
 * {@link WorkerDecisionHandler}.
 *
 * <p>This is the consumer-side counterpart of the Worker's {@code WorkerBroadcaster}.
 * Incoming AMQP messages are deserialized into {@link WorkerMessage} records and
 * routed by decision type:
 * <ul>
 *   <li><b>HOT</b> ({@link WorkerMessage#TYPE_HOT}): Delegates to
 *       {@link WorkerDecisionHandler#handleHot}.</li>
 *   <li><b>COOL</b> ({@link WorkerMessage#TYPE_COOL}): Delegates to
 *       {@link WorkerDecisionHandler#handleCool}.</li>
 * </ul>
 *
 * <p><b>Ack-before-update pattern:</b> The AMQP message is acknowledged immediately after
 * parsing (see {@link #handleWorkerMessage}). The actual cache mutation is scheduled
 * asynchronously with a random jitter (see {@link WorkerListenerProperties#broadcastJitterMs}).
 * This decoupling provides at-most-once delivery semantics — if the application crashes
 * after ack but before the cache write, the decision is lost and will be re-driven by the
 * next Worker heartbeat cycle (see ADR-0004).
 *
 * @see WorkerMessage
 * @see WorkerDecisionHandler
 * @see CacheSyncListener
 */
@Slf4j
@Internal
public class WorkerListener {

  /** Configuration for Worker exchange name, queue prefix, jitter settings, and rate limiter. */
  private final WorkerListenerProperties properties;

  /** Scheduler for running jitter-delayed cache update tasks, spreading Redis load.
   * Supplied externally to allow shared-pool reuse across listeners. */
  private final ScheduledExecutorService scheduler;

  /** Strategy that processes HOT and COOL decisions. */
  private final WorkerDecisionHandler decisionHandler;

  /**
   * This application's appName (ADR-0068). When non-blank, decisions carrying a
   * {@code appName} header that differs are dropped before dispatch — the fanout
   * exchange ignores the routing key, so on a shared broker every bound queue
   * receives every app's decisions. {@code null} or blank disables the filter
   * (all decisions processed), preserving the pre-0068 behavior; decisions
   * <em>without</em> the header (pre-0068 Workers) are always processed.
   */
  private final String appName;

  /** Per-key FIFO dispatcher for ordered cache mutation execution. */
  private PerKeyOrderedDispatcher dispatcher;

  public WorkerListener(
    WorkerListenerProperties properties,
    ScheduledExecutorService scheduler,
    WorkerDecisionHandler decisionHandler
  ) {
    this(properties, scheduler, decisionHandler, null);
  }

  public WorkerListener(
    WorkerListenerProperties properties,
    ScheduledExecutorService scheduler,
    WorkerDecisionHandler decisionHandler,
    String appName
  ) {
    this.properties = properties;
    this.scheduler = scheduler;
    this.decisionHandler = decisionHandler;
    this.appName = appName;
  }

  /**
   * Initializes the per-key ordered dispatcher for cache-mutation ordering.
   *
   * <p>Called automatically by the Spring container after all dependencies are
   * injected. Creates a {@link PerKeyOrderedDispatcher} that guarantees FIFO
   * processing of Worker decisions per cache key and spreads Redis load via a
   * start-jitter park of {@code broadcastJitterMs} on each key's first cycle.
   */
  @PostConstruct
  public void init() {
    this.dispatcher = new PerKeyOrderedDispatcher(
      scheduler,
      "worker-listener",
      PerKeyOrderedDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
      PerKeyOrderedDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      properties.getMaxPendingUnits(),
      properties.getBroadcastJitterMs()
    );
  }

  /**
   * Gracefully shuts down the ordered dispatcher.
   *
   * <p>Cancels any pending tasks and releases the dispatcher's internal
   * resources. Called automatically by the Spring container during
   * application shutdown.
   *
   * <p>Queued Worker decisions (and jitter-delayed submissions still pending
   * on the shared scheduler) are dropped without execution. COOL decisions
   * are never replayed (ADR-0024) and a dropped HOT leaves the entry to
   * expire at its hard TTL / be re-promoted by the next broadcast. The same
   * applies to decisions the dispatcher drops at runtime (per-key queue full,
   * global budget exhausted): a dropped HOT is re-driven by ADR-0024's
   * periodic rebroadcast, but a dropped COOL is NOT re-driven — the entry
   * stays HOT until its hard TTL expires (ADR-0035's orphan demotion covers
   * only a dead Worker incarnation, not a saturated dispatcher). The shutdown
   * window is intentionally lossy and self-healing for HOT; for COOL the
   * staleness is bounded by the hot hard TTL.
   */
  @PreDestroy
  public void destroy() {
    if (dispatcher != null) {
      dispatcher.close();
    }
  }

  /**
   * Snapshot of the decision plane's ordered-dispatcher gate and backlog (ADR-0072 D-1).
   *
   * <p>The gate's capacity comes from
   * {@link WorkerListenerProperties#getMaxPendingUnits()}; during a saturated dispatcher a dropped
   * HOT is re-driven by the periodic rebroadcast (ADR-0024) whereas a dropped COOL is not (the
   * entry stays HOT until its hard TTL), which is why this counter is the signal that tells the two
   * budgets apart in production.
   *
   * @return the dispatcher snapshot, or {@code null} before {@link #init()} has run (the
   *         {@code @PostConstruct} hook runs during context refresh, ahead of any scrape)
   */
  public DispatcherStats dispatcherStats() {
    PerKeyOrderedDispatcher current = dispatcher;
    return current == null ? null : current.stats();
  }

  /**
   * RabbitMQ message callback for incoming Worker decisions. Acknowledges the message
   * immediately after parsing (ack-before-update), then schedules the actual cache
   * mutation asynchronously with a random jitter to spread Redis load.
   *
   * <p>If processing fails with an exception, the message is negatively acknowledged
   * with {@code requeue=false} to prevent poison-message loops. The decision will be
   * re-driven by the next Worker heartbeat cycle.
   *
   * @param channel the AMQP channel used for ack/nack operations
   * @param msg     the raw AMQP message containing the Worker decision; must not be null
   * @throws IOException if the channel's basicAck or basicNack call fails
   */
  public void handleWorkerMessage(Channel channel, Message msg) throws IOException {
    long tag = msg.getMessageProperties().getDeliveryTag();
    try {
      processWorker(msg);
      channel.basicAck(tag, false); // ack before cache write
    } catch (Exception e) {
      log.error("Worker message processing failed: body={}", AmqpMessageReader.abbreviateBody(msg.getBody(), 256), e);
      channel.basicNack(tag, false, false); // do not requeue
    }
  }

  /**
   * Decodes the raw AMQP message into a {@link WorkerMessage} and submits it to the
   * ordered dispatcher. The dispatcher applies a random start-jitter park within
   * {@link WorkerListenerProperties#getBroadcastJitterMs()} to the key's worker before
   * its first cycle, spreading Redis reads across a small time window when the Worker
   * broadcasts to many instances.
   *
   * <p>The jitter is a dispatcher-level park, NOT a sleep inside the task (a sleeping
   * task would hold a scheduler thread for the whole jitter window). Per-key FIFO
   * ordering is unaffected either way — ordering is fixed by the atomic submission into
   * the dispatcher's map, so a stale COOL can never land after a newer HOT from another
   * Worker (with cross-Worker decisions both are accepted unconditionally, which makes
   * strict arrival order the only safe ordering).
   *
   * @param msg the raw AMQP message; may have null or empty body, in which case
   *            the message is silently dropped
   */
  private void processWorker(Message msg) {
    WorkerMessage wm = WorkerMessage.from(msg);
    if (wm == null) {
      log.debug("Received worker message with empty body");
      return;
    }

    // ADR-0068 shared-broker isolation: the fanout exchange ignores the routing key,
    // so a foreign app's decisions land in this queue too. Drop when the sender
    // declared its appName (0068+ Workers) and it differs from ours; a message
    // without the header (pre-0068 Worker, rolling upgrade) or a listener without
    // a configured appName (legacy wiring) is always processed.
    Object senderApp = msg.getMessageProperties().getHeader(HEADER_APP_NAME);
    if (senderApp != null
      && appName != null
      && !appName.isBlank()
      && !appName.contentEquals(senderApp.toString())
    ) {
      log.debug(
        "Dropped worker decision for foreign app: senderApp={}, localApp={}, key={}, type={}",
        senderApp,
        appName,
        wm.cacheKey(),
        wm.type()
      );
      return;
    }

    Runnable task = () -> {
      try {
        workerMessageRouter(wm);
      } catch (Exception e) {
        log.error(
          "Error processing WorkerMessage: cacheKey={}, type={}, decisionVersion={}, nodeId={}, epoch={}",
          wm.cacheKey(),
          wm.type(),
          wm.decisionVersion(),
          wm.nodeId(),
          wm.epoch(),
          e
        );
      }
    };

    dispatcher.submit(wm.cacheKey(), task);
  }

  /**
   * Routes the deserialized {@link WorkerMessage} to the appropriate handler based
   * on its type field. Delegates to {@link WorkerDecisionHandler#handleHot} for
   * {@code TYPE_HOT} and {@link WorkerDecisionHandler#handleCool} for
   * {@code TYPE_COOL}. Messages with a null or unknown type are logged and dropped.
   *
   * @param msg the deserialized Worker message to route; must not be null
   */
  private void workerMessageRouter(WorkerMessage msg) {
    if (msg.type() == null) {
      log.debug("Received worker message with null type, skipping");
      return;
    }
    switch (msg.type()) {
      case TYPE_HOT -> decisionHandler.handleHot(msg);
      case TYPE_COOL -> decisionHandler.handleCool(msg);
      default -> log.warn("Unknown worker message type: {}, cacheKey: {}", msg.type(), msg.cacheKey());
    }
  }
}
