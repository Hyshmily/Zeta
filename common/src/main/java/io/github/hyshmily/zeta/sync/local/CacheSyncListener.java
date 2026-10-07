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

import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.*;
import static io.github.hyshmily.zeta.sync.local.SyncMessage.*;

import com.rabbitmq.client.Channel;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.sync.AppIsolationFilter;
import io.github.hyshmily.zeta.sync.dispatcher.BatchAwareTask;
import io.github.hyshmily.zeta.sync.dispatcher.DispatcherStats;
import io.github.hyshmily.zeta.sync.dispatcher.PerKeyOrderedDispatcher;
import io.github.hyshmily.zeta.sync.worker.WorkerListener;
import io.github.hyshmily.zeta.util.AmqpMessageReader;
import io.github.hyshmily.zeta.util.InstanceIdGenerator;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
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
 * (ack-before-update pattern, see ADR-0004); the mutation is started on the ordered
 * dispatcher after a random start-jitter park (see {@link PerKeyOrderedDispatcher})
 * to spread Redis load across instances.
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

  /**
   * Scheduler shared by the ordered dispatcher's cycle executor and its start-jitter park.
   * Supplied externally to allow shared-pool reuse across listeners.
   *
   * <p><b>Throughput ceiling:</b> every dispatcher task submitted by this listener holds a
   * scheduler thread for the duration of a synchronous Redis load inside the decision handler
   * — the {@code warmupJitterMs} delay (default max 50ms, see
   * {@link CacheSyncProperties#getWarmupJitterMs()}) is a scheduler-side park, NOT a
   * {@code Thread.sleep} inside the task, so it no longer consumes a pool thread. Under
   * invalidate storms the pool therefore processes at most
   * {@code poolSize / loadTimeMs} sync tasks per second; sizing the shared scheduler (and its
   * {@code concurrentConsumers} feed) should account for that per-task latency floor.
   */
  private final ScheduledExecutorService scheduler;

  /** Strategy that processes REFRESH, INVALIDATE, INVALIDATE_ALL, and RULES_SYNC messages. */
  private final SyncDecisionHandler decisionHandler;

  /**
   * Shared-broker appName isolation (ADR-0068, see {@link AppIsolationFilter}):
   * sync messages that belong to a <em>different</em> application sharing the
   * same broker are dropped. {@code null}/blank disables the filter — the
   * pre-0068 behaviour, which legacy wiring and rolling upgrades still need.
   *
   * <p>The sync exchange is a fanout whose name is a global constant, so on a
   * shared broker every instance receives every application's INVALIDATE /
   * REFRESH / INVALIDATE_ALL / RULES_SYNC. A foreign INVALIDATE_ALL would
   * evict this application's entire L1 (and its dedup cache), stampeding this
   * application's traffic back to the shared backend — hence the drop.
   */
  private final AppIsolationFilter isolation;

  /**
   * Bit-shift divisor converting a message body length into ~1KB cost units
   * for dispatcher admission weighting ({@code body.length >> 10} ≈ KBs).
   */
  private static final int BYTE_WEIGHT = 10;

  /** Per-key FIFO dispatcher for ordered cache mutation execution. */
  // volatile: written once from start() (container lifecycle thread), read from
  // the AMQP consumer threads and Actuator threads.
  private volatile PerKeyOrderedDispatcher dispatcher;

  /**
   * Creates a listener. {@code appName} may be {@code null}/blank to process
   * sync messages from every application (the pre-0068 behaviour, which
   * legacy wiring and rolling upgrades still need).
   */
  public CacheSyncListener(
    CacheSyncProperties properties,
    ScheduledExecutorService scheduler,
    SyncDecisionHandler decisionHandler,
    String appName
  ) {
    this.properties = properties;
    this.scheduler = scheduler;
    this.decisionHandler = decisionHandler;
    this.isolation = new AppIsolationFilter(appName);
  }

  @PostConstruct
  public void init() {
    this.dispatcher = new PerKeyOrderedDispatcher(
      scheduler,
      "cache-sync",
      PerKeyOrderedDispatcher.DEFAULT_MAX_QUEUE_PER_KEY,
      PerKeyOrderedDispatcher.DEFAULT_MAX_TASKS_PER_CYCLE,
      properties.getMaxPendingUnits(),
      properties.getWarmupJitterMs()
    );
  }

  /**
   * Shuts down the ordered dispatcher.
   *
   * <p>Queued sync tasks (and jitter-delayed submissions still pending on the
   * shared scheduler) are dropped without execution: the shutdown window is
   * intentionally lossy. Lost INVALIDATE/REFRESH messages self-heal via the
   * next periodic broadcast / application-level write (ADR-0004), so a drain
   * is not worth blocking context shutdown for.
   */
  @PreDestroy
  public void destroy() {
    if (dispatcher != null) {
      dispatcher.close();
    }
  }

  /**
   * Snapshot of the sync plane's ordered-dispatcher gate and backlog (ADR-0072 D-1).
   *
   * <p>The gate's capacity comes from {@code CacheSyncProperties#getMaxPendingUnits()}; the drops
   * and rejections counted here are the very events whose WARN logs are throttled (ADR-0037), so
   * this is the only way to observe how close the sync plane is to shedding messages without
   * waiting for a log line.
   *
   * @return the dispatcher snapshot, or {@code null} before {@link #init()} has run (the
   *         {@code @PostConstruct} hook runs during context refresh, ahead of any scrape)
   */
  public DispatcherStats dispatcherStats() {
    PerKeyOrderedDispatcher current = dispatcher;
    return current == null ? null : current.stats();
  }

  /**
   * Cumulative foreign-app sync drops (ADR-0068). Non-zero means this instance
   * is discarding another application's sync messages — or, when paired with
   * missing cross-instance coherence, that the local {@code app-name} is
   * misconfigured.
   *
   * @return the total foreign-app drop count since startup
   */
  public long foreignAppDrops() {
    return isolation.drops();
  }

  /**
   * The most recently observed foreign sender appName, or {@code null} when no
   * foreign sync message has been dropped yet.
   *
   * @return the last foreign appName, or {@code null}
   */
  public String lastForeignApp() {
    return isolation.lastForeignApp();
  }

  /**
   * RabbitMQ message callback for incoming sync messages. Acknowledges the message
   * immediately after parsing (ack-before-update), then submits the actual cache
   * mutation to the ordered dispatcher, which starts it after a random start-jitter
   * park to spread Redis load across peers.
   *
   * <p><b>Self-message filtering (ADR-0067):</b> the fanout exchange delivers the
   * sender's own broadcasts back to its per-instance queue. A self-addressed
   * REFRESH is dropped before parsing: the sender's L1 entry at the broadcast
   * version is definitionally current (putThrough stamps it with the exact
   * post-INCR version), so re-processing it could only discard the sender's
   * fresh write through the no-value fallback (the default deployment) or pay a
   * redundant Redis fetch (value-channel deployments). Other self-message types
   * keep their historical processing: a self-INVALIDATE heals
   * invalidate-vs-reload repopulations stamped with older versions, and
   * RULES_SYNC / INVALIDATE_ALL self-processing is already a version-guarded
   * no-op. Messages without an origin header (pre-0067 senders, rolling
   * upgrades) are always processed.
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
      Object senderApp = msg.getMessageProperties().getHeader(HEADER_APP_NAME);
      if (isolation.isForeign(senderApp)) {
        // Ack, not nack: the message is valid, it just is not ours. Nacking would
        // requeue it into a poison loop. senderApp is provably non-null here —
        // isForeign is false without a declared header.
        long total = isolation.noteDrop(senderApp.toString());
        // Counted, not just DEBUG: a misconfigured app-name drops 100% of sync
        // while the process looks healthy, so the mismatch needs a WARN
        // (rate-limited) plus a counter/endpoint signal (ADR-0068).
        if (isolation.tryLog()) {
          log.warn(
            "Dropped sync message from another application: senderApp={}, localApp={} ({} total drops, further warnings suppressed for 10s; check zeta.local.app-name)",
            senderApp,
            isolation.appName(),
            total
          );
        } else if (log.isDebugEnabled()) {
          log.debug("Dropped sync message from another application (appName header mismatch, ADR-0068)");
        }
        channel.basicAck(tag, false);
        return;
      }
      if (isOwnRefresh(msg)) {
        if (log.isDebugEnabled()) {
          log.debug("Dropped self-addressed REFRESH (sender = receiver, ADR-0067)");
        }
        channel.basicAck(tag, false);
        return;
      }
      processSync(msg);
      channel.basicAck(tag, false);
    } catch (Exception e) {
      // Body is truncated: an INVALIDATE_ALL / RULES_SYNC payload can be tens of KB,
      // and a parse-failure storm must not echo full bodies into the log.
      log.error("CacheSync processing failed: body={}", AmqpMessageReader.abbreviateBody(msg.getBody(), 256), e);
      channel.basicNack(tag, false, false);
    }
  }

  /**
   * Whether the message is this instance's own REFRESH broadcast: it carries
   * this instance's ID in the origin header (ADR-0067) and its type is
   * {@link SyncMessage#TYPE_REFRESH}. Messages from other instances, without
   * an origin header, or of any other type return {@code false}.
   */
  private boolean isOwnRefresh(Message msg) {
    Object origin = msg.getMessageProperties().getHeader(HEADER_ORIGIN_INSTANCE);
    if (origin == null || !InstanceIdGenerator.get().equals(origin.toString())) {
      return false;
    }
    Object type = msg.getMessageProperties().getHeader(HEADER_TYPE);
    return SyncMessage.TYPE_REFRESH.equals(type);
  }

  /**
   * Decodes the raw AMQP message into a {@link SyncMessage} and submits it to the ordered
   * dispatcher. The dispatcher applies a random start-jitter park within
   * {@link CacheSyncProperties#getWarmupJitterMs()} to the key's worker before its first
   * cycle, spreading Redis reads when multiple peers process the same sync send
   * simultaneously.
   *
   * <p>The task is submitted as a {@link BatchAwareTask} (ADR-0071): the dispatcher marks
   * the last task of the key's granted batch with {@code endOfBatch == true}, which lets
   * {@link DefaultSyncDecisionHandler} amortize a same-key REFRESH burst to a single Redis
   * load at the batch tail instead of one load per queued message. All other message types
   * ignore the flag and process exactly as before.
   *
   * <p>The jitter is a dispatcher-level park, NOT a sleep inside the task (a sleeping task
   * would hold a scheduler thread for the whole jitter window). Per-key FIFO ordering is
   * unaffected either way — ordering is fixed by the atomic submission into the dispatcher's
   * map, and the parked worker still holds its key exclusively, so two same-key messages
   * execute strictly in arrival order regardless of the random draws.
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

    BatchAwareTask task = endOfBatch -> {
      try {
        syncMessageRouter(sm, endOfBatch);
      } catch (Exception e) {
        log.error("Async sync task failed: type={}, key={}, version={}", sm.type(), sm.cacheKey(), sm.version(), e);
      }
    };

    // Weight the task against the dispatcher's global pending budget by payload size
    // (~1 unit per KB): batch payloads (INVALIDATE_ALL body = JSON key list) are far
    // heavier than single-key messages, so the budget must track bytes, not message count.
    byte[] body = msg.getBody();
    int weight = 1 + (body == null ? 0 : body.length >> BYTE_WEIGHT);

    dispatcher.submitWithWeight(sm.cacheKey(), task, weight);
  }

  /**
   * Routes the deserialized {@link SyncMessage} to the appropriate handler based
   * on its type field. Delegates to {@link SyncDecisionHandler#handleLocalInvalidate},
   * {@link SyncDecisionHandler#handleLocalInvalidateAll},
   * {@link SyncDecisionHandler#handleRefresh(SyncMessage, boolean)}, or
   * {@link SyncDecisionHandler#handleRulesSync} accordingly. Only the REFRESH path
   * consumes the batch-end signal (ADR-0071); the other types pass through unchanged.
   *
   * @param msg        the deserialized sync message to route; must not be null
   * @param endOfBatch the dispatcher's batch-end hint for this task (ADR-0071)
   */
  private void syncMessageRouter(SyncMessage msg, boolean endOfBatch) {
    if (msg.type() == null) {
      log.debug("Received sync with null type, skipping");
      return;
    }
    switch (msg.type()) {
      case TYPE_INVALIDATE -> decisionHandler.handleLocalInvalidate(msg);
      case TYPE_INVALIDATE_ALL -> decisionHandler.handleLocalInvalidateAll(msg);
      case TYPE_REFRESH -> decisionHandler.handleRefresh(msg, endOfBatch);
      case TYPE_RULES_SYNC -> decisionHandler.handleRulesSync(msg);
      default -> log.warn("Unknown sync type: {}, cacheKey: {}", msg.type(), msg.cacheKey());
    }
  }
}
