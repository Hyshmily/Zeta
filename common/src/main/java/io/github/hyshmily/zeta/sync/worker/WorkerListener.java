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

import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.HEADER_TYPE;

import com.rabbitmq.client.Channel;
import io.github.hyshmily.zeta.sync.dispatcher.PerKeyOrderedDispatcher;
import io.github.hyshmily.zeta.sync.local.CacheSyncListener;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.amqp.AmqpConsume;
import io.github.hyshmily.zeta.util.amqp.AmqpMessageReader;
import io.github.hyshmily.zeta.util.amqp.AmqpMessageReader.AppIsolationFilter;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;

/**
 * Listens for Worker hot/cool decisions via the {@code zeta.send.exchange} direct
 * exchange (bound under {@code send.<appName>}) and applies them to the local Caffeine L1
 * cache via {@link WorkerDecisionHandler}.
 *
 * <p>This is the consumer-side counterpart of the Worker's {@code WorkerBroadcaster}.
 * Incoming AMQP messages are deserialized into {@link WorkerMessage} records and
 * routed by decision type:
 * <ul>
 *   <li><b>HOT</b> ({@link WorkerDecisionType#HOT}): Delegates to
 *       {@link WorkerDecisionHandler#handleHot}.</li>
 *   <li><b>COOL</b> ({@link WorkerDecisionType#COOL}): Delegates a run of consecutive
 *       COOLs to {@link WorkerDecisionHandler#handleCoolBatch}.</li>
 * </ul>
 *
 * <p>A decision is acknowledged once it is decoded and submitted; the ordered dispatcher
 * applies it asynchronously. A COOL the dispatcher refuses at submit time is applied
 * inline on the consumer thread (see {@link #onDispatchRejected}), while a refused HOT is
 * dropped.
 *
 * @see WorkerMessage
 * @see WorkerDecisionHandler
 * @see CacheSyncListener
 */
@Slf4j
@Internal
public class WorkerListener {

  /**
   * Cumulative COOL decisions the per-key ordered dispatcher refused (per-key
   * queue full or dispatcher closed). Each one is applied inline on the
   * consumer thread (see {@link #onDispatchRejected}), so a growing add signals
   * dispatcher saturation — degraded ordering, not data loss.
   */
  private final LongAdder coolDispatchRejections = new LongAdder();

  /**
   * Per-key FIFO dispatcher for ordered cache mutation execution. Built in the
   * constructor and {@code final}: the AMQP consumer threads must never be able
   * to observe a null dispatcher.
   */
  private final PerKeyOrderedDispatcher<WorkerMessage> dispatcher;

  /** Strategy that applies HOT and COOL decisions. */
  private final WorkerDecisionHandler decisionHandler;

  /**
   * Shared-broker appName isolation (design decisions: sync messages, see {@link AppIsolationFilter}):
   * decisions carrying a foreign {@code appName} header are dropped before
   * dispatch. {@code null}/blank local appName disables the filter (all
   * decisions processed); decisions <em>without</em> the header are always processed.
   */
  private final AppIsolationFilter isolation;

  /**
   * Creates a listener. {@code appName} may be {@code null}/blank to disable
   * the foreign-app filter (rolling-upgrade behavior).
   *
   * <p>Also builds the per-key ordered dispatcher that guarantees FIFO
   * processing of Worker decisions per cache key and spreads cluster-loader load via a
   * start-jitter park of {@code broadcastJitterMs} on each key's first cycle.
   */
  public WorkerListener(
    WorkerListenerProperties properties,
    ScheduledExecutorService scheduler,
    WorkerDecisionHandler decisionHandler,
    String appName
  ) {
    this.decisionHandler = decisionHandler;
    this.isolation = new AppIsolationFilter(appName);
    this.dispatcher = new PerKeyOrderedDispatcher<>(
      scheduler,
      "worker-listener",
      this::dispatchBatch,
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
   * <p>Queued Worker decisions (and jitter-delayed submissions still pending on the
   * shared scheduler) are dropped without execution: a queued COOL is never replayed and a
   * dropped HOT leaves the entry at its normal TTL until the key changes phase again.
   * Refusals caused by this close() reach {@link #onDispatchRejected} like any other
   * submit refusal.
   */
  @PreDestroy
  public void destroy() {
    dispatcher.close();
  }

  /**
   * Cumulative foreign-app decision drops (design decisions: sync messages). Non-zero means this
   * instance is discarding another application's decisions — or, when paired
   * with missing prewarming, that the local {@code app-name} is misconfigured.
   *
   * @return the total foreign-app drop add since startup
   */
  public long foreignAppDrops() {
    return isolation.drops();
  }

  /**
   * The most recently observed foreign sender appName, or {@code null} when no
   * foreign decision has been dropped yet.
   *
   * @return the last foreign appName, or {@code null}
   */
  public String lastForeignApp() {
    return isolation.lastForeignApp();
  }

  /**
   * RabbitMQ message callback for incoming Worker decisions. Decodes and submits the
   * decision, then acknowledges the message; an accepted decision runs on the ordered
   * dispatcher (with an optional start-jitter park for a HOT), while a refused COOL is
   * applied inline before the ack.
   *
   * <p>If processing fails with an exception, the message is negatively acknowledged
   * with {@code requeue=false} to prevent poison-message loops. Neither decision type is
   * re-driven — the Worker sends each one exactly once (design decisions: sync messages) — so a
   * lost HOT leaves the entry at its normal TTL and a lost COOL stays bounded by the hot hard TTL.
   *
   * @param channel the AMQP channel used for ack/nack operations
   * @param msg     the raw AMQP message containing the Worker decision; must not be null
   * @throws IOException if the channel's basicAck or basicNack call fails
   */
  public void handleWorkerMessage(Channel channel, Message msg) throws IOException {
    AmqpConsume.ackOrNack(channel, msg, log, "Worker message", () -> processWorker(msg));
  }

  /**
   * Decodes the raw AMQP message into a {@link WorkerMessage} and submits it to the
   * ordered dispatcher. When enabled, the dispatcher applies a random start-jitter park
   * within {@link WorkerListenerProperties#getBroadcastJitterMs()} to the key's worker
   * before its first cycle, spreading cluster-loader reads across a small time window
   * when the Worker broadcasts to many instances.
   *
   * <p>The jitter is a dispatcher-level park, NOT a sleep inside the task (a sleeping
   * task would hold a scheduler thread for the whole jitter window). Per-key FIFO
   * ordering is unaffected either way — ordering is fixed by the atomic submission into
   * the dispatcher's map, so a stale COOL can never land after a newer HOT from another
   * Worker (with cross-Worker decisions both are accepted unconditionally, which makes
   * strict arrival order the only safe ordering).
   *
   * @param msg the raw AMQP message; may have null or empty body, in which case
   *            the message is dropped without scheduling a task
   */
  private void processWorker(Message msg) {
    // design decisions: sync messages shared-broker isolation (see AppIsolationFilter): the
    // binding key already keeps a foreign app's decisions out of this queue, so this is the
    // receiver-side boundary for a mis-bound queue rather than the delivery mechanism. Drop
    // when the sender declared its appName and it differs from ours; a message without the
    // header (older Worker, rolling upgrade) or a listener without a configured
    // appName (unconfigured wiring) is always processed.
    //
    // Checked from the raw header BEFORE decoding: WorkerMessage.from allocates
    // the body String and reads every decision header, and all of that is wasted
    // work for a message we are about to throw away. On a shared broker with a
    // misconfigured app-name that is 100% of the traffic.
    if (isolation.dropIfForeign(msg, log, "worker decision")) {
      return;
    }

    Object typeHeader = msg.getMessageProperties().getHeader(HEADER_TYPE);
    WorkerDecisionType type = WorkerDecisionType.parse(typeHeader);
    if (type == null) {
      // Validate before the dispatcher: an unknown type must not occupy a queue slot and
      // a worker-thread grant only to be discarded.
      AmqpMessageReader.logUnknownType(log, "worker decision", typeHeader, msg);
      return;
    }

    WorkerMessage message = WorkerMessage.from(msg, type);
    if (message == null) {
      // Includes the unorderable (non-positive decision version) case, rejected at the
      // parse boundary before any dispatcher admission.
      log.debug("Received worker message with an empty body, an invalid key or a non-positive decision version");
      return;
    }
    boolean accepted = type.isControl()
      ? dispatcher.submitControl(message.cacheKey(), message)
      : dispatcher.submit(message.cacheKey(), message, 1, type.needsJitter());
    if (!accepted) {
      onDispatchRejected(message);
    }
  }

  /**
   * Runs one granted batch: the queued decisions of one key, applied in arrival order.
   * A run of consecutive COOL decisions is handed to the handler as one unit so it can
   * coalesce the pure in-memory part; HOT decisions stay one per call (they create entries
   * and run the cluster loader, so dropping one can change the final state). A failing
   * decision is logged and does not strand the rest of the batch.
   *
   * @param batch the granted batch, in submission order
   */
  private void dispatchBatch(List<WorkerMessage> batch) {
    int index = 0;
    while (index < batch.size()) {
      WorkerMessage message = batch.get(index);
      if (message.type() == WorkerDecisionType.COOL) {
        int end = index + 1;
        while (end < batch.size() && batch.get(end).type() == WorkerDecisionType.COOL) {
          end++;
        }
        try {
          decisionHandler.handleCoolBatch(batch.subList(index, end));
        } catch (Exception e) {
          logDispatchFailure(message, e);
        }
        index = end;
        continue;
      }
      try {
        decisionHandler.handleHot(message);
      } catch (Exception e) {
        logDispatchFailure(message, e);
      }
      index++;
    }
  }

  private void logDispatchFailure(WorkerMessage message, Exception e) {
    log.warn(
      "Error processing WorkerMessage: cacheKey={}, type={}, decisionVersion={}, nodeId={}, epoch={}",
      message.cacheKey(),
      message.type(),
      message.stamp().decisionVersion(),
      message.stamp().decisionNodeId(),
      message.stamp().decisionEpoch(),
      e
    );
  }

  /**
   * Cumulative COOL decisions the per-key ordered dispatcher refused; each was
   * re-applied inline on the consumer thread. Monotonic; safe to read from any
   * thread.
   *
   * @return the total dispatcher-refused COOL add since startup
   */
  public long coolDispatchRejections() {
    return coolDispatchRejections.sum();
  }

  /**
   * Fallback for a decision the ordered dispatcher refused at submit time
   * (per-key queue full or dispatcher closed).
   *
   * <p>A COOL is applied inline ({@link WorkerDecisionHandler#handleCoolInline},
   * a pure in-memory compute with no hooks): the Worker sends each decision
   * once, and a dropped COOL pins the entry at its HOT TTL until hard expiry.
   * A refused HOT stays a DEBUG drop — inlining it would run the cluster-loader
   * load on the AMQP consumer thread.
   *
   * @param msg the decision whose dispatcher submission was refused
   */
  private void onDispatchRejected(WorkerMessage msg) {
    if (msg.type() != WorkerDecisionType.COOL) {
      log.debug("Worker decision dispatcher refused a HOT for key={} (dropped: nothing re-sends it)", msg.cacheKey());
      return;
    }

    coolDispatchRejections.increment();
    log.debug("Worker decision dispatcher refused a COOL for key={}; applying inline", msg.cacheKey());
    // Applying a COOL is a local, bounded mutation; it is safe even while the
    // listener is closing, and dropping it would pin the entry at its HOT TTL.
    decisionHandler.handleCoolInline(msg);
  }
}
