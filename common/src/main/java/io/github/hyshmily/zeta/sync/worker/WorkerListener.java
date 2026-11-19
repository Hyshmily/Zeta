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
import io.github.hyshmily.zeta.cache.EntryView;
import io.github.hyshmily.zeta.sync.dispatcher.KeyDispatcher;
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
import org.jspecify.annotations.Nullable;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * Listens for Worker hot/cool decisions via the {@code zeta.send.exchange} direct
 * exchange (bound under {@code send.<appName>}) and applies them to the local Caffeine L1
 * cache through {@link WorkerPolicy} and the framework-owned {@link DefaultWorkerContext}.
 *
 * <p>Incoming AMQP messages are deserialized into {@link WorkerDecision} records and routed
 * by {@link WorkerType}: HOT goes through {@link WorkerPolicy#hot}, COOL through
 * {@link WorkerPolicy#cool}. A decision is acknowledged once it is decoded and submitted;
 * the ordered dispatcher applies it asynchronously. A COOL the dispatcher refuses at submit
 * time is applied inline on the consumer thread (see {@link #onDispatchRejected}), while a
 * refused HOT is dropped.
 *
 * @see WorkerDecision
 * @see WorkerPolicy
 * @see DefaultWorkerContext
 * @see CacheSyncListener
 */
@Slf4j
@Internal
public class WorkerListener {

  /**
   * Cumulative COOL decisions the per-key ordered dispatcher refused (per-key
   * queue full or dispatcher closed). Each one is applied inline on the
   * consumer thread (see {@link #onDispatchRejected}), so a growing count signals
   * dispatcher saturation — degraded ordering, not data loss.
   */
  private final LongAdder coolDispatchRejections = new LongAdder();

  /**
   * Per-key FIFO dispatcher for ordered cache mutation execution. Built in the
   * constructor and {@code final}: the AMQP consumer threads must never be able
   * to observe a null dispatcher.
   */
  private final KeyDispatcher<WorkerDecision> dispatcher;

  /** Strategy that decides HOT and COOL outcomes. */
  private final WorkerPolicy policy;

  /** Framework-owned guarded write path and batch reducer. */
  private final DefaultWorkerContext context;

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
   * start-jitter park of {@code jitterMs} on each key's first cycle.
   *
   * @param properties      the Worker listener configuration
   * @param scheduler       executor shared by the ordered dispatcher's cycle execution and
   *                        its start-jitter park
   * @param policy   strategy that decides HOT and COOL outcomes
   * @param context  framework-owned guarded write path and batch reducer
   * @param appName  this application's {@code zeta.app-name}; {@code null}/blank
   *                 disables foreign-app filtering
   */
  public WorkerListener(
    WorkerListenerProperties properties,
    ScheduledExecutorService scheduler,
    WorkerPolicy policy,
    DefaultWorkerContext context,
    String appName
  ) {
    this.context = context;
    this.policy = policy;
    this.isolation = new AppIsolationFilter(appName);
    this.dispatcher = new KeyDispatcher<>(
      scheduler,
      "worker-listener",
      this::dispatchBatch,
      properties.getMaxPendingUnits(),
      properties.getJitterMs()
    );
  }

  /**
   * Closes the ordered dispatcher at context shutdown: queued decisions are dropped and a
   * start-jitter park is cancelled, so a parked first cycle does not pin its payload.
   *
   * <p>A queued COOL is never replayed and a dropped HOT leaves the entry at its normal TTL
   * until the key changes phase again. Refusals caused by this close reach
   * {@link #onDispatchRejected} like any other submit refusal.
   */
  @PreDestroy
  public void destroy() {
    dispatcher.close();
  }

  /**
   * Cumulative foreign-app decision drops (design decisions: sync messages). Non-zero means
   * this instance is discarding another application's decisions — or, when paired with
   * missing prewarming, that the local {@code app-name} is misconfigured.
   *
   * @return the total foreign-app drops since startup
   */
  public long foreignAppDrops() {
    return isolation.drops();
  }

  /**
   * The most recently observed foreign sender appName, or {@code null} when no foreign
   * decision has been dropped yet.
   *
   * @return the last foreign appName, or {@code null}
   */
  public @Nullable String lastForeignApp() {
    return isolation.lastForeignApp();
  }

  /**
   * RabbitMQ message callback for incoming Worker decisions. Decodes and submits the
   * decision, then acknowledges the message; an accepted decision runs on the ordered
   * dispatcher (with an optional start-jitter park for a HOT), while a refused COOL is
   * applied inline before the ack.
   *
   * <p>If processing fails with an exception, the message is negatively acknowledged with
   * {@code requeue=false} to prevent poison-message loops. Neither decision type is
   * re-driven — the Worker sends each one exactly once (design decisions: sync messages) — so
   * a lost HOT leaves the entry at its normal TTL and a lost COOL stays bounded by the hot
   * hard TTL.
   *
   * @param channel the AMQP channel used for ack/nack operations
   * @param msg     the raw AMQP message containing the Worker decision; must not be null
   * @throws IOException if the channel's basicAck or basicNack call fails
   */
  public void onMessage(Channel channel, Message msg) throws IOException {
    AmqpConsume.ackOrNack(channel, msg, log, "Worker message", () -> process(msg));
  }

  /**
   * Decodes the raw AMQP message into a {@link WorkerDecision} and submits it to the ordered
   * dispatcher.
   *
   * @param msg the raw AMQP message; may have null or empty body, in which case the message
   *            is dropped without scheduling a task
   */
  private void process(Message msg) {
    MessageProperties props = msg.getMessageProperties();
    if (isolation.dropIfForeign(msg, log, "worker decision")) {
      return;
    }

    Object typeHeader = props.getHeader(HEADER_TYPE);
    WorkerType type = WorkerType.parse(typeHeader);
    if (type == null) {
      AmqpMessageReader.logUnknownType(log, "worker decision", typeHeader, msg);
      return;
    }

    WorkerDecision decision = WorkerDecision.from(msg, type);
    if (decision == null) {
      log.debug("Received worker message with an empty body, an invalid key or a non-positive decision version");
      return;
    }
    boolean accepted = type.isControl()
      ? dispatcher.submitControl(decision.cacheKey(), decision)
      : dispatcher.submit(decision.cacheKey(), decision, 1, type.needsJitter());
    if (!accepted) {
      onDispatchRejected(decision);
    }
  }

  /**
   * Runs one granted batch: the queued decisions of one key, applied in arrival order. A run
   * of consecutive COOL decisions is reduced to the decision that survives the version guard;
   * covered decisions are reported as skipped and the policy sees only the effective ones. A
   * failing decision is logged and does not strand the rest of the batch.
   *
   * @param batch the granted batch, in submission order
   */
  private void dispatchBatch(List<WorkerDecision> batch) {
    int index = 0;
    while (index < batch.size()) {
      WorkerDecision decision = batch.get(index);
      if (decision.type() == WorkerType.HOT) {
        invokeHot(decision);
        index++;
        continue;
      }

      int end = index + 1;
      while (end < batch.size() && batch.get(end).type() == WorkerType.COOL) {
        end++;
      }
      if (end == index + 1) {
        invokeCool(decision);
      } else {
        try {
          for (WorkerDecision effective : context.reduceCool(batch.subList(index, end))) {
            invokeCool(effective);
          }
        } catch (Exception e) {
          logDispatchFailure(decision, e);
        }
      }
      index = end;
    }
  }

  private void invokeHot(WorkerDecision decision) {
    try {
      HotAction prepared = context.resolveHot(decision);
      HotAction action;
      if (prepared instanceof HotAction.Skip) {
        action = prepared;
      } else {
        HotValue candidate = ((HotAction.Promote) prepared).value();
        try {
          action = policy.hot(decision, candidate, context.view(decision.cacheKey()));
        } catch (Exception e) {
          logDispatchFailure(decision, e);
          action = new HotAction.Skip(HotSkipReason.POLICY_FAILED);
        }
        if (action == null) {
          warnNullAction(decision, "hot");
          action = new HotAction.Skip(HotSkipReason.POLICY_FAILED);
        }
      }
      context.settleHot(decision, action);
    } catch (Exception e) {
      logDispatchFailure(decision, e);
    }
  }

  private void invokeCool(WorkerDecision decision) {
    try {
      CoolSkipReason precheck = context.preCheckCool(decision);
      if (precheck != null) {
        context.settleCool(decision, new CoolAction.Skip(precheck));
        return;
      }
      EntryView current = context.view(decision.cacheKey());
      CoolAction action;
      try {
        action = policy.cool(decision, current);
      } catch (Exception e) {
        logDispatchFailure(decision, e);
        action = new CoolAction.Skip(CoolSkipReason.POLICY_FAILED);
      }
      if (action == null) {
        warnNullAction(decision, "cool");
        action = new CoolAction.Skip(CoolSkipReason.POLICY_FAILED);
      }
      context.settleCool(decision, action);
    } catch (Exception e) {
      logDispatchFailure(decision, e);
    }
  }

  private void warnNullAction(WorkerDecision decision, String method) {
    log.warn("WorkerPolicy.{} returned null; settling as POLICY_FAILED: key={}", method, decision.cacheKey());
  }

  private void logDispatchFailure(WorkerDecision decision, Exception e) {
    log.warn(
      "Error processing WorkerDecision: cacheKey={}, type={}, decisionVersion={}, nodeId={}, epoch={}",
      decision.cacheKey(),
      decision.type(),
      decision.stamp().decisionVersion(),
      decision.stamp().decisionNodeId(),
      decision.stamp().decisionEpoch(),
      e
    );
  }

  /**
   * Cumulative COOL decisions the per-key ordered dispatcher refused; each was re-applied
   * inline on the consumer thread. Monotonic; safe to read from any thread.
   *
   * @return the total dispatcher-refused COOL decisions since startup
   */
  public long coolDispatchRejections() {
    return coolDispatchRejections.sum();
  }

  /**
   * Fallback for a decision the ordered dispatcher refused at submit time (per-key queue
   * full or dispatcher closed).
   *
   * <p>A COOL is applied inline by the framework context (a pure in-memory compute with no
   * hooks): the Worker sends each decision once, and a dropped COOL pins the entry at its HOT
   * TTL until hard expiry. A refused HOT stays a DEBUG drop — inlining it would run the
   * cluster-loader load on the AMQP consumer thread.
   *
   * @param decision the decision whose dispatcher submission was refused
   */
  private void onDispatchRejected(WorkerDecision decision) {
    if (decision.type() != WorkerType.COOL) {
      log.debug(
        "Worker decision dispatcher refused a HOT for key={} (dropped: nothing re-sends it)",
        decision.cacheKey()
      );
      return;
    }

    coolDispatchRejections.increment();
    log.debug("Worker decision dispatcher refused a COOL for key={}; applying inline", decision.cacheKey());
    context.coolNow(decision);
  }
}
