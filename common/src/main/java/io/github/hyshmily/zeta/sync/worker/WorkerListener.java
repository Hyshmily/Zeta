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

import com.rabbitmq.client.Channel;
import io.github.hyshmily.zeta.heat.*;
import io.github.hyshmily.zeta.model.DecisionKind;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.scheduler.WarmScheduler;
import io.github.hyshmily.zeta.sync.DecisionPlaneSupport;
import io.github.hyshmily.zeta.sync.dispatcher.KeyDispatcher;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.amqp.AmqpConsume;
import io.github.hyshmily.zeta.util.amqp.AmqpMessageReader.AppIsolationFilter;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.amqp.core.Message;

/**
 * Inbound listener for Worker HOT/COOL heat decisions, applying them to {@link HeatDirectory}
 * through a per-key ordered {@link KeyDispatcher}.
 *
 * <p>Decisions are acknowledged at the AMQP boundary once submitted; {@link HeatDirectory} owns
 * version, epoch, incarnation, and ghost-COOL guards, {@link HotKeyHandler} runs optional business
 * actions, and {@link HotKeyListener}s observe settled transitions. Nothing on this plane alters
 * an entry's TTL.
 *
 * <p><b>Best-effort delivery.</b> Workers broadcast each decision once. When the ordered dispatcher
 * refuses a COOL decision, its lease is still revoked inline on the consumer thread (without user
 * callbacks to keep the AMQP thread non-blocking); a refused HOT decision is dropped.
 */
@Slf4j
@Internal
public class WorkerListener {

  private final KeyDispatcher<WorkerDecision> dispatcher;
  private final HeatDirectory heatDirectory;
  private final HotKeyHandler handler;
  private final List<HotKeyListener> listeners;
  private final HotKeyContextFactory contexts;
  private final @Nullable WarmScheduler warmScheduler;
  private final AppIsolationFilter isolation;

  private final LongAdder coolDispatchRejections = new LongAdder();
  private volatile boolean closed;

  /**
   * Creates a Worker decision listener and wires the passive lease-end callback on
   * {@code heatDirectory}.
   *
   * @param properties    listener configuration supplying dispatcher bounds and start jitter
   * @param scheduler     scheduler driving ordered per-key dispatch cycles
   * @param heatDirectory cluster lease table owning ordering and incarnation guards
   * @param handler       replaceable HOT/COOL business handler
   * @param listeners     best-effort lifecycle observers
   * @param contexts      factory creating a guarded {@link HotKeyContext} for one applied decision
   * @param warmScheduler the local renewal scheduler, or {@code null} when warming is unavailable
   * @param appName       application name for shared-broker isolation, or {@code null}/blank to disable
   * @throws NullPointerException if any required collaborator is {@code null}
   */
  public WorkerListener(
    WorkerListenerProperties properties,
    ScheduledExecutorService scheduler,
    HeatDirectory heatDirectory,
    HotKeyHandler handler,
    List<HotKeyListener> listeners,
    HotKeyContextFactory contexts,
    @Nullable WarmScheduler warmScheduler,
    @Nullable String appName
  ) {
    Objects.requireNonNull(properties, "properties must not be null");
    Objects.requireNonNull(scheduler, "scheduler must not be null");
    this.heatDirectory = Objects.requireNonNull(heatDirectory, "heatDirectory must not be null");
    this.handler = Objects.requireNonNull(handler, "handler must not be null");
    this.listeners = List.copyOf(Objects.requireNonNull(listeners, "listeners must not be null"));
    this.contexts = Objects.requireNonNull(contexts, "contexts must not be null");
    this.warmScheduler = warmScheduler;
    this.isolation = new AppIsolationFilter(appName);
    this.dispatcher = new KeyDispatcher<>(
      scheduler,
      "worker-listener",
      this::dispatchBatch,
      properties.getMaxPendingUnits(),
      properties.getJitterMs()
    );
    heatDirectory.setPassiveEndSink((lease, reason) -> {
      if (!closed) {
        cancelColdRenewal(lease.key());
        notifyCool(lease, reason);
      }
    });
  }

  /** Closes the ordered dispatcher and detaches the lease table's passive-end callback. */
  @PreDestroy
  public void destroy() {
    closed = true;
    heatDirectory.setPassiveEndSink(null);
    dispatcher.close();
  }

  /**
   * AMQP consumer callback handling incoming Worker heat decisions, acknowledging on completion
   * and nacking with {@code requeue=false} on processing failures.
   *
   * @param channel the AMQP channel used for ack/nack
   * @param msg     the inbound AMQP message
   * @throws IOException if {@code basicAck} or {@code basicNack} fails
   */
  public void onMessage(Channel channel, Message msg) throws IOException {
    AmqpConsume.ackOrNack(channel, msg, log, "Worker message", () -> process(msg));
  }

  /** Returns the cumulative count of foreign-application Worker decisions dropped since startup. */
  public long foreignAppDrops() {
    return isolation.drops();
  }

  /** Returns the most recently dropped foreign sender app name, or {@code null} if none. */
  public @Nullable String lastForeignApp() {
    return isolation.lastForeignApp();
  }

  /** Returns the cumulative count of COOL decisions refused by the dispatcher and revoked inline. */
  public long coolDispatchRejections() {
    return coolDispatchRejections.sum();
  }

  /**
   * Filters, decodes, and submits one AMQP decision message to {@link #dispatcher}, falling back
   * to {@link #onDispatchRejected} if refused.
   */
  private void process(Message msg) {
    if (isolation.dropIfForeign(msg, log, "worker decision")) {
      return;
    }
    WorkerDecision decision = WorkerDecision.from(msg);
    if (decision == null) {
      return;
    }
    DecisionKind kind = decision.kind();
    boolean accepted = kind.isControl()
      ? dispatcher.submitControl(decision.cacheKey(), decision)
      : dispatcher.submit(decision.cacheKey(), decision, 1, kind.needsJitter());
    if (!accepted) {
      onDispatchRejected(decision);
    }
  }

  /**
   * Applies one granted per-key decision batch, folding same-node runs down to at most one COOL
   * followed by one HOT so redundant prewarms and duplicate cool callbacks are avoided.
   *
   * <p>Cross-node or anonymous batches are non-comparable and execute verbatim in arrival order.
   * Stale candidates report {@link DecisionSkipReason#VERSION_STALE}; superseded kept candidates
   * report {@link DecisionSkipReason#COVERED_BY_NEWER_DECISION}.
   *
   * @param batch non-empty list of decisions for one key in arrival order
   */
  private void dispatchBatch(List<WorkerDecision> batch) {
    if (batch.size() == 1) {
      applyDecision(batch.get(0));
      return;
    }
    if (!isSameIncarnationBatch(batch)) {
      for (WorkerDecision decision : batch) {
        applyDecision(decision);
      }
      return;
    }

    // Fold against the same liveness rule grant() applies so a dead incarnation's stamp cannot
    // suppress decisions from the replacement incarnation.
    DecisionStamp effective = heatDirectory.liveStampOf(batch.get(0).cacheKey());
    WorkerDecision cool = null;
    WorkerDecision hot = null;
    for (WorkerDecision candidate : batch) {
      if (VersionGuard.shouldSkipForWorker(effective, candidate.stamp())) {
        notifySkipped(candidate, DecisionSkipReason.VERSION_STALE);
        continue;
      }
      if (candidate.isHot()) {
        if (hot != null) {
          notifySkipped(hot, DecisionSkipReason.COVERED_BY_NEWER_DECISION);
        }
        hot = candidate;
      } else {
        // A newer COOL supersedes both any preceding HOT and any earlier COOL in this batch.
        if (hot != null) {
          notifySkipped(hot, DecisionSkipReason.COVERED_BY_NEWER_DECISION);
          hot = null;
        }
        if (cool != null) {
          notifySkipped(cool, DecisionSkipReason.COVERED_BY_NEWER_DECISION);
        }
        cool = candidate;
      }
      effective = candidate.stamp();
    }
    if (cool != null) {
      applyDecision(cool);
    }
    if (hot != null) {
      applyDecision(hot);
    }
  }

  /**
   * Applies one HOT or COOL decision to {@link #heatDirectory}, invokes {@link #handler}, and
   * notifies listeners in strict causal order so a replaced dead lease's {@code onCool} always
   * precedes the new lease's {@code onHot}.
   *
   * @param decision the decision to apply
   */
  private void applyDecision(WorkerDecision decision) {
    try {
      HeatDirectory.LeaseOutcome result = decision.isHot()
        ? heatDirectory.grant(decision)
        : heatDirectory.revoke(decision);
      if (!result.applied()) {
        notifySkipped(decision, result.skipReason());
        return;
      }

      HeatLease endedLease = result.endedLease();
      HeatLease.EndReason endReason = result.endReason();
      if (decision.isHot()) {
        // End any replaced dead lease first so its onCool never wipes state created by onHot.
        if (endedLease != null && endReason != null) {
          notifyCool(endedLease, endReason);
        }

        invokeHandler(decision, result.newlyHot());
        if (!listeners.isEmpty()) {
          fireListeners("onHot", decision.cacheKey(), listener -> listener.onHot(decision, result.newlyHot()));
        }
      } else {
        invokeHandler(decision, false);
        if (endedLease != null && endReason != null) {
          notifyCool(endedLease, endReason);
        }
      }
    } catch (Exception e) {
      log.warn("Error processing WorkerDecision: {}", decision, e);
    }
  }

  /**
   * Consumer-thread fallback when {@link #dispatcher} refuses a decision: revokes a COOL lease
   * inline and cancels keep-warm without running user hooks; drops refused HOT decisions so
   * prewarm work never runs on the AMQP thread.
   */
  private void onDispatchRejected(WorkerDecision decision) {
    if (decision.isHot()) {
      return;
    }
    coolDispatchRejections.increment();
    if (heatDirectory.revoke(decision).endedLease() != null) {
      // A COOL leaves the value resident but stops justifying background warming; cancelling keeps
      // a scheduled task from reloading a key the control plane just classified as cold.
      cancelColdRenewal(decision.cacheKey());
    }
  }

  /** Cancels the key's renewal when the namespace policy no longer accepts it as cold. */
  private void cancelColdRenewal(String key) {
    if (warmScheduler != null) {
      warmScheduler.cancelIfCold(key);
    }
  }

  /**
   * Returns {@code true} if every decision in {@code batch} originates from one Worker incarnation.
   *
   * <p>The epoch is part of the identity: after a restart the version counter restarts too, so a
   * batch mixing both incarnations of one node carries two incomparable version spaces. Such a batch
   * runs verbatim, leaving each decision to the lease table's incarnation and version guards.
   */
  private static boolean isSameIncarnationBatch(List<WorkerDecision> batch) {
    DecisionStamp first = batch.get(0).stamp();
    String nodeId = first.decisionNodeId();
    if (nodeId == null || nodeId.isBlank()) {
      return false;
    }
    long epoch = first.decisionEpoch();
    for (int i = 1; i < batch.size(); i++) {
      DecisionStamp stamp = batch.get(i).stamp();
      if (!nodeId.equals(stamp.decisionNodeId()) || stamp.decisionEpoch() != epoch) {
        return false;
      }
    }
    return true;
  }

  /**
   * Invokes {@link #handler} after the lease transition, containing failures so registered
   * listeners still run. The context is built from the post-transition lease, so a handler always
   * observes the HOT or COOL state that the decision just established; a throwing handler is
   * logged and swallowed rather than aborting notification of the other listeners.
   */
  private void invokeHandler(WorkerDecision decision, boolean newlyHot) {
    try {
      HotKeyContext context = contexts.create(decision, newlyHot);
      if (decision.isHot()) {
        handler.onHot(context);
      } else {
        handler.onCool(context);
      }
    } catch (RuntimeException e) {
      log.warn("HotKeyHandler.{} failed: {}", decision.isHot() ? "onHot" : "onCool", decision, e);
    }
  }

  /** Reports a skipped or folded-away decision to registered listeners. */
  private void notifySkipped(WorkerDecision decision, @Nullable DecisionSkipReason reason) {
    if (reason == null || listeners.isEmpty()) {
      return;
    }
    if (decision.isHot()) {
      fireListeners("onHotSkipped", decision.cacheKey(), listener -> listener.onHotSkipped(decision, reason));
    } else {
      fireListeners("onCoolSkipped", decision.cacheKey(), listener -> listener.onCoolSkipped(decision, reason));
    }
  }

  /** Notifies registered listeners that a heat lease ended. */
  private void notifyCool(HeatLease lease, HeatLease.EndReason reason) {
    if (listeners.isEmpty()) {
      return;
    }
    fireListeners("onCool", lease.key(), listener -> listener.onCool(lease, reason));
  }

  /** Dispatches a listener callback safely through {@link DecisionPlaneSupport#fireHooks}. */
  private void fireListeners(String hookName, String key, Consumer<HotKeyListener> callback) {
    DecisionPlaneSupport.fireHooks(listeners, "HotKeyListener", hookName, key, callback, log);
  }
}
