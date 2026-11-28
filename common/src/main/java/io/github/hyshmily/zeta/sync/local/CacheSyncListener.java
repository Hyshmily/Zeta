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

import com.rabbitmq.client.Channel;
import io.github.hyshmily.zeta.sync.dispatcher.KeyDispatcher;
import io.github.hyshmily.zeta.sync.worker.WorkerListener;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.NodeIdentity;
import io.github.hyshmily.zeta.util.amqp.AmqpConsume;
import io.github.hyshmily.zeta.util.amqp.AmqpMessageReader;
import io.github.hyshmily.zeta.util.amqp.AmqpMessageReader.AppIsolationFilter;
import io.github.hyshmily.zeta.util.clock.StampClock;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * Inbound listener for peer cache synchronization messages (INVALIDATE, REFRESH, PUT, and clear
 * operations) delivered via the {@code zeta.sync.exchange} fanout exchange.
 *
 * <p>Messages are filtered for app isolation and self-origin rules at the AMQP consumer boundary,
 * merged into the node-local {@link StampClock}, and dispatched to a per-key FIFO
 * {@link KeyDispatcher} (with a dedicated shared channel for clear sweeps). Accepted mutations run
 * asynchronously through {@link DefaultSyncContext} and {@link SyncPolicy}; refused submissions
 * fall back inline to non-blocking local invalidation before the delivery is acknowledged.
 *
 * @see CacheSyncPublisher
 * @see SyncMessage
 * @see SyncPolicy
 * @see WorkerListener
 */
@Slf4j
@Internal
public class CacheSyncListener {

  /**
   * Dedicated dispatch key shared by {@code INVALIDATE_ALL} and {@code INVALIDATE_PREFIX} so
   * sweeps serialize and coalesce without colliding with any application cache key.
   */
  private static final Object CLEAR_DISPATCH_KEY = new Object() {
    @Override
    public String toString() {
      return "__zeta_clear__";
    }
  };

  /** Bit shift converting body byte length into ~1 KB dispatcher budget weight units. */
  private static final int KB_SHIFT = 10;

  private final KeyDispatcher<SyncMessage> dispatcher;
  private final SyncPolicy policy;
  private final DefaultSyncContext context;
  private final NodeIdentity identity;
  private final AppIsolationFilter isolation;
  private final @Nullable StampClock stampClock;

  private final AtomicBoolean nodeIdCollisionWarned = new AtomicBoolean();
  private final LongAdder nodeIdCollisions = new LongAdder();
  private final LongAdder syncDispatchRejections = new LongAdder();

  /**
   * Creates a sync listener and eagerly initializes its per-key ordered {@link KeyDispatcher}.
   *
   * @param properties sync configuration providing dispatcher capacity and start-jitter bounds
   * @param scheduler  scheduler driving ordered dispatch cycles and start-jitter parks
   * @param policy     strategy governing how decoded sync messages are applied
   * @param context    framework-owned guarded write path for sync mutations
   * @param appName    application name for shared-broker isolation; {@code null}/blank disables it
   * @param stampClock node-local data-stamp clock, or {@code null} to skip stamp observation
   * @param identity   this instance's identity for self-filtering and node-id collision diagnostics
   * @throws NullPointerException if any required collaborator is {@code null}
   */
  public CacheSyncListener(
    CacheSyncProperties properties,
    ScheduledExecutorService scheduler,
    SyncPolicy policy,
    DefaultSyncContext context,
    @Nullable String appName,
    @Nullable StampClock stampClock,
    NodeIdentity identity
  ) {
    Objects.requireNonNull(properties, "properties must not be null");
    Objects.requireNonNull(scheduler, "scheduler must not be null");
    this.policy = Objects.requireNonNull(policy, "policy must not be null");
    this.context = Objects.requireNonNull(context, "context must not be null");
    this.identity = Objects.requireNonNull(identity, "identity must not be null");
    this.stampClock = stampClock;
    this.isolation = new AppIsolationFilter(appName);
    this.dispatcher = new KeyDispatcher<>(
      scheduler,
      "cache-sync",
      this::dispatchBatch,
      properties.getMaxPendingUnits(),
      properties.getJitterMs()
    );
  }

  /**
   * Closes the ordered dispatcher on shutdown, dropping queued tasks and cancelling parked jitter
   * cycles (intentionally lossy under the at-most-once sync contract).
   */
  @PreDestroy
  public void destroy() {
    dispatcher.close();
  }

  /**
   * AMQP consumer callback handling inbound peer sync deliveries.
   *
   * <p>Applies app-name isolation, drops every echo of this instance's own fanout broadcast, parses
   * the message type header, checks for peer node-id collisions, and dispatches the decoded
   * message. Acknowledges on success and nacks with {@code requeue=false} on processing errors.
   *
   * @param channel the AMQP channel used for ack/nack
   * @param msg     the inbound AMQP message; must not be {@code null}
   * @throws IOException if {@code basicAck} or {@code basicNack} fails
   */
  public void onMessage(Channel channel, Message msg) throws IOException {
    AmqpConsume.ackOrNack(channel, msg, log, "CacheSync", () -> {
      MessageProperties props = msg.getMessageProperties();
      if (isolation.dropIfForeign(msg, log, "sync message")) {
        return;
      }

      // Every fanout echo of this instance's own broadcast is dropped unread. The local mutation
      // already recorded its tombstone and removed the entry under the bin lock, and a
      // Scope.REMOTE invalidation intentionally leaves local L1 untouched on the sending node.
      String origin = AmqpMessageReader.readStringHeader(props, HEADER_ORIGIN_INSTANCE);
      if (origin != null && identity.instanceId().equals(origin)) {
        return;
      }

      Object typeHeader = props.getHeader(HEADER_TYPE);
      SyncType type = SyncType.parse(typeHeader);
      if (type == null) {
        AmqpMessageReader.logUnknownType(log, "sync message", typeHeader, msg);
        return;
      }

      detectNodeIdCollision(props, origin);
      process(msg, type);
    });
  }

  /** Returns the cumulative count of foreign-application sync messages dropped since startup. */
  public long foreignAppDrops() {
    return isolation.drops();
  }

  /** Returns the most recently dropped foreign sender app name, or {@code null} if none. */
  public @Nullable String lastForeignApp() {
    return isolation.lastForeignApp();
  }

  /** Returns the cumulative count of peer messages sharing this instance's 10-bit node id. */
  public long nodeIdCollisions() {
    return nodeIdCollisions.sum();
  }

  /** Returns the cumulative count of sync messages refused by the dispatcher and handled inline. */
  public long syncDispatchRejections() {
    return syncDispatchRejections.sum();
  }

  /**
   * Decodes an inbound AMQP message into one or more {@link SyncMessage} tasks, observes peer
   * version stamps on {@link #stampClock}, and submits them to {@link #dispatcher}.
   *
   * @param msg  the raw AMQP message
   * @param type the validated {@link SyncType}
   */
  private void process(Message msg, SyncType type) {
    if (type == SyncType.INVALIDATE_BATCH) {
      processBatchInvalidate(msg);
      return;
    }

    SyncMessage message = SyncMessage.from(msg, type);
    if (message == null) {
      return;
    }

    // Merge the peer stamp on the consumer thread; a stamp rejected by anti-drift is
    // re-allocated later on the ordered dispatcher instead of here.
    if (stampClock != null) {
      stampClock.observe(message.version());
    }
    byte[] body = msg.getBody();
    submit(message, body == null ? 0 : body.length);
  }

  /**
   * Decomposes an {@link SyncType#INVALIDATE_BATCH} wire frame into per-key invalidation tasks so
   * each entry preserves per-key FIFO ordering against PUT/REFRESH messages.
   *
   * @param msg the raw AMQP batch-invalidation message
   */
  private void processBatchInvalidate(Message msg) {
    SyncMessage.ParsedBatch batch = SyncMessage.parseBatch(msg);
    if (batch == null) {
      return;
    }
    if (stampClock != null && !stampClock.observe(batch.maxVersion())) {
      // Frame max was rejected by anti-drift and does not cover individual entries; probe each one.
      for (SyncMessage entry : batch.entries()) {
        stampClock.observe(entry.version());
      }
    }
    // Each entry carries only key + version, so its dispatcher budget weight stays at the 1 floor.
    for (SyncMessage entry : batch.entries()) {
      submit(entry, 0);
    }
  }

  /**
   * Submits a decoded sync message to {@link #dispatcher}, invoking {@link #onDispatchRejected}
   * inline if the dispatcher refuses admission.
   *
   * @param message   the decoded sync message
   * @param bodyBytes raw AMQP body length used for global budget weighting (~1 unit per KB)
   */
  private void submit(SyncMessage message, int bodyBytes) {
    // Clears share CLEAR_DISPATCH_KEY; anti-resurrection against per-key work is enforced by
    // EntryLifecycle watermarks rather than dispatcher serialization.
    Object key = message.type().isClear() ? CLEAR_DISPATCH_KEY : message.cacheKey();
    boolean accepted = message.type().isControl()
      ? dispatcher.submitControl(key, message)
      : dispatcher.submit(key, message, 1 + (bodyBytes >> KB_SHIFT), message.type().needsJitter());
    if (!accepted) {
      onDispatchRejected(message);
    }
  }

  /**
   * Reduces and applies one granted dispatcher batch through {@link #context} and {@link #policy}.
   *
   * @param messages non-empty batch in submission order
   */
  private void dispatchBatch(List<SyncMessage> messages) {
    try {
      context.apply(Batch.reduce(messages), policy);
    } catch (Exception e) {
      SyncMessage first = messages.get(0);
      log.error(
        "Sync batch failed: type={}, key={}, version={}, batchSize={}",
        first.type(),
        first.cacheKey(),
        first.version(),
        messages.size(),
        e
      );
    }
  }

  /**
   * No-hook fallback applied on the consumer thread when {@link #dispatcher} refuses a message,
   * converting it to an O(1) local invalidation or clear so L1 is not left stale for its hard TTL.
   *
   * @param message the refused sync message
   */
  private void onDispatchRejected(SyncMessage message) {
    syncDispatchRejections.increment();
    try {
      // Never fire user hooks on the AMQP consumer thread so delivery ack is never stalled.
      context.fallbackEvict(message);
    } catch (Exception e) {
      log.warn("Sync drop fallback failed: type={}, key={}", message.type(), message.cacheKey(), e);
    }
  }

  /** Logs a one-shot warning when a non-self peer shares this instance's 10-bit node id. */
  private void detectNodeIdCollision(MessageProperties props, @Nullable String origin) {
    long peer = AmqpMessageReader.readLongHeader(props, HEADER_ORIGIN_NODE_ID, -1L);
    if (peer != identity.nodeId()) {
      return;
    }
    nodeIdCollisions.increment();
    if (nodeIdCollisionWarned.compareAndSet(false, true)) {
      log.warn(
        "dataVersion node id {} is shared with peer instance '{}': equal stamps from these instances are skipped in " +
          "both directions. Set a unique zeta.local.node-id (0..1023) per instance.",
        peer,
        origin
      );
    }
  }
}
