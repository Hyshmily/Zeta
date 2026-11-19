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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * Listens for cache synchronization messages (INVALIDATE / REFRESH / PUT and the
 * clear family) from peer application instances via the {@code zeta.sync.exchange}
 * FanoutExchange and applies them through {@link SyncPolicy} and {@link DefaultSyncContext}.
 *
 * <p>This is the inbound half of the instance-to-instance cache coherence protocol.
 * The outbound half is {@link CacheSyncPublisher}. Together they ensure that a data
 * mutation on one instance is propagated to all peers.
 *
 * <p><b>Thread safety:</b> all cache mutations use
 * {@link com.github.benmanes.caffeine.cache.Cache#asMap()}{@code .compute()} for
 * atomic per-key updates. A message is acknowledged at the consumer boundary; an accepted
 * message's mutation runs asynchronously on the ordered dispatcher, and a REFRESH's first
 * cycle is parked for a random start-jitter delay (see {@link KeyDispatcher}) to
 * spread cluster-loader load across instances.
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
   * Dispatch key shared by every clear ({@code INVALIDATE_ALL} / {@code INVALIDATE_PREFIX}):
   * one sweep at a time instead of concurrent full scans starving the per-key tasks, and the
   * context coalesces the clears of one batch into a single pass. An {@link Object} that no
   * application cache key can equal, so a key named like the constant cannot join this channel.
   *
   * <p>See design decisions: buffering and ordering.
   */
  private static final Object CLEAR_DISPATCH_KEY = new Object() {
    @Override
    public String toString() {
      return "__zeta_clear__";
    }
  };

  /**
   * Bit shift converting a message body length into ~1 KB cost units
   * for dispatcher admission weighting ({@code body.length >> 10} ≈ KBs).
   */
  private static final int KB_SHIFT = 10;

  /** One-shot guard for the static node-id collision warning; one WARN is actionable. */
  private final AtomicBoolean nodeIdCollisionWarned = new AtomicBoolean();

  /** Per-key FIFO dispatcher for cache mutations; final, so no rebuild can orphan queued tasks. */
  private final KeyDispatcher<SyncMessage> dispatcher;

  /**
   * Cumulative inbound messages from a peer sharing this node's 10-bit {@code dataVersion}
   * node id: the two can mint equal stamps and the equal-version guards then skip updates in
   * both directions. A growing count means explicit node ids ({@code zeta.local.node-id})
   * should be assigned.
   */
  private final LongAdder nodeIdCollisions = new LongAdder();

  /**
   * Cumulative messages the ordered dispatcher refused (per-key queue full, global budget
   * exhausted, or the listener closing). Each is applied by the no-hook rejection fallback;
   * a growing count signals dispatcher saturation.
   */
  private final LongAdder syncDispatchRejections = new LongAdder();

  /** Strategy that decides how REFRESH, INVALIDATE, PUT and the clear family are applied. */
  private final SyncPolicy policy;

  /** Framework-owned guarded write path; the policy never receives it. */
  private final DefaultSyncContext context;

  /** This instance's identity: self-filter and node-id collision diagnostics. */
  private final NodeIdentity identity;

  /**
   * Shared-broker appName isolation (design decisions: sync messages, see {@link AppIsolationFilter}):
   * sync messages that belong to a <em>different</em> application sharing the
   * same broker are dropped. {@code null}/blank disables the filter, so every
   * application's messages are processed.
   *
   * <p>The sync exchange is a fanout whose name is a global constant, so on a
   * shared broker every instance receives every application's sync messages.
   */
  private final AppIsolationFilter isolation;

  /**
   * Data-stamp clock of this node, or {@code null} when observation is disabled. Every
   * inbound message carries a peer-allocated {@code dataVersion}; merging it into
   * the local clock is what keeps a lagging node's writes from being silently
   * outranked forever (design decisions: version stamps). The merge is a single atomic max, so
   * it runs on the AMQP consumer thread before the message is dispatched.
   */
  @Nullable
  private final StampClock stampClock;

  /**
   * Creates a listener and its per-key ordered dispatcher, fail-fast: a message can
   * arrive before any later lifecycle hook would have run.
   *
   * <p>{@code appName} may be {@code null}/blank to process sync messages from every
   * application.
   *
   * <p>Every positive inbound stamp is merged into {@code stampClock} unless the
   * anti-drift guard refuses it; a refused stamp is never allocated a replacement on this
   * consumer thread. The per-key applier re-stamps it on the ordered dispatcher instead
   * (design decisions: version stamps); a {@code null} clock skips observation.
   *
   * <p><b>Throughput ceiling of the supplied scheduler:</b> a REFRESH task submitted
   * by this listener holds a scheduler thread for the duration of its synchronous
   * cluster-loader load inside the sync context — the
   * {@code zeta.sync.jitter-ms} delay (default max 50ms) is a scheduler-side
   * park, NOT a {@code Thread.sleep} inside the task, so it does not consume a pool
   * thread. Under REFRESH storms the pool therefore processes at most
   * {@code poolSize / loadTimeMs} load-bearing sync tasks per second; sizing the shared
   * scheduler (and its {@code concurrentConsumers} feed) should account for that
   * per-task latency floor.
   *
   * @param properties      the sync configuration backing the dispatcher bounds and start jitter
   * @param scheduler       executor shared by the ordered dispatcher's cycle execution and its
   *                        start-jitter park
   * @param policy    strategy that decides how the decoded REFRESH, INVALIDATE, PUT
   *                  and clear messages are applied
   * @param context   framework-owned guarded write path; never handed to the policy
   * @param appName   this application's {@code zeta.app-name}; {@code null}/blank disables
   *                  foreign-app filtering
   * @param stampClock the node's data-stamp clock, or {@code null} to skip observation
   * @param identity  this instance's identity; never {@code null}
   */
  public CacheSyncListener(
    CacheSyncProperties properties,
    ScheduledExecutorService scheduler,
    SyncPolicy policy,
    DefaultSyncContext context,
    String appName,
    @Nullable StampClock stampClock,
    NodeIdentity identity
  ) {
    this.stampClock = stampClock;
    this.policy = policy;
    this.context = context;
    this.identity = identity;
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
   * Shuts down the ordered dispatcher.
   *
   * <p>Queued sync tasks are dropped without execution; a start-jitter park is cancelled by
   * the dispatcher, so a parked first cycle does not pin its payload until the delay expires.
   * The shutdown window is intentionally lossy. A lost INVALIDATE/REFRESH heals through the
   * next application-level write or the entry's hard TTL (design decisions: sync messages),
   * so a drain is not worth blocking context shutdown for.
   */
  @PreDestroy
  public void destroy() {
    dispatcher.close();
  }

  /**
   * Cumulative foreign-app sync drops (design decisions: sync messages). Non-zero means this instance
   * is discarding another application's sync messages — or, when paired with
   * missing cross-instance coherence, that the local {@code app-name} is
   * misconfigured.
   *
   * @return the total foreign-app drops since startup
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
  public @Nullable String lastForeignApp() {
    return isolation.lastForeignApp();
  }

  /**
   * RabbitMQ message callback for incoming sync messages. Parses the message and hands the
   * cache mutation to the ordered dispatcher, then acknowledges the message; an accepted
   * mutation runs after a random start-jitter park, while a refused submission falls back
   * inline before the ack (see {@link #onDispatchRejected}).
   *
   * <p>Self-message filtering (design decisions: sync messages): the fanout delivers the
   * sender's own broadcasts back to its queue. A self REFRESH/PUT and a self clear are dropped
   * before the body is decoded — the sender already applied them — while a self INVALIDATE and
   * INVALIDATE_BATCH are still processed because they heal an invalidate-vs-reload
   * repopulation. Messages without an origin header (older senders, rolling upgrades) are
   * always processed.
   *
   * <p>On success, the message is acknowledged via {@link Channel#basicAck}. On any processing
   * exception the message is negatively acknowledged with {@code requeue=false} to prevent
   * poison-message loops; the next application-level write re-sends the operation.
   *
   * @param channel the AMQP channel used for ack/nack operations
   * @param msg     the raw AMQP message whose body and headers carry the sync payload;
   *                must not be null
   * @throws IOException if the channel's basicAck or basicNack call fails
   */
  @SuppressWarnings("all")
  public void onMessage(Channel channel, Message msg) throws IOException {
    AmqpConsume.ackOrNack(channel, msg, log, "CacheSync", () -> {
      MessageProperties props = msg.getMessageProperties();
      if (isolation.dropIfForeign(msg, log, "sync message")) {
        return;
      }

      Object typeHeader = props.getHeader(HEADER_TYPE);
      SyncType type = SyncType.parse(typeHeader);
      if (type == null) {
        AmqpMessageReader.logUnknownType(log, "sync message", typeHeader, msg);
        return;
      }
      String origin = AmqpMessageReader.readStringHeader(props, HEADER_ORIGIN_INSTANCE);
      boolean own = origin != null && identity.instanceId().equals(origin);
      if (own && type.ignoresSelf()) {
        return;
      }
      if (!own) {
        detectNodeIdCollision(props, origin);
      }
      process(msg, type);
    });
  }

  /**
   * Cumulative count of inbound sync messages from a peer instance sharing this
   * node's {@code dataVersion} node id. Monotonic; safe to read from any thread.
   *
   * @return the total node-id collisions since startup
   */
  public long nodeIdCollisions() {
    return nodeIdCollisions.sum();
  }

  /**
   * Cumulative messages the ordered dispatcher refused and the listener applied through the
   * no-hook fallback. Monotonic; safe to read from any thread.
   *
   * @return the total dispatcher-refused sync message count since startup
   */
  public long syncDispatchRejections() {
    return syncDispatchRejections.sum();
  }

  /**
   * Warns once when a peer broadcasts under this node's 10-bit {@code dataVersion} node id:
   * equal node ids can mint equal stamps, which the equal-version guards skip in both
   * directions. The caller admits only messages that are not this instance's own broadcast.
   * Absent headers (older senders) are ignored: the check is diagnostic, never a drop reason.
   *
   * @param props  the sender's message properties (already read by the caller)
   * @param origin the already-read origin-instance header, used only for the warning
   */
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

  /**
   * Decodes the raw AMQP message into a {@link SyncMessage} and submits it to the ordered
   * dispatcher. The dispatcher applies a random start-jitter park within
   * {@link CacheSyncProperties#getJitterMs()} to the key's worker before its first cycle,
   * spreading cluster-loader reads across peers.
   *
   * <p>The dispatcher hands the whole granted batch of a key to the listener, which reduces it
   * before calling the typed handler method, so a REFRESH burst pays one loader call, a clear
   * burst one sweep and a chain of superseded PUTs one apply.
   *
   * @param msg  the raw AMQP message; if the body is null, empty, or cannot be
   *             parsed into a valid {@link SyncMessage}, the message is dropped
   *             without scheduling any task
   * @param type the type parsed and validated at the listener boundary
   */
  private void process(Message msg, SyncType type) {
    // A batched INVALIDATE (design decisions: invalidation) is a wire frame: it decomposes
    // into per-key tasks at the boundary, each with its own ordered queue, so every entry still
    // orders against that key's PUT/REFRESH. Each entry captures only key + version, so its
    // budget weight stays at the 1 floor.
    // Observe the frame maximum once, then each entry only when it was refused; the per-key
    // apply allocates any replacement stamp (design decisions: version stamps).
    if (type == SyncType.INVALIDATE_BATCH) {
      SyncMessage.ParsedBatch batch = SyncMessage.parseBatch(msg);
      if (batch == null) {
        log.debug("Received an unusable INVALIDATE_BATCH frame");
        return;
      }

      boolean frameAccepted = stampClock == null || stampClock.observe(batch.maxVersion());
      if (!frameAccepted) {
        // A refused maximum does not cover the entries; probe each one before dispatching.
        for (SyncMessage entry : batch.entries()) {
          stampClock.observe(entry.version());
        }
      }
      for (SyncMessage entry : batch.entries()) {
        submit(entry, 0);
      }
      return;
    }

    SyncMessage message = SyncMessage.from(msg, type);
    if (message == null) {
      log.debug("Received sync message with an empty body, an invalid key or a non-positive data version");
      return;
    }

    // Every type dispatches under its real key: a PUT's key comes from the HEADER_CACHE_KEY
    // header (decoded into cacheKey() at parse time), and both clear types share the dedicated
    // clear channel (see {@link #CLEAR_DISPATCH_KEY}). The clear channel does not serialize
    // against per-key work on the same namespace: anti-resurrection is carried by
    // EntryLifecycle's clear watermarks, not by dispatcher ordering.
    // Merge the peer stamp only; a refused one is re-stamped by the per-key apply.
    if (stampClock != null) {
      stampClock.observe(message.version());
    }
    submit(message, msg.getBody().length);
  }

  /**
   * Submit one decoded sync message to the ordered dispatcher, falling back to
   * the refusal ladder when the dispatcher refuses the task.
   *
   * @param message   the decoded message
   * @param bodyBytes the AMQP body length (0 when unknown) — weighted into the
   *                  dispatcher's global pending budget (~1 unit per KB)
   */
  private void submit(SyncMessage message, int bodyBytes) {
    boolean accepted = message.type().isClear()
      ? dispatcher.submitControl(CLEAR_DISPATCH_KEY, message)
      : dispatcher.submit(message.cacheKey(), message, 1 + (bodyBytes >> KB_SHIFT), message.type().needsJitter());
    if (!accepted) {
      onDispatchRejected(message);
    }
  }

  /**
   * Runs one granted batch: every queued message of one key (or of the clear channel) is
   * reduced to its effective message, covered messages are reported, and the typed handler
   * method runs the guarded apply.
   *
   * @param messages the granted batch, in submission order
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
   * Fallback for a message the ordered dispatcher refused at submit time (per-key queue full,
   * global budget exhausted, or the listener closing). A refused task is never retried, so
   * dropping it would leave a stale L1 entry for its whole hard TTL: a clear records its
   * watermarks and schedules the sweep, anything else is an O(1) in-memory invalidation.
   * Neither path fires a user hook, so the AMQP consumer thread never waits on application
   * code and can ack the delivery.
   *
   * @param message the decoded message that was refused
   */
  private void onDispatchRejected(SyncMessage message) {
    syncDispatchRejections.increment();
    try {
      SyncMessage fallback = message.asInvalidate();
      if (fallback.type().isClear()) {
        context.clearNow(new Batch.Clear(List.of(fallback)));
      } else {
        context.invalidateNow(fallback);
      }
    } catch (Exception e) {
      log.warn("Sync drop fallback failed: type={}, key={}", message.type(), message.cacheKey(), e);
    }
  }
}
