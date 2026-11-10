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
import static io.github.hyshmily.zeta.sync.local.SyncMessage.from;

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
 * FanoutExchange and applies them through {@link SyncPolicy} and {@link SyncContext}.
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
   * clearing is table- or namespace-wide, so two clears of unrelated namespaces compete for the
   * same L1 table. One key puts them on one worker thread — one sweep at a time instead of N
   * concurrent full scans starving the per-key tasks — and lets the context coalesce the clears
   * of one batch into a single pass.
   *
   * <p>An {@link Object} rather than a magic String on purpose: the dispatcher map is keyed by
   * {@code Object}, and no application cache key can equal this instance, so a key named like
   * the constant can never join the clear channel.
   */
  private static final Object CLEAR_DISPATCH_KEY = new Object() {
    @Override
    public String toString() {
      return "__zeta_clear__";
    }
  };

  /**
   * Bit-shift divisor converting a message body length into ~1KB cost units
   * for dispatcher admission weighting ({@code body.length >> 10} ≈ KBs).
   */
  private static final int BYTE_WEIGHT = 10;

  /**
   * One-shot guard for the node-id collision warning. The misconfiguration is
   * static, so every inbound peer message would repeat the same line; one
   * WARN at startup-adjacent traffic is enough to make it actionable.
   */
  private final AtomicBoolean nodeIdCollisionWarned = new AtomicBoolean();

  /**
   * Per-key FIFO dispatcher for ordered cache mutation execution. Built in the
   * constructor and {@code final}: a rebuild seam could orphan queued tasks, and
   * {@code volatile} buys nothing once the reference is safely published by the
   * constructor.
   */
  private final KeyDispatcher<SyncMessage> dispatcher;

  /**
   * Cumulative inbound messages observed from a peer instance sharing this
   * node's 10-bit {@code dataVersion} node id. Two such instances can mint
   * equal stamps in the same millisecond (both start at sequence zero) and the
   * equal-version guards then skip the update in both directions, so the peers
   * never converge until the next millisecond or the TTL. A growing add —
   * roughly a coin flip beyond ~38 instances — means explicit node ids
   * ({@code zeta.local.node-id}) should be assigned.
   */
  private final LongAdder nodeIdCollisions = new LongAdder();

  /** Strategy that decides how REFRESH, INVALIDATE, PUT and the clear family are applied. */
  private final SyncPolicy policy;

  /** Framework-owned guarded write path passed to the policy. */
  private final SyncContext context;

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
   * anti-drift guard refuses it, in which case the mutation carries a fresh local stamp
   * instead (design decisions: version stamps); a {@code null} clock skips
   * observation.
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
   * @param context   framework-owned guarded write path handed to the policy
   * @param appName   this application's {@code zeta.app-name}; {@code null}/blank disables
   *                  foreign-app filtering
   * @param stampClock the node's data-stamp clock, or {@code null} to skip observation
   */
  public CacheSyncListener(
    CacheSyncProperties properties,
    ScheduledExecutorService scheduler,
    SyncPolicy policy,
    SyncContext context,
    String appName,
    @Nullable StampClock stampClock
  ) {
    this.stampClock = stampClock;
    this.policy = policy;
    this.context = context;
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
   * <p>Queued sync tasks (and jitter-delayed submissions still pending on the
   * shared scheduler) are dropped without execution: the shutdown window is
   * intentionally lossy. A lost INVALIDATE/REFRESH heals through the next
   * application-level write or the entry's hard TTL (design decisions: sync messages), so
   * a drain is not worth blocking context shutdown for.
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
   * @return the total foreign-app drop add since startup
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
   * RabbitMQ message callback for incoming sync messages. Parses the message and hands the
   * cache mutation to the ordered dispatcher, then acknowledges the message; an accepted
   * mutation runs after a random start-jitter park on the dispatcher, while a refused
   * submission falls back inline before the ack (see {@link #onDispatchRejected}).
   *
   * <p><b>Self-message filtering (design decisions: dedup lifecycle):</b> the fanout exchange delivers the
   * sender's own broadcasts back to its per-instance queue. A self-addressed
   * REFRESH/PUT is dropped before parsing: the sender's L1 entry at the broadcast
   * version is definitionally current (putThrough stamps it with the exact
   * post-bump version), so re-processing it could only discard the sender's
   * fresh write through the no-value fallback or pay a redundant cluster-loader
   * fetch. A self-addressed clear (INVALIDATE_ALL / INVALIDATE_PREFIX /
   * INVALIDATE_BATCH) is dropped for the same reason: the sender recorded the
   * watermark and swept its L1 before broadcasting, so the loopback would only
   * repeat a full-table scan. A plain self-INVALIDATE is still processed: it
   * is O(1) and heals invalidate-vs-reload repopulations stamped with older
   * versions. Messages without an origin header (older senders, rolling
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
  public void onMessage(Channel channel, Message msg) throws IOException {
    AmqpConsume.ackOrNack(channel, msg, log, "CacheSync", () -> {
      // Ack, not nack: a foreign message is valid, it just is not ours. Nacking
      // would requeue it into a poison loop.
      if (isolation.dropIfForeign(msg, log, "sync message")) {
        return;
      }
      // One header read and one origin read for both the self-filter and the collision
      // diagnostic; the parsed type travels into process so it is converted once.
      MessageProperties props = msg.getMessageProperties();
      Object typeHeader = props.getHeader(HEADER_TYPE);
      SyncType type = SyncType.parse(typeHeader);
      if (type == null) {
        // Validate before the origin/collision work and before the body decoder: an
        // unknown type has no operation to apply and must not produce an "empty body"
        // line after a successful header read.
        AmqpMessageReader.logUnknownType(log, "sync message", typeHeader, msg);
        return;
      }
      String origin = AmqpMessageReader.readStringHeader(props, HEADER_ORIGIN_INSTANCE);
      boolean own = origin != null && NodeIdentity.getInstanceId().equals(origin);
      if (own && type.ignoresSelf()) {
        // The sender's L1 already holds the value / watermark this broadcast carries;
        // replaying it would only re-read or re-sweep what was just written.
        return;
      }
      if (!own) {
        detectNodeIdCollision(props, origin);
      }
      process(msg, type);
    });
  }

  /**
   * Cumulative add of inbound sync messages from a peer instance sharing this
   * node's {@code dataVersion} node id. Monotonic; safe to read from any thread.
   *
   * @return the total node-id collision add since startup
   */
  public long nodeIdCollisions() {
    return nodeIdCollisions.sum();
  }

  /**
   * Warn once when a peer instance broadcasts under this node's 10-bit
   * {@code dataVersion} node id. Equal node ids can mint equal stamps in the
   * same millisecond (both start at sequence zero); the equal-version guards
   * then skip the update in both directions, so the peers never converge.
   * The caller admits only messages that are not this instance's own broadcast.
   *
   * <p>Absent headers (older senders) are ignored: the check is diagnostic,
   * never a reason to drop a message.
   *
   * @param props  the sender's message properties (already read by the caller)
   * @param origin the already-read origin-instance header, used only for the warning
   */
  private void detectNodeIdCollision(MessageProperties props, @Nullable String origin) {
    long peer = AmqpMessageReader.readLongHeader(props, HEADER_ORIGIN_NODE_ID, -1L);
    if (peer != NodeIdentity.getNodeId()) {
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
   * {@link CacheSyncProperties#getJitterMs()} to the key's worker before its first
   * cycle, spreading cluster-loader reads when multiple peers process the same sync send
   * simultaneously.
   *
   * <p>Messages are submitted as their decoded {@link SyncMessage}: the dispatcher hands the
   * whole granted batch of a key to the listener at once, which reduces it (one loader call
   * for a REFRESH burst, one sweep for a clear burst, one apply for a chain of superseded
   * PUTs) before calling the typed handler method.
   *
   * @param msg  the raw AMQP message; if the body is null, empty, or cannot be
   *             parsed into a valid {@link SyncMessage}, the message is dropped
   *             without scheduling any task
   * @param type the type parsed and validated at the listener boundary
   */
  private void process(Message msg, SyncType type) {
    // A batched INVALIDATE (design decisions: invalidation) is a wire frame: it decomposes
    // into per-key tasks at the boundary, so every entry keeps its own ordered queue and
    // still orders against that key's PUT/REFRESH messages exactly as a single-key
    // INVALIDATE would. The entries are already the INVALIDATE tasks to submit, so only a
    // refused stamp needs a new message. Each decomposed entry captures only key + version
    // (no payload), so its budget weight stays at the 1 floor; the batch body's byte length
    // is not attributable to any single entry. A refused frame's entries share one local
    // stamp: each targets a distinct key, so the per-key guards never compare them against
    // each other.
    if (type == SyncType.INVALIDATE_BATCH) {
      SyncMessage.ParsedBatch batch = SyncMessage.parseBatch(msg);
      if (batch == null) {
        log.debug("Received an unusable INVALIDATE_BATCH frame");
        return;
      }

      long localStamp = observe(batch.maxVersion());
      for (SyncMessage entry : batch.entries()) {
        submit(restamped(entry, localStamp), 0);
      }
      return;
    }

    SyncMessage message = from(msg, type);
    if (message == null) {
      // Includes the unorderable (non-positive version) case, rejected at the parse
      // boundary before any clock merge or dispatcher admission.
      log.debug("Received sync message with an empty body, an invalid key or a non-positive data version");
      return;
    }

    // Every type now dispatches under its real key: a PUT's key comes from the
    // HEADER_CACHE_KEY header (decoded into cacheKey() at parse time), and both clear types
    // share the dedicated clear channel (see {@link #CLEAR_DISPATCH_KEY}). Note this does NOT
    // serialize the sweep against per-key work on the same namespace — the clear channel and
    // the full-key queues are independent; anti-resurrection is carried entirely by
    // EntryLifecycle's clear watermarks, not by dispatcher ordering.
    submit(restamped(message, observe(message.version())), msg.getBody().length);
  }

  /**
   * Merge one peer stamp into the local clock before any guard runs (design decisions:
   * version stamps decision 7). Observing only applied messages is too late — a message
   * rejected as stale still proves the peer's clock is ahead, and without the merge this
   * node's later writes stay numerically below it forever. A refused merge gets one fresh
   * LOCAL stamp for the mutation: persisting the over-drift peer version as a dataVersion
   * would blind every later local write to the key until wall time caught up with the
   * poisoned stamp.
   *
   * @param version the peer stamp (a batch's highest entry/header stamp)
   * @return {@code 0} when the peer stamp was accepted, otherwise one fresh local stamp
   */
  private long observe(long version) {
    return (stampClock == null || stampClock.observe(version)) ? 0L : stampClock.nextStamp();
  }

  /** The message at the local replacement stamp, or unchanged when the peer stamp was accepted. */
  private static SyncMessage restamped(SyncMessage message, long localStamp) {
    return localStamp > 0L ? message.withVersion(localStamp) : message;
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
    // Only a REFRESH touches the cluster loader; every other type is a pure in-memory
    // apply, so it skips the anti-herd park and starts immediately (FIFO order is
    // preserved either way — a worker already parked for the key runs first). Clears are
    // O(1) control signals: a value storm must not lock them out.
    boolean accepted = message.type().isClear()
      ? dispatcher.submitControl(CLEAR_DISPATCH_KEY, message)
      : dispatcher.submit(message.cacheKey(), message, 1 + (bodyBytes >> BYTE_WEIGHT), message.type().needsJitter());
    if (!accepted) {
      onDispatchRejected(message);
    }
  }

  /**
   * Runs one granted batch: every queued message of one key (or of the clear channel) is
   * reduced to its effective message, covered value messages are reported, and the typed
   * handler method runs the guarded apply.
   *
   * @param messages the granted batch, in submission order
   */
  private void dispatchBatch(List<SyncMessage> messages) {
    try {
      Batch batch = Batch.reduce(messages);
      if (batch instanceof Batch.Value value) {
        for (SyncMessage covered : value.covered()) {
          context.skip(covered);
        }

        if (value.winner().type() == SyncType.INVALIDATE) {
          policy.invalidate(value, context);
        } else {
          policy.value(value, context);
        }
        return;
      }
      policy.clear((Batch.Clear) batch, context);
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
   * Fallback for a message the ordered dispatcher refused at submit time (per-key queue
   * full, global budget exhausted, or the listener closing). A refused task is never
   * retried, so dropping it would leave a stale L1 entry for its whole hard TTL: a clear
   * is applied as a local sweep, anything else as an O(1) in-memory invalidation — neither
   * touches the cluster loader, so the consumer thread cannot stall.
   *
   * @param message the decoded message that was refused
   */
  private void onDispatchRejected(SyncMessage message) {
    try {
      SyncMessage fallback = message.asInvalidate();
      if (fallback.type().isClear()) {
        context.clear(new Batch.Clear(List.of(fallback)));
      } else {
        context.invalidate(fallback);
      }
    } catch (Exception e) {
      log.warn("Sync drop fallback failed: type={}, key={}", message.type(), message.cacheKey(), e);
    }
  }
}
