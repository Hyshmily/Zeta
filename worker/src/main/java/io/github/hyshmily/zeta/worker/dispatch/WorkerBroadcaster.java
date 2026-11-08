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
package io.github.hyshmily.zeta.worker.dispatch;

import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.*;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Routing.PREFIX_DECISION;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.sync.worker.WorkerDecisionType;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.worker.dispatch.connection.WorkerIdentity;
import io.github.hyshmily.zeta.worker.metrics.WorkerDetectionMetrics;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * Publishes HOT and COOL decisions to all application instances via the
 * configured RabbitMQ {@code broadcastExchange}.
 *
 * <p>Each send message carries a monotonically increasing
 * <b>decision version</b> (worker‑local counter) that is used by receivers to
 * discard stale or out‑of‑order messages. The version of a decision is
 * allocated <em>upstream</em>, under the per-key lock that performed
 * the transition — see {@link #tryBroadcast(ZetaDecision, boolean, long)} — so the order the versions
 * express is causal, not the order in which threads happened to reach the wire.
 *
 * <p>Messages reach every instance of this application's dedicated queue through a
 * <b>direct</b> exchange ({@code zeta.send.exchange}) with the routing key
 * {@code send.<appName>} — every queue bound under that key receives a copy, so the fan-out
 * stays inside one application even when several share a broker. The receiver still
 * differentiates the message type via the {@code HEADER_TYPE} header and re-checks the
 * {@code appName} header at its own boundary.
 *
 * <p><b>Ordering state outlives every decision that can still travel, and a hot state outlives
 * every age bound.</b> A cold accepted record is retained for twice
 * {@code broadcast.max-decision-age-ms} and the cache has no size cap, so it expires only after its
 * key's oldest queued decision has already been dropped as expired. A record whose last accepted
 * decision is HOT is retained until a COOL is accepted: it is the only proof the cluster was told
 * the key is hot, and a hot key can keep its seat for far longer than any queue bound. The cold
 * retention assumes a producer submits a decision within {@code max-decision-age-ms} of the
 * transition that allocated its version; a producer stalled longer than twice the age bound is
 * outside the report path's latency contract and is a bounded loss, like a stalled window add.
 *
 * <p>This class is the <b>single funnel</b> for decisions leaving the Worker. Only the dispatcher's
 * drain thread calls the package-private classification and send methods, so the mutable
 * {@link AcceptedDecision} fields need no cross-thread hand-off.
 *
 * <p><b>One send per decision.</b> The pipeline is fire-and-forget (design decisions: sync
 * messages): no repeat chain, no periodic re-emission. A lost decision is not retried; the cluster
 * converges through its coarse backstops (the App's hot hard TTL, the local TopK taking TTL
 * authority while the Worker is unhealthy). The accepted-decision record exists for ordering and
 * duplicate suppression, not for delivery. A {@link Delivery#SENT} means the AMQP client accepted
 * the publish call; no publisher confirm is requested, so it does not prove broker receipt.
 */
@Slf4j
public class WorkerBroadcaster {

  /**
   * What happened to one decision at the funnel.
   *
   * <p>The dispatcher consumes a token only for {@link #SENT}; an in-memory suppression costs no
   * network budget. {@link #FAILED} has already been counted as a dropped decision here.
   */
  enum Delivery {
    /**
     * Handed to the AMQP client without an exception. No publisher confirm is requested, so this is
     * not a broker acknowledgement.
     */
    SENT,
    /**
     * Suppressed in memory: the cluster's state would not change, or the exact version was already
     * accepted. The caller must not return a HOT seat for this outcome.
     */
    ELIDED,
    /**
     * Dropped because a <em>newer</em> version for the key was already accepted. The transition
     * that produced that newer version owns the key's current seat, so a HOT rollback is
     * identity-guarded and cannot undo it.
     */
    SUPERSEDED,
    /**
     * The queue-residency bound passed after the in-memory state machine proved a send was required.
     * The decision never touched the wire; the dispatcher counts it as expired and returns a HOT's
     * seat. A decision the state machine would have elided or dropped as superseded is classified as
     * that outcome instead, because its age cannot change the cluster's state.
     */
    EXPIRED,
    /** The publish threw; the drop is counted. */
    FAILED,
  }

  /** Length of the same-type debounce window, anchored at the last <em>sent</em> decision. */
  private static final long DEDUP_WINDOW_MS = 100L;

  /**
   * Retention of a record whose last accepted decision is HOT. The record is the only evidence the
   * cluster was told the key is hot, and a hot key can keep its seat indefinitely, so the record
   * must outlive every state the cluster can still hold; one hundred years is a bound no process
   * reaches while staying clear of duration arithmetic.
   */
  private static final long HOT_RECORD_RETENTION_NANOS = TimeUnit.DAYS.toNanos(36_500L);

  /**
   * Per-key accepted-decision record. The mutable fields are written by the single drain thread;
   * the Caffeine cache supplies time-based retention only, never size eviction.
   */
  private static final class AcceptedDecision {

    /**
     * Debounce anchor for a record established by an elision: no decision has ever been sent, so
     * the 100 ms same-type window must not be measured from a synthetic zero timestamp.
     */
    private static final long NEVER_SENT = Long.MIN_VALUE;

    /** Highest accepted decision version; {@code 0} means no decision has been accepted yet. */
    private long version;

    /** Whether {@link #version} is the state the cluster holds, even if it was elided. */
    private boolean hot;

    /** Monotonic send instant of the last real send; {@link #NEVER_SENT} until one occurs. */
    private final long lastSentAtMs;

    private AcceptedDecision(ZetaDecision decision, long sentAtMs) {
      this.version = decision.decisionVersion();
      this.hot = decision.type() == ZetaDecision.DecisionType.HOT;
      this.lastSentAtMs = sentAtMs;
    }

    long version() {
      return version;
    }

    boolean isHot() {
      return hot;
    }

    /**
     * Whether this record is the same type and was sent inside the debounce window.
     *
     * <p>A record established by an elision has no send anchor: it can suppress a redundant COOL
     * through {@link #isHot()} being false, but it must not suppress a same-type decision as if a
     * send had just happened. The sentinel check keeps the arithmetic away from
     * {@link #NEVER_SENT}, whose subtraction would overflow.
     *
     * @param hot   the type of the decision being considered
     * @param nowMs the monotonic sample of that decision
     * @return {@code true} when the decision is a duplicate of the last sent one
     */
    boolean sameTypeWithinDedupWindow(boolean hot, long nowMs) {
      if (this.hot != hot || lastSentAtMs == NEVER_SENT) {
        return false;
      }
      long elapsed = nowMs - lastSentAtMs;
      return elapsed >= 0L && elapsed < DEDUP_WINDOW_MS;
    }

    /** Advances the accepted version and type without moving the debounce anchor. */
    void elide(ZetaDecision decision) {
      this.version = decision.decisionVersion();
      this.hot = decision.type() == ZetaDecision.DecisionType.HOT;
    }
  }

  /**
   * Caffeine variable expiry: a record that represents HOT is retained for the lifetime of the
   * Worker's hot state, while a cold record keeps the queue-residency bound.
   *
   * <p>A hot record is the only proof the cluster was told the key is hot. Its age says nothing
   * about that fact: a key can stay hot for minutes or hours without another decision. Expiring it
   * would let a later COOL that displaced a still-pending HOT be mistaken for the demotion of a
   * promotion the cluster never saw, and the cluster would stay hot until the App's hard TTL. The
   * record turns cold when a COOL is accepted, and the cold retention then bounds how long an older
   * queued decision can still be ordered against it.
   */
  private record AcceptedDecisionExpiry(long coldRetentionNanos) implements Expiry<String, AcceptedDecision> {
    @Override
    public long expireAfterCreate(@NonNull String key, @NonNull AcceptedDecision value, long currentTime) {
      return retentionNanos(value);
    }

    @Override
    public long expireAfterUpdate(
      @NonNull String key,
      @NonNull AcceptedDecision value,
      long currentTime,
      long currentDuration
    ) {
      return retentionNanos(value);
    }

    @Override
    public long expireAfterRead(
      @NonNull String key,
      @NonNull AcceptedDecision value,
      long currentTime,
      long currentDuration
    ) {
      return currentDuration;
    }

    private long retentionNanos(AcceptedDecision value) {
      return value.isHot() ? Math.max(HOT_RECORD_RETENTION_NANOS, coldRetentionNanos) : coldRetentionNanos;
    }
  }

  /**
   * A terminal in-memory classification together with the accepted record it was decided against.
   * Carrying the record from {@link #classifyInMemory} to {@link #commitInMemory} keeps one cache
   * read for the whole head-drain step: a second read could observe an expiry the first did not and
   * regress the accepted version to an older decision.
   */
  record InMemory(Delivery outcome, @Nullable AcceptedDecision prev) {}

  /**
   * Per-key accepted decision, retention state-dependent but never size-bounded. A cold record is
   * retained for twice the maximum queue residency, so it expires only after every decision that
   * could still be compared against it has already been dropped as expired; a hot record is
   * retained until a COOL is accepted.
   */
  private final Cache<String, AcceptedDecision> acceptedDecisions;

  /**
   * Detection-plane meters (documented in docs/MONITOR.md). A meter-less instance is a no-op, so
   * the send funnel never branches on whether metrics are wired.
   */
  private final WorkerDetectionMetrics metrics;

  /** RabbitMQ template for HOT/COOL decisions and heartbeats. */
  private final RabbitTemplate rabbitTemplate;

  /** Target exchange name for all send messages (typically {@code zeta.send.exchange}). */
  private final String broadcastExchange;

  /**
   * This Worker incarnation's node identity and epoch, stamped on every send header. The epoch
   * lets receivers detect a Worker restart; the node id scopes cross-Worker version tracking in
   * {@code VersionGuard}.
   */
  private final WorkerIdentity identity;

  /** Precomputed {@code send.<appName>}; the value is fixed for the lifetime of the bean. */
  private final String routingKey;

  /** Application name used in the send routing key and header. */
  private final String appName;

  /**
   * Constructs a broadcaster that sends each decision once.
   *
   * @param rabbitTemplate    the template that publishes messages
   * @param broadcastExchange the direct decision exchange name
   * @param appName           the application name carried in the send header
   * @param identity          this process's immutable node id and epoch
   * @param metrics           the emission counters (a meter-less instance is a no-op)
   * @param maxDecisionAgeMs  the queue-residency bound whose double sizes the cold-record retention
   */
  public WorkerBroadcaster(
    RabbitTemplate rabbitTemplate,
    String broadcastExchange,
    String appName,
    WorkerIdentity identity,
    WorkerDetectionMetrics metrics,
    long maxDecisionAgeMs
  ) {
    this.appName = appName;
    this.metrics = metrics;
    this.identity = identity;
    this.rabbitTemplate = rabbitTemplate;
    this.broadcastExchange = broadcastExchange;
    this.routingKey = PREFIX_DECISION + appName;
    long retentionMs = maxDecisionAgeMs > Long.MAX_VALUE / 2L ? Long.MAX_VALUE : 2L * maxDecisionAgeMs;
    this.acceptedDecisions = Caffeine.newBuilder()
      .expireAfter(new AcceptedDecisionExpiry(retentionNanos(retentionMs)))
      .build();
  }

  /** Converts a millisecond retention to nanoseconds, saturating instead of wrapping. */
  private static long retentionNanos(long retentionMs) {
    if (retentionMs <= 0L) {
      return 0L;
    }
    return retentionMs > Long.MAX_VALUE / 1_000_000L ? Long.MAX_VALUE : retentionMs * 1_000_000L;
  }

  /**
   * The highest decision version the funnel has accepted for the key, or {@code 0} when the key
   * has no live record.
   *
   * <p>Used by a pre-funnel rollback that has no funnel outcome of its own (the shutdown drain and
   * an unexpected round failure), where it decides whether a HOT seat is safe to return.
   *
   * @param cacheKey the key the decision is about
   * @return the accepted version, or {@code 0} when the key has no live record
   */
  long acceptedVersion(String cacheKey) {
    AcceptedDecision prev = acceptedDecisions.getIfPresent(cacheKey);
    return prev == null ? 0L : prev.version();
  }

  /**
   * Broadcasts one decision, honouring its causal version, in <b>one</b> accepted-decision lookup.
   *
   * <p>A decision at or below the last version accepted for the key describes a state the cluster
   * has already moved past: it is suppressed in memory, and the caller sees
   * {@link Delivery#ELIDED} for an equal version or {@link Delivery#SUPERSEDED} for a strictly
   * lower one, never a send failure.
   *
   * <p>The displacement flag is a parameter because the accepted record alone cannot recover it: an
   * empty record plus a COOL that displaced a HOT not yet accepted by the funnel — still queued, or
   * a registry promotion never submitted — proves that HOT never left the dispatcher, so the
   * demotion would change nothing.
   *
   * <p>The age bound is applied by {@link #classify} only after the in-memory state machine proves a
   * send is required: a decision whose age cannot change the cluster's state is elided or dropped as
   * superseded, never misreported as expired, and an expired decision proven sendable has no accepted
   * record at or above its version.
   *
   * <p>The caller guarantees a non-{@code NONE} decision with a positive causal version:
   * {@link DecisionDispatcher#submit} is the single boundary that rejects those, so this funnel
   * does not repeat the check.
   *
   * @param decision           the decision to publish
   * @param replacedPendingHot {@code true} when this COOL displaced a HOT that had not reached the
   *                           funnel, so an empty record proves that HOT never travelled
   * @param expiresAtMs        the first monotonic instant at which queue residency has expired, or
   *                           a value no sample reaches for an unbounded caller
   * @return the outcome of the funnel
   */
  Delivery tryBroadcast(ZetaDecision decision, boolean replacedPendingHot, long expiresAtMs) {
    String key = decision.cacheKey();
    long sentAtMs = TimeSource.monotonicMillis();
    AcceptedDecision prev = acceptedDecisions.getIfPresent(key);
    Delivery inMemory = classify(sentAtMs, decision, replacedPendingHot, expiresAtMs, prev);
    return inMemory != null ? applyInMemory(inMemory, prev, decision) : send(decision, key, sentAtMs);
  }

  /**
   * Sends one decision and records its outcome: on success the accepted record advances; on a
   * publish failure the drop is counted and the caller's HOT seat is returned.
   *
   * @param decision the decision to publish
   * @param key      the decision's cache key
   * @param sentAtMs the monotonic send instant
   * @return {@link Delivery#SENT} or {@link Delivery#FAILED}
   */
  private Delivery send(ZetaDecision decision, String key, long sentAtMs) {
    ZetaDecision.DecisionType type = decision.type();
    long version = decision.decisionVersion();
    String typeCode = messageType(type);
    try {
      sendBroadcast(key, typeCode, version);
      metrics.countSentDecision(type);
      acceptedDecisions.put(key, new AcceptedDecision(decision, sentAtMs));
      if (log.isDebugEnabled()) {
        log.debug("Broadcast {}: key={}, dv={}", typeCode, key, version);
      }
      return Delivery.SENT;
    } catch (Exception e) {
      metrics.countDroppedDecision(type);
      if (log.isDebugEnabled()) {
        log.debug("Failed to broadcast {} decision for key={}: {}", typeCode, key, e.toString());
      }
      // no throw — design decisions: sync messages fire-and-forget
      return Delivery.FAILED;
    }
  }

  /**
   * Classifies a token-starved queue head without sending and <b>without</b> mutating the accepted
   * record or the meters. The dispatcher calls this before it owns the item, then calls
   * {@link #commitInMemory} only after the queue removal and ownership take succeeded, so a
   * shutdown withdrawal cannot leave the funnel claiming a decision the submitter dropped.
   *
   * <p>The age bound is applied inside {@link #classify}: only a decision that would otherwise be
   * sent can be expired.
   *
   * @param decision           the decision to classify
   * @param replacedPendingHot {@code true} when this COOL displaced a HOT that had not reached the
   *                           funnel, so an empty accepted record proves that HOT never travelled
   * @param expiresAtMs        the first monotonic instant at which queue residency has expired, or
   *                           a value no sample reaches for an unbounded caller
   * @return the terminal classification, or {@code null} when the decision must be sent
   */
  @Nullable
  InMemory classifyInMemory(ZetaDecision decision, boolean replacedPendingHot, long expiresAtMs) {
    long nowMs = TimeSource.monotonicMillis();
    AcceptedDecision prev = acceptedDecisions.getIfPresent(decision.cacheKey());
    Delivery outcome = classify(nowMs, decision, replacedPendingHot, expiresAtMs, prev);
    return outcome != null ? new InMemory(outcome, prev) : null;
  }

  /**
   * Applies a terminal in-memory classification after the dispatcher owns the item: advances the
   * accepted record for an elision and charges the outcome's meter. An elision with no prior record
   * creates the cold record, so a delayed older decision is still ordered against the version that
   * ended the lifecycle. The write-expiry anchor is refreshed so an elided decision keeps its key's
   * record alive for the same window a send would.
   *
   * @param classification the classification and the record it was decided against
   * @param decision       the classified decision
   * @return the classification's outcome
   */
  Delivery commitInMemory(InMemory classification, ZetaDecision decision) {
    return applyInMemory(classification.outcome(), classification.prev(), decision);
  }

  /**
   * Applies one terminal in-memory outcome: advances the accepted record for an elision and charges
   * the outcome's meter. The send path and the token-starved head share this tail, so both treat the
   * same classification identically.
   *
   * @param outcome  the terminal outcome
   * @param prev     the accepted record it was decided against, or {@code null}
   * @param decision the classified decision
   * @return the outcome, for the caller to return
   */
  private Delivery applyInMemory(Delivery outcome, @Nullable AcceptedDecision prev, ZetaDecision decision) {
    switch (outcome) {
      case EXPIRED -> {
      }
      case ELIDED -> {
        acceptedDecisions.put(decision.cacheKey(), elided(prev, decision));
        metrics.countElidedDecision(decision.type());
        if (log.isDebugEnabled()) {
          log.debug(
            "Skipping elided {}: key={}, dv={}",
            messageType(decision.type()),
            decision.cacheKey(),
            decision.decisionVersion()
          );
        }
      }
      case SUPERSEDED -> {
        metrics.countSupersededDecision(decision.type());
        if (log.isDebugEnabled()) {
          log.debug(
            "Skipping superseded {}: key={}, dv={}, latest={}",
            messageType(decision.type()),
            decision.cacheKey(),
            decision.decisionVersion(),
            prev != null ? prev.version() : 0L
          );
        }
      }
      default -> throw new IllegalArgumentException("Unexpected in-memory outcome: " + outcome);
    }
    return outcome;
  }

  /**
   * The single in-memory state machine shared by the send path and the token-starved head.
   *
   * <p>The state machine decides before the age bound: a decision whose age cannot change the
   * cluster's state is elided or dropped as superseded, never misreported as expired, and only a
   * decision that would otherwise be sent can be expired.
   *
   * @param nowMs              the monotonic sample
   * @param decision           the decision being classified
   * @param replacedPendingHot whether an empty record proves the demotion is a no-op
   * @param expiresAtMs        the first monotonic instant at which queue residency has expired
   * @param prev               its accepted record, or {@code null}
   * @return the terminal outcome, or {@code null} when the decision must be sent
   */
  @Nullable
  private static Delivery classify(
    long nowMs,
    ZetaDecision decision,
    boolean replacedPendingHot,
    long expiresAtMs,
    @Nullable AcceptedDecision prev
  ) {
    if (prev == null) {
      if (replacedPendingHot) {
        return Delivery.ELIDED;
      }
      return nowMs >= expiresAtMs ? Delivery.EXPIRED : null;
    }

    long prevVersion = prev.version();
    long dv = decision.decisionVersion();
    if (prevVersion >= dv) {
      // The exact version is already the state this Worker holds, whatever its type. A replay is
      // not an undelivered transition: its caller must not return the seat. A strictly newer
      // decision was accepted while this one waited; the transition that produced it owns the
      // key's seat, so the caller's rollback is a no-op.
      return prevVersion == dv ? Delivery.ELIDED : Delivery.SUPERSEDED;
    }

    boolean hot = decision.type() == ZetaDecision.DecisionType.HOT;
    if (!hot && !prev.isHot()) {
      // The last accepted decision for this key is already a COOL and no HOT has been accepted
      // since, so every App is cold and this demotion would change nothing.
      return Delivery.ELIDED;
    }
    // Same type inside the debounce window is the identical cluster state; a type change travels.
    if (prev.sameTypeWithinDedupWindow(hot, nowMs)) {
      return Delivery.ELIDED;
    }
    return nowMs >= expiresAtMs ? Delivery.EXPIRED : null;
  }

  /**
   * Advances the accepted record for one elided decision.
   *
   * <p>A record elided with no predecessor is itself the proof of the state the Worker
   * holds: the displaced HOT never travelled, so the COOL that replaced it establishes the cold
   * state even though no send occurred. Creating that record is what lets a later stale decision be
   * classified against the version that actually ended the lifecycle. Its debounce anchor stays at
   * {@link AcceptedDecision#NEVER_SENT}, because no send happened.
   *
   * <p>An existing record advances only when the elided decision carries a higher version; a replay
   * of the accepted version leaves it unchanged. The send instant is kept, so the debounce window
   * stays anchored at the last real send.
   *
   * @param prev     the accepted record, or {@code null}
   * @param decision the elided decision
   * @return the record to retain
   */
  private static AcceptedDecision elided(@Nullable AcceptedDecision prev, ZetaDecision decision) {
    if (prev == null) {
      return new AcceptedDecision(decision, AcceptedDecision.NEVER_SENT);
    }
    if (prev.version() < decision.decisionVersion()) {
      prev.elide(decision);
    }
    return prev;
  }

  private static String messageType(ZetaDecision.DecisionType type) {
    return type == ZetaDecision.DecisionType.HOT ? WorkerDecisionType.HOT.code() : WorkerDecisionType.COOL.code();
  }

  /**
   * Common send helper for HOT/COOL decisions.
   *
   * @param cacheKey the key the decision is about
   * @param type     the message type ({@link WorkerDecisionType#HOT} or
   *                 {@link WorkerDecisionType#COOL})
   * @param version  the monotonically increasing decision version for ordering
   */
  private void sendBroadcast(String cacheKey, String type, long version) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, type);
    props.setHeader(HEADER_VERSION, version);
    props.setHeader(HEADER_NODE_ID, identity.nodeId());
    props.setHeader(HEADER_EPOCH, identity.epoch());
    // design decisions: sync messages — the routing key ({@code send.<appName>}) is what keeps
    // decisions inside one application on a shared broker; the header repeats the address for
    // the receiver's own boundary check.
    props.setHeader(HEADER_APP_NAME, appName);
    // Trace-only ID (no plane compares it for ordering): the cached TimeSource
    // read keeps it time-sortable without contending the data-version clock or
    // burning a sequence number of the shared 4096/ms stamp window.
    props.setHeader(HEADER_MESSAGE_ID, TimeSource.currentTimeMillis());

    Message msg = new Message(cacheKey.getBytes(StandardCharsets.UTF_8), props);
    rabbitTemplate.send(broadcastExchange, routingKey, msg);
  }
}
