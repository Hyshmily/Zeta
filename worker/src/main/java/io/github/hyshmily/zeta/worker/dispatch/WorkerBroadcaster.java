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
import io.github.hyshmily.zeta.sync.worker.WorkerType;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.worker.dispatch.connection.WorkerIdentity;
import io.github.hyshmily.zeta.worker.metrics.WorkerDetectionMetrics;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
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
 * the transition — see {@link #send(ZetaDecision, long)} — so the order the versions
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
 * decision is HOT is retained until a COOL is accepted: it keeps the version and debounce
 * watermarks across a hot episode that can outlive any queue bound. A COOL with no record is still
 * sent — the cluster may be hot from local promotion or from an earlier lifecycle whose COOL was
 * lost — so the hot retention is an ordering and duplicate-suppression optimization, not the only
 * demotion signal. The cold retention assumes a producer submits a decision within
 * {@code max-decision-age-ms} of the transition that allocated its version; a producer stalled
 * longer than twice the age bound is outside the report path's latency contract and is a bounded
 * loss, like a stalled window add.
 *
 * <p>This class is the <b>single funnel</b> for decisions leaving the Worker. Only the dispatcher's
 * drain thread calls the package-private classification and send methods, so a record replacement
 * needs no cross-thread hand-off; {@link AcceptedDecision} values are immutable.
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
   * Per-key accepted-decision record. Every field is immutable so a record read through the cache
   * is a stable fact; the single drain thread replaces the value on an elision or a state change.
   * The Caffeine cache supplies time-based retention only, never size eviction.
   */
  private static final class AcceptedDecision {

    /**
     * Debounce anchor for a record established by an elision: no decision has ever been sent, so
     * the 100 ms same-type window must not be measured from a synthetic zero timestamp.
     */
    private static final long NEVER_SENT = Long.MIN_VALUE;

    /** What the cluster is known to hold for this key. */
    private enum State {
      HOT,
      COLD,
      /**
       * A cold record whose coldness is no longer proven: a HOT that could have corrected it ended
       * before the wire. Keeping the version still supersedes older decisions; the state only
       * stops a later COOL from being elided as redundant.
       */
      UNKNOWN,
    }

    /** Highest accepted decision version; {@code 0} means no decision has been accepted yet. */
    private final long version;

    /** The cluster state this version describes, or {@link State#UNKNOWN} after a failed HOT. */
    private final State state;

    /** Monotonic send instant of the last real send; {@link #NEVER_SENT} until one occurs. */
    private final long lastSentAtMs;

    private AcceptedDecision(ZetaDecision decision, long sentAtMs) {
      this(decision.decisionVersion(), stateOf(decision), sentAtMs);
    }

    private AcceptedDecision(long version, State state, long lastSentAtMs) {
      this.version = version;
      this.state = state;
      this.lastSentAtMs = lastSentAtMs;
    }

    /**
     * The cold state recorded when a HOT is dropped before the wire and no record exists. Version
     * {@code 0} is below every causal decision version and the never-sent anchor keeps it from
     * debouncing a real send.
     *
     * @return a cold accepted record with no send anchor
     */
    private static AcceptedDecision coldTombstone() {
      return new AcceptedDecision(0L, State.COLD, NEVER_SENT);
    }

    private static State stateOf(ZetaDecision decision) {
      return decision.type() == ZetaDecision.DecisionType.HOT ? State.HOT : State.COLD;
    }

    long version() {
      return version;
    }

    boolean isHot() {
      return state == State.HOT;
    }

    /**
     * Whether the cluster is known to be cold at {@link #version}.
     *
     * @return {@code true} only for a COLD record
     */
    boolean isKnownCold() {
      return state == State.COLD;
    }

    /**
     * Whether this record is the same type and was sent inside the debounce window.
     *
     * <p>A record established by an elision has no send anchor: it can suppress a redundant COOL
     * through {@link #isKnownCold()}, but it must not suppress a same-type decision as if a send
     * had just happened. The sentinel check keeps the arithmetic away from {@link #NEVER_SENT},
     * whose subtraction would overflow.
     *
     * @param hot   the type of the decision being considered
     * @param nowMs the monotonic sample of that decision
     * @return {@code true} when the decision is a duplicate of the last sent one
     */
    boolean debounced(boolean hot, long nowMs) {
      State target = hot ? State.HOT : State.COLD;
      if (state != target || lastSentAtMs == NEVER_SENT) {
        return false;
      }
      long elapsed = nowMs - lastSentAtMs;
      return elapsed >= 0L && elapsed < DEDUP_WINDOW_MS;
    }

    /** Advances the accepted version and type without moving the debounce anchor. */
    AcceptedDecision elide(ZetaDecision decision) {
      return new AcceptedDecision(decision.decisionVersion(), stateOf(decision), lastSentAtMs);
    }

    /**
     * Clears only the cold knowledge, keeping the version watermark and the send anchor.
     *
     * @return this record when it is not cold; otherwise an {@link State#UNKNOWN} copy
     */
    AcceptedDecision clearColdKnowledge() {
      return state == State.COLD ? new AcceptedDecision(version, State.UNKNOWN, lastSentAtMs) : this;
    }
  }

  /**
   * Caffeine variable expiry: a record that represents HOT is retained for the lifetime of the
   * Worker's hot state, while a cold record keeps the queue-residency bound.
   *
   * <p>A hot record is the ordering and duplicate-suppression watermark for a key that can stay
   * hot for minutes or hours without another decision. Expiring it would let a stale HOT or an old
   * COOL be re-sent after the key's queue decisions have settled; the App's version guard makes the
   * replay harmless, but the send is wasted. The record turns cold when a COOL is accepted, and the
   * cold retention then bounds how long an older queued decision can still be ordered against it.
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
   * Carrying the record from {@link #classify} to {@link #commit} keeps one cache
   * read for the whole head-drain step: a second read could observe an expiry the first did not and
   * regress the accepted version to an older decision.
   */
  record Verdict(Delivery outcome, @Nullable AcceptedDecision prev) {}

  /**
   * Per-key accepted decision, retention state-dependent but never size-bounded. A cold record is
   * retained for twice the maximum queue residency, so it expires only after every decision that
   * could still be compared against it has already been dropped as expired; a hot record is
   * retained until a COOL is accepted.
   */
  private final Cache<String, AcceptedDecision> accepted;

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
    this.accepted = Caffeine.newBuilder().expireAfter(new AcceptedDecisionExpiry(retentionNanos(retentionMs))).build();
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
    AcceptedDecision prev = accepted.getIfPresent(cacheKey);
    return prev == null ? 0L : prev.version();
  }

  /**
   * Records that a HOT was superseded before the wire. The cluster is cold unless a previous
   * accepted decision already describes another state. A later COOL with no predecessor then sees
   * a cold record and is elided, instead of demoting a key the cluster was never told was hot.
   *
   * <p>The record is installed only when the key has none. An existing record keeps its version,
   * state and send anchor: a COLD record already elides the lifecycle's COOL, and an UNKNOWN record
   * must stay unknown because a newer failed HOT still owes a corrective COOL. A hot record is
   * never demoted: it is proof the cluster was told the key is hot. Called by the dispatcher's drain
   * thread only, like every other funnel mutation.
   *
   * @param cacheKey the key whose HOT never reached the wire
   */
  void rememberCold(String cacheKey) {
    accepted.asMap().putIfAbsent(cacheKey, AcceptedDecision.coldTombstone());
  }

  /**
   * Clears the cold knowledge of an accepted record when a HOT ends before the wire. The
   * conservative correction for that HOT is to send its COOL anyway, because the cluster may hold a
   * state from an earlier lifecycle whose COOL was lost; a record still marked cold would otherwise
   * elide that correction.
   *
   * <p>The version watermark and the send anchor stay: a stale decision below {@code hotVersion} is
   * still superseded, and the debounce window is not restarted. Only a non-hot record below
   * {@code hotVersion} is downgraded to unknown. Called for a failed or expired HOT, and before a
   * superseded HOT either installs its own record or yields to a pending COOL. Called by the
   * dispatcher's drain thread only.
   *
   * @param cacheKey   the key whose HOT never reached the wire
   * @param hotVersion the failed, expired or superseded HOT's causal version
   */
  void forgetCold(String cacheKey, long hotVersion) {
    accepted
      .asMap()
      .computeIfPresent(cacheKey, (key, prev) ->
        !prev.isHot() && prev.version() < hotVersion ? prev.clearColdKnowledge() : prev
      );
  }

  /**
   * Broadcasts one decision, honouring its causal version, in <b>one</b> accepted-decision lookup.
   *
   * <p>A decision at or below the last version accepted for the key describes a state the cluster
   * has already moved past: it is suppressed in memory, and the caller sees
   * {@link Delivery#ELIDED} for an equal version or {@link Delivery#SUPERSEDED} for a strictly
   * lower one, never a send failure.
   *
   * <p>An empty record is not proof that the cluster is cold: the App promotes locally, and a COOL
   * whose earlier lifecycle lost its record is the only demotion signal. A COOL with no record is
   * therefore sent; a HOT that was queued and dropped before the wire installs a cold record, so the
   * later COOL of that same lifecycle is still elided.
   *
   * <p>The age bound is applied by {@link #decide} only after the in-memory state machine proves a
   * send is required: a decision whose age cannot change the cluster's state is elided or dropped as
   * superseded, never misreported as expired, and an expired decision proven sendable has no accepted
   * record at or above its version.
   *
   * <p>The caller guarantees a non-{@code NONE} decision with a positive causal version:
   * {@link DecisionDispatcher#submit} is the single boundary that rejects those, so this funnel
   * does not repeat the check.
   *
   * @param decision    the decision to publish
   * @param expiresAtMs the first monotonic instant at which queue residency has expired, or a value
   *                    no sample reaches for an unbounded caller
   * @return the outcome of the funnel
   */
  Delivery send(ZetaDecision decision, long expiresAtMs) {
    String key = decision.cacheKey();
    long sentAtMs = TimeSource.monotonicMillis();
    AcceptedDecision prev = accepted.getIfPresent(key);
    Delivery terminal = decide(sentAtMs, decision, expiresAtMs, prev);
    return terminal != null ? apply(terminal, prev, decision) : publish(decision, key, sentAtMs);
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
  private Delivery publish(ZetaDecision decision, String key, long sentAtMs) {
    ZetaDecision.DecisionType type = decision.type();
    long version = decision.decisionVersion();
    String typeCode = messageType(type);
    try {
      sendBroadcast(key, typeCode, version);
      metrics.countSentDecision(type);
      accepted.put(key, new AcceptedDecision(decision, sentAtMs));
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
   * {@link #commit} only after the queue removal and ownership take succeeded, so a shutdown
   * withdrawal cannot leave the funnel claiming a decision the submitter dropped.
   *
   * <p>The age bound is applied inside {@link #decide}: only a decision that would otherwise be
   * sent can be expired.
   *
   * @param decision    the decision to classify
   * @param expiresAtMs the first monotonic instant at which queue residency has expired, or a value
   *                    no sample reaches for an unbounded caller
   * @return the terminal classification, or {@code null} when the decision must be sent
   */
  @Nullable
  Verdict classify(ZetaDecision decision, long expiresAtMs) {
    long nowMs = TimeSource.monotonicMillis();
    AcceptedDecision prev = accepted.getIfPresent(decision.cacheKey());
    Delivery outcome = decide(nowMs, decision, expiresAtMs, prev);
    return outcome != null ? new Verdict(outcome, prev) : null;
  }

  /**
   * Applies a terminal in-memory classification after the dispatcher owns the item: advances the
   * accepted record for an elision and charges the outcome's meter. An elision with no prior record
   * creates the cold record, so a delayed older decision is still ordered against the version that
   * ended the lifecycle. The write-expiry anchor is refreshed so an elided decision keeps its key's
   * record alive for the same window a send would.
   *
   * @param verdict  the classification and the record it was decided against
   * @param decision the classified decision
   * @return the classification's outcome
   */
  Delivery commit(Verdict verdict, ZetaDecision decision) {
    return apply(verdict.outcome(), verdict.prev(), decision);
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
  private Delivery apply(Delivery outcome, @Nullable AcceptedDecision prev, ZetaDecision decision) {
    switch (outcome) {
      case EXPIRED -> {
      }
      case ELIDED -> {
        // decide() returns ELIDED only with a predecessor; the record advances to a new value.
        AcceptedDecision record = Objects.requireNonNull(prev, "elision requires a predecessor");
        accepted.put(decision.cacheKey(), record.elide(decision));
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
   * @param nowMs       the monotonic sample
   * @param decision    the decision being classified
   * @param expiresAtMs the first monotonic instant at which queue residency has expired
   * @param prev        its accepted record, or {@code null}
   * @return the terminal outcome, or {@code null} when the decision must be sent
   */
  @Nullable
  private static Delivery decide(long nowMs, ZetaDecision decision, long expiresAtMs, @Nullable AcceptedDecision prev) {
    if (prev == null) {
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
    if (!hot && prev.isKnownCold()) {
      // The last accepted decision for this key is a COOL and no HOT has been accepted since, so
      // every App is cold and this demotion would change nothing. An UNKNOWN record still sends
      // the corrective COOL: a HOT that could have corrected the cold state ended before the wire.
      return Delivery.ELIDED;
    }
    // Same type inside the debounce window is the identical cluster state; a type change travels.
    if (prev.debounced(hot, nowMs)) {
      return Delivery.ELIDED;
    }
    return nowMs >= expiresAtMs ? Delivery.EXPIRED : null;
  }

  private static String messageType(ZetaDecision.DecisionType type) {
    return type == ZetaDecision.DecisionType.HOT ? WorkerType.HOT.code() : WorkerType.COOL.code();
  }

  /**
   * Common send helper for HOT/COOL decisions.
   *
   * @param cacheKey the key the decision is about
   * @param type     the message type ({@link WorkerType#HOT} or
   *                 {@link WorkerType#COOL})
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
