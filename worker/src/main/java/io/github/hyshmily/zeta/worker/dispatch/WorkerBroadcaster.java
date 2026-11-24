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
import io.github.hyshmily.zeta.model.DecisionKind;
import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.worker.dispatch.connection.WorkerIdentity;
import io.github.hyshmily.zeta.worker.metrics.WorkerDetectionMetrics;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * Publishes HOT and COOL decisions to all application instances via the configured RabbitMQ
 * {@code broadcastExchange}.
 *
 * <p>Each message carries a monotonically increasing <b>decision version</b> allocated upstream under
 * the per-key lock that performed the transition (see {@link #send(ZetaDecision, long)}), so version
 * order is causal, not the order threads reached the wire. Messages reach every instance through a
 * <b>direct</b> exchange with the routing key {@code send.<appName>}, which keeps the fan-out inside
 * one application even when several share a broker; the receiver still checks the type and
 * {@code appName} headers.
 *
 * <p><b>The accepted record is wire truth, not a guess about the cluster.</b> It stores the last
 * decision that reached the wire, plus the version watermark advanced by an elided decision. Only an
 * equal/lower version and the 100 ms same-type debounce suppress a decision; a COOL is never
 * suppressed because the last wire state is COOL, since an App may have promoted locally after it. A
 * send failure keeps the last confirmed version and wire type but clears the confirmation and
 * debounce anchor. A record whose last wire state is COOL is retained for twice
 * {@code broadcast.max-decision-age-ms}; a HOT record is retained for as long as it remains the
 * last confirmed wire state, with a size cap as the backstop for a COOL that never reached the
 * funnel (queue saturation).
 *
 * <p>Only the dispatcher's drain thread calls the package-private classification and send methods,
 * so every accepted-record mutation is single-threaded; {@link AcceptedDecision} values are
 * immutable. Delivery is fire-and-forget: a lost decision converges through the App's hard TTL and
 * the local TopK's TTL authority, not through a retry here.
 *
 * <p>design decisions: decision delivery
 */
@Slf4j
public class WorkerBroadcaster {

  /**
   * What happened to one decision at the funnel. The dispatcher consumes a token only for
   * {@link #SENT}; an in-memory suppression costs no network budget, and {@link #FAILED} has already
   * been counted as a dropped decision here.
   */
  enum Delivery {
    /**
     * Handed to the AMQP client without an exception. No publisher confirm is requested, so this is
     * not a broker acknowledgement.
     */
    SENT,
    /**
     * Suppressed in memory without changing the wire state: the exact version was already the
     * watermark, or the same type was sent inside the debounce window. The caller must not return a
     * HOT seat for this outcome.
     */
    ELIDED,
    /**
     * Dropped because a <em>newer</em> version for the key was already accepted; the rollback is
     * identity-guarded and cannot undo that newer transition.
     */
    SUPERSEDED,
    /**
     * The queue-residency bound passed after the state machine proved a send was required. The
     * decision never touched the wire; the dispatcher counts it as expired and returns a HOT's
     * seat.
     */
    EXPIRED,
    /** The publish threw; the drop is counted. */
    FAILED,
  }

  /** Length of the same-type debounce window, anchored at the last <em>sent</em> decision. */
  private static final long DEDUP_WINDOW_MS = 100L;

  /**
   * Retention of a record that still carries the long HOT retention. Such a record is the only
   * evidence the cluster was told the key is hot, and a hot key can keep its seat indefinitely, so
   * it must outlive every state the cluster can still hold; one hundred years is a bound no process
   * reaches while staying clear of duration arithmetic. A failed or expired COOL clears the
   * retention bit, so the bound applies only while the Worker still holds the key hot.
   */
  private static final long HOT_RECORD_RETENTION_NANOS = TimeUnit.DAYS.toNanos(36_500L);

  /**
   * Per-key wire state and ordering watermark. Every field is immutable so a record read through the
   * cache is a stable fact. {@code flags} packs the last <em>confirmed</em> wire type, whether a send
   * completed, and whether the record still needs the long HOT retention; {@code sentAtMs} is the
   * monotonic instant of that send, used only for the 100 ms debounce. The instant is a full
   * {@code long}; a narrowed field aliases intervals across its wrap, and a HOT record can outlive
   * any such wrap by years.
   */
  private record AcceptedDecision(long sentAtMs, long version, int flags) {
    private static final int HOT_BIT = 1;
    private static final int CONFIRMED_BIT = 1 << 1;
    private static final int HOT_RETENTION_BIT = 1 << 2;

    /**
     * Records a decision that reached the AMQP client.
     *
     * @param version   the causal decision version
     * @param hot       whether the decision was HOT
     * @param sentAtMs  the monotonic send instant
     * @return the confirmed record
     */
    static AcceptedDecision confirmed(long version, boolean hot, long sentAtMs) {
      int flags = CONFIRMED_BIT | (hot ? HOT_BIT | HOT_RETENTION_BIT : 0);
      return new AcceptedDecision(sentAtMs, version, flags);
    }

    /**
     * Advances the version watermark without asserting a new wire state. Called for an elided
     * decision, which never touched the broker.
     *
     * <p>A debounced HOT is a real re-promotion that only the 100 ms window kept off the wire: the
     * Worker holds an active HOT seat again, so the long retention has to come back even when an
     * expired COOL cleared it a moment earlier. A same-version replay must not resurrect it — that
     * decides nothing new about the key.
     *
     * @param version   the new watermark
     * @param retainHot whether this elision re-establishes the long HOT retention
     * @return a copy with the wire state and debounce anchor unchanged
     */
    AcceptedDecision advance(long version, boolean retainHot) {
      return new AcceptedDecision(sentAtMs, version, retainHot ? flags | HOT_RETENTION_BIT : flags);
    }

    /**
     * Clears the confirmation and debounce anchor while keeping the last confirmed version and wire
     * type. Called when a publish throws: the broker may or may not have received the failed
     * decision, so it cannot become wire truth, but the version it was ordered against must stay
     * suppressed. The long HOT retention is cleared too: the failed transition may be the one whose
     * seat the dispatcher returns, so the record only needs the queue-residency bound to keep its
     * version watermark.
     *
     * @return a copy that only carries the last confirmed watermark and wire type
     */
    AcceptedDecision unconfirmed() {
      return new AcceptedDecision(0L, version, flags & HOT_BIT);
    }

    /**
     * Gives up the long HOT retention while keeping the wire state and debounce anchor. Called when
     * a COOL expired before it could be sent: the cluster may still be hot, but this Worker is not,
     * so the record only needs the queue-residency bound.
     *
     * @return a copy whose retention is the COOL bound
     */
    AcceptedDecision retentionLimited() {
      return new AcceptedDecision(sentAtMs, version, flags & ~HOT_RETENTION_BIT);
    }

    /**
     * @return whether the last confirmed wire state was HOT
     */
    boolean hot() {
      return (flags & HOT_BIT) != 0;
    }

    /**
     * @return whether this record still requires the long HOT retention
     */
    boolean hotRetained() {
      return (flags & HOT_RETENTION_BIT) != 0;
    }

    /**
     * Whether this record has the same wire state and a real send inside the debounce window.
     *
     * @param hot   the decision type being considered
     * @param nowMs the monotonic sample of that decision
     * @return {@code true} when the decision is a duplicate of the last real send
     */
    boolean debounced(boolean hot, long nowMs) {
      if ((flags & CONFIRMED_BIT) == 0 || hot() != hot) {
        return false;
      }
      long elapsed = nowMs - sentAtMs;
      return elapsed >= 0L && elapsed < DEDUP_WINDOW_MS;
    }
  }

  /**
   * Caffeine variable expiry: a record that still carries the long HOT retention is kept for the
   * lifetime of the Worker's hot state, while a record whose COOL was accepted, whose publish
   * failed or whose COOL expired keeps the queue-residency bound. Expiring a long-retained record
   * would let a stale HOT or an old COOL be re-sent after the key's queue decisions settled; the
   * App's version guard makes the replay harmless, but the send is wasted.
   */
  private record AcceptedDecisionExpiry(long coolRetentionNanos) implements Expiry<String, AcceptedDecision> {
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
      return value.hotRetained() ? Math.max(HOT_RECORD_RETENTION_NANOS, coolRetentionNanos) : coolRetentionNanos;
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
   * Per-key wire state with a state-dependent retention and a size cap. A COOL-state record is
   * retained for twice the maximum queue residency, so it expires only after every decision that
   * could still be compared against it has already been dropped as expired; a hot record keeps the
   * long retention until a COOL is accepted, a COOL publish fails or a COOL expires. The cap is the
   * backstop for a COOL dropped before the funnel (queue saturation), which no record update sees.
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
  @Getter
  private final WorkerIdentity identity;

  /** Precomputed {@code send.<appName>}; the value is fixed for the lifetime of the bean. */
  private final String routingKey;

  /**
   * Headers fixed for the process: the node id, epoch and application name. Built once because the
   * drain thread is the only publisher and none of the values can change after construction.
   */
  private final Map<String, Object> baseHeaders;

  /**
   * Constructs a broadcaster that sends each decision once.
   *
   * @param rabbitTemplate    the template that publishes messages
   * @param broadcastExchange the direct decision exchange name
   * @param appName           the application name carried in the send header
   * @param identity          this process's immutable node id and epoch
   * @param metrics           the emission counters (a meter-less instance is a no-op)
   * @param maxDecisionAgeMs  the queue-residency bound whose double sizes the COOL retention
   * @param maxAcceptedEntries the hard cap on retained per-key records
   */
  public WorkerBroadcaster(
    RabbitTemplate rabbitTemplate,
    String broadcastExchange,
    String appName,
    WorkerIdentity identity,
    WorkerDetectionMetrics metrics,
    long maxDecisionAgeMs,
    long maxAcceptedEntries
  ) {
    if (maxDecisionAgeMs <= 0L) {
      throw new IllegalArgumentException("maxDecisionAgeMs must be positive, got " + maxDecisionAgeMs);
    }
    if (maxAcceptedEntries <= 0L) {
      throw new IllegalArgumentException("maxAcceptedEntries must be positive, got " + maxAcceptedEntries);
    }
    this.metrics = metrics;
    this.identity = identity;
    this.rabbitTemplate = rabbitTemplate;
    this.broadcastExchange = broadcastExchange;
    this.routingKey = PREFIX_DECISION + appName;
    this.baseHeaders = Map.of(
      HEADER_NODE_ID,
      identity.nodeId(),
      HEADER_EPOCH,
      identity.epoch(),
      HEADER_APP_NAME,
      appName
    );
    long retentionMs = maxDecisionAgeMs > Long.MAX_VALUE / 2L ? Long.MAX_VALUE : 2L * maxDecisionAgeMs;
    this.accepted = Caffeine.newBuilder()
      .maximumSize(maxAcceptedEntries)
      .expireAfter(new AcceptedDecisionExpiry(toNanos(retentionMs)))
      .build();
  }

  /** Converts a millisecond retention to nanoseconds, saturating instead of wrapping. */
  private static long toNanos(long retentionMs) {
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
   * Broadcasts one decision, honouring its causal version, in <b>one</b> wire-state lookup.
   *
   * <p>A decision at or below the last accepted version is suppressed in memory:
   * {@link Delivery#ELIDED} for an equal version, {@link Delivery#SUPERSEDED} for a lower one, never
   * a send failure. An empty record is not proof that the cluster is cold, so a COOL with no record
   * is sent. The age bound is applied by {@link #decide} only after the state machine proves a send
   * is required, so a decision that cannot change the cluster's state is never reported expired.
   *
   * <p>The caller guarantees a non-{@code NONE} decision with a positive causal version:
   * {@link DecisionDispatcher#submit} is the single boundary that rejects those.
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
    return terminal != null ? settle(terminal, prev, decision) : emit(decision, key, sentAtMs, prev);
  }

  /**
   * Sends one decision and records its outcome: on success the accepted record becomes the new wire
   * state; on a publish failure the last confirmed version and wire type stay as the ordering
   * watermark, the confirmation and debounce anchor are cleared, and the drop is counted. Once the
   * publish returns, the record update and the sent meter are in-memory bookkeeping and are not
   * guarded: they cannot fail for a valid key/value, and a guard could only turn a diagnostics error
   * into a delivery failure.
   *
   * @param decision the decision to publish
   * @param key      the decision's cache key
   * @param sentAtMs the monotonic send instant
   * @param prev     the accepted record {@link #send} classified against, or {@code null}
   * @return {@link Delivery#SENT} or {@link Delivery#FAILED}
   */
  private Delivery emit(ZetaDecision decision, String key, long sentAtMs, @Nullable AcceptedDecision prev) {
    ZetaDecision.DecisionType type = decision.type();
    long version = decision.decisionVersion();
    String typeCode = messageType(type);
    try {
      publish(key, typeCode, version);
    } catch (RuntimeException e) {
      if (prev != null) {
        accepted.put(key, prev.unconfirmed());
      }
      metrics.countDroppedDecision(type);
      if (log.isDebugEnabled()) {
        log.debug("Failed to broadcast {} decision for key={}: {}", typeCode, key, e.toString());
      }
      return Delivery.FAILED;
    }

    accepted.put(key, AcceptedDecision.confirmed(version, type == ZetaDecision.DecisionType.HOT, sentAtMs));
    metrics.countSentDecision(type);
    return Delivery.SENT;
  }

  /**
   * Classifies a token-starved queue head without sending and <b>without</b> mutating the accepted
   * record or the meters. The dispatcher calls this before it owns the item, then calls
   * {@link #commit} only after the queue removal and ownership take succeeded, so a shutdown
   * withdrawal cannot leave the funnel claiming a decision the submitter dropped.
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
   * Applies a terminal classification after the dispatcher owns the item: advances the version
   * watermark for an elision, charges the outcome's meter, and refreshes the record's write expiry.
   * An elision never changes the wire state, only the version a later decision is ordered against.
   *
   * @param verdict  the classification and the record it was decided against
   * @param decision the classified decision
   * @return the classification's outcome
   */
  Delivery commit(Verdict verdict, ZetaDecision decision) {
    return settle(verdict.outcome(), verdict.prev(), decision);
  }

  /**
   * Settles one terminal in-memory outcome: advances the version watermark for an elision, keeps the
   * wire state unchanged, and charges the outcome's meter. The send path and the token-starved head
   * share this tail, so both treat the same classification identically.
   *
   * @param outcome  the terminal outcome
   * @param prev     the accepted record it was decided against, or {@code null}
   * @param decision the classified decision
   * @return the outcome, for the caller to return
   */
  private Delivery settle(Delivery outcome, @Nullable AcceptedDecision prev, ZetaDecision decision) {
    switch (outcome) {
      case EXPIRED -> {
        // A COOL that expired before it could be sent is the last evidence the Worker held the
        // key hot. Drop the century retention so a one-off key cannot pin its record forever;
        // the wire state and watermark stay untouched because the cluster may still be hot.
        if (prev != null && prev.hotRetained() && decision.type() == ZetaDecision.DecisionType.COOL) {
          accepted.put(decision.cacheKey(), prev.retentionLimited());
        }
      }
      case ELIDED -> {
        AcceptedDecision record = Objects.requireNonNull(prev, "elision requires a predecessor");
        boolean retainHot =
          decision.type() == ZetaDecision.DecisionType.HOT && decision.decisionVersion() > record.version();
        accepted.put(decision.cacheKey(), record.advance(decision.decisionVersion(), retainHot));
        metrics.countElidedDecision(decision.type());
      }
      case SUPERSEDED -> metrics.countSupersededDecision(decision.type());
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
    if (prev.debounced(hot, nowMs)) {
      return Delivery.ELIDED;
    }
    return nowMs >= expiresAtMs ? Delivery.EXPIRED : null;
  }

  private static String messageType(ZetaDecision.DecisionType type) {
    return type == ZetaDecision.DecisionType.HOT ? DecisionKind.HOT.code() : DecisionKind.COOL.code();
  }

  /**
   * Common publish helper for HOT/COOL decisions.
   *
   * @param cacheKey the key the decision is about
   * @param type     the message type ({@link DecisionKind#HOT} or
   *                 {@link DecisionKind#COOL})
   * @param version  the monotonically increasing decision version for ordering
   */
  private void publish(String cacheKey, String type, long version) {
    MessageProperties props = new MessageProperties();
    props.getHeaders().putAll(baseHeaders);
    props.setHeader(HEADER_TYPE, type);
    props.setHeader(HEADER_VERSION, version);
    // design decisions: sync messages — the routing key ({@code send.<appName>}) is what keeps
    // decisions inside one application on a shared broker; the header repeats the address for
    // the receiver's own boundary check.
    // Trace-only ID (no plane compares it for ordering): the cached TimeSource
    // read keeps it time-sortable without contending the data-version clock or
    // burning a sequence number of the shared 4096/ms stamp window.
    props.setHeader(HEADER_MESSAGE_ID, TimeSource.currentTimeMillis());

    Message msg = new Message(cacheKey.getBytes(StandardCharsets.UTF_8), props);
    rabbitTemplate.send(broadcastExchange, routingKey, msg);
  }
}
