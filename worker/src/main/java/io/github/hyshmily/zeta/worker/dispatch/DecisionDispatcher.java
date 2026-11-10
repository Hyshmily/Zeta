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

import io.github.hyshmily.zeta.model.ZetaDecision;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.executor.ZetaThreadFactory;
import io.github.hyshmily.zeta.worker.detection.lifecycle.HE;
import io.github.hyshmily.zeta.worker.metrics.WorkerDetectionMetrics;
import jakarta.annotation.PreDestroy;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * The single submission point for Worker decisions: one HOT queue, one COOL queue, and a drain thread
 * that rate-limits both.
 *
 * <p>
 * A COOL releases a seat and a HOT spends one, so the two decisions get separate queues and token
 * buckets: a mass-heat wave of promotions must not delay the demotions that make room for them. The
 * drain sends COOL first, then HOT, each capped by its per-round burst; a submission only offers to a
 * bounded queue and never waits for a send. The one exception is a COOL already superseded by a newer
 * HOT: when the HOT lane can send, the round lets the HOT be accepted first, so the next pass drops
 * the obsolete demotion in memory instead of demoting and re-promoting every App.
 *
 * <p>
 * A newer decision for a key makes an older queued HOT obsolete, marked through the older item's own
 * hand-off word, so the drain reads one field instead of hashing the key again; when the newer
 * decision is already pending, the older HOT is dropped before the offer and never takes a queue slot.
 * A HOT is equally obsolete when the registry no longer holds its promotion: the report thread can
 * stall between the promotion and the submission while the key cools (or cools and re-promotes), and
 * the demotion's own decision may be refused, expired or failed — so neither the pending map nor the
 * accepted record would ever prove the HOT stale. A queued HOT never suppresses a COOL on its own: a
 * demotion is dropped as obsolete only when the accepted record already covers it. A COOL with no
 * accepted record is sent, because the cluster may be hot from local promotion or from an earlier
 * lifecycle whose COOL was lost. A decision that waited longer than {@code max-decision-age-ms} is
 * dropped at the funnel only after the in-memory state machine proves it would otherwise be sent.
 * The broadcaster keeps a cold accepted record for twice this age bound and a hot one until a COOL
 * is accepted, never size-evicts it, so every decision still allowed to travel can be compared
 * against the newest accepted one.
 *
 * <p>
 * Delivery is fire-and-forget: a decision is sent once and never retried. A HOT whose send call never
 * succeeds returns its seat through {@link Rollback}, identity-guarded by the decision
 * version so a later promotion of the same key is never undone; a dropped submission returns
 * {@code false} so the caller aborts its own promotion.
 *
 * <p>
 * design decisions: decision delivery
 */
@Slf4j
@Internal
public class DecisionDispatcher {

  /**
   * Retry pace after a failed send; the broker, not a token bucket, is the reason to pause.
   */
  private static final long FAILURE_BACKOFF_MS = 10L;

  /**
   * Most queue items one lane may process in a round. Sends are separately capped by the lane burst;
   * this bound keeps a continuously refilled lane of terminal items from monopolizing the single
   * drain thread.
   */
  private static final int LANE_WORK_BUDGET = 1_024;

  /**
   * Pending item lifecycle, one orthogonal bit per fact. {@link #UNQUEUED} is the construction state:
   * the item is not visible to the drain until {@link Pending#enqueue(int)} runs immediately before
   * the queue offer, so a refused offer returns it to a state no other thread has observed.
   * {@link #QUEUED} records the successful offer; {@link #OWNED} records that exactly one thread has
   * taken the item and owns its terminal outcome — the drain's claim and the shutdown withdraw are the
   * same operation, because both only decide who reports the outcome. Every lifecycle bit is disjoint
   * from the flags below, so an ownership claim and a supersede mark never invalidate each other.
   */
  private static final int UNQUEUED = 0;
  private static final int QUEUED = 1;
  private static final int OWNED = 1 << 1;

  /**
   * Set when a newer decision for the key is already published: on the map value it displaced, or on
   * an older COOL queued behind it. The drain skips the identity removal for such an item because the
   * map points at, or past, a different item. It is also set by
   * {@link Pending#drop()} on an older HOT that is never offered, which is how
   * {@link #submit} recognises that drop without a second result channel.
   */
  private static final int SUPERSEDED_BY_PENDING = 1 << 2;

  /** Hands an undelivered HOT promotion back to the registry. */
  @FunctionalInterface
  public interface Rollback {
    /**
     * @param key             the key whose promotion is undone
     * @param decisionVersion the version of the HOT decision that never reached the broker
     */
    void abort(String key, long decisionVersion);

    /**
     * A no-op callback for constructions that do not own a registry (tests, embedded use).
     */
    Rollback NOOP = (key, decisionVersion) -> {};
  }

  /**
   * Reads the registry's current promotion for a key: whether the transition that allocated the HOT's
   * version still owns the key's seat.
   */
  @FunctionalInterface
  public interface Probe {
    /**
     * @param key             the key the decision is about
     * @param decisionVersion the version of the HOT decision
     * @return {@code true} while the registry still holds exactly this promotion
     */
    boolean isCurrent(String key, long decisionVersion);

    /**
     * A probe that never reports a promotion stale, for constructions without a registry.
     */
    Probe ALWAYS = (key, decisionVersion) -> true;
  }

  /**
   * A decision, the instant its queue residency ends, and the hand-off state between {@link #submit}
   * and the drain.
   *
   * <p>
   * The state is a plain {@code volatile int}, updated through {@link #STATE}. Keeping it primitive
   * avoids the second header an {@code AtomicInteger} would add; the item stays one small allocation
   * (header, one reference, a {@code long} and the {@code int}). All hand-off facts — not yet
   * offered, queued, owned, displaced — live in this one word, so the submitter and the drain never
   * maintain two overlapping status fields.
   */
  private static final class Pending {

    private volatile int state = UNQUEUED;
    private static final VarHandle STATE;

    static {
      try {
        STATE = MethodHandles.lookup().findVarHandle(Pending.class, "state", int.class);
      } catch (ReflectiveOperationException e) {
        throw new ExceptionInInitializerError(e);
      }
    }

    /**
     * First monotonic instant at which the decision is expired, from the queue's residency bound.
     */
    final long expiresAtMs;

    final ZetaDecision decision;

    Pending(ZetaDecision decision, long expiresAtMs) {
      this.decision = decision;
      this.expiresAtMs = expiresAtMs;
    }

    /**
     * Publishes the item as queued with every lifecycle flag already in the word and offers it to
     * the lane. Called immediately before the offer, so the drain can only observe the finished
     * state; no other thread can see the item yet, so the write needs no CAS. A refused offer
     * returns the item to the construction state, which no other thread has observed.
     *
     * @param queue        the lane queue to offer to
     * @param initialFlags the lifecycle bits decided under the bucket lock
     * @return {@code true} when the queue accepted the item
     */
    boolean enqueue(ArrayBlockingQueue<Pending> queue, int initialFlags) {
      state = QUEUED | initialFlags;
      if (queue.offer(this)) {
        return true;
      }
      state = UNQUEUED;
      return false;
    }

    /**
     * @return whether the queue offer was refused, so this item never became visible
     */
    boolean refused() {
      return state == UNQUEUED;
    }

    /**
     * Takes ownership of a queued item; {@code false} when it was never queued or already owned.
     *
     * <p>
     * One atomic read-modify-write with no retry loop: the ownership bit is disjoint from every other
     * lifecycle flag, so a concurrent {@link #supersede()} cannot invalidate this claim and force a
     * re-read.
     */
    boolean claim() {
      int previous = (int) STATE.getAndBitwiseOr(this, OWNED);
      return (previous & (QUEUED | OWNED)) == QUEUED;
    }

    /**
     * Marks this item as displaced in {@link #queued} by a newer decision.
     */
    void supersede() {
      // The bit is disjoint from QUEUED/OWNED, so the OR cannot disturb a concurrent claim.
      STATE.getAndBitwiseOr(this, SUPERSEDED_BY_PENDING);
    }

    /**
     * Marks an older HOT dropped before its offer. The QUEUED bit stays clear, so
     * {@link #dropped()} can tell this path from an item that was actually queued.
     */
    void drop() {
      state = SUPERSEDED_BY_PENDING;
    }

    /**
     * @return whether this item was dropped before the offer because a newer decision was pending
     */
    boolean dropped() {
      return state == SUPERSEDED_BY_PENDING;
    }

    /**
     * @return whether a newer decision for this key was published while this item was queued
     */
    boolean superseded() {
      return (state & SUPERSEDED_BY_PENDING) != 0;
    }

    /** @return the causal decision version */
    long version() {
      return decision.decisionVersion();
    }

    /** @return the decision's cache key */
    String key() {
      return decision.cacheKey();
    }

    /** @return the decision type */
    ZetaDecision.DecisionType type() {
      return decision.type();
    }

    /** @return whether this item carries a HOT decision */
    boolean isHot() {
      return type() == ZetaDecision.DecisionType.HOT;
    }
  }

  /**
   * One queue and its token bucket. The two lanes differ only in capacity, rate and burst; the drain
   * logic is identical and lives in {@link #drain(Lane)}.
   */
  private static final class Lane {

    private final ArrayBlockingQueue<Pending> queue;
    private final int burst;
    private final int rate;

    /** Touched only by the drain thread. */
    private double tokens;

    private Lane(int capacity, int rate, int burst) {
      this.rate = rate;
      this.burst = burst;
      this.queue = new ArrayBlockingQueue<>(capacity);
    }

    /**
     * Refills at the sustained rate, capped at one second of budget or the per-round burst when that
     * is larger. A burst above the rate would otherwise be unreachable: the bucket would cap the
     * round below the configured burst, silently ignoring the setting.
     */
    private void refill(double elapsedSeconds) {
      tokens = Math.min(Math.max(rate, burst), tokens + elapsedSeconds * rate);
    }

    /**
     * Nanoseconds until this lane can make progress without a new submission: the first token, or the
     * queued head reaching its age bound.
     */
    private long nanosToNextEvent(long nowMs) {
      Pending head = queue.peek();
      if (head == null) {
        return Long.MAX_VALUE;
      }
      if (tokens >= 1.0d) {
        return 0L;
      }
      long nanosToToken = (long) Math.ceil(((1.0d - tokens) * 1_000_000_000.0d) / rate);
      long nanosToExpiry = TimeUnit.MILLISECONDS.toNanos(Math.max(0L, head.expiresAtMs - nowMs));
      return Math.min(nanosToToken, nanosToExpiry);
    }

    private int depth() {
      return queue.size();
    }
  }

  /**
   * What one round settled. {@code sent} drives the token accounting; {@code processed} also counts
   * items that ended in memory, so a round that only discarded terminal decisions still advances the
   * loop instead of waiting for a token.
   */
  private record RoundResult(int sent, int processed) {
    private static final RoundResult NONE = new RoundResult(0, 0);
  }

  private final Rollback rollback;
  private final Probe probe;
  private final WorkerDetectionMetrics metrics;
  private final WorkerBroadcaster broadcaster;

  /**
   * Newest queued or in-flight decision per key. The value is the queue item itself, so a newer
   * submission marks the displaced item through its own state word instead of boxing a version or
   * forcing the drain to hash the key again. Every terminal outcome removes the entry when it still
   * maps to that item, so the map is bounded by the queues. The queue offer and the map publication
   * are one per-key step (see {@link #submit}), so a newer submission either sees the older item or the
   * older offer was refused and never suppressed anything.
   */
  private final ConcurrentHashMap<String, Pending> queued = new ConcurrentHashMap<>();

  /**
   * Released on every successful submission and on shutdown, so an idle drain blocks instead of polling.
   */
  private final Semaphore pendingSignal = new Semaphore(0);

  private final Thread drainThread;
  private final long maxAgeMs;
  private final Lane coolLane;
  private final Lane hotLane;

  /**
   * Cleared by {@link #shutdown()}; new submissions are then dropped and counted.
   */
  private volatile boolean running = true;

  /** Refill clock, touched only by the drain thread. */
  private long lastRefillNanos;

  /** Drain-thread only: whether the last round's send attempt failed. */
  private boolean sendFailed;

  /**
   * Drain-thread only: the cool head that yielded to a newer HOT in the previous round. A second
   * round with the same head sends it instead of yielding again, so one deep HOT cannot starve the
   * unrelated demotions behind it.
   */
  private @Nullable Pending yieldedCoolHead;

  /**
   * Creates the dispatcher and starts its drain thread.
   *
   * @param hotCapacity       pending HOT decisions before drop-on-saturation
   * @param coolCapacity      pending COOL decisions before drop-on-saturation
   * @param hotRate           HOT sends per second
   * @param coolRate          COOL sends per second
   * @param hotBurst          most HOT sends in one round
   * @param coolBurst         most COOL sends in one round
   * @param maxAgeMs          maximum queue residency before a decision is dropped
   * @param broadcaster       the funnel every decision is sent through
   * @param metrics           detection meters (a meter-less instance is a no-op)
   * @param rollback undo sink for a HOT that never reaches the broker; never {@code null}
   * @param probe currency probe for a queued HOT; never {@code null}
   */
  public DecisionDispatcher(
    int hotCapacity,
    int coolCapacity,
    int hotRate,
    int coolRate,
    int hotBurst,
    int coolBurst,
    long maxAgeMs,
    WorkerBroadcaster broadcaster,
    WorkerDetectionMetrics metrics,
    Rollback rollback,
    Probe probe
  ) {
    if (hotCapacity <= 0 || coolCapacity <= 0) {
      throw new IllegalArgumentException("Lane capacity must be positive");
    }
    if (hotRate <= 0 || coolRate <= 0) {
      throw new IllegalArgumentException("Lane rate must be positive");
    }
    if (hotBurst <= 0 || coolBurst <= 0) {
      throw new IllegalArgumentException("Lane burst must be positive");
    }
    if (maxAgeMs <= 0L) {
      throw new IllegalArgumentException("maxAgeMs must be positive");
    }
    this.broadcaster = Objects.requireNonNull(broadcaster, "broadcaster");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.rollback = Objects.requireNonNull(rollback, "rollback");
    this.probe = Objects.requireNonNull(probe, "probe");
    this.maxAgeMs = maxAgeMs;
    this.hotLane = new Lane(hotCapacity, hotRate, hotBurst);
    this.coolLane = new Lane(coolCapacity, coolRate, coolBurst);
    this.drainThread = new ZetaThreadFactory("zeta-worker-broadcast").newThread(this::drainLoop);
    this.drainThread.start();
  }

  /**
   * Submits one HOT promotion produced by the registry. On a refused enqueue this call returns the
   * seat and the caller owns nothing.
   *
   * @param promoted the promotion to deliver
   */
  public void submit(HE promoted) {
    if (!submit(promoted.decision())) {
      abort(promoted.key(), promoted.version());
    }
  }

  /**
   * Submits one decision for delivery.
   *
   * @param decision the HOT or COOL decision; {@code NONE} and {@code null} are ignored
   * @return {@code true} when the caller must not undo the promotion: the decision was queued,
   *         ignored as {@code NONE}, or this call already compensated an older HOT that a newer
   *         pending decision superseded; {@code false} when the decision was dropped and the caller
   *         owns the promotion rollback
   * @throws IllegalArgumentException when a HOT or COOL decision carries no causal version
   */
  public boolean submit(@Nullable ZetaDecision decision) {
    if (decision == null || decision.type() == ZetaDecision.DecisionType.NONE) {
      return true;
    }
    if (decision.decisionVersion() <= 0) {
      throw new IllegalArgumentException(
        "Worker decisions must carry the causal version allocated under the registry's per-key lock: key=" +
          decision.cacheKey()
      );
    }

    ZetaDecision.DecisionType type = decision.type();
    Lane lane = type == ZetaDecision.DecisionType.COOL ? coolLane : hotLane;
    ArrayBlockingQueue<Pending> queue = lane.queue;
    if (!running) {
      drop(type);
      return false;
    }

    Pending pending = new Pending(decision, expiresAt(TimeSource.monotonicMillis()));
    // The offer and the publication are one per-key step: the bin lock makes the newest decision visible
    // before a newer one can replace it, and a refused offer leaves the previous record untouched.
    queued.compute(decision.cacheKey(), (key, previous) -> publish(pending, previous, queue));
    if (pending.dropped()) {
      // A strictly newer decision proves a later transition for this key already ran: the entry this HOT
      // created is gone or replaced, so the identity-guarded abort is a no-op and needs no
      // accepted-version read on this producer thread. This HOT never entered a queue; the newer
      // pending decision is what travels.
      metrics.countSupersededDecision(type);
      abort(pending);
      return true;
    }

    if (pending.refused()) {
      drop(type);
      return false;
    }
    // The drain owns the terminal outcome from here. Its identity-matched remove cannot pass the bin lock
    // this compute held while offering, so by the time this call returns the record is either still
    // present (the drain will remove it) or already replaced by a newer submission. Shutdown may have
    // flipped running between the check and the offer. Either this thread withdraws the item (and reports
    // the drop synchronously) or the drain already claimed it and owns the terminal outcome, including
    // the HOT rollback. The claim() hand-off is what makes the return value match the owner.
    if (!running && pending.claim()) {
      queue.remove(pending);
      forget(pending);
      drop(type);
      return false;
    }

    pendingSignal.release();
    return true;
  }

  /**
   * Saturating expiry: a huge age must not wrap to a negative instant that expires instantly.
   */
  private long expiresAt(long nowMs) {
    long expiresAt = nowMs + maxAgeMs;
    if (expiresAt < nowMs) {
      return Long.MAX_VALUE;
    }
    return expiresAt == Long.MAX_VALUE ? Long.MAX_VALUE : expiresAt + 1L;
  }

  /**
   * The per-key publication step, run under the map's bin lock: decides which item the map keeps and
   * offers the new one when it is still needed.
   *
   * <p>
   * Three cases: a newer pending decision drops an older HOT before the offer; an older COOL is queued
   * behind the newer item without taking ownership of the map; otherwise the new item is offered and
   * becomes the map value. Equal versions are duplicates and both may be enqueued, but the map tracks the
   * copy offered last so a newer decision still finds a predecessor after the first one settles.
   *
   * @param pending  the item to publish
   * @param previous the map's current item, or {@code null}
   * @param queue    the lane queue {@code pending} belongs to
   * @return the item the map should keep
   */
  private Pending publish(Pending pending, @Nullable Pending previous, ArrayBlockingQueue<Pending> queue) {
    if (!running) {
      // Shutdown began while this submission waited for the bin lock: refuse the offer without
      // touching the predecessor. The caller reports the drop synchronously, exactly like the
      // pre-compute check.
      return previous;
    }
    if (previous != null && previous.version() > pending.version()) {
      if (!pending.isHot()) {
        // A COOL is still queued: only the accepted record may drop a demotion, and the drain must skip
        // this item's map removal because the newer item owns the entry.
        pending.enqueue(queue, SUPERSEDED_BY_PENDING);
        return previous;
      }
      // The newer decision owns the key's delivery; this older HOT changes nothing and must not
      // consume a queue slot.
      pending.drop();
      return previous;
    }

    boolean wins = previous == null || previous.version() < pending.version();
    // Every lifecycle bit is decided under the bin lock and published by this one plain write. The
    // item is not visible to the drain before the offer, so no atomic OR is needed: the drain either
    // polls the finished word or does not see the item at all.
    if (!pending.enqueue(queue, 0)) {
      return previous;
    }
    if (wins && previous != null) {
      previous.supersede();
    }
    // An equal version is a duplicate offered while the first copy is still queued: both may be
    // enqueued, but the map tracks the copy offered last, so a newer decision still finds a
    // predecessor after the first copy settles.
    return pending;
  }

  /**
   * The single delivery loop: refill both buckets, drain COOL before HOT, and block on the submission
   * semaphore when both queues are empty. A failed send backs off instead of spinning, and shutdown
   * breaks the loop only after the current round so the final drain owns the remainder.
   */
  private void drainLoop() {
    lastRefillNanos = System.nanoTime();
    // Start empty: a restart must not emit a full second of budget in its first round. At the default
    // 500/s the first token arrives within 2ms.
    hotLane.tokens = 0.0d;
    coolLane.tokens = 0.0d;
    try {
      while (running || !queuesEmpty()) {
        try {
          RoundResult round = drainRound();
          if (sendFailed) {
            if (!running) {
              // Shutdown with a failing broker: retrying at 10ms intervals would keep the thread alive
              // past the join timeout. The remainder is counted by drainRemaining().
              break;
            }
            // Keep the broker-failure retry pace; a failed decision never consumes a token.
            Thread.sleep(FAILURE_BACKOFF_MS);
            continue;
          }
          if (round.sent() > 0 || round.processed() > 0) {
            // Permits released while this round was busy describe decisions already handled; drop them so
            // the next empty round blocks instead of spinning through stale signals. A round that only
            // settled terminal decisions is also progress: the next round continues the backlog without
            // waiting for a token those decisions do not need.
            pendingSignal.drainPermits();
            continue;
          }
          if (!awaitWork()) {
            break;
          }
        } catch (RuntimeException e) {
          // A single unexpected failure must not kill the only delivery thread while running stays true:
          // submissions would keep filling the queues and every HOT would be aborted on refusal. The
          // in-flight item's HOT seat was already returned before the failure propagated out of drain.
          log.error("Decision drain round failed; retrying after {}ms", FAILURE_BACKOFF_MS, e);
          if (!running) {
            // Shutdown does not wait for the backoff: the final drain counts the remainder.
            break;
          }
          Thread.sleep(FAILURE_BACKOFF_MS);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      // Anything left after shutdown is a drop, and must be counted: the loop's exit condition and a
      // concurrent submit are not one atomic step.
      drainRemaining();
    }
  }

  /**
   * One send round: refill both buckets and drain COOL before HOT.
   *
   * <p>A broker failure is global: do not pull a HOT only to lose it in the same outage. The round
   * still returns the COOL count so the caller can account for the tokens it spent.
   *
   * @return the decisions sent and the items whose queue residency ended this round
   */
  private RoundResult drainRound() {
    refill();
    sendFailed = false;
    RoundResult cool = drain(coolLane);
    RoundResult hot = sendFailed ? RoundResult.NONE : drain(hotLane);
    return new RoundResult(cool.sent() + hot.sent(), cool.processed() + hot.processed());
  }

  /**
   * Waits until this round has work it can do without a new submission.
   *
   * <p>An empty queue waits on the submission semaphore; a non-empty but token-starved queue waits
   * for the next refill or the head's age bound. The permits are drained before waiting so a stale
   * wakeup cannot spin the loop through work this round cannot perform.
   *
   * @return {@code false} when shutdown was observed and the loop must stop
   * @throws InterruptedException when the drain thread is interrupted
   */
  private boolean awaitWork() throws InterruptedException {
    if (!queuesEmpty()) {
      if (hasReadyWork()) {
        return true;
      }
      // Shutdown: the tokens available now were already spent by the two drains above. Waiting for a
      // refill only burns the join timeout; drainRemaining() counts the remainder as dropped and
      // returns the HOT seats. Drop the accumulated permits before checking, so the shutdown release
      // below is observed by the same check.
      pendingSignal.drainPermits();
      if (!running) {
        return false;
      }
      // A submission between the first check and the permit drain can make a head terminal; its
      // release was just discarded, so re-check before sleeping or that head waits for a token it
      // does not need.
      if (hasReadyWork()) {
        return true;
      }
      // Pending work but no usable token: wait for the next refill. The wait is on the semaphore, not
      // a fixed sleep, because the other queue may be idle and needs no token — a COOL arriving
      // behind a HOT-starved round must wake the drain immediately instead of waiting out a pause
      // nothing can signal. The bound also covers the head's age limit, so an expired decision still
      // returns its HOT seat on time. A round that only elided, superseded or expired decisions loops
      // immediately without a pause: those outcomes cost no send budget.
      pendingSignal.tryAcquire(nanosToNextEvent(), TimeUnit.NANOSECONDS);
      return true;
    }

    // Clear permits left by a suppressed round, then re-check: a submission between the emptiness
    // check and here is visible in the queue, so draining its permit cannot lose the wakeup. Only a
    // still-empty queue blocks.
    pendingSignal.drainPermits();
    if (!running) {
      return false;
    }
    if (queuesEmpty()) {
      pendingSignal.acquire();
    }
    return true;
  }

  /** @return whether both lanes have no queued decision */
  private boolean queuesEmpty() {
    return coolLane.queue.isEmpty() && hotLane.queue.isEmpty();
  }

  /**
   * Whether any non-empty lane can act now. A lane with a token can send; without one, a head the
   * funnel would settle in memory (superseded, expired, elided or a redundant COOL) leaves without
   * a send budget. A cool head with a token always counts: the next round either sends it or yields
   * once to a newer HOT that the hot lane can send in the same round.
   *
   * @return {@code true} when the next round can make progress without a new submission
   */
  private boolean hasReadyWork() {
    Pending hotHead = hotLane.queue.peek();
    if (hotHead != null && (hotLane.tokens >= 1.0d || isTerminalHead(hotHead))) {
      return true;
    }
    Pending coolHead = coolLane.queue.peek();
    return coolHead != null && (coolLane.tokens >= 1.0d || isTerminalHead(coolHead));
  }

  /**
   * Whether the funnel would settle this head without a send. Mirrors the checks in
   * {@link #drain(Lane)}: a HOT with a newer pending decision or a released promotion, or any head
   * the broadcaster's state machine classifies as terminal.
   *
   * @param head the lane head
   * @return {@code true} when the head can leave without a token
   */
  private boolean isTerminalHead(Pending head) {
    return supersededHot(head) || broadcaster.classify(head.decision, head.expiresAtMs) != null;
  }

  /**
   * Nanoseconds until the drain has something to do without a new submission: the first starved queue
   * receives a full token, or a queued head reaches {@code maxAgeMs} and becomes droppable.
   *
   * <p>
   * Both buckets refill continuously from the same elapsed time, so the earlier refill is what unblocks
   * the round. The head's age bound matters for the same reason it is checked before the token gate: a
   * decision that expires while starved must be dropped — and a HOT's seat returned — at its bound,
   * not at the next refill.
   *
   * <p>
   * Only read while a non-empty lane has no token; the fallback covers a state change between that
   * check and this call, not an ordinary round.
   *
   * @return the wait bound for the drain's semaphore
   */
  private long nanosToNextEvent() {
    long now = TimeSource.monotonicMillis();
    long nanos = Math.min(coolLane.nanosToNextEvent(now), hotLane.nanosToNextEvent(now));
    return nanos == Long.MAX_VALUE ? TimeUnit.MILLISECONDS.toNanos(FAILURE_BACKOFF_MS) : nanos;
  }

  /**
   * Drains one lane for at most its per-round burst.
   *
   * <p>
   * With a token available the drain polls and hands the decision to the funnel. The pending map is the
   * one check the funnel cannot make: it suppresses an older HOT behind a newer queued decision, while a
   * COOL is corrected only by the accepted record. The funnel then applies its in-memory state machine
   * and the queue's age bound in one pass, with one accepted-decision read and one clock sample: a
   * duplicate, superseded version, redundant COOL or debounced same-type decision is elided or dropped
   * as superseded regardless of age, and only a decision that would otherwise reach the wire can be
   * reported expired. An expired HOT needs no second record read — the funnel proved no accepted record
   * at or above its version exists — so its seat returns through the unguarded abort.
   *
   * <p>
   * A COOL with a newer HOT still pending therefore yields only when it is still sendable: the HOT is
   * accepted first, and the next pass drops the demotion as superseded instead of demoting and
   * re-promoting every App. The yield is bounded to one round per head, so a HOT that is deeper than
   * the per-round burst cannot block every unrelated demotion behind that one key.
   *
   * @param lane the lane to drain
   * @return the decisions sent and the items whose queue residency ended
   */
  private RoundResult drain(Lane lane) {
    ArrayBlockingQueue<Pending> queue = lane.queue;
    int sent = 0;
    int processed = 0;
    // The bucket is touched only by this thread; work on a local and publish it in the finally so a send
    // that throws cannot leave the field stale.
    double tokens = lane.tokens;
    try {
      // A terminal head does not consume a send budget, but it does consume a work slot. Without the
      // second bound a continuously refilled lane of elided/superseded/expired items would keep this
      // loop inside one lane forever and delay the other lane's sendable decisions.
      while (sent < lane.burst && processed < LANE_WORK_BUDGET) {
        Pending head = queue.peek();
        if (head == null) {
          if (lane == coolLane) {
            yieldedCoolHead = null;
          }
          break;
        }
        // Whenever this lane cannot send the head, settle the terminal in-memory outcomes first. A COOL
        // that yields to a newer HOT must still shed an obsolete or expired head; otherwise it blocks
        // every decision behind it even though it would never take the wire.
        boolean yieldToHot = lane == coolLane && tokens >= 1.0d && head != yieldedCoolHead && hasNewerHotPending(head);
        if (yieldToHot) {
          yieldedCoolHead = head;
        }
        if (tokens < 1.0d || yieldToHot) {
          if (!discardHeadIfTerminal(queue, head)) {
            break;
          }
          // The head left the queue; let the next cool head have its own first-round yield.
          if (head == yieldedCoolHead) {
            yieldedCoolHead = null;
          }
          processed++;
          continue;
        }
        Pending pending = queue.poll();
        if (pending == null) {
          // A shutdown withdrawal took the head between the peek and the poll.
          if (head == yieldedCoolHead) {
            yieldedCoolHead = null;
          }
          break;
        }
        if (head == yieldedCoolHead) {
          yieldedCoolHead = null;
        }
        processed++;
        if (!pending.claim()) {
          continue;
        }
        WorkerBroadcaster.Delivery delivery = null;
        try {
          // Two checks the funnel cannot make: a newer HOT may still be queued and not yet accepted, and
          // the registry may have moved past this promotion since it was submitted. Both apply to HOT
          // only; a COOL is corrected only by the accepted record, because a queued HOT may still fail.
          delivery = deliver(pending);
          applyOutcome(pending, delivery);
          if (delivery == WorkerBroadcaster.Delivery.SENT) {
            tokens -= 1.0d;
            sent++;
          } else if (delivery == WorkerBroadcaster.Delivery.FAILED) {
            // applyOutcome set the round's global failure flag; stop before touching the HOT lane.
            break;
          }
        } catch (RuntimeException e) {
          // The item was claimed but no delivery outcome was reached: charge the missing ledger entry and
          // return its HOT seat before the drain's outer catch decides whether the thread can continue. A
          // failure after the funnel returned an outcome must not count a second outcome or roll back a
          // delivered promotion.
          if (delivery == null) {
            metrics.countDroppedDecision(pending.type());
            logDeliveryLoss();
            rollbackHot(pending);
          }
          throw e;
        } finally {
          forget(pending);
        }
      }
    } finally {
      lane.tokens = tokens;
    }
    return new RoundResult(sent, processed);
  }

  /**
   * Classifies and delivers a claimed item. A HOT superseded before the funnel is terminal without a
   * send: the newer transition already replaced or removed its registry entry, so the identity-guarded
   * abort is a no-op and needs no accepted-record read. Every outcome, including {@code FAILED}, is
   * charged by {@link #applyOutcome(Pending, WorkerBroadcaster.Delivery)} and its single log line,
   * so this method neither counts nor logs one.
   */
  private WorkerBroadcaster.Delivery deliver(Pending pending) {
    if (supersededHot(pending)) {
      metrics.countSupersededDecision(pending.type());
      return WorkerBroadcaster.Delivery.SUPERSEDED;
    }
    return broadcaster.send(pending.decision, pending.expiresAtMs);
  }

  /**
   * Applies one delivery outcome: the meters, the delivery-loss line, the HOT rollback and the
   * global send-failure flag that stops the round. SENT and ELIDED leave the seat in place.
   *
   * <p>A HOT that ends {@code SUPERSEDED} before the wire leaves the lifecycle cold when the key
   * has no accepted record. That outcome records a cold accepted state, so the lifecycle's later
   * COOL is elided instead of demoting a key the cluster was never told was hot. A failed or
   * expired HOT does not record one: its COOL is still sent, because the cluster may hold a state
   * from an earlier lifecycle whose COOL was lost. Any cold record an earlier superseded HOT
   * installed is cleared first, so it cannot elide that corrective COOL.
   */
  private void applyOutcome(Pending pending, WorkerBroadcaster.Delivery outcome) {
    switch (outcome) {
      // ELIDED: the funnel already holds this exact version, or the COOL is already cold. Neither
      // outcome is an undelivered promotion, so the seat stays.
      case SENT, ELIDED -> {
      }
      case FAILED -> {
        logDeliveryLoss();
        // The funnel rejected this version before the send, so it was never accepted; the identity-
        // guarded abort is the only rollback needed. A cold record left by an older superseded HOT
        // must not survive: it would elide the COOL this failure requires.
        abort(pending);
        if (pending.isHot()) {
          broadcaster.forgetCold(pending.key(), pending.version());
        }
        sendFailed = true;
      }
      case EXPIRED -> {
        metrics.countExpiredDecision(pending.type());
        logDeliveryLoss();
        // Classified as sendable before the age test, so no accepted record covers this version: the
        // unguarded abort is exact. The cold record is cleared for the same reason as FAILED.
        abort(pending);
        if (pending.isHot()) {
          broadcaster.forgetCold(pending.key(), pending.version());
        }
      }
      case SUPERSEDED -> {
        // A newer decision moved past this one after the pop, or a pre-funnel check dropped it before
        // the wire. The abort is identity-guarded either way. A cold record from an earlier superseded
        // HOT must not outlive this one: if the newer decision is a COOL it would elide that corrective
        // demotion, and if it is a HOT the newer outcome owns the record from here.
        abort(pending);
        if (pending.isHot()) {
          broadcaster.forgetCold(pending.key(), pending.version());
          if (!supersededByPendingCool(pending)) {
            broadcaster.rememberCold(pending.key());
          }
        }
      }
    }
  }

  /**
   * Whether the cool lane should yield this round to a newer HOT that is still pending.
   *
   * <p>
   * The newest pending decision for the key must be a HOT that outranks the head, and the HOT lane must
   * have a token. The HOT is then accepted in the same drain round and the next pass classifies the
   * demotion as superseded. If the HOT expires or fails, its map entry disappears and the COOL is sent
   * on a later round as the correction. A head yields at most once: if the newer HOT is deeper than one
   * hot-lane burst, the demotion is sent on the following round so unrelated keys keep progressing.
   *
   * @param head the cool lane's head
   * @return {@code true} when the head must wait for the HOT lane
   */
  private boolean hasNewerHotPending(Pending head) {
    if (!head.superseded()) {
      return false;
    }
    Pending latest = queued.get(head.key());
    return (latest != null && latest.isHot() && latest.version() > head.version() && hotLane.tokens >= 1.0d);
  }

  /**
   * Removes a head that is already obsolete or expired while the queue has no token. The checks are
   * terminal in-memory outcomes and cost no send budget; one is the queue's own hand-off state (a HOT
   * with a newer pending decision or a released promotion), the other is the funnel's own state machine,
   * so a duplicate, a superseded version, a redundant COOL and a debounced same-type decision leave the
   * head exactly as they would leave a send. The age bound is applied by the same funnel pass, after the
   * state machine: an obsolete head is never reported expired.
   *
   * <p>
   * A COOL is only terminal through the funnel: an older queued HOT never suppresses a demotion, because
   * that HOT may still fail. The COOL leaves the head as soon as the funnel's accepted record covers it —
   * after the newer HOT is accepted, in the next pass of the same drain loop. Blocking here costs no
   * send: the lane's token gates every COOL behind the head equally.
   *
   * @param queue the queue to inspect
   * @return {@code true} when the loop should re-check the queue, {@code false} when the head is still
   *         sendable or the queue is empty
   */
  private boolean discardHeadIfTerminal(ArrayBlockingQueue<Pending> queue, Pending head) {
    // A HOT behind a newer pending decision, or behind a promotion the registry has already released,
    // is terminal without an accepted-record read: the newer transition already replaced or removed
    // the entry this HOT created, so the identity-guarded abort is a no-op. A COOL never coalesces
    // this way; only the funnel's state machine may drop a demotion before the send.
    boolean superseded = supersededHot(head);
    WorkerBroadcaster.Verdict verdict = null;
    if (!superseded) {
      verdict = broadcaster.classify(head.decision, head.expiresAtMs);
      if (verdict == null) {
        return false;
      }
    }

    if (!queue.remove(head) || !head.claim()) {
      // Shutdown withdrew this item between the peek and the remove; re-read the queue.
      return true;
    }
    try {
      if (superseded) {
        metrics.countSupersededDecision(head.type());
        applyOutcome(head, WorkerBroadcaster.Delivery.SUPERSEDED);
      } else {
        applyOutcome(head, broadcaster.commit(verdict, head.decision));
      }
    } finally {
      forget(head);
    }
    return true;
  }

  /**
   * Whether the key's newest pending decision is a COOL that displaced this HOT. Such a COOL is the
   * corrective demotion the supersession requires, so the HOT must not install the cold record that
   * would elide it.
   *
   * @param hot the superseded HOT
   * @return {@code true} when a newer COOL is still pending for the key
   */
  private boolean supersededByPendingCool(Pending hot) {
    Pending latest = queued.get(hot.key());
    return latest != null && !latest.isHot() && latest.version() > hot.version();
  }

  /**
   * Whether a HOT is obsolete before the funnel: a newer decision for the same key is already queued or
   * in flight, or the registry no longer holds the promotion. Such a decision describes the state the
   * Worker holds now, so an older <b>HOT</b> is obsolete before it reaches the broadcaster.
   *
   * <p>
   * The registry probe closes the stalled-submit window: a report thread can stall between the transition
   * that allocated the version and this submission, and the key can cool — or cool and re-promote — in
   * between. The newer decision's own delivery may be refused, expired or failed, so neither
   * {@link #pending} nor the accepted record is guaranteed to remember it; the registry's entry is
   * the authority on whether the promotion still stands.
   *
   * <p>
   * A COOL is never suppressed this way: the newer HOT may not reach the broker, and the COOL is what
   * corrects the cluster if it does not.
   *
   * <p>
   * The token-starved head also consults the funnel's in-memory state machine; see
   * {@link #discardHeadIfTerminal}.
   *
   * @param pending the decision to test
   * @return {@code true} when an older HOT must not reach the funnel
   */
  private boolean supersededHot(Pending pending) {
    if (!pending.isHot()) {
      return false;
    }
    return (pending.superseded() || !probe.isCurrent(pending.key(), pending.version()));
  }

  /**
   * Drops the key's pending entry only when it still belongs to this decision. A superseded item was
   * displaced by a newer one, so the map does not point at it and the removal cannot succeed; skipping it
   * saves the bin lock.
   */
  private void forget(Pending pending) {
    if (!pending.superseded()) {
      queued.remove(pending.key(), pending);
    }
  }

  /**
   * Returns a HOT seat for a decision dropped before the funnel, after proving its exact version was not
   * already accepted.
   *
   * <p>
   * A duplicate submission whose exact version already reached the funnel describes a promotion the
   * cluster may still hold hot; the abort would remove a live seat. A strictly newer accepted version
   * does not short-circuit: the transition that superseded this HOT owns the seat and the
   * identity-guarded abort is a no-op for it.
   *
   * @param pending the dropped decision
   */
  private void rollbackHot(Pending pending) {
    if (!pending.isHot()) {
      return;
    }
    if (broadcaster.acceptedVersion(pending.key()) == pending.version()) {
      return;
    }
    abort(pending);
  }

  private void abort(Pending pending) {
    if (pending.isHot()) {
      abort(pending.key(), pending.version());
    }
  }

  private void abort(String key, long version) {
    try {
      rollback.abort(key, version);
    } catch (RuntimeException e) {
      log.error("Failed to roll back an undelivered HOT promotion: key={}, version={}", key, version, e);
    }
  }

  /**
   * Counts one decision the dispatcher refused or lost before the funnel, and reports it.
   *
   * @param type the decision type
   */
  private void drop(ZetaDecision.DecisionType type) {
    metrics.countDroppedDecision(type);
    logDeliveryLoss();
  }

  /**
   * Refills both lanes from the elapsed monotonic time; the drain thread is the only caller.
   *
   * <p>No tokens are created after shutdown: the shutdown contract spends the budget available at
   * that instant, so a slow broker must not be kept alive by a bucket that keeps refilling.
   */
  private void refill() {
    if (!running) {
      return;
    }
    long now = System.nanoTime();
    double elapsedSeconds = (now - lastRefillNanos) / 1_000_000_000.0d;
    if (elapsedSeconds <= 0) {
      return;
    }

    // The bucket holds one second of budget, or the configured per-round burst when that is larger:
    // the burst cap must be reachable, and the queue capacity remains the hard bound.
    hotLane.refill(elapsedSeconds);
    coolLane.refill(elapsedSeconds);
    lastRefillNanos = now;
  }

  /**
   * Counts and rolls back everything still queued after the drain thread has exited. COOL first, then
   * HOT, matching the live drain's order and the seat ledger.
   */
  private void drainRemaining() {
    drainQueue(coolLane.queue);
    drainQueue(hotLane.queue);
  }

  /**
   * Empties one queue after shutdown, taking ownership of each item so a submitter that withdrew it
   * concurrently does not get a second terminal outcome.
   *
   * @param queue the lane queue to empty
   */
  private void drainQueue(ArrayBlockingQueue<Pending> queue) {
    Pending pending;
    while ((pending = queue.poll()) != null) {
      // A submit that lost the shutdown race withdrew the item and counted the drop itself; it also
      // aborted the promotion synchronously, so this drain must not count or roll back twice.
      if (!pending.claim()) {
        continue;
      }
      try {
        drop(pending.type());
        rollbackHot(pending);
      } finally {
        forget(pending);
      }
    }
  }

  /**
   * Reports a decision that was not delivered, aggregating the cause classes in
   * one DEBUG line; the per-type drop meter carries the volume.
   */
  private void logDeliveryLoss() {
    log.debug(
      "Failed to deliver decision(s) (send failures, saturation drops and expired decisions; " +
        "fire-and-forget, no retry)"
    );
  }

  /** @return pending HOT decisions */
  public int hotDepth() {
    return hotLane.depth();
  }

  /** @return pending COOL decisions */
  public int coolDepth() {
    return coolLane.depth();
  }

  /**
   * Stops the dispatcher at context shutdown. Decisions already queued are sent with the tokens available
   * at that instant; anything left is counted as dropped and its HOT seat is returned. Submissions after
   * shutdown are dropped and counted.
   *
   * <p>
   * Shutdown never waits for the token buckets to refill: the join timeout is for a broker call that does
   * not return, not for rate limiting. The drain is woken immediately instead of waiting for its poll
   * timeout; if it is still alive after the bounded join, it is interrupted and given a second bounded
   * join. A blocking AMQP call can ignore the interrupt, so the final state is reported rather than
   * claimed impossible.
   */
  @PreDestroy
  public void shutdown() {
    running = false;
    pendingSignal.release();
    boolean interrupted = false;
    try {
      drainThread.join(5_000L);
      if (drainThread.isAlive()) {
        drainThread.interrupt();
        drainThread.join(1_000L);
      }
    } catch (InterruptedException e) {
      interrupted = true;
      drainThread.interrupt();
    }
    if (drainThread.isAlive()) {
      log.warn(
        "Decision drain thread did not stop within the shutdown joins; the broker call is blocking the interrupt"
      );
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }
}
