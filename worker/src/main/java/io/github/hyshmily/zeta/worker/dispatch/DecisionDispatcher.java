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
import org.springframework.context.SmartLifecycle;

/**
 * The single submission point for Worker decisions: one HOT queue, one COOL queue, and a drain thread
 * that rate-limits both.
 *
 * <p>
 * A COOL releases a seat and a HOT spends one, so the two decisions get separate queues and token
 * buckets: a mass-heat wave of promotions must not delay the demotions that make room for them. The
 * drain sends COOL first, then HOT, each capped by its per-round burst; a submission only offers to a
 * bounded queue and never waits for a send. A COOL already superseded by a newer HOT yields one round
 * when the HOT lane can send, so the next pass drops the obsolete demotion in memory instead of
 * demoting and re-promoting every App.
 *
 * <p>
 * Newer decisions make older ones obsolete through the older item's own hand-off word. An older HOT
 * is dropped before the offer when a newer decision is already pending, and before the funnel when its
 * registry promotion is gone; an older COOL is always retained as the fallback demotion and is
 * suppressed only by the broadcaster's accepted record. A decision older than
 * {@code max-decision-age-ms} is dropped at the funnel after the in-memory state machine proves it
 * would otherwise be sent.
 *
 * <p>
 * Delivery is fire-and-forget: a decision is sent once and never retried. A HOT whose send call never
 * succeeds returns its seat through {@link Rollback}, identity-guarded by the decision version so a
 * later promotion of the same key is never undone. A refused submission returns {@code false} so the
 * caller aborts its own promotion; a submission dropped because a newer decision is already pending
 * aborts internally and returns {@code true}.
 *
 * <p>
 * design decisions: decision delivery
 */
@Slf4j
@Internal
public class DecisionDispatcher implements SmartLifecycle {

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
   * {@link #QUEUED} records the successful offer; {@link #OWNED} records that exactly one thread owns
   * the item's terminal outcome. Every lifecycle bit is disjoint from the flags below, so an
   * ownership tryClaim and a supersede mark never invalidate each other.
   */
  private static final int UNQUEUED = 0;
  private static final int QUEUED = 1;
  private static final int OWNED = 1 << 1;

  /**
   * Set when a newer decision for the key is already published: on the map value it displaced, or on
   * an older COOL queued behind a newer item. The drain skips the identity removal for such an item
   * because the map points at, or past, a different item. It is also set by
   * {@link Pending#drop()} on an older HOT that is never offered, which is how
   * {@link #submit} recognises that drop without a second result channel.
   */
  private static final int SUPERSEDED_BY_PENDING = 1 << 2;

  /**
   * Set by the first round in which a cool head yields to a newer pending HOT. The bit lives on the
   * item, so a head that has yielded once sends on the next round and the dispatcher keeps no
   * reference to a processed item.
   */
  private static final int YIELDED_ONCE = 1 << 3;

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
   * All hand-off facts — not yet offered, queued, owned, displaced, yielded — live in one
   * {@code volatile int} updated through {@link #STATE}, so the item stays one small allocation and
   * the submitter and the drain never maintain two overlapping status fields.
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
     * lifecycle flag, so a concurrent {@link #supersede()} cannot invalidate this tryClaim and force a
     * re-read.
     */
    boolean tryClaim() {
      int previous = (int) STATE.getAndBitwiseOr(this, OWNED);
      return (previous & (QUEUED | OWNED)) == QUEUED;
    }

    /**
     * Marks this item as displaced in {@link #queued} by a newer decision.
     */
    void supersede() {
      // The bit is disjoint from QUEUED/OWNED, so the OR cannot disturb a concurrent tryClaim.
      STATE.getAndBitwiseOr(this, SUPERSEDED_BY_PENDING);
    }

    /**
     * Marks this item's one allowed yield to a newer pending HOT.
     *
     * <p>The bit is disjoint from every lifecycle flag, so one atomic OR both tests and sets it
     * without a retry loop. A head that has already yielded returns {@code false} and is sent on
     * this round instead of yielding again.
     *
     * @return {@code true} when this call set the bit; {@code false} when the head had already
     *         yielded
     */
    boolean tryMarkYielded() {
      return ((int) STATE.getAndBitwiseOr(this, YIELDED_ONCE) & YIELDED_ONCE) == 0;
    }

    /**
     * Marks an older HOT dropped before its offer because a newer decision is pending. The QUEUED
     * bit stays clear, so {@link #dropped()} can tell this path from an item that was actually
     * queued.
     */
    void drop() {
      state = SUPERSEDED_BY_PENDING;
    }

    /**
     * @return whether this HOT was dropped before the offer because a newer decision was pending
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
    boolean hot() {
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
      long nanosToExpiry = TimeUnit.MILLISECONDS.toNanos(saturatedRemainingMs(head.expiresAtMs, nowMs));
      return Math.min(nanosToToken, nanosToExpiry);
    }

    /**
     * Remaining milliseconds to the head's expiry, saturated at both ends. A plain subtraction
     * overflows when the expiry is the {@link Long#MAX_VALUE} sentinel and the monotonic clock is
     * negative, turning an effectively unbounded wait into a non-positive one.
     */
    private static long saturatedRemainingMs(long expiresAtMs, long nowMs) {
      long remaining = expiresAtMs - nowMs;
      if (((expiresAtMs ^ nowMs) & (expiresAtMs ^ remaining)) < 0L) {
        return expiresAtMs > nowMs ? Long.MAX_VALUE : 0L;
      }
      return Math.max(0L, remaining);
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
   * Newest queued or in-flight decision per key; the value is the queue item itself, so a newer
   * submission marks the displaced item through its own state word instead of boxing a version or
   * forcing the drain to hash the key again. Every terminal outcome removes the entry when it still
   * maps to that item, so the map is bounded by the queues. The offer and the publication are one
   * per-key step (see {@link #submit}).
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
   * Submits one HOT promotion produced by the registry. A refused enqueue and an unexpected
   * failure both return the seat before this call completes; the caller owns no rollback.
   *
   * @param promoted the promotion to deliver
   */
  public void submit(HE promoted) {
    boolean submitted;
    try {
      submitted = submit(promoted.decision());
    } catch (RuntimeException | Error e) {
      abort(promoted.key(), promoted.version());
      throw e;
    }
    if (!submitted) {
      abort(promoted.key(), promoted.version());
    }
  }

  /**
   * Submits one decision for delivery.
   *
   * @param decision the HOT or COOL decision; {@code NONE} and {@code null} are ignored
   * @return {@code true} when the caller must not undo the promotion: the decision was queued,
   *         ignored as {@code NONE}, or this call dropped an older decision because a newer pending
   *         decision superseded it; {@code false} when the decision was dropped and the caller owns
   *         the promotion rollback
   * @throws IllegalArgumentException when a HOT or COOL decision carries no causal version
   */
  @SuppressWarnings("ResultOfMethodCallIgnored")
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

    Pending current = new Pending(decision, expiresAt(TimeSource.monotonicMillis()));
    // The offer and the publication are one per-key step: the bin lock makes the newest decision visible
    // before a newer one can replace it, and a refused offer leaves the previous record untouched.
    queued.compute(decision.cacheKey(), (key, previous) -> offer(current, previous, queue));
    if (current.dropped()) {
      // A strictly newer decision already ran, so the entry this HOT created is gone or replaced and
      // the identity-guarded abort is a no-op. This HOT never entered a queue.
      abort(current);
      metrics.countSupersededDecision(type);
      return true;
    }

    if (current.refused()) {
      drop(type);
      return false;
    }
    // The drain owns the terminal outcome from here. Its identity-matched remove cannot pass the bin lock
    // this compute held while offering, so the record is either still present (the drain will remove it)
    // or already replaced by a newer submission. Shutdown may have flipped running between the check and
    // the offer: either this thread withdraws the item and reports the drop, or the drain already claimed
    // it and owns the terminal outcome, including the HOT rollback.
    if (!running && current.tryClaim()) {
      queue.remove(current);
      forget(current);
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
    return expiresAt < nowMs || expiresAt == Long.MAX_VALUE ? Long.MAX_VALUE : expiresAt + 1L;
  }

  /**
   * The per-key publication step, run under the map's bin lock: decides which item the map keeps and
   * offers the new one when it is still needed.
   *
   * <p>
   * A newer pending decision drops an older HOT before the offer. An older COOL is queued behind the
   * newer item without taking ownership of the map: it is the corrective demotion if the newer
   * decision fails. Equal versions are duplicates and both may be enqueued, but the map tracks the
   * copy offered last so a newer decision still finds a predecessor after the first one settles.
   *
   * @param current  the item to offer
   * @param previous the map's current item, or {@code null}
   * @param queue    the lane queue {@code current} belongs to
   * @return the item the map should keep
   */
  private Pending offer(Pending current, @Nullable Pending previous, ArrayBlockingQueue<Pending> queue) {
    if (!running) {
      return previous;
    }

    if (previous != null && previous.version() > current.version()) {
      if (current.hot()) {
        // The newer decision owns the key's delivery; this older HOT changes nothing and must not
        // consume a queue slot.
        current.drop();
      } else {
        // An older COOL is the fallback demotion if the newer decision fails. It is queued behind
        // the newer item without taking ownership of the map; the drain skips its map removal.
        current.enqueue(queue, SUPERSEDED_BY_PENDING);
      }
      return previous;
    }

    if (!current.enqueue(queue, 0)) {
      return previous;
    }

    // Every lifecycle bit is decided under the bin lock and published by this one plain write. The
    // item is not visible to the drain before the offer, so no atomic OR is needed: the drain either
    // polls the finished word or does not see the item at all.
    if (previous != null && previous.version() < current.version()) {
      previous.supersede();
    }

    // An equal version is a duplicate offered while the first copy is still queued: both may be
    // enqueued, and the map tracks the copy offered last. The earlier copy is not marked; the cool
    // yield reads the map directly, so a newer HOT behind the duplicate still suppresses the head.
    return current;
  }

  /**
   * The single delivery loop: refill both buckets, drain COOL before HOT, and block on the submission
   * semaphore when both queues are empty. A failed send backs off instead of spinning, and shutdown
   * breaks the loop only after the current round so the final drain owns the remainder.
   */
  @SuppressWarnings("all")
  private void drainLoop() {
    lastRefillNanos = System.nanoTime();
    hotLane.tokens = 0.0d;
    coolLane.tokens = 0.0d;
    try {
      while (running || !queuesEmpty()) {
        try {
          RoundResult round = drainRound();
          if (sendFailed) {
            if (!running) {
              break;
            }
            // Keep the broker-failure retry pace; a failed decision never consumes a token. Drop
            // the permits accumulated while the broker was failing so a sustained outage cannot
            // grow an unbounded semaphore count that the next empty round has to drain.
            pendingSignal.drainPermits();
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
          log.error("Decision drain round failed; retrying after {}ms", FAILURE_BACKOFF_MS, e);
          if (!running) {
            break;
          }
          pendingSignal.drainPermits();
          Thread.sleep(FAILURE_BACKOFF_MS);
        } catch (Error e) {
          log.error("Decision drain thread terminated by Error; stopping dispatcher", e);
          throw e;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      // The drain thread is the only consumer. If it exits for any reason (including an Error),
      // submissions must stop believing the dispatcher is alive: otherwise they keep filling
      // queues nobody drains and leak HOT seats until saturation.
      running = false;
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
  // tryAcquire's result is intentionally ignored: waking and timing out both continue the round.
  @SuppressWarnings("all")
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
      // release was just discarded, so re-check before sleeping.
      if (hasReadyWork()) {
        return true;
      }
      // No usable token: wait for the next refill or the head's age bound on the semaphore, so a
      // COOL arriving behind a HOT-starved round wakes the drain immediately instead of waiting out a
      // pause nothing can signal. An expired decision still returns its HOT seat on time.
      pendingSignal.tryAcquire(nanosToNextEvent(), TimeUnit.NANOSECONDS);
      return true;
    }

    // Clear permits left by a suppressed round, then re-check: a submission between the emptiness
    // check and here is visible in the queue, so draining its permit cannot lose the wakeup.
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
   * funnel would settle in memory (superseded, expired or elided) leaves without a send budget. A
   * cool head with a token always counts: the next round either sends it or yields once to a newer
   * HOT that the hot lane can send in the same round.
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
   * Nanoseconds until the drain has something to do without a new submission: the first starved lane
   * receives a token, or a queued head reaches {@code maxAgeMs} and becomes droppable. Both buckets
   * refill from the same elapsed time, so the earlier refill unblocks the round.
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
   * With a token available the drain polls and hands the decision to the funnel. The pending map is
   * the one check the funnel cannot make: it suppresses an older HOT behind a newer queued decision,
   * while a COOL is corrected only by the accepted record. The funnel applies its in-memory state
   * machine before the queue's age bound, with one accepted-decision read and one clock sample, so a
   * decision that cannot change the cluster's state is never reported expired.
   *
   * <p>
   * A COOL with a newer HOT still pending yields only when it is still sendable: the HOT is accepted
   * first, and the next pass drops the demotion as superseded instead of demoting and re-promoting
   * every App. The item's own {@code YIELDED_ONCE} bounds the yield to one round per head.
   *
   * @param lane the lane to drain
   * @return the decisions sent and the items whose queue residency ended
   */
  private RoundResult drain(Lane lane) {
    ArrayBlockingQueue<Pending> queue = lane.queue;
    int sent = 0;
    int processed = 0;
    double tokens = lane.tokens;
    try {
      // A terminal head does not consume a send budget, but it does consume a work slot. Without the
      // second bound a continuously refilled lane of elided/superseded/expired items would keep this
      // loop inside one lane forever and delay the other lane's sendable decisions.
      while (sent < lane.burst && processed < LANE_WORK_BUDGET) {
        Pending head = queue.peek();
        if (head == null) {
          break;
        }
        // Whenever this lane cannot send the head, settle the terminal in-memory outcomes first. A COOL
        // that yields to a newer HOT must still shed an obsolete or expired head; otherwise it blocks
        // every decision behind it even though it would never take the wire.
        boolean yieldToHot = lane == coolLane && tokens >= 1.0d && hasNewerHotPending(head) && head.tryMarkYielded();
        if (tokens < 1.0d || yieldToHot) {
          if (!discardHeadIfTerminal(queue, head)) {
            break;
          }
          processed++;
          continue;
        }
        Pending pending = queue.poll();
        if (pending == null) {
          break;
        }
        processed++;
        if (!pending.tryClaim()) {
          continue;
        }
        WorkerBroadcaster.Delivery delivery = null;
        try {
          // Two checks the funnel cannot make: a newer HOT may still be queued and not yet accepted,
          // and the registry may have moved past this promotion. Both apply to HOT only.
          delivery = deliver(pending);
          // Spend the token before the outcome bookkeeping: the publish already returned, so a metrics
          // or rollback failure must not refund a send that reached the wire.
          if (delivery == WorkerBroadcaster.Delivery.SENT) {
            tokens -= 1.0d;
            sent++;
          }
          applyOutcome(pending, delivery);
          if (delivery == WorkerBroadcaster.Delivery.FAILED) {
            break;
          }
        } catch (RuntimeException | Error e) {
          if (delivery == null) {
            rollbackHot(pending);
            metrics.countDroppedDecision(pending.type());
            logDeliveryLoss();
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
   * abort is a no-op and needs no accepted-record read. This pre-funnel {@code SUPERSEDED} is charged
   * here; the funnel charges the outcomes it returns, and
   * {@link #applyOutcome(Pending, WorkerBroadcaster.Delivery)} charges only {@code EXPIRED} plus the
   * seat rollback.
   */
  private WorkerBroadcaster.Delivery deliver(Pending pending) {
    if (supersededHot(pending)) {
      metrics.countSupersededDecision(pending.type());
      return WorkerBroadcaster.Delivery.SUPERSEDED;
    }
    return broadcaster.send(pending.decision, pending.expiresAtMs);
  }

  /**
   * Applies one delivery outcome: the {@code EXPIRED} meter, the delivery-loss line, the HOT
   * rollback and the global send-failure flag that stops the round. SENT and ELIDED leave the seat
   * in place. No outcome touches the broadcaster's accepted record: the record is wire truth, and a
   * HOT that never reached the wire cannot change the cluster's state.
   */
  private void applyOutcome(Pending pending, WorkerBroadcaster.Delivery outcome) {
    switch (outcome) {
      case SENT, ELIDED -> {
      }
      case FAILED -> {
        logDeliveryLoss();
        abort(pending);
        sendFailed = true;
      }
      case EXPIRED -> {
        abort(pending);
        metrics.countExpiredDecision(pending.type());
        logDeliveryLoss();
      }
      case SUPERSEDED -> abort(pending);
    }
  }

  /**
   * Whether the cool lane should yield this round to a newer HOT that is still pending: the map's
   * newest item for the key is a HOT that outranks the head, and the hot lane can send.
   *
   * <p>
   * The map is the authority, not the head's supersede bit: an equal-version duplicate replaces the
   * map value without marking the copy it displaced, so the bit cannot prove that no newer HOT is
   * pending.
   *
   * @param head the cool lane's head
   * @return {@code true} when the head must wait for the HOT lane
   */
  private boolean hasNewerHotPending(Pending head) {
    Pending latest = queued.get(head.key());
    return latest != null && latest.hot() && latest.version() > head.version() && hotLane.tokens >= 1.0d;
  }

  /**
   * Removes a head that is already obsolete or expired while the lane has no token. The checks are
   * terminal in-memory outcomes and cost no send budget: one is the queue's own hand-off state, the
   * other the funnel's state machine and age bound, so an obsolete head is never reported expired.
   *
   * <p>
   * A COOL is only terminal through the funnel: an older queued HOT never suppresses a demotion,
   * because that HOT may still fail. Blocking here costs no send: the lane's token gates every COOL
   * behind the head equally.
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

    if (!queue.remove(head) || !head.tryClaim()) {
      // Shutdown withdrew this item between the peek and the remove; re-read the queue.
      return true;
    }
    try {
      if (superseded) {
        applyOutcome(head, WorkerBroadcaster.Delivery.SUPERSEDED);
        metrics.countSupersededDecision(head.type());
      } else {
        applyOutcome(head, broadcaster.commit(verdict, head.decision));
      }
    } finally {
      forget(head);
    }
    return true;
  }

  /**
   * Whether a HOT is obsolete before the funnel: a newer decision for the same key is already queued
   * or in flight, or the registry no longer holds the promotion.
   *
   * <p>
   * The registry probe closes the stalled-submit window: a report thread can stall between the
   * transition that allocated the version and this submission while the key cools — or cools and
   * re-promotes — and the newer decision's own delivery may be refused, expired or failed, so neither
   * {@link #queued} nor the accepted record is guaranteed to remember it. A COOL is never suppressed
   * this way: the newer HOT may not reach the broker, and the demotion is the correction.
   *
   * @param pending the decision to test
   * @return {@code true} when an older HOT must not reach the funnel
   */
  private boolean supersededHot(Pending pending) {
    if (!pending.hot()) {
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
    if (!pending.hot()) {
      return;
    }
    if (broadcaster.acceptedVersion(pending.key()) == pending.version()) {
      return;
    }
    abort(pending);
  }

  private void abort(Pending pending) {
    if (pending.hot()) {
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
      if (!pending.tryClaim()) {
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
   * Stops the dispatcher at context shutdown. Decisions already queued are sent with the tokens
   * available at that instant; anything left is counted as dropped and its HOT seat is returned.
   * Submissions after shutdown are dropped and counted.
   *
   * <p>
   * Shutdown never waits for the token buckets to refill: the join timeout is for a broker call that
   * does not return. If the drain is still alive after the bounded join it is interrupted and given a
   * second bounded join; a blocking AMQP call can ignore the interrupt, so the final state is
   * reported rather than claimed impossible.
   */
  /**
   * The drain thread is started by the constructor, so the lifecycle start has nothing to do.
   * Implemented so {@link #stop()} runs before singleton destruction: the broker connection
   * factory is then still open while the final drain spends its remaining budget.
   */
  @Override
  public void start() {
    // The constructor starts the drain thread.
  }

  @Override
  public void stop() {
    shutdown();
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  /**
   * Stops after the cooling scheduler ({@code MAX_VALUE - 10}) and after the listeners (which use
   * the default phase), so no COOL is submitted once the final drain has begun.
   */
  @Override
  public int getPhase() {
    return Integer.MAX_VALUE - 20;
  }

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
