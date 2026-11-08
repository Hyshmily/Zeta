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
package io.github.hyshmily.zeta.cache.cachesupport.circuitbreaker;

import static io.github.hyshmily.zeta.util.TimeSource.monotonicMillis;

import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.util.Internal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Sliding-window circuit breaker with type-filtered failure classification.
 *
 * <p>States: {@code CLOSED → OPEN} on failure rate, {@code OPEN → HALF_OPEN} after the
 * timeout, {@code HALF_OPEN → CLOSED} only after consecutive probe successes (any probe
 * failure reopens). The machine — state, episode generation, live probe quota, consecutive
 * successes, the episode clock and the probe-credit multiplier — is one immutable
 * {@link Epoch} swapped by a single compare-and-set, so no reader can observe a state
 * without the quota and the timer that belong to it, and every transition has exactly one
 * winner. A HALF_OPEN episode whose
 * probes stop settling is force-retired after {@code singleTestIntervalMs} so a hung probe
 * cannot pin the breaker.
 *
 * <p>The failure window is a clock-driven ring: a settlement stamps its bucket with the
 * aligned window index it belongs to and zeroes it when that index moved on, so the window
 * advances with traffic instead of with a timer. There is no background task, no
 * {@code start()}/{@code close()} lifecycle and no cross-window watermark to version, so a
 * stalled shared scheduler cannot freeze the trip decision.
 *
 * <p>Exception filtering and the asymmetric probe credit are specified in design
 * decisions: load protection — this class implements them, so change both together.
 */
@Slf4j
@Internal
public class CircuitBreakerImpl implements CircuitBreaker {

  /**
   * Maximum cause-chain depth for {@link #isIgnorableException}: a cyclic cause
   * chain (constructible via two mutually-linked throwables) would otherwise
   * spin forever on an application thread.
   */
  private static final int MAX_CAUSE_CHAIN_DEPTH = 32;

  /**
   * Width of one bucket counter. Two 20-bit counters and a 24-bit window index
   * share one word; a bucket saturates at {@value #COUNT_MASK} settlements,
   * far above any plausible per-slice volume, and cannot overflow into its
   * neighbour.
   */
  private static final int COUNT_BITS = 20;
  /** Bit shift of the failure counter within a packed bucket. */
  private static final int FAIL_SHIFT = COUNT_BITS;
  /** Bit shift of the window index within a packed bucket. */
  private static final int WINDOW_SHIFT = 2 * COUNT_BITS;
  /** Largest count a bucket can hold; also the field mask. */
  private static final long COUNT_MASK = (1L << COUNT_BITS) - 1;
  /** Mask covering the window index of a packed bucket. */
  private static final long WINDOW_MASK = (1L << (Long.SIZE - WINDOW_SHIFT)) - 1;

  /**
   * Asymmetric probe-quota credit for HALF_OPEN recovery, adapted from
   * RocksDB {@code db/write_controller.cc::SetupDelay} — but applied once per
   * <em>episode</em>, never once per probe: a failed episode multiplies the
   * quota by {@link #K_PROBE_PENALTY_RATIO}, a recovered one by
   * {@link #K_PROBE_RECOVER_RATIO}. Both are folded into the {@link Epoch} swap
   * that publishes the episode's outcome.
   *
   * <p>The ratio pair is deliberately not reciprocal: a failed episode costs
   * 20% while a clean one earns 10%, so a source whose recovery episodes fail
   * more than ~30% of the time converges to the floor — one probe at a time —
   * while a healthy one climbs back to full aggression. The credit only ever
   * TIGHTENS: {@code halfOpenMaxProbes} stays the hard ceiling (an operator knob
   * must not be exceeded by an internal multiplier).
   *
   * <p>Deliberately constants, not config: policy-type tuning ratios follow the
   * convention (design decisions: read path) (add-type thresholds are configuration,
   * strategy ratios are code).
   */
  private static final double K_PROBE_PENALTY_RATIO = 0.8;
  private static final double K_PROBE_RECOVER_RATIO = 1.1;
  private static final double PROBE_CREDIT_BASELINE = 1.0;
  private static final double PROBE_CREDIT_FLOOR = 0.25;

  private final ZetaProperties.CircuitBreaker config;

  /**
   * The whole breaker machine: state, episode, probe quota, streak, clock and probe credit.
   * One reference means a reader can never observe a state without the quota and timer that
   * belong to it, and every transition has exactly one winner.
   *
   * <p>Never recycles an instance — a CAS compares by identity, so reusing one would let a
   * read from an older epoch succeed against a newer one.
   */
  private final AtomicReference<Epoch> epoch = new AtomicReference<>(
    new Epoch(CircuitBreakerState.CLOSED, 0L, 0, 0, 0L, PROBE_CREDIT_BASELINE)
  );

  /**
   * Exception classes that must NOT trip the breaker, resolved at construction from
   * {@code excludeExceptions} ({@code includeExceptions} is the mirror array below).
   * Flat arrays, not sets: classification runs on a failure path that has already
   * paid for a stack trace, and an array walk allocates no iterator and touches no
   * hash table for the one-or-two entries a real filter list holds.
   *
   * <p>Resolved at construction: a lazy first-use resolve would let concurrent early
   * failures observe the still-empty filters and count as breaker failures. The list
   * is resolved once either way (live config changes never re-resolve), so
   * construction is the only race-free moment.
   */
  private final Class<?>[] excludeClasses;

  /** Mirror of {@link #excludeClasses} for the {@code includeExceptions} list. */
  private final Class<?>[] includeClasses;

  /**
   * The rolling window: one packed {@code [window | failures | successes]} word
   * per {@code slideMs} slice, rotated lazily by the clock (see the class comment).
   *
   * <p>One atomic read returns the window index and both counters together, so a
   * rotation cannot land between the failure sum and the success sum and
   * manufacture a rate that never existed. All concurrent settlements of the
   * live slice target one slot; the remaining slots are read-only until the
   * clock reaches them.
   */
  private final AtomicLongArray buckets;

  private final int bucketSize;

  private final long slideMs;

  /**
   * @param config circuit breaker configuration (buckets, thresholds, exception lists);
   *               read live on every decision so runtime toggles keep working (design decisions: load protection).
   */
  public CircuitBreakerImpl(ZetaProperties.CircuitBreaker config) {
    Assert.notNull(config, "config must not be null");
    Assert.isTrue(config.getWindowBuckets() > 0, "window-buckets (" + config.getWindowBuckets() + ") must be > 0");
    Assert.isTrue(
      config.getWindowTimeMs() >= config.getWindowBuckets(),
      "window-time-ms (" + config.getWindowTimeMs() + ") must be >= window-buckets (" + config.getWindowBuckets() + ")"
    );
    this.config = config;
    this.bucketSize = config.getWindowBuckets();
    this.buckets = new AtomicLongArray(bucketSize);
    // >= 1 guaranteed by the window-time >= window-buckets assertion above
    this.slideMs = config.getWindowTimeMs() / bucketSize;
    this.excludeClasses = resolveClasses(config.getExcludeExceptions());
    this.includeClasses = resolveClasses(config.getIncludeExceptions());
    // Every slot starts on the current window with zero counters, so the first
    // settlement in each slot is a same-window increment instead of a rotation
    // race against an uninitialised slot.
    resetBuckets();
  }

  /**
   * Explicit-stamp admission: the probe reservation travels in the returned
   * stamp instead of a thread-local slot, so one thread admitting many keys
   * (batch) or nested cache calls can never cross-consume probes.
   *
   * <p>CLOSED → allow all. OPEN → allow only after {@code singleTestIntervalMs}
   * has elapsed since opening (HALF_OPEN probe). HALF_OPEN → allow probes only
   * within the credit-scaled quota.
   *
   * @return {@link CircuitBreaker#STAMP_REJECTED} when denied,
   *         {@link CircuitBreaker#STAMP_CLOSED} on the CLOSED path,
   *         otherwise the HALF_OPEN probe generation (strictly positive)
   */
  @Override
  public long tryAcquireStamp() {
    if (!config.isEnabled()) {
      forceClosedWhenDisabled();
      return STAMP_CLOSED;
    }

    Epoch e = epoch.get();
    return switch (e.state) {
      case CLOSED -> STAMP_CLOSED;
      case OPEN -> admitFirstProbe(e);
      case HALF_OPEN -> admitProbe();
    };
  }

  /**
   * OPEN → HALF_OPEN: opens a fresh episode owning one probe slot (the
   * transitioner) and returns its generation. The cooldown check reads the same
   * snapshot it CASes on, so a timer that is one transition behind can never
   * admit a probe — and the CAS is the whole election, so a storm that all sees
   * an elapsed cooldown still opens exactly one episode.
   */
  private long admitFirstProbe(Epoch open) {
    long now = monotonicMillis();
    if (now - open.anchorMillis <= config.getSingleTestIntervalMs()) {
      return STAMP_REJECTED;
    }

    Epoch episode = open.startNew(now);
    if (!epoch.compareAndSet(open, episode)) {
      // Another thread opened the episode (or the state moved on): its admission stands.
      return STAMP_REJECTED;
    }
    if (config.isLogEnabled()) {
      log.info("CB HALF_OPEN probe");
    }
    return episode.generation;
  }

  /**
   * HALF_OPEN admission against the credit-scaled quota. Rejected surplus
   * threads never touch the counter, so their retreat cannot corrupt a
   * concurrently-reset episode. An episode whose probes stop settling (all
   * slots held by hung stamps) is force-retired after {@code
   * singleTestIntervalMs} without any activity: the fresh episode evicts the
   * hung stamps, whose later settlements are ignored by generation. Slow-but-alive
   * probes keep settling and keep the episode alive.
   */
  private long admitProbe() {
    for (;;) {
      Epoch e = epoch.get();
      if (e.state != CircuitBreakerState.HALF_OPEN) {
        return STAMP_REJECTED;
      }

      int maxProbes = Math.max(1, (int) (config.getHalfOpenMaxProbes() * e.probeCredit));
      if (e.inflight < maxProbes) {
        if (epoch.compareAndSet(e, e.slotAcquire())) {
          return e.generation;
        }
        continue;
      }

      long now = monotonicMillis();
      if (now - e.anchorMillis <= config.getSingleTestIntervalMs()) {
        return STAMP_REJECTED;
      }
      Epoch episode = e.startNew(now);
      if (epoch.compareAndSet(e, episode)) {
        if (config.isLogEnabled()) {
          log.info("CB HALF_OPEN episode reset (no probe settled within {} ms)", config.getSingleTestIntervalMs());
        }
        return episode.generation;
      }
    }
  }

  /**
   * Settle an explicit stamp as success. Only a CLOSED-path stamp counts into
   * the window — probe stamps and {@code STAMP_REJECTED} are refused, mirroring
   * {@link #onFailure(Throwable, long)}. A live probe releases its slot and may
   * close the breaker once the consecutive-success threshold is met.
   */
  @Override
  public void onSuccess(long stamp) {
    settle(stamp, true);
  }

  /**
   * Settle an explicit stamp as failure. Ignorable exceptions degrade to
   * success; a stale generation is ignored. A {@code null} cause skips the
   * filter and goes straight to the accounting.
   */
  @Override
  public void onFailure(Throwable error, long stamp) {
    settle(stamp, error != null && isIgnorableException(error));
  }

  /**
   * The one settlement entry for both outcomes. The stamp already encodes the admission
   * path: {@link #STAMP_CLOSED} is issued only while CLOSED, and a HALF_OPEN probe stamp is
   * strictly positive. A CLOSED-path settlement therefore records into the window only when
   * the breaker is still CLOSED; a probe stamp goes straight to {@link #probe}, whose single
   * {@code epoch} read performs the generation check.
   *
   * @param stamp   the admission stamp
   * @param success {@code true} for a success, {@code false} for a failure
   */
  private void settle(long stamp, boolean success) {
    if (!config.isEnabled() || stamp == STAMP_REJECTED) {
      return;
    }
    if (stamp > STAMP_CLOSED) {
      probe(stamp, success);
      return;
    }
    if (epoch.get().state == CircuitBreakerState.CLOSED) {
      // One clock sample for the settlement and the evaluation it triggers: a
      // rotation between the two would otherwise let the evaluation judge a window
      // the failure did not land in.
      long now = monotonicMillis();
      record(success, now);
      if (!success) {
        evaluateThreshold(now);
      }
    }
  }

  /**
   * Release an explicit stamp without an outcome (executor rejection before
   * any data-source call). Only a live-generation probe releases a slot.
   */
  @Override
  public void onAbandoned(long stamp) {
    if (!config.isEnabled() || stamp <= STAMP_CLOSED) {
      return;
    }

    for (;;) {
      Epoch e = epoch.get();
      if (
        e.state != CircuitBreakerState.HALF_OPEN ||
        e.generation != stamp ||
        e.inflight <= 0 ||
        epoch.compareAndSet(e, e.slotReleased(monotonicMillis()))
      ) {
        return;
      }
    }
  }

  /**
   * Returns true when the breaker is OPEN (fast-failing), skipping probes.
   */
  @Override
  public boolean isOpen() {
    return config.isEnabled() && epoch.get().state == CircuitBreakerState.OPEN;
  }

  /**
   * Returns true while the breaker is OPEN or in any HALF_OPEN episode. An admitted probe is
   * still allowed to run; the state is reported as degraded because a denied surplus probe or a
   * failed probe must degrade to the stale fallback. Only a CLOSED breaker is fully healthy.
   */
  @Override
  public boolean isDegraded() {
    return config.isEnabled() && epoch.get().state != CircuitBreakerState.CLOSED;
  }

  /**
   * Settles a live probe: the slot release, the outcome and the resulting state
   * are one CAS. A failed probe therefore reopens in the same atomic step that
   * retires the episode, leaving no window in which a retired episode still holds
   * free slots for a "ghost" probe to occupy — the failure that is supposed to
   * tighten the breaker must not admit traffic on its way out.
   *
   * <p>A success that crosses the recovery threshold closes in that same CAS:
   * the window is cleared before publishing CLOSED, so an evaluation never sees
   * CLOSED with the pre-recovery failure surplus. No transient HALF_OPEN with a
   * released slot is published between the threshold check and the close.
   *
   * @param stamp   the generation the probe was admitted under
   * @param success {@code true} to count a consecutive success, {@code false} to reopen
   */
  private void probe(long stamp, boolean success) {
    // A non-positive threshold degenerates to "one success closes"; clamping keeps
    // the crossing test meaningful instead of disabling recovery.
    int threshold = Math.max(1, config.getConsecutiveSuccessThreshold());
    for (;;) {
      Epoch e = epoch.get();
      if (e.state != CircuitBreakerState.HALF_OPEN || e.generation != stamp || e.inflight <= 0) {
        // A straggler from a retired episode, or a stamp this caller never held.
        return;
      }

      if (success && e.consecutiveSuccesses + 1 >= threshold) {
        resetBuckets();
        if (epoch.compareAndSet(e, e.closed(true))) {
          if (config.isLogEnabled()) {
            log.info("CB CLOSED after {} consecutive successes", e.consecutiveSuccesses + 1);
          }
          return;
        }
        continue;
      }

      long now = monotonicMillis();
      Epoch settled = success ? e.probeSucceeded(now) : e.probeFailed(now);
      if (!epoch.compareAndSet(e, settled)) {
        continue;
      }

      if (!success && config.isLogEnabled()) {
        log.info("CB HALF_OPEN -> OPEN (probe failed, probeCredit={})", settled.probeCredit);
      }
      return;
    }
  }

  /**
   * Forces the breaker CLOSED while disabled, retiring any live episode and
   * clearing the window — otherwise stale failure buckets would re-trip OPEN
   * on every slide until they slid out, flapping the breaker and its log while
   * disabled. The window is cleared before CLOSED is published (the recovery
   * path's ordering) and the clearance is idempotent, so the retry after a lost
   * CAS is harmless. The CAS makes one concurrent caller the only cleaner.
   */
  private void forceClosedWhenDisabled() {
    for (;;) {
      Epoch e = epoch.get();
      if (e.state == CircuitBreakerState.CLOSED) {
        return;
      }

      resetBuckets();
      if (epoch.compareAndSet(e, e.closed(false))) {
        return;
      }
    }
  }

  /**
   * Settles one CLOSED-path outcome into the bucket owning {@code now}.
   *
   * <p>The window index and both counters are one word, so a rotation is a single
   * compare-and-set: an increment either lands in the slice it sampled or is
   * discarded, never erased by a concurrent rotation. The index is written
   * together with the zeroed counters, so a reader that observes the new index
   * also observes the empty counters.
   *
   * <p>A slot advanced by at most one ring is an out-of-order settlement and is
   * discarded; a slot behind by any larger distance is the previous slice or a
   * bucket idle past half the window-id range and is rotated to the caller's
   * slice. The residual ambiguity is the wrap: a bucket within one ring width
   * before {@code 2^24} slices is discarded for at most that many settlements,
   * and a bucket exactly {@code 2^24} slices old contributes its stale counts
   * once before the next settlement rotates it.
   *
   * @param success {@code true} for a success, {@code false} for a failure
   * @param now     the caller's clock sample — the same one its evaluation uses
   */
  private void record(boolean success, long now) {
    long windowIndex = now / slideMs;
    long window = windowIndex & WINDOW_MASK;
    int index = (int) Math.floorMod(windowIndex, (long) bucketSize);
    int shift = success ? 0 : FAIL_SHIFT;
    long increment = 1L << shift;

    for (;;) {
      long raw = buckets.get(index);
      long stored = raw >>> WINDOW_SHIFT;
      long forward = (stored - windowIndex) & WINDOW_MASK;
      long next;
      if (forward == 0L) {
        if (((raw >>> shift) & COUNT_MASK) >= COUNT_MASK) {
          return;
        }
        next = raw + increment;
      } else if (forward <= bucketSize) {
        // The slot already advanced past this sample by at most one ring; a
        // settlement that sampled the clock before that rotation must not erase
        // the current window's counters.
        return;
      } else {
        // The previous slice, or a stale bucket idle past half the window-id
        // range: rotate is one write of the new window plus this settlement.
        next = (window << WINDOW_SHIFT) | increment;
      }
      if (buckets.compareAndSet(index, raw, next)) {
        return;
      }
    }
  }

  /**
   * Recalculates the aggregate failure rate across the live window and opens the
   * breaker when the rate strictly exceeds {@code failThreshold} with at least
   * {@code requestVolumeThreshold} requests.
   *
   * <p>Runs on every CLOSED failure, over the buckets the clock still counts.
   * Each bucket is one atomic read, so the pair it contributes is always from
   * one slice; a rotation between two buckets is inherent and bounded by the
   * ring width.
   *
   * <p>The CAS on the read snapshot makes one thread the only transitioner, so a
   * storm produces a single {@code anchorMillis} stamp and a single OPEN log, and
   * once OPEN this method short-circuits.
   *
   * @param now the caller's clock sample, shared with the settlement that triggered it
   */
  private void evaluateThreshold(long now) {
    Epoch e = epoch.get();
    if (e.state != CircuitBreakerState.CLOSED || !config.isEnabled()) {
      return;
    }

    long current = (now / slideMs) & WINDOW_MASK;
    long totalFail = 0L;
    long totalSuccess = 0L;
    for (int i = 0; i < bucketSize; i++) {
      long raw = buckets.get(i);
      long stored = raw >>> WINDOW_SHIFT;
      if (((current - stored) & WINDOW_MASK) < bucketSize) {
        totalSuccess += raw & COUNT_MASK;
        totalFail += (raw >>> FAIL_SHIFT) & COUNT_MASK;
      }
    }
    long volume = totalSuccess + totalFail;
    if (volume < config.getRequestVolumeThreshold()) {
      return;
    }
    double rate = (double) totalFail / volume;
    if (rate <= config.getFailThreshold()) {
      return;
    }

    if (epoch.compareAndSet(e, e.opened(now)) && config.isLogEnabled()) {
      log.info("CB OPEN (failRate={}, total={})", rate, volume);
    }
  }

  private Class<?>[] resolveClasses(List<String> classNames) {
    if (classNames == null || classNames.isEmpty()) {
      return new Class<?>[0];
    }

    List<Class<?>> resolved = new ArrayList<>(classNames.size());
    for (String name : classNames) {
      try {
        // Context-classloader-aware resolution (Spring): plain Class.forName would resolve
        // against this class's loader and miss business exception types in
        // layered-classloader deployments.
        resolved.add(ClassUtils.forName(name, null));
      } catch (ClassNotFoundException | LinkageError e) {
        // Resolution runs exactly once per breaker (constructor), so each missing
        // name warns at most once per breaker lifetime — inherently rate-limited;
        // no recurring-WARN window needed here.
        log.warn("Exception class not loadable: {}", name);
      }
    }
    return resolved.toArray(new Class<?>[0]);
  }

  /**
   * Checks whether the given throwable (or its cause chain) matches the
   * configured exception filters.
   *
   * <p>Semantics:
   * <ul>
   *   <li>{@code excludeExceptions} non-empty → treat matches as ignorable.</li>
   *   <li>{@code includeExceptions} non-empty → treat non-matches as ignorable.</li>
   *   <li>Both empty → never ignorable (backward compatible).</li>
   * </ul>
   *
   * <p>Both lists are resolved eagerly at construction; when both are set, the
   * exclude rule runs first.
   */
  private boolean isIgnorableException(Throwable t) {
    if (excludeClasses.length > 0 && causeChainMatchesAny(t, excludeClasses)) {
      return true;
    }

    if (includeClasses.length > 0) {
      return !causeChainMatchesAny(t, includeClasses);
    }

    return false;
  }

  /**
   * Traverses the entire cause chain to determine if any exception matches the
   * given types. The walk is depth-capped at {@link #MAX_CAUSE_CHAIN_DEPTH}:
   * a cyclic cause chain (a↔b linked via {@code initCause}) would otherwise
   * spin forever on the calling thread.
   */
  private static boolean causeChainMatchesAny(Throwable t, Class<?>[] classes) {
    Throwable current = t;
    for (int depth = 0; current != null && depth < MAX_CAUSE_CHAIN_DEPTH; depth++) {
      if (matchesAny(current.getClass(), classes)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  /**
   * Checks whether the given class matches any of the specified types.
   * {@code isAssignableFrom} covers the full subtype relation in one call —
   * superclasses and (transitively) super-interfaces alike, which a manual
   * walk would only partially enumerate.
   */
  private static boolean matchesAny(Class<?> thrown, Class<?>[] classes) {
    for (Class<?> target : classes) {
      if (target.isAssignableFrom(thrown)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Zeros every bucket at the current window, so the next settlement in every
   * slot is a same-window increment and a pre-clear settlement is discarded as
   * older. Called before publishing CLOSED: every path that reaches CLOSED
   * clears the window, so the failure surplus of the previous episode can never
   * re-trip the breaker the instant it heals.
   */
  private void resetBuckets() {
    long empty = ((monotonicMillis() / slideMs) & WINDOW_MASK) << WINDOW_SHIFT;
    for (int i = 0; i < bucketSize; i++) {
      buckets.set(i, empty);
    }
  }

  /**
   * Immutable snapshot of the breaker machine: the state, the HALF_OPEN episode
   * generation, the live probe slots, the consecutive probe successes, the episode
   * clock and the probe-credit multiplier. One reference keeps them mutually
   * consistent; a reader can never observe a state without the quota and the timer
   * that belong to it.
   *
   * <p>A probe stamp is a generation; it is live only while the state is HALF_OPEN
   * and the generation still matches, so a straggler from a retired episode settles
   * into nothing and can never drain a newer episode's quota.
   *
   * @param generation           bumped by every state transition, so any stamp minted
   *                             before it is dead
   * @param inflight             probe slots currently held by admitted requests
   * @param consecutiveSuccesses the number of consecutive successful probes
   * @param anchorMillis         the instant this epoch's clock started counting: the
   *                             open timestamp while OPEN (the cooldown anchor), the
   *                             last probe activity while HALF_OPEN (the wedged-episode
   *                             anchor), unused while CLOSED
   * @param probeCredit          the probe-quota multiplier over {@code halfOpenMaxProbes},
   *                             clamped to {@code [PROBE_CREDIT_FLOOR, PROBE_CREDIT_BASELINE]}
   */
  private record Epoch(
    CircuitBreakerState state,
    long generation,
    int inflight,
    int consecutiveSuccesses,
    long anchorMillis,
    double probeCredit
  ) {
    private static double clampCredit(double credit) {
      return Math.max(PROBE_CREDIT_FLOOR, Math.min(PROBE_CREDIT_BASELINE, credit));
    }

    private Epoch slotAcquire() {
      return new Epoch(
        CircuitBreakerState.HALF_OPEN,
        generation,
        inflight + 1,
        consecutiveSuccesses,
        anchorMillis,
        probeCredit
      );
    }

    private Epoch slotReleased(long now) {
      return new Epoch(CircuitBreakerState.HALF_OPEN, generation, inflight - 1, consecutiveSuccesses, now, probeCredit);
    }

    private Epoch probeSucceeded(long now) {
      return new Epoch(
        CircuitBreakerState.HALF_OPEN,
        generation,
        inflight - 1,
        consecutiveSuccesses + 1,
        now,
        probeCredit
      );
    }

    /** Opens with a fresh cooldown, keeping the credit that produced this episode's outcome. */
    private Epoch opened(long now) {
      return new Epoch(CircuitBreakerState.OPEN, generation + 1, 0, 0, now, probeCredit);
    }

    /** A failed probe reopens in the same CAS that applies the episode's credit penalty. */
    private Epoch probeFailed(long now) {
      return new Epoch(
        CircuitBreakerState.OPEN,
        generation + 1,
        0,
        0,
        now,
        clampCredit(probeCredit * K_PROBE_PENALTY_RATIO)
      );
    }

    /** Starts a HALF_OPEN episode owning exactly one slot — the transitioner's. */
    private Epoch startNew(long now) {
      return new Epoch(CircuitBreakerState.HALF_OPEN, generation + 1, 1, 0, now, probeCredit);
    }

    /** Recovery or a forced close; {@code reward} applies the credit reward in the same CAS. */
    private Epoch closed(boolean reward) {
      return new Epoch(
        CircuitBreakerState.CLOSED,
        generation + 1,
        0,
        0,
        0L,
        reward ? clampCredit(probeCredit * K_PROBE_RECOVER_RATIO) : probeCredit
      );
    }
  }
}
