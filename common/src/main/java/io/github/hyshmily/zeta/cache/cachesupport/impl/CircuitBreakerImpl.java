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
package io.github.hyshmily.zeta.cache.cachesupport.impl;

import static io.github.hyshmily.zeta.util.TimeSource.monotonicMillis;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.cache.cachesupport.CircuitBreaker;
import io.github.hyshmily.zeta.cache.cachesupport.CircuitBreakerSettings;
import io.github.hyshmily.zeta.cache.cachesupport.CircuitBreakerState;
import io.github.hyshmily.zeta.util.executor.SafeScheduledExecutorService;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * Sliding-window circuit breaker.
 *
 * <p>State machine:
 * <pre>
 *   CLOSED  ──(failure rate > threshold)──→ OPEN
 *   OPEN    ──(timeout expired)────────────→ HALF_OPEN (probe)
 *   HALF_OPEN ──(N consecutive successes)──→ CLOSED
 *   HALF_OPEN ──(any failure)──────────────→ OPEN
 * </pre>
 *
 * <p><b>Exception filtering</b> (from neural-circuitbreaker design):
 * {@code excludeExceptions} — these exception types never trip the breaker;
 * {@code includeExceptions} — only these cause breaker trips (empty = all).
 * Matching is by assignable type, not exact class: a thrown exception matches
 * when any configured type is assignable from the exception's class, any of
 * its superclasses, or any directly-declared interface — so subclasses and
 * interface implementations count. The configured filter names are resolved
 * once (lazily, on the first classified exception) and cached for the
 * lifetime of the breaker — including the resolves-to-empty outcome — so a
 * mistyped class name neither re-runs {@code Class.forName} on every
 * classification nor re-logs its warning. {@code excludeExceptions} is
 * evaluated first: an exception matched by both lists is ignorable (excluded
 * wins over included). The whole cause chain is walked (depth-capped) — an
 * exception wrapped by the caller still matches its underlying cause.
 *
 * <p><b>Consecutive success counting</b> (from neural-circuitbreaker design):
 * In HALF_OPEN state, requires {@code consecutiveSuccessThreshold} consecutive
 * probe successes before closing, preventing flapping from a single lucky probe.
 *
 * <p><b>Asymmetric probe credit</b> (RocksDB {@code write_controller.cc::SetupDelay}
 * asymmetry): the HALF_OPEN probe quota is scaled by a persistent credit —
 * failed recovery episodes tighten it ×0.8, clean probe successes recover it
 * toward the 1.0 baseline (×1.25), a full recovery resets it to 1.0. Chronic
 * flapping therefore converges to single-probe episodes; see {@link #probeCreditBits}.
 *
 * <p><b>Contention control:</b> Successes use one {@link LongAdder} per bucket
 * because every cache key records through the same breaker and would otherwise
 * contend on one atomic array slot. Failures stay in a padded {@code long[]}
 * because their rate is expected to be much lower.
 */
@Slf4j
@Internal
public class CircuitBreakerImpl implements CircuitBreaker, InitializingBean {

  private static final VarHandle VH = MethodHandles.arrayElementVarHandle(long[].class);

  private static final int STRIDE = 16;
  private static final int FAIL_OFFSET = 8;

  private final CircuitBreakerSettings config;
  private final int bucketSize;
  private final LongAdder[] successCounts;
  private final long[] failureCounts;
  private final ScheduledExecutorService scheduler;
  private final long slideMs;
  private volatile int currentIndex;

  private volatile CircuitBreakerState state = CircuitBreakerState.CLOSED;
  private final AtomicLong lastOpenedTime = new AtomicLong(0L);
  private final AtomicLong lastHalfOpenAttempt = new AtomicLong(0L);
  private final AtomicInteger consecutiveSuccessCounter = new AtomicInteger(0);
  private final AtomicInteger halfOpenInflight = new AtomicInteger(0);

  /**
   * HALF_OPEN episode generation attached to reservations. An old episode may
   * finish after a failure has reset the in-flight count; tagging its callback
   * prevents it from decrementing a newer episode's quota.
   */
  private final AtomicLong probeGeneration = new AtomicLong(0L);

  /**
   * HALF_OPEN slots reserved by calls on the current thread but not yet
   * settled. Tracking reservations at the call boundary makes batch settlement
   * idempotent: one admission releases one slot even when the batch reports one
   * result per key, while a completed-future replay admitted in CLOSED cannot
   * release a slot from a later HALF_OPEN episode.
   */
  private final ThreadLocal<ArrayDeque<Long>> probeReservations = ThreadLocal.withInitial(ArrayDeque::new);

  /**
   * Asymmetric probe-quota credit for HALF_OPEN recovery, adapted from
   * RocksDB {@code db/write_controller.cc::SetupDelay}
   * ({@code kIncSlowdownRatio=0.8 / kDecSlowdownRatio=1.25} — the penalty
   * is steeper than the reward): a failed recovery episode tightens the
   * next episode's probe quota (×0.8, floored at {@link #PROBE_CREDIT_FLOOR}
   * so at least one probe always passes), while clean probe successes
   * recover it toward the 1.0 baseline (×1.25, capped there). The credit
   * persists across OPEN→HALF_OPEN episodes, so chronic flapping converges
   * to single-probe episodes instead of re-opening with full aggression; a
   * full recovery (HALF_OPEN→CLOSED) resets it to 1.0 — a healed data
   * source gets a clean slate.
   *
   * <p>The credit only ever TIGHTENS: {@code halfOpenMaxProbes} stays the
   * hard quota ceiling (an operator knob must not be silently exceeded by
   * an internal multiplier).
   *
   * <p>Deliberately constants, not config: policy-type tuning ratios follow
   * the ADR-0018 convention (count-type thresholds are configuration,
   * strategy ratios are code).
   */
  private static final double K_PROBE_PENALTY_RATIO = 0.8;
  private static final double K_PROBE_RECOVER_RATIO = 1.25;
  private static final double PROBE_CREDIT_FLOOR = 0.25;
  private static final double PROBE_CREDIT_BASELINE = 1.0;

  /**
   * Raw bits of the live probe-quota multiplier over {@code halfOpenMaxProbes},
   * always in {@code [PROBE_CREDIT_FLOOR, PROBE_CREDIT_BASELINE]}. A CAS loop is
   * required because multiple probes can settle concurrently; a volatile
   * floating-point read-modify-write would lose penalties or rewards.
   */
  private final AtomicLong probeCreditBits = new AtomicLong(Double.doubleToLongBits(PROBE_CREDIT_BASELINE));

  private final AtomicBoolean started = new AtomicBoolean(false);
  private volatile ScheduledFuture<?> slideFuture;

  /**
   * @param config Circuit breaker configuration view (buckets, thresholds, exception lists);
   *               read live on every decision so runtime toggles keep working (ADR-0082).
   */
  public CircuitBreakerImpl(CircuitBreakerSettings config) {
    this.config = config;
    this.bucketSize = config.getWindowBuckets();
    this.successCounts = new LongAdder[bucketSize];
    for (int i = 0; i < bucketSize; i++) {
      successCounts[i] = new LongAdder();
    }
    this.failureCounts = new long[bucketSize * STRIDE];
    this.slideMs = config.getWindowTimeMs() / bucketSize;
    Assert.isTrue(
      slideMs >= 1,
      "window-time-ms (" + config.getWindowTimeMs() + ") must be >= window-buckets (" + bucketSize + ")"
    );
    // Instance-level single-thread scheduler: the slide task is trivial, one daemon thread per
    // breaker is enough. Owned by this instance and shut down in close(), so hot-restart
    // (new classloader) scenarios do not leak threads or this instance via the task chain.
    this.scheduler = new SafeScheduledExecutorService(1, r -> {
      Thread t = new Thread(r, "zeta-cb");
      t.setDaemon(true);
      return t;
    });
  }

  /**
   * Starts the sliding-window clock after construction has safely published all
   * final fields. Scheduling from the constructor would let the executor invoke
   * {@link #slide()} through an escaped {@code this} before construction completes.
   */
  public void start() {
    if (started.compareAndSet(false, true)) {
      slideFuture = scheduler.scheduleAtFixedRate(this::slide, slideMs, slideMs, TimeUnit.MILLISECONDS);
    }
  }

  /** Spring invokes this only after the bean has completed construction and dependency injection. */
  @Override
  public void afterPropertiesSet() {
    start();
  }

  /**
   * Decides whether the current request may pass through.
   *
   * <p>CLOSED → allow all. OPEN → allow only after {@code singleTestIntervalMs}
   * has elapsed since opening (HALF_OPEN probe). HALF_OPEN → allow probes only;
   * concurrent callers are serialised via {@code lastHalfOpenAttempt} CAS.
   */
  @Override
  @SuppressWarnings("all")
  public boolean allowRequest() {
    if (!config.isEnabled()) {
      if (state != CircuitBreakerState.CLOSED) {
        state = CircuitBreakerState.CLOSED;
      }
      return true;
    }

    CircuitBreakerState s = state;
    if (s == CircuitBreakerState.CLOSED) {
      return true;
    }

    if (s == CircuitBreakerState.OPEN) {
      long now = monotonicMillis();

      if (now - lastOpenedTime.get() > config.getSingleTestIntervalMs()) {
        if (lastHalfOpenAttempt.compareAndSet(0L, now)) {
          return transitionToHalfOpen(true);
        }

        long lastProbe = lastHalfOpenAttempt.get();
        if (now - lastProbe > config.getSingleTestIntervalMs() && lastHalfOpenAttempt.compareAndSet(lastProbe, now)) {
          return transitionToHalfOpen(false);
        }
      }
      return false;
    }

    if (s == CircuitBreakerState.HALF_OPEN) {
      // Credit-scaled quota (see probeCreditBits): min 1 probe even at the floor.
      int maxProbes = Math.max(1, (int) (config.getHalfOpenMaxProbes() * currentProbeCredit()));
      if (halfOpenInflight.incrementAndGet() <= maxProbes) {
        reserveProbeForCurrentThread(probeGeneration.get());
        return true;
      }
      halfOpenInflight.decrementAndGet();
      return false;
    }
    return false;
  }

  /**
   * Apply the OPEN → HALF_OPEN transition and own the caller's probe slot.
   * The transitioner itself is the first probe: a slot is reserved BEFORE the
   * HALF_OPEN state becomes visible, so its later onSuccess() release is
   * balanced. Returning without a reservation leaked -1 per OPEN→HALF_OPEN
   * transition and let halfOpenInflight drift negative.
   *
   * <p>Caller must have won the transition race (a CAS on
   * {@code lastHalfOpenAttempt}); this method only applies the state change.
   *
   * @param firstProbe {@code true} for the probe that opened the window,
   *                   {@code false} for a single-test retry probe
   * @return always {@code true} — the caller may emit its probe
   */
  private boolean transitionToHalfOpen(boolean firstProbe) {
    long generation = probeGeneration.incrementAndGet();
    halfOpenInflight.incrementAndGet();
    reserveProbeForCurrentThread(generation);
    state = CircuitBreakerState.HALF_OPEN;
    consecutiveSuccessCounter.set(0);

    if (config.isLogEnabled()) {
      if (firstProbe) {
        log.info("CB HALF_OPEN probe");
      } else {
        log.info("CB HALF_OPEN retry probe");
      }
    }
    return true;
  }

  private void reserveProbeForCurrentThread(long generation) {
    probeReservations.get().addLast(generation);
  }

  /** Returns the reserved episode, or {@code Long.MIN_VALUE} when this call did not reserve a probe. */
  private long consumeProbeReservation() {
    ArrayDeque<Long> reservations = probeReservations.get();
    Long generation = reservations.pollFirst();
    if (reservations.isEmpty()) {
      probeReservations.remove();
    }
    return generation != null ? generation : Long.MIN_VALUE;
  }

  /**
   * Records a success. Increments the current success bucket.
   * In HALF_OPEN state, counts consecutive successes and transitions to CLOSED
   * when {@code consecutiveSuccessThreshold} is reached.
   */
  @Override
  @SuppressWarnings("all")
  public void onSuccess() {
    long reservationGeneration = consumeProbeReservation();
    if (!config.isEnabled()) {
      return;
    }

    successCounts[currentIndex].increment();

    CircuitBreakerState s = state;
    if (s == CircuitBreakerState.CLOSED) {
      return;
    }

    if (s == CircuitBreakerState.HALF_OPEN && reservationGeneration == probeGeneration.get()) {
      halfOpenInflight.decrementAndGet();
      int consec = consecutiveSuccessCounter.incrementAndGet();
      // Reward branch of the asymmetry (see probeCreditBits): every clean probe
      // success recovers the credit toward the baseline, capped there.
      recoverProbeCredit();

      if (consec >= config.getConsecutiveSuccessThreshold()) {
        state = CircuitBreakerState.CLOSED;
        lastHalfOpenAttempt.set(0L);
        halfOpenInflight.set(0);
        resetAllBuckets();
        // Full recovery clears the tightening memory — the healed data source
        // starts its next failure cycle from the unmodified quota.
        probeCreditBits.set(Double.doubleToLongBits(PROBE_CREDIT_BASELINE));
        if (config.isLogEnabled()) {
          log.info("CB CLOSED after {} consecutive successes", consec);
        }
      }
    }
  }

  /**
   * Records a failure (no exception context). Always counts toward the failure rate.
   * In HALF_OPEN state, any failure immediately reverts to OPEN.
   */
  @Override
  @SuppressWarnings("all")
  public void onFailure() {
    long reservationGeneration = consumeProbeReservation();
    if (!config.isEnabled()) {
      return;
    }

    CircuitBreakerState s = state;
    if (s == CircuitBreakerState.CLOSED) {
      VH.getAndAdd(failureCounts, currentIndex * STRIDE + FAIL_OFFSET, 1L);
      evaluateThreshold();
      return;
    }

    if (s == CircuitBreakerState.HALF_OPEN && reservationGeneration == probeGeneration.get()) {
      halfOpenInflight.decrementAndGet();
      // Penalty branch of the asymmetry (see probeCreditBits): a failed recovery
      // episode tightens the NEXT episode's quota; the credit persists across
      // episodes, so chronic flapping converges to single-probe episodes.
      double probeCredit = penalizeProbeCredit();
      halfOpenInflight.set(0);
      consecutiveSuccessCounter.set(0);
      state = CircuitBreakerState.OPEN;
      lastOpenedTime.set(monotonicMillis());
      if (config.isLogEnabled()) {
        log.info("CB HALF_OPEN -> OPEN (probe failed, probeCredit={})", probeCredit);
      }
    }
  }

  /**
   * Records a failure with exception context. Checks the exception against the
   * configured {@code excludeExceptions} / {@code includeExceptions} lists.
   * Ignorable exceptions are treated as success (call {@link #onSuccess()}).
   */
  @Override
  public void onFailure(Throwable t) {
    if (isIgnorableException(t)) {
      onSuccess();
      return;
    }
    onFailure();
  }

  /**
   * Releases a request slot given up without an outcome (executor rejection
   * before any data-source call). The per-call reservation token prevents an
   * unadmitted callback or an old HALF_OPEN episode from decrementing the live
   * quota, so the paired release can use an exact decrement rather than hiding
   * accounting defects behind a floor-at-zero clamp.
   */
  @Override
  public void onAbandoned() {
    long reservationGeneration = consumeProbeReservation();
    if (!config.isEnabled()) {
      return;
    }
    if (state == CircuitBreakerState.HALF_OPEN && reservationGeneration == probeGeneration.get()) {
      halfOpenInflight.decrementAndGet();
    }
  }

  /**
   * Returns true when the breaker is OPEN (fast-failing), skipping probes.
   */
  @Override
  public boolean isOpen() {
    return state == CircuitBreakerState.OPEN && config.isEnabled();
  }

  /**
   * Returns the current {@link CircuitBreakerState} for diagnostics.
   */
  @Override
  public CircuitBreakerState getState() {
    return state;
  }

  private double currentProbeCredit() {
    return Double.longBitsToDouble(probeCreditBits.get());
  }

  private void recoverProbeCredit() {
    updateProbeCredit(K_PROBE_RECOVER_RATIO);
  }

  private double penalizeProbeCredit() {
    return updateProbeCredit(K_PROBE_PENALTY_RATIO);
  }

  /**
   * Atomically applies a reward or penalty so concurrent probe completions are
   * all reflected instead of overwriting one another's volatile RMW result.
   */
  private double updateProbeCredit(double ratio) {
    while (true) {
      long currentBits = probeCreditBits.get();
      double current = Double.longBitsToDouble(currentBits);
      double updated = Math.max(PROBE_CREDIT_FLOOR, Math.min(PROBE_CREDIT_BASELINE, current * ratio));
      long updatedBits = Double.doubleToLongBits(updated);
      if (probeCreditBits.compareAndSet(currentBits, updatedBits)) {
        return updated;
      }
    }
  }

  /**
   * Advances the sliding window: moves to the next bucket (resetting it)
   * and re-evaluates the failure threshold.
   */
  private void slide() {
    int next = currentIndex + 1;
    if (next == bucketSize) {
      next = 0;
    }
    int base = next * STRIDE;
    successCounts[next].reset();
    VH.setVolatile(failureCounts, base + FAIL_OFFSET, 0L);
    currentIndex = next;
    evaluateThreshold();
  }

  /**
   * Recalculates the aggregate failure rate across all window buckets.
   * Transitions to OPEN when the rate exceeds {@code failThreshold}
   * and the total request volume is above {@code requestVolumeThreshold}.
   */
  private void evaluateThreshold() {
    if (state != CircuitBreakerState.CLOSED) {
      return;
    }

    long totalSuccess = 0;
    long totalFail = 0;

    for (int i = 0; i < bucketSize; i++) {
      totalSuccess += successCounts[i].sum();
      totalFail += (long) VH.getVolatile(failureCounts, i * STRIDE + FAIL_OFFSET);
    }

    if (totalFail > 0 && totalSuccess + totalFail >= config.getRequestVolumeThreshold()) {
      double rate = (double) totalFail / (totalSuccess + totalFail);

      if (rate > config.getFailThreshold()) {
        state = CircuitBreakerState.OPEN;
        lastOpenedTime.set(monotonicMillis());
        if (config.isLogEnabled()) {
          log.info("CB OPEN (failRate={}, total={})", rate, totalSuccess + totalFail);
        }
      }
    }
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
   * <p>The two lists are mutually exclusive at configuration time.
   */
  @SuppressWarnings("java:S3077")
  private volatile Set<Class<?>> excludeClasses = Set.of();

  @SuppressWarnings("java:S3077")
  private volatile Set<Class<?>> includeClasses = Set.of();

  /**
   * Guard so the configured class names are resolved exactly once, including
   * the resolves-to-empty outcome. The former {@code isEmpty()} sentinel
   * conflated "not initialized yet" with "configured names failed to resolve",
   * re-running {@code Class.forName} on every exception classification (and
   * re-logging a warning) forever when a name was mistyped.
   */
  private final AtomicBoolean filtersInitialized = new AtomicBoolean(false);

  /**
   * Maximum cause-chain depth for {@link #causeChainMatchesAny}: a cyclic
   * cause chain (constructible via two mutually-linked throwables) would
   * otherwise spin forever on an application thread.
   */
  private static final int MAX_CAUSE_CHAIN_DEPTH = 32;

  private Set<Class<?>> resolveClasses(List<String> classNames) {
    if (classNames == null || classNames.isEmpty()) {
      return Set.of();
    }

    Set<Class<?>> classes = new HashSet<>();
    for (String name : classNames) {
      try {
        classes.add(Class.forName(name));
      } catch (ClassNotFoundException e) {
        // Resolution runs exactly once per breaker (see filtersInitialized), so
        // each missing name warns at most once per breaker lifetime — inherently
        // rate-limited; no recurring-WARN window needed here.
        log.warn("Exception class not found: {}", name);
      }
    }
    // Return an unmodifiable copy to ensure safe publication
    return Collections.unmodifiableSet(classes);
  }

  /**
   * Resolves the configured exclude/include class lists into the cached class
   * sets, exactly once per breaker. Failures inside resolution (a custom
   * classloader throwing, link errors) leave the sets empty and are reported
   * once — the breaker path must never propagate.
   */
  private void initExceptionFiltersOnce() {
    if (!filtersInitialized.compareAndSet(false, true)) {
      return;
    }
    try {
      excludeClasses = resolveClasses(config.getExcludeExceptions());
      includeClasses = resolveClasses(config.getIncludeExceptions());
    } catch (RuntimeException e) {
      log.warn("Circuit breaker exception-filter resolution failed, filters disabled (all exceptions counted)", e);
    }
  }

  /**
   * Checks whether the given throwable (or its cause chain) matches any of the specified types.
   * Uses isAssignableFrom to support both class inheritance and interface implementation.
   */
  @SuppressWarnings("all")
  private boolean matchesAny(Throwable t, Set<Class<?>> classes) {
    if (t == null || classes.isEmpty()) {
      return false;
    }

    for (Class<?> clazz = t.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
      for (Class<?> target : classes) {
        if (target.isAssignableFrom(clazz)) {
          return true;
        }
      }
      // Check interfaces
      for (Class<?> iface : clazz.getInterfaces()) {
        for (Class<?> target : classes) {
          if (target.isAssignableFrom(iface)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  /**
   * Traverses the entire cause chain to determine if any exception matches the
   * given types. The walk is depth-capped at {@link #MAX_CAUSE_CHAIN_DEPTH}:
   * a cyclic cause chain (a↔b linked via {@code initCause}) would otherwise
   * spin forever on the calling thread.
   */
  private boolean causeChainMatchesAny(Throwable t, Set<Class<?>> classes) {
    Throwable current = t;
    int depth = 0;
    while (current != null && depth < MAX_CAUSE_CHAIN_DEPTH) {
      if (matchesAny(current, classes)) {
        return true;
      }
      current = current.getCause();
      depth++;
    }
    return false;
  }

  @SuppressWarnings("all")
  private boolean isIgnorableException(Throwable t) {
    initExceptionFiltersOnce();

    if (!excludeClasses.isEmpty() && causeChainMatchesAny(t, excludeClasses)) {
      return true;
    }

    if (!includeClasses.isEmpty()) {
      return !causeChainMatchesAny(t, includeClasses);
    }

    return false;
  }

  /** Zeros all success and failure buckets. Called when transitioning to CLOSED. */
  private void resetAllBuckets() {
    for (int i = 0; i < bucketSize; i++) {
      successCounts[i].reset();
      VH.setVolatile(failureCounts, i * STRIDE + FAIL_OFFSET, 0L);
    }
  }

  /**
   * Cancels the scheduled sliding task and shuts down the instance-level scheduler.
   * Called by the Spring container (AutoCloseable destroy-method inference) on
   * context shutdown — releasing this instance and its config for GC.
   */
  @Override
  public void close() {
    ScheduledFuture<?> future = slideFuture;
    if (future != null) {
      future.cancel(false);
    }
    scheduler.shutdownNow();
  }
}
