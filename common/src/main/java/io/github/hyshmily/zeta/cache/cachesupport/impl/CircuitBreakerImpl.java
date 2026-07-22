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

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.CircuitBreaker;
import io.github.hyshmily.zeta.cache.cachesupport.CircuitBreakerState;
import lombok.extern.slf4j.Slf4j;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.github.hyshmily.zeta.util.TimeSource.currentTimeMillis;

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
 *
 * <p><b>Consecutive success counting</b> (from neural-circuitbreaker design):
 * In HALF_OPEN state, requires {@code consecutiveSuccessThreshold} consecutive
 * probe successes before closing, preventing flapping from a single lucky probe.
 *
 * <p><b>False-sharing prevention:</b> Single {@code long[]} with stride-based
 * padding slots. success[i] and fail[i] are placed 8 longs apart (64 bytes =
 * one x86-64 cache line), and consecutive logical entries are 16 longs apart.
 * All access via {@link VarHandle} for atomic RMW semantics.
 */
@Slf4j
@Internal
public class CircuitBreakerImpl implements CircuitBreaker {

  private static final ScheduledExecutorService SCHEDULER = new ScheduledThreadPoolExecutor(
    Runtime.getRuntime().availableProcessors(),
    r -> {
      Thread t = new Thread(r, "zeta-cb");
      t.setDaemon(true);
      return t;
    }
  );

  private static final VarHandle VH = MethodHandles.arrayElementVarHandle(long[].class);

  private static final int STRIDE = 16;
  private static final int SUCCESS_OFFSET = 0;
  private static final int FAIL_OFFSET = 8;

  private final ZetaProperties.CircuitBreaker config;
  private final int bucketSize;
  private final long[] counts;
  private volatile int currentIndex;

  private volatile CircuitBreakerState state = CircuitBreakerState.CLOSED;
  private final AtomicLong lastOpenedTime = new AtomicLong(0L);
  private final AtomicLong lastHalfOpenAttempt = new AtomicLong(0L);
  private final AtomicInteger consecutiveSuccessCounter = new AtomicInteger(0);
  private final AtomicInteger halfOpenInflight = new AtomicInteger(0);

  private final ScheduledFuture<?> slideFuture;

  /**
   * @param config Circuit breaker configuration (buckets, thresholds, exception lists).
   */
  public CircuitBreakerImpl(ZetaProperties.CircuitBreaker config) {
    this.config = config;
    this.bucketSize = config.getWindowBuckets();
    this.counts = new long[bucketSize * STRIDE];
    long slideMs = config.getWindowTimeMs() / bucketSize;
    if (slideMs < 1) {
      throw new IllegalArgumentException(
        "window-time-ms (" + config.getWindowTimeMs() + ") must be >= window-buckets (" + bucketSize + ")"
      );
    }
    this.slideFuture = SCHEDULER.scheduleAtFixedRate(this::slide, slideMs, slideMs, TimeUnit.MILLISECONDS);
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

    CircuitBreakerState s = this.state;
    if (s == CircuitBreakerState.CLOSED) {
      return true;
    }

    if (s == CircuitBreakerState.OPEN) {
      long now = currentTimeMillis();

      if (now - lastOpenedTime.get() > config.getSingleTestIntervalMs()) {
        if (lastHalfOpenAttempt.compareAndSet(0L, now)) {
          state = CircuitBreakerState.HALF_OPEN;
          consecutiveSuccessCounter.set(0);

          if (config.isLogEnabled()) {
            log.info("CB HALF_OPEN probe");
          }
          return true;
        }

        long lastProbe = lastHalfOpenAttempt.get();
        if (now - lastProbe > config.getSingleTestIntervalMs() && lastHalfOpenAttempt.compareAndSet(lastProbe, now)) {
          state = CircuitBreakerState.HALF_OPEN;
          consecutiveSuccessCounter.set(0);

          if (config.isLogEnabled()) {
            log.info("CB HALF_OPEN retry probe");
          }
          return true;
        }
      }
      return false;
    }

    if (s == CircuitBreakerState.HALF_OPEN) {
      if (halfOpenInflight.incrementAndGet() <= config.getHalfOpenMaxProbes()) {
        return true;
      }
      halfOpenInflight.decrementAndGet();
      return false;
    }
    return false;
  }

  /**
   * Records a success. Increments the current success bucket.
   * In HALF_OPEN state, counts consecutive successes and transitions to CLOSED
   * when {@code consecutiveSuccessThreshold} is reached.
   */
  @Override
  public void onSuccess() {
    if (!config.isEnabled()) {
      return;
    }

    VH.getAndAdd(counts, currentIndex * STRIDE + SUCCESS_OFFSET, 1L);

    CircuitBreakerState s = this.state;
    if (s == CircuitBreakerState.CLOSED) {
      return;
    }

    if (s == CircuitBreakerState.HALF_OPEN) {
      halfOpenInflight.decrementAndGet();
      int consec = consecutiveSuccessCounter.incrementAndGet();

      if (consec >= config.getConsecutiveSuccessThreshold()) {
        state = CircuitBreakerState.CLOSED;
        lastHalfOpenAttempt.set(0L);
        halfOpenInflight.set(0);
        resetAllBuckets();
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
  public void onFailure() {
    if (!config.isEnabled()) {
      return;
    }

    CircuitBreakerState s = this.state;
    if (s == CircuitBreakerState.CLOSED) {
      VH.getAndAdd(counts, currentIndex * STRIDE + FAIL_OFFSET, 1L);
      evaluateThreshold();
      return;
    }

    if (s == CircuitBreakerState.HALF_OPEN) {
      halfOpenInflight.set(0);
      consecutiveSuccessCounter.set(0);
      state = CircuitBreakerState.OPEN;
      lastOpenedTime.set(currentTimeMillis());
      if (config.isLogEnabled()) {
        log.info("CB HALF_OPEN -> OPEN (probe failed)");
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

  /**
   * Advances the sliding window: moves to the next bucket (resetting it)
   * and re-evaluates the failure threshold.
   */
  private void slide() {
    int next = (currentIndex + 1) % bucketSize;
    int base = next * STRIDE;
    VH.setVolatile(counts, base + SUCCESS_OFFSET, 0L);
    VH.setVolatile(counts, base + FAIL_OFFSET, 0L);
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
      int base = i * STRIDE;
      totalSuccess += (long) VH.getVolatile(counts, base + SUCCESS_OFFSET);
      totalFail += (long) VH.getVolatile(counts, base + FAIL_OFFSET);
    }

    if (totalFail > 0 && totalSuccess + totalFail >= config.getRequestVolumeThreshold()) {
      double rate = (double) totalFail / (totalSuccess + totalFail);

      if (rate > config.getFailThreshold()) {
        state = CircuitBreakerState.OPEN;
        lastOpenedTime.set(currentTimeMillis());
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
  private volatile Set<Class<?>> excludeClasses;
  private volatile Set<Class<?>> includeClasses;

  private Set<Class<?>> resolveClasses(List<String> classNames) {
    if (classNames == null || classNames.isEmpty()) return Set.of();
    Set<Class<?>> classes = new HashSet<>();
    for (String name : classNames) {
      try {
        classes.add(Class.forName(name));
      } catch (ClassNotFoundException e) {
        log.warn("Exception class not found: {}", name);
      }
    }
    return classes;
  }

  private boolean matchesAny(Throwable t, Set<Class<?>> classes) {
    if (classes.isEmpty()) return false;
    for (Class<?> clazz = t.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
      if (classes.contains(clazz)) return true;
    }
    return false;
  }

  @SuppressWarnings("all")
  private boolean isIgnorableException(Throwable t) {
    var exclude = config.getExcludeExceptions();
    if (exclude != null && !exclude.isEmpty()) {
      if (excludeClasses == null) excludeClasses = resolveClasses(exclude);
      if (matchesAny(t, excludeClasses)) return true;
      if (t.getCause() != null && matchesAny(t.getCause(), excludeClasses)) return true;
    }

    var include = config.getIncludeExceptions();
    if (include != null && !include.isEmpty()) {
      if (includeClasses == null) includeClasses = resolveClasses(include);
      if (!matchesAny(t, includeClasses)) return true;
      return t.getCause() == null || !matchesAny(t.getCause(), includeClasses);
    }
    return false;
  }

  /** Zeros all success and failure buckets. Called when transitioning to CLOSED. */
  private void resetAllBuckets() {
    for (int i = 0; i < bucketSize; i++) {
      int base = i * STRIDE;
      VH.setVolatile(counts, base + SUCCESS_OFFSET, 0L);
      VH.setVolatile(counts, base + FAIL_OFFSET, 0L);
    }
  }

  /**
   * Cleans up the scheduled sliding task. Called by the owning component
   * (e.g., {@code HotKeyCache}) during shutdown.
   */
  @Override
  public void close() {
    if (slideFuture != null) {
      slideFuture.cancel(false);
    }
  }
}
