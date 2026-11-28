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
import org.jspecify.annotations.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Sliding-window {@link CircuitBreaker} with type-filtered failure classification and asymmetric
 * half-open probe throttling.
 *
 * <p><b>State machine.</b> Transitions follow {@code CLOSED -> OPEN} on failure rate,
 * {@code OPEN -> HALF_OPEN} after {@code singleTestIntervalMs}, and {@code HALF_OPEN -> CLOSED}
 * after consecutive probe successes (any probe failure reopens). All mutable state — breaker state,
 * episode generation, live probe slots, consecutive successes, last-activity timestamp, probe
 * credit, and closed-window floor — lives in one immutable {@link Epoch} swapped via a single CAS,
 * guaranteeing consistent snapshots and single-winner transitions. A wedged {@code HALF_OPEN}
 * episode whose probes stop settling is force-retired after {@code singleTestIntervalMs}.
 *
 * <p><b>Clock-driven ring window.</b> Each bucket packs {@code [24-bit window | 20-bit failures |
 * 20-bit successes]} into a single {@code long} and rotates lazily with traffic rather than a
 * background timer, requiring no {@code start()}/{@code close()} lifecycle. Closing advances the
 * window offset and floor atomically inside the epoch CAS so pre-recovery failures cannot re-trip
 * the breaker.
 */
@Slf4j
@Internal
public class SlidingWindowBreaker implements CircuitBreaker {

  /** Maximum cause-chain depth inspected by {@link #isIgnorableException} to guard against cycles. */
  private static final int MAX_CAUSE_CHAIN_DEPTH = 32;

  /** Bit width of each per-bucket counter (saturates at {@value #COUNT_MASK}). */
  private static final int COUNT_BITS = 20;

  /** Bit shift of the failure counter within a packed bucket word. */
  private static final int FAIL_SHIFT = COUNT_BITS;

  /** Bit shift of the 24-bit window index within a packed bucket word. */
  private static final int WINDOW_SHIFT = 2 * COUNT_BITS;

  /** Mask and saturation ceiling for a single bucket counter. */
  private static final long COUNT_MASK = (1L << COUNT_BITS) - 1;

  /** Mask covering the 24-bit window index of a packed bucket word. */
  private static final long WINDOW_MASK = (1L << (Long.SIZE - WINDOW_SHIFT)) - 1;

  /** Half of the 24-bit ring span, used to disambiguate wrapped window indices. */
  private static final long HALF_WINDOW_RANGE = WINDOW_MASK >>> 1;

  /** Sentinel returned by {@link #tryStartHalfOpen} when a CAS race requires retrying admission. */
  private static final long CAS_RETRY = 0L;

  /**
   * Asymmetric per-episode probe-credit ratios for {@code HALF_OPEN} recovery: a failed episode
   * tightens probe concurrency by 20% while a clean recovery restores 10%, converging flaky
   * backends toward single-probe recovery without ever exceeding {@code halfOpenMaxProbes}.
   */
  private static final double PROBE_PENALTY_RATIO = 0.8;
  private static final double PROBE_RECOVER_RATIO = 1.1;
  private static final double PROBE_CREDIT_BASELINE = 1.0;
  private static final double PROBE_CREDIT_FLOOR = 0.25;

  private static final Class<?>[] EMPTY_CLASSES = new Class<?>[0];

  private final ZetaProperties.CircuitBreaker config;

  /** Atomic state-machine snapshot; instances are never recycled so CAS identity stays unique. */
  private final AtomicReference<Epoch> epoch = new AtomicReference<>();

  /** Resolved exception classes excluded from tripping the breaker. */
  private final Class<?>[] excludeClasses;

  /**
   * Resolved exception classes explicitly eligible to trip the breaker, or {@code null} when no
   * include list is configured. An empty array means a configured list whose every name failed to
   * load: it matches no exception, so nothing is eligible — a broken whitelist must not silently
   * invert into "every exception trips".
   */
  private final @Nullable Class<?>[] includeClasses;

  /** Clock-driven ring buffer of packed {@code [window | failures | successes]} words. */
  private final AtomicLongArray buckets;

  private final int bucketSize;
  private final long slideMs;

  /** Duration after which the 24-bit closed-window floor has wrapped and is no longer checked. */
  private final long halfRingMs;

  /**
   * Creates a sliding-window circuit breaker backed by live configuration properties.
   *
   * @param config circuit breaker configuration (buckets, thresholds, and exception filters); read
   *               live on every decision so runtime toggles take effect immediately
   * @throws IllegalArgumentException if {@code config} is {@code null} or its window parameters
   *                                  violate the 24-bit ring invariants
   */
  public SlidingWindowBreaker(ZetaProperties.CircuitBreaker config) {
    this(config, new Spec(config));
  }

  /**
   * Creates a shard from pre-validated, pre-resolved window constants.
   *
   * <p>Every shard of one configuration shares a single {@link Spec}, so the window validation and
   * the {@code ClassUtils.forName} resolution of the exception filters run once per configuration
   * instead of once per prefix (which also re-logged every unloadable class name per shard).
   *
   * @param config circuit breaker configuration, read live on every decision
   * @param spec   window geometry and resolved exception filters shared by every shard
   */
  SlidingWindowBreaker(ZetaProperties.CircuitBreaker config, Spec spec) {
    this.config = config;
    this.bucketSize = spec.bucketSize;
    this.slideMs = spec.slideMs;
    this.halfRingMs = spec.halfRingMs;

    long now = monotonicMillis();
    long initialWindow = currentWindowIndex(now);
    this.epoch.set(Epoch.initial(initialWindow, now));
    this.buckets = new AtomicLongArray(bucketSize);
    this.excludeClasses = spec.excludeClasses;
    this.includeClasses = spec.includeClasses;
    initializeBuckets(initialWindow);
  }

  /**
   * Immutable window geometry and exception filters derived once from a configuration.
   *
   * <p>Building it validates the configuration, so constructing one at startup turns a bad window
   * into a startup failure instead of a first-request {@link IllegalArgumentException}. Thresholds
   * and the enabled switch are deliberately NOT snapshot here: they stay live on the configuration.
   */
  static final class Spec {

    final int bucketSize;
    final long slideMs;
    final long halfRingMs;
    final Class<?>[] excludeClasses;
    final @Nullable Class<?>[] includeClasses;

    Spec(ZetaProperties.CircuitBreaker config) {
      validateConfig(config);
      this.bucketSize = config.getWindowBuckets();
      this.slideMs = config.getWindowTimeMs() / bucketSize;
      this.halfRingMs = HALF_WINDOW_RANGE > Long.MAX_VALUE / slideMs ? Long.MAX_VALUE : HALF_WINDOW_RANGE * slideMs;
      this.excludeClasses = resolveClasses(config.getExcludeExceptions());
      this.includeClasses = resolveIncludeClasses(config.getIncludeExceptions());
    }
  }

  /**
   * Attempts to admit a single request and returns its explicit settlement stamp.
   *
   * <p>{@code CLOSED} admits unconditionally; {@code OPEN} admits only after
   * {@code singleTestIntervalMs} elapses (transitioning to {@code HALF_OPEN}); {@code HALF_OPEN}
   * admits probes up to the credit-scaled quota and force-retires wedged episodes idle for
   * {@code singleTestIntervalMs}.
   *
   * @return {@link CircuitBreaker#STAMP_REJECTED} when denied,
   *         {@link CircuitBreaker#STAMP_CLOSED} on the {@code CLOSED} path, or a strictly positive
   *         {@code HALF_OPEN} probe generation
   */
  @Override
  public long tryAcquireStamp() {
    if (!config.isEnabled()) {
      forceClosedWhenDisabled();
      return STAMP_CLOSED;
    }

    for (;;) {
      Epoch current = epoch.get();
      switch (current.state) {
        case CLOSED -> {
          return STAMP_CLOSED;
        }
        case OPEN -> {
          long stamp = tryStartHalfOpen(current, monotonicMillis(), false);
          if (stamp != CAS_RETRY) {
            return stamp;
          }
        }
        case HALF_OPEN -> {
          int maxProbes = effectiveMaxProbes(current);
          long now = monotonicMillis();
          if (current.inflight < maxProbes) {
            if (epoch.compareAndSet(current, current.acquireSlot(now))) {
              return current.generation;
            }
            continue;
          }
          long stamp = tryStartHalfOpen(current, now, true);
          if (stamp != CAS_RETRY) {
            return stamp;
          }
        }
      }
    }
  }

  /**
   * Settles an admission stamp as a successful call.
   *
   * @param stamp the stamp previously returned by {@link #tryAcquireStamp()}
   */
  @Override
  public void onSuccess(long stamp) {
    settle(stamp, true);
  }

  /**
   * Settles an admission stamp as a failed call. Ignorable exceptions configured via
   * {@code excludeExceptions}/{@code includeExceptions} degrade to successes; a {@code null} error
   * bypasses filtering and counts as a failure.
   *
   * @param error the failure cause, or {@code null}
   * @param stamp the stamp previously returned by {@link #tryAcquireStamp()}
   */
  @Override
  public void onFailure(Throwable error, long stamp) {
    settle(stamp, error != null && isIgnorableException(error));
  }

  /**
   * Releases an admitted probe slot without recording a success or failure (for example, when work
   * is rejected by an executor before reaching the backend).
   *
   * @param stamp the stamp previously returned by {@link #tryAcquireStamp()}
   */
  @Override
  public void onAbandoned(long stamp) {
    if (!config.isEnabled() || stamp <= STAMP_CLOSED) {
      return;
    }

    for (;;) {
      Epoch current = epoch.get();
      if (
        current.state != CircuitBreakerState.HALF_OPEN ||
        current.generation != stamp ||
        current.inflight <= 0 ||
        epoch.compareAndSet(current, current.slotReleased(monotonicMillis()))
      ) {
        return;
      }
    }
  }

  /**
   * Returns whether the breaker is currently enabled and in the {@link CircuitBreakerState#OPEN}
   * state.
   *
   * @return {@code true} when fast-failing requests in {@code OPEN}
   */
  @Override
  public boolean isOpen() {
    return config.isEnabled() && epoch.get().state == CircuitBreakerState.OPEN;
  }

  /**
   * Returns whether the breaker is currently enabled and in any non-{@code CLOSED} state
   * ({@code OPEN} or {@code HALF_OPEN}).
   *
   * @return {@code true} when degraded and fallback behavior applies to denied or failed probes
   */
  @Override
  public boolean isDegraded() {
    return config.isEnabled() && epoch.get().state != CircuitBreakerState.CLOSED;
  }

  /**
   * Attempts to transition from {@code OPEN} or a wedged {@code HALF_OPEN} episode into a fresh
   * {@code HALF_OPEN} episode owning one probe slot once {@code singleTestIntervalMs} has elapsed.
   *
   * @param current the observed epoch snapshot
   * @param now     the current monotonic timestamp in milliseconds
   * @param reset   {@code true} when retiring a wedged {@code HALF_OPEN} episode, {@code false}
   *                when transitioning from {@code OPEN}
   * @return the new probe generation on success, {@link #STAMP_REJECTED} if the cooldown has not
   *         elapsed, or {@link #CAS_RETRY} if another thread updated the epoch concurrently
   */
  private long tryStartHalfOpen(Epoch current, long now, boolean reset) {
    if (now - current.lastActivityMs < config.getSingleTestIntervalMs()) {
      return STAMP_REJECTED;
    }
    Epoch next = current.startNew(now);
    if (!epoch.compareAndSet(current, next)) {
      return CAS_RETRY;
    }
    if (config.isLogEnabled()) {
      if (reset) {
        log.info("CB HALF_OPEN episode reset (no probe activity within {} ms)", config.getSingleTestIntervalMs());
      } else {
        log.info("CB HALF_OPEN probe");
      }
    }
    return next.generation;
  }

  /**
   * Routes a settlement outcome according to the stamp kind: positive stamps settle a
   * {@code HALF_OPEN} probe, while {@link #STAMP_CLOSED} records into the sliding window only if
   * the breaker remains in the same {@code CLOSED} epoch.
   *
   * @param stamp   the admission stamp
   * @param success {@code true} for success, {@code false} for failure
   */
  private void settle(long stamp, boolean success) {
    if (!config.isEnabled() || stamp == STAMP_REJECTED) {
      return;
    }
    if (stamp > STAMP_CLOSED) {
      probe(stamp, success);
      return;
    }

    Epoch current = epoch.get();
    if (current.state != CircuitBreakerState.CLOSED) {
      return;
    }
    // Share one clock sample between bucket recording and threshold evaluation, and confirm the
    // epoch did not rotate across the clock read so the floor offset stays valid.
    long now = monotonicMillis();
    if (epoch.get() != current) {
      return;
    }
    record(current, success, now);
    if (!success) {
      evaluateThreshold(current, now);
    }
  }

  /**
   * Atomically settles one live {@code HALF_OPEN} probe: releases its slot and either increments
   * consecutive successes, closes the breaker when the recovery threshold is met, or reopens the
   * breaker with a probe-credit penalty on failure.
   *
   * @param stamp   the generation stamp under which the probe was admitted
   * @param success {@code true} if the probe succeeded, {@code false} if it failed
   */
  private void probe(long stamp, boolean success) {
    int threshold = Math.max(1, config.getConsecutiveSuccessThreshold());
    for (;;) {
      Epoch current = epoch.get();
      if (current.state != CircuitBreakerState.HALF_OPEN || current.generation != stamp || current.inflight <= 0) {
        return;
      }

      long now = monotonicMillis();
      int nextSuccesses = current.consecutiveSuccesses + 1;
      if (success && nextSuccesses >= threshold) {
        if (epoch.compareAndSet(current, current.closed(true, rawSlice(now), now))) {
          if (config.isLogEnabled()) {
            log.info("CB CLOSED after {} consecutive successes", nextSuccesses);
          }
          return;
        }
        continue;
      }

      Epoch settled = success ? current.probeSucceeded(now) : current.probeFailed(now);
      if (!epoch.compareAndSet(current, settled)) {
        continue;
      }
      if (!success && config.isLogEnabled()) {
        log.info("CB HALF_OPEN -> OPEN (probe failed, probeCredit={})", settled.probeCredit);
      }
      return;
    }
  }

  /**
   * Forces the breaker back to {@code CLOSED} when disabled, retiring any active episode and
   * advancing the window floor atomically so stale buckets cannot trip upon re-enabling.
   */
  private void forceClosedWhenDisabled() {
    for (;;) {
      Epoch current = epoch.get();
      if (current.state == CircuitBreakerState.CLOSED) {
        return;
      }
      long now = monotonicMillis();
      if (epoch.compareAndSet(current, current.closed(false, rawSlice(now), now))) {
        return;
      }
    }
  }

  /**
   * Records a single {@code CLOSED}-path outcome into the ring bucket owning {@code now}.
   *
   * <p>The bucket word packs the 24-bit window index with both counters so rotation and
   * incrementing execute in one CAS. Out-of-order late settlements targeting an already-rotated
   * active slice are discarded without overwriting newer counts, while buckets older than the
   * closed-window floor or past the active window rotate atomically to {@code now}.
   *
   * @param current the {@code CLOSED} epoch sampled by the caller
   * @param success {@code true} to increment successes, {@code false} to increment failures
   * @param now     the caller's monotonic clock sample
   */
  private void record(Epoch current, boolean success, long now) {
    long windowIndex = effectiveSlice(current, now);
    long window = windowIndex & WINDOW_MASK;
    int index = (int) Math.floorMod(windowIndex, (long) bucketSize);
    int shift = success ? 0 : FAIL_SHIFT;
    long increment = 1L << shift;
    boolean filterFloor = isFloorActive(current, now);
    long closedFloor = current.closedWindowIndex;

    for (;;) {
      long raw = buckets.get(index);
      long stored = raw >>> WINDOW_SHIFT;
      long forward = (stored - windowIndex) & WINDOW_MASK;
      long next;

      if (filterFloor && isWindowBefore(stored, closedFloor)) {
        next = packBucket(window, increment);
      } else if (forward == 0L) {
        if (((raw >>> shift) & COUNT_MASK) >= COUNT_MASK) {
          return;
        }
        next = raw + increment;
      } else if (forward <= bucketSize) {
        // Bucket already advanced by at most one window; discard out-of-order late settlement.
        return;
      } else if (forward <= HALF_WINDOW_RANGE) {
        // Cold path: sample the live clock and confirm the caller's generation is still current, so
        // a stalled caller cannot apply a closed epoch's offset to a newer window's bucket.
        long liveNow = monotonicMillis();
        if (epoch.get() != current) {
          return;
        }
        long liveWindow = effectiveSlice(current, liveNow) & WINDOW_MASK;
        if (isWithinWindow(liveWindow, stored)) {
          return;
        }
        next = packBucket(window, increment);
      } else {
        // Previous slice or bucket idle past half the 24-bit ring; rotate with this settlement.
        next = packBucket(window, increment);
      }

      if (buckets.compareAndSet(index, raw, next)) {
        return;
      }
    }
  }

  /**
   * Sums active buckets across the sliding window and transitions {@code CLOSED -> OPEN} via CAS
   * when volume meets {@code requestVolumeThreshold} and failure rate strictly exceeds
   * {@code failThreshold}.
   *
   * @param current the {@code CLOSED} epoch snapshot to transition from
   * @param now     the caller's monotonic clock sample shared with {@link #record}
   */
  private void evaluateThreshold(Epoch current, long now) {
    if (!config.isEnabled()) {
      return;
    }

    long currentWindow = effectiveSlice(current, now) & WINDOW_MASK;
    boolean filterFloor = isFloorActive(current, now);
    long closedFloor = current.closedWindowIndex;
    long totalFail = 0L;
    long totalSuccess = 0L;

    for (int i = 0; i < bucketSize; i++) {
      long raw = buckets.get(i);
      long stored = raw >>> WINDOW_SHIFT;
      if (isWithinWindow(currentWindow, stored) && (!filterFloor || !isWindowBefore(stored, closedFloor))) {
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

    if (epoch.compareAndSet(current, current.opened(now)) && config.isLogEnabled()) {
      log.info("CB OPEN (failRate={}, total={})", rate, volume);
    }
  }

  /**
   * Initializes all ring slots at construction to {@code window} with zeroed counters.
   *
   * @param window the initial 24-bit window index
   */
  private void initializeBuckets(long window) {
    long empty = packBucket(window, 0L);
    for (int i = 0; i < bucketSize; i++) {
      buckets.set(i, empty);
    }
  }

  /**
   * Computes the credit-scaled {@code HALF_OPEN} probe limit, always at least {@code 1}.
   *
   * @param current the active {@code HALF_OPEN} epoch
   * @return the maximum concurrent probes allowed for this episode
   */
  private int effectiveMaxProbes(Epoch current) {
    return Math.max(1, (int) (config.getHalfOpenMaxProbes() * current.probeCredit));
  }

  /**
   * Returns the raw slice index owning {@code now}, using {@link Math#floorDiv(long, long)} so
   * slices remain uniform even across negative monotonic test offsets.
   *
   * @param now the monotonic timestamp in milliseconds
   * @return the unbounded slice index
   */
  private long rawSlice(long now) {
    return Math.floorDiv(now, slideMs);
  }

  /**
   * Returns the effective slice index for {@code current}, incorporating its close-generation
   * {@code windowOffset}.
   *
   * @param current the active epoch
   * @param now     the monotonic timestamp in milliseconds
   * @return the effective unbounded slice index
   */
  private long effectiveSlice(Epoch current, long now) {
    return rawSlice(now) + current.windowOffset;
  }

  /**
   * Returns the 24-bit ring index of the raw slice owning {@code now}.
   *
   * @param now the monotonic timestamp in milliseconds
   * @return the 24-bit window index
   */
  private long currentWindowIndex(long now) {
    return rawSlice(now) & WINDOW_MASK;
  }

  /**
   * Returns whether {@code storedWindow} falls within the active {@link #bucketSize} window ending
   * at {@code currentWindow}.
   *
   * @param currentWindow the current 24-bit window index
   * @param storedWindow  the 24-bit window index stored in a bucket
   * @return {@code true} if the bucket belongs to the live window
   */
  private boolean isWithinWindow(long currentWindow, long storedWindow) {
    return ((currentWindow - storedWindow) & WINDOW_MASK) < bucketSize;
  }

  /**
   * Returns whether the closed-window floor recorded in {@code current} is still within half a
   * 24-bit ring of {@code now} and therefore valid for filtering stale pre-close buckets.
   *
   * @param current the epoch holding the closed-window floor
   * @param now     the current monotonic timestamp in milliseconds
   * @return {@code true} if floor filtering is active
   */
  private boolean isFloorActive(Epoch current, long now) {
    return now - current.closedAtMs < halfRingMs;
  }

  /**
   * Packs a 24-bit window index and counter bits into a single bucket word.
   *
   * @param window    the 24-bit window index
   * @param countBits the pre-shifted counter bits
   * @return the packed bucket word
   */
  private static long packBucket(long window, long countBits) {
    return ((window & WINDOW_MASK) << WINDOW_SHIFT) | countBits;
  }

  /**
   * Returns whether {@code window} precedes {@code floor} in modular 24-bit ring order. Only valid
   * when {@link #isFloorActive(Epoch, long)} is {@code true}.
   *
   * @param window the bucket's 24-bit window index
   * @param floor  the epoch's closed-window floor
   * @return {@code true} if {@code window} is strictly older than {@code floor}
   */
  private static boolean isWindowBefore(long window, long floor) {
    return ((window - floor) & WINDOW_MASK) > HALF_WINDOW_RANGE;
  }

  /**
   * Validates circuit breaker window configuration invariants at construction time.
   *
   * @param config the configuration to validate
   * @throws IllegalArgumentException if any invariant is violated
   */
  private static void validateConfig(ZetaProperties.CircuitBreaker config) {
    Assert.notNull(config, "config must not be null");
    int buckets = config.getWindowBuckets();
    long windowTimeMs = config.getWindowTimeMs();
    Assert.isTrue(buckets > 0, "window-buckets (" + buckets + ") must be > 0");
    Assert.isTrue(
      windowTimeMs >= buckets,
      "window-time-ms (" + windowTimeMs + ") must be >= window-buckets (" + buckets + ")"
    );
    Assert.isTrue(buckets <= HALF_WINDOW_RANGE, "window-buckets (" + buckets + ") must be <= " + HALF_WINDOW_RANGE);
    Assert.isTrue(
      windowTimeMs <= Long.MAX_VALUE / HALF_WINDOW_RANGE,
      "window-time-ms (" + windowTimeMs + ") is too large for the 24-bit window"
    );
  }

  /**
   * Resolves configured exception class names once at construction using Spring's context
   * classloader resolution, logging and skipping any unloadable class names.
   *
   * @param classNames the configured fully qualified exception class names
   * @return a flat array of resolved {@link Class} instances
   */
  private static Class<?>[] resolveClasses(List<String> classNames) {
    if (classNames == null || classNames.isEmpty()) {
      return EMPTY_CLASSES;
    }
    List<Class<?>> resolved = new ArrayList<>(classNames.size());
    for (String name : classNames) {
      if (name == null || name.isBlank()) {
        continue;
      }
      try {
        resolved.add(ClassUtils.forName(name, null));
      } catch (ClassNotFoundException | LinkageError ignored) {
        log.warn("Exception class not loadable: {}", name);
      }
    }
    return resolved.toArray(EMPTY_CLASSES);
  }

  /**
   * Resolves the include whitelist, keeping "no whitelist configured" distinct from "whitelist
   * configured but nothing loadable". Both resolve to zero classes, but only the first admits every
   * exception as a failure; the second admits none, so a misspelled name or an absent optional
   * driver narrows the breaker instead of silently turning every business exception into a trip.
   *
   * @param classNames the configured fully qualified exception class names
   * @return the resolved classes, an empty array for a configured list that resolved nothing, or
   *         {@code null} when no list is configured
   */
  private static @Nullable Class<?>[] resolveIncludeClasses(@Nullable List<String> classNames) {
    return classNames == null || classNames.isEmpty() ? null : resolveClasses(classNames);
  }

  /**
   * Determines whether {@code throwable} (or any cause in its chain) should be ignored by the
   * circuit breaker according to {@code excludeExceptions} and {@code includeExceptions}.
   *
   * <p>When both lists are configured, {@code excludeExceptions} takes precedence over
   * {@code includeExceptions}. When neither is configured, no exception is ignored. A configured
   * include list matches exactly the types it resolved to, so a list that resolved to nothing
   * ignores every failure.
   *
   * @param throwable the failure to classify
   * @return {@code true} if the failure is ignorable and should count as a success
   */
  private boolean isIgnorableException(Throwable throwable) {
    if (excludeClasses.length > 0 && causeChainMatchesAny(throwable, excludeClasses)) {
      return true;
    }
    if (includeClasses != null) {
      return !causeChainMatchesAny(throwable, includeClasses);
    }
    return false;
  }

  /**
   * Walks up to {@link #MAX_CAUSE_CHAIN_DEPTH} causes of {@code throwable} to check whether any
   * exception is assignable to at least one type in {@code classes}.
   *
   * @param throwable the root throwable
   * @param classes   the target exception classes
   * @return {@code true} if any cause in the bounded chain matches
   */
  private static boolean causeChainMatchesAny(Throwable throwable, Class<?>[] classes) {
    Throwable current = throwable;
    for (int depth = 0; current != null && depth < MAX_CAUSE_CHAIN_DEPTH; depth++) {
      if (matchesAny(current.getClass(), classes)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  /**
   * Checks whether {@code thrown} is assignable to any class in {@code classes}.
   *
   * @param thrown  the runtime class of the thrown exception
   * @param classes the candidate superclasses or interfaces
   * @return {@code true} if any target class is assignable from {@code thrown}
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
   * Immutable state-machine snapshot swapped atomically via {@link #epoch}.
   *
   * <p>A {@code HALF_OPEN} probe stamp is its {@code generation}; settlements only take effect
   * while {@code state == HALF_OPEN} and the generation matches, ensuring stragglers from retired
   * episodes never affect newer ones.
   *
   * @param state                the current breaker state
   * @param generation           monotonically increasing transition generation invalidating old stamps
   * @param inflight             live {@code HALF_OPEN} probe slots currently held
   * @param consecutiveSuccesses consecutive probe successes observed in the current episode
   * @param lastActivityMs       cooldown anchor in {@code OPEN}, last probe activity in
   *                             {@code HALF_OPEN}, and the epoch's creation instant in {@code CLOSED}
   *                             (never a zero sentinel, so a negative monotonic sample cannot pin
   *                             the cooldown anchor at zero)
   * @param probeCredit          probe-quota multiplier clamped to
   *                             {@code [PROBE_CREDIT_FLOOR, PROBE_CREDIT_BASELINE]}
   * @param windowOffset         slice offset bumped on every close so same-slice pre-close buckets
   *                             fall behind {@code closedWindowIndex}
   * @param closedWindowIndex    24-bit effective window floor recorded upon entering {@code CLOSED}
   * @param closedAtMs           monotonic timestamp when {@code closedWindowIndex} was recorded
   */
  private record Epoch(
    CircuitBreakerState state,
    long generation,
    int inflight,
    int consecutiveSuccesses,
    long lastActivityMs,
    double probeCredit,
    long windowOffset,
    long closedWindowIndex,
    long closedAtMs
  ) {
    /**
     * Creates the initial {@code CLOSED} epoch at breaker construction.
     *
     * @param initialWindow the initial 24-bit window index
     * @param now           the construction monotonic timestamp in milliseconds
     * @return the initial {@code CLOSED} epoch
     */
    private static Epoch initial(long initialWindow, long now) {
      return new Epoch(CircuitBreakerState.CLOSED, 0L, 0, 0, now, PROBE_CREDIT_BASELINE, 0L, initialWindow, now);
    }

    /**
     * Clamps a probe-credit multiplier to {@code [PROBE_CREDIT_FLOOR, PROBE_CREDIT_BASELINE]}.
     *
     * @param credit the unclamped credit value
     * @return the clamped credit value
     */
    private static double clampCredit(double credit) {
      return Math.max(PROBE_CREDIT_FLOOR, Math.min(PROBE_CREDIT_BASELINE, credit));
    }

    /**
     * Returns a non-decreasing activity anchor so an older clock sample never rewinds the clock.
     *
     * @param now the candidate monotonic timestamp
     * @return {@code max(lastActivityMs, now)}
     */
    private long anchor(long now) {
      return Math.max(lastActivityMs, now);
    }

    /**
     * Reserves one additional {@code HALF_OPEN} probe slot in the current episode.
     *
     * @param now the admission monotonic timestamp
     * @return the updated {@code HALF_OPEN} epoch
     */
    private Epoch acquireSlot(long now) {
      return halfOpen(generation, inflight + 1, consecutiveSuccesses, now, probeCredit);
    }

    /**
     * Releases one abandoned {@code HALF_OPEN} probe slot without altering the success streak.
     *
     * @param now the abandonment monotonic timestamp
     * @return the updated {@code HALF_OPEN} epoch
     */
    private Epoch slotReleased(long now) {
      return halfOpen(generation, inflight - 1, consecutiveSuccesses, now, probeCredit);
    }

    /**
     * Releases one {@code HALF_OPEN} probe slot and increments {@code consecutiveSuccesses}.
     *
     * @param now the settlement monotonic timestamp
     * @return the updated {@code HALF_OPEN} epoch
     */
    private Epoch probeSucceeded(long now) {
      return halfOpen(generation, inflight - 1, consecutiveSuccesses + 1, now, probeCredit);
    }

    /**
     * Transitions from {@code CLOSED} to {@code OPEN} with a fresh cooldown and unchanged credit.
     *
     * @param now the opening monotonic timestamp
     * @return the new {@code OPEN} epoch
     */
    private Epoch opened(long now) {
      return open(generation + 1, now, probeCredit);
    }

    /**
     * Reopens from {@code HALF_OPEN} on probe failure and applies the episode credit penalty.
     *
     * @param now the failure monotonic timestamp
     * @return the new {@code OPEN} epoch with reduced probe credit
     */
    private Epoch probeFailed(long now) {
      return open(generation + 1, now, clampCredit(probeCredit * PROBE_PENALTY_RATIO));
    }

    /**
     * Starts a new {@code HALF_OPEN} episode holding one probe slot for the transitioning caller.
     *
     * @param now the transition monotonic timestamp
     * @return the new {@code HALF_OPEN} epoch
     */
    private Epoch startNew(long now) {
      return halfOpen(generation + 1, 1, 0, now, probeCredit);
    }

    /**
     * Transitions to {@code CLOSED}, advancing {@code windowOffset} and {@code closedWindowIndex}
     * atomically so pre-close buckets are ignored, and optionally rewarding {@code probeCredit}.
     *
     * @param reward         {@code true} to reward probe credit after a clean recovery
     * @param rawWindowIndex the unbounded raw slice index at close time
     * @param closedAtMs     the monotonic timestamp when the close occurred
     * @return the new {@code CLOSED} epoch
     */
    private Epoch closed(boolean reward, long rawWindowIndex, long closedAtMs) {
      long nextOffset = windowOffset + 1;
      return new Epoch(
        CircuitBreakerState.CLOSED,
        generation + 1,
        0,
        0,
        closedAtMs,
        reward ? clampCredit(probeCredit * PROBE_RECOVER_RATIO) : probeCredit,
        nextOffset,
        (rawWindowIndex + nextOffset) & WINDOW_MASK,
        closedAtMs
      );
    }

    /**
     * Creates a {@code HALF_OPEN} epoch preserving the current window floor metadata.
     *
     * @param nextGeneration the episode generation
     * @param nextInflight   the active probe slot count
     * @param nextSuccesses  the consecutive probe success count
     * @param now            the activity timestamp
     * @param credit         the probe-credit multiplier
     * @return the constructed {@code HALF_OPEN} epoch
     */
    private Epoch halfOpen(long nextGeneration, int nextInflight, int nextSuccesses, long now, double credit) {
      return new Epoch(
        CircuitBreakerState.HALF_OPEN,
        nextGeneration,
        nextInflight,
        nextSuccesses,
        anchor(now),
        credit,
        windowOffset,
        closedWindowIndex,
        closedAtMs
      );
    }

    /**
     * Creates an {@code OPEN} epoch with zeroed probe counters and preserved window floor metadata.
     *
     * @param nextGeneration the bumped generation
     * @param now            the cooldown anchor timestamp
     * @param credit         the probe-credit multiplier
     * @return the constructed {@code OPEN} epoch
     */
    private Epoch open(long nextGeneration, long now, double credit) {
      return new Epoch(
        CircuitBreakerState.OPEN,
        nextGeneration,
        0,
        0,
        anchor(now),
        credit,
        windowOffset,
        closedWindowIndex,
        closedAtMs
      );
    }
  }
}
