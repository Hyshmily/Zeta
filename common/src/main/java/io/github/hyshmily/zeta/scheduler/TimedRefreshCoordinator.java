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
package io.github.hyshmily.zeta.scheduler;

import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.executor.SafeScheduledExecutorService;
import io.github.hyshmily.zeta.util.executor.ZetaThreadFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Per-key timed background refresh registry backing {@code Zeta.registerRefresh}.
 *
 * <p>
 * <b>Responsibility:</b> own the lazily-created refresh scheduler and the
 * per-key future map; replace-and-cancel registrations atomically; compute the
 * tick interval from a resolved soft TTL. The tick <i>body</i> (the cache read
 * that re-arms the stale-while-revalidate cycle) stays with the facade — this
 * class only schedules the {@link Runnable} it is handed, keeping the
 * coordinator free of cache-layer coupling.
 *
 * <p>
 * <b>Interval contract:</b> {@link #intervalFor} applies the ×1.1 cadence
 * factor to the resolved soft TTL (see the facade's {@code registerRefresh}
 * javadoc): with the default ±5% TTL jitter a plain soft-TTL interval would
 * fire before the entry is stale roughly half the time and the refresh would
 * be skipped; the factor guarantees the entry is stale at every tick.
 *
 * <p>
 * <b>Scheduler lifecycle:</b> created lazily on first registration (no threads
 * before first use), 2 threads so a slow supplier cannot block other refresh
 * keys, shut down by {@link #destroy()} when the facade bean is destroyed.
 *
 * <p>
 * <b>Starvation bound (deliberately no per-tick timeout here).</b> A tick is
 * the facade's soft-expire read: on a stale entry it only submits to
 * {@code BackgroundRefresher} (per-key dedup + semaphore + 30s
 * interrupting timeout) and returns immediately, so the scheduler thread is
 * never held; only a hard-missing entry performs a synchronous load on the
 * tick thread, and that load is bounded by SingleFlight's per-supplier
 * timeout plus the circuit breaker. A tick that overruns delays the next tick
 * of its own key by one interval and never overlaps it
 * ({@code SafeScheduledExecutorService} runs one link per chain at a time). A
 * slow key therefore delays other keys by at most one bounded load, not
 * indefinitely — isolating ticks onto a bigger pool would only move the same bound.
 *
 * <p>
 * Thread safety: all public methods are thread-safe. Registrations use an
 * atomic {@code compute} replace-and-cancel so two concurrent registrations
 * each cancel only the future they actually displace — the last writer's
 * future always survives (no dead future left registered for the key).
 */
@Internal
public class TimedRefreshCoordinator {

  private static final String DESTROYED_MESSAGE = "TimedRefreshCoordinator is destroyed";

  /**
   * Per-key scheduled refresh futures, keyed by cache key.
   */
  private final ConcurrentHashMap<String, ScheduledFuture<?>> refreshFutures = new ConcurrentHashMap<>();

  /**
   * Scheduler for timed background refresh tasks (lazily initialized).
   */
  @SuppressWarnings("java:S3077") // ScheduledExecutorService is thread-safe; we manage its lifecycle
  private volatile ScheduledExecutorService refreshScheduler;

  /**
   * Set by {@link #destroy()}. Registrations racing with shutdown fail fast
   * instead of surfacing the scheduler's {@code RejectedExecutionException}.
   */
  private volatile boolean destroyed;

  /**
   * Compute the timed-refresh interval from the resolved soft TTL:
   * {@code resolvedSoftTtlMs × 1.1}, rounded up to a whole millisecond.
   *
   * <p>
   * The ×1.1 cadence is load-bearing: with the default ±5% TTL jitter, a plain
   * soft-TTL interval would fire before the entry is stale roughly half the
   * time and the refresh would be skipped; the factor guarantees the entry is
   * stale at every tick, fulfilling the {@code Zeta.registerRefresh} contract.
   *
   * <p>
   * Integer-exact ceil(softTtl * 1.1) = softTtl + ceil(softTtl / 10): floating
   * point is avoided on purpose — 100 * 1.1 evaluates to 110.00000000000001
   * and a naive Math.ceil bumps the documented interval by a full ms, while
   * Math.round would violate the >= 1.1x staleness guarantee for tiny TTLs.
   * The ceiling division is written without {@code + 9} so it cannot overflow
   * before the division; the final addition saturates to {@link Long#MAX_VALUE}
   * because both terms are non-negative, so an overflowing sum is always
   * negative. A saturated interval still honours the >= 1.1x contract (a tick
   * that never fires cannot refresh too early) and is accepted by the scheduler.
   *
   * @param resolvedSoftTtlMs the effective soft TTL in milliseconds; must be positive
   * @return the refresh interval in milliseconds, saturated at {@link Long#MAX_VALUE}
   */
  public static long intervalFor(long resolvedSoftTtlMs) {
    if (resolvedSoftTtlMs <= 0) {
      throw new IllegalArgumentException(
        "resolvedSoftTtlMs must be positive (soft-expire disabled has no refresh cadence): " + resolvedSoftTtlMs
      );
    }
    long tenth = resolvedSoftTtlMs / 10 + (resolvedSoftTtlMs % 10 == 0 ? 0 : 1);
    long interval = resolvedSoftTtlMs + tenth;
    return interval < 0 ? Long.MAX_VALUE : interval;
  }

  /**
   * Register (or atomically replace) the timed refresh for the given key.
   *
   * <p>
   * Atomic replace-and-cancel: separate put + cancel inside one {@code compute}
   * lets two concurrent registrations cancel each other's replacement (T1
   * cancels F2, T2 cancels F1) and leave a dead future registered for the key.
   * Under compute, each caller cancels only the future it actually displaced,
   * so the last writer's future always survives.
   *
   * <p>
   * <b>Input contract:</b> {@code key} and {@code tick} are non-null and
   * {@code intervalMs} is positive. {@code DefaultZeta.registerRefresh} validates
   * all three (the key at the facade boundary, the interval through
   * {@link #intervalFor}) before calling, so this internal seam does not repeat
   * the checks — and therefore cannot start the lazy scheduler for input the
   * facade would already have rejected (design decisions: api surface).
   *
   * @param key        the cache key to refresh
   * @param intervalMs the fixed-delay interval in milliseconds (from
   *                   {@link #intervalFor})
   * @param tick       the per-tick action (the facade's soft-expire read for
   *                   the key with its frozen policy)
   * @throws IllegalStateException when the coordinator is destroyed, including
   *                               a {@link #destroy()} that races this
   *                               registration (the just-scheduled future is
   *                               cancelled and removed before the throw)
   */
  public void register(String key, long intervalMs, Runnable tick) {
    if (destroyed) {
      throw new IllegalStateException(DESTROYED_MESSAGE);
    }
    ScheduledExecutorService executor = scheduler();
    // Schedule OUTSIDE the map compute: holding a CHM bin lock while calling
    // into the scheduler extends the lock hold over queue allocation, and if
    // the schedule throws the already-cancelled predecessor would be left
    // registered. Each contender creates its future first; the compute only
    // swaps the reference and cancels the displaced future, so the last
    // writer's future always survives and a scheduling failure mutates nothing.
    ScheduledFuture<?> future;
    try {
      future = executor.scheduleWithFixedDelay(tick, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    } catch (RejectedExecutionException e) {
      // Lost the race with destroy(): report the stable contract exception
      // instead of the executor's shutdown rejection.
      if (destroyed) {
        throw new IllegalStateException(DESTROYED_MESSAGE, e);
      }
      throw e;
    }
    refreshFutures.compute(key, (k, prev) -> {
      if (prev != null) {
        prev.cancel(false);
      }
      return future;
    });
    // A destroy() racing this registration has already cancelled the map and
    // shut the scheduler down; remove the just-registered future again so no
    // never-firing entry leaks past shutdown. The registration did not survive,
    // so report the same contract exception as the entry check instead of
    // letting the caller believe it is scheduled.
    if (destroyed) {
      refreshFutures.remove(key, future);
      future.cancel(false);
      throw new IllegalStateException(DESTROYED_MESSAGE);
    }
  }

  /**
   * Cancel the timed refresh for the given key.
   *
   * @param key the cache key to stop refreshing
   */
  public void unregister(String key) {
    ScheduledFuture<?> future = refreshFutures.remove(key);
    if (future != null) {
      future.cancel(false);
    }
  }

  /**
   * Cancel all timed refreshes and shut down the refresh scheduler. Called by
   * the facade's {@code destroy()} when the Spring container closes the bean.
   */
  public void destroy() {
    destroyed = true;
    for (Map.Entry<String, ScheduledFuture<?>> entry : refreshFutures.entrySet()) {
      entry.getValue().cancel(false);
    }

    refreshFutures.clear();
    // Synchronized against scheduler()'s creation block so a scheduler
    // created concurrently with shutdown is still observed and stopped.
    synchronized (this) {
      ScheduledExecutorService executor = refreshScheduler;
      if (executor != null) {
        executor.shutdown();
      }
    }
  }

  /**
   * Lazy-initialize the refresh scheduler. Uses double-checked locking for
   * thread safety.
   */
  private ScheduledExecutorService scheduler() {
    ScheduledExecutorService current = refreshScheduler;
    if (current == null) {
      synchronized (this) {
        if (destroyed) {
          throw new IllegalStateException(DESTROYED_MESSAGE);
        }
        current = refreshScheduler;
        if (current == null) {
          current = new SafeScheduledExecutorService(2, new ZetaThreadFactory("zeta-timed-refresh-"));
          refreshScheduler = current;
        }
      }
    }
    return current;
  }
}
