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

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.util.ZetaThreadFactory;
import io.github.hyshmily.zeta.util.executor.SafeScheduledExecutorService;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Per-key timed background refresh registry backing {@code Zeta.registerRefresh}
 * — the scheduling infrastructure extracted from the {@code Zeta} facade so the
 * facade stays a validation-and-delegation surface.
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
 * {@code DefaultBackgroundRefresher} (per-key dedup + semaphore + 30s
 * interrupting timeout) and returns immediately, so the scheduler thread is
 * never held; only a hard-missing entry performs a synchronous load on the
 * tick thread, and that load is bounded by SingleFlight's per-supplier
 * timeout plus the circuit breaker. Overshooting ticks are skipped, never
 * backlogged ({@code SafeScheduledExecutorService} runs strictly
 * non-overlapping). A slow key therefore delays other keys by at most one
 * bounded load, not indefinitely — isolating ticks onto a bigger pool would
 * only move the same bound.
 *
 * <p>
 * Thread safety: all public methods are thread-safe. Registrations use an
 * atomic {@code compute} replace-and-cancel so two concurrent registrations
 * each cancel only the future they actually displace — the last writer's
 * future always survives (no dead future left registered for the key).
 */
@Internal
public class TimedRefreshCoordinator {

  /**
   * Cadence factor applied to the resolved soft TTL when computing the timed
   * refresh interval. With the default ±5% TTL jitter, a plain soft-TTL
   * interval would fire before the entry is stale roughly half the time and
   * the refresh would be skipped; the ×1.1 factor guarantees the entry is
   * stale at every tick, fulfilling the {@code Zeta.registerRefresh} contract.
   */
  private static final double REFRESH_INTERVAL_SOFT_TTL_FACTOR = 1.1;

  /**
   * Scheduler for timed background refresh tasks (lazily initialized).
   */
  @SuppressWarnings("java:S3077") // ScheduledExecutorService is thread-safe; we manage its lifecycle
  private volatile ScheduledExecutorService refreshScheduler;

  /**
   * Per-key scheduled refresh futures, keyed by cache key.
   */
  private final ConcurrentHashMap<String, ScheduledFuture<?>> refreshFutures = new ConcurrentHashMap<>();

  /**
   * Set by {@link #destroy()}. Registrations racing with shutdown fail fast
   * instead of surfacing the scheduler's {@code RejectedExecutionException}.
   */
  private volatile boolean destroyed;

  /**
   * Compute the timed-refresh interval from the resolved soft TTL:
   * {@code resolvedSoftTtlMs × 1.1}, clamped to at least 1 ms.
   *
   * <p>
   * Integer-exact ceil(softTtl * 1.1) = softTtl + ceil(softTtl / 10): floating
   * point is avoided on purpose — 100 * 1.1 evaluates to 110.00000000000001
   * and a naive Math.ceil bumps the documented interval by a full ms, while
   * Math.round would violate the >= 1.1x staleness guarantee for tiny TTLs.
   *
   * @param resolvedSoftTtlMs the effective soft TTL in milliseconds
   * @return the refresh interval in milliseconds
   */
  public static long intervalFor(long resolvedSoftTtlMs) {
    return Math.max(1, resolvedSoftTtlMs + (resolvedSoftTtlMs + 9) / 10);
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
   * @param key        the cache key to refresh
   * @param intervalMs the fixed-delay interval in milliseconds (from
   *                   {@link #intervalFor})
   * @param tick       the per-tick action (the facade's soft-expire read for
   *                   the key with its frozen policy)
   */
  public void register(String key, long intervalMs, Runnable tick) {
    if (destroyed) {
      throw new IllegalStateException("TimedRefreshCoordinator is destroyed");
    }
    ScheduledExecutorService scheduler = getScheduler();
    // Schedule OUTSIDE the map compute: holding a CHM bin lock while calling
    // into the scheduler extends the lock hold over queue allocation, and if
    // the schedule throws the already-cancelled predecessor would be left
    // registered. Each contender creates its future first; the compute only
    // swaps the reference and cancels the displaced future, so the last
    // writer's future always survives and a scheduling failure mutates nothing.
    ScheduledFuture<?> future;
    try {
      future = scheduler.scheduleWithFixedDelay(tick, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    } catch (java.util.concurrent.RejectedExecutionException e) {
      // Lost the race with destroy(): report the stable contract exception
      // instead of the executor's shutdown rejection.
      if (destroyed) {
        throw new IllegalStateException("TimedRefreshCoordinator is destroyed", e);
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
    // never-firing entry leaks past shutdown.
    if (destroyed && refreshFutures.remove(key, future)) {
      future.cancel(false);
    }
  }

  /**
   * Cancel the timed refresh for the given key.
   *
   * @param key the cache key to stop refreshing
   */
  public void unregister(String key) {
    ScheduledFuture<?> f = refreshFutures.remove(key);
    if (f != null) {
      f.cancel(false);
    }
  }

  /**
   * Cancel all timed refreshes and shut down the refresh scheduler. Called by
   * the facade's {@code destroy()} when the Spring container closes the bean.
   */
  public void destroy() {
    destroyed = true;
    for (Map.Entry<String, ScheduledFuture<?>> e : refreshFutures.entrySet()) {
      e.getValue().cancel(false);
    }
    refreshFutures.clear();
    // Synchronized against getScheduler()'s creation block so a scheduler
    // created concurrently with shutdown is still observed and stopped.
    synchronized (this) {
      ScheduledExecutorService s = refreshScheduler;
      if (s != null) {
        s.shutdown();
      }
    }
  }

  /**
   * Lazy-initialize the refresh scheduler. Uses double-checked locking for
   * thread safety.
   */
  private ScheduledExecutorService getScheduler() {
    ScheduledExecutorService s = refreshScheduler;
    if (s == null) {
      synchronized (this) {
        s = refreshScheduler;
        if (s == null) {
          s = new SafeScheduledExecutorService(2, new ZetaThreadFactory("zeta-refresh"));
          refreshScheduler = s;
        }
      }
    }
    return s;
  }
}
