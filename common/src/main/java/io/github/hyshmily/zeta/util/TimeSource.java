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
package io.github.hyshmily.zeta.util;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.github.hyshmily.zeta.util.executor.ZetaThreadFactory;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import lombok.extern.slf4j.Slf4j;

/**
 * Performance-optimized clock source that caches {@link System#currentTimeMillis()}
 * on a background daemon thread (one tick every 5 ms) to avoid the cost of a
 * {@code native} call on every read via {@link #currentTimeMillis()}.
 * <p>
 * The background daemon thread is started via {@link #start()} and runs
 * until the last lifecycle owner is released ({@link #releaseLifecycleUser()})
 * or the JVM exits.
 * If the thread is interrupted or dies unexpectedly it is restarted with a
 * 1-second backoff; reads fall back to calling {@link System#currentTimeMillis()}
 * (never below the last cached value) until the replacement thread is up.
 * <p>
 * All time-sensitive components in the hot-path (expiry, rate limiting, circuit
 * breaker) use this source instead of {@code System.currentTimeMillis()}.
 */
@Internal
@Slf4j
public final class TimeSource {

  /** Monotonic baseline: wall clock at JVM start, used to render monotonic values near wall time. */
  private static final long BOOT_WALL_MS = System.currentTimeMillis();
  private static final long BOOT_NANO = System.nanoTime();

  /**
   * Largest forward wall-clock leap (beyond the time the ticker actually slept) adopted
   * verbatim; a larger leap is not adopted and the cached clock slews at twice real-time
   * rate until it catches up (design decisions: version stamps).
   */
  private static final long MAX_FORWARD_JUMP_MS = 5_000L;

  /** CAS handle for the sign-encoded clock state written by the ticker. */
  private static final VarHandle CURRENT_MILLIS;

  static {
    try {
      CURRENT_MILLIS = MethodHandles.lookup().findStaticVarHandle(TimeSource.class, "currentMillis", long.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /** Ticker cadence (ms). */
  private static final long TICK_MS = 5L;

  /**
   * The cached wall clock and the ticker lifecycle in one {@code long}.
   *
   * <ul>
   *   <li><b>{@code > 0} — running:</b> the cached wall clock, <b>including</b>
   *       {@link #wallOffsetMs}, so the hot read is one volatile read.</li>
   *   <li><b>{@code <= 0} — stopped:</b> {@code ~rawFloor}, where
   *       {@code rawFloor} is the last cached wall clock before the ticker
   *       stopped. The read path falls back to
   *       {@code max(rawFloor, System.currentTimeMillis()) + wallOffsetMs}.</li>
   * </ul>
   *
   * The sign bit is free because epoch milliseconds are positive for the
   * lifetime of the library. The two transitions run under the class monitor;
   * the ticker publishes each tick with a CAS (design decisions: version stamps).
   */
  private static volatile long currentMillis = ~System.currentTimeMillis();

  /** Wall-clock offset applied by tests to simulate NTP jumps; 0 in production. */
  private static volatile long wallOffsetMs = 0L;

  /** Monotonic offset applied by tests; 0 in production. */
  private static volatile long monoOffsetMs = 0L;

  /**
   * The installed ticker thread, so {@link #stopTicker()} can interrupt it and
   * a dying ticker can decide whether it is still the owner. {@code null} when
   * no ticker is installed. The sign of {@link #currentMillis} is the read-path
   * switch: during the 1s restart backoff the dying thread is still referenced
   * here while the state is already negative, so reads use the wall-clock
   * fallback.
   */
  private static volatile Thread clockThread;

  /**
   * Number of live lifecycle owners (one per application context). The ticker
   * is JVM-global, so a context teardown must not stop it while another context
   * in the same JVM still uses it. Guarded by the class monitor.
   */
  @GuardedBy("TimeSource.class")
  private static int lifecycleUsers = 0;

  /** Ticker start attempts; guarded by the class monitor. */
  @GuardedBy("TimeSource.class")
  private static int startAttempts = 0;

  /**
   * Starts the background clock-cache thread. Idempotent while the ticker is running or
   * inside its restart backoff; only a ticker with no installed thread is started, with
   * the lifecycle and read fallback described in the class doc. Reached through
   * {@link #registerLifecycleUser()} and from the production stamp clock.
   */
  @SuppressWarnings("all")
  public static synchronized void start() {
    // clockThread != null also covers the 1s backoff: the dying ticker still owns the
    // slot and will re-arm itself, so starting a second thread here would only force
    // the first one to exit.
    if (currentMillis > 0L || clockThread != null) {
      return;
    }
    // Refresh the cache synchronously BEFORE the running sign can be observed:
    // between this assignment and the new thread's first update, readers take
    // the cached branch and would otherwise see a value as old as the
    // class-load initialiser (first start) or the dying thread's last tick
    // plus the 1s restart backoff.
    publishRunning();
    if (++startAttempts == 1) {
      log.info("TimeSource clock-cache thread starting");
    } else {
      log.info("TimeSource clock-cache thread restarted (attempt #{})", startAttempts);
    }
    Thread ticker = new ZetaThreadFactory("TimeSource").newThread(() -> {
      long lastTickNanos = System.nanoTime();
      while (clockThread == Thread.currentThread()) {
        long cached = currentMillis;
        if (cached <= 0L) {
          // stopTicker() or the test hook encoded the stopped state.
          return;
        }
        // Wall clock first: elapsedMs then covers everything the wall reading could have
        // missed, so a pause between the two reads inflates both sides equally and cannot
        // turn a real tick into a rejected leap.
        long wallNow = System.currentTimeMillis();
        long nowNanos = System.nanoTime();
        long elapsedMs = Math.max(0L, (nowNanos - lastTickNanos) / 1_000_000L);
        // Advance by the whole milliseconds actually accounted for, keeping
        // the sub-millisecond remainder for the next tick: resetting to
        // nowNanos would discard up to 0.9ms per 5ms tick and under-measure
        // elapsed time (and thus the slew step).
        lastTickNanos += elapsedMs * 1_000_000L;
        long offset = wallOffsetMs;
        // Monotonic floor: a backward NTP step must never make the cached
        // wall clock jump backwards. Consumers compute deltas like
        // hardExpireAt - now against this source; a backwards step would
        // render those negative (early expiry). Clamping to the last observed
        // value keeps the cached clock monotonic; it catches up naturally once
        // the wall clock moves past the floor.
        long cachedWall = cached - offset;
        if (wallNow > cachedWall) {
          long delta = wallNow - cachedWall;
          // Adopt a normal tick or a pause whose growth the ticker observed;
          // slew anything that leaps beyond the elapsed real time.
          long plausible = MAX_FORWARD_JUMP_MS + elapsedMs;
          long next;
          if (delta <= plausible) {
            next = wallNow + offset;
          } else {
            // Catch-up slew at twice real time, capped at the wall clock. A real step
            // (VM resume, a first NTP correction) is recovered in about the time it
            // jumped, while a bogus leap that NTP reverts within seconds leaves only
            // seconds of overhang. elapsedMs (not TICK_MS) keeps sub-millisecond
            // remainders from being counted twice.
            long slewMs = Math.max(1L, elapsedMs) * 2L;
            next = Math.min(wallNow + offset, cached + slewMs);
            log.debug(
              "Rejected forward wall-clock leap of {}ms (max plausible {}ms); slewing the cached clock at twice real time",
              delta,
              plausible
            );
          }
          // CAS, not a plain write: stopTicker() may have encoded the stopped
          // state between the read above and this line, and a plain write would
          // resurrect the running sign.
          CURRENT_MILLIS.compareAndSet(cached, next);
        }
        try {
          Thread.sleep(TICK_MS);
        } catch (InterruptedException ignored) {
          if (currentMillis <= 0L || clockThread != Thread.currentThread()) {
            // Explicit stopTicker(), or this ticker was replaced before the interrupt arrived.
            // Either way it no longer owns the slot and must not enter the backoff.
            return;
          }
          // An unexpected interrupt must not degrade every read to a native call
          // forever, and it must not cost an OS thread either: the ticker is still
          // alive, so park for the backoff and re-arm it in place.
          if (!resumeAfterBackoff()) {
            return;
          }
          // The backoff is not clock progress: restart the elapsed-time anchor so the
          // next tick does not adopt a 1s leap.
          lastTickNanos = System.nanoTime();
        }
      }
    });
    ticker.setUncaughtExceptionHandler((thread, failure) -> {
      log.error("TimeSource thread terminated unexpectedly, will restart after 1s.", failure);
      restartWithBackoff();
    });
    clockThread = ticker;
    try {
      ticker.start();
    } catch (Throwable e) {
      // A thread that never started must not leave the running sign and a dead slot
      // behind: start()'s guard would treat the ticker as installed forever.
      clockThread = null;
      publishStopped();
      throw e;
    }
  }

  /**
   * Marks the clock-cache thread dead, waits the 1-second restart backoff
   * (best-effort: an interrupted backoff still proceeds to the restart), then
   * starts a replacement via {@link #start()}. Runs on the dying thread itself; a concurrent
   * {@code start()} sees the installed thread and leaves the re-arm to this thread, while a
   * concurrent {@link #stopTicker()} clears ownership and makes the final check a no-op.
   */
  @SuppressWarnings("all")
  private static void restartWithBackoff() {
    if (!markStopped()) {
      // This ticker was superseded by stopTicker()/start() before it could claim the slot:
      // never resurrect it and never pay the backoff.
      return;
    }
    // Clear whatever interrupted the dying thread (the failure that reached this handler may
    // itself have been an interrupt), or sleep would throw immediately and skip the backoff.
    Thread.interrupted();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    // Decide under the same monitor as stopTicker(): a stop that cleared the
    // thread reference must not be undone by a restart already in flight.
    synchronized (TimeSource.class) {
      if (clockThread == Thread.currentThread()) {
        // Release the slot before start(): this thread is dead, but it still occupies
        // clockThread and would otherwise satisfy start()'s "ticker already installed"
        // guard forever.
        clockThread = null;
        start();
      }
    }
  }

  /**
   * Re-arms the ticker on its own thread after an unexpected interrupt: mark the
   * clock stopped (readers fall back to the wall clock), wait the restart backoff,
   * then publish the running sign again. The thread is never replaced, so a
   * transient interrupt costs no OS thread and no re-registration.
   *
   * @return {@code false} when {@link #stopTicker()} or a replacement start won
   *         during the backoff and this thread must exit
   */
  private static boolean resumeAfterBackoff() {
    if (!markStopped()) {
      // A stopTicker() or a replacement start won before this thread could claim the stopped
      // state; the backoff would only delay the stopper's bounded join.
      return false;
    }
    try {
      Thread.sleep(1000L);
    } catch (InterruptedException ignored) {
      // stopTicker() interrupts again to cut the backoff short; the monitor
      // check below is the authority on whether this thread still owns the ticker.
    }
    synchronized (TimeSource.class) {
      if (clockThread != Thread.currentThread()) {
        return false;
      }
      publishRunning();
      log.info("TimeSource clock-cache thread resumed after an unexpected interrupt");
      return true;
    }
  }

  /**
   * Encodes the stopped state while preserving the raw monotonic floor. The class monitor
   * serialises this transition with {@link #start()} and {@link #stopTicker()}; the ticker's own
   * write is a CAS, so it cannot overwrite the negative sign after this returns.
   *
   * @return {@code true} when the caller still owned the ticker slot and the stopped state was
   *         written; {@code false} when a stop or a replacement start already superseded it
   */
  private static synchronized boolean markStopped() {
    if (clockThread != Thread.currentThread()) {
      return false;
    }
    publishStopped();
    return true;
  }

  /**
   * Raw monotonic floor behind a cached value: a running value drops
   * {@link #wallOffsetMs}, an already-stopped value is its own floor. Callers
   * publish the stopped state by complementing the result.
   */
  private static long rawFloor(long cached) {
    return cached > 0L ? cached - wallOffsetMs : ~cached;
  }

  /**
   * Publishes the running state: the cached value becomes the later of the
   * monotonic floor and the wall clock, shifted by the test offset. Callers hold
   * the class monitor, which is what orders these sign transitions.
   */
  @SuppressWarnings("all")
  private static void publishRunning() {
    currentMillis = Math.max(rawFloor(currentMillis), System.currentTimeMillis()) + wallOffsetMs;
  }

  /**
   * Publishes the stopped state: the sign bit is set and the raw monotonic floor
   * preserved, so readers fall back to the wall clock until the ticker resumes.
   */
  @SuppressWarnings("all")
  private static void publishStopped() {
    currentMillis = ~rawFloor(currentMillis);
  }

  /**
   * Registers a lifecycle owner (an application context) and ensures the
   * ticker is running. Paired with {@link #releaseLifecycleUser()}.
   *
   * <p>The increment and {@link #start()} run in one class-monitor section. {@code start()}
   * is reentrant, so this is one acquisition and no other registration or teardown can
   * interleave between "there is an owner" and "the ticker is started". A registration
   * racing another context's teardown is still re-checked by {@link #stopTicker()}, which
   * refuses to stop while an owner is live.
   */
  public static void registerLifecycleUser() {
    synchronized (TimeSource.class) {
      lifecycleUsers++;
      start();
    }
  }

  /**
   * Releases a lifecycle owner; the ticker stops when the last one is gone, leaving the
   * JVM-global clock running for every other context (design decisions: version stamps).
   *
   * <p>The decrement is decided under the class monitor; the actual stop — and its
   * bounded join — runs outside it, so a ticker entering {@link #markStopped()} or
   * {@link #resumeAfterBackoff()} is never blocked behind the joiner.
   */
  public static void releaseLifecycleUser() {
    boolean lastOwner;
    synchronized (TimeSource.class) {
      // An unmatched release (no registered owner) must not stop a ticker another component
      // started directly, e.g. a stamp clock's constructor.
      if (lifecycleUsers <= 0) {
        return;
      }
      lastOwner = --lifecycleUsers == 0;
    }
    if (lastOwner) {
      stopTicker();
    }
  }

  /**
   * Stops the clock-cache thread. Reads keep working: they fall back to the
   * monotonic floor over {@link System#currentTimeMillis()}, so the clock never
   * moves backwards, only loses its JNI-free fast path. {@link #start()} may be
   * called again to restart the ticker.
   *
   * <p>The state transition runs under the class monitor; the interrupt and the
   * bounded join run outside it. The owner re-check keeps a registration that raced
   * {@link #releaseLifecycleUser()}'s decision from having its ticker stopped
   * underneath it.
   */
  private static void stopTicker() {
    Thread ticker;
    synchronized (TimeSource.class) {
      if (lifecycleUsers > 0) {
        return;
      }
      ticker = clockThread;
      clockThread = null;
      publishStopped();
    }
    if (ticker == null) {
      return;
    }
    ticker.interrupt();
    if (ticker != Thread.currentThread()) {
      try {
        // The interrupt makes the ticker return immediately; the join only
        // serialises a following start() against the exiting thread.
        ticker.join(200L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Returns the current time in milliseconds since the epoch.
   * <p>
   * When the background thread is running, this returns the cached value
   * updated every 5ms — avoiding a {@code native} JNI call on every read.
   * Falls back to {@link System#currentTimeMillis()} if the ticker is not
   * running — but never below the last cached value: the fallback keeps the
   * same monotonic floor, so a restart backoff cannot make time step backwards.
   *
   * @return current time in milliseconds (epoch-based, wall clock)
   */
  public static long currentTimeMillis() {
    long cached = currentMillis;
    // Running: the cached value already includes wallOffsetMs. One volatile
    // read, no second offset read and no JNI call.
    if (cached > 0L) {
      return cached;
    }
    // Stopped: ~cached is the raw monotonic floor; the test offset applies on
    // top of it.
    return Math.max(~cached, System.currentTimeMillis()) + wallOffsetMs;
  }

  /**
   * Returns the current time in milliseconds on a monotonic axis that never
   * moves backwards, anchored to the wall clock at JVM start.
   * <p>
   * Implemented as {@code bootWallMs + (System.nanoTime() - bootNano) / 1e6},
   * so wall-clock jumps (NTP steps, container clock drift) do not affect
   * elapsed-time computations. Use this source for <em>all delta
   * computations</em> (heartbeat timeouts, sliding windows, backoff, EMA
   * decay intervals, uptime). Keep {@link #currentTimeMillis()} for absolute
   * expiry timestamps, epoch/version stamping, and display — those semantics
   * inherently require the wall clock.
   * <p>
   * The returned value approximates the wall clock (deviation grows only
   * with wall-clock jumps), which keeps logs and diagnostics readable.
   *
   * @return current time in milliseconds on a monotonic axis
   */
  public static long monotonicMillis() {
    return BOOT_WALL_MS + (System.nanoTime() - BOOT_NANO) / 1_000_000L + monoOffsetMs;
  }

  /**
   * Seconds since JVM boot on the monotonic axis ({@code System.nanoTime()}
   * based, never moves backwards). Fits an {@code int} for ~68 years; used
   * for compact entry-age stamps where second precision suffices.
   *
   * <p>Never returns {@code 0}: that value is reserved as the "unknown birth
   * stamp" sentinel on {@code CacheEntry.getBirthStampSec()}, so the first boot
   * second reports {@code 1} instead. The one-second lie only affects a stamp
   * whose whole purpose is a minute-scale staleness bound.
   *
   * @return monotonic seconds since boot, at least {@code 1}
   */
  public static int monotonicSecondsSinceBoot() {
    // The test offset participates with the same millisecond unit it applies
    // to monotonicMillis(), so a simulated monotonic jump moves entry-age
    // stamps (birthStampSec) too.
    int seconds = (int) ((System.nanoTime() - BOOT_NANO) / 1_000_000_000L + monoOffsetMs / 1000L);
    return Math.max(1, seconds);
  }

  /**
   * Test-only hook to simulate wall-clock jumps (e.g. NTP steps) in regression
   * tests. A positive {@code wallOffsetMs} simulates a forward jump, a negative
   * one a backward jump; the monotonic axis is offset independently so tests can
   * model "wall clock jumped, monotonic did not". Reset with
   * {@code setTimeOffsetForTest(0, 0)}. No effect in production (defaults are
   * zero).
   *
   * @param wallOffsetMs offset applied to {@link #currentTimeMillis()}
   * @param monoOffsetMs offset applied to {@link #monotonicMillis()}
   */
  public static synchronized void setTimeOffsetForTest(long wallOffsetMs, long monoOffsetMs) {
    TimeSource.monoOffsetMs = monoOffsetMs;
    long delta = wallOffsetMs - TimeSource.wallOffsetMs;
    // Publish the offset before the cached value derived from it: a reader that observes the
    // shifted value is guaranteed to see the new offset, never the previous one. The class
    // monitor keeps start()/stopTicker() out of this section.
    TimeSource.wallOffsetMs = wallOffsetMs;
    if (delta != 0L) {
      // The running state already contains the old offset, so apply the delta
      // to the cached value. The stopped state stores a raw floor and applies
      // the offset on read, so it needs no adjustment.
      for (;;) {
        long cached = currentMillis;
        if (cached <= 0L) {
          break;
        }
        long next = cached + delta;
        if (next <= 0L) {
          // Test-only safety: never let a synthetic offset flip the running
          // sign and masquerade as a stopped ticker.
          next = 1L;
        }
        if (CURRENT_MILLIS.compareAndSet(cached, next)) {
          break;
        }
      }
    }
  }

  private TimeSource() {}
}
