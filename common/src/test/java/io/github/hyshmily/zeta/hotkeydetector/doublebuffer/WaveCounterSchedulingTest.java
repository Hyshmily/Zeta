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
package io.github.hyshmily.zeta.hotkeydetector.doublebuffer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.util.TimeSource;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Earliest-first tide scheduling with nudge coalescing (Caffeine's
 * {@code Pacer} policy): at most one tide is pending, a meaningfully
 * earlier {@code nudgeTide()} cancels and re-arms it, requests within the
 * tolerance band merge into the pending fire, degenerate delays are
 * clamped up to {@code EARLY_TIDE_MIN_INTERVAL_MS}, and the 0-sentinel
 * keeps "unscheduled" distinct from a committed fire time.
 */
class WaveCounterSchedulingTest {

  private RecordingScheduler scheduler;
  private WaveCounter counter;

  @BeforeEach
  void setUp() {
    scheduler = new RecordingScheduler();
    counter = new WaveCounter(ignored -> {}, scheduler);
  }

  /**
   * Earliest-first: a nudge (50ms) meaningfully earlier than the pending
   * base-interval tide (500ms) cancels it and re-arms at the minimum.
   */
  @Test
  void nudge_reschedulesEarliest() {
    counter.afterPropertiesSet();
    assertThat(scheduler.delaysMillis).containsExactly(500L);

    counter.nudgeTide();
    assertThat(scheduler.delaysMillis).containsExactly(500L, 50L);
  }

  /**
   * Coalescing: a second nudge while the re-armed tide is still pending
   * (fire time unchanged, well inside the tolerance band) is merged —
   * multiple nudges produce at most one schedule.
   */
  @Test
  void repeatedNudges_coalesce() {
    counter.afterPropertiesSet();
    counter.nudgeTide();
    counter.nudgeTide();
    counter.nudgeTide();
    assertThat(scheduler.delaysMillis).containsExactly(500L, 50L);
  }

  /**
   * Coalescing boundary: a nudge arriving just before the pending fire
   * (still in the future, within the tolerance band) is skipped.
   *
   * <p>Time is simulated instead of slept: {@code Thread.sleep} is
   * timer-granular and overshoots under CI load (the pending 50ms fire
   * would expire and the nudge would legitimately re-arm), making the
   * boundary race nondeterministic.  Planting {@code nextFireTimeMs} at
   * {@code now + 20ms} exercises the same comparison on the same code
   * path with no wall-clock dependency.
   */
  @Test
  void nudge_withinToleranceBandOfPendingFire_skips() throws Exception {
    counter.afterPropertiesSet();
    counter.nudgeTide(); // pending fires at now + 50ms
    Field f = WaveCounter.class.getDeclaredField("nextFireTimeMs");
    f.setAccessible(true);
    f.setLong(counter, TimeSource.monotonicMillis() + 20L); // pending in ~20ms
    counter.nudgeTide(); // proposed now + 50ms: 30ms later than pending: in band
    assertThat(scheduler.delaysMillis).containsExactly(500L, 50L);
  }

  /**
   * Earliest-first recovery: once the pending fire time has passed, the
   * next nudge re-arms a fresh 50ms tide instead of skipping.
   */
  @Test
  void nudge_afterPendingFirePassed_reschedules() throws Exception {
    counter.afterPropertiesSet();
    counter.nudgeTide(); // pending fires at now + 50ms
    Thread.sleep(120);
    counter.nudgeTide(); // pending fire already past: reschedule
    assertThat(scheduler.delaysMillis).containsExactly(500L, 50L, 50L);
  }

  /**
   * Clamp/sentinel defense: a degenerate (sub-minimum) delay is clamped up
   * to the 50ms floor — never armed at 0.  The second invocation happens
   * after the first pending fire has passed, so it re-arms instead of
   * coalescing into the still-pending one.
   */
  @Test
  void degenerateDelay_isClampedUp() throws Exception {
    Method m = WaveCounter.class.getDeclaredMethod("scheduleTide", long.class);
    m.setAccessible(true);
    m.invoke(counter, 0L);
    assertThat(scheduler.delaysMillis).containsExactly(50L);
    Thread.sleep(120);
    m.invoke(counter, -7L);
    assertThat(scheduler.delaysMillis).containsExactly(50L, 50L);
  }

  /**
   * Destroy cancels the pending tide, and a post-destroy nudge is a no-op
   * (the shutdown flag short-circuits before arming).
   */
  @Test
  void destroy_cancelsPending_andNudgeNoOps() {
    counter.afterPropertiesSet();
    counter.destroy();
    counter.nudgeTide();
    assertThat(scheduler.delaysMillis).containsExactly(500L);
  }

  /**
   * Tide-failure log rate limit: a persistently throwing consumer fires a
   * tide every ~50ms, so an unthrottled full-stack ERROR per tide would
   * flood the log unboundedly. The FIRST failure opens the 10s window
   * (ERROR with stack), failures inside the window are suppressed (DEBUG),
   * and the window-opening repeat reports the suppressed count. Asserted
   * log-free via the rate-limit state; the +1 schedule per tide proves the
   * self-reschedule in the finally keeps the delivery chain alive through
   * every failure.
   */
  @Test
  void tideFailure_errorLogIsRateLimited_andChainReArms() throws Exception {
    WaveCounter failing = new WaveCounter(
      ignored -> {
        throw new IllegalStateException("boom");
      },
      scheduler
    );
    failing.afterPropertiesSet();
    // The finally re-arm in tide() is guarded by deliveryStarted so
    // reflection-driven tides in tests never arm a background chain — arm it
    // explicitly so the +1-schedule-per-tide assertion below is meaningful.
    Field deliveryStartedField = WaveCounter.class.getDeclaredField("deliveryStarted");
    deliveryStartedField.setAccessible(true);
    deliveryStartedField.setBoolean(failing, true);
    int schedulesBefore = scheduler.delaysMillis.size();

    // Plant an expired window so the FIRST failure surely opens it. The stamp now
    // lives inside a LogThrottle (an AtomicLong claimed by compare-and-set), so the
    // test reaches through the throttle to it rather than touching the field directly.
    Field throttleField = WaveCounter.class.getDeclaredField("tideErrorLogThrottle");
    throttleField.setAccessible(true);
    Object throttle = throttleField.get(failing);
    Field openedAtField = throttle.getClass().getDeclaredField("lastAcquiredAtMs");
    openedAtField.setAccessible(true);
    AtomicLong openedAt = (AtomicLong) openedAtField.get(throttle);
    long before = TimeSource.monotonicMillis();
    openedAt.set(before - 60_000L);

    Method tide = WaveCounter.class.getDeclaredMethod("tide");
    tide.setAccessible(true);
    // In production the pending one-shot future has FIRED by the time tide()'s
    // finally re-arms (isDone → the coalescing pacer falls through and schedules
    // the next tick). The recording scheduler never fires futures, so drop the
    // pending reference BEFORE each tide to mimic that fired state — otherwise
    // the equal-cadence re-arm request is absorbed by the still-pending fire.
    Field pendingTideField = WaveCounter.class.getDeclaredField("pendingTide");
    pendingTideField.setAccessible(true);
    for (int i = 0; i < 3; i++) {
      pendingTideField.set(failing, null);
      failing.count("hot-key", 1); // non-empty snapshot → the consumer throws
      tide.invoke(failing); // 1st: window-opening ERROR (stack) — 2nd/3rd: suppressed
    }

    Field suppressedField = WaveCounter.class.getDeclaredField("tideErrorsSuppressed");
    suppressedField.setAccessible(true);
    AtomicLong suppressed = (AtomicLong) suppressedField.get(failing);
    assertThat(suppressed.get()).as("failures suppressed inside the window").isEqualTo(2);
    assertThat(openedAt.get()).as("window timestamp refreshed by the opening failure").isGreaterThanOrEqualTo(before);
    assertThat(scheduler.delaysMillis).as("delivery chain re-armed once per failing tide").hasSize(schedulesBefore + 3);
  }

  /** Records every one-shot schedule request without executing it. */
  static final class RecordingScheduler extends AbstractExecutorService implements ScheduledExecutorService {

    final List<Long> delaysMillis = new ArrayList<>();
    final List<Runnable> commands = new ArrayList<>();

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
      delaysMillis.add(unit.toMillis(delay));
      commands.add(command);
      return new PendingScheduledFuture();
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void execute(Runnable command) {}

    @Override
    public void shutdown() {}

    @Override
    public List<Runnable> shutdownNow() {
      return List.of();
    }

    @Override
    public boolean isShutdown() {
      return false;
    }

    @Override
    public boolean isTerminated() {
      return true;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return true;
    }
  }

  /** A scheduled task that stays pending (never completes) until cancelled. */
  static final class PendingScheduledFuture implements ScheduledFuture<Object> {

    private volatile boolean cancelled;

    @Override
    public long getDelay(TimeUnit unit) {
      return 0;
    }

    @Override
    public int compareTo(Delayed other) {
      return 0;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      cancelled = true;
      return true;
    }

    @Override
    public boolean isCancelled() {
      return cancelled;
    }

    @Override
    public boolean isDone() {
      return false;
    }

    @Override
    public Object get() {
      return null;
    }

    @Override
    public Object get(long timeout, TimeUnit unit) {
      return null;
    }
  }
}
