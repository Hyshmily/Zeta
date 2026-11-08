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
package io.github.hyshmily.zeta.counter;

import static org.assertj.core.api.Assertions.assertThat;

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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tide scheduling defenses: degenerate delays are clamped up to
 * {@code EARLY_TIDE_MIN_INTERVAL_MS}, the 0-sentinel keeps "unscheduled"
 * distinct from a committed fire time, and the delivery chain re-arms through
 * every consumer failure so a persistently throwing consumer cannot kill it.
 */
class WaveCounterSchedulingTest {

  private RecordingScheduler scheduler;
  private WaveCounter counter;

  @BeforeEach
  void setUp() {
    scheduler = new RecordingScheduler();
    counter = new WaveCounter((ignored, windowMs) -> {}, scheduler);
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
   * The self-reschedule in {@code tide()}'s {@code finally} keeps the delivery
   * chain alive through every consumer failure: the +1 schedule per tide is the
   * evidence, with the failure logged once per tide.
   */
  @Test
  void tideFailure_chainReArmsOnEveryFailure() throws Exception {
    WaveCounter failing = new WaveCounter(
      (ignored, windowMs) -> {
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
      tide.invoke(failing);
    }

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
