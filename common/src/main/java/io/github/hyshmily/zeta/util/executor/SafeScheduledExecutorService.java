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
package io.github.hyshmily.zeta.util.executor;

import io.github.hyshmily.zeta.Internal;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;

/**
 * A {@link ScheduledThreadPoolExecutor} whose periodic tasks are exception-safe and never overlap.
 *
 * <p><b>Deviations from JDK semantics:</b>
 * <ul>
 *   <li><b>Non-overlapping cadence.</b> Both {@link #scheduleAtFixedRate(Runnable, long, long, TimeUnit)}
 *       and {@link #scheduleWithFixedDelay(Runnable, long, long, TimeUnit)} schedule the next run
 *       <em>after</em> the current run completes, so the gap between the end of one run and the
 *       start of the next is the configured period/delay. If a run takes longer than the period,
 *       the cadence degrades to {@code period + execution time} instead of overlapping.
 *       This matches the behaviour of the JDK's {@code scheduleWithFixedDelay} for the delay case
 *       and replaces the JDK's start-to-start fixed-rate behaviour for the rate case.</li>
 *   <li><b>Exception tolerance.</b> If a task throws {@link Throwable}, the exception is logged
 *       at WARN level and the periodic chain continues. The JDK permanently cancels a periodic
 *       task after the first uncaught exception, which would silently kill cluster heartbeats,
 *       window sliding, and buffer flushes.</li>
 * </ul>
 *
 * <p>One-shot scheduling via {@link #schedule(Runnable, long, TimeUnit)} (and the callable
 * variant) is inherited unchanged from the JDK.
 *
 * <p>The returned {@link ScheduledFuture} tracks the chain: {@link #cancel(boolean)} stops all
 * future runs and optionally interrupts the currently executing run; {@code get()} and
 * {@code isDone()} reflect the state of the most recently scheduled run.
 */
@Slf4j
@Internal
public class SafeScheduledExecutorService extends ScheduledThreadPoolExecutor {

  /**
   * Creates a safe scheduled executor with the given core pool size.
   *
   * @param corePoolSize the number of threads to keep in the pool
   */
  public SafeScheduledExecutorService(int corePoolSize) {
    super(corePoolSize);
  }

  /**
   * Creates a safe scheduled executor with the given core pool size and thread factory.
   *
   * @param corePoolSize  the number of threads to keep in the pool
   * @param threadFactory the factory used when creating new threads
   */
  public SafeScheduledExecutorService(int corePoolSize, ThreadFactory threadFactory) {
    super(corePoolSize, threadFactory);
  }

  /**
   * Creates a safe scheduled executor with the given core pool size, thread factory and
   * rejected-execution handler.
   *
   * @param corePoolSize  the number of threads to keep in the pool
   * @param threadFactory the factory used when creating new threads
   * @param handler       the handler for tasks that cannot be executed
   */
  public SafeScheduledExecutorService(int corePoolSize, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
    super(corePoolSize, threadFactory, handler);
  }

  @Override
  @NonNull
  public ScheduledFuture<?> scheduleAtFixedRate(
    @NonNull Runnable command,
    long initialDelay,
    long period,
    @NonNull TimeUnit unit
  ) {
    return new SafePeriodicTask(command, unit.toNanos(period)).start(initialDelay, unit);
  }

  @Override
  @NonNull
  public ScheduledFuture<?> scheduleWithFixedDelay(
    @NonNull Runnable command,
    long initialDelay,
    long delay,
    @NonNull TimeUnit unit
  ) {
    return new SafePeriodicTask(command, unit.toNanos(delay)).start(initialDelay, unit);
  }

  /**
   * A single-run link of a periodic chain. Each run reschedules the next link after completing,
   * so the chain is exception-tolerant and never executes concurrently. The task is only ever
   * executed by this executor's threads, one link at a time.
   */
  @SuppressWarnings("java:S3077")
  private final class SafePeriodicTask implements ScheduledFuture<Void>, Runnable {

    private final Runnable command;
    private final long gapNanos;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private volatile ScheduledFuture<?> current;

    private SafePeriodicTask(Runnable command, long gapNanos) {
      this.command = command;
      this.gapNanos = gapNanos;
    }

    private ScheduledFuture<Void> start(long initialDelay, TimeUnit unit) {
      scheduleNext(unit.toNanos(initialDelay));
      return this;
    }

    @Override
    public void run() {
      if (cancelled.get()) {
        return;
      }
      try {
        command.run();
      } catch (Exception t) {
        log.warn("SafeScheduledExecutorService task threw; the next run is still scheduled", t);
      } finally {
        scheduleNext(gapNanos);
      }
    }

    private void scheduleNext(long delayNanos) {
      if (cancelled.get() || isShutdown()) {
        return;
      }
      try {
        current = schedule(this, delayNanos, TimeUnit.NANOSECONDS);
      } catch (RejectedExecutionException e) {
        // Executor is shutting down; the chain ends silently.
      }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      cancelled.set(true);
      ScheduledFuture<?> f = current;
      return f != null && f.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return cancelled.get();
    }

    @Override
    public boolean isDone() {
      return cancelled.get() || (current != null && current.isDone());
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
      ScheduledFuture<?> f = current;
      if (f == null) {
        throw new CancellationException("task was never scheduled");
      }
      f.get();
      return null;
    }

    @Override
    public Void get(long timeout, @NonNull TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
      ScheduledFuture<?> f = current;
      if (f == null) {
        throw new CancellationException("task was never scheduled");
      }
      f.get(timeout, unit);
      return null;
    }

    @Override
    public long getDelay(@NonNull TimeUnit unit) {
      ScheduledFuture<?> f = current;
      return f != null ? f.getDelay(unit) : 0L;
    }

    @Override
    public int compareTo(@NonNull Delayed other) {
      return Long.compare(getDelay(TimeUnit.NANOSECONDS), other.getDelay(TimeUnit.NANOSECONDS));
    }
  }
}
