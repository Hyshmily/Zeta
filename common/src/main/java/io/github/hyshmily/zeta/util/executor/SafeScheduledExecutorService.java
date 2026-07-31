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
import io.github.hyshmily.zeta.exception.ZetaExceptionHandler;
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
 *       <em>after</em> the current run completes, so a single task is never executed by two
 *       threads at the same time.</li>
 *   <li><b>Phase-anchored fixed rate.</b> {@code scheduleAtFixedRate} keeps the cadence anchored
 *       to the original phase instead of backsliding: after each run the next slot advances by
 *       exactly one {@code period} from the previous slot. If a run overshoots its slot, the
 *       missed ticks are <em>skipped</em> and the cadence is re-anchored to the next future slot
 *       (no back-to-back catch-up bursts, no cumulative drift). This matches the design of
 *       Threadly's {@code RecurringRateTaskWrapper} (cadence anchored to the schedule, never
 *       degraded to {@code period + execution time}) while deliberately avoiding its
 *       back-to-back catch-up behaviour, which would burst heartbeat and window-sliding tasks
 *       after a slow run.</li>
 *   <li><b>Exception tolerance.</b> If a task throws {@link Throwable}, the exception is routed
 *       through {@link ZetaExceptionHandler} (WARN log by default, injectable handler chain) and
 *       the periodic chain continues. The JDK permanently cancels a periodic task after the
 *       first uncaught exception, which would silently kill cluster heartbeats, window sliding,
 *       and buffer flushes.</li>
 *   <li><b>Cancellable periodic chains.</b> {@link #remove(Runnable)} unregisters a periodic
 *       chain started with {@code scheduleAtFixedRate} / {@code scheduleWithFixedDelay} by the
 *       original {@code command}, preventing all future runs. The JDK's {@code remove} cannot
 *       find the command because the executor only knows the internal chain wrapper.</li>
 * </ul>
 *
 * <p>One-shot scheduling via {@link #schedule(Runnable, long, TimeUnit)} (and the callable
 * variant) is inherited unchanged from the JDK.
 *
 * <p>The returned {@link ScheduledFuture} tracks the chain: {@link #cancel(boolean)} stops all
 * future runs and optionally interrupts the currently executing run — it returns {@code true}
 * once the chain is stopped, regardless of whether the link that happened to be executing at
 * that moment could be interrupted; {@code get()} and {@code isDone()} reflect the state of
 * the most recently scheduled run.
 */
@Slf4j
@Internal
public class SafeScheduledExecutorService extends ScheduledThreadPoolExecutor {

  /**
   * Registry of periodic chains keyed by the original {@code command}. Used by
   * {@link #remove(Runnable)}: the JDK's {@code DelayedWorkQueue} only knows the internal chain
   * wrapper, so removal has to go through this registry. Entries are removed when a chain ends
   * (cancellation, shutdown, or replacement by a re-registered command).
   */
  private final ConcurrentHashMap<Runnable, SafePeriodicTask> chains = new ConcurrentHashMap<>();

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
    if (period <= 0) {
      // Matches JDK validation. A non-positive period would otherwise produce a busy
      // self-rescheduling loop inside the chain.
      throw new IllegalArgumentException("period must be greater than zero");
    }
    return new SafePeriodicTask(command, unit.toNanos(period), true).start(initialDelay, unit);
  }

  @Override
  @NonNull
  public ScheduledFuture<?> scheduleWithFixedDelay(
    @NonNull Runnable command,
    long initialDelay,
    long delay,
    @NonNull TimeUnit unit
  ) {
    if (delay <= 0) {
      // Matches JDK validation. A non-positive delay would otherwise produce a busy
      // self-rescheduling loop inside the chain.
      throw new IllegalArgumentException("delay must be greater than zero");
    }
    return new SafePeriodicTask(command, unit.toNanos(delay), false).start(initialDelay, unit);
  }

  /**
   * Unregisters a periodic task started via {@link #scheduleAtFixedRate(Runnable, long, long, TimeUnit)}
   * or {@link #scheduleWithFixedDelay(Runnable, long, long, TimeUnit)}, preventing all future runs.
   *
   * <p>Semantics differ from the JDK: the JDK's {@code remove} plucks a task out of the
   * {@code DelayedWorkQueue}, but it can only find the internal chain wrapper, never the original
   * {@code command}. This override looks the chain up in the registry by the original command
   * and cancels the whole chain. A currently executing run is allowed to finish (use
   * {@code cancel(true)} on the returned future to interrupt it instead).
   *
   * <p>One-shot tasks scheduled via {@link #schedule(Runnable, long, TimeUnit)} keep the JDK
   * behaviour (this registry only contains periodic chains).
   *
   * @param task the original command that was registered as a periodic task
   * @return {@code true} if the command was registered as a periodic chain and the chain was
   *         cancelled; {@code false} if the command is unknown
   */
  @Override
  public boolean remove(@NonNull Runnable task) {
    SafePeriodicTask chain = chains.remove(task);
    return chain != null && chain.cancel(false);
  }

  /**
   * A single-run link of a periodic chain. Each run reschedules the next link after completing,
   * so the chain is exception-tolerant and never executes concurrently. The task is only ever
   * executed by this executor's threads, one link at a time.
   *
   * <p>Two scheduling modes:
   * <ul>
   *   <li><b>Fixed delay</b> ({@code rateMode == false}): the gap between the end of one run and
   *       the start of the next is the configured delay — the historical Zeta behaviour.</li>
   *   <li><b>Phase-anchored rate</b> ({@code rateMode == true}): {@code nextRunTimeNanos} starts
   *       at {@code now + initialDelay} and advances by exactly one {@code period} per run, so
   *       the cadence never drifts. If the run overshoots the slot, the missed tick is skipped
   *       and the cadence re-anchors to {@code now + period} instead of firing back-to-back.</li>
   * </ul>
   */
  // Suppressing java:S3077/S3078: the volatile `current` reference is only ever REASSIGNED
  // (the ScheduledFuture object itself is thread-safe by JDK contract — we never mutate its
  // internals), and `nextRunTimeNanos` is read/written exclusively along the chain's serialized
  // execution path where the schedule() submission establishes the happens-before edge.
  @SuppressWarnings({"java:S3077", "java:S3078"})
  private final class SafePeriodicTask implements ScheduledFuture<Void>, Runnable {

    private final Runnable command;
    private final long gapNanos;
    private final boolean rateMode;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    /**
     * Absolute slot of the next run, in {@link System#nanoTime()} scale. Only meaningful in rate
     * mode. It is read and written exclusively along the chain's own serialized execution path
     * (the {@code schedule()} submission establishes a happens-before edge between links), so a
     * plain field is safe.
     */
    private long nextRunTimeNanos;
    private volatile ScheduledFuture<?> current;

    private SafePeriodicTask(Runnable command, long gapNanos, boolean rateMode) {
      this.command = command;
      this.gapNanos = gapNanos;
      this.rateMode = rateMode;
      SafePeriodicTask previous = chains.put(command, this);
      if (previous != null) {
        // The same command was registered twice: the previous chain must stop, otherwise both
        // chains would keep rescheduling the same command indefinitely.
        previous.cancel(false);
      }
    }

    private ScheduledFuture<Void> start(long initialDelay, TimeUnit unit) {
      long initialDelayNanos = unit.toNanos(initialDelay);
      if (rateMode) {
        // Anchor the cadence phase: the first slot is now + initialDelay, and every subsequent
        // slot advances by exactly one period from here (see scheduleNext()).
        nextRunTimeNanos = System.nanoTime() + initialDelayNanos;
      }
      scheduleNext(initialDelayNanos);
      return this;
    }

    @Override
    @SuppressWarnings("java:S1181")
    // catching Throwable is deliberate: an Error must not kill the cadence of
    // heartbeats/window-slides/flushes, and the chain link must never leak an exception into the worker loop
    public void run() {
      if (cancelled.get()) {
        return;
      }
      try {
        command.run();
      } catch (Throwable t) {
        // Exception tolerance: the throwable is reported through the injectable handler chain
        // (WARN log by default) and the finally block below keeps the chain alive. The JDK
        // would permanently cancel the task on the first uncaught exception.
        ZetaExceptionHandler.handleException(
          "SafeScheduledExecutorService task threw; the next run is still scheduled",
          t
        );
      } finally {
        scheduleNext();
      }
    }

    /**
     * Schedules the next link of the chain, deriving the delay from this task's mode:
     * <ul>
     *   <li><b>Fixed delay</b> — the configured gap measured from the completion of the previous
     *       run (historical Zeta behaviour).</li>
     *   <li><b>Phase-anchored rate</b> — {@code nextRunTimeNanos} advanced by exactly one period
     *       from the previous slot; a run that overshot its slot skips the missed tick (no
     *       back-to-back catch-up burst) and re-anchors to the next future slot.</li>
     * </ul>
     * The chain ends silently when the executor shuts down.
     */
    private void scheduleNext() {
      if (cancelled.get() || isShutdown()) {
        unregister();
        return;
      }
      long delayNanos;
      if (rateMode) {
        nextRunTimeNanos += gapNanos;
        delayNanos = nextRunTimeNanos - System.nanoTime();
        if (delayNanos <= 0) {
          // The previous run overshot its slot. Skip the missed tick (no back-to-back catch-up
          // burst) and re-anchor the cadence to the next future slot.
          nextRunTimeNanos = System.nanoTime() + gapNanos;
          delayNanos = gapNanos;
        }
      } else {
        // Fixed delay: the gap between the end of one run and the start of the next.
        delayNanos = gapNanos;
      }
      try {
        current = schedule(this, delayNanos, TimeUnit.NANOSECONDS);
      } catch (RejectedExecutionException e) {
        // Executor is shutting down; the chain ends silently.
        unregister();
      }
    }

    /**
     * Schedules the next link with an explicit delay, used for the initial link (which carries
     * {@code initialDelay} rather than a mode-derived delay).
     *
     * @param explicitDelayNanos the delay for this link
     */
    private void scheduleNext(long explicitDelayNanos) {
      if (cancelled.get() || isShutdown()) {
        unregister();
        return;
      }
      try {
        current = schedule(this, explicitDelayNanos, TimeUnit.NANOSECONDS);
      } catch (RejectedExecutionException e) {
        // Executor is shutting down; the chain ends silently.
        unregister();
      }
    }

    /**
     * Removes this chain from the registry. Safe to call multiple times — a chain that already
     * deregistered is a no-op.
     */
    private void unregister() {
      chains.remove(command, this);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      cancelled.set(true);
      unregister();
      ScheduledFuture<?> f = current;
      if (f != null) {
        // Best-effort cancellation of the link that is currently queued or running. The return
        // value is deliberately ignored: it only reflects the state of that single link (a
        // running link cannot be cancelled without interruption), while the chain itself is
        // already stopped by the cancelled flag above.
        f.cancel(mayInterruptIfRunning);
      }
      return true;
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
