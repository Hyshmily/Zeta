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

import io.github.hyshmily.zeta.exception.ZetaExceptionHandler;
import io.github.hyshmily.zeta.util.Internal;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.List;
import java.util.concurrent.*;
import org.jspecify.annotations.NonNull;
import org.springframework.util.Assert;

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
 *       missed ticks are <em>skipped</em> and the cadence lands on the next slot of the original
 *       grid (no back-to-back catch-up bursts, no cumulative drift). This matches the design of
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
 *   <li><b>Terminal {@code isDone()}.</b> The returned future reports {@code isDone() == true}
 *       only when the chain has stopped for good — cancelled, or ended by executor shutdown —
 *       never merely because the current link finished, which would flicker between the links
 *       of a healthy chain. {@code get()} still reflects the most recently scheduled run.</li>
 * </ul>
 *
 * <p>One-shot scheduling via {@link #schedule(Runnable, long, TimeUnit)} (and the callable
 * variant) is inherited unchanged from the JDK.
 *
 * <p>The returned {@link ScheduledFuture} tracks the chain: {@link ScheduledFuture#cancel(boolean)} stops all
 * future runs and optionally interrupts the currently executing run — it returns {@code true}
 * once the chain is stopped, regardless of whether the link that happened to be executing at
 * that moment could be interrupted; {@code get()} reflects the most recently scheduled run,
 * while {@code isDone()} reports the chain's terminal state (cancelled or ended by shutdown).
 */
@Internal
public class SafeScheduledExecutorService extends ScheduledThreadPoolExecutor {

  /** Chain lifecycle codes; {@link #CANCELLED_BIT} is set by both cancelled states. */
  private static final int ACTIVE = 0;
  private static final int TERMINATED = 1;
  private static final int CANCELLED_NO_INTERRUPT = 2;
  private static final int CANCELLED_INTERRUPT = 3;
  private static final int CANCELLED_BIT = 2;

  /** CAS handle for {@link SafePeriodicTask#state}. */
  private static final VarHandle PERIODIC_STATE;

  static {
    try {
      PERIODIC_STATE = MethodHandles.privateLookupIn(SafePeriodicTask.class, MethodHandles.lookup()).findVarHandle(
        SafePeriodicTask.class,
        "state",
        int.class
      );
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Registry of periodic chains keyed by the original {@code command}. Used by
   * {@link #remove(Runnable)}: the JDK's {@code DelayedWorkQueue} only knows the internal chain
   * wrapper, so removal has to go through this registry. Entries are removed when a chain ends
   * (cancellation, shutdown, or replacement by a re-registered command).
   */
  private final ConcurrentHashMap<Runnable, SafePeriodicTask> chains = new ConcurrentHashMap<>();

  /**
   * Creates a safe scheduled executor with the given core pool size and thread factory.
   *
   * @param corePoolSize  the number of threads to keep in the pool
   * @param threadFactory the factory used when creating new threads
   */
  public SafeScheduledExecutorService(int corePoolSize, ThreadFactory threadFactory) {
    super(corePoolSize, threadFactory);
    // Cancelled chain links must leave the delay queue immediately. Without this the JDK keeps
    // the cancelled ScheduledFutureTask — and every object the link closes over — queued for its
    // full delay, and a link cancelled concurrently with shutdown() keeps the pool from reaching
    // TERMINATED until that delay expires.
    setRemoveOnCancelPolicy(true);
  }

  /**
   * Publishes a periodic chain's next link before the executor makes it visible to a worker.
   *
   * <p>{@link ScheduledThreadPoolExecutor#schedule} calls this hook before
   * {@code delayedExecute}, so the chain's {@code current} reference is already the queued link
   * when a worker can take it. A link that runs immediately and schedules its successor therefore
   * cannot be overwritten by the scheduling thread's post-{@code schedule} assignment, and an
   * external {@code get()} cannot observe the first link before it is installed.
   *
   * @param runnable the caller's command
   * @param task     the executor's scheduled wrapper
   * @param <V>      the future's result type
   * @return {@code task}, unchanged
   */
  @Override
  protected <V> RunnableScheduledFuture<V> decorateTask(Runnable runnable, RunnableScheduledFuture<V> task) {
    if (runnable instanceof SafePeriodicTask periodic) {
      periodic.current = task;
    }
    return task;
  }

  @Override
  @NonNull
  public ScheduledFuture<?> scheduleAtFixedRate(
    @NonNull Runnable command,
    long initialDelay,
    long period,
    @NonNull TimeUnit unit
  ) {
    // Matches JDK validation. A non-positive period would otherwise produce a busy
    // self-rescheduling loop inside the chain.
    Assert.isTrue(period > 0, "period must be greater than zero");
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
    // Matches JDK validation. A non-positive delay would otherwise produce a busy
    // self-rescheduling loop inside the chain.
    Assert.isTrue(delay > 0, "delay must be greater than zero");
    // Negative gap = completion-relative delay; positive = phase-anchored rate.
    return new SafePeriodicTask(command, -unit.toNanos(delay)).start(initialDelay, unit);
  }

  /**
   * Removes a periodic chain started by the original {@code command}, in addition to
   * the JDK's one-shot task removal.
   *
   * <p>The JDK's queue only knows the internal chain wrapper, so it cannot match the
   * command the caller scheduled. The registry resolves the chain and cancels it: no
   * later link runs, and the chain's future reports done.
   *
   * @param task the original command, or {@code null}
   * @return {@code true} when a queued one-shot task or a registered chain was removed
   */
  @Override
  public boolean remove(Runnable task) {
    if (task != null) {
      SafePeriodicTask chain = chains.remove(task);
      if (chain != null) {
        return chain.cancel(false);
      }
    }
    return super.remove(task);
  }

  /**
   * Initiates an orderly shutdown and terminates every registered periodic
   * chain.
   *
   * <p>A chain waits for its next run as an ordinary <em>one-shot delayed</em>
   * task, so the JDK's shutdown policy ({@code
   * executeExistingDelayedTasksAfterShutdown = true}, {@code
   * continueExistingPeriodicTasksAfterShutdown = false}) would keep it queued
   * for a full period and then run the command one last time. That contradicts
   * the class contract: a periodic chain ends at shutdown, and its future
   * reports {@code isDone() == true} from then on. Terminating the chains
   * explicitly also clears the registry, so neither the chain object nor the
   * task it closes over is retained by a drained queue.
   *
   * <p>A run already in progress is allowed to finish (JDK {@code shutdown}
   * semantics); only the next link is cancelled.
   */
  @Override
  public void shutdown() {
    terminateChains();
    super.shutdown();
  }

  /**
   * Initiates an immediate shutdown, terminating every registered periodic
   * chain and returning the tasks that never commenced execution.
   *
   * @return the queued tasks that were drained without running
   */
  @Override
  @NonNull
  public List<Runnable> shutdownNow() {
    terminateChains();
    return super.shutdownNow();
  }

  /**
   * Ends every registered periodic chain: marks it terminal, cancels its
   * queued next link (a running link is not interrupted) and drops it from the
   * registry.
   */
  private void terminateChains() {
    for (SafePeriodicTask chain : chains.values()) {
      chain.terminate();
    }
    chains.clear();
  }

  /**
   * A single-run link of a periodic chain. Each run reschedules the next link after completing,
   * so the chain is exception-tolerant and never executes concurrently. The task is only ever
   * executed by this executor's threads, one link at a time.
   *
   * <p>Two scheduling modes, encoded in the sign of {@link #gapNanos}:
   * <ul>
   *   <li><b>Fixed delay</b> ({@code gapNanos < 0}): the gap between the end of one run and
   *       the start of the next is {@code -gapNanos}.</li>
   *   <li><b>Phase-anchored rate</b> ({@code gapNanos > 0}): {@code nextRunTimeNanos} starts
   *       at {@code now + initialDelay} and advances by exactly one {@code period} per run, so
   *       the cadence never drifts. If the run overshoots the slot, the missed ticks are skipped
   *       and the cadence lands on the next slot of the original grid instead of firing
   *       back-to-back.</li>
   * </ul>
   */
  // Suppressing java:S3077/S3078: the volatile `current` reference is only ever REASSIGNED
  // (the ScheduledFuture object itself is thread-safe by JDK contract — we never mutate its
  // internals), and `nextRunTimeNanos` is read/written exclusively along the chain's serialized
  // execution path where the schedule() submission establishes the happens-before edge.
  @SuppressWarnings({ "java:S3077", "java:S3078" })
  private final class SafePeriodicTask implements ScheduledFuture<Void>, Runnable {

    private final Runnable command;
    /** Positive = phase-anchored rate; negative = completion-relative delay. */
    private final long gapNanos;

    /**
     * The newest link. {@link SafeScheduledExecutorService#decorateTask} installs it before the
     * executor enqueues the wrapper, and the scheduling thread never assigns it after
     * {@code schedule()} returns; a fast run that schedules its successor therefore cannot be
     * overwritten by the previous link's scheduler.
     */
    private volatile ScheduledFuture<?> current;

    /**
     * Absolute slot of the next run, in {@link System#nanoTime()} scale. Only meaningful in rate
     * mode. It is read and written exclusively along the chain's own serialized execution path
     * (the {@code schedule()} submission establishes a happens-before edge between links), so a
     * plain field is safe.
     */
    private long nextRunTimeNanos;

    /**
     * Lifecycle code: {@link #ACTIVE}, {@link #TERMINATED},
     * {@link #CANCELLED_NO_INTERRUPT} or {@link #CANCELLED_INTERRUPT}. Accessed
     * through {@link #PERIODIC_STATE} so the compound cancel transition is one CAS
     * and every read is volatile. A terminal code distinguishes shutdown from
     * cancellation and keeps the caller's interrupt request for the compensating
     * cancel in {@link #scheduleLink}.
     */
    @SuppressWarnings("unused")
    private int state;

    private SafePeriodicTask(Runnable command, long gapNanos) {
      this.command = command;
      this.gapNanos = gapNanos;
      // Registration happens before start() by design (the double-registration
      // guard below needs the registry slot). A remove() landing in this
      // window cancels the chain, and start()'s scheduleLink then sees
      // cancelled and submits nothing. The one racy interleave — cancel
      // between the scheduleLink check and the schedule submission — submits
      // a single link that observes cancelled at run() entry and stops there,
      // so no run ever escapes a cancellation that preceded it.
      SafePeriodicTask previous = chains.put(command, this);
      if (previous != null) {
        // The same command was registered twice: the previous chain must stop, otherwise both
        // chains would keep rescheduling the same command indefinitely.
        previous.cancel(false);
      }
    }

    private ScheduledFuture<Void> start(long initialDelay, TimeUnit unit) {
      // The JDK treats a negative initial delay as "run now"; matching that here keeps the
      // phase anchor from landing in the past and skewing every later slot.
      long initialDelayNanos = Math.max(0L, unit.toNanos(initialDelay));
      if (gapNanos > 0L) {
        // Anchor the cadence phase: the first slot is now + initialDelay, and every subsequent
        // slot advances by exactly one period from here (see scheduleNext()).
        nextRunTimeNanos = System.nanoTime() + initialDelayNanos;
      }
      scheduleLink(initialDelayNanos);
      return this;
    }

    private int stateCode() {
      return (int) PERIODIC_STATE.getVolatile(this);
    }

    @Override
    @SuppressWarnings("all")
    // catching Throwable is deliberate: an Error must not kill the cadence of
    // heartbeats/window-slides/flushes, and the chain link must never leak an exception into the worker loop
    public void run() {
      if (stateCode() != ACTIVE) {
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
        // A task that caught InterruptedException and restored the flag (or one
        // interrupted by cancel(true)) leaves it on the pooled thread. The JDK
        // clears the flag only on shutdown, so without this cleanup the next
        // unrelated task scheduled on this thread fails on its first blocking call.
        // Kept while shutting down: shutdownNow's interrupt must survive.
        if (!isShutdown()) {
          Thread.interrupted();
        }
        scheduleNext();
      }
    }

    /**
     * Schedules the next link of the chain, deriving the delay from this task's mode:
     * <ul>
     *   <li><b>Fixed delay</b> — the configured gap measured from the completion of the previous
     *       run.</li>
     *   <li><b>Phase-anchored rate</b> — {@code nextRunTimeNanos} advanced by exactly one period
     *       from the previous slot; a run that overshot its slot skips the missed ticks (no
     *       back-to-back catch-up burst) and lands on the next slot of the original grid.</li>
     * </ul>
     * The chain ends silently when the executor shuts down.
     */
    private void scheduleNext() {
      long delayNanos;
      if (gapNanos > 0L) {
        long now = System.nanoTime();
        nextRunTimeNanos += gapNanos;
        if (nextRunTimeNanos - now <= 0) {
          // The previous run overshot one or more slots. Skip every missed slot
          // and stay on the ORIGINAL phase grid: resetting the anchor to
          // now + period would degrade the cadence to fixed-delay and lose the
          // grid the caller configured (an hourly boundary-aligned task would
          // drift by the execution time on every run).
          long missed = (now - nextRunTimeNanos) / gapNanos + 1L;
          nextRunTimeNanos += missed * gapNanos;
        }
        delayNanos = nextRunTimeNanos - now;
      } else {
        delayNanos = -gapNanos;
      }
      scheduleLink(delayNanos);
    }

    private void scheduleLink(long delayNanos) {
      int code = stateCode();
      if (code != ACTIVE) {
        unregister();
        return;
      }
      if (isShutdown()) {
        // The executor refused the chain while it was healthy: terminal end,
        // visible through isDone().
        PERIODIC_STATE.compareAndSet(this, ACTIVE, TERMINATED);
        unregister();
        return;
      }
      try {
        // decorateTask installs `current` before delayedExecute enqueues the link. Assigning the
        // return value here would republish a link that an immediate run has already superseded.
        schedule(this, delayNanos, TimeUnit.NANOSECONDS);
      } catch (RejectedExecutionException e) {
        // Executor is shutting down; the chain ends terminally. decorateTask has already
        // installed a wrapper the queue never accepted: cancel it so a get() that raced the
        // rejection observes a completed link, and leave the reference in place — clearing it
        // would turn a terminal chain into "never scheduled".
        PERIODIC_STATE.compareAndSet(this, ACTIVE, TERMINATED);
        ScheduledFuture<?> rejected = current;
        if (rejected != null) {
          rejected.cancel(false);
        }
        unregister();
        return;
      }
      // cancel()/terminate() may have raced the assignment above: it cancelled
      // the OLD link while this new one was being queued. The state code is the
      // authority — drop the freshly queued link too, or it (and every object
      // the task closes over) lingers in the delay queue for its full period
      // before the run() guard discards it.
      code = stateCode();
      if (code != ACTIVE) {
        current.cancel(code == CANCELLED_INTERRUPT);
        unregister();
      }
    }

    /**
     * Terminal end of the chain at executor shutdown: mark it done, drop the
     * queued next link (a running link is left to finish) and deregister it.
     * Idempotent.
     */
    private void terminate() {
      int code = stateCode();
      if (code == ACTIVE) {
        PERIODIC_STATE.compareAndSet(this, ACTIVE, TERMINATED);
        // Re-read: a concurrent cancel(true) wins over the shutdown transition
        // and must keep its interrupt request.
        code = stateCode();
      }
      finish(code);
    }

    /**
     * Cancels the queued or running link with the terminal code's interrupt request and
     * drops the chain from the registry. The caller owns the state transition.
     *
     * @param code the terminal state observed after the transition
     */
    private void finish(int code) {
      ScheduledFuture<?> link = current;
      if (link != null) {
        // Best-effort cancellation of the link that is currently queued or running. The return
        // value is deliberately ignored: it only reflects the state of that single link (a
        // running link cannot be cancelled without interruption), while the chain itself is
        // already stopped by the state code.
        link.cancel(code == CANCELLED_INTERRUPT);
      }
      unregister();
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
      // Future contract: a chain that already stopped cannot be cancelled again.
      // One CAS publishes the cancellation and the interrupt request together,
      // so no monitor is needed and the compensating cancel cannot observe a
      // half-written pair.
      int target = mayInterruptIfRunning ? CANCELLED_INTERRUPT : CANCELLED_NO_INTERRUPT;
      if (!PERIODIC_STATE.compareAndSet(this, ACTIVE, target)) {
        return false;
      }
      finish(target);
      return true;
    }

    @Override
    public boolean isCancelled() {
      return (stateCode() & CANCELLED_BIT) != 0;
    }

    @Override
    public boolean isDone() {
      // Terminal state only: a periodic chain is done when it has stopped for
      // good — cancelled, or ended by executor shutdown. Judging the current
      // link instead would read done between a link finishing and
      // scheduleNext() swapping in the next one (false → true → false on a
      // healthy chain).
      return stateCode() != ACTIVE;
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
      ScheduledFuture<?> link = current;
      if (link != null) {
        try {
          link.get();
        } catch (CancellationException e) {
          // The link was cancelled because the chain stopped, not because a caller
          // cancelled the chain: a shutdown-terminated chain is done, not cancelled.
          if (!isCancelled()) {
            return null;
          }
          throw e;
        }
      }
      return settled();
    }

    @Override
    public Void get(long timeout, @NonNull TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
      ScheduledFuture<?> link = current;
      if (link != null) {
        try {
          link.get(timeout, unit);
        } catch (CancellationException e) {
          if (!isCancelled()) {
            return null;
          }
          throw e;
        }
      }
      return settled();
    }

    /** The chain's post-link result: {@code null}, or the cancellation the {@code Future} contract requires. */
    private Void settled() {
      if (isCancelled()) {
        throw new CancellationException("periodic task was cancelled");
      }
      return null;
    }

    @Override
    public long getDelay(@NonNull TimeUnit unit) {
      ScheduledFuture<?> link = current;
      return link != null ? link.getDelay(unit) : 0L;
    }

    @Override
    public int compareTo(@NonNull Delayed other) {
      // compareTo contract: identity compares equal without consulting getDelay().
      return other == this ? 0 : Long.compare(getDelay(TimeUnit.NANOSECONDS), other.getDelay(TimeUnit.NANOSECONDS));
    }
  }
}
