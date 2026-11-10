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
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Async supplier execution whose timeout <b>interrupts</b> the thread still
 * running the task — unlike a bare {@code CompletableFuture.supplyAsync(...).
 * orTimeout(...)}, which only abandons the future and leaves the pool thread
 * blocked until the task finishes on its own, silently shrinking the executor.
 *
 * <p><b>Mechanism.</b> The task runs inside an {@link InterruptingTask} that records
 * the executing thread under its own monitor and clears its phase reference on
 * exit; {@link InterruptingTask#completeExceptionally} (a timeout completes that way)
 * and {@link InterruptingTask#cancel(boolean)} interrupt the recorded thread only while
 * holding the same monitor with the reference still pointing at it. The monitor makes
 * "interrupt" and "task finished" mutually exclusive: finished → no interrupt; timeout
 * first → interrupt the thread while it is still executing this task. A timeout that
 * arrives after completion never interrupts a reused pool thread running an unrelated
 * task.
 * The interrupt flag left behind by a timed-out task (which may ignore the interrupt,
 * or re-interrupt itself from a {@code catch (InterruptedException)}) is also cleared
 * on task exit, so the next task on the same pool thread starts clean.
 *
 * <p><b>Timeout scope.</b> The timeout is an <em>execution</em> budget: it starts when
 * the executor actually begins the task, not when this method is called. Time spent
 * queued behind a saturated executor is not charged to the task, so a deep backlog
 * cannot make a fast reader time out before it ever runs. Queue saturation is an
 * admission concern for the caller; a task that never starts never times out.
 *
 * <p><b>Executor contract.</b> The interrupt flag on the wrapped task's thread
 * is owned exclusively by this class's timeout machinery — pass an executor
 * dedicated to these tasks (or at least one whose other tasks never rely on
 * foreign interrupts), and never expose its threads elsewhere.
 *
 * <p><b>Cancellation.</b> {@code future.cancel(true)} completes the future with a
 * {@link CancellationException}; the same callback that handles a timeout then
 * interrupts the executing thread, so an external cancellation actually releases
 * the pool thread. A cancelled task that has not started never runs its supplier.
 *
 * <p>Exception propagation (including {@link Error}) matches
 * {@link CompletableFuture#supplyAsync}'s standard {@code encodeThrowable}
 * semantics; a timeout surfaces as a {@link TimeoutException} — the
 * {@link CompletionException} cause on {@code join()}, the
 * {@link java.util.concurrent.ExecutionException} cause on {@code get()}. A
 * saturated executor throws
 * {@link java.util.concurrent.RejectedExecutionException} synchronously from
 * this method, exactly like {@code supplyAsync}.
 *
 * <p><b>Allocation.</b> The state lock, the single phase reference and the
 * Runnable are all the one {@link InterruptingTask} instance — one
 * short-lived object per call. The completion hooks are overridden on that
 * future, so no dependent callback node is registered.
 *
 * <p>Thread-safe.
 */
@Internal
public final class InterruptingAsync {

  private InterruptingAsync() {}

  /**
   * Executes {@code supplier} asynchronously on {@code executor}, completing
   * exceptionally with a {@link TimeoutException} — and interrupting the
   * executing thread — when its execution outlives {@code timeout}. Queue wait
   * before the task starts is not counted; see the class doc.
   *
   * @param supplier the task to execute asynchronously
   * @param executor the executor running the task; see the class doc for the
   *                 interrupt-ownership contract
   * @param timeout  the maximum time the task may run; must be positive
   * @param unit     the time unit of {@code timeout}
   * @param <T>      the result type
   * @return a future completing with the supplier's result, a
   *         {@link TimeoutException}, or the task's failure
   */
  public static <T> CompletableFuture<T> supplyAsync(
    Supplier<? extends T> supplier,
    Executor executor,
    long timeout,
    TimeUnit unit
  ) {
    // Fail at submission, not on the pool thread: a null unit must surface here
    // as a synchronous validation failure, like supplyAsync's own argument checks.
    Objects.requireNonNull(supplier, "supplier must not be null");
    Objects.requireNonNull(executor, "executor must not be null");
    Objects.requireNonNull(unit, "unit must not be null");
    if (timeout <= 0) {
      throw new IllegalArgumentException("timeout must be > 0, got " + timeout);
    }
    InterruptingTask<T> task = new InterruptingTask<>(supplier, unit, timeout);
    // Submit directly so a rejected task still throws synchronously, exactly like
    // CompletableFuture.supplyAsync(..., executor).
    executor.execute(task);
    return task;
  }

  /**
   * The one object per call: the future, the Runnable the executor runs, and
   * the completion callback that interrupts on timeout/cancellation. The task's
   * own monitor guards {@code state}, so an interrupt can never land after the
   * task exited and its pool thread was reused for an unrelated task.
   *
   * <p>{@code state} carries the mutually exclusive phases: a
   * {@link Supplier} while queued, the running {@link Thread}, and {@code null}
   * once done or interrupted.
   */
  private static final class InterruptingTask<T> extends CompletableFuture<T> implements Runnable {

    /** The execution budget in nanoseconds — the unit is folded in at construction. */
    private final long timeoutNanos;

    @GuardedBy("this")
    private Object state;

    InterruptingTask(Supplier<? extends T> supplier, TimeUnit unit, long timeout) {
      this.state = supplier;
      this.timeoutNanos = unit.toNanos(timeout);
    }

    @Override
    @SuppressWarnings("all")
    public void run() {
      Supplier<? extends T> supplier;
      synchronized (this) {
        // isDone covers a cancellation that arrived while the task was queued:
        // a cancelled future must not run its supplier.
        if (!(state instanceof Supplier<?> queued) || isDone()) {
          state = null;
          return;
        }

        supplier = (Supplier<? extends T>) queued;
        state = Thread.currentThread();
      }
      T result = null;
      Throwable failure = null;
      try {
        // Armed here, not at submission: queue wait is not execution time, so a
        // batch's tail keys get the same budget as its head.
        orTimeout(timeoutNanos, TimeUnit.NANOSECONDS);
        result = supplier.get();
      } catch (Throwable t) {
        failure = t;
      } finally {
        synchronized (this) {
          // Leave the interruptible phase before publishing the outcome: complete*() runs the
          // dependent actions synchronously on this thread, and a stale interrupt flag (or a
          // still-armed timeout/cancel that could target this thread) would hit those callbacks
          // instead of the reader.
          state = null;
          Thread.interrupted();
        }
      }
      if (failure == null) {
        complete(result);
      } else {
        // Same encoding as CompletableFuture.supplyAsync: a CompletionException
        // travels as-is, everything else is wrapped once.
        completeExceptionally(failure instanceof CompletionException ? failure : new CompletionException(failure));
      }
    }

    @Override
    public boolean completeExceptionally(Throwable cause) {
      // Complete first: a reader that catches the interrupt can otherwise return and win
      // the race with complete(value), turning a budget timeout into a loaded value. Once
      // the future is settled the reader's completion is a no-op, so the interrupt only
      // has to stop the still-running call.
      boolean completed = super.completeExceptionally(cause);
      if (completed && cause instanceof TimeoutException) {
        interruptRunning();
      }
      return completed;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      // CompletableFuture.cancel() does not know the thread running run(); the
      // interrupt has to come from the task's own state. Cancellation releases
      // the pool thread even when the caller passes false.
      boolean cancelled = super.cancel(mayInterruptIfRunning);
      if (cancelled) {
        interruptRunning();
      }
      return cancelled;
    }

    /**
     * Interrupts the task thread once, and only while it is still inside
     * {@link #run()}. The monitor is what makes the interrupt and the exit path
     * mutually exclusive, so a later task on the same pooled thread is never hit.
     */
    private void interruptRunning() {
      synchronized (this) {
        Object phase = state;
        state = null;
        if (phase instanceof Thread running) {
          running.interrupt();
        }
      }
    }
  }
}
