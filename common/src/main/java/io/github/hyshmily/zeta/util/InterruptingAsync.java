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
import java.util.concurrent.*;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;

/**
 * Asynchronous supplier execution whose timeout and cancellation <b>interrupt</b> the executing
 * worker thread, preventing slow or wedged tasks from silently shrinking the pool.
 *
 * <p><b>Interrupt isolation.</b> Each invocation allocates a single {@link InterruptingTask}
 * serving as future, {@link Runnable}, and phase monitor. Timeout and cancellation interrupt the
 * worker thread only while holding that monitor with the task still active, and task exit clears
 * both the thread reference and any residual interrupt flag so reused pool threads never observe a
 * stray interrupt.
 *
 * <p><b>Execution-only timeout.</b> The timeout starts when the executor begins running the task,
 * not at submission time, so queue wait under backlog does not consume execution budget.
 *
 * <p><b>Executor contract.</b> Pass an executor dedicated to tasks whose interrupt flag is owned
 * exclusively by this timeout machinery. Exception propagation matches
 * {@link CompletableFuture#supplyAsync}; executor saturation throws
 * {@link RejectedExecutionException} synchronously.
 *
 * <p>Thread-safe.
 */
@Internal
public final class InterruptingAsync {

  /** Prevents instantiation of this utility class. */
  private InterruptingAsync() {
    throw new AssertionError("No InterruptingAsync instances");
  }

  /**
   * Executes {@code supplier} asynchronously on {@code executor}, completing exceptionally with
   * {@link TimeoutException} and interrupting the executing thread if execution exceeds
   * {@code timeout}. Queue wait prior to task start is excluded from the budget.
   *
   * @param supplier the task to execute asynchronously
   * @param executor the executor running the task (must dedicate interrupt ownership to this helper)
   * @param timeout  maximum execution duration; must be strictly positive
   * @param unit     time unit for {@code timeout}
   * @param <T>      the result type
   * @return a future completing with the supplier's result, {@link TimeoutException},
   *         {@link CancellationException}, or the supplier's failure
   * @throws NullPointerException       if {@code supplier}, {@code executor}, or {@code unit} is {@code null}
   * @throws IllegalArgumentException   if {@code timeout <= 0}
   * @throws RejectedExecutionException if {@code executor} rejects the task
   */
  public static <T> CompletableFuture<T> supplyAsync(
    Supplier<? extends T> supplier,
    Executor executor,
    long timeout,
    TimeUnit unit
  ) {
    // Validate synchronously at submission, matching CompletableFuture.supplyAsync semantics.
    Objects.requireNonNull(supplier, "supplier must not be null");
    Objects.requireNonNull(executor, "executor must not be null");
    Objects.requireNonNull(unit, "unit must not be null");
    if (timeout <= 0L) {
      throw new IllegalArgumentException("timeout must be > 0, got " + timeout);
    }
    InterruptingTask<T> task = new InterruptingTask<>(supplier, timeout, unit);
    executor.execute(task);
    return task;
  }

  /**
   * Single per-invocation allocation combining {@link CompletableFuture}, {@link Runnable}, and
   * interrupt-synchronization monitor.
   *
   * <p>Mutually exclusive {@link #state} phases under {@code this}:
   * {@code Supplier} (queued) -&gt; {@code Thread} (running) -&gt; {@code ABORTED} (timed out or
   * cancelled) -&gt; {@code null} (exited).
   */
  private static final class InterruptingTask<T> extends CompletableFuture<T> implements Runnable {

    /**
     * Sentinel written before timeout/cancellation completes the future so a finishing worker
     * recognizes it lost ownership and does not race to publish a loaded result.
     */
    private static final Object ABORTED = new Object();

    private final long timeoutNanos;

    @GuardedBy("this")
    private @Nullable Object state;

    /** Creates a queued task capturing {@code supplier} and its execution budget in nanoseconds. */
    private InterruptingTask(@Nullable Supplier<? extends T> supplier, long timeout, TimeUnit unit) {
      this.state = supplier;
      this.timeoutNanos = unit.toNanos(timeout);
    }

    /**
     * Transitions from queued to running, arms the execution timeout, runs the supplier, leaves
     * the interruptible phase cleanly, and publishes the outcome unless already aborted.
     */
    @Override
    @SuppressWarnings({ "unchecked", "java:S1181" })
    public void run() {
      Supplier<? extends T> supplier;
      synchronized (this) {
        // Guard against cancellation that landed while queued so the supplier never runs.
        if (!(state instanceof Supplier<?> queued) || isDone()) {
          state = null;
          return;
        }
        supplier = (Supplier<? extends T>) queued;
        state = Thread.currentThread();
      }

      T result = null;
      Throwable failure = null;
      boolean aborted;
      try {
        // Arm timeout on execution start so queue wait does not eat into the execution budget.
        orTimeout(timeoutNanos, TimeUnit.NANOSECONDS);
        result = supplier.get();
      } catch (Throwable t) {
        failure = t;
      } finally {
        synchronized (this) {
          // Leave the interruptible phase and clear any stale interrupt before publishing:
          // complete*() runs dependent callbacks inline on this thread.
          aborted = state == ABORTED;
          state = null;
          Thread.interrupted();
        }
      }

      if (aborted) {
        // Timeout or cancellation already interrupted this thread and owns the terminal outcome.
        return;
      }
      if (failure == null) {
        complete(result);
      } else {
        // Mirror CompletableFuture.supplyAsync: keep CompletionException as-is, wrap others once.
        completeExceptionally(failure instanceof CompletionException ? failure : new CompletionException(failure));
      }
    }

    /**
     * Interrupts the running worker thread before completing on {@link TimeoutException} so slow
     * synchronous dependents cannot delay the interrupt or deadlock the JDK delay scheduler.
     */
    @Override
    public boolean completeExceptionally(Throwable cause) {
      if (cause instanceof TimeoutException && !isDone()) {
        interruptRunning();
      }
      return super.completeExceptionally(cause);
    }

    /**
     * Cancels the future and interrupts the running worker thread (even when
     * {@code mayInterruptIfRunning} is {@code false}) so external cancellation releases the pool
     * thread promptly.
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      if (!isDone()) {
        interruptRunning();
      }
      return super.cancel(mayInterruptIfRunning);
    }

    /** Marks the task aborted and interrupts the executing thread once if still inside {@link #run()}. */
    private void interruptRunning() {
      synchronized (this) {
        Object phase = state;
        state = ABORTED;
        if (phase instanceof Thread running) {
          running.interrupt();
        }
      }
    }
  }
}
