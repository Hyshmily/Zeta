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

import io.github.hyshmily.zeta.Internal;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Async supplier execution whose timeout <b>interrupts</b> the thread still
 * running the task — unlike a bare {@code CompletableFuture.supplyAsync(...).
 * orTimeout(...)}, which only abandons the future and leaves the pool thread
 * blocked until the task finishes on its own, silently shrinking the executor.
 *
 * <p><b>Mechanism.</b> The task runs inside a wrapper that records the executing
 * thread and lowers a {@code stillRunning} latch on exit; the timeout callback
 * interrupts the recorded thread only while the latch is still raised. The
 * latch makes "interrupt" and "task finished" race exactly once: finished → no
 * interrupt; timeout first → interrupt the thread while it is still executing
 * this task (the thread reference was cleared on exit). A timeout that arrives
 * after completion never interrupts a reused pool thread running an unrelated
 * task. The interrupt flag left behind by a timed-out task (which may ignore
 * the interrupt, or re-interrupt itself from a {@code catch
 * (InterruptedException)}) is also cleared on task exit, so the next task on
 * the same pool thread starts clean.
 *
 * <p><b>Executor contract.</b> The interrupt flag on the wrapped task's thread
 * is owned exclusively by this class's timeout machinery — pass an executor
 * dedicated to these tasks (or at least one whose other tasks never rely on
 * foreign interrupts), and never expose its threads elsewhere.
 *
 * <p>Exception propagation (including {@link Error}) matches
 * {@link CompletableFuture#supplyAsync}'s standard {@code encodeThrowable}
 * semantics; a timeout surfaces as a {@link TimeoutException} via
 * {@link ExecutionException} on join. A saturated executor throws
 * {@link java.util.concurrent.RejectedExecutionException} synchronously from
 * this method, exactly like {@code supplyAsync}.
 *
 * <p>Thread-safe.
 */
@Internal
public final class InterruptingAsync {

  private InterruptingAsync() {}

  /**
   * Executes {@code supplier} asynchronously on {@code executor}, completing
   * exceptionally with a {@link TimeoutException} — and interrupting the
   * executing thread — when it outlives {@code timeout}.
   *
   * @param supplier the task to execute asynchronously
   * @param executor the executor running the task; see the class doc for the
   *                 interrupt-ownership contract
   * @param timeout  the maximum time to wait
   * @param unit     the time unit of {@code timeout}
   * @param <T>      the result type
   * @return a future completing with the supplier's result, a
   *         {@link TimeoutException}, or the task's failure
   */
  @SuppressWarnings("all")
  public static <T> CompletableFuture<T> supplyAsync(
    Supplier<? extends T> supplier,
    Executor executor,
    long timeout,
    TimeUnit unit
  ) {
    AtomicReference<Thread> runningThread = new AtomicReference<>();
    AtomicBoolean stillRunning = new AtomicBoolean(true);
    Executor wrapped = task ->
      executor.execute(() -> {
        runningThread.set(Thread.currentThread());
        try {
          task.run();
        } finally {
          // Task finished: forbid any late timeout interrupt (the captured
          // thread reference may be reused for an unrelated task) and clear
          // the interrupt flag so the next task on this pool thread starts
          // clean. Per the class contract, the flag can only have come from
          // this class's timeout machinery.
          stillRunning.set(false);
          runningThread.set(null);
          Thread.interrupted();
        }
      });

    // Lambda adapter: CompletableFuture.supplyAsync takes Supplier<T> exactly,
    // so the wildcard supplier is bridged through a capturing lambda.
    CompletableFuture<T> future = CompletableFuture.supplyAsync(() -> supplier.get(), wrapped);
    future.orTimeout(timeout, unit);
    future.whenComplete((r, ex) -> {
      // The latch makes "interrupt" and "task finished" race exactly once:
      // finished → no interrupt; timeout first → interrupt the thread while
      // it is still executing this task (runningThread was cleared on exit).
      if (ex instanceof TimeoutException && stillRunning.getAndSet(false)) {
        Thread t = runningThread.get();
        if (t != null) {
          t.interrupt();
        }
      }
    });
    return future;
  }
}
