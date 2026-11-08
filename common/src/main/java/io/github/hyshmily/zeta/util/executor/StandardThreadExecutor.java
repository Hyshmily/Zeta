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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.jspecify.annotations.NonNull;

/**
 * A {@link ThreadPoolExecutor} with Tomcat-style execution ordering: core → max → queue → reject,
 * as opposed to the JDK default of core → queue → max → reject.
 *
 * <p>This ordering is better suited for I/O-bound operations where threads waiting on remote
 * resources (network, disk) should not prevent new threads from being created. The JDK default
 * favours CPU-bound workloads where queuing is preferred over creating more threads.
 *
 * <p>The executor uses a {@link StandardExecutorQueue} (backed by {@link java.util.concurrent.LinkedTransferQueue})
 * whose {@code offer()} method cooperates with this executor's thread-add tracking to implement the
 * Tomcat-style policy. A {@code submittedTasksCount} counter limits the total number of in-flight
 * tasks (queued + active) to {@code queueCapacity + maxThreads}.
 *
 * <p><b>Execution order comparison:</b>
 * <pre>
 *   ThreadPoolExecutor:       coreThread → queue → maxThread → reject  (CPU-bound)
 *   StandardThreadExecutor:   coreThread → maxThread → queue → reject  (I/O-bound)
 * </pre>
 *
 * <p><b>Rejection accounting.</b> The counter is incremented per submission and decremented in
 * {@link #afterExecute} (worker execution) plus on every path where a task is known never to
 * execute. Rejections raised inside {@link ThreadPoolExecutor#execute} itself (shutdown, or the
 * offer → addWorker race) land in an internal force-first handler that owns the whole
 * resolution: when the pool is still running the task is forced into the queue and the add is
 * kept (the later {@code afterExecute} balances it); once the force fails (shutdown) the user's
 * handler runs and the internal handler rolls the increment back — a returning handler
 * ({@link ThreadPoolExecutor.CallerRunsPolicy}, discard-style) decrements on return, a throwing
 * one (Abort) decrements before propagating its own exception. The over-cap pre-check in
 * {@link #execute(Runnable)} (past {@code maxSubmittedTaskCount}) calls the user's handler
 * directly after rolling the increment back. The counter is therefore symmetric for arbitrary
 * handlers, with the force attempted exactly once per submission.
 * Documented edge case that is intentionally not detected:
 * {@link ThreadPoolExecutor.DiscardOldestPolicy} drops a QUEUED task that already counted and
 * will never run (a residual +1 leak per drop). It bypasses the queue-removal accounting by
 * calling {@code getQueue().poll()} directly.
 */
@Internal
public class StandardThreadExecutor extends ThreadPoolExecutor {

  /**
   * Records a task removed by {@link #remove(Runnable)} while the pool is shutting down.
   * {@link ThreadPoolExecutor#execute(Runnable)} only ever calls {@code remove(command)} inside its
   * {@code !isRunning(recheck) && remove(command)} shutdown recheck, immediately before calling
   * {@code reject(command)} on the same thread. Keeping the marker off the running path means
   * normal {@link #execute(Runnable)} and {@link #remove(Runnable)} calls never touch a
   * {@link ThreadLocal}.
   */
  private static final ThreadLocal<Runnable> SHUTDOWN_REMOVED = new ThreadLocal<>();

  private final AtomicInteger submittedTasksCount = new AtomicInteger(0);

  /** Configured queue capacity, the fixed half of {@link #maxSubmittedTaskCount}. */
  private final int queueCapacity;

  /**
   * The user-supplied rejection handler (unwrapped). The pool itself always runs
   * {@code InternalForceQueueHandler}, which resolves the offer → addWorker race via
   * {@code queue.force} and delegates here only on genuine overload or shutdown.
   * Volatile: replaceable at runtime via {@link #setRejectedExecutionHandler}.
   */
  @SuppressWarnings("java:S3077")
  private volatile RejectedExecutionHandler userHandler;

  /**
   * Admission ceiling — {@code queueCapacity + maximumPoolSize}. Volatile and
   * recomputed by {@link #setMaximumPoolSize(int)}: a runtime pool resize must
   * move the ceiling with the pool, or the executor either over-admits past the
   * new bound or keeps rejecting under the old one forever.
   */
  @Getter
  private volatile int maxSubmittedTaskCount;

  /**
   * Creates a new {@code StandardThreadExecutor}.
   *
   * @param coreThreads    the number of core threads
   * @param maxThreads     the maximum number of threads
   * @param keepAliveTime  the time to keep idle threads alive
   * @param unit           the time unit for {@code keepAliveTime}
   * @param queueCapacity  the capacity of the task queue
   * @param threadFactory  the factory to create new threads
   * @param handler        the handler to use when execution is rejected
   */
  public StandardThreadExecutor(
    int coreThreads,
    int maxThreads,
    long keepAliveTime,
    TimeUnit unit,
    int queueCapacity,
    ThreadFactory threadFactory,
    RejectedExecutionHandler handler
  ) {
    super(coreThreads, maxThreads, keepAliveTime, unit, new StandardExecutorQueue(), threadFactory, handler);
    ((StandardExecutorQueue) getQueue()).setStandardThreadExecutor(this);
    this.userHandler = handler;
    this.queueCapacity = queueCapacity;
    this.maxSubmittedTaskCount = queueCapacity + maxThreads;
    // Install the force-first handler only after the counter and handler fields are
    // initialized. The executor is not yet published, so no task can be rejected
    // before this line runs.
    super.setRejectedExecutionHandler(new InternalForceQueueHandler());
  }

  @Override
  public void execute(@NonNull Runnable command) {
    Objects.requireNonNull(command, "command must not be null");
    if (submittedTasksCount.incrementAndGet() > maxSubmittedTaskCount) {
      submittedTasksCount.decrementAndGet();
      userHandler.rejectedExecution(command, this);
      return;
    }
    // No catch around super.execute: every rejection it can raise is already
    // fully resolved by the internal force-first handler — the force, the
    // user-handler delegation, and the counter compensation all happen there,
    // with the user handler's own exception propagating unchanged.
    try {
      super.execute(command);
    } finally {
      // Only the shutdown recheck on this thread can leave a marker: a running
      // pool never touches the ThreadLocal at all.
      if (isShutdown()) {
        SHUTDOWN_REMOVED.remove();
      }
    }
  }

  @Override
  public boolean remove(Runnable task) {
    // A removed queued task never reaches afterExecute's decrement — balance
    // the submission increment here, or each external removal permanently
    // leaks one slot of the admission ceiling (maxSubmittedTaskCount). When the
    // JDK's shutdown recheck removes the command it is about to reject, the
    // marker lets the rejection handler skip the second decrement.
    if (super.remove(task)) {
      submittedTasksCount.decrementAndGet();
      if (isShutdown()) {
        SHUTDOWN_REMOVED.set(task);
      }
      return true;
    }
    return false;
  }

  /**
   * Removes cancelled queued futures and compensates their submission increments.
   *
   * <p>The JDK's implementation removes through the queue iterator and never calls
   * {@link #remove(Runnable)}, so without this override every purged cancelled task leaves one
   * admission slot held forever. The snapshot-then-{@code queue.remove} form counts only a removal
   * this call actually won; a worker or a concurrent purge that won the race owns the matching
   * compensation (the worker through {@code afterExecute}).
   */
  @Override
  public void purge() {
    int removed = 0;
    BlockingQueue<Runnable> queue = getQueue();
    for (Runnable task : queue.toArray(new Runnable[0])) {
      if (task instanceof Future<?> future && future.isCancelled() && queue.remove(task)) {
        removed++;
      }
    }
    if (removed != 0) {
      submittedTasksCount.addAndGet(-removed);
    }
  }

  /**
   * Keeps the admission ceiling in sync with a runtime pool resize. The queue
   * reads {@code getMaximumPoolSize()} from this executor on every
   * thread-creation decision, so it has no snapshot to refresh.
   *
   * @param maximumPoolSize the new maximum pool size
   * @throws IllegalArgumentException if the size is not positive or below the core size
   */
  @Override
  public void setMaximumPoolSize(int maximumPoolSize) {
    super.setMaximumPoolSize(maximumPoolSize);
    this.maxSubmittedTaskCount = queueCapacity + maximumPoolSize;
  }

  @Override
  protected void afterExecute(@NonNull Runnable task, Throwable failure) {
    submittedTasksCount.decrementAndGet();
    if (failure != null) {
      // Report task failures through the injectable exception-handler chain (WARN log by
      // default). The throwable is reported, then dropped; retiring a worker thread that
      // escaped a task remains the ThreadPoolExecutor's own responsibility.
      ZetaExceptionHandler.handleException("StandardThreadExecutor task failed", failure);
    }
  }

  /**
   * Stop the executor and drain the queue of tasks that never started.
   *
   * <p>Tasks returned here never execute, so {@link #afterExecute} never fires for them;
   * their {@code submittedTasksCount} increments are compensated against the drained batch
   * to keep the counter symmetric across the shutdown paths. The drain runs under the
   * queue lifecycle lock shared with the force path, so a forced task cannot be inserted
   * between the STOP transition and the drain.
   *
   * @return the list of tasks that never commenced execution
   */
  @Override
  @NonNull
  public List<Runnable> shutdownNow() {
    List<Runnable> drained;
    synchronized (((StandardExecutorQueue) getQueue()).lifecycleLock()) {
      drained = super.shutdownNow();
    }
    submittedTasksCount.addAndGet(-drained.size());
    return drained;
  }

  /**
   * Returns the user-supplied rejection handler, not the internal force-first handler —
   * the handler is a queue-resolution detail of this class, invisible to callers.
   *
   * @return the handler passed to the constructor
   */
  @Override
  public RejectedExecutionHandler getRejectedExecutionHandler() {
    return userHandler;
  }

  /**
   * Replaces the user-supplied rejection handler while keeping the internal force-first
   * handler installed on the pool. The pool's own handler owns the force and the
   * submission-counter compensation; overwriting it would break both.
   *
   * @param handler the new user handler; must not be {@code null}
   */
  @Override
  public void setRejectedExecutionHandler(@NonNull RejectedExecutionHandler handler) {
    this.userHandler = Objects.requireNonNull(handler, "handler must not be null");
  }

  /**
   * Return the current number of in-flight submissions (queued + executing tasks).
   *
   * <p>The counter is conservative between a task body finishing and
   * {@link #afterExecute} running, so a transient over-add is normal; it
   * drains back to zero.
   *
   * @return the current submitted-task add
   */
  public int getSubmittedTasksCount() {
    return submittedTasksCount.get();
  }

  /**
   * Internal rejection handler for rejections raised inside
   * {@link ThreadPoolExecutor#execute(Runnable)} itself (shutdown, or the
   * offer → addWorker race when {@code poolSize < maxPoolSize}). It owns the
   * whole rejection resolution — {@link #execute(Runnable)} wraps
   * {@code super.execute} without a catch — so the force is attempted exactly
   * once and a throwing user handler propagates its own exception, not a
   * queue-force substitute.
   *
   * <p>When the pool is still running the race is resolved by forcing the task into the
   * queue: no warning, no exception, no {@code CallerRuns} inline run — the later worker
   * {@code afterExecute} balances the submit-time increment. The user's handler runs only
   * once the force has failed (shutdown raced in). The counter is compensated here for
   * every not-queued outcome: a returning handler (CallerRuns/discard — neither path
   * reaches {@code afterExecute}) decrements on return, and a throwing one (Abort)
   * decrements before its original exception propagates. This handler never sees the
   * over-cap pre-check rejection in {@link #execute(Runnable)}, which calls the user
   * handler directly after rolling the increment back.
   */
  private final class InternalForceQueueHandler implements RejectedExecutionHandler {

    @Override
    public void rejectedExecution(Runnable task, ThreadPoolExecutor pool) {
      var queue = (StandardExecutorQueue) pool.getQueue();
      if (queue.force(task)) {
        return;
      }

      // The task was never queued and never executed, so afterExecute will never
      // fire: compensate the increment on EVERY outcome. finally instead of a
      // catch: a CallerRunsPolicy that runs the task inline can throw an Error
      // (OOM, StackOverflow, AssertionError), which a catch(RuntimeException) misses
      // and leaks a slot permanently.
      //
      // remove() already decremented the exact task the JDK is now rejecting, so
      // consume that marker instead of decrementing twice. Clearing it before
      // user code keeps a CallerRunsPolicy that removes another queued task on
      // the normal decrement path.
      boolean alreadyCompensated = SHUTDOWN_REMOVED.get() == task;
      if (alreadyCompensated) {
        SHUTDOWN_REMOVED.remove();
      }
      try {
        userHandler.rejectedExecution(task, pool);
      } finally {
        if (!alreadyCompensated) {
          submittedTasksCount.decrementAndGet();
        }
      }
    }
  }
}
