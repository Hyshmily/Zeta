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
 * <p>The executor uses a {@link ElasticTaskQueue} (backed by {@link java.util.concurrent.LinkedTransferQueue})
 * whose {@code offer()} method cooperates with this executor's thread-count tracking to implement the
 * Tomcat-style policy. A {@code submittedTasksCount} counter limits the total number of in-flight
 * tasks (queued + active) to {@code queueCapacity + maxThreads}.
 *
 * <p><b>Execution order comparison:</b>
 * <pre>
 *   ThreadPoolExecutor:       coreThread → queue → maxThread → reject  (CPU-bound)
 *   ElasticThreadPool:   coreThread → maxThread → queue → reject  (I/O-bound)
 * </pre>
 *
 * <p><b>Rejection accounting.</b> The counter is incremented per submission and decremented in
 * {@link #afterExecute} (worker execution) plus on every path where a task is known never to
 * execute. Rejections raised inside {@link ThreadPoolExecutor#execute} itself (shutdown, or the
 * offer → addWorker race) land in an internal force-first handler that owns the whole
 * resolution: when the pool is still running the task is forced into the queue and the add is
 * kept (the later {@code afterExecute} balances it); once the force fails (shutdown) the user's
 * handler runs and the internal handler rolls the increment back — a returning handler
 * ({@link ThreadPoolExecutor.CallerRunsPolicy}, discard-style) decrements on the following line,
 * while a throwing one (Abort) skips that line and lets {@link #execute(Runnable)}'s finally
 * roll the same submission back exactly once. The over-cap pre-check in
 * {@link #execute(Runnable)} (past {@code maxSubmittedTaskCount}) calls the user's handler
 * directly after rolling the increment back. A failure that never reaches {@code reject} —
 * a throwing {@link ThreadFactory} or {@code Thread.start} failing — is rolled back by
 * {@link #execute(Runnable)}'s finally. The counter is therefore symmetric for arbitrary
 * handlers, with the force attempted exactly once per submission.
 * Documented edge case that is intentionally not detected:
 * {@link ThreadPoolExecutor.DiscardOldestPolicy} drops a QUEUED task that already counted and
 * will never run (a residual +1 leak per drop). It bypasses the queue-removal accounting by
 * calling {@code getQueue().poll()} directly.
 */
@Internal
public class ElasticThreadPool extends ThreadPoolExecutor {

  /** Drain budget of {@link #shutdownGracefully()}: a bounded window for queued I/O work. */
  private static final long SHUTDOWN_DRAIN_MILLIS = 3_000L;

  /**
   * Stack-walk handle used on the shutdown path only, to tell the JDK's internal
   * {@code remove} apart from an external one.
   */
  private static final StackWalker STACK_WALKER = StackWalker.getInstance();

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
   * Creates a new {@code ElasticThreadPool}.
   *
   * @param coreThreads    the number of core threads
   * @param maxThreads     the maximum number of threads
   * @param keepAliveTime  the time to keep idle threads alive
   * @param unit           the time unit for {@code keepAliveTime}
   * @param queueCapacity  the capacity of the task queue
   * @param threadFactory  the factory to create new threads
   * @param handler        the handler to use when execution is rejected
   */
  public ElasticThreadPool(
    int coreThreads,
    int maxThreads,
    long keepAliveTime,
    TimeUnit unit,
    int queueCapacity,
    ThreadFactory threadFactory,
    RejectedExecutionHandler handler
  ) {
    super(coreThreads, maxThreads, keepAliveTime, unit, new ElasticTaskQueue(), threadFactory, handler);
    ((ElasticTaskQueue) getQueue()).attach(this);
    this.userHandler = handler;
    this.queueCapacity = queueCapacity;
    this.maxSubmittedTaskCount = computeMaxSubmitted(queueCapacity, maxThreads);
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
    // A rejection the internal force-first handler resolved has already compensated or queued the
    // task; what can still leak the increment is a failure that never reaches reject() or
    // afterExecute — a throwing ThreadFactory, Thread.start failing, or a rejected task whose
    // handler threw. The handler decrements only when it returns, so this rollback covers exactly
    // the throwing case and never double-counts.
    boolean completed = false;
    try {
      super.execute(command);
      completed = true;
    } finally {
      if (!completed) {
        submittedTasksCount.decrementAndGet();
      }
    }
  }

  /**
   * The admission ceiling, saturating instead of wrapping: a large
   * {@code queueCapacity} (up to {@link Integer#MAX_VALUE}) overflowing into a
   * negative ceiling would reject every submission.
   *
   * @param queueCapacity the configured queue capacity
   * @param maxThreads    the maximum pool size
   * @return {@code queueCapacity + maxThreads}, capped at {@link Integer#MAX_VALUE}
   */
  private static int computeMaxSubmitted(int queueCapacity, int maxThreads) {
    if (queueCapacity < 0) {
      throw new IllegalArgumentException("queueCapacity must be >= 0, but got: " + queueCapacity);
    }
    return (int) Math.min((long) queueCapacity + maxThreads, Integer.MAX_VALUE);
  }

  /**
   * Removes a queued task and compensates its submission increment, except when the removal comes
   * from {@link ThreadPoolExecutor#execute}'s shutdown recheck: that call removes the task only to
   * reject it on the same thread, and the internal rejection handler owns the single decrement.
   * Every other caller — including an explicit {@code remove} while the pool is shutting down —
   * balances the submission here, because a removed task never reaches {@code afterExecute}.
   *
   * @param task the task to remove
   * @return whether the task was still queued and is now removed
   */
  @Override
  public boolean remove(Runnable task) {
    if (!super.remove(task)) {
      return false;
    }
    // Outside the recheck the task can never reach afterExecute, so the increment is balanced here
    // on every path, including an explicit remove() while the pool is shutting down.
    if (!isShutdown() || !isShutdownRecheckCaller()) {
      submittedTasksCount.decrementAndGet();
    }
    return true;
  }

  /**
   * Whether {@link #remove(Runnable)} was entered from {@link ThreadPoolExecutor#execute}'s
   * shutdown recheck — the only {@code remove(command)} the JDK performs itself, and only after
   * {@code isRunning} turned false, immediately before {@code reject(command)}.
   */
  private static boolean isShutdownRecheckCaller() {
    return STACK_WALKER.walk(frames ->
      frames
        // 0: this method, 1: remove, 2: the frame that invoked remove
        .skip(2)
        .findFirst()
        .map(
          frame ->
            ThreadPoolExecutor.class.getName().equals(frame.getClassName()) && "execute".equals(frame.getMethodName())
        )
        .orElse(false)
    );
  }

  /**
   * Removes cancelled queued futures and compensates their submission increments.
   *
   * <p>The JDK's implementation removes through the queue iterator and never calls
   * {@link #remove(Runnable)}, so without this override every purged cancelled task leaves one
   * admission slot held forever. The iteration is the queue's own weakly consistent iterator —
   * no array snapshot — and {@code queue.remove} reports only a removal this call actually won; a
   * worker or a concurrent purge that won the race owns the matching compensation (the worker
   * through {@code afterExecute}).
   */
  @Override
  public void purge() {
    int removed = 0;
    BlockingQueue<Runnable> queue = getQueue();
    for (Runnable task : queue) {
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
    this.maxSubmittedTaskCount = computeMaxSubmitted(queueCapacity, maximumPoolSize);
  }

  @Override
  protected void afterExecute(@NonNull Runnable task, Throwable failure) {
    submittedTasksCount.decrementAndGet();
    if (failure != null) {
      // Report task failures through the injectable exception-handler chain (WARN log by
      // default). The throwable is reported, then dropped; retiring a worker thread that
      // escaped a task remains the ThreadPoolExecutor's own responsibility.
      ZetaExceptionHandler.handleException("ElasticThreadPool task failed", failure);
    }
  }

  /**
   * Initiates an orderly shutdown under the queue's lifecycle lock, so {@link ElasticTaskQueue#force}
   * cannot pass its running check and then insert a task after the last worker has exited. Without
   * the shared lock, a forced task could be queued into a pool that is already terminating and
   * never execute.
   */
  @Override
  public void shutdown() {
    synchronized (((ElasticTaskQueue) getQueue()).lifecycleLock()) {
      super.shutdown();
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
    synchronized (((ElasticTaskQueue) getQueue()).lifecycleLock()) {
      drained = super.shutdownNow();
    }
    submittedTasksCount.addAndGet(-drained.size());
    return drained;
  }

  /**
   * Stop accepting tasks, drain the queue for a bounded window, then force-stop what is left.
   * The send pool uses this on context shutdown: its queued tasks are already-committed peer
   * broadcasts, and an immediate {@link #shutdownNow()} would discard them after the business
   * transaction committed — widening the other nodes' stale window for no reason. The budget is
   * short enough not to delay shutdown on a wedged broker.
   *
   * <p>An interrupt during the await forces the stop and restores the caller's interrupt flag.
   */
  public void shutdownGracefully() {
    shutdown();
    try {
      if (!awaitTermination(SHUTDOWN_DRAIN_MILLIS, TimeUnit.MILLISECONDS)) {
        shutdownNow();
      }
    } catch (InterruptedException e) {
      shutdownNow();
      Thread.currentThread().interrupt();
    }
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
   * {@link #afterExecute} running, so a transient over-count is normal; it
   * drains back to zero.
   *
   * @return the current number of in-flight submissions
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
   * reaches {@code afterExecute}) decrements on the following line, and a throwing one
   * (Abort, or an {@code Error} from an inline run) skips that line so
   * {@link #execute(Runnable)}'s finally performs the single rollback. This handler never
   * sees the over-cap pre-check rejection in {@link #execute(Runnable)}, which calls the
   * user handler directly after rolling the increment back.
   */
  private final class InternalForceQueueHandler implements RejectedExecutionHandler {

    @Override
    public void rejectedExecution(Runnable task, ThreadPoolExecutor pool) {
      var queue = (ElasticTaskQueue) pool.getQueue();
      if (queue.force(task)) {
        return;
      }

      // The task was never queued and never executed, so afterExecute will never fire: compensate
      // the increment on every outcome. A handler that throws — Abort, or an Error escaping
      // CallerRuns — never reaches the decrement, and execute()'s finally rolls that submission
      // back instead, so each submission is decremented exactly once.
      userHandler.rejectedExecution(task, pool);
      submittedTasksCount.decrementAndGet();
    }
  }
}
