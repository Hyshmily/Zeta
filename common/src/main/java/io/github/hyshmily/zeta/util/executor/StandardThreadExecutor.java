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
import java.util.List;
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
 * whose {@code offer()} method cooperates with this executor's thread-count tracking to implement the
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
 * execute. Rejection handlers that return WITHOUT throwing — {@link ThreadPoolExecutor.CallerRunsPolicy}
 * runs the task inline in the submitting thread and discard-style handlers drop it, and neither
 * path reaches {@code afterExecute} — are compensated by an internal accounting wrapper
 * ({@code RejectionAccountingHandler}) installed around the user's handler, so the counter is
 * symmetric for arbitrary handlers. Documented edge cases that are intentionally not detected:
 * {@link ThreadPoolExecutor.DiscardOldestPolicy} purges a QUEUED task that already counted and
 * will never run (a residual +1 leak per purge), and a handler that throws a non-{@link
 * RejectedExecutionException} bypasses the compensation chain entirely.
 */
@Internal
public class StandardThreadExecutor extends ThreadPoolExecutor {

  private final AtomicInteger submittedTasksCount = new AtomicInteger(0);

  /**
   * The user-supplied rejection handler (unwrapped). The pool itself always runs
   * {@code RejectionAccountingHandler}, which delegates here and compensates the
   * {@code submittedTasksCount} increment when the delegate returns without throwing.
   * Volatile: replaceable at runtime via {@link #setRejectedExecutionHandler}.
   */
  @SuppressWarnings("java:S3077")
  private volatile RejectedExecutionHandler userHandler;

  @Getter
  private final int maxSubmittedTaskCount;

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
    this.maxSubmittedTaskCount = queueCapacity + maxThreads;
    // Install the accounting wrapper only after the counter and handler fields are
    // initialized. The executor is not yet published, so no task can be rejected
    // before this line runs.
    super.setRejectedExecutionHandler(new RejectionAccountingHandler());
  }

  @Override
  public void execute(@NonNull Runnable command) {
    int count = submittedTasksCount.incrementAndGet();
    if (count > maxSubmittedTaskCount) {
      submittedTasksCount.decrementAndGet();
      getRejectedExecutionHandler().rejectedExecution(command, this);
      return;
    }
    try {
      super.execute(command);
    } catch (RejectedExecutionException rx) {
      StandardExecutorQueue queue = (StandardExecutorQueue) getQueue();
      try {
        if (!queue.force(command)) {
          submittedTasksCount.decrementAndGet();
          getRejectedExecutionHandler().rejectedExecution(command, this);
        }
      } catch (RejectedExecutionException forceRx) {
        // The task was never queued and never executed, so afterExecute will never fire:
        // compensate the increment here. force() throws exactly when the pool is shutting
        // down (see StandardExecutorQueue#force), the one path where the JDK recheck
        // (offer -> remove -> reject) also lands in this catch.
        submittedTasksCount.decrementAndGet();
        throw forceRx;
      }
    }
  }

  @Override
  protected void afterExecute(@NonNull Runnable r, Throwable t) {
    submittedTasksCount.decrementAndGet();
    if (t != null) {
      // Report task failures through the injectable exception-handler chain (WARN log by
      // default). The throwable is intentionally NOT swallowed: the worker-thread replacement
      // remains the ThreadPoolExecutor's own responsibility.
      ZetaExceptionHandler.handleException("StandardThreadExecutor task failed", t);
    }
  }

  /**
   * Stop the executor and drain the queue of tasks that never started.
   *
   * <p>Tasks returned here never execute, so {@link #afterExecute} never fires for them;
   * their {@code submittedTasksCount} increments are compensated against the drained batch
   * to keep the counter symmetric across the shutdown paths. The drain performed by
   * {@link ThreadPoolExecutor#shutdownNow()} is atomic with the STOP transition, so no new
   * task can slip into the queue after the drained batch is accounted for.
   *
   * @return the list of tasks that never commenced execution
   */
  @Override
  @NonNull
  public List<Runnable> shutdownNow() {
    List<Runnable> drained = super.shutdownNow();
    submittedTasksCount.addAndGet(-drained.size());
    return drained;
  }

  /**
   * Returns the user-supplied rejection handler, not the internal accounting wrapper —
   * the wrapper is a counter-compensation detail of this class, invisible to callers.
   *
   * @return the handler passed to the constructor or {@link #setRejectedExecutionHandler}
   */
  @Override
  public RejectedExecutionHandler getRejectedExecutionHandler() {
    return userHandler;
  }

  /**
   * Replaces the user-supplied rejection handler. The internal accounting wrapper stays
   * installed in the pool (it reads the handler through the volatile field), so the
   * {@code submittedTasksCount} compensation contract holds for any handler.
   *
   * @param handler the new rejection handler
   */
  @Override
  public void setRejectedExecutionHandler(@NonNull RejectedExecutionHandler handler) {
    this.userHandler = handler;
  }

  public int getSubmittedTasksCount() {
    return submittedTasksCount.get();
  }

  /**
   * Wraps the user's rejection handler for the rejections raised inside
   * {@link ThreadPoolExecutor#execute(Runnable)} itself (shutdown, or the
   * offer → addWorker race). When the delegate returns WITHOUT throwing, the task either
   * ran inline in the submitting thread (CallerRunsPolicy) or was silently dropped
   * (discard-style policies) — it will never reach a worker's {@code afterExecute}, so
   * the submit-time increment is compensated here. When the delegate throws, the
   * increment is intentionally left in place: the exception propagates to
   * {@link #execute(Runnable)}, whose force() fallback resolves it (task queued →
   * {@code afterExecute} decrements; rejected → explicit compensation).
   */
  private final class RejectionAccountingHandler implements RejectedExecutionHandler {

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      userHandler.rejectedExecution(r, executor);
      submittedTasksCount.decrementAndGet();
    }
  }
}
