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
import org.jspecify.annotations.NonNull;
import org.springframework.util.Assert;

import java.util.Objects;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link LinkedTransferQueue} whose {@link #offer(Runnable)} method cooperates with
 * {@link StandardThreadExecutor} to implement Tomcat-style core → max → queue → reject ordering.
 *
 * <p>The key insight is that returning {@code false} from {@code offer()} causes
 * {@link java.util.concurrent.ThreadPoolExecutor} to create a new thread (up to maxPoolSize),
 * while returning {@code true} queues the task. This queue:
 *
 * <ul>
 *   <li>Returns {@code false} when pool threads are saturated (submitted count &gt; pool size)
 *       and the pool has not yet reached maximum size, forcing thread creation.</li>
 *   <li>Returns {@code true} when the pool is already at maximum size, queueing the task.</li>
 *   <li>Returns {@code true} when idle threads exist (submitted count &le; pool size).</li>
 * </ul>
 *
 * <p><b>Single-snapshot decision:</b> each counter ({@code submittedTasksCount}, pool size)
 * is read exactly once per {@code offer()} call, and the pool-size comparison reuses the
 * same snapshot — the previous formulation read them independently, leaving a TOCTOU window
 * in which the two values could be observed from different instants. {@code maximumPoolSize}
 * is snapshotted when the executor is attached ({@link #setStandardThreadExecutor}) and is
 * assumed stable for the executor's lifetime: a later {@code setMaximumPoolSize} call is
 * NOT observed (the thread-creation decision keeps using the value from attach time).
 *
 * <p>The queue must be attached to a {@link StandardThreadExecutor} before use — {@link #offer}
 * and {@link #force} dereference the attached executor and fail otherwise. The queue is
 * single-use: it attaches to exactly one executor and cannot be re-attached.
 *
 * <p>Unlike a standard {@link java.util.concurrent.LinkedBlockingQueue}, this queue has no
 * capacity limit — the upper bound on in-flight tasks is enforced by
 * {@link StandardThreadExecutor#maxSubmittedTaskCount}.
 */
@Internal
public class StandardExecutorQueue extends LinkedTransferQueue<Runnable> {

  private final AtomicReference<StandardThreadExecutor> executorRef = new AtomicReference<>();

  /**
   * Snapshot of the executor's maximum pool size, taken when the executor is attached.
   * Taken once — a later {@code setMaximumPoolSize} on the executor is not observed
   * (see class Javadoc).
   */
  private volatile int maxPoolSize;

  /**
   * Attaches this queue to its executor and snapshots {@code maxPoolSize} for the
   * lock-free {@link #offer} decision. The queue is single-use: a second attach — even
   * to the same executor — throws.
   *
   * @param executor the owning executor; must not be {@code null}
   * @throws NullPointerException  if {@code executor} is {@code null}
   * @throws IllegalStateException if the queue is already attached to an executor
   */
  public void setStandardThreadExecutor(StandardThreadExecutor executor) {
    Objects.requireNonNull(executor, "executor must not be null");
    Assert.state(
      executorRef.compareAndSet(null, executor),
      "StandardExecutorQueue is already attached to " + executorRef.get() + "; queues are single-use"
    );

    this.maxPoolSize = executor.getMaximumPoolSize();
  }

  /**
   * Attempt to forcibly insert a task after a rejected execution, as a last resort.
   *
   * <p>The termination check covers every non-RUNNING pool state ({@link
   * java.util.concurrent.ThreadPoolExecutor#isShutdown()} is already {@code true} for all of
   * them); {@code isTerminating()} is an additional guard for the shutdown-transition window.
   * Rejecting here is deliberate: once the pool is shutting down, a task queued through this
   * fallback would be drained at best, or silently left unexecuted at worst, so failing fast
   * lets the caller's rejection handler decide.
   *
   * @param o the task to insert
   * @return {@code true} if the task was accepted
   * @throws RejectedExecutionException if the executor is shut down
   */
  public boolean force(@NonNull Runnable o) {
    StandardThreadExecutor executor = executorRef.get();
    if (executor.isShutdown() || executor.isTerminating()) {
      throw new RejectedExecutionException("Executor is shut down, cannot force task into queue");
    }
    return super.offer(o);
  }

  /**
   * Tomcat-style queue-or-force-thread decision. Requires an attached executor —
   * the executor's {@code submittedTasksCount} and pool size drive the decision (see
   * class Javadoc); a queue used without {@link #setStandardThreadExecutor} fails with
   * a {@link NullPointerException} on the missing executor reference.
   */
  @Override
  public boolean offer(@NonNull Runnable o) {
    StandardThreadExecutor executor = executorRef.get();
    // Single-snapshot decision: read each volatile counter exactly once. Pool size is read
    // after the submitted count so the comparison always uses the freshest thread count.
    int submitted = executor.getSubmittedTasksCount();
    int poolSize = executor.getPoolSize();

    // idle threads available — queue the task
    if (submitted <= poolSize) {
      return super.offer(o);
    }

    // saturated and room to grow — force thread creation
    if (poolSize < maxPoolSize) {
      return false;
    }

    // at maximum size — queue
    return super.offer(o);
  }
}
