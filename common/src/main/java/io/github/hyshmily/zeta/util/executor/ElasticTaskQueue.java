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

import io.github.hyshmily.zeta.util.Internal;
import java.util.Objects;
import java.util.concurrent.LinkedTransferQueue;
import org.jspecify.annotations.NonNull;
import org.springframework.util.Assert;

/**
 * A {@link LinkedTransferQueue} whose {@link #offer(Runnable)} method cooperates with
 * {@link ElasticThreadPool} to implement Tomcat-style core → max → queue → reject ordering.
 *
 * <p>The key insight is that returning {@code false} from {@code offer()} causes
 * {@link java.util.concurrent.ThreadPoolExecutor} to create a new thread (up to maxPoolSize),
 * while returning {@code true} queues the task. This queue:
 *
 * <ul>
 *   <li>Returns {@code false} when pool threads are saturated (submitted count &gt; pool size)
 *       and the pool has not yet reached maximum size, forcing thread creation.</li>
 *   <li>Returns {@code true} when the pool is already at maximum size, queueing the task.</li>
 *   <li>Returns {@code true} when the pool can absorb the task without growing
 *       (submitted count &le; pool size).</li>
 * </ul>
 *
 * <p><b>Lock-free counters first:</b> the decision reads {@code submittedTasksCount},
 * {@code corePoolSize} and {@code maximumPoolSize} — all plain fields — and only reads the
 * pool size (which takes the pool's {@code mainLock}) in the elastic range where it can
 * change the outcome. Before that read it tries a direct transfer to a worker already
 * waiting in {@code take()}/{@code poll()}: a successful transfer proves the queue was
 * empty and {@code submitted <= poolSize}, so growing the pool could not have been the
 * decision anyway — the transfer only removes the {@code mainLock} acquisition. The
 * maximum is read from the owning executor on each decision, so a runtime
 * {@code setMaximumPoolSize} is observed without a mirror field to keep in sync.
 *
 * <p>The queue must be attached to a {@link ElasticThreadPool} before use: {@link #offer}
 * dereferences the attached executor and fails without one, and {@link #force} declines
 * ({@code false}). The queue is single-use: it attaches to exactly one executor and cannot be
 * re-attached.
 *
 * <p>Unlike a standard {@link java.util.concurrent.LinkedBlockingQueue}, this queue has no
 * capacity limit — the upper bound on in-flight tasks is enforced by
 * {@link ElasticThreadPool#maxSubmittedTaskCount}.
 */
@Internal
public class ElasticTaskQueue extends LinkedTransferQueue<Runnable> {

  /**
   * Owning executor. Written once by {@link #attach} and read lock-free by {@link #offer}; volatile
   * so a thread that attaches after construction is still seen by an unsynchronized offer.
   */
  private volatile ElasticThreadPool executor;

  /**
   * Attaches this queue to its executor. The queue is single-use: a second attach — even
   * to the same executor — throws.
   *
   * @param executor the owning executor; must not be {@code null}
   * @throws NullPointerException  if {@code executor} is {@code null}
   * @throws IllegalStateException if the queue is already attached to an executor
   */
  public synchronized void attach(ElasticThreadPool executor) {
    Objects.requireNonNull(executor, "executor must not be null");
    Assert.state(
      this.executor == null,
      "ElasticTaskQueue is already attached to " + this.executor + "; queues are single-use"
    );
    this.executor = executor;
  }

  /** Serializes the force path with {@code shutdownNow}'s STOP-and-drain transition. */
  private final Object lifecycleLock = new Object();

  /**
   * The lock shared with the owning executor's {@code shutdownNow()}. A forced
   * insert must not slip into the queue between the executor's shutdown check
   * and its drain.
   *
   * @return the lifecycle lock
   */
  Object lifecycleLock() {
    return lifecycleLock;
  }

  /**
   * Attempt to forcibly insert a task after a rejected execution, as a last resort.
   *
   * <p>Once the pool is shutting down the task is refused: a task queued through this
   * fallback would be drained at best, or silently left unexecuted at worst, so the
   * caller's rejection handler decides. The shutdown check, the offer and the worker
   * guarantee share the lifecycle lock with both shutdown methods, so neither a concurrent
   * STOP-and-drain nor a graceful shutdown can strand the task after the check.
   *
   * <p>A forced insert bypasses {@code ThreadPoolExecutor}'s own "workerCount == 0 → start a
   * worker" recheck. With the pool's threads timed out (core threads may time out) the task
   * would sit in the queue forever, and a later {@code shutdown()} could never terminate.
   * The insert therefore starts a core worker, or — when the pool has no core thread to
   * start and no live worker — takes the task back out and lets the rejection handler run.
   *
   * @param task the task to insert
   * @return {@code true} if the task was accepted; {@code false} when the queue is
   *         unattached, the executor is shut down, or no worker can consume it
   */
  public boolean force(@NonNull Runnable task) {
    synchronized (lifecycleLock) {
      ElasticThreadPool owner = executor;
      if (owner == null || owner.isShutdown() || !super.offer(task)) {
        return false;
      }
      boolean workerStarted;
      try {
        workerStarted = owner.prestartCoreThread();
      } catch (RuntimeException | Error e) {
        // A thread factory failure must not leave the inserted task behind: undo the insert and
        // let the caller's rejection handler own the failure. A concurrent worker may already
        // have taken the task; remove() then returns false and the task is being executed.
        return !super.remove(task);
      }
      if (workerStarted) {
        return true;
      }
      if (owner.getPoolSize() > 0) {
        // A non-core worker exists; the pool's own shutdown path also handles the queue.
        return true;
      }
      // No core thread to start and no live worker: remove the task and report the refusal
      // instead of leaving it in a queue nothing can drain. If the remove loses to a worker
      // that appeared concurrently, the task is already owned and will execute.
      return !super.remove(task);
    }
  }

  /**
   * Tomcat-style queue-or-force-thread decision. Requires an attached executor —
   * the executor's {@code submittedTasksCount} and pool size drive the decision (see
   * class Javadoc); a queue used without {@link #attach} fails with
   * a {@link NullPointerException} on the missing executor reference.
   */
  @Override
  public boolean offer(@NonNull Runnable task) {
    ElasticThreadPool owner = executor;
    int submitted = owner.getSubmittedTasksCount();
    // ThreadPoolExecutor.execute reaches workQueue.offer only when workerCount is already at
    // least corePoolSize, so fewer outstanding tasks than core threads means an idle core
    // thread: queueing is the correct outcome, and it avoids getPoolSize() — which takes the
    // pool's global mainLock on every submission.
    int coreSize = owner.getCorePoolSize();
    if (submitted <= coreSize) {
      return super.offer(task);
    }

    int maxSize = owner.getMaximumPoolSize();
    // A fixed-size pool (core == max) has no growth step to force, so the pool size cannot
    // change the decision either.
    if (coreSize >= maxSize) {
      return super.offer(task);
    }

    // A waiting worker is an idle consumer: hand the task over directly instead of
    // taking the pool's global mainLock to read the size. A successful transfer proves
    // the queue was empty and submitted <= poolSize, so the size read could not have
    // changed the decision from "queue" to "grow".
    if (tryTransfer(task)) {
      return true;
    }

    // Elastic range only: read the pool size (one mainLock acquisition) to decide between
    // growing and queueing. Read after the submitted count so the comparison uses the
    // freshest thread add.
    int poolSize = owner.getPoolSize();
    if (submitted <= poolSize || poolSize >= maxSize) {
      return super.offer(task);
    }
    return false;
  }
}
