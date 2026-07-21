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
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.NonNull;

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
 * <p>Unlike a standard {@link java.util.concurrent.LinkedBlockingQueue}, this queue has no
 * capacity limit — the upper bound on in-flight tasks is enforced by
 * {@link StandardThreadExecutor#maxSubmittedTaskCount}.
 */
@Internal
public class StandardExecutorQueue extends LinkedTransferQueue<Runnable> {

  private final AtomicReference<StandardThreadExecutor> executorRef = new AtomicReference<>();

  public void setStandardThreadExecutor(StandardThreadExecutor executor) {
    this.executorRef.set(executor);
  }

  /**
   * Attempt to forcibly insert a task after a rejected execution, as a last resort.
   *
   * @param o the task to insert
   * @return {@code true} if the task was accepted
   * @throws RejectedExecutionException if the executor is shut down
   */
  public boolean force(@NonNull Runnable o) {
    StandardThreadExecutor executor = executorRef.get();
    if (executor.isShutdown()) {
      throw new RejectedExecutionException("Executor is shut down, cannot force task into queue");
    }
    return super.offer(o);
  }

  @Override
  public boolean offer(@NonNull Runnable o) {
    StandardThreadExecutor executor = executorRef.get();
    int poolSize = executor.getPoolSize();
    int maxPoolSize = executor.getMaximumPoolSize();

    if (poolSize == maxPoolSize) {
      return super.offer(o);
    }

    // idle threads available — queue the task
    if (executor.getSubmittedTasksCount() <= poolSize) {
      return super.offer(o);
    }

    // force thread creation
    if (poolSize < maxPoolSize) {
      return false;
    }

    return super.offer(o);
  }
}
