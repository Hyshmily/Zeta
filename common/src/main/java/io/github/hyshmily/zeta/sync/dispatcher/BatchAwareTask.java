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
package io.github.hyshmily.zeta.sync.dispatcher;

import io.github.hyshmily.zeta.Internal;

/**
 * A dispatch task that receives the batch-end signal of its key's current dispatch cycle
 * (LMAX Disruptor {@code BatchEventProcessor} semantics, borrowed via ADR-0071).
 *
 * <p>{@link PerKeyOrderedDispatcher} grants up to {@code maxTasksPerCycle} queued tasks of a
 * single key per executor cycle and runs them back-to-back. A batch-aware task is invoked with
 * {@code endOfBatch == false} while tasks remain in the granted batch, and with
 * {@code endOfBatch == true} on the <b>last task of the granted batch</b> — the same hint
 * Disruptor's {@code EventHandler.onEvent(event, sequence, endOfBatch)} gives its consumers.
 *
 * <p><b>Intended use — amortizing expensive work to the batch tail.</b> If several queued tasks
 * for the same key each perform an expensive I/O that a later task in the same batch would
 * repeat (e.g. a REFRESH sync message triggering a Redis load for a key that has further
 * REFRESHes queued behind it), the non-final tasks can skip the I/O and let the
 * {@code endOfBatch == true} task produce the final state once per batch.
 *
 * <p><b>Best-effort, like Disruptor.</b> The batch is "what is queued at grant time": a task may
 * be granted alone (and always runs with {@code endOfBatch == true}), and a submission racing the
 * batch extends the key's work into a follow-up cycle whose last task gets the signal again.
 * Consumers must stay correct for any batch boundaries — the signal reduces I/O, it never
 * changes what the final applied state must be.
 *
 * <p>Submit via {@link PerKeyOrderedDispatcher#submitWithWeight(Object, BatchAwareTask, int)}.
 * Plain {@link Runnable} submissions are unaffected and keep their zero-overhead path.
 */
@FunctionalInterface
@Internal
public interface BatchAwareTask {

  /**
   * Executes the task.
   *
   * @param endOfBatch {@code true} if this is the last task of the key's currently granted
   *                   batch; {@code false} if further tasks of the same batch follow
   */
  void run(boolean endOfBatch);
}
