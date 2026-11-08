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
package io.github.hyshmily.zeta.cache.cachesupport;

import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.util.Internal;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Runs cache write-path tasks at the right Spring transaction boundary.
 * <p>
 * Within a transaction the task is deferred to {@code afterCommit} and runs
 * <i>synchronously on the committing thread</i>; outside one it executes
 * inline on the caller's thread ({@link #run(boolean, Runnable)}). The deferred
 * task itself has no executor-backed async path: the version bump and the L1
 * apply run on the committing thread, so their order is a property of the call
 * site rather than of executor scheduling (design decisions: invalidation).
 *
 * <p>The deferral preserves commit order for a single transaction's own
 * deferred work: a key's write-through is version-bumped and applied only
 * after its transaction commits, so it can never precede the commit that
 * produced it. Two transactions committing concurrently run their afterCommit
 * hooks on different threads with no mutual exclusion — their version INCR and
 * L1 applies DO interleave. What makes that safe is the {@code VersionGuard}
 * skip inside the putThrough L1 apply: a losing (older-version) write never
 * overwrites a newer entry. Keep this in mind when estimating request tail
 * latency: within a transaction the deferred work (reader, version bump, L1
 * apply, broadcast submission) is paid by the committing thread before the hook
 * returns.
 *
 * <p><b>Failure propagation.</b> A task that fails inside the {@code afterCommit}
 * hook is rethrown to the committing caller, because a silently skipped cache
 * update after a committed write is worse than a loud commit-path error — the
 * transaction is already committed either way. Outside a transaction the
 * failure propagates directly to the caller.
 *
 * <p><b>Rollback behavior:</b> Deferred actions registered via
 * {@code run} are NOT executed if the surrounding
 * transaction rolls back. This is standard Spring
 * {@code TransactionSynchronization.afterCommit()} behavior.
 * Callers using {@code invalidate()} with {@code immediate = false} inside a
 * transaction that may roll back should be aware that the invalidation is
 * dropped on rollback, whether or not it broadcasts; {@code immediate = true}
 * runs on the calling thread before the transaction settles. For critical
 * invalidations, register an {@code afterCompletion} callback manually.
 */
@Internal
public final class TransactionSupport {

  /**
   * Utility class — no instantiation.
   */
  private TransactionSupport() {}

  /**
   * Whether a Spring transaction synchronization is active on the current thread.
   *
   * <p>Write paths consult this to decide version-allocation timing: inside a transaction the
   * version bump must wait for {@code afterCommit} (registration order = version order),
   * otherwise an uncommitted bump leaks on rollback and a same-transaction put-then-invalidate
   * inverts versions.
   *
   * @return {@code true} when deferred work would run in {@code afterCommit}
   */
  private static boolean isTransactionActive() {
    return TransactionSynchronizationManager.isSynchronizationActive();
  }

  /**
   * Whether a write-path task must wait for {@code afterCommit}: the transaction is
   * active and the caller did not request an immediate run. This is the single owner
   * of that decision for both {@link #run} and the clear paths.
   *
   * @param immediate {@code true} runs on the calling thread even inside a transaction
   * @return {@code true} when the task belongs in {@code afterCommit}
   */
  public static boolean isDeferred(boolean immediate) {
    return !immediate && isTransactionActive();
  }

  /**
   * Run a write-path task at the right boundary: inline when {@code immediate} is set or no
   * transaction is active, otherwise from {@code afterCommit} on the committing thread.
   *
   * <p>Delegates the deferral decision to {@link #isDeferred(boolean)}; call sites pass their
   * {@code immediate} flag instead of testing the transaction state themselves. Used by the
   * {@link HotKeyCache} write paths ({@code putThrough}, {@code putLocal}, {@code refresh},
   * {@code invalidate}) so version allocation and the L1 apply share the commit boundary
   * (design decisions: invalidation).
   *
   * @param immediate {@code true} runs on the calling thread even inside a transaction
   * @param task      the task to execute
   */
  public static void run(boolean immediate, Runnable task) {
    if (isDeferred(immediate)) {
      registerAfterCommit(task);
    } else {
      task.run();
    }
  }

  /**
   * Registers the task on the current transaction's afterCommit hook. The caller
   * has already established that a synchronization is active.
   *
   * @param task the task to run after commit
   */
  private static void registerAfterCommit(Runnable task) {
    TransactionSynchronizationManager.registerSynchronization(
      new TransactionSynchronization() {
        @Override
        public void afterCommit() {
          // The committing thread runs one deferred task; a failure reaches the commit caller
          // as a transaction-system error instead of being silently skipped.
          task.run();
        }
      }
    );
  }
}
