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

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Executes tasks with awareness of Spring transaction boundaries.
 * <p>
 * Within a transaction, tasks are deferred to {@code afterCommit}.
 * Outside a transaction:
 * <ul>
 *   <li>{@link #runAsyncAfterCommit} submits to an async executor</li>
 *   <li>{@link #runNowOrAfterCommit} executes synchronously on the caller's thread</li>
 * </ul>
 *
 *
 * <p><b>Asymmetry (deliberate):</b> despite the name,
 * {@link #runAsyncAfterCommit} runs the task <i>synchronously on the
 * committing thread</i> inside the {@code afterCommit} hook — only the
 * non-transactional path is asynchronous. The deferral preserves commit order
 * for a single transaction's own deferred work: a key's write-through is
 * version-bumped and applied only after its transaction commits, so it can
 * never precede the commit that produced it. Two transactions committing
 * concurrently run their afterCommit hooks on different threads with no
 * mutual exclusion — their version INCR and L1 applies DO interleave. What
 * makes that safe is the {@code VersionGuard} skip inside the putThrough L1
 * apply: a losing (older-version) write never overwrites a newer entry.
 * Keep this in mind when estimating request tail latency: within a
 * transaction the deferred work (writer, version bump, broadcast) is paid by
 * the committing thread before the hook returns.
 *
 * <p><b>Rollback behavior:</b> Deferred actions registered via
 * {@code runNowOrAfterCommit} are NOT executed if the surrounding
 * transaction rolls back. This is standard Spring
 * {@code TransactionSynchronization.afterCommit()} behavior.
  * Callers using {@code invalidate()} inside a transaction that may
  * roll back should be aware that the invalidation will be silently
  * dropped on rollback (every invalidation defers to after-commit, with or
  * without broadcast). For critical invalidations, register an
  * {@code afterCompletion} callback manually.
 */
@Slf4j
@Internal
public final class TransactionSupport {

  /**
   * Utility class — no instantiation.
   */
  private TransactionSupport() {}

  /**
   * Defer a task to after transaction commit, or submit to async executor outside a transaction.
   * Used by {@link HotKeyCache#putThrough} for async write-through.
   * Errors during async execution are logged but not propagated to the caller.
   * <p>
   * <b>Semantics note:</b> "async" applies to the non-transactional path only.
   * Inside a transaction the task runs synchronously on the committing thread
   * in the {@code afterCommit} hook (see the class Javadoc for why the
   * ordering guarantee is deliberate).
   * <p>
   * <b>Submission failure is absorbed too.</b> {@code CompletableFuture.runAsync}
   * throws <em>synchronously</em> when the executor rejects the task, and
   * {@code exceptionally()} only sees failures of the async stage — it cannot
   * catch a submit-time rejection. Left unhandled, that {@link
   * RejectedExecutionException} would surface as a success-turned-exception on a
   * business call whose method body already ran (and whose data-source write is
   * inside the rejected task, so the write silently never happens). The
   * rejection is therefore caught and logged at WARN here, matching what
   * {@code HotKeyCache.bumpAndInvalidate} already does for its own executor
   * submission — the cache update is best-effort, the caller's contract is not.
   *
   * @param task     the task to execute
   * @param executor async executor for the non-transactional case
   */
  public static void runAsyncAfterCommit(Runnable task, Executor executor) {
    if (TransactionSynchronizationManager.isSynchronizationActive()) {
      TransactionSynchronizationManager.registerSynchronization(
        new TransactionSynchronization() {
          @Override
          public void afterCommit() {
            // Same guard as runNowOrAfterCommit: a failure of the deferred body
            // must not turn an already-committed transaction into an error for
            // the caller. This is also what makes the "errors are logged, not
            // propagated" contract in the method Javadoc true on BOTH paths.
            // NOTE: failures are rethrown so commit callers observe them —
            // a silently skipped cache update after a committed DB write is
            // worse than a loud commit-path error (P0-4). Spring wraps this
            // as a transaction-system error; the data is already committed.
            try {
              task.run();
            } catch (RuntimeException e) {
              log.error("Deferred post-commit async task failed; transaction already committed", e);
              throw e;
            } catch (Exception e) {
              log.error("Deferred post-commit async task failed; transaction already committed", e);
              throw new IllegalStateException("Deferred post-commit task failed after commit", e);
            }
          }
        }
      );
      return;
    }
    log.debug("Called outside transaction, submitting to async executor");
    try {
      CompletableFuture.runAsync(task, executor).exceptionally(e -> {
        log.error("Async task failed after non-transactional call", e);
        return null;
      });
    } catch (RejectedExecutionException e) {
      log.warn(
        "Async cache task rejected by executor after a non-transactional call; cache update skipped " +
          "(the underlying write may not have been applied): {}",
        e.getMessage()
      );
    }
  }

  /**
   * Defer a task to after transaction commit, or execute synchronously outside a transaction.
   * Used by {@link HotKeyCache#invalidate(String, boolean)},
   * {@link HotKeyCache#invalidate(Iterable, boolean)}, and
   * {@link HotKeyCache#invalidateAfterPut(String, Runnable, boolean)}.
   * Exceptions propagate directly to the caller when executed outside a transaction.
   *
   * @param task the task to execute
   */
  public static void runNowOrAfterCommit(Runnable task) {
    if (TransactionSynchronizationManager.isSynchronizationActive()) {
      TransactionSynchronizationManager.registerSynchronization(
        new TransactionSynchronization() {
          @Override
          public void afterCommit() {
            // afterCommit() runs on the commit path: an exception escaping here
            // surfaces to the caller of commit() as a transaction-system error
            // even though the data was already committed. That loud failure is
            // deliberate (P0-4): a silently skipped cache update after a
            // committed DB write leaves cache and DB inconsistent with the
            // caller believing they agree.
            try {
              task.run();
            } catch (RuntimeException e) {
              log.error("Deferred post-commit cache task failed; transaction already committed", e);
              throw e;
            } catch (Exception e) {
              log.error("Deferred post-commit cache task failed; transaction already committed", e);
              throw new IllegalStateException("Deferred post-commit cache task failed after commit", e);
            }
          }
        }
      );
      return;
    }
    task.run();
  }
}
