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
package io.github.hyshmily.hotkey.cache.cachesupport;

import io.github.hyshmily.hotkey.Internal;
import io.github.hyshmily.hotkey.cache.HotKeyCache;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
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
 * <p><b>Rollback behavior:</b> Deferred actions registered via
 * {@code runNowOrAfterCommit} are NOT executed if the surrounding
 * transaction rolls back. This is standard Spring
 * {@code TransactionSynchronization.afterCommit()} behavior.
 * Callers using {@code invalidate()} inside a transaction that may
 * roll back should be aware that the invalidation will be silently
 * dropped on rollback. For critical invalidations, use
 * {@code invalidateLocal()} (immediate, no tx deferral) or register
 * an {@code afterCompletion} callback manually.
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
            task.run();
          }
        }
      );
      return;
    }
    log.debug("Called outside transaction, submitting to async executor");
    CompletableFuture.runAsync(task, executor).exceptionally(e -> {
      log.error("Async task failed after non-transactional call", e);
      return null;
    });
  }

  /**
   * Defer a task to after transaction commit, or execute synchronously outside a transaction.
   * Used by {@link HotKeyCache#invalidate}, {@link HotKeyCache#invalidateAllLocal},
   * and {@link HotKeyCache#invalidateAfterPut}.
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
            task.run();
          }
        }
      );
      return;
    }
    task.run();
  }
}
