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
package io.github.hyshmily.hotkey.cache;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import io.github.hyshmily.hotkey.logging.DefaultLogger;
import io.github.hyshmily.hotkey.logging.HotKeyLogger;

/**
 * Executes tasks with awareness of Spring transaction boundaries.
 * <p>
 * Within a transaction, tasks are deferred to {@code afterCommit}.
 * Outside a transaction:
 * <ul>
 *   <li>{@link #runAsyncAfterCommit} submits to an async executor</li>
 *   <li>{@link #runNowOrAfterCommit} executes synchronously on the caller's thread</li>
 * </ul>
 */
public final class TransactionSupport {

  /** Logger for this class. */
  private static final HotKeyLogger log = new DefaultLogger(TransactionSupport.class);

  /**
   * Utility class — no instantiation.
   */
  private TransactionSupport() {}

  /**
   * Defer a task to after transaction commit, or submit to async executor outside a transaction.
   * Used by {@link HotKeyCache#putThrough} for async write-through.
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
   * Used by {@link HotKeyCache#invalidate}, {@link HotKeyCache#invalidateAll},
   * and {@link HotKeyCache#putBeforeInvalidate}.
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
