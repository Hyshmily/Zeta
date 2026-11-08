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
package io.github.hyshmily.zeta.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hyshmily.zeta.cache.cachesupport.TransactionSupport;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Tests for {@link TransactionSupport}: synchronous execution outside a transaction,
 * deferral to {@code afterCommit} inside one, and failure propagation.
 */
class TransactionSupportTest {

  /**
   * Verifies that {@code run(false, ...)} executes the action synchronously when there is no active transaction.
   */
  @Test
  void run_shouldExecuteSynchronouslyOutsideTransaction() {
    AtomicBoolean executed = new AtomicBoolean(false);
    TransactionSupport.run(false, () -> executed.set(true));
    assertThat(executed).isTrue();
  }

  /**
   * Verifies that an exception thrown inside {@code run(false, ...)} propagates to the caller
   * when called outside a transaction (fault mode: exception propagation).
   */
  @Test
  void run_withException_shouldPropagate() {
    assertThatThrownBy(() ->
      TransactionSupport.run(false, () -> {
        throw new RuntimeException("task-failed");
      })
    )
      .isInstanceOf(RuntimeException.class)
      .hasMessage("task-failed");
  }

  /**
   * Verifies that {@code run(false, ...)} executes the task even when the task throws,
   * and the exception is not wrapped (fault mode: bare exception propagation).
   */
  @Test
  void run_withCheckedExceptionInRunnable_shouldPropagate() {
    assertThatThrownBy(() ->
      TransactionSupport.run(false, () -> {
        throw new IllegalStateException("state-error");
      })
    )
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("state-error");
  }

  // ── Transaction-aware paths ──

  /**
   * Verifies that {@code run(false, ...)} defers the task when a transaction is active,
   * and executes it only after commit.
   */
  @Test
  void run_withinTransaction_shouldDeferAfterCommit() {
    TransactionSynchronizationManager.initSynchronization();
    try {
      AtomicBoolean executed = new AtomicBoolean(false);
      TransactionSupport.run(false, () -> executed.set(true));

      assertThat(executed).isFalse();

      TransactionSynchronizationManager.getSynchronizations().forEach(s -> s.afterCommit());

      assertThat(executed).isTrue();
    } finally {
      TransactionSynchronizationManager.clear();
    }
  }

  /**
   * Verifies that a failure inside the deferred {@code afterCommit} body is logged and rethrown
   * to the committing caller (P0-4) — a post-commit cache failure must never be silent.
   */
  @Test
  void run_withinTransaction_failure_shouldRethrow() {
    TransactionSynchronizationManager.initSynchronization();
    try {
      TransactionSupport.run(false, () -> {
        throw new IllegalStateException("post-commit-fail");
      });

      assertThatThrownBy(() -> TransactionSynchronizationManager.getSynchronizations().forEach(s -> s.afterCommit()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("post-commit-fail");
    } finally {
      TransactionSynchronizationManager.clear();
    }
  }
}
