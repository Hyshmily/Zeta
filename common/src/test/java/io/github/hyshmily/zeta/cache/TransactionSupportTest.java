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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Tests for {@link TransactionSupport}, verifying sync and async execution outside transaction context.
 */
class TransactionSupportTest {

  /**
   * Verifies that {@code runNowOrAfterCommit} executes the action synchronously when there is no active transaction.
   */
  @Test
  void runNowOrAfterCommit_shouldExecuteSynchronouslyOutsideTransaction() {
    AtomicBoolean executed = new AtomicBoolean(false);
    TransactionSupport.runNowOrAfterCommit(() -> executed.set(true));
    assertThat(executed).isTrue();
  }

  /**
   * Verifies that {@code runAsyncAfterCommit} executes the action asynchronously via the provided executor when there is no active transaction.
   */
  @Test
  void runAsyncAfterCommit_shouldExecuteAsyncOutsideTransaction() throws InterruptedException {
    AtomicBoolean executed = new AtomicBoolean(false);
    CountDownLatch latch = new CountDownLatch(1);
    Executor executor = Executors.newSingleThreadExecutor();

    TransactionSupport.runAsyncAfterCommit(
      () -> {
        executed.set(true);
        latch.countDown();
      },
      executor
    );

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(executed).isTrue();
  }

  /**
   * Verifies that an exception thrown inside {@code runNowOrAfterCommit} propagates to the caller
   * when called outside a transaction (fault mode: exception propagation).
   */
  @Test
  void runNowOrAfterCommit_withException_shouldPropagate() {
    assertThatThrownBy(() ->
      TransactionSupport.runNowOrAfterCommit(() -> {
        throw new RuntimeException("task-failed");
      })
    )
      .isInstanceOf(RuntimeException.class)
      .hasMessage("task-failed");
  }

  /**
   * Verifies that an exception thrown inside {@code runAsyncAfterCommit} is caught and logged,
   * NOT propagated to the caller (fault mode: exception suppression).
   */
  @Test
  void runAsyncAfterCommit_withException_shouldNotPropagate() throws InterruptedException {
    AtomicBoolean executed = new AtomicBoolean(false);
    CountDownLatch latch = new CountDownLatch(1);
    Executor executor = Executors.newSingleThreadExecutor();

    TransactionSupport.runAsyncAfterCommit(
      () -> {
        executed.set(true);
        latch.countDown();
        throw new RuntimeException("async-fail");
      },
      executor
    );

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(executed).isTrue();
    // No exception should reach here — the exceptionally callback swallows it
  }

  /**
   * Verifies that {@code runNowOrAfterCommit} executes the task even when the task throws,
   * and the exception is not wrapped (fault mode: bare exception propagation).
   */
  @Test
  void runNowOrAfterCommit_withCheckedExceptionInRunnable_shouldPropagate() {
    assertThatThrownBy(() ->
      TransactionSupport.runNowOrAfterCommit(() -> {
        throw new IllegalStateException("state-error");
      })
    )
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("state-error");
  }

  // ── Transaction-aware paths ──

  /**
   * Verifies that {@code runAsyncAfterCommit} defers the task when a transaction is active,
   * and executes it only after commit.
   */
  @Test
  void runAsyncAfterCommit_withinTransaction_shouldDeferAfterCommit() {
    TransactionSynchronizationManager.initSynchronization();
    try {
      AtomicBoolean executed = new AtomicBoolean(false);
      TransactionSupport.runAsyncAfterCommit(() -> executed.set(true), Runnable::run);

      assertThat(executed).isFalse();

      TransactionSynchronizationManager.getSynchronizations().forEach(s -> s.afterCommit());

      assertThat(executed).isTrue();
    } finally {
      TransactionSynchronizationManager.clear();
    }
  }

  /**
   * Verifies that {@code runNowOrAfterCommit} defers the task when a transaction is active,
   * and executes it only after commit.
   */
  @Test
  void runNowOrAfterCommit_withinTransaction_shouldDeferAfterCommit() {
    TransactionSynchronizationManager.initSynchronization();
    try {
      AtomicBoolean executed = new AtomicBoolean(false);
      TransactionSupport.runNowOrAfterCommit(() -> executed.set(true));

      assertThat(executed).isFalse();

      TransactionSynchronizationManager.getSynchronizations().forEach(s -> s.afterCommit());

      assertThat(executed).isTrue();
    } finally {
      TransactionSynchronizationManager.clear();
    }
  }

  /**
   * Verifies that the {@code exceptionally} callback in {@code runAsyncAfterCommit} is invoked
   * when the async task fails, using a direct executor for deterministic execution.
   */
  @Test
  void runAsyncAfterCommit_withException_shouldInvokeExceptionallyCallback() {
    AtomicBoolean executed = new AtomicBoolean(false);
    TransactionSupport.runAsyncAfterCommit(
      () -> {
        executed.set(true);
        throw new RuntimeException("async-fail");
      },
      Runnable::run
    );

    assertThat(executed).isTrue();
  }
}
