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
package io.github.hyshmily.zeta.worker.dispatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the worker decision-send buffer (ADR-0061): asynchronous drain,
 * rollback callback on send failure, drop-on-saturation with the rollback
 * applied on the caller thread, and drain-on-shutdown.
 */
class WorkerBroadcastBufferTest {

  private WorkerBroadcastBuffer buffer;

  @AfterEach
  void tearDown() {
    if (buffer != null) {
      buffer.shutdown();
    }
  }

  @Test
  void submit_shouldExecuteSendAsynchronously() throws Exception {
    buffer = new WorkerBroadcastBuffer(100);
    CountDownLatch sent = new CountDownLatch(1);
    AtomicInteger rollbacks = new AtomicInteger();

    buffer.submit(
      () -> {
        sent.countDown();
        return true;
      },
      rollbacks::incrementAndGet
    );

    assertThat(sent.await(2, TimeUnit.SECONDS)).as("send executed on the drain thread").isTrue();
    assertThat(rollbacks.get()).isZero();
  }

  @Test
  void submit_failedSend_shouldInvokeRollbackOnDrainThread() throws Exception {
    buffer = new WorkerBroadcastBuffer(100);
    CountDownLatch rolledBack = new CountDownLatch(1);
    AtomicInteger rollbacks = new AtomicInteger();

    buffer.submit(
      () -> false,
      () -> {
        rollbacks.incrementAndGet();
        rolledBack.countDown();
      }
    );

    assertThat(rolledBack.await(2, TimeUnit.SECONDS)).as("rollback applied after failed send").isTrue();
    assertThat(rollbacks.get()).isEqualTo(1);
  }

  @Test
  void submit_throwingSend_shouldInvokeRollbackAndNotPropagate() throws Exception {
    buffer = new WorkerBroadcastBuffer(100);
    CountDownLatch rolledBack = new CountDownLatch(1);

    buffer.submit(
      () -> {
        throw new IllegalStateException("send blew up");
      },
      rolledBack::countDown
    );

    assertThat(rolledBack.await(2, TimeUnit.SECONDS)).as("rollback applied after throwing send").isTrue();
  }

  @Test
  void submit_saturatedQueue_shouldDropAndRollbackOnCallerThread() throws Exception {
    // Capacity 1; the first send BLOCKS on a latch so the single drain thread
    // stays busy and the queue fills up — the next submit must be rejected.
    buffer = new WorkerBroadcastBuffer(1);
    CountDownLatch blocker = new CountDownLatch(1);
    CountDownLatch firstSendStarted = new CountDownLatch(1);
    AtomicInteger rollbacks = new AtomicInteger();
    String[] rollbackThreadName = new String[1];

    buffer.submit(
      () -> {
        firstSendStarted.countDown();
        try {
          blocker.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return true;
      },
      rollbacks::incrementAndGet
    );
    // Fill the single queue slot.
    buffer.submit(() -> true, rollbacks::incrementAndGet);

    assertThat(firstSendStarted.await(2, TimeUnit.SECONDS)).isTrue();

    // Queue full + drain thread busy → RejectedExecutionException on THIS
    // (caller) thread: dropped decision, rollback applied synchronously.
    buffer.submit(
      () -> true,
      () -> {
        rollbacks.incrementAndGet();
        rollbackThreadName[0] = Thread.currentThread().getName();
      }
    );

    assertThat(rollbacks.get()).as("saturated submit rolled back immediately").isEqualTo(1);
    assertThat(rollbackThreadName[0]).as("drop rollback runs on the caller thread").isEqualTo(Thread.currentThread().getName());

    blocker.countDown();
  }

  @Test
  void submit_afterShutdown_shouldDropAndRollbackWithoutThrowing() {
    buffer = new WorkerBroadcastBuffer(100);
    buffer.shutdown();
    AtomicInteger rollbacks = new AtomicInteger();

    assertThatCode(() -> buffer.submit(() -> true, rollbacks::incrementAndGet)).doesNotThrowAnyException();
    assertThat(rollbacks.get()).as("post-shutdown submit rolled back on the caller thread").isEqualTo(1);
  }

  @Test
  void submit_multipleKeys_shouldPreserveSubmissionOrder() throws Exception {
    buffer = new WorkerBroadcastBuffer(100);
    CountDownLatch drained = new CountDownLatch(1);
    StringBuilder order = new StringBuilder();

    for (int i = 0; i < 5; i++) {
      int seq = i;
      buffer.submit(
        () -> {
          order.append(seq);
          return true;
        },
        () -> {}
      );
    }
    buffer.submit(
      () -> {
        drained.countDown();
        return true;
      },
      () -> {}
    );

    assertThat(drained.await(2, TimeUnit.SECONDS)).isTrue();
    assertThat(order.toString()).as("single drain thread preserves submission order").isEqualTo("01234");
  }
}
