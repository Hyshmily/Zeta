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
package io.github.hyshmily.hotkey.hotkeycache;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TransactionSupport}, verifying sync and async execution outside transaction context.
 */
class TransactionSupportTest {

  @Test
  void runNowOrAfterCommit_shouldExecuteSynchronouslyOutsideTransaction() {
    AtomicBoolean executed = new AtomicBoolean(false);
    TransactionSupport.runNowOrAfterCommit(() -> executed.set(true));
    assertThat(executed).isTrue();
  }

  @Test
  void runAsyncAfterCommit_shouldExecuteAsyncOutsideTransaction() throws InterruptedException {
    AtomicBoolean executed = new AtomicBoolean(false);
    CountDownLatch latch = new CountDownLatch(1);
    Executor executor = Executors.newSingleThreadExecutor();

    TransactionSupport.runAsyncAfterCommit(() -> {
      executed.set(true);
      latch.countDown();
    }, executor);

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(executed).isTrue();
  }
}
