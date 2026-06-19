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
package io.github.hyshmily.hotkey.hotkeydetector.doublebuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BufferedCounterTest {

  private List<Map<String, Long>> batches;
  private Consumer<Map<String, Long>> consumer;
  private BufferedCounter counter;

  @BeforeEach
  void setUp() {
    batches = new ArrayList<>();
    consumer = batches::add;
    counter = new BufferedCounter(consumer);
  }

  @AfterEach
  void tearDown() {
    counter.destroy();
  }

  @Test
  void count_shouldRecordSingleKey() {
    counter.count("key1", 1);
    counter.destroy();

    assertThat(batches).hasSize(1);
    assertThat(batches.get(0)).containsEntry("key1", 1L);
  }

  @Test
  void count_shouldRecordMultipleKeysIndependently() {
    counter.count("key1", 1);
    counter.count("key2", 2);
    counter.destroy();

    assertThat(batches).hasSize(1);
    assertThat(batches.get(0))
        .containsEntry("key1", 1L)
        .containsEntry("key2", 2L);
  }

  @Test
  void count_shouldAccumulateDelta() {
    counter.count("key1", 3);
    counter.count("key1", 4);
    counter.destroy();

    assertThat(batches).hasSize(1);
    assertThat(batches.get(0)).containsEntry("key1", 7L);
  }

  @Test
  void flush_shouldDeliverAndReset() {
    counter.count("key1", 5);
    counter.destroy();

    assertThat(batches).hasSize(1);
    assertThat(batches.get(0)).containsEntry("key1", 5L);
  }

  @Test
  void flush_shouldNotCallConsumerWhenEmpty() {
    List<Map<String, Long>> emptyBatches = new ArrayList<>();
    BufferedCounter emptyCounter = new BufferedCounter(emptyBatches::add);
    emptyCounter.destroy();

    assertThat(emptyBatches).isEmpty();
  }

  @Test
  void afterPropertiesSet_shouldStartScheduler() throws Exception {
    counter.afterPropertiesSet();
    counter.count("key1", 1);
    Thread.sleep(600);

    assertThat(batches).isNotEmpty();
    Map<String, Long> merged = new HashMap<>();
    for (Map<String, Long> batch : batches) {
      merged.putAll(batch);
    }
    assertThat(merged).containsKey("key1");
  }

  @Test
  void destroy_shouldFlushRemainingCounts() {
    counter.count("key1", 2);
    counter.destroy();

    assertThat(batches).hasSize(1);
    assertThat(batches.get(0)).containsEntry("key1", 2L);
  }

  @Test
  void count_shouldBeThreadSafe() throws Exception {
    int threadCount = 5;
    int incrementsPerThread = 1000;
    CountDownLatch latch = new CountDownLatch(threadCount);
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);

    for (int i = 0; i < threadCount; i++) {
      String key = "key" + i;
      executor.execute(() -> {
        for (int j = 0; j < incrementsPerThread; j++) {
          counter.count(key, 1);
        }
        latch.countDown();
      });
    }

    assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
    executor.shutdown();
    counter.destroy();

    Map<String, Long> merged = new HashMap<>();
    for (Map<String, Long> batch : batches) {
      for (Map.Entry<String, Long> entry : batch.entrySet()) {
        merged.merge(entry.getKey(), entry.getValue(), Long::sum);
      }
    }

    for (int i = 0; i < threadCount; i++) {
      assertThat(merged).containsEntry("key" + i, (long) incrementsPerThread);
    }
  }

  @Test
  void count_shouldTriggerSwitchViaFlushCycle() throws Exception {
    counter.afterPropertiesSet();
    counter.count("k1", 1);
    counter.count("k2", 1);
    Thread.sleep(600);
    counter.count("k3", 1);
    counter.destroy();

    Map<String, Long> merged = new HashMap<>();
    for (Map<String, Long> batch : batches) {
      for (Map.Entry<String, Long> entry : batch.entrySet()) {
        merged.merge(entry.getKey(), entry.getValue(), Long::sum);
      }
    }

    assertThat(merged)
        .containsEntry("k1", 1L)
        .containsEntry("k2", 1L)
        .containsEntry("k3", 1L);
  }

  @Test
  void doubleBuffer_shouldIsolateAfterSwitch() throws Exception {
    counter.afterPropertiesSet();
    counter.count("before_switch", 1);
    Thread.sleep(600);
    counter.count("after_switch", 1);
    counter.destroy();

    assertThat(batches).isNotEmpty();
    Map<String, Long> merged = new HashMap<>();
    for (Map<String, Long> batch : batches) {
      for (Map.Entry<String, Long> entry : batch.entrySet()) {
        merged.merge(entry.getKey(), entry.getValue(), Long::sum);
      }
    }

    assertThat(merged)
        .containsEntry("before_switch", 1L)
        .containsEntry("after_switch", 1L);
  }

  @Test
  void count_shouldThrowOnNullKey() {
    assertThatThrownBy(() -> counter.count(null, 1))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void count_shouldHandleZeroDelta() {
    counter.count("key1", 0);
    counter.destroy();
    assertThat(batches).isEmpty();
  }

  @Test
  void count_shouldHandleNegativeDelta() {
    counter.count("key1", 5);
    counter.count("key1", -3);
    counter.destroy();
    assertThat(batches).hasSize(1);
    assertThat(batches.get(0)).containsEntry("key1", 2L);
  }

  @Test
  void destroy_shouldBeIdempotent() {
    counter.destroy();
    counter.destroy();
  }

  @Test
  void consumerException_shouldNotPropagate() throws Exception {
    List<Map<String, Long>> failingBatches = new ArrayList<>();
    Consumer<Map<String, Long>> throwingConsumer = batch -> {
      failingBatches.add(batch);
      throw new RuntimeException("simulated consumer failure");
    };
    BufferedCounter throwingCounter = new BufferedCounter(throwingConsumer);
    throwingCounter.afterPropertiesSet();
    throwingCounter.count("key1", 10);
    Thread.sleep(600);
    throwingCounter.destroy();
    assertThat(failingBatches).isNotEmpty();
  }

  @Test
  void count_withNegativeDeltaLargerThanPositive_shouldAllowNegative() {
    counter.count("key1", 3);
    counter.count("key1", -5);
    counter.destroy();
    // Net count is -2; drain() filters out non-positive values
    assertThat(batches).isEmpty();
  }

  @Test
  void count_withDeltaLongMinValue_shouldNotThrow() {
    counter.count("key1", Long.MIN_VALUE);
    counter.destroy();
    // Long.MIN_VALUE is negative, filtered out during drain
    assertThat(batches).isEmpty();
  }

  /**
   * Verifies that {@code destroy()} with a shared (externally-provided) scheduler does NOT
   * shut down the scheduler ({@code ownsScheduler=false} branch).
   */
  @Test
  void destroy_withSharedScheduler_shouldNotShutdownScheduler() {
    ScheduledExecutorService shared = Executors.newSingleThreadScheduledExecutor();
    try {
      List<Map<String, Long>> sharedBatches = new ArrayList<>();
      BufferedCounter sharedCounter = new BufferedCounter(sharedBatches::add, shared);

      sharedCounter.count("key1", 1);
      sharedCounter.destroy();

      assertThat(shared.isShutdown()).isFalse();
      assertThat(sharedBatches).hasSize(1);
      assertThat(sharedBatches.get(0)).containsEntry("key1", 1L);
    } finally {
      shared.shutdown();
    }
  }

  @Test
  void destroy_withConcurrentCount_shouldNotDeadlock() throws Exception {
    ExecutorService exec = Executors.newFixedThreadPool(2);
    AtomicBoolean stopped = new AtomicBoolean(false);
    exec.submit(() -> {
      while (!stopped.get()) {
        try {
          counter.count("key", 1);
        } catch (Exception e) {
          // ignore
        }
      }
    });
    Thread.sleep(50);
    exec.submit(() -> {
      counter.destroy();
      stopped.set(true);
    });
    exec.shutdown();
    assertThat(exec.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
  }
}
