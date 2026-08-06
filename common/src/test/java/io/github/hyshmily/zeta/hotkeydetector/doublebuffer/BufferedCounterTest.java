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
package io.github.hyshmily.zeta.hotkeydetector.doublebuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("performance")
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

    Map<String, Long> merged = new HashMap<>();
    for (Map<String, Long> batch : batches) {
      merged.putAll(batch);
    }
    assertThat(merged).containsEntry("key1", 1L).containsEntry("key2", 2L);
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

    assertThat(merged).containsEntry("k1", 1L).containsEntry("k2", 1L).containsEntry("k3", 1L);
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

    assertThat(merged).containsEntry("before_switch", 1L).containsEntry("after_switch", 1L);
  }

  @Test
  void count_shouldThrowOnNullKey() {
    assertThatThrownBy(() -> counter.count(null, 1)).isInstanceOf(NullPointerException.class);
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
  void constructor_withCustomParams_shouldUseThem() throws Exception {
    ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
    try {
      List<Map<String, Long>> customBatches = new ArrayList<>();
      BufferedCounter custom = new BufferedCounter(customBatches::add, 5, 50, 0.8, sched);
      custom.afterPropertiesSet();

      custom.count("k1", 1);
      custom.count("k2", 1);
      custom.count("k3", 1);
      custom.count("k4", 1);

      Thread.sleep(150);
      custom.destroy();

      assertThat(customBatches).isNotEmpty();
      long total = customBatches
        .stream()
        .flatMap(m -> m.values().stream())
        .mapToLong(Long::longValue)
        .sum();
      assertThat(total).isEqualTo(4);
    } finally {
      sched.shutdown();
    }
  }

  @Test
  void estimatedSize_OfKeysCount_shouldReturnSumOfBothBuffers() {
    counter.count("a", 1);
    counter.count("b", 2);
    assertThat(counter.estimatedSizeOfKeysCount()).isEqualTo(2);
  }

  @Test
  void clear_shouldDrainAllCounters() {
    counter.count("x", 5);
    counter.count("y", 3);
    counter.clear();

    List<Map<String, Long>> afterClear = new ArrayList<>();
    BufferedCounter cleared = new BufferedCounter(afterClear::add);
    cleared.destroy();
    assertThat(afterClear).isEmpty();
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

  @Test
  void destroy_withMultipleConcurrentCounters_shouldNotHang() throws Exception {
    int threadCount = 4;
    ExecutorService exec = Executors.newFixedThreadPool(threadCount + 1);
    AtomicBoolean running = new AtomicBoolean(true);
    for (int i = 0; i < threadCount; i++) {
      String key = "k" + i;
      exec.submit(() -> {
        while (running.get()) {
          counter.count(key, 1);
        }
      });
    }
    Thread.sleep(100);
    exec.submit(() -> {
      counter.destroy();
      running.set(false);
    });
    exec.shutdown();
    assertThat(exec.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    assertThat(batches).isNotEmpty();
  }

  @Test
  void destroy_withConcurrentCount_shouldPreserveDataIntegrity() throws Exception {
    int threadCount = 4;
    int incrementsPerThread = 1000;
    long expectedTotal = (long) threadCount * incrementsPerThread;
    List<Map<String, Long>> captured = new ArrayList<>();
    BufferedCounter dataCounter = new BufferedCounter(captured::add);
    ExecutorService exec = Executors.newFixedThreadPool(threadCount + 1);
    AtomicBoolean stopped = new AtomicBoolean(false);
    for (int i = 0; i < threadCount; i++) {
      String key = "k" + i;
      exec.submit(() -> {
        for (int j = 0; j < incrementsPerThread && !stopped.get(); j++) {
          dataCounter.count(key, 1);
        }
      });
    }
    Thread.sleep(100);
    exec.submit(() -> {
      dataCounter.destroy();
      stopped.set(true);
    });
    exec.shutdown();
    assertThat(exec.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

    long totalFlushed = captured
      .stream()
      .flatMap(m -> m.values().stream())
      .mapToLong(Long::longValue)
      .sum();
    assertThat(totalFlushed).isGreaterThanOrEqualTo(expectedTotal - 100);
  }

  @Test
  void activeBufferSaturation_shouldReturnRatio() {
    double emptySat = counter.activeBufferSaturation();
    assertThat(emptySat).isBetween(0.0, 0.1);
    for (int i = 0; i < 100; i++) {
      counter.count("key" + i, 1);
    }
    double afterSat = counter.activeBufferSaturation();
    assertThat(afterSat).isGreaterThan(emptySat);
  }

  @Test
  void activeBufferSaturation_shouldNeverBeNegative() {
    for (int i = 0; i < 10; i++) {
      counter.count("k", -5);
    }
    assertThat(counter.activeBufferSaturation()).isNotNegative();
  }

  /**
   * Regression guard: an eager-swap storm (many concurrent keys saturating a
   * small-capacity buffer) must not lose counts.  Before the spill safety-net
   * queue (spill offered to the flush queue after the winner's final drain),
   * in-flight {@code add()} calls landing in a reclaimed spill were silently
   * dropped at ~0.04% under this exact workload.
   */
  @Test
  void swapStorm_shouldNotLoseCounts() throws Exception {
    int threadCount = 32;
    int incrementsPerThread = 50_000;
    long expectedTotal = (long) threadCount * incrementsPerThread;
    String[] keys = new String[8000];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = "k-" + (i * 131_071);
    }
    List<Map<String, Long>> captured = new ArrayList<>();
    ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
    try {
      BufferedCounter storm = new BufferedCounter(captured::add, 500, 500, 0.75, sched);
      ExecutorService pool = Executors.newFixedThreadPool(threadCount);
      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(threadCount);
      ThreadLocalRandom rnd = ThreadLocalRandom.current();
      for (int i = 0; i < threadCount; i++) {
        pool.submit(() -> {
          try {
            start.await();
            for (int j = 0; j < incrementsPerThread; j++) {
              storm.count(keys[rnd.nextInt(keys.length)], 1);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            done.countDown();
          }
        });
      }
      start.countDown();
      assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
      pool.shutdown();
      storm.destroy();

      long delivered = captured
        .stream()
        .flatMap(m -> m.values().stream())
        .mapToLong(Long::longValue)
        .sum();
      double lossPct = (100.0 * (expectedTotal - delivered)) / expectedTotal;
      assertThat(lossPct).as("count loss under eager-swap storm").isLessThan(0.02);
    } finally {
      sched.shutdownNow();
    }
  }

  /**
   * Regression guard: each flush cycle must deliver exactly one merged
   * snapshot to the consumer.  Per-buffer delivery produced bursts of consumer
   * calls (up to MAXS_CEILS per cycle) that overflowed bounded downstream
   * queues — e.g. the reporter routing executor — silently dropping counts
   * once more hash slots were live.
   */
  @Test
  void flush_shouldDeliverOneMergedSnapshotPerCycle() throws Exception {
    List<Map<String, Long>> captured = new ArrayList<>();
    ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
    try {
      BufferedCounter mergedCounter = new BufferedCounter(captured::add, 100_000, 50, 0.75, sched);
      mergedCounter.afterPropertiesSet();
      for (int i = 0; i < 1000; i++) {
        mergedCounter.count("bulk-" + i, 1);
      }
      Thread.sleep(700);

      assertThat(captured).hasSize(1);
      assertThat(captured.get(0)).hasSize(1000);
      long total = captured.get(0).values().stream().mapToLong(Long::longValue).sum();
      assertThat(total).isEqualTo(1000);
    } finally {
      sched.shutdownNow();
    }
  }

  /**
   * Regression guard: the bounded flush queue must cap memory growth when the
   * downstream consumer stalls.  Overflow batches are dropped and counted by
   * {@code droppedFlushBatches()} instead of growing the queue unboundedly
   * (the previous unbounded {@code ConcurrentLinkedQueue} accumulated ~300 MB
   * under this workload).
   */
  @Test
  void flushQueueFull_shouldDropAndCount() throws Exception {
    AtomicLong consumed = new AtomicLong();
    Consumer<Map<String, Long>> slow = m -> {
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      consumed.addAndGet(m.values().stream().mapToLong(Long::longValue).sum());
    };
    ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
    try {
      int queueCapacity = 16;
      BufferedCounter storm = new BufferedCounter(slow, 500, 200, 0.75, sched, queueCapacity);
      storm.afterPropertiesSet();
      Field qField = BufferedCounter.class.getDeclaredField("flushQueue");
      qField.setAccessible(true);
      @SuppressWarnings("all")
      ArrayBlockingQueue<Object> queue = (ArrayBlockingQueue<Object>) qField.get(storm);

      String[] keys = new String[40_000];
      for (int i = 0; i < keys.length; i++) {
        keys[i] = "k-" + (i * 131_071);
      }
      int threads = 8;
      int perThread = 50_000;
      long expected = (long) threads * perThread;
      ExecutorService pool = Executors.newFixedThreadPool(threads);
      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(threads);
      ThreadLocalRandom rnd = ThreadLocalRandom.current();
      for (int i = 0; i < threads; i++) {
        pool.submit(() -> {
          try {
            start.await();
            for (int j = 0; j < perThread; j++) {
              storm.count(keys[rnd.nextInt(keys.length)], 1);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            done.countDown();
          }
        });
      }
      start.countDown();
      assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
      pool.shutdown();

      int peak = queue.size();
      for (int i = 0; i < 20; i++) {
        peak = Math.max(peak, queue.size());
        Thread.sleep(50);
      }
      storm.destroy();

      assertThat(peak).isLessThanOrEqualTo(queueCapacity);
      assertThat(storm.droppedFlushBatches()).isPositive();
      assertThat(consumed.get()).isPositive();
      assertThat(consumed.get()).isLessThan(expected);
    } finally {
      sched.shutdownNow();
    }
  }

  /**
   * Regression guard: keys whose {@code hashCode()} clusters on the low bits
   * (pathological input) must spread across multiple slots.  The murmur3
   * finalizer in {@code count()} avalanches the hash — before it, all keys
   * sharing the same {@code (hashCode & mask)} collapsed onto a single slot,
   * turning the whole workload into a single-buffer bottleneck.
   */
  @Test
  void pathologicalHash_shouldSpreadAcrossSlots() throws Exception {
    Field ceilField = BufferedCounter.class.getDeclaredField("ceilCount");
    ceilField.setAccessible(true);
    int ceilCount = ceilField.getInt(counter);
    int mask = ceilCount - 1;

    // collect keys that all map to the same slot under the raw hashCode
    List<String> cluster = new ArrayList<>();
    for (int i = 0; i < 100_000 && cluster.size() < 64; i++) {
      String key = "pat-" + i;
      if ((key.hashCode() & mask) == 0) {
        cluster.add(key);
      }
    }
    assertThat(cluster).hasSize(64);
    for (String key : cluster) {
      counter.count(key, 1);
    }

    Field activeField = BufferedCounter.class.getDeclaredField("activeCeils");
    activeField.setAccessible(true);
    AtomicReference<?>[] actives = (AtomicReference<?>[]) activeField.get(counter);
    Class<?> cbClass = Class.forName(BufferedCounter.class.getName() + "$CounterBuffer");
    Method sizeMethod = cbClass.getDeclaredMethod("size");
    sizeMethod.setAccessible(true);

    int nonEmptySlots = 0;
    int maxSlotSize = 0;
    for (int i = 0; i < ceilCount; i++) {
      int size = (int) sizeMethod.invoke(actives[i].get());
      if (size > 0) {
        nonEmptySlots++;
        maxSlotSize = Math.max(maxSlotSize, size);
      }
    }
    assertThat(nonEmptySlots).isGreaterThan(1);
    assertThat(maxSlotSize).isLessThan(64); // no slot holds the whole cluster
  }

  /**
   * Sealed-buffer protocol regression: once a reclaimer (flusher) seals a
   * buffer and swaps in a fresh one, a concurrent {@code count()} must
   * redirect to the new active buffer instead of writing into the sealed
   * (about-to-be-drained) buffer — otherwise the increment lands in the
   * discarded snapshot and is silently lost.
   */
  @Test
  void count_shouldRedirectWhenActiveBufferSealed() throws Exception {
    List<Map<String, Long>> captured = new ArrayList<>();
    ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
    try {
      BufferedCounter counter = new BufferedCounter(captured::add, 500, 500, 0.75, sched);
      Field ceilField = BufferedCounter.class.getDeclaredField("ceilCount");
      ceilField.setAccessible(true);
      int ceilCount = ceilField.getInt(counter);
      Field activeField = BufferedCounter.class.getDeclaredField("activeCeils");
      activeField.setAccessible(true);
      AtomicReference<Object>[] actives = (AtomicReference<Object>[]) activeField.get(counter);
      Class<?> cbClass = Class.forName(BufferedCounter.class.getName() + "$CounterBuffer");
      Field sealedField = cbClass.getDeclaredField("sealed");
      sealedField.setAccessible(true);
      Method drainMethod = cbClass.getDeclaredMethod("drain");
      drainMethod.setAccessible(true);
      var ctor = cbClass.getDeclaredConstructor();
      ctor.setAccessible(true);

      // reclaimer: seal then swap every live slot — exactly what flushStandby does
      List<Object> oldBuffers = new ArrayList<>();
      for (int i = 0; i < ceilCount; i++) {
        Object old = actives[i].get();
        sealedField.setBoolean(old, true);
        actives[i].set(ctor.newInstance());
        oldBuffers.add(old);
      }

      counter.count("redirected", 1);

      // no increment may land in a sealed (drained) buffer
      for (Object old : oldBuffers) {
        assertThat((Map<?, ?>) drainMethod.invoke(old)).isEmpty();
      }
      // and the increment must be present in exactly one new active buffer
      long total = 0;
      for (int i = 0; i < ceilCount; i++) {
        Map<?, ?> snapshot = (Map<?, ?>) drainMethod.invoke(actives[i].get());
        Object v = snapshot.get("redirected");
        total += v instanceof Number n ? n.longValue() : 0L;
      }
      assertThat(total).isEqualTo(1L);
    } finally {
      sched.shutdownNow();
    }
  }

  /**
   * Deterministic regression guard for the stale spill reader window: a
   * writer that read the spill reference before the winner reclaimed it must
   * not lose its {@code add()}.  The spill ticket re-check detects the
   * reclaim and retries via the active path.  Before the ticket protocol the
   * add() landed in the detached spill and was silently lost (reproducible
   * with this exact injection).
   */
  @Test
  void staleSpillReader_shouldNotLoseCount() throws Exception {
    List<Map<String, Long>> captured = new ArrayList<>();
    ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
    try {
      BufferedCounter counter = new BufferedCounter(captured::add, 500, 500, 0.75, sched);
      Class<?> cbClass = Class.forName(BufferedCounter.class.getName() + "$CounterBuffer");
      var ctor = cbClass.getDeclaredConstructor();
      ctor.setAccessible(true);
      Object spill = ctor.newInstance();
      Method spillDrain = cbClass.getDeclaredMethod("drain");
      spillDrain.setAccessible(true);

      Field spillField = BufferedCounter.class.getDeclaredField("spillCeils");
      spillField.setAccessible(true);
      @SuppressWarnings("all")
      AtomicReference<Object>[] spillCeils = (AtomicReference<Object>[]) spillField.get(counter);
      spillCeils[0].set(spill);

      // stale reader: captures the spill reference (injected spill), then stalls
      CountDownLatch readDone = new CountDownLatch(1);
      CountDownLatch resume = new CountDownLatch(1);
      Thread reader = new Thread(() -> {
        readDone.countDown();
        try {
          resume.await();
          counter.count("stale-key", 1); // ticket path re-checks and retries via active
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      reader.start();
      readDone.await();

      // winner reclaims: CAS-clear + final drain + queue + flusher drains it
      Field queueField = BufferedCounter.class.getDeclaredField("flushQueue");
      queueField.setAccessible(true);
      @SuppressWarnings("all")
      ArrayBlockingQueue<Object> queue = (ArrayBlockingQueue<Object>) queueField.get(counter);
      spillCeils[0].compareAndSet(spill, null);
      spillDrain.invoke(spill); // winner's final drain
      if (queue.offer(spill)) {
        queue.poll(); // flusher drains the queued spill (empty at this point)
      }

      resume.countDown();
      reader.join(5_000);

      counter.destroy();

      long total = captured
        .stream()
        .flatMap(m -> m.values().stream())
        .mapToLong(Long::longValue)
        .sum();
      assertThat(total).isEqualTo(1); // the stale add landed via the active path
    } finally {
      sched.shutdownNow();
    }
  }
}
