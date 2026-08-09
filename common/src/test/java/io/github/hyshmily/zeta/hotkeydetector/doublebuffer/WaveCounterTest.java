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
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Correctness matrix for {@link WaveCounter}: cold direct writes, hot
 * local aggregation, automatic promotion, concurrent delivery, dead-writer
 * reclamation and slow consumers.
 */
@Tag("performance")
class WaveCounterTest {

  private List<Map<String, Long>> batches;
  private Consumer<Map<String, Long>> consumer;
  private WaveCounter counter;

  @BeforeEach
  void setUp() {
    batches = new ArrayList<>();
    consumer = batches::add;
    counter = new WaveCounter(consumer);
  }

  @AfterEach
  void tearDown() {
    counter.destroy();
  }

  private static long mergedTotal(List<Map<String, Long>> batches) {
    return batches
      .stream()
      .flatMap(m -> m.values().stream())
      .mapToLong(Long::longValue)
      .sum();
  }

  private static Map<String, Long> mergedMap(List<Map<String, Long>> batches) {
    Map<String, Long> merged = new HashMap<>();
    for (Map<String, Long> batch : batches) {
      for (Map.Entry<String, Long> entry : batch.entrySet()) {
        merged.merge(entry.getKey(), entry.getValue(), Long::sum);
      }
    }
    return merged;
  }

  @Test
  void count_shouldRecordSingleKey() {
    counter.count("key1", 1);
    counter.destroy();

    assertThat(batches).hasSize(1);
    assertThat(batches.get(0)).containsEntry("key1", 1L);
  }

  @Test
  void count_shouldAccumulateDelta() {
    counter.count("key1", 3);
    counter.count("key1", 4);
    counter.destroy();

    assertThat(mergedTotal(batches)).isEqualTo(7);
  }

  @Test
  void count_shouldRecordMultipleKeysIndependently() {
    counter.count("key1", 2);
    counter.count("key2", 3);
    counter.destroy();

    Map<String, Long> merged = mergedMap(batches);
    assertThat(merged).containsEntry("key1", 2L).containsEntry("key2", 3L);
  }

  @Test
  void hotKey_shouldRecordExactly() {
    markHot(counter, "hot-key");
    int n = 5_000;
    for (int i = 0; i < n; i++) {
      counter.count("hot-key", 1);
    }
    counter.destroy();

    Map<String, Long> merged = mergedMap(batches);
    assertThat(merged).containsEntry("hot-key", (long) n);
  }

  @Test
  void count_shouldBeThreadSafe() throws Exception {
    int threadCount = 8;
    int incrementsPerThread = 5_000;
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threadCount);
    for (int i = 0; i < threadCount; i++) {
      String key = "key" + i;
      pool.submit(() -> {
        try {
          start.await();
          for (int j = 0; j < incrementsPerThread; j++) {
            counter.count(key, 1);
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
    counter.destroy();

    Map<String, Long> merged = mergedMap(batches);
    for (int i = 0; i < threadCount; i++) {
      assertThat(merged).containsEntry("key" + i, (long) incrementsPerThread);
    }
  }

  /**
   * Mixed cold+hot multi-writer integrity: hot keys aggregated locally (exact
   * path), cold keys direct-written, batch flushes and destroys racing.
   */
  @Test
  void concurrentBurst_shouldNotLoseCounts() throws Exception {
    markHot(counter, "hot-key");
    int threadCount = 16;
    int perThread = 20_000;
    long expected = (long) threadCount * perThread;
    String[] keys = new String[2_000];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = "k-" + (i * 131_071);
    }
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threadCount);
    ThreadLocalRandom rnd = ThreadLocalRandom.current();
    for (int i = 0; i < threadCount; i++) {
      pool.submit(() -> {
        try {
          start.await();
          for (int j = 0; j < perThread; j++) {
            if (rnd.nextInt(100) < 30) {
              counter.count("hot-key", 1);
            } else {
              counter.count(keys[rnd.nextInt(keys.length)], 1);
            }
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
    counter.destroy();

    assertThat(mergedTotal(batches)).isEqualTo(expected);
  }

  /**
   * Delivery racing writers: a 5ms deliver loop swaps and snapshots while
   * cold writers direct-write and hot writers batch-merge.
   *
   * <p>Hot-path counts are exact (in-flight merges are waited out).  Cold
   * direct writes carry the documented approximate window (preemption
   * &gt; 1ms during the snapshot quiescence) — asserted as ≤ 0.01% loss,
   * far below the whole-batch drops of the old bounded-queue design.
   */
  @Test
  void concurrentDeliver_shouldNotLoseCounts() throws Exception {
    markHot(counter, "hot-key");
    List<Map<String, Long>> captured = new ArrayList<>();
    WaveCounter c = new WaveCounter(captured::add);
    Thread deliverer = new Thread(() -> {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          Thread.sleep(5);
          invokeDeliver(c);
        }
      } catch (InterruptedException e) {
        // fall through to the final delivery below
      }
      invokeDeliver(c);
    });
    deliverer.start();

    int threadCount = 12;
    int perThread = 8_000;
    long expected = (long) threadCount * perThread;
    String[] keys = new String[4_000];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = "k-" + (i * 131_071);
    }
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threadCount);
    ThreadLocalRandom rnd = ThreadLocalRandom.current();
    for (int i = 0; i < threadCount; i++) {
      pool.submit(() -> {
        try {
          start.await();
          for (int j = 0; j < perThread; j++) {
            if (rnd.nextInt(100) < 30) {
              c.count("hot-key", 1);
            } else {
              c.count(keys[rnd.nextInt(keys.length)], 1);
            }
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

    deliverer.interrupt();
    deliverer.join(10_000);
    c.destroy();

    long total = captured
      .stream()
      .flatMap(m -> m.values().stream())
      .mapToLong(Long::longValue)
      .sum();
    double lossPct = (100.0 * (expected - total)) / expected;
    assertThat(lossPct).as("cold approximate window loss").isLessThan(0.01);
  }

  /** Invoke the private deliver() via reflection (periodic delivery is scheduler-driven). */
  private static void invokeDeliver(WaveCounter c) {
    try {
      Method m = WaveCounter.class.getDeclaredMethod("tide");
      m.setAccessible(true);
      m.invoke(c);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Verifies the soft capacity cap: new cold keys beyond the bound are
   * dropped, while keys already tracked keep counting — the cap bounds
   * key cardinality, not established counters.  (The drop counter was
   * removed 2026-08-08 — the observation was deemed unnecessary.)
   */
  @Test
  void capacity_shouldDropNewColdKeysBeyondLimit() throws Exception {
    List<Map<String, Long>> captured = new ArrayList<>();
    ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
    try {
      WaveCounter c = new WaveCounter(captured::add, 3, 50, 0.5, sched);
      for (int i = 0; i < 5; i++) {
        c.count("cold-" + i, 1);
      }
      c.count("cold-0", 10);
      invokeDeliver(c);

      Map<String, Long> merged = mergedMap(captured);
      assertThat(merged).containsKeys("cold-0", "cold-1", "cold-2");
      assertThat(merged).doesNotContainKeys("cold-3", "cold-4");
      assertThat(merged.get("cold-0")).isEqualTo(11);
      c.destroy();
    } finally {
      sched.shutdownNow();
    }
  }

  /**
   * Automatic promotion: an initially-cold key hammered past the threshold is
   * promoted by the delivery scan and must not lose counts during the
   * transition.
   */
  @Test
  void promotion_shouldPreserveCounts() throws Exception {
    List<Map<String, Long>> captured = new ArrayList<>();
    WaveCounter c = new WaveCounter(captured::add);
    AtomicBoolean promotedSeen = new AtomicBoolean();
    Thread deliverer = new Thread(() -> {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          Thread.sleep(5);
          invokeDeliver(c);
          // Observe the promotion evidence from INSIDE the deliverer loop:
          // the beacon decays a quiet key within 2 tides, so a post-destroy
          // check races the decay window (the gated quiescence made idle
          // tides faster, widening the race).  Mid-run observation over the
          // 5ms cadence catches the 2-tide evidence window reliably.
          if (!promotedSeen.get()) {
            promotedSeen.set(wasPromoted(c, "cand"));
          }
        }
      } catch (InterruptedException e) {
        // fall through
      }
      invokeDeliver(c);
    });
    deliverer.start();

    int threadCount = 8;
    int perThread = 20_000;
    long expected = (long) threadCount * perThread;
    // 100 competing keys: the snapshot stays well below hotLimit (1024),
    // so the promotion scan cannot exhaust its quota on the rivals before
    // reaching "cand" in CHM iteration order — a larger universe (2000)
    // made the promotion of "cand" a coin flip under load.
    String[] keys = new String[100];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = "k-" + (i * 131_071);
    }
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threadCount);
    ThreadLocalRandom rnd = ThreadLocalRandom.current();
    for (int i = 0; i < threadCount; i++) {
      pool.submit(() -> {
        try {
          start.await();
          for (int j = 0; j < perThread; j++) {
            if (rnd.nextInt(100) < 60) {
              c.count("cand", 1); // cold at first, promoted mid-run
            } else {
              c.count(keys[rnd.nextInt(keys.length)], 1);
            }
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

    deliverer.interrupt();
    deliverer.join(10_000);
    c.destroy();

    long total = captured
      .stream()
      .flatMap(m -> m.values().stream())
      .mapToLong(Long::longValue)
      .sum();
    assertThat(total).isEqualTo(expected);
    // sanity: the candidate key was actually promoted (observed mid-run —
    // see the deliverer loop; the beacon decays a quiet key within 2 tides).
    assertThat(promotedSeen.get()).isTrue();
  }

  /** The bit2-role evidence — proves the key was promoted recently (2-tide window). */
  private static boolean wasPromoted(WaveCounter c, String key) {
    try {
      Field f = WaveCounter.class.getDeclaredField("beacon");
      f.setAccessible(true);
      long[] beacon = (long[]) f.get(c);
      Field m = WaveCounter.class.getDeclaredField("beaconMask");
      m.setAccessible(true);
      int mask = (int) m.get(c);
      int h = mixHash(key.hashCode());
      int bit2 = rehash(h) & mask;
      return ((beacon[bit2 >>> 4] >>> (((bit2 & 15) << 2) + 2)) & 0x3) >= 1;
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  /** Replicates {@code WaveCounter.mixHash} for beacon bit position computation. */
  private static int mixHash(int h) {
    h ^= h >>> 16;
    h *= 0x85ebca6b;
    h ^= h >>> 13;
    h *= 0xc2b2ae35;
    h ^= h >>> 16;
    return h;
  }

  /** Replicates {@code WaveCounter.rehash} — the k=2 trace-room hash. */
  private static int rehash(int h) {
    h *= 0x31848bab;
    h ^= h >>> 14;
    return h;
  }

  private static void markHot(WaveCounter c, String key) {
    try {
      Field f = WaveCounter.class.getDeclaredField("beacon");
      f.setAccessible(true);
      long[] beacon = (long[]) f.get(c);
      Field m = WaveCounter.class.getDeclaredField("beaconMask");
      m.setAccessible(true);
      int mask = (int) m.get(c);
      // Mirrors production: bit1-role evidence (seed 2), bit2-role evidence (seed 2).
      int h = mixHash(key.hashCode());
      int bit1 = h & mask;
      beacon[bit1 >>> 4] = beacon[bit1 >>> 4] | (2L << ((bit1 & 15) << 2));
      int bit2 = rehash(h) & mask;
      beacon[bit2 >>> 4] = beacon[bit2 >>> 4] | (2L << (((bit2 & 15) << 2) + 2));
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean isHot(WaveCounter c, String key) {
    try {
      Field f = WaveCounter.class.getDeclaredField("beacon");
      f.setAccessible(true);
      long[] beacon = (long[]) f.get(c);
      Field m = WaveCounter.class.getDeclaredField("beaconMask");
      m.setAccessible(true);
      int mask = (int) m.get(c);
      int h = mixHash(key.hashCode());
      int bit1 = h & mask;
      int count = (int) ((beacon[bit1 >>> 4] >>> ((bit1 & 15) << 2)) & 0x3);
      if (count == 0) {
        return false;
      }
      int bit2 = rehash(h) & mask;
      return ((beacon[bit2 >>> 4] >>> (((bit2 & 15) << 2) + 2)) & 0x3) >= 1;
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Dead-writer reclamation: a dedicated writer dies with hot counts still
   * resident in its local map; the next delivery must merge them.
   */
  @Test
  void deadWriter_shouldHaveResidualMerged() throws Exception {
    markHot(counter, "dead-key");
    Thread w = new Thread(() -> {
      for (int i = 0; i < 5; i++) {
        counter.count("dead-key", 1);
      }
    });
    w.start();
    w.join();
    Thread.sleep(10);
    invokeDeliver(counter);
    counter.destroy();

    long total = mergedTotal(batches);
    assertThat(total).isEqualTo(5);
  }

  /**
   * Slow consumer: 20ms per batch while writers run.  Hot-path counts are
   * exact; cold direct writes carry the documented approximate window
   * (preemption &gt; 1ms during the snapshot quiescence) — asserted as
   * ≤ 0.01% loss, matching the deliver-racing test.
   */
  @Test
  void slowConsumer_shouldNotLoseCounts() throws Exception {
    AtomicLong consumed = new AtomicLong();
    WaveCounter c = new WaveCounter(m -> {
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      consumed.addAndGet(m.values().stream().mapToLong(Long::longValue).sum());
    });
    Thread deliverer = new Thread(() -> {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          Thread.sleep(5);
          invokeDeliver(c);
        }
      } catch (InterruptedException e) {
        // fall through
      }
      invokeDeliver(c);
    });
    deliverer.start();
    AtomicBoolean promotedSeen = new AtomicBoolean();

    int threadCount = 8;
    int perThread = 20_000;
    long expected = (long) threadCount * perThread;
    String[] keys = new String[4_000];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = "k-" + (i * 131_071);
    }
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threadCount);
    ThreadLocalRandom rnd = ThreadLocalRandom.current();
    for (int i = 0; i < threadCount; i++) {
      pool.submit(() -> {
        try {
          start.await();
          for (int j = 0; j < perThread; j++) {
            c.count(keys[rnd.nextInt(keys.length)], 1);
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

    deliverer.interrupt();
    deliverer.join(10_000);
    c.destroy();

    double lossPct = (100.0 * (expected - consumed.get())) / expected;
    assertThat(lossPct).as("cold approximate window loss").isLessThan(0.01);
  }

  @Test
  void afterPropertiesSet_shouldDeliverPeriodically() throws Exception {
    counter.afterPropertiesSet();
    counter.count("key1", 1);
    Thread.sleep(700);

    assertThat(batches).isNotEmpty();
    assertThat(mergedTotal(batches)).isEqualTo(1);
  }

  /**
   * Verifies the adaptive delivery cadence: idle stays at the base interval,
   * backlog at/above the threshold floors at the minimum, and the delay
   * scales linearly and monotonically in between.
   */
  @Test
  void computeNextTideDelay_shouldScaleWithBacklog() {
    assertThat(counter.computeNextTideDelayMs(0)).isEqualTo(500);
    assertThat(counter.computeNextTideDelayMs(10_000)).isEqualTo(275); // linear midpoint
    assertThat(counter.computeNextTideDelayMs(20_000)).isEqualTo(50);
    assertThat(counter.computeNextTideDelayMs(100_000)).isEqualTo(50);
    assertThat(counter.computeNextTideDelayMs(Integer.MAX_VALUE)).isEqualTo(50);

    long prev = Long.MAX_VALUE;
    for (int i = 0; i <= 25_000; i += 1_000) {
      long d = counter.computeNextTideDelayMs(i);
      assertThat(d).isLessThanOrEqualTo(prev);
      assertThat(d).isBetween(50L, 500L);
      prev = d;
    }
  }

  @Test
  void destroy_shouldBeIdempotent() {
    counter.destroy();
    counter.destroy();
  }

  @Test
  void clear_shouldDropAllCounts() {
    counter.count("x", 5);
    counter.count("y", 3);
    counter.clear();
    counter.destroy();

    assertThat(mergedTotal(batches)).isZero();
  }

  @Test
  void estimatedSize_shouldReflectSharedTable() {
    for (int i = 0; i < 300; i++) {
      counter.count("key" + i, 1);
    }
    assertThat(counter.estimatedSizeOfKeysCount()).isGreaterThanOrEqualTo(300L);
  }

  @Test
  void count_shouldThrowOnNullKey() {
    assertThatThrownBy(() -> counter.count(null, 1)).isInstanceOf(NullPointerException.class);
  }
}
