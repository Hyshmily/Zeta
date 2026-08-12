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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Quiescence-gate matrix (2026-08-09; extended 2026-08-11): the
 * {@code coldWriteSeen} flag is set by every shared-table insert — cold
 * first-inserts AND hot-path merges (hot LOCAL accumulation alone never
 * marks; the merge into the shared table does, because its entries are
 * visible to cold hit-writers) — cleared at every tide/clear, and never
 * set by the capacity-drop branch; tides with no shared-table writes skip
 * the 1ms window without losing hot-path counts.
 */
@Tag("performance")
class WaveCounterQuiescenceGateTest {

  private static boolean readFlag(WaveCounter c) {
    try {
      Field f = WaveCounter.class.getDeclaredField("coldWriteSeen");
      f.setAccessible(true);
      return (boolean) f.get(c);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  /** Invoke the private tide() via reflection (periodic delivery is scheduler-driven). */
  private static void invokeDeliver(WaveCounter c) {
    try {
      Method m = WaveCounter.class.getDeclaredMethod("tide");
      m.setAccessible(true);
      m.invoke(c);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  private static void markHot(WaveCounter c, String key) {
    try {
      Field f = WaveCounter.class.getDeclaredField("beacon");
      f.setAccessible(true);
      long[] beacon = (long[]) f.get(c);
      Field m = WaveCounter.class.getDeclaredField("beaconMask");
      m.setAccessible(true);
      int mask = (int) m.get(c);
      int h = mixHash(key.hashCode());
      int bit1 = h & mask;
      beacon[bit1 >>> 4] = beacon[bit1 >>> 4] | (2L << ((bit1 & 15) << 2));
      int bit2 = rehash(h) & mask;
      beacon[bit2 >>> 4] = beacon[bit2 >>> 4] | (2L << (((bit2 & 15) << 2) + 2));
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  private static int mixHash(int h) {
    h ^= h >>> 16;
    h *= 0x85ebca6b;
    h ^= h >>> 13;
    h *= 0xc2b2ae35;
    h ^= h >>> 16;
    return h;
  }

  private static int rehash(int h) {
    h *= 0x31848bab;
    h ^= h >>> 14;
    return h;
  }

  @Test
  void coldMiss_shouldSetFlag() {
    WaveCounter c = new WaveCounter(m -> {});
    assertThat(readFlag(c)).isFalse();
    c.count("cold-0", 1);
    assertThat(readFlag(c)).isTrue();
    c.destroy();
  }

  @Test
  void hotLocalAccumulation_shouldNotSetFlag() {
    WaveCounter c = new WaveCounter(m -> {});
    markHot(c, "hot-key");
    c.count("hot-key", 1);
    c.count("hot-key", 1);
    // Local accumulation only — no merge into the shared table yet (the
    // batch trigger needs opMaxCount distinct keys or the flush clock),
    // so nothing is visible to cold hit-writers and the flag stays clear.
    assertThat(readFlag(c)).isFalse();
    c.destroy();
  }

  /**
   * A hot-path MERGE marks the flag: once hot data is drained into the
   * shared table its entries are visible to cold hit-writers, whose adds
   * need the window's bound.  The local map fills at {@code opMaxCount}
   * distinct keys (128), so the 128th add triggers the discharge.
   */
  @Test
  void hotMerge_shouldSetFlag() {
    WaveCounter c = new WaveCounter(m -> {});
    for (int i = 0; i < 128; i++) {
      markHot(c, "hot-" + i);
    }
    for (int i = 0; i < 128; i++) {
      c.count("hot-" + i, 1);
    }
    assertThat(readFlag(c)).isTrue();
    invokeDeliver(c);
    assertThat(readFlag(c)).isFalse();
    c.destroy();
  }

  @Test
  void capacityDrop_shouldLeaveFlagUntouched() {
    ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
    try {
      WaveCounter c = new WaveCounter(m -> {}, 1, 50, 0.5, sched);
      c.count("a", 1);
      c.clear();
      assertThat(readFlag(c)).isFalse();
      // Reach capacity again (insert sets the flag), then a dropped key
      // must not clear or re-set anything beyond the insert's own mark.
      c.count("a", 1);
      assertThat(readFlag(c)).isTrue();
      c.count("b", 1); // dropped at capacity
      assertThat(readFlag(c)).isTrue();
      c.destroy();
    } finally {
      sched.shutdownNow();
    }
  }

  @Test
  void tide_shouldCaptureAndClearFlag() {
    WaveCounter c = new WaveCounter(m -> {});
    c.count("cold-0", 1);
    assertThat(readFlag(c)).isTrue();
    invokeDeliver(c);
    assertThat(readFlag(c)).isFalse();
    c.destroy();
  }

  @Test
  void idleTides_shouldKeepFlagCleared() {
    WaveCounter c = new WaveCounter(m -> {});
    invokeDeliver(c);
    assertThat(readFlag(c)).isFalse();
    invokeDeliver(c);
    assertThat(readFlag(c)).isFalse();
    c.destroy();
  }

  @Test
  void clear_shouldResetFlag() {
    WaveCounter c = new WaveCounter(m -> {});
    c.count("cold-0", 1);
    assertThat(readFlag(c)).isTrue();
    c.clear();
    assertThat(readFlag(c)).isFalse();
    c.destroy();
  }

  /**
   * Hot-only racing stress: 8 writers hammer promoted keys while a
   * deliverer thread races tides at 5ms.  Hot discharges now mark the
   * flag (their entries are visible to cold hit-writers), so the tide
   * pays the window on merge cycles — but the hot path is exact
   * regardless of the gate: the mergesInFlight settle is paid
   * unconditionally, so the merged total must be exact, proving the
   * window never strands hot merges.
   */
  @Test
  void hotRacing_shouldBeExactUnderSkippedWindow() throws Exception {
    List<Map<String, Long>> captured = new ArrayList<>();
    WaveCounter c = new WaveCounter(captured::add);
    for (int i = 0; i < 32; i++) {
      markHot(c, "hot-" + i);
    }
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

    int threadCount = 8;
    int perThread = 20_000;
    long expected = (long) threadCount * perThread;
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threadCount);
    for (int t = 0; t < threadCount; t++) {
      final int off = t;
      pool.submit(() -> {
        try {
          start.await();
          for (int j = 0; j < perThread; j++) {
            c.count("hot-" + ((off + j) & 31), 1);
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
    deliverer.join(5000);
    c.destroy();

    long total = captured
      .stream()
      .flatMap(m -> m.values().stream())
      .mapToLong(Long::longValue)
      .sum();
    assertThat(total).isEqualTo(expected);
    assertThat(readFlag(c)).isFalse();
  }

  /**
   * Cold racing stress with idle gaps: a deliverer thread races tides at
   * 5ms while 8 writers count cold keys in two bursts separated by a quiet
   * phase (idle tides skip the window).  Loss must stay within the
   * documented approximate window (same 0.01% bound as the deliver-racing
   * stress in {@link WaveCounterTest}).
   */
  @Test
  void coldRacingWithIdleGaps_shouldStayWithinApproximateWindow() throws Exception {
    List<Map<String, Long>> captured = new ArrayList<>();
    WaveCounter c = new WaveCounter(captured::add);
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

    int threadCount = 8;
    int perThread = 20_000;
    long expected = 2L * threadCount * perThread;
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threadCount);
    for (int t = 0; t < threadCount; t++) {
      final int off = t;
      pool.submit(() -> {
        try {
          start.await();
          for (int j = 0; j < perThread; j++) {
            c.count("cold-" + ((off + j) & 2047), 1);
          }
          // quiet phase: idle tides skip the window
          Thread.sleep(20);
          for (int j = 0; j < perThread; j++) {
            c.count("cold-" + ((off + j) & 2047), 1);
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
    deliverer.join(5000);
    c.destroy();

    long total = captured
      .stream()
      .flatMap(m -> m.values().stream())
      .mapToLong(Long::longValue)
      .sum();
    double lossPct = (100.0 * (expected - total)) / expected;
    assertThat(lossPct).as("cold approximate window loss").isLessThan(0.01);
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
}
