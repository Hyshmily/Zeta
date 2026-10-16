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
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Regression guards for the drain/writer race and the writer-map lifecycle.
 *
 * <ul>
 * <li>The sweep's slot teardown order: a writer may claim a slot the sweep
 * is still tearing down, and the sweep must never publish the slot as empty
 * (tag 0) before its key and claim bit are gone — otherwise the next merge
 * passes a null key into {@code mergeKey} and NPEs inside
 * {@code ConcurrentHashMap}.</li>
 * <li>{@code Ceils} must not reference its owner: it is the value of a
 * {@code ThreadLocal} that is deliberately never removed, so an outer-class
 * reference would pin a destroyed counter for the lifetime of every writer
 * thread.</li>
 * <li>{@code destroy()} must release the writer registry and the map
 * pool — every residual has been merged by then.</li>
 * </ul>
 */
class WaveCounterDrainRaceTest {

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

  /**
   * The writer-local map must be a STATIC nested class: as the value of a
   * {@code ThreadLocal} that is never removed, an outer-class reference
   * would form {@code Thread -> entry.value -> Ceils -> WaveCounter ->
   * hotLocals}, keeping the {@code ThreadLocal} key strongly reachable and
   * pinning a destroyed counter (beacon, scan arrays, registry, pool) for
   * the lifetime of every writer thread.
   */
  @Test
  void ceils_shouldNotReferenceItsOwner() throws Exception {
    markHot(counter, "hot");
    counter.count("hot", 1);

    Object ceils = registry(counter).get(Thread.currentThread());
    assertThat(ceils).as("writer must be registered").isNotNull();

    Class<?> ceilsClass = ceils.getClass();
    assertThat(Modifier.isStatic(ceilsClass.getModifiers())).as("Ceils must be a static nested class").isTrue();
    for (Field f : ceilsClass.getDeclaredFields()) {
      assertThat(f.getName()).as("no synthetic outer reference").isNotEqualTo("this$0");
    }
    for (Field f : ceilsClass.getDeclaredFields()) {
      assertThat(f.getType().getName())
        .as("owner must not be stored as a field")
        .isNotEqualTo(WaveCounter.class.getName());
    }
  }

  /**
   * {@code destroy()} merges every residual and then drops the registry and
   * the pool: a destroyed counter that is still referenced elsewhere must not
   * keep dead {@code Thread} keys or up to {@code CEIL_POOL_CAP} ~7KB maps
   * alive.
   */
  @Test
  void destroy_shouldReleaseRegistryAndPool() throws Exception {
    markHot(counter, "hot");
    counter.count("hot", 1);
    assertThat(registry(counter)).as("precondition: writer registered").isNotEmpty();

    counter.destroy();

    assertThat(registry(counter)).as("registry released").isEmpty();
    assertThat(pool(counter)).as("ceil pool released").isEmpty();
  }

  /**
   * Writers cycling over several hot keys (full-path {@code add}, not the
   * last-key fast hit) while a concurrent drainer empties their maps: no
   * throwable may escape on either side and the hot path must stay EXACT
   * (every count is merged, none twice).
   *
   * <p>
   * The drainer calls {@code discharge} reflectively instead of
   * {@code tide()} on purpose: {@code tide()} catches {@code Throwable} and
   * would swallow the very NPE this guards against.
   */
  @Test
  void concurrentDrainAndAdd_shouldNeitherThrowNorLoseCounts() throws Exception {
    int writers = 4;
    int keysPerWriter = 8;
    int loops = 40_000;

    for (int w = 0; w < writers; w++) {
      for (int k = 0; k < keysPerWriter; k++) {
        markHot(counter, key(w, k));
      }
    }

    AtomicReference<Throwable> failure = new AtomicReference<>();
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(writers + 1);

    List<Thread> threads = new ArrayList<>();
    for (int w = 0; w < writers; w++) {
      final int id = w;
      Thread t = new Thread(
        () -> {
          try {
            start.await();
            for (int i = 0; i < loops; i++) {
              counter.count(key(id, i % keysPerWriter), 1);
            }
          } catch (Throwable e) {
            failure.compareAndSet(null, e);
          } finally {
            done.countDown();
          }
        },
        "drain-race-writer-" + w
      );
      threads.add(t);
    }

    Thread drainer = new Thread(
      () -> {
        try {
          start.await();
          Method discharge = null;
          while (done.getCount() > 1) {
            for (Object ceils : registry(counter).values()) {
              if (discharge == null || discharge.getParameterTypes()[0] != ceils.getClass()) {
                discharge = WaveCounter.class.getDeclaredMethod("discharge", ceils.getClass());
                discharge.setAccessible(true);
              }
              discharge.invoke(counter, ceils);
            }
          }
        } catch (Throwable e) {
          failure.compareAndSet(null, e);
        } finally {
          done.countDown();
        }
      },
      "drain-race-drainer"
    );

    for (Thread t : threads) {
      t.start();
    }
    drainer.start();
    start.countDown();
    assertThat(done.await(30, TimeUnit.SECONDS)).as("writers and drainer finished").isTrue();
    drainer.join(5_000);

    assertThat(failure.get()).as("no throwable on the writer or drain path (a null-key merge NPEs)").isNull();

    counter.destroy();
    counter.destroy(); // idempotent; no-op after the first

    // Each writer performs `loops` counts (cycling over its keysPerWriter
    // keys), so the exact total is writers * loops.
    long expected = (long) writers * loops;
    long total = 0;
    Map<String, Long> merged = new HashMap<>();
    for (Map<String, Long> batch : batches) {
      for (Map.Entry<String, Long> e : batch.entrySet()) {
        merged.merge(e.getKey(), e.getValue(), Long::sum);
      }
    }
    for (long v : merged.values()) {
      total += v;
    }
    assertThat(total).as("hot path is exact — every count merged exactly once").isEqualTo(expected);
  }

  private static String key(int writer, int slot) {
    return "k-" + writer + "-" + slot;
  }

  /** Write both beacon rooms (mirrors production's promotion). */
  private static void markHot(WaveCounter c, String key) {
    try {
      Field f = WaveCounter.class.getDeclaredField("beacon");
      f.setAccessible(true);
      long[] beacon = (long[]) f.get(c);
      Field m = WaveCounter.class.getDeclaredField("beaconMask");
      m.setAccessible(true);
      int mask = (int) m.get(c);
      Method mix = WaveCounter.class.getDeclaredMethod("mixHash", int.class);
      mix.setAccessible(true);
      Method rehash = WaveCounter.class.getDeclaredMethod("rehash", int.class);
      rehash.setAccessible(true);
      int h = (int) mix.invoke(null, key.hashCode());
      int bit1 = h & mask;
      beacon[bit1 >>> 4] = beacon[bit1 >>> 4] | (2L << ((bit1 & 15) << 2));
      int bit2 = (int) rehash.invoke(null, h) & mask;
      beacon[bit2 >>> 4] = beacon[bit2 >>> 4] | (2L << (((bit2 & 15) << 2) + 2));
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private static ConcurrentHashMap<Thread, Object> registry(WaveCounter c) {
    try {
      Field f = WaveCounter.class.getDeclaredField("hotRegistry");
      f.setAccessible(true);
      return (ConcurrentHashMap<Thread, Object>) f.get(c);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private static ConcurrentLinkedQueue<Object> pool(WaveCounter c) {
    try {
      Field f = WaveCounter.class.getDeclaredField("ceilPool");
      f.setAccessible(true);
      return (ConcurrentLinkedQueue<Object>) f.get(c);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}
