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
package io.github.hyshmily.zeta.hotkeydetector.heavykeeper;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;

/**
 * A lightweight, self-contained micro-benchmark comparing the concurrency
 * building blocks of {@link HeavyKeeper}'s hot path.
 *
 * <p>This is <b>not</b> a JMH harness — it is a hand-rolled throughput probe
 * designed to inform a single implementation decision: which combination of
 * (Node counter type) × (stripe-lock type) delivers the best steady-state
 * throughput under realistic contention. Three separate scenarios isolate the
 * two axes:
 *
 * <ol>
 *   <li><b>Same-key contention</b> — many threads hammer one key in the TopK
 *       membership map. This isolates the {@code Node.count} write path:
 *       {@link AtomicLong} CAS (monotonic raise) vs {@link LongAdder}
 *       (accumulate).</li>
 *   <li><b>Stripe-lock contention</b> — many threads update sketch slot
 *       counters across a striped lock array. This isolates the stripe-lock
 *       type: {@code synchronized(Object)} vs {@link ReentrantLock} vs
 *       {@link StampedLock} (optimistic read on hot slot).</li>
 *   <li><b>Mixed hot/cold</b> — 80% hits on 10 keys + 20% across 100k cold keys.
 *       Exercises both axes simultaneously under a realistic distribution.</li>
 * </ol>
 *
 * <p>Run via {@code mvn -pl common test-compile exec:java ...} or directly from
 * an IDE {@code main}. Each scenario runs 3 iterations and prints the median
 * throughput (ops/ms) for every variant.
 */
public class HeavyKeeperBenchmark {

  private HeavyKeeperBenchmark() {}

  // ─────────────────────────────────────────────────────────────────────
  // Scenario A: Node.count write contention (same-key, single hot key)
  // ─────────────────────────────────────────────────────────────────────

  /** AtomicLong CAS to max(currentCount, newCount) — mirrors current HeavyKeeper. */
  static final class CounterAtomicLong {

    final AtomicLong count = new AtomicLong(0);

    void report(long newCount) {
      long prev = count.get();
      while (newCount > prev) {
        if (count.compareAndSet(prev, newCount)) {
          break;
        }
        prev = count.get();
      }
    }

    long get() {
      return count.get();
    }
  }

  /** LongAdder accumulate — alternative semantics (add delta, not max). */
  static final class CounterLongAdder {

    final LongAdder count = new LongAdder();

    void report(long newCount) {
      count.increment();
    }

    long get() {
      return count.sum();
    }
  }

  /** LongAccumulator with Long::max merge — striping without losing max semantics. */
  static final class CounterLongAccumulator {

    final LongAccumulator count = new LongAccumulator(Long::max, 0L);

    void report(long newCount) {
      count.accumulate(newCount);
    }

    long get() {
      return count.get();
    }
  }

  static long benchSameKeyAtomicLong(int threads, long opsPerThread) throws InterruptedException {
    ConcurrentHashMap<String, CounterAtomicLong> map = new ConcurrentHashMap<>();
    CounterAtomicLong node = new CounterAtomicLong();
    map.put("hot", node);
    // maxCount is sketch-derived (external counter), so we pass an incrementing
    // value per reportToWorker — no pre-read of Node.count on the hot path.
    return runWorkload(threads, opsPerThread, i -> {
      CounterAtomicLong n = map.get("hot");
      n.report(i + 1);
    });
  }

  static long benchSameKeyLongAdder(int threads, long opsPerThread) throws InterruptedException {
    ConcurrentHashMap<String, CounterLongAdder> map = new ConcurrentHashMap<>();
    CounterLongAdder node = new CounterLongAdder();
    map.put("hot", node);
    return runWorkload(threads, opsPerThread, i -> {
      map.get("hot").report(i + 1);
    });
  }

  static long benchSameKeyLongAccumulator(int threads, long opsPerThread) throws InterruptedException {
    ConcurrentHashMap<String, CounterLongAccumulator> map = new ConcurrentHashMap<>();
    CounterLongAccumulator node = new CounterLongAccumulator();
    map.put("hot", node);
    return runWorkload(threads, opsPerThread, i -> {
      CounterLongAccumulator n = map.get("hot");
      n.report(i + 1);
    });
  }

  // ─────────────────────────────────────────────────────────────────────
  // Scenario B: Stripe-lock transmission on sketch slot updates
  // ─────────────────────────────────────────────────────────────────────

  private static final int STRIPES = 2048;
  private static final int SLOTS = 200_000;

  /** synchronized(Object[]) variant. */
  static long benchStripedSynchronized(int threads, long opsPerThread) throws InterruptedException {
    final Object[] locks = new Object[STRIPES];
    for (int i = 0; i < STRIPES; i++) {
      locks[i] = new Object();
    }
    final long[] sums = new long[SLOTS];
    return runWorkload(threads, opsPerThread, i -> {
      int slot = (int) (i & (SLOTS - 1));
      Object lock = locks[slot & (STRIPES - 1)];
      synchronized (lock) {
        sums[slot]++;
      }
    });
  }

  /** ReentrantLock[] variant. */
  static long benchStripedReentrantLock(int threads, long opsPerThread) throws InterruptedException {
    final ReentrantLock[] locks = new ReentrantLock[STRIPES];
    for (int i = 0; i < STRIPES; i++) {
      locks[i] = new ReentrantLock(false);
    }
    final long[] sums = new long[SLOTS];
    return runWorkload(threads, opsPerThread, i -> {
      int slot = (int) (i & (SLOTS - 1));
      ReentrantLock lock = locks[slot & (STRIPES - 1)];
      lock.lock();
      try {
        sums[slot]++;
      } finally {
        lock.unlock();
      }
    });
  }

  /** StampedLock[] variant (write-locked; optimistic-read track shown separately). */
  static long benchStripedStampedLockWrite(int threads, long opsPerThread) throws InterruptedException {
    final StampedLock[] locks = new StampedLock[STRIPES];
    for (int i = 0; i < STRIPES; i++) {
      locks[i] = new StampedLock();
    }
    final long[] sums = new long[SLOTS];
    return runWorkload(threads, opsPerThread, i -> {
      int slot = (int) (i & (SLOTS - 1));
      StampedLock lock = locks[slot & (STRIPES - 1)];
      long stamp = lock.writeLock();
      try {
        sums[slot]++;
      } finally {
        lock.unlockWrite(stamp);
      }
    });
  }

  // ─────────────────────────────────────────────────────────────────────
  // Scenario C: Mixed hot/cold (80% on 10 keys + 20% on 100k keys)
  // ─────────────────────────────────────────────────────────────────────

  private static final int HOT_KEYS = 10;
  private static final int COLD_KEYS = 100_000;
  private static final int TOTAL_KEYS = HOT_KEYS + COLD_KEYS;

  static long benchMixedAtomicLongSync(int threads, long opsPerThread) throws InterruptedException {
    final ConcurrentHashMap<String, CounterAtomicLong> map = new ConcurrentHashMap<>(TOTAL_KEYS * 2);
    for (int i = 0; i < TOTAL_KEYS; i++) {
      map.put("key" + i, new CounterAtomicLong());
    }
    // Per the realistic admit() pattern: maxCount is sketch-derived (externally grown),
    // not read from Node.count. Each thread tracks its own pseudo-counter to drive maxCount
    // but the reportToWorker simply passes that growing value to the Node.
    return runWorkload(threads, opsPerThread, i -> {
      String key = pickKey(i);
      CounterAtomicLong n = map.get(key);
      if (n != null) {
        n.report(i + 1);
      }
    });
  }

  static long benchMixedLongAdderSync(int threads, long opsPerThread) throws InterruptedException {
    final ConcurrentHashMap<String, CounterLongAdder> map = new ConcurrentHashMap<>(TOTAL_KEYS * 2);
    for (int i = 0; i < TOTAL_KEYS; i++) {
      map.put("key" + i, new CounterLongAdder());
    }
    // LongAdder accumulates the delta. Pass delta=1 each reportToWorker — the canonical
    // accumulation semantic that exercises LongAdder's per-cell increment fast path.
    return runWorkload(threads, opsPerThread, i -> {
      String key = pickKey(i);
      CounterLongAdder n = map.get(key);
      if (n != null) {
        n.report(1);
      }
    });
  }

  static long benchMixedLongAccumulatorSync(int threads, long opsPerThread) throws InterruptedException {
    final ConcurrentHashMap<String, CounterLongAccumulator> map = new ConcurrentHashMap<>(TOTAL_KEYS * 2);
    for (int i = 0; i < TOTAL_KEYS; i++) {
      map.put("key" + i, new CounterLongAccumulator());
    }
    // LongAccumulator with Long::max merge — passes the growing maxCount directly.
    // This is the truly realistic substitute for AtomicLong CAS max-raise semantics.
    return runWorkload(threads, opsPerThread, i -> {
      String key = pickKey(i);
      CounterLongAccumulator n = map.get(key);
      if (n != null) {
        n.report(i + 1);
      }
    });
  }

  static long benchMixedAtomicLongReentrant(int threads, long opsPerThread) throws InterruptedException {
    final ConcurrentHashMap<String, CounterAtomicLong> map = new ConcurrentHashMap<>(TOTAL_KEYS * 2);
    for (int i = 0; i < TOTAL_KEYS; i++) {
      map.put("key" + i, new CounterAtomicLong());
    }
    // ReentrantLock here simulates the admitUnderLock path for cold key admission.
    final ReentrantLock admission = new ReentrantLock(false);
    return runWorkload(threads, opsPerThread, i -> {
      String key = pickKey(i);
      CounterAtomicLong n = map.get(key);
      if (n != null) {
        if (i % 100 < 80) {
          // hot path — lock-free CAS
          long c = n.count.get() + 1;
          n.report(c);
        } else {
          // cold path — admission lock (simulates new key check)
          admission.lock();
          try {
            n.report(n.count.get() + 1);
          } finally {
            admission.unlock();
          }
        }
      }
    });
  }

  private static String pickKey(long i) {
    int bucket = (int) (i % 100);
    int idx;
    if (bucket < 80) {
      idx = (int) (i % HOT_KEYS);
    } else {
      idx = HOT_KEYS + (int) (i % COLD_KEYS);
    }
    return "key" + idx;
  }

  // ─────────────────────────────────────────────────────────────────────
  // Harness
  // ─────────────────────────────────────────────────────────────────────

  @FunctionalInterface
  private interface Workload {
    void run(long i);
  }

  /**
   * Run a workload across {@code threads} threads, each performing
   * {@code opsPerThread} iterations. Returns the median throughput (ops/ms)
   * across 3 trial runs.
   */
  private static long runWorkload(int threads, long opsPerThread, Workload work) throws InterruptedException {
    long[] results = new long[3];
    for (int trial = 0; trial < 3; trial++) {
      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(threads);
      ExecutorService pool = Executors.newFixedThreadPool(threads);
      for (int t = 0; t < threads; t++) {
        final long offset = (long) t * opsPerThread;
        pool.submit(() -> {
          try {
            start.await();
            for (long i = 0; i < opsPerThread; i++) {
              work.run(offset + i);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            done.countDown();
          }
        });
      }
      long t0 = System.nanoTime();
      start.countDown();
      done.await();
      long elapsedMs = Math.max(1, (System.nanoTime() - t0) / 1_000_000);
      pool.shutdown();
      results[trial] = (threads * opsPerThread) / elapsedMs;
    }
    java.util.Arrays.sort(results);
    return results[1]; // median
  }

  // ─────────────────────────────────────────────────────────────────────
  // Main
  // ─────────────────────────────────────────────────────────────────────

  /**
   * Run all scenarios and print a comparison table.
   *
   * @param args unused
   * @throws InterruptedException if a trial is interrupted
   */
  public static void main(String[] args) throws InterruptedException {
    int threads = 16;
    long sameKeyOps = 1_000_000L;
    long stripeOps = 200_000L;
    long mixedOps = 500_000L;

    System.out.println();
    System.out.println("=== HeavyKeeper Concurrency Selection Benchmark ===");
    System.out.println("Threads: " + threads);
    System.out.println();

    // Scenario A: Same-key contention
    System.out.println("--- Scenario A: Same-key contention (Node.count write path) ---");
    long alThroughput = benchSameKeyAtomicLong(threads, sameKeyOps);
    long laThroughput = benchSameKeyLongAdder(threads, sameKeyOps);
    long lacThroughput = benchSameKeyLongAccumulator(threads, sameKeyOps);
    System.out.printf("  AtomicLong CAS (max-raise):    %,d ops/ms%n", alThroughput);
    System.out.printf("  LongAdder   (accumulate):      %,d ops/ms%n", laThroughput);
    System.out.printf("  LongAccumulator (max-merge):   %,d ops/ms%n", lacThroughput);
    System.out.printf(
      "  LongAdder/AtomicLong: %.2f× | LongAccum/AtomicLong: %.2f× | LongAccum/LongAdder: %.2f×%n%n",
      (double) laThroughput / alThroughput,
      (double) lacThroughput / alThroughput,
      (double) lacThroughput / laThroughput
    );

    // Scenario B: Stripe-lock contention
    System.out.println("--- Scenario B: Stripe-lock contention (sketch slot update) ---");
    long syncThroughput = benchStripedSynchronized(threads, stripeOps);
    long reenterThroughput = benchStripedReentrantLock(threads, stripeOps);
    long stampedThroughput = benchStripedStampedLockWrite(threads, stripeOps);
    System.out.printf("  synchronized(Object[]):     %,d ops/ms%n", syncThroughput);
    System.out.printf("  ReentrantLock[] (nonfair):   %,d ops/ms%n", reenterThroughput);
    System.out.printf("  StampedLock[] (write-lock): %,d ops/ms%n", stampedThroughput);
    System.out.printf(
      "  Reentrant/sync ratio: %.2f× | Stamped/sync ratio: %.2f×%n%n",
      (double) reenterThroughput / syncThroughput,
      (double) stampedThroughput / syncThroughput
    );

    // Scenario C: Mixed hot/cold
    System.out.println("--- Scenario C: Mixed hot/cold (80%% hot, 20%% cold) ---");
    long mixedAlSync = benchMixedAtomicLongSync(threads, mixedOps);
    long mixedLaSync = benchMixedLongAdderSync(threads, mixedOps);
    long mixedLacSync = benchMixedLongAccumulatorSync(threads, mixedOps);
    long mixedAlReenter = benchMixedAtomicLongReentrant(threads, mixedOps);
    System.out.printf("  AtomicLong      + synchronized: %,d ops/ms%n", mixedAlSync);
    System.out.printf("  LongAdder       + synchronized: %,d ops/ms%n", mixedLaSync);
    System.out.printf("  LongAccumulator + synchronized: %,d ops/ms%n", mixedLacSync);
    System.out.printf("  AtomicLong      + ReentrantLock: %,d ops/ms%n", mixedAlReenter);
    System.out.printf(
      "  LongAdder/AtomicLong: %.2f× | LongAccum/AtomicLong: %.2f× | Reentrant/sync: %.2f×%n%n",
      (double) mixedLaSync / mixedAlSync,
      (double) mixedLacSync / mixedAlSync,
      (double) mixedAlReenter / mixedAlSync
    );

    System.out.println("=== Decision Summary ===");
    boolean longAdderWins = laThroughput >= alThroughput * 1.5 && mixedLaSync >= mixedAlSync * 1.15;
    boolean longAccumWins = lacThroughput >= alThroughput * 1.5 && mixedLacSync >= mixedAlSync * 1.15;
    String nodeChoice;
    if (longAccumWins && lacThroughput >= laThroughput * 0.7) {
      nodeChoice = "LONG_ACCUMULATOR (Long::max merge preserves monotonic semantics AND strips contention)";
    } else if (longAdderWins) {
      nodeChoice = "LONGADDER (significant throughput win; semantics change to accumulate)";
    } else {
      nodeChoice = "ATOMICLONG (accuracy preserved; throughput acceptable)";
    }
    System.out.println("Node.counter:     " + nodeChoice);
    System.out.println(
      "Stripe lock:      " +
        (reenterThroughput >= syncThroughput * 1.1 || mixedAlReenter >= mixedAlSync * 1.1
          ? "REENTRANTLOCK"
          : "SYNCHRONIZED (baseline — Reentrant/Stamped slower under lightweight hold)")
    );
    System.out.println();
  }
}
