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
package io.github.hyshmily.zeta.benchmark;

import io.github.hyshmily.zeta.hotkeydetector.doublebuffer.WaveCounter;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Cost of the {@link WaveCounter#count(String, long)} add path — the single hottest
 * write path in the library (every reported key on every app instance flows through
 * it).
 *
 * <p>Two key-space sizes isolate the two shapes of the path:
 *
 * <ul>
 *   <li>{@code keySpace=64}: a small working set that always hits the per-thread local
 *       buffer (the steady-state hot-key case — pure buffer append, amortised bulk
 *       merge into the shared table every {@code DEFAULT_MAX_OPCOUNT} ops).</li>
 *   <li>{@code keySpace=100000}: a wide stream that mostly misses the local buffer and
 *       inserts into the shared table (the high-cardinality case — probe cost, table
 *       growth, and the capacity-drop governor all join the picture).</li>
 * </ul>
 *
 * <p>The single-thread and 8-thread variants share one {@code WaveCounter}, matching
 * the production shape where several app threads report concurrently.
 *
 * <p>Run with the shaded jar:
 *
 * <pre>
 *   java -jar benchmark/target/benchmarks.jar WaveCounterBenchmark
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class WaveCounterBenchmark {

  @Param({"64", "100000"})
  int keySpace;

  WaveCounter counter;
  String[] keys;

  /** Per-thread cursor so the 8-thread variant never shares a mutable index. */
  final ThreadLocal<int[]> cursor = ThreadLocal.withInitial(() -> new int[1]);

  @Setup(Level.Trial)
  public void setup() {
    counter = new WaveCounter(batch -> {});
    counter.afterPropertiesSet();
    keys = new String[keySpace];
    for (int k = 0; k < keySpace; k++) {
      keys[k] = "bench:key:" + k;
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    counter.destroy();
  }

  /** Single writer: the uncontended add path. */
  @Benchmark
  public void count_singleThread() {
    int[] c = cursor.get();
    if (++c[0] >= keySpace) {
      c[0] = 0;
    }
    counter.count(keys[c[0]], 1);
  }

  /** Eight writers sharing one counter: the concurrent production shape. */
  @Benchmark
  @Threads(8)
  public void count_8threads() {
    int[] c = cursor.get();
    if (++c[0] >= keySpace) {
      c[0] = 0;
    }
    counter.count(keys[c[0]], 1);
  }
}
