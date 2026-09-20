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

import io.github.hyshmily.zeta.cache.codec.DefaultWeigher;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Cost of {@link DefaultWeigher#weigh(String, Object)} for the value shapes named in
 * ADR-0069's calibration. The weigher runs inside Caffeine's {@code computeIfAbsent}
 * mapping function <em>while holding the per-bin lock</em> (ADR-0030: no heavy work
 * under the lock), so its per-call cost is a design constraint, not just a statistic.
 *
 * <p>Values are built once in setup and reused: the weigher walks the value's object
 * graph without mutating it, so re-measuring one instance measures the walk, not the
 * construction.
 *
 * <p>Run with the shaded jar:
 *
 * <pre>
 *   java -jar benchmark/target/benchmarks.jar DefaultWeigherBenchmark
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class DefaultWeigherBenchmark {

  DefaultWeigher weigher;

  /** 32-char ASCII key: the per-key fast path from the ADR-0069 calibration table. */
  String key;

  /** 64-char ASCII String: the typical cached value after the ADR-0069 fix (~370 B entry). */
  String value64;

  /** byte[1024]: the compressed-value shape (LZ4 stores byte[] payloads). */
  byte[] bytes1k;

  /** 8-element ArrayList of 16-char Strings: small container, exact (non-sampled) walk. */
  List<String> smallList;

  /** 8-entry LinkedHashMap of 16-char Strings: non-RandomAccess container walk. */
  Map<String, String> smallMap;

  @Setup(Level.Trial)
  public void setup() {
    weigher = DefaultWeigher.INSTANCE;
    key = "bench:key:0123456789abcdef0123456789abcdef";
    value64 = "value:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    bytes1k = new byte[1024];
    for (int i = 0; i < bytes1k.length; i++) {
      bytes1k[i] = (byte) (i & 0x7f);
    }
    smallList = new ArrayList<>(8);
    smallMap = new LinkedHashMap<>(16);
    for (int i = 0; i < 8; i++) {
      String element = "elem:" + i + ":0123456789ab";
      smallList.add(element);
      smallMap.put("k" + i + ":0123456789ab", element);
    }
  }

  @Benchmark
  public int weigh_string64() {
    return weigher.weigh(key, value64);
  }

  @Benchmark
  public int weigh_bytes1k() {
    return weigher.weigh(key, bytes1k);
  }

  @Benchmark
  public int weigh_smallList() {
    return weigher.weigh(key, smallList);
  }

  @Benchmark
  public int weigh_smallMap() {
    return weigher.weigh(key, smallMap);
  }
}
