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

import io.github.hyshmily.zeta.util.LogThrottle;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Cost of {@link LogThrottle#tryAcquire()} on the suppressed path — the path every
 * throttled call site takes in steady state (the window is already claimed, so the
 * benchmark measures the monotonic-clock read plus the {@code AtomicLong.get()},
 * <em>not</em> the CAS).
 *
 * <p>The window is set to one hour so it never rolls over during a trial: a window
 * rollover would admit one caller mid-measurement and distort the sample.
 *
 * <p>This is the number the ADR-0037 one-per-window convention rests on: the throttle
 * must be cheap enough that guarding a hot-path log with it is free relative to the
 * log call it protects. Run with the shaded jar:
 *
 * <pre>
 *   java -jar benchmark/target/benchmarks.jar LogThrottleBenchmark
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class LogThrottleBenchmark {

  /** One hour: pre-claimed at setup and never rolls over mid-trial, so every call is suppressed. */
  private static final long WINDOW_MS = 3_600_000;

  private LogThrottle singleThreadThrottle;
  private LogThrottle contendedThrottle;

  @Setup(Level.Trial)
  public void setup() {
    singleThreadThrottle = new LogThrottle(WINDOW_MS);
    singleThreadThrottle.tryAcquire();
    contendedThrottle = new LogThrottle(WINDOW_MS);
    contendedThrottle.tryAcquire();
  }

  /** Steady-state cost for one thread hammering an already-claimed window. */
  @Benchmark
  public boolean tryAcquire_suppressed() {
    return singleThreadThrottle.tryAcquire();
  }

  /**
   * Eight threads sharing one throttle: measures the contended clock read plus the
   * shared {@code AtomicLong} read. This mirrors the "several app threads hit the
   * same failing path" production shape the throttle was built for.
   */
  @Benchmark
  @Threads(8)
  public boolean tryAcquire_suppressedContended() {
    return contendedThrottle.tryAcquire();
  }
}
