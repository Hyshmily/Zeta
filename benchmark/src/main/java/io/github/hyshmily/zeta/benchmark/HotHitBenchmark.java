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

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.AccessCounter;
import io.github.hyshmily.zeta.cache.BroadcastBuffer;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycleImpl;
import io.github.hyshmily.zeta.cache.cachesupport.loader.LoaderOutcome;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.HeavyKeeper;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.reporting.ReportMessage;
import io.github.hyshmily.zeta.reporting.ReportPublisher;
import io.github.hyshmily.zeta.reporting.impl.KeyReporterImpl;
import io.github.hyshmily.zeta.scheduler.BackgroundRefresher;
import io.github.hyshmily.zeta.scheduler.DefaultBackgroundRefresher;
import io.github.hyshmily.zeta.sharding.ConsistentHashRing;
import io.github.hyshmily.zeta.sharding.ConsistentHashRingImpl;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sharding.HealthViewImpl;
import io.github.hyshmily.zeta.util.clock.MonotonicStampClock;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.openjdk.jmh.annotations.*;

/**
 * Cost of a full L1-hit {@code get} — the single hottest read path in the
 * library: Caffeine lookup → unwrap →
 * detector add + buffered report → local-promotion check.
 *
 * <p>Wiring mirrors production ({@code ZetaAutoConfiguration} shapes: real
 * {@link HeavyKeeper}, {@link HotKeyDetector}, {@link KeyReporter} with
 * a dropping publisher sink, and the L1 table built by
 * {@link HitPathAuditBenchmark#buildL1Table} in the production form —
 * maximumSize + {@code recordStats()} + variable expiry; the table shape must
 * keep mirroring the auto-configuration) except:
 * <ul>
 *   <li>{@link SingleFlight} is a pass-through fake — the hit path never
 *       touches dedup (a miss would, and misses are out of scope here).</li>
 *   <li>No Worker is registered, so each 50ms flush reconciles to an empty
 *       alive set and drops before routing. The per-hit cost under test is
 *       the buffered append ({@code WaveCounter.add}); routing/publish are
 *       amortized background work, identical to a production App whose
 *       Workers are (transiently) unreachable.</li>
 *   <li>Keys are pre-warmed into the sketch ({@code addDirect}), so the
 *       measurement reflects the warmed-hot steady state: entries sit HOT,
 *       promotion is a pure check, and no background refresh fires (TTLs are
 *       far-future).</li>
 * </ul>
 *
 * <p>Scenarios isolate the per-hit shape costs: plain small-String values,
 * 1&nbsp;KB Strings (written once via the real {@code putLocal} path), and the
 * 8-thread contention shape.
 *
 * <p>Run with the shaded jar:
 *
 * <pre>
 *   java -jar benchmark/target/benchmarks.jar HotHitBenchmark
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class HotHitBenchmark {

  private static final int KEY_SPACE = 64;

  /** Default get options: no TTL override, no stale policy — the plain-read shape under test. */
  private static final GetPolicy HIT_OPTIONS = GetPolicy.defaults();

  /** Tripwire reader: a hit must never invoke it. */
  private static <T> T unreachableReader() {
    throw new AssertionError("reader invoked on an L1 hit");
  }

  /** Hit path never touches dedup; a miss would be a setup bug, fail loudly. */
  static final class PassThroughFlight implements SingleFlight {

    @Override
    public boolean isBreakerOpen() {
      return false;
    }

    @Override
    public boolean isBreakerOpen(String cacheKey) {
      return false;
    }

    @Override
    public long estimatedInFlightSize() {
      return 0;
    }

    @Override
    public <T> LoaderOutcome<T> load(
      String cacheKey,
      Supplier<T> reader,
      boolean callerRuns,
      java.util.function.Consumer<? super T> leaderCommit
    ) {
      throw new AssertionError("SingleFlight.load invoked on an L1 hit");
    }

    @Override
    public <T> LoaderOutcome<T> loadIsolated(
      String cacheKey,
      Supplier<T> reader,
      java.util.function.Consumer<? super T> leaderCommit
    ) {
      throw new AssertionError("SingleFlight.loadIsolated invoked on an L1 hit");
    }

    @Override
    public void invalidate(String cacheKey) {}

    @Override
    public void invalidateAll() {}
  }

  /** Publisher sink: drops every batch (no broker in benchmarks). */
  static final class DropPublisher extends ReportPublisher {

    DropPublisher() {
      super(null, "bench", "benchApp");
    }

    @Override
    public void publish(String target, ReportMessage message) {}
  }

  ScheduledExecutorService scheduler;
  HotKeyDetector detector;
  KeyReporter reporter;
  HotKeyCache cachePlain;
  String[] keys;
  String[] bigKeys;

  /** Per-thread cursor so multi-thread variants never share a mutable index. */
  final ThreadLocal<int[]> cursor = ThreadLocal.withInitial(() -> new int[1]);

  private String next(String[] space) {
    int[] c = cursor.get();
    if (++c[0] >= space.length) {
      c[0] = 0;
    }
    return space[c[0]];
  }

  @Setup(Level.Trial)
  public void setup() {
    scheduler = Executors.newSingleThreadScheduledExecutor();
    ZetaProperties zetaProps = new ZetaProperties();
    // Production table shape (recordStats + variable expiry): a bare
    // Caffeine table measures a path production never runs — the table form
    // must mirror ZetaAutoConfiguration (buildL1Table), see HitPathAuditBenchmark.
    Cache<String, CacheEntry> caffeine = HitPathAuditBenchmark.buildL1Table(zetaProps);
    MonotonicStampClock stampClock = new MonotonicStampClock(1L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS);
    HealthView healthView = new HealthViewImpl(30_000, 3, false, 0);
    HeavyKeeper sketch = new HeavyKeeper(
      zetaProps.getTopK(),
      zetaProps.getWidth(),
      zetaProps.getDepth(),
      zetaProps.getDecay(),
      zetaProps.getMinCount(),
      zetaProps.getExpelledQueueCapacity(),
      zetaProps.getSketchWindowCount(),
      true
    );
    this.detector = new HotKeyDetector(sketch);
    EntryLifecycle entryLifecycle = new EntryLifecycleImpl(caffeine, zetaProps, healthView);
    BackgroundRefresher refresher = new DefaultBackgroundRefresher(caffeine, Runnable::run, entryLifecycle, 10);
    BroadcastBuffer broadcast = new BroadcastBuffer(scheduler, null, 500, 2_000, null);
    ConsistentHashRing ring = new ConsistentHashRingImpl(150);
    KeyReporter reporter = new KeyReporterImpl(new DropPublisher(), "bench", 1000, 100, 1, ring, healthView);
    reporter.start();
    this.reporter = reporter;
    // design decisions: access counting: one shared access counter feeds detection AND reporting; the
    // per-hit cost under test is exactly one buffered append.
    AccessCounter accessCounter = new AccessCounter(detector, reporter, scheduler, 50L, 500L);
    accessCounter.afterPropertiesSet();
    SingleFlight flight = new PassThroughFlight();
    cachePlain = new HotKeyCache(
      detector,
      caffeine,
      flight,
      entryLifecycle,
      refresher,
      accessCounter,
      broadcast,
      stampClock,
      healthView
    );

    long farFuture = System.currentTimeMillis() + 300_000;
    keys = new String[KEY_SPACE];
    for (int k = 0; k < KEY_SPACE; k++) {
      keys[k] = "bench:key:" + k;
      caffeine.put(keys[k], entry("v-" + k, farFuture));
      detector.addDirect(keys[k], 100_000L);
    }
    bigKeys = new String[KEY_SPACE];
    String bigValue = "v".repeat(1024);
    for (int k = 0; k < KEY_SPACE; k++) {
      bigKeys[k] = "bench:big:" + k;
      // Real write path, once per key: values are stored exactly as passed
      // (design decisions: api surface removed the value codec), so every measured hit below is a
      // plain L1 unwrap with no decode step.
      cachePlain.putLocal(bigKeys[k], bigValue, 0, 0);
      detector.addDirect(bigKeys[k], 100_000L);
    }
  }

  private static CacheEntry entry(String value, long farFuture) {
    return EntryDraft.blank(null)
      .value(value)
      .version(1)
      .ttl(300_000, 30_000, 300_000, 30_000)
      .expiryAt(farFuture, farFuture)
      .keyState(KeyState.NORMAL)
      .build();
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    // Orderly first: stop the tide loop before killing the shared
    // scheduler, otherwise an in-flight reschedule logs a shutdown-race
    // ERROR (harmless, but noisy).
    reporter.stop();
    scheduler.shutdownNow();
  }

  /** Full hit, small-String values. */
  @Benchmark
  public String get_hit_smallString() {
    return cachePlain.get(next(keys), HotHitBenchmark::unreachableReader, HIT_OPTIONS);
  }

  /** Full hit, small-String values, 8 contending readers. */
  @Benchmark
  @Threads(8)
  public String get_hit_smallString_8t() {
    return cachePlain.get(next(keys), HotHitBenchmark::unreachableReader, HIT_OPTIONS);
  }

  /** Full hit, 1KB String values. */
  @Benchmark
  public String get_hit_1kString() {
    return cachePlain.get(next(bigKeys), HotHitBenchmark::unreachableReader, HIT_OPTIONS);
  }

  /**
   * Side-effect-free {@code peek} hit: Caffeine lookup + unwrap only — no
   * detection, no report, no promotion. The floor every other scenario pays
   * on top of.
   */
  @Benchmark
  public String peek_hit() {
    return cachePlain.peek(next(keys));
  }
}
