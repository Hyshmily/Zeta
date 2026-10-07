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
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.CentralDispatcher;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.cache.cachesupport.BroadcastBuffer;
import io.github.hyshmily.zeta.cache.cachesupport.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.impl.EntryLifecycleImpl;
import io.github.hyshmily.zeta.cache.codec.CacheCompressor;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.HeavyKeeper;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.model.ReadPolicy;
import io.github.hyshmily.zeta.reporting.ReportMessage;
import io.github.hyshmily.zeta.reporting.ReportPublisher;
import io.github.hyshmily.zeta.reporting.impl.KeyReporterImpl;
import io.github.hyshmily.zeta.rule.Rule;
import io.github.hyshmily.zeta.rule.impl.RuleMatcherImpl;
import io.github.hyshmily.zeta.scheduler.DefaultBackgroundRefresher;
import io.github.hyshmily.zeta.sharding.impl.HealthViewImpl;
import io.github.hyshmily.zeta.sharding.impl.RingManagerImpl;
import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import io.github.hyshmily.zeta.util.version.impl.VersionControllerImpl;
import org.openjdk.jmh.annotations.*;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Cost of a full L1-hit {@code get} — the single hottest read path in the
 * library: normalize → rule memo probe → Caffeine lookup → unwrap →
 * detector add + buffered report → local-promotion check.
 *
 * <p>Wiring mirrors production ({@code ZetaAutoConfiguration} shapes: real
 * {@link HeavyKeeper}, {@link HotKeyDetector}, {@link KeyReporterImpl} with
 * a dropping publisher sink) except:
 * <ul>
 *   <li>{@link SingleFlight} is a pass-through fake — the hit path never
 *       touches dedup (a miss would, and misses are out of scope here).</li>
 *   <li>No Worker is registered, so each 50ms flush reconciles to an empty
 *       alive set and drops before routing. The per-hit cost under test is
 *       the buffered append ({@code WaveCounter.count}); routing/publish are
 *       amortized background work, identical to a production App whose
 *       Workers are (transiently) unreachable.</li>
 *   <li>Keys are pre-warmed into the sketch ({@code addDirect}), so the
 *       measurement reflects the warmed-hot steady state: entries sit HOT,
 *       promotion is a pure check, and no background refresh fires (TTLs are
 *       far-future).</li>
 * </ul>
 *
 * <p>Scenarios isolate the three per-hit shape costs: plain small-String
 * values (no compression anywhere), 1&nbsp;KB Strings (LZ4 decompress on
 * every hit — written once via the real {@code putLocal} path), and a
 * 10-rule matcher (memo probe on every hit; see {@code RuleBenchmark} for
 * the matcher matrix itself).
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

  /** Tripwire reader: a hit must never invoke it. */
  private static final ReadPolicy HIT_POLICY = ReadPolicy.of(HotHitBenchmark::unreachableReader);

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
    public long estimatedInflightSize() {
      return 0;
    }

    @Override
    public <T> Optional<T> load(String cacheKey, Supplier<T> reader) {
      throw new AssertionError("SingleFlight.load invoked on an L1 hit");
    }

    @Override
    public <T> Map<String, Optional<T>> load(Iterable<String> cacheKeys, Function<? super String, ? extends T> reader) {
      throw new AssertionError("SingleFlight.load invoked on an L1 hit");
    }

    @Override
    public void invalidate(String cacheKey) {}
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
  KeyReporterImpl reporter;
  HotKeyCache cachePlain;
  HotKeyCache cacheRuled;
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
    Cache<String, CacheEntry> caffeine = Caffeine.newBuilder().maximumSize(100_000).build();
    ZetaProperties ttlConfig = new ZetaProperties();
    SnowflakeIdGenerator snowflake = new SnowflakeIdGenerator(0, 1, 5L, false);
    HealthViewImpl healthView = new HealthViewImpl(30_000, 3);
    HeavyKeeper sketch = new HeavyKeeper(
      ttlConfig.getTopK(),
      ttlConfig.getWidth(),
      ttlConfig.getDepth(),
      ttlConfig.getDecay(),
      ttlConfig.getMinCount(),
      ttlConfig.getExpelledQueueCapacity(),
      ttlConfig.getSketchWindowCount(),
      true
    );
    HotKeyDetector detector = new HotKeyDetector(sketch, scheduler);
    detector.afterPropertiesSet();
    this.detector = detector;
    EntryLifecycle entryLifecycle = new EntryLifecycleImpl(caffeine, ttlConfig, CacheCompressor.NONE, healthView);
    RuleMatcherImpl plainRules = new RuleMatcherImpl(Optional.empty(), Optional.empty());
    RuleMatcherImpl tenRules = new RuleMatcherImpl(Optional.empty(), Optional.empty());
    addTenRules(tenRules);
    DefaultBackgroundRefresher refresher = new DefaultBackgroundRefresher(
      caffeine,
      Runnable::run,
      entryLifecycle.ttlPolicy(),
      CacheCompressor.NONE,
      10
    );
    BroadcastBuffer broadcast = new BroadcastBuffer(scheduler, Optional.empty(), 500, 2_000, null);
    RingManagerImpl ring = new RingManagerImpl(150);
    KeyReporterImpl reporter = new KeyReporterImpl(
      new DropPublisher(),
      scheduler,
      50,
      "bench",
      1000,
      100,
      1,
      ring,
      healthView,
      snowflake
    );
    reporter.start();
    this.reporter = reporter;
    CentralDispatcher dispatcher = new CentralDispatcher(Optional.of(reporter), Optional.empty(), broadcast, detector);
    VersionControllerImpl versions = new VersionControllerImpl(Optional.empty(), 60, snowflake);
    SingleFlight flight = new PassThroughFlight();
    cachePlain = new HotKeyCache(
      detector,
      caffeine,
      flight,
      entryLifecycle,
      refresher,
      Runnable::run,
      dispatcher,
      plainRules,
      versions,
      ttlConfig,
      healthView,
      CacheCompressor.NONE,
      null
    );
    cacheRuled = new HotKeyCache(
      detector,
      caffeine,
      flight,
      entryLifecycle,
      refresher,
      Runnable::run,
      dispatcher,
      tenRules,
      versions,
      ttlConfig,
      healthView,
      CacheCompressor.NONE,
      null
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
      // Real write path (compresses once here); every measured hit below
      // pays the matching decompress.
      cachePlain.putLocal(bigKeys[k], bigValue, 0, 0);
      detector.addDirect(bigKeys[k], 100_000L);
    }
  }

  private static CacheEntry entry(String value, long farFuture) {
    return CacheEntry.builder()
      .value(value)
      .dataVersion(1)
      .isVersionDegraded(false)
      .decisionVersion(0)
      .hardTtlMs(300_000)
      .hardExpireAtMs(farFuture)
      .softTtlMs(30_000)
      .softExpireAtMs(farFuture)
      .keyState(KeyState.NORMAL)
      .normalHardTtlMs(300_000)
      .normalSoftTtlMs(30_000)
      .build();
  }

  private static void addTenRules(RuleMatcherImpl matcher) {
    matcher.addRule(new Rule(Rule.RuleType.PREFIX, "bench:nope:p0:", Rule.RuleAction.BLOCK));
    matcher.addRule(new Rule(Rule.RuleType.PREFIX, "bench:nope:p1:", Rule.RuleAction.BLOCK));
    matcher.addRule(new Rule(Rule.RuleType.PREFIX, "bench:nope:p2:", Rule.RuleAction.BLOCK));
    matcher.addRule(new Rule(Rule.RuleType.WILDCARD, "bench:*:nope:w0", Rule.RuleAction.BLOCK));
    matcher.addRule(new Rule(Rule.RuleType.WILDCARD, "bench:*:nope:w1", Rule.RuleAction.BLOCK));
    matcher.addRule(new Rule(Rule.RuleType.REGEX, "bench:\\d+:nope:r0", Rule.RuleAction.BLOCK));
    matcher.addRule(new Rule(Rule.RuleType.REGEX, "bench:\\d+:nope:r1", Rule.RuleAction.BLOCK));
    matcher.addRule(new Rule(Rule.RuleType.REGEX, "bench:\\d+:nope:r2", Rule.RuleAction.BLOCK));
    matcher.addRule(new Rule(Rule.RuleType.EXACT, "bench:never:exact0", Rule.RuleAction.BLOCK));
    matcher.addRule(new Rule(Rule.RuleType.EXACT, "bench:never:exact1", Rule.RuleAction.BLOCK));
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    // Orderly first: stop the tide loops before killing their shared
    // scheduler, otherwise an in-flight reschedule logs a shutdown-race
    // ERROR (harmless, but noisy).
    reporter.stop();
    detector.destroy();
    scheduler.shutdownNow();
  }

  /** Full hit, small-String values, empty rule set. */
  @Benchmark
  public Optional<String> get_hit_smallString() {
    return cachePlain.get(next(keys), HIT_POLICY);
  }

  /** Full hit, small-String values, 8 contending readers. */
  @Benchmark
  @Threads(8)
  public Optional<String> get_hit_smallString_8t() {
    return cachePlain.get(next(keys), HIT_POLICY);
  }

  /** Full hit, 1KB LZ4 values: adds one decompress per hit vs the small-String shape. */
  @Benchmark
  public Optional<String> get_hit_lz4String() {
    return cachePlain.get(next(bigKeys), HIT_POLICY);
  }

  /** Full hit, small-String values, 10-rule matcher: adds one memo probe per hit. */
  @Benchmark
  public Optional<String> get_hit_ruled() {
    return cacheRuled.get(next(keys), HIT_POLICY);
  }

  /**
   * Side-effect-free {@code peek} hit: Caffeine lookup + unwrap only — no
   * detection, no report, no promotion. The floor every other scenario pays
   * on top of.
   */
  @Benchmark
  public Optional<String> peek_hit() {
    return cachePlain.peek(next(keys));
  }
}
