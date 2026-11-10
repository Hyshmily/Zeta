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
import com.github.benmanes.caffeine.cache.Expiry;
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
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.scheduler.BackgroundRefresher;
import io.github.hyshmily.zeta.scheduler.DefaultBackgroundRefresher;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sharding.HealthViewImpl;
import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatMessage;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.clock.MonotonicStampClock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
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
 * Audit benchmark for the three production hit shapes of {@code HotKeyCache.get}
 * — the companion to {@link HotHitBenchmark}, which only covers the warmed-HOT
 * steady state (both benchmarks now build the L1 table through the same
 * production-shaped wiring).
 *
 * <h2>What each {@code mode} means</h2>
 * <ul>
 *   <li><b>{@code LOCAL_NORMAL}</b> — a plain {@link KeyState#NORMAL} entry the
 *       local TopK does <em>not</em> track: the long-tail shape. Exercises the
 *       per-hit {@code hotKeyDetector.contains} probe
 *       ({@code LocalPromotion.isPromotable}).</li>
 *   <li><b>{@code WORKER_HOT}</b> — a Worker-stamped HOT entry
 *       ({@code decisionNodeId} set, live health record): the highest-QPS shape.
 *       Exercises the per-hit Decision-Validity liveness probe
 *       ({@code EntryLifecycle.demoteIfRemoteNodeIdInvalid} →
 *       {@code HealthView.isCurrentIncarnation(String, long)}).</li>
 *   <li><b>{@code SOFT_EXPIRED}</b> — a NORMAL entry past its soft TTL with a
 *       <b>slow</b> refresh source (500&nbsp;ms). This pins the steady state
 *       inside the soft-expire window, so every hit runs the
 *       {@code BackgroundRefresher.triggerBackgroundRefresh} in-flight
 *       check (one lock-free {@code refreshes.get} plus a volatile flag
 *       read; only the arming read installs a marker via {@code putIfAbsent}).
 *       The "one arming per soft-TTL window" shape of a fast source is
 *       <em>not</em> what this mode measures — it measures the slow-loader /
 *       in-flight case.</li>
 * </ul>
 *
 * <h2>Key form</h2>
 * {@code REUSED} looks up the instance seeded into L1 (its {@code String.hashCode}
 * is cached); {@code FRESH} builds an equal-but-new {@code String} per operation
 * (hash recomputed from scratch), which is what a caller doing
 * {@code "user:" + id} pays on every hit.
 *
 * <h2>Fidelity to production</h2>
 * The L1 table is built by {@link #buildL1Table} — a copy of
 * {@code ZetaAutoConfiguration.buildL1Table} (maximumSize, {@code recordStats()},
 * variable {@link Expiry}). Known deviations, all deliberate:
 * <ul>
 *   <li>{@link SingleFlight} is a throwing stub — the hit paths must never touch
 *       dedup, so a hit that did would fail loudly rather than be silently
 *       measured.</li>
 *   <li>{@code AccessCounter} is wired with {@code reporter == null} (detector
 *       leg only): {@code WaveCounter.add} is identical, but the 50&nbsp;ms
 *       reporter tide (routing/publishing) is not in the picture, so the
 *       background thread pool cannot perturb the measurement.</li>
 *   <li>Values are small {@code String}s; the value size is not a parameter.</li>
 * </ul>
 *
 * <h2>Run</h2>
 * <pre>
 *   java -jar benchmark/target/benchmarks.jar HitPathAuditBenchmark -prof gc
 * </pre>
 * The class reports both {@link Mode#AverageTime} (ns/op) and
 * {@link Mode#SampleTime} (percentiles: p50/p90/p99/p99.9/p100).
 */
@BenchmarkMode({ Mode.AverageTime, Mode.SampleTime })
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 2, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class HitPathAuditBenchmark {

  /** The hit shape under test — see the class Javadoc. */
  public enum HitMode {
    LOCAL_NORMAL,
    WORKER_HOT,
    SOFT_EXPIRED,
  }

  /** Whether the lookup key is the seeded instance or an equal fresh one. */
  public enum KeyForm {
    REUSED,
    FRESH,
  }

  private static final String WORKER_NODE = "worker-bench";
  private static final long WORKER_EPOCH = 1L;
  private static final long HARD_TTL_MS = 600_000L;
  private static final long SOFT_TTL_MS = 1_000L;
  /** How long the background-refresh source blocks (SOFT_EXPIRED only). */
  private static final long SLOW_READER_PARK_MS = 500L;

  @Param
  HitMode mode;

  @Param
  KeyForm keyForm;

  @Param({ "1024", "65536" })
  int keySpace;

  private static final GetPolicy HIT_OPTIONS = GetPolicy.defaults();

  /** The shared engine under test. */
  HotKeyCache cache;

  /** The seeded key instances (physical keys, exactly as L1 holds them). */
  String[] keys;

  /** The value source: never invoked on a hit; blocks for SOFT_EXPIRED's refresher. */
  Supplier<String> reader = () -> {
    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(SLOW_READER_PARK_MS));
    return "v";
  };

  private ScheduledExecutorService scheduler;
  private ExecutorService refreshPool;
  private AccessCounter accessCounter;

  /** Per-thread cursor so multi-thread trials never share a mutable index. */
  @State(Scope.Thread)
  public static class Cursor {

    int index;

    /** Advances and wraps without a ThreadLocal lookup on the measured path. */
    int next(int size) {
      if (++index >= size) {
        index = 0;
      }
      return index;
    }
  }

  @Setup(Level.Trial)
  public void setup() {
    TimeSource.start();
    scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "bench-scheduler");
      t.setDaemon(true);
      return t;
    });
    refreshPool = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), r -> {
      Thread t = new Thread(r, "bench-refresh");
      t.setDaemon(true);
      return t;
    });

    ZetaProperties props = new ZetaProperties();
    Cache<String, CacheEntry> caffeine = buildL1Table(props);
    HealthView healthView = new HealthViewImpl(300_000L, 3, false, 0);
    // A live, non-stale Worker incarnation so WORKER_HOT entries stay valid
    // (a dead one would demote on the first hit and drift into LOCAL_NORMAL).
    healthView.onHeartbeat(new WorkerHeartbeatMessage(1L, WORKER_NODE, WORKER_EPOCH, true, null));

    HeavyKeeper sketch = new HeavyKeeper(
      props.getTopK(),
      props.getWidth(),
      props.getDepth(),
      props.getDecay(),
      props.getMinCount(),
      props.getExpelledQueueCapacity(),
      props.getSketchWindowCount(),
      true
    );
    HotKeyDetector detector = new HotKeyDetector(sketch);
    EntryLifecycle entryLifecycle = new EntryLifecycleImpl(caffeine, props, healthView);
    BackgroundRefresher refresher = new DefaultBackgroundRefresher(
      caffeine,
      refreshPool,
      entryLifecycle,
      props.effectiveRefreshMaxPools()
    );
    BroadcastBuffer broadcast = new BroadcastBuffer(scheduler, null, 500, 2_000, null);
    accessCounter = new AccessCounter(detector, null, scheduler, 50L, 500L);
    accessCounter.afterPropertiesSet();
    MonotonicStampClock clock = new MonotonicStampClock(1L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS);
    cache = new HotKeyCache(
      detector,
      caffeine,
      new NoLoadFlight(),
      entryLifecycle,
      refresher,
      accessCounter,
      broadcast,
      clock,
      healthView
    );

    long now = TimeSource.currentTimeMillis();
    keys = new String[keySpace];
    for (int i = 0; i < keySpace; i++) {
      String key = "bench:key:" + i;
      keys[i] = key;
      caffeine.put(key, entry(now, i));
    }
  }

  private CacheEntry entry(long now, int index) {
    EntryDraft draft = EntryDraft.blank(null)
      .value("v-" + index)
      .version(1)
      .ttl(HARD_TTL_MS, 0L, HARD_TTL_MS, 0L)
      .expiryAt(now + HARD_TTL_MS, 0L);
    switch (mode) {
      case LOCAL_NORMAL -> draft.keyState(KeyState.NORMAL);
      case WORKER_HOT -> draft.keyState(KeyState.HOT).decision(DecisionStamp.hot(5, WORKER_NODE, WORKER_EPOCH));
      case SOFT_EXPIRED -> draft
        .keyState(KeyState.NORMAL)
        .ttl(HARD_TTL_MS, SOFT_TTL_MS, HARD_TTL_MS, SOFT_TTL_MS)
        // Already stale: the soft window is the state under test.
        .expiryAt(now + HARD_TTL_MS, now - 60_000L);
      default -> throw new IllegalStateException(mode.toString());
    }
    return draft.build();
  }

  /**
   * Copy of {@code ZetaAutoConfiguration.buildL1Table} — the production L1
   * configuration this benchmark must match for the numbers to be comparable.
   * Kept as a copy (the original is private) so a drift is visible here; shared
   * with {@link HotHitBenchmark}, whose table must stay in the same shape.
   */
  @SuppressWarnings("unchecked")
  static Cache<String, CacheEntry> buildL1Table(ZetaProperties properties) {
    var cfg = properties.getCache();
    Caffeine<Object, Object> builder = Caffeine.newBuilder();
    builder.maximumSize(cfg.getMaxSize());
    builder.recordStats();
    Caffeine<String, CacheEntry> typed = builder.expireAfter(
      new Expiry<String, CacheEntry>() {
        @Override
        public long expireAfterCreate(String key, CacheEntry value, long currentTimeNanos) {
          return remainingHardTtlNanos(value.getHardExpireAtMs());
        }

        @Override
        public long expireAfterUpdate(String key, CacheEntry value, long currentTimeNanos, long currentDuration) {
          return remainingHardTtlNanos(value.getHardExpireAtMs());
        }

        @Override
        public long expireAfterRead(String key, CacheEntry value, long currentTimeNanos, long currentDuration) {
          return currentDuration;
        }
      }
    );
    return typed.build();
  }

  /** Verbatim copy of {@code ZetaAutoConfiguration.remainingHardTtlNanos}. */
  private static long remainingHardTtlNanos(long hardExpireAtMs) {
    if (hardExpireAtMs == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    long remainingMs = hardExpireAtMs - TimeSource.currentTimeMillis();
    return TimeUnit.MILLISECONDS.toNanos(Math.max(1, remainingMs));
  }

  @Benchmark
  public String hit_singleThread(Cursor cursor) {
    return hit(cursor);
  }

  @Benchmark
  @Threads(8)
  public String hit_eightThreads(Cursor cursor) {
    return hit(cursor);
  }

  private String hit(Cursor cursor) {
    int i = cursor.next(keySpace);
    String key = keyForm == KeyForm.REUSED ? keys[i] : "bench:key:" + i;
    return cache.get(key, reader, HIT_OPTIONS);
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    if (accessCounter != null) {
      accessCounter.destroy();
    }
    if (refreshPool != null) {
      refreshPool.shutdownNow();
    }
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
  }

  /**
   * Hit paths must never touch dedup: any call fails the trial loudly instead of
   * silently changing what is being measured.
   */
  static final class NoLoadFlight implements SingleFlight {

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
}
