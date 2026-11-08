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
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.util.TimeSource;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * The Caffeine-only floor of an L1 hit — the number every Zeta hit pays before
 * any Zeta code runs.
 *
 * <p>Purpose: attribute a measured Zeta hit
 * ({@link HitPathAuditBenchmark}) between "Caffeine's own per-read work" and
 * "what Zeta adds on top". The four cells are the axes that differ between
 * production and the {@link HotHitBenchmark} wiring:
 * <ul>
 *   <li>{@code recordStats} — production always enables it
 *       ({@code ZetaAutoConfiguration.buildL1Table}). With it, every read takes
 *       a stats-counter increment inside {@code getIfPresent}.</li>
 *   <li>{@code variableExpiry} — production installs a per-entry
 *       {@link Expiry} (the logical-TTL table). Without it Caffeine skips the
 *       variable-expiry branch of {@code hasExpired} and the expiry arithmetic
 *       on writes.</li>
 * </ul>
 *
 * <h2>Alignment with production</h2>
 * Matched: {@code maximumSize} ({@code zeta.cache.max-size}), {@code recordStats()},
 * the variable {@link Expiry} with {@code expireAfterRead} returning
 * {@code currentDuration} unchanged, {@code CacheEntry} values (so the expiry
 * callback reads the same field), and the {@code getIfPresent} call shape.
 *
 * <p>Residual differences, deliberate:
 * <ul>
 *   <li>No Zeta wrapper: this benchmark is <b>only</b> {@code Cache.getIfPresent},
 *       so the difference from {@link HitPathAuditBenchmark} is exactly Zeta's
 *       added work.</li>
 *   <li>Keys are the seeded instances (cached {@code String.hashCode}) — the
 *       {@code FRESH} key-form cost is isolated in the audit benchmark, not here.</li>
 *   <li>No eviction pressure: the key space (1024) sits far below
 *       {@code maximumSize} (100&nbsp;000), so the admission policy never evicts
 *       during a trial. Production may evict; that cost is not measured here.</li>
 * </ul>
 *
 * <h2>Run</h2>
 * <pre>
 *   java -jar benchmark/target/benchmarks.jar CaffeineFloorBenchmark -prof gc
 * </pre>
 */
@BenchmarkMode({ Mode.AverageTime, Mode.SampleTime })
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 2, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class CaffeineFloorBenchmark {

  private static final int KEY_SPACE = 1024;
  private static final long HARD_TTL_MS = 600_000L;

  /** Production enables stats recording; both cells are measured. */
  @Param({ "false", "true" })
  boolean recordStats;

  /** Production installs a variable per-entry {@link Expiry}; both cells are measured. */
  @Param({ "false", "true" })
  boolean variableExpiry;

  private Cache<String, CacheEntry> cache;
  private String[] keys;

  /** Per-thread cursor: no ThreadLocal on the measured path. */
  @State(Scope.Thread)
  public static class Cursor {

    int index;

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
    cache = build(recordStats, variableExpiry, new ZetaProperties().getCache().getMaxSize());
    long now = TimeSource.currentTimeMillis();
    keys = new String[KEY_SPACE];
    for (int i = 0; i < KEY_SPACE; i++) {
      String key = "floor:key:" + i;
      keys[i] = key;
      cache.put(key, entry(now, i));
    }
  }

  @SuppressWarnings("unchecked")
  private static Cache<String, CacheEntry> build(boolean stats, boolean variable, long maxSize) {
    // Caffeine.newBuilder() is not generic: the builder starts as <Object, Object>
    // and only expireAfter(...) narrows it. The no-variable cell must NOT install
    // an Expiry (that branch of hasExpired is one of the cells under test), so the
    // untyped build is cast here — the cache genuinely is untyped at that point.
    Caffeine<Object, Object> builder = Caffeine.newBuilder();
    builder.maximumSize(maxSize);
    if (stats) {
      builder.recordStats();
    }
    if (variable) {
      return builder.expireAfter(new ProductionExpiry()).build();
    }
    // The older benchmark's shape: a fixed write expiry, no per-entry callback.
    builder.expireAfterWrite(1, TimeUnit.DAYS);
    return (Cache<String, CacheEntry>) (Cache<?, ?>) builder.build();
  }

  private static CacheEntry entry(long now, int index) {
    return EntryDraft.blank(null)
      .value("v-" + index)
      .version(1)
      .ttl(HARD_TTL_MS, 0L, HARD_TTL_MS, 0L)
      .expiryAt(now + HARD_TTL_MS, 0L)
      .keyState(KeyState.NORMAL)
      .build();
  }

  @Benchmark
  public CacheEntry getIfPresent_singleThread(Cursor cursor) {
    return hit(cursor);
  }

  @Benchmark
  @Threads(8)
  public CacheEntry getIfPresent_eightThreads(Cursor cursor) {
    return hit(cursor);
  }

  private CacheEntry hit(Cursor cursor) {
    return cache.getIfPresent(keys[cursor.next(KEY_SPACE)]);
  }

  /**
   * Copy of {@code ZetaAutoConfiguration.buildL1Table}'s {@link Expiry}: the
   * remaining wall-clock time to the entry's logical hard expiry, floored at
   * 1&nbsp;ms, {@code Long.MAX_VALUE} for permanent entries, and a read that
   * never moves the duration.
   */
  private static final class ProductionExpiry implements Expiry<String, CacheEntry> {

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

  /** Verbatim copy of {@code ZetaAutoConfiguration.remainingHardTtlNanos}. */
  private static long remainingHardTtlNanos(long hardExpireAtMs) {
    if (hardExpireAtMs == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    long remainingMs = hardExpireAtMs - TimeSource.currentTimeMillis();
    return TimeUnit.MILLISECONDS.toNanos(Math.max(1, remainingMs));
  }
}
