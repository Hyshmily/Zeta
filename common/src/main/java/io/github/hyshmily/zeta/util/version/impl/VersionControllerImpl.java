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
package io.github.hyshmily.zeta.util.version.impl;

import static io.github.hyshmily.zeta.constants.ZetaConstants.Redis.VERSION_KEY_PREFIX;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.util.LogThrottle;
import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import io.github.hyshmily.zeta.util.version.VersionController;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

/**
 * Manages per-key monotonically increasing version numbers for the application-level
 * {@code dataVersion} space. Provides dual-mode version allocation: Redis-based
 * (primary) and local fallback (degraded).
 *
 * <p><b>Version space design (see ADR-0008):</b>
 * <ul>
 *   <li><b>Normal (Redis):</b> Uses {@code Redis INCR} inside an atomic Lua script
 *       ({@code INCR + EXPIRE}) to produce positive {@code long} values. Guarantees
 *       global monotonic ordering across all application instances.</li>
 *   <li><b>Degraded (local fallback):</b> When Redis is unavailable, versions are
 *       assigned in the <em>negative</em> long space as
 *       {@code Long.MIN_VALUE | snowflakeId.nextId()} (see ADR-0019/ADR-0022).
 *       The Snowflake component makes degraded versions globally comparable
 *       across instances (time-sortable, collision-free in practice), and the
 *       negative space keeps them strictly below any positive Redis version so
 *       the 4-case guard in {@link io.github.hyshmily.zeta.util.version.VersionGuard#shouldSkipForSync}
 *       works without flag-aware comparison logic.</li>
 * </ul>
 *
 * <p><b>Thread safety:</b> Redis INCR is atomic by nature. The local fallback uses
 * {@link AtomicLong} and is safe for concurrent access from multiple threads.
 *
 * @see VersionGuard
 */
@Internal
@Slf4j
@RequiredArgsConstructor
public class VersionControllerImpl implements VersionController {

  /** Optional Redis template — absent when Redis is not configured. */
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private final Optional<StringRedisTemplate> redisTemplate;

  /** TTL (minutes) for the per-key version keys in Redis. */
  private final int versionKeyTtlMinutes;

  /** Snowflake ID generator for time-sortable, globally unique degraded versions. */
  private final SnowflakeIdGenerator snowflakeIdGenerator;

  /** Counter tracking total degraded version allocations since startup. */
  private final AtomicLong fallbackVersionCounter = new AtomicLong(0);

  /**
   * Per-key highest seen dataVersion floor for wraparound detection.
   * <p>Tracks the maximum dataVersion value observed per cache key. After a
   * successful Redis INCR, the new version is compared against this floor.
   * A new version lower than the floor indicates the Redis version key expired
   * and was re-created (wraparound). This cache is bounded at 10k entries with
   * LRU eviction on strong keys — the bound keeps memory fixed regardless of
   * the number of distinct cache keys.
   * <p><b>Update protocol:</b> the floor is only ever merged upwards via an
   * atomic {@code asMap().compute()} max-merge (see {@link #nextVersion}) —
   * a smaller observed value can never lower the floor.
   */
  private final Cache<String, Long> versionFloorCache = Caffeine.newBuilder().maximumSize(10_000).build();

  /**
   * Window (ms) for the rate-limited wraparound ERROR — the standard 10s
   * window used across the codebase (e.g. BroadcastBuffer flush failures).
   * Concurrent {@link #nextVersion} interleavings legitimately produce a
   * version below the observed floor (a later INCR can commit its floor
   * update before an earlier INCR observes it), so v &lt; floor is not always
   * a genuine wraparound — an unthrottled ERROR would log false alarms at
   * concurrency rate.
   */
  /**
   * Strict one-per-window admission for the wraparound ERROR. Within the
   * window the alarm is suppressed: an unthrottled ERROR would log false
   * alarms at concurrency rate. Admission is strict — {@link LogThrottle}
   * claims the window with a compare-and-set, so exactly one caller per window
   * logs. The atomicity and the monotonic clock are provided by
   * {@link LogThrottle}.
   */
  private final LogThrottle wraparoundLogThrottle = LogThrottle.perDefaultWindow();

  /**
   * Process-wide gate for probe-failure alarms (E1, ADR-0037 convention): a
   * Redis outage or poisoned version key would otherwise WARN once per L1 miss
   * at miss rate. Probe outage is a process-wide condition, so one shared gate
   * (not per instance) admits a single WARN per 10s window; the rest degrade
   * to DEBUG. Shared (not per instance) also because {@link #parseVersion} is
   * static and must not force a signature change.
   */
  private static final LogThrottle PROBE_WARN_THROTTLE = LogThrottle.perDefaultWindow();

  /**
   * Process-wide gate for the version-increment failure WARN (ADR-0037
   * convention). A Redis outage reaches this branch on <em>every</em> write, so
   * an unthrottled WARN — carrying a full stack trace — would emit at write rate
   * and turn the log into its own availability problem. The increment path
   * carries a full exception (unlike the probe path, which carries a message),
   * so the admitted line is an ERROR and suppressed occurrences fall to DEBUG.
   *
   * <p>Process-wide rather than per instance because a Redis outage is a
   * process-wide condition, and the diagnostic value lies in the rate, not in
   * which key triggered it.
   */
  private static final LogThrottle.Counting INCREMENT_WARN_THROTTLE = new LogThrottle.Counting();

  /**
   * How far below the observed floor a freshly allocated version must fall
   * before the counter is treated as genuinely restarted rather than as a
   * benign concurrent interleaving, and is repaired.
   *
   * <p>{@code v < floor} alone is ambiguous: two concurrent INCRs on one key
   * return consecutive values, and whichever commits its floor update first
   * makes the other observe a value below the floor. That skew equals the
   * number of concurrent allocations on the same key — single-digit in
   * practice. A genuine restart (the version key expired, was evicted under an
   * {@code allkeys-*} policy, was lost in a {@code FLUSHALL}, or a failover
   * promoted a replica missing the key) resets INCR to 1, so the gap is the
   * whole floor.
   *
   * <p>A margin of {@value #WRAPAROUND_REPAIR_MARGIN} is therefore far above any
   * plausible concurrent skew and far below any floor worth repairing, which
   * makes a false-positive repair effectively impossible. Residual: a restart
   * combined with more than the margin of same-key writes in the very first
   * moment is not repaired, and is reported by the wraparound alarm instead —
   * the alarm is the fallback, not the repair.
   */
  private static final long WRAPAROUND_REPAIR_MARGIN = 1024L;

  /** Holder for the Redis INCR script — lazily loaded to avoid {@code NoClassDefFoundError} when Redis is absent. */
  @SuppressWarnings("java:S3985")
  private static class IncrScriptHolder {

    static final DefaultRedisScript<Long> SCRIPT = new DefaultRedisScript<>(
      "local v = redis.call('INCR', KEYS[1]); redis.call('EXPIRE', KEYS[1], ARGV[1]); return v",
      Long.class
    );
  }

  /**
   * Atomically increments the version counter for the given cache key and returns
   * the new version with its degradation status.
   *
   * <p><b>Redis path:</b> Executes a Lua script that performs an atomic
   * {@code INCR} on the key {@code zeta:version:{cacheKey}} followed by
   * {@code EXPIRE} with the configured TTL ({@code versionKeyTtlMinutes}).
   * The Lua atomicity guarantees that the INCR and EXPIRE happen together.
   *
   * <p><b>Floor observation:</b> the INCR result is merged into the per-key
   * wraparound floor inside a single atomic {@code compute()} that only ever
   * moves the floor upwards (max-merge). Observing and updating in one step
   * closes the race where a concurrent higher version commits its floor update
   * between this thread's read and write: a read-then-put pair would raise a
   * false wraparound alarm <em>and</em> regress the floor with the stale put,
   * hiding a later genuine INCR restart.
   *
   * <p><b>Fallback path:</b> If {@link StringRedisTemplate} is not configured
   * (Redis dependency absent), or if any Redis operation throws an exception
   * (connection failure, timeout), the method falls back to the local degraded
   * counter via {@link #fallbackVersion()}.
   *
   * <p>The fallback supplier is resolved <b>lazily</b> ({@code orElseGet}, never
   * {@code orElse}): {@link #fallbackVersion()} has side effects — it bumps
   * {@code fallbackVersionCounter} and consumes a Snowflake ID under a
   * {@code synchronized} generator — so evaluating it on the healthy Redis path
   * would inflate the degraded-version metric and burn a sequence number on every
   * single write.
   *
   * @param cacheKey the key to version; must not be null or empty
   * @return a {@link VersionResult} containing the new version number and a
   *         {@code degraded} flag indicating whether the version came from the
   *         local fallback ({@code true}) or from Redis ({@code false})
   */
  @Override
  public VersionResult nextVersion(String cacheKey) {
    return redisTemplate
      .map(t -> {
        try {
          long v = t.execute(
            // IncrScriptHolder is private, so its member cannot be static-imported.
            IncrScriptHolder.SCRIPT,
            List.of(VERSION_KEY_PREFIX + cacheKey),
            String.valueOf(versionKeyTtlMinutes * 60L)
          );
          // Atomic max-merge: the floor is observed and raised in one step and
          // can never regress. `regression` marks a v below the already-observed
          // floor — either a genuine INCR restart (version key expired, see
          // ADR-0022) or a benign concurrent interleaving; the alarm is
          // rate-limited either way (see LogThrottle.DEFAULT_WINDOW_MS).
          boolean[] regression = new boolean[1];
          long[] floor = new long[1];
          versionFloorCache
            .asMap()
            .compute(cacheKey, (k, current) -> {
              if (current == null || v > current) {
                return v;
              }
              regression[0] = true;
              floor[0] = current;
              return current;
            });
          if (regression[0]) {
            reportWraparound(cacheKey, v);
            return repairRestartedCounter(t, cacheKey, v, floor[0]);
          }
          return new VersionResult(v, false);
        } catch (Exception e) {
          // A Redis outage lands here on every write, so an unthrottled WARN with
          // a full stack trace would emit at write rate — the log becomes its own
          // availability problem and evicts the diagnostics an operator needs.
          // ADR-0037 one-per-window convention; the occurrence count rides along
          // so a suppressed burst is still visible.
          LogThrottle.Counting.Attempt attempt = INCREMENT_WARN_THROTTLE.record();
          if (!attempt.admitted()) {
            log.debug(
              "Redis version increment failed, fallback to local counter ({} in current window): {}",
              cacheKey,
              attempt.count(),
              e.toString()
            );
          } else if (attempt.count() > 0) {
            log.error(
              "Redis version increment failed for key {}, fallback to local counter ({} suppressed in the last window)",
              cacheKey,
              attempt.count(),
              e
            );
          } else {
            log.error("Redis version increment failed, fallback to local counter: {}", cacheKey, e);
          }
          return fallbackVersion();
        }
      })
      .orElseGet(this::fallbackVersion);
  }

  /**
   * Decide whether a version that regressed below the floor needs the Redis
   * counter repaired, and do so.
   *
   * <p>Without the repair the regressed version is handed out and then
   * <em>silently rejected everywhere</em>: this node's own L1 write is skipped by
   * {@code VersionGuard.shouldSkipForSync} (the existing entry carries the
   * higher version), every peer skips the broadcast for the same reason, and the
   * value stays pinned until the INCR counter re-climbs past the floor — which
   * on a cold key means 5000 more writes, i.e. potentially never. A lost
   * {@code FLUSHALL}, an {@code allkeys-lru} eviction of {@code zeta:version:*},
   * or a failover to a replica missing the key all reach this state without
   * waiting out the 7-day TTL.
   *
   * <p>Repairs only when the gap exceeds {@link #WRAPAROUND_REPAIR_MARGIN}, which
   * excludes benign same-key concurrent skew. The repair re-seeds the Redis
   * counter to {@code floor + 1}, so the returned version is authoritative, the
   * local write applies, and peers accept the broadcast.
   *
   * @param template  the Redis template, for the repair write
   * @param cacheKey  the key whose counter restarted
   * @param v         the observed (regressed) version
   * @param floor     the highest version previously observed for this key
   * @return a result carrying {@code floor + 1} when the counter was repaired,
   *         otherwise the unmodified {@code v}
   */
  private VersionResult repairRestartedCounter(StringRedisTemplate template, String cacheKey, long v, long floor) {
    if (floor - v <= WRAPAROUND_REPAIR_MARGIN) {
      // Benign concurrent interleaving (or a restart too recent to separate
      // from one): report the observed value unchanged, exactly as before. The
      // write is applied by whoever holds the floor; this one loses the race,
      // which is the correct outcome for two writes to the same key.
      return new VersionResult(v, false);
    }
    long repaired = floor + 1;
    try {
      template
        .opsForValue()
        .set(VERSION_KEY_PREFIX + cacheKey, String.valueOf(repaired), Duration.ofMinutes(versionKeyTtlMinutes));
      log.error(
        "dataVersion counter restarted for key {}: INCR returned {} against an observed floor of {}. " +
          "Re-seeded zeta:version:{} to {} so this write and its broadcast are not silently rejected. " +
          "Increase versionKeyTtlMinutes (currently {} min) or avoid Long.MAX_VALUE hard TTL on this key.",
        cacheKey,
        v,
        floor,
        cacheKey,
        repaired,
        versionKeyTtlMinutes
      );
      return new VersionResult(repaired, false);
    } catch (Exception repairFailure) {
      // The counter is still behind the floor, so the write will be rejected by
      // the version matrix. Degrade to reporting the observed value and let the
      // alarm above carry the diagnosis — a failed repair must not turn into an
      // exception on the write path.
      log.warn("Failed to re-seed the dataVersion counter for key {} to {}", cacheKey, repaired, repairFailure);
      return new VersionResult(v, false);
    }
  }

  /**
   * Logs the wraparound ERROR, rate-limited to one per
   * {@value LogThrottle#DEFAULT_WINDOW_MS}ms window. Within the window the alarm
   * is suppressed: under concurrency, benign interleavings (a later INCR
   * committing its floor update before an earlier INCR observes it) produce
   * the same v &lt; floor signature as a genuine wraparound, and a genuine
   * wraparound repeats on every subsequent allocation until the floor
   * re-climbs — an unthrottled ERROR would flood the log at allocation rate.
   *
   * @param cacheKey the cache key whose version regressed below the floor
   * @param v        the observed (lower) version
   */
  private void reportWraparound(String cacheKey, long v) {
    if (wraparoundLogThrottle.tryAcquire()) {
      log.error(
        "dataVersion wraparound detected for key {}: {} < floor {}. " +
          "The Redis version key (zeta:version:{}) expired before the L1 entry, " +
          "causing INCR to restart from 1. Increase versionKeyTtlMinutes (currently {} min) " +
          "or avoid Long.MAX_VALUE hard TTL on this key.",
        cacheKey,
        v,
        versionFloorCache.getIfPresent(cacheKey),
        cacheKey,
        versionKeyTtlMinutes
      );
      return;
    }
    log.debug(
      "dataVersion below observed floor for key {}: {} (wraparound alarm suppressed within {}ms window)",
      cacheKey,
      v,
      LogThrottle.DEFAULT_WINDOW_MS
    );
  }

  /**
   * Allocates a version in the negative {@code long} space for degraded operation.
   *
   * <p>The version is computed as {@code Long.MIN_VALUE + Snowflake ID}, ensuring
   * that every degraded version is numerically less than any positive Redis INCR
   * version. Snowflake IDs provide global time-sortability, so degraded versions
   * from different instances are directly comparable — unlike the previous per-JVM
   * counter approach that added a local counter to {@code Long.MIN_VALUE}.
   *
   * @return a {@link VersionResult} with a negative version and {@code degraded=true},
   *         never null
   */
  @Override
  public VersionResult fallbackVersion() {
    fallbackVersionCounter.incrementAndGet();
    long version = Long.MIN_VALUE | snowflakeIdGenerator.nextId();
    return new VersionResult(version, true);
  }

  /**
   * Whether Redis is configured for version tracking.
   *
   * @return {@code true} if a {@link StringRedisTemplate} bean is available;
   *         {@code false} if only local fallback is possible
   */
  @Override
  public boolean isRedisConfigured() {
    return redisTemplate.isPresent();
  }

  /**
   * Read-only current version for the given cache key via Redis GET.
   *
   * <p>Returns empty when the key does not exist, Redis is unavailable,
   * or deserialization fails.
   */
  @Override
  public Optional<Long> currentVersion(String cacheKey) {
    return redisTemplate
      .map(t -> {
        try {
          String v = t.opsForValue().get(VERSION_KEY_PREFIX + cacheKey);
          return v != null ? Optional.of(Long.parseLong(v)) : Optional.<Long>empty();
        } catch (Exception e) {
          if (PROBE_WARN_THROTTLE.tryAcquire()) {
            log.warn("Failed to read current version for key {}", cacheKey, e);
          } else {
            log.debug("Failed to read current version for key {} (suppressed)", cacheKey);
          }
          return Optional.<Long>empty();
        }
      })
      .orElse(Optional.empty());
  }

  /**
   * Read-only current versions for the given cache keys via a single
   * pipelined MGET (1 round trip, ADR-0033).
   *
   * <p>Per-key fail-open: a key whose value is missing, malformed, or part
   * of a failed batch maps to an empty {@code Optional}. A batch-wide Redis
   * failure maps every key to empty — a failed probe never demotes an entry.
   */
  @Override
  public Map<String, Optional<Long>> currentVersions(Iterable<String> cacheKeys) {
    List<String> keys = new ArrayList<>();
    cacheKeys.forEach(keys::add);
    if (keys.isEmpty()) {
      return Collections.emptyMap();
    }
    return redisTemplate
      .map(t -> {
        try {
          List<String> versionKeys = keys
            .stream()
            .map(k -> VERSION_KEY_PREFIX + k)
            .toList();
          List<String> values = t.opsForValue().multiGet(versionKeys);
          Map<String, Optional<Long>> out = new LinkedHashMap<>();

          for (int i = 0; i < keys.size(); i++) {
            String v = values != null && i < values.size() ? values.get(i) : null;
            out.put(keys.get(i), parseVersion(v));
          }
          return out;
        } catch (Exception e) {
          if (PROBE_WARN_THROTTLE.tryAcquire()) {
            log.warn("Failed to read current versions for {} keys", keys.size(), e);
          } else {
            log.debug("Failed to read current versions for {} keys (suppressed)", keys.size());
          }
          return allEmpty(keys);
        }
      })
      .orElseGet(() -> allEmpty(keys));
  }

  /**
   * Parse a raw version-key value, mapping malformed values to empty
   * (fail-open).
   */
  private static Optional<Long> parseVersion(String raw) {
    if (raw == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(Long.parseLong(raw));
    } catch (NumberFormatException e) {
      if (PROBE_WARN_THROTTLE.tryAcquire()) {
        log.warn("Malformed version value: {}", raw, e);
      } else {
        log.debug("Malformed version value (suppressed): {}", raw);
      }
      return Optional.empty();
    }
  }

  /** Map every key to an empty version (fail-open). */
  private static Map<String, Optional<Long>> allEmpty(List<String> keys) {
    Map<String, Optional<Long>> out = new LinkedHashMap<>();
    for (String key : keys) {
      out.put(key, Optional.empty());
    }
    return out;
  }

  /**
   * Cumulative count of fallbacks to the local degraded counter.
   * <p>
   * Each call to {@link #fallbackVersion()} increments this counter.
   * A non-zero value indicates that Redis was unavailable at some point
   * during this JVM's lifetime.
   *
   * @return the total number of degraded version allocations since startup
   */
  @Override
  public long getDegradedVersionCount() {
    return fallbackVersionCounter.get();
  }
}
