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

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import io.github.hyshmily.zeta.util.version.VersionController;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import java.util.List;
import java.util.Optional;
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
 *       assigned in the <em>negative</em> long space ({@code Long.MIN_VALUE + localCounter}).
 *       This ensures all degraded versions sort below any positive Redis version in
 *       numeric comparison, enabling the 4-case guard in {@link io.github.hyshmily.zeta.util.version.VersionGuard#shouldSkipForSync}
 *       to work correctly without flag-aware comparison logic.</li>
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

  /** Holder for the Redis INCR script — lazily loaded to avoid {@code NoClassDefFoundError} when Redis is absent. */
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
   * <p><b>Fallback path:</b> If {@link StringRedisTemplate} is not configured
   * (Redis dependency absent), or if any Redis operation throws an exception
   * (connection failure, timeout), the method falls back to the local degraded
   * counter via {@link #fallbackVersion()}.
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
          Long v = t.execute(
            IncrScriptHolder.SCRIPT,
            List.of(ZetaConstants.Redis.VERSION_KEY_PREFIX + cacheKey),
            String.valueOf(versionKeyTtlMinutes * 60L)
          );
          return new VersionResult(v, false);
        } catch (Exception e) {
          log.warn("Redis version increment failed, fallback to local counter: {}", cacheKey, e);
          return fallbackVersion();
        }
      })
      .orElse(fallbackVersion());
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
    long version = Long.MIN_VALUE + snowflakeIdGenerator.nextId();
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
