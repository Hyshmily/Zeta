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
package io.github.hyshmily.hotkey.hotkeycache;

import io.github.hyshmily.hotkey.constant.HotKeyConstants;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import io.github.hyshmily.hotkey.log.DefaultLogger;
import io.github.hyshmily.hotkey.log.HotKeyLogger;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

/**
 * Manages per-key version numbers using either Redis INCR (primary) or a
 * local {@link AtomicLong} fallback. When Redis is unavailable, versions
 * are assigned in the negative {@code long} space so they always sort below
 * normal (positive) Redis versions — this guarantees correct broadcast
 * ordering without flag-aware comparison logic.
 */
@RequiredArgsConstructor
public class VersionController {

  private static final HotKeyLogger log = new DefaultLogger(VersionController.class);

  /**
   * Result of a version allocation.
   *
   * @param dataVersion the allocated version number
   * @param degraded    {@code true} if this version came from the local
   *                    fallback (Redis was unavailable)
   */
  public record VersionResult(long dataVersion, boolean degraded) {}

  private final Optional<StringRedisTemplate> redisTemplate;
  private final int versionKeyTtlMinutes;
  private final AtomicLong fallbackVersionCounter = new AtomicLong(0);

  /**
   * Atomically increment the version counter for the given cache key.
   * Uses a Lua script for atomic INCR + EXPIRE in Redis.
   * Falls back to {@link #fallbackVersion()} on any Redis failure.
   *
   * @param cacheKey the key to version
   * @return a {@link VersionResult} containing the new version and degraded flag
   */
  public VersionResult nextVersion(String cacheKey) {
    return redisTemplate
      .map(t -> {
        try {
          String script =
            "local v = redis.call('INCR', KEYS[1]) " +
            "if tonumber(ARGV[1]) > 0 then " +
            "    redis.call('EXPIRE', KEYS[1], ARGV[1]) " +
            "end " +
            "return v";

          Long v = t.execute(
            new DefaultRedisScript<>(script, Long.class),
            List.of(HotKeyConstants.REDIS_VERSION_KEY_PREFIX + cacheKey),
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
   * Build a degraded version in negative {@code long} space so that all
   * degraded versions sort below any normal (positive) Redis INCR version.
   * This guarantees the {@code sendDeduped} numeric comparison in
   * {@link io.github.hyshmily.hotkey.broadcast.CacheSyncPublisher} correctly
   * prefers normal broadcasts over degraded ones without flag-aware logic.
   */
  public VersionResult fallbackVersion() {
    long version = Long.MIN_VALUE + fallbackVersionCounter.incrementAndGet();
    return new VersionResult(version, true);
  }

  /** Whether Redis is configured for version tracking. */
  public boolean isRedisConfigured() {
    return redisTemplate.isPresent();
  }

  /** Cumulative count of fallbacks to the local degraded counter. */
  public long getDegradedVersionCount() {
    return fallbackVersionCounter.get();
  }
}
