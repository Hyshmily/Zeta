package io.github.hyshmily.hotkey.hotkeycache;

import io.github.hyshmily.hotkey.constant.HotKeyConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class VersionController {

  public record VersionResult(long dataVersion, boolean degraded) {}

  private final Optional<StringRedisTemplate> redisTemplate;
  private final int versionKeyTtlMinutes;
  private final AtomicLong fallbackVersionCounter = new AtomicLong(0);

  public VersionController(Optional<StringRedisTemplate> redisTemplate, int versionKeyTtlMinutes) {
    this.redisTemplate = redisTemplate;
    this.versionKeyTtlMinutes = versionKeyTtlMinutes;
  }

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
}
