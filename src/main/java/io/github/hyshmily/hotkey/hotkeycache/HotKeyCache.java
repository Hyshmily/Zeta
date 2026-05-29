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

import static io.github.hyshmily.hotkey.hotkeycache.CacheKeysPolicy.invalidCacheKey;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.algorithm.TopK;
import io.github.hyshmily.hotkey.broadcast.BroadcastPublisher;
import io.github.hyshmily.hotkey.entity.CacheEntry;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

@Slf4j
public class HotKeyCache {

  private final TopK hotKeyDetector;
  private final Cache<String, Object> caffeineCache;
  private final SingleFlight singleFlight;
  private final SoftExpireManager softExpireManager;
  private final Optional<BroadcastPublisher> broadcastPublisher;
  private final Executor hotKeyExecutor;
  private final Optional<StringRedisTemplate> redisTemplate;
  private final int versionKeyTtlMinutes;
  private static final String NO_BROADCAST_PUBLISHER = "No broadcast publisher found, please enable Broadcast";

  public HotKeyCache(
    TopK hotKeyDetector,
    Cache<String, Object> caffeineCache,
    SingleFlight singleFlight,
    SoftExpireManager softExpireManager,
    Optional<BroadcastPublisher> broadcastPublisher,
    Executor hotKeyExecutor,
    Optional<StringRedisTemplate> redisTemplate,
    int versionKeyTtlMinutes
  ) {
    this.hotKeyDetector = hotKeyDetector;
    this.caffeineCache = caffeineCache;
    this.singleFlight = singleFlight;
    this.softExpireManager = softExpireManager;
    this.broadcastPublisher = broadcastPublisher;
    this.hotKeyExecutor = hotKeyExecutor;
    this.redisTemplate = redisTemplate;
    this.versionKeyTtlMinutes = versionKeyTtlMinutes;
  }

  private static long expireAt(long hardTtlMs) {
    return hardTtlMs > 0 ? System.currentTimeMillis() + hardTtlMs : Long.MAX_VALUE;
  }

  public boolean isHotKey(String cacheKey) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("isHotKey: invalid cacheKey");
      return false;
    }
    return hotKeyDetector.contains(cacheKey);
  }

  @SuppressWarnings("unchecked")
  public <T> Optional<T> peek(String cacheKey) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("peek: invalid cacheKey");
      return Optional.empty();
    }

    return Optional.ofNullable(caffeineCache.getIfPresent(cacheKey)).map(raw ->
      raw instanceof CacheEntry vv ? (T) vv.getValue() : (T) raw
    );
  }

  public <T> Optional<T> get(String cacheKey, Supplier<T> reader) {
    return get(cacheKey, reader, Long.MAX_VALUE);
  }

  @SuppressWarnings("unchecked")
  public <T> Optional<T> get(String cacheKey, Supplier<T> reader, long hardTtlMs) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("get: invalid cacheKey");
      return Optional.empty();
    }

    return Optional.ofNullable(caffeineCache.getIfPresent(cacheKey))
      .map(raw -> {
        T val = raw instanceof CacheEntry vv ? (T) vv.getValue() : (T) raw;
        hotKeyDetector.add(cacheKey, 1);
        return val;
      })
      .or(() ->
        singleFlight
          .load(cacheKey, reader)
          .map(value -> {
            if (hotKeyDetector.add(cacheKey, 1).isHotKey()) {
              caffeineCache.put(cacheKey, new CacheEntry(value, 0L, false, expireAt(hardTtlMs)));
              softExpireManager.refresh(cacheKey, softExpireManager.getDefaultSoftTtlMs());
              broadcastPublisher.ifPresent(p -> p.broadcastHotKey(cacheKey));
              log.debug("HotKey detected and loaded into local caffeine cache: {}", cacheKey);
            }
            return value;
          })
      );
  }

  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader) {
    return getWithSoftExpire(cacheKey, reader, Long.MAX_VALUE, softExpireManager.getDefaultSoftTtlMs());
  }

  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader, long softTtlMs) {
    return getWithSoftExpire(cacheKey, reader, Long.MAX_VALUE, softTtlMs);
  }

  @SuppressWarnings("unchecked")
  public <T> Optional<T> getWithSoftExpire(String cacheKey, Supplier<T> reader, long hardTtlMs, long softTtlMs) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("getWithSoftExpire: invalid cacheKey");
      return Optional.empty();
    }
    if (!softExpireManager.isEnabled()) {
      log.debug("Soft expire not enabled (softTtlMs=0), fallback to get()");
      return get(cacheKey, reader, hardTtlMs);
    }

    return Optional.ofNullable(caffeineCache.getIfPresent(cacheKey))
      .map(raw -> {
        T cached = raw instanceof CacheEntry vv ? (T) vv.getValue() : (T) raw;

        if (softExpireManager.isExpired(cacheKey)) {
          softExpireManager.triggerAsyncRefresh(cacheKey, reader, softTtlMs);
        }

        hotKeyDetector.add(cacheKey, 1);
        return cached;
      })
      .or(() ->
        singleFlight
          .load(cacheKey, reader)
          .map(value -> {
            if (hotKeyDetector.add(cacheKey, 1).isHotKey()) {
              caffeineCache.put(cacheKey, new CacheEntry(value, 0L, false, expireAt(hardTtlMs)));
              softExpireManager.refresh(cacheKey, softTtlMs);
              broadcastPublisher.ifPresent(p -> p.broadcastHotKey(cacheKey));
            }
            return value;
          })
      );
  }

  public void invalidate(String cacheKey) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("invalidate: invalid cacheKey");
      return;
    }
    TransactionSupport.runNowOrAfterCommit(() -> {
      VersionResult vr = nextVersion(cacheKey);
      caffeineCache.invalidate(cacheKey);
      broadcastPublisher.ifPresentOrElse(
        p -> p.broadcastHotKeyWithVersion(cacheKey, vr.version(), vr.degraded()),
        () -> log.debug("invalidate:" + NO_BROADCAST_PUBLISHER)
      );
    });
  }

  public void invalidateAll(Collection<String> cacheKeys) {
    List<String> validKeys = cacheKeys
      .stream()
      .filter(k -> !invalidCacheKey(k))
      .toList();
    if (validKeys.isEmpty()) {
      log.debug("invalidateAll: all cacheKeys are invalid");
      return;
    }
    TransactionSupport.runNowOrAfterCommit(() -> {
      caffeineCache.invalidateAll(validKeys);
      if (broadcastPublisher.isPresent()) {
        BroadcastPublisher p = broadcastPublisher.get();
        validKeys.forEach(p::invalidateHotKey);
      } else {
        log.debug("No broadcast publisher found, skip broadcast for {} keys", validKeys.size());
      }
    });
  }

  public <T> void putThrough(String cacheKey, T value, Runnable writer) {
    putThrough(cacheKey, value, writer, Long.MAX_VALUE, softExpireManager.getDefaultSoftTtlMs());
  }

  public <T> void putThrough(String cacheKey, T value, Runnable writer, long hardTtlMs) {
    putThrough(cacheKey, value, writer, hardTtlMs, softExpireManager.getDefaultSoftTtlMs());
  }

  public <T> void putThrough(String cacheKey, T value, Runnable writer, long hardTtlMs, long softTtlMs) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("putThrough: invalid cacheKey");
      return;
    }
    TransactionSupport.runAfterCommit(
      () -> {
        writer.run();
        VersionResult vr = nextVersion(cacheKey);

        caffeineCache.put(cacheKey, new CacheEntry(value, vr.version(), vr.degraded(), expireAt(hardTtlMs)));
        softExpireManager.refresh(cacheKey, softTtlMs);
        broadcastPublisher.ifPresentOrElse(
          p -> p.broadcastHotKeyWithVersion(cacheKey, vr.version(), vr.degraded()),
          () -> log.debug("putThrough:" + NO_BROADCAST_PUBLISHER)
        );
      },
      hotKeyExecutor
    );
  }

  public void putBeforeInvalidate(String cacheKey, Runnable mutation) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("putBeforeInvalidate: invalid cacheKey");
      return;
    }
    TransactionSupport.runNowOrAfterCommit(() -> {
      try {
        mutation.run();
      } catch (Exception e) {
        log.error("putBeforeInvalidate failed, skip local invalidate and broadcast: {}", cacheKey, e);
        return;
      }
      VersionResult vr = nextVersion(cacheKey);
      caffeineCache.invalidate(cacheKey);
      broadcastPublisher.ifPresentOrElse(
        p -> p.broadcastHotKeyWithVersion(cacheKey, vr.version(), vr.degraded()),
        () -> log.debug("putBeforeInvalidate:" + NO_BROADCAST_PUBLISHER)
      );
    });
  }

  private record VersionResult(long version, boolean degraded) {}

  private VersionResult nextVersion(String cacheKey) {
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
            List.of("hotkey:ver:" + cacheKey),
            String.valueOf(versionKeyTtlMinutes * 60L)
          );
          return new VersionResult(v != null ? v : System.nanoTime(), false);
        } catch (Exception e) {
          log.warn("Redis version increment failed, fallback to nanoTime: {}", cacheKey, e);
          return new VersionResult(System.nanoTime(), true);
        }
      })
      .orElse(new VersionResult(System.nanoTime(), true));
  }
}
