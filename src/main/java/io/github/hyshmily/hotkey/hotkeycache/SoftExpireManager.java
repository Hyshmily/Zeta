package io.github.hyshmily.hotkey.hotkeycache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.hotkey.entity.CacheEntry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SoftExpireManager {

  private final Cache<String, Long> softExpireAt;
  private final Semaphore refreshLimiter;
  private final Cache<String, Object> caffeineCache;
  private final Executor executor;

  @Getter
  private final long defaultSoftTtlMs;

  public SoftExpireManager(
    Cache<String, Object> caffeineCache,
    Executor executor,
    long softTtlMs,
    int refreshConcurrency,
    int softExpireMaxSize,
    int softExpireTtlMinutes
  ) {
    this.caffeineCache = caffeineCache;
    this.executor = executor;
    this.defaultSoftTtlMs = softTtlMs;
    if (softTtlMs > 0) {
      this.softExpireAt = Caffeine.newBuilder()
        .maximumSize(softExpireMaxSize > 0 ? softExpireMaxSize : 50_000)
        .expireAfterWrite(softExpireTtlMinutes > 0 ? softExpireTtlMinutes : 60, TimeUnit.MINUTES)
        .build();
      this.refreshLimiter = new Semaphore(refreshConcurrency > 0 ? refreshConcurrency : 100);
    } else {
      this.softExpireAt = null;
      this.refreshLimiter = null;
    }
  }

  public boolean isEnabled() {
    return softExpireAt != null;
  }

  public boolean isExpired(String cacheKey) {
    if (softExpireAt == null) {
      throw new IllegalStateException("SoftExpireManager is disabled, isExpired() should not be called");
    }
    Long expireAt = softExpireAt.getIfPresent(cacheKey);
    return expireAt == null || expireAt < System.currentTimeMillis();
  }

  public void refresh(String cacheKey, long softTtlMs) {
    if (softExpireAt != null) {
      softExpireAt.put(cacheKey, System.currentTimeMillis() + softTtlMs);
    }
  }

  public void triggerAsyncRefresh(String cacheKey, Supplier<?> reader, long softTtlMs) {
    if (softExpireAt == null || !refreshLimiter.tryAcquire()) {
      if (softExpireAt != null) {
        log.debug("Refresh limiter blocked, skip async refresh: {}", cacheKey);
      }
      return;
    }

    CompletableFuture.supplyAsync(reader, executor).whenComplete((value, error) -> {
      try {
        if (error != null) {
          log.warn("Async soft refresh failed: {}", cacheKey, error);
          return;
        }
        if (value != null) {
          Object existing = caffeineCache.getIfPresent(cacheKey);
          long keepExpireAt = (existing instanceof CacheEntry ce) ? ce.getExpireAtMs() : Long.MAX_VALUE;
          caffeineCache.put(cacheKey, new CacheEntry(value, 0L, false, keepExpireAt));
          softExpireAt.put(cacheKey, System.currentTimeMillis() + softTtlMs);
        }
      } finally {
        refreshLimiter.release();
      }
    });
  }
}
