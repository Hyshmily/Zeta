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
package io.github.hyshmily.zeta.sync.distributedlock.impl;

import static io.github.hyshmily.zeta.cache.cachesupport.CacheKeysPolicy.invalidCacheKey;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.sync.distributedlock.AutoReleaseLock;
import io.github.hyshmily.zeta.sync.distributedlock.LockProvider;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

/**
 * Redis-backed {@link LockProvider} using {@code SET NX PX} with UUID-based
 * safe release and configurable retry.
 *
 * <p>Validation and fallback are concentrated in the implementation:
 * <ul>
 *   <li>{@link #tryLock(String, long, TimeUnit)} — uses configured defaults</li>
 *   <li>{@link #tryLock(String, long, TimeUnit, int, int, int)} — validates
 *       each count; negative values fall back to defaults with a warning</li>
 * </ul>
 *
 * <p>Algorithm (JetCache-inspired):
 * <ol>
 *   <li>{@code SET key uuid NX PX ttl} — atomically acquire with expiry</li>
 *   <li>On transient failure ({@code null}) — {@code GET} to inquire;
 *       if the value is our UUID the lock is considered acquired
 *       (optimistic recovery)</li>
 *   <li>{@code close()} — Lua {@code GET + DEL} with UUID comparison
 *       for safe release, with retry on transient Redis errors</li>
 * </ol>
 */
@Slf4j
@RequiredArgsConstructor
@Internal
public class RedisLockProvider implements LockProvider {

  private static final DefaultRedisScript<Long> UNLOCK_SCRIPT = new DefaultRedisScript<>(
    "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end",
    Long.class
  );

  private static String lockId() {
    return (
      Long.toHexString(ThreadLocalRandom.current().nextLong()) +
      Long.toHexString(ThreadLocalRandom.current().nextLong())
    );
  }

  private final StringRedisTemplate redisTemplate;
  private final int defaultLockCount;
  private final int defaultInquiryCount;
  private final int defaultUnlockCount;

  @Override
  public AutoReleaseLock tryLock(String key, long expire, TimeUnit unit) {
    return acquireLock(key, expire, unit, defaultLockCount, defaultInquiryCount, defaultUnlockCount);
  }

  @Override
  public AutoReleaseLock tryLock(
    String key,
    long expire,
    TimeUnit unit,
    int lockCount,
    int inquiryCount,
    int unlockCount
  ) {
    int effectiveLock = lockCount >= 0 ? lockCount : defaultLockCount;
    int effectiveInquiry = inquiryCount >= 0 ? inquiryCount : defaultInquiryCount;
    int effectiveUnlock = unlockCount >= 0 ? unlockCount : defaultUnlockCount;

    if (lockCount < 0 || inquiryCount < 0 || unlockCount < 0) {
      log.warn(
        "Invalid lock counts [{}, {}, {}], fallback to defaults [{}, {}, {}]",
        lockCount,
        inquiryCount,
        unlockCount,
        effectiveLock,
        effectiveInquiry,
        effectiveUnlock
      );
    }

    return acquireLock(key, expire, unit, effectiveLock, effectiveInquiry, effectiveUnlock);
  }

  /**
   * Core lock acquisition logic with exponential backoff between retries.
   *
   * <p>Algorithm:
   * <ol>
   *   <li>Atomically {@code SET key uuid NX PX ttl}</li>
   *   <li>On transient failure ({@code null}) — single {@code GET} to inquire;
   *       if the value is our UUID the lock is considered acquired
   *       (optimistic recovery)</li>
   *   <li>Between retries, sleep with exponential backoff
   *       ({@code min(10ms × 2^i, 100ms)})</li>
   * </ol>
   *
   * @param key           the lock key
   * @param expire        lock TTL duration
   * @param unit          time unit for {@code expire}
   * @param lockCount     SET NX retries
   * @param inquiryCount  GET inquiries after transient failure;
   *                      continues retrying while {@code value == null}
   * @param unlockCount   DEL retries on release
   * @return a handle if acquired, or {@code null} on failure
   */
  @SuppressWarnings("all")
  private AutoReleaseLock acquireLock(
    String key,
    long expire,
    TimeUnit unit,
    int lockCount,
    int inquiryCount,
    int unlockCount
  ) {
    if (invalidCacheKey(key)) {
      return null;
    }

    String lockKey = ZetaConstants.REDIS_LOCK_KEY_PREFIX + key;
    String uuid = lockId();
    long expireMs = unit.toMillis(expire);
    long expireTimestamp = System.currentTimeMillis() + expireMs;

    for (int i = 0; i < lockCount; i++) {
      if (i > 0) {
        try {
          Thread.sleep(Math.min(10L * (1L << i), 100L));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return null;
        }
      }

      try {
        Boolean acquired = redisTemplate.opsForValue().setIfAbsent(lockKey, uuid, Duration.ofMillis(expireMs));

        if (Boolean.TRUE.equals(acquired)) {
          if (i > 0) {
            log.debug("Lock '{}' acquired after {} retries", key, i);
          }
          return new RedisLockHandle(redisTemplate, lockKey, uuid, expireTimestamp, unlockCount);
        }

        if (acquired == null) {
          for (int j = 0; j < inquiryCount; j++) {
            String value = redisTemplate.opsForValue().get(lockKey);

            if (uuid.equals(value)) {
              if (i > 0) {
                log.debug("Lock '{}' acquired after {} retries (inquiry path)", key, i);
              }
              return new RedisLockHandle(redisTemplate, lockKey, uuid, expireTimestamp, unlockCount);
            }
            if (value != null) {
              break;
            }
          }
        }
      } catch (RedisSystemException e) {
        log.error("Redis system error occurred while acquiring lock '{}'", key, e);
      }
    }
    log.warn("Failed to acquire lock '{}' after {} attempts", key, lockCount);
    return null;
  }

  @RequiredArgsConstructor
  @Slf4j
  public static class RedisLockHandle implements AutoReleaseLock {

    private final StringRedisTemplate redisTemplate;
    private final String lockKey;
    private final String uuid;
    private final long expireTimestamp;
    private final int unlockCount;

    /**
     * Release the lock atomically.
     *
     * <p>Uses Lua {@code GET + DEL} to ensure only the lock owner can
     * delete the key.  Retries up to {@code unlockCount} times on
     * transient Redis failures.  Silently skips when the lock has
     * already expired (Redis TTL handles cleanup).
     */
    @Override
    @SuppressWarnings("all")
    public void close() {
      for (int i = 0; i < unlockCount; i++) {
        if (System.currentTimeMillis() < expireTimestamp) {
          if (i > 0) {
            try {
              Thread.sleep(1L);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            }
          }
          try {
            Long result = redisTemplate.execute(UNLOCK_SCRIPT, List.of(lockKey), uuid);
            if (Objects.equals(result, 1L)) {
              return;
            }
          } catch (RedisSystemException e) {
            log.warn("Redis unlock attempt {}/{} failed for key {}", i + 1, unlockCount, lockKey, e);
          }
        } else {
          return;
        }
      }
      log.warn("Failed to release lock '{}' after {} attempts; lock may persist until TTL", lockKey, unlockCount);
    }
  }
}
