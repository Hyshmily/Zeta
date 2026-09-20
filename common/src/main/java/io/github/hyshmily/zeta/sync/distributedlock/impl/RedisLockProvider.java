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
import io.github.hyshmily.zeta.util.LogThrottle;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

/**
 * Redis-backed {@link LockProvider} using {@code SET NX PX} with a random
 * 128-bit hex lock token for safe release, configurable retry, and optional
 * watchdog auto-renewal.
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
 *   <li>{@link RedisLockHandle} starts a <b>watchdog</b> that renews the
 *       TTL via Lua {@code GET + PEXPIRE}, keeping the lock alive while the
 *       caller holds the handle</li>
 *   <li>{@code close()} — Lua {@code GET + DEL} with UUID comparison
 *       for safe release, with retry on transient Redis errors</li>
 * </ol>
 *
 * <p><b>Watchdog semantics:</b> the watchdog is active by default (it is
 * disabled only by passing a {@code null} scheduler to the constructor) and
 * renews every {@code max(expireMs / 3, 1s)} milliseconds, clamped down to
 * {@code expireMs / 2} for sub-second TTLs and floored at
 * {@value #MIN_WATCHDOG_INTERVAL_MS}ms to bound the Redis renewal rate. It
 * runs from acquisition until
 * {@link RedisLockHandle#close()} cancels it. Renewal is conditional on the
 * caller's token (Lua {@code GET + PEXPIRE}), so a stolen or released lock is
 * never renewed. <b>A handle that is never closed keeps the lock alive
 * indefinitely</b> — the watchdog renews past every TTL for the lifetime of
 * the JVM (the scheduler's threads are daemons, so on JVM exit the lock
 * expires after one final TTL). Always release in a {@code finally} block or
 * via try-with-resources.
 *
 * <p><b>Not reentrant:</b> a second {@code tryLock} on the same key fails even
 * when the same caller already holds the lock (see {@link LockProvider}).
 */
@Slf4j
@Internal
public class RedisLockProvider implements LockProvider {

  private static final DefaultRedisScript<Long> UNLOCK_SCRIPT = new DefaultRedisScript<>(
    "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end",
    Long.class
  );

  private static final DefaultRedisScript<Long> RENEW_SCRIPT = new DefaultRedisScript<>(
    "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) else return 0 end",
    Long.class
  );

  /**
   * Floor for the watchdog renewal interval. Without it, a pathological TTL
   * (e.g. 4ms) degraded to a 2ms renewal cadence — 500 PEXPIRE/s per held
   * lock, flooding Redis. A TTL too short for this floor is warned about at
   * handle construction: the lock still works for the caller's critical
   * section, but background renewal can no longer be guaranteed before expiry.
   */
  private static final long MIN_WATCHDOG_INTERVAL_MS = 50;

  /**
   * Generates a random 128-bit hex lock token using {@link ThreadLocalRandom}.
   *
   * <p>Two consecutive {@code long} values from {@code ThreadLocalRandom} are
   * hex-encoded and concatenated, producing a 128-bit token (32 hex chars).
   * This is <em>not</em> a UUID (no version/variant bits), but the collision
   * probability is negligible for the lock-token use case.
   *
   * @return a 32-character hex string suitable as a lock token
   */
  private static String lockId() {
    return (
      String.format("%016x", ThreadLocalRandom.current().nextLong()) +
      String.format("%016x", ThreadLocalRandom.current().nextLong())
    );
  }

  private final StringRedisTemplate redisTemplate;
  private final int defaultLockCount;
  private final int defaultInquiryCount;
  private final int defaultUnlockCount;
  private final ScheduledExecutorService scheduler;

  /**
   * Rate-limits the acquire-path Redis error. Widening the catch to
   * {@link DataAccessException} means this branch now fires on the <em>most
   * common</em> failure (Redis unreachable during an outage) instead of only on
   * the rarest one, and {@code tryLock} is a per-request API — so an unthrottled
   * ERROR here would emit one line per attempt (up to {@code lockCount}) per
   * call for the whole outage. Counts suppressed lines so the next emitted line
   * carries them; repo standard, see {@link LogThrottle}.
   */
  private final LogThrottle.Counting acquireFailureLog = new LogThrottle.Counting();

  /**
   * Constructs a {@code RedisLockProvider} with the given template and default retry counts.
   *
   * @param redisTemplate         the Redis template for executing lock commands; must not be null
   * @param defaultLockCount      default number of SET NX attempts
   * @param defaultInquiryCount   default number of GET inquiries after transient failure
   * @param defaultUnlockCount    default number of DEL retries on release
   * @param scheduler             executor for watchdog renewal tasks; may be null to disable watchdog
   */
  public RedisLockProvider(
    StringRedisTemplate redisTemplate,
    int defaultLockCount,
    int defaultInquiryCount,
    int defaultUnlockCount,
    ScheduledExecutorService scheduler
  ) {
    this.redisTemplate = redisTemplate;
    this.defaultLockCount = defaultLockCount;
    this.defaultInquiryCount = defaultInquiryCount;
    this.defaultUnlockCount = defaultUnlockCount;
    this.scheduler = scheduler;
  }

  /**
   * Attempts to acquire a lock using the configured default retry counts.
   *
   * @param key    the lock key; must not be null or blank
   * @param expire lock TTL duration
   * @param unit   time unit for {@code expire}
   * @return a handle if acquired, or {@code null} on failure
   */
  @Override
  public AutoReleaseLock tryLock(String key, long expire, TimeUnit unit) {
    return acquireLock(key, expire, unit, defaultLockCount, defaultInquiryCount, defaultUnlockCount);
  }

  /**
   * Attempts to acquire a lock with explicit retry counts, falling back to
   * defaults when any count is negative.
   *
   * @param key           the lock key; must not be null or blank
   * @param expire        lock TTL duration
   * @param unit          time unit for {@code expire}
   * @param lockCount     SET NX retries; negative falls back to default
   * @param inquiryCount  GET inquiries after transient failure; negative falls back to default
   * @param unlockCount   DEL retries on release; negative falls back to default
   * @return a handle if acquired, or {@code null} on failure
   */
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

    String lockKey = ZetaConstants.Redis.LOCK_KEY_PREFIX + key;
    String uuid = lockId();
    long expireMs = unit.toMillis(expire);

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
          return new RedisLockHandle(redisTemplate, lockKey, uuid, unlockCount, scheduler, expireMs);
        }

        if (acquired == null) {
          for (int j = 0; j < inquiryCount; j++) {
            if (j > 0) {
              try {
                Thread.sleep(Math.min(2L * (1L << j), 10L));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
              }
            }
            String value = redisTemplate.opsForValue().get(lockKey);

            if (uuid.equals(value)) {
              if (i > 0) {
                log.debug("Lock '{}' acquired after {} retries (inquiry path)", key, i);
              }
              return new RedisLockHandle(redisTemplate, lockKey, uuid, unlockCount, scheduler, expireMs);
            }
            if (value != null) {
              break;
            }
          }
        }
      } catch (DataAccessException e) {
        // DataAccessException, not the narrower RedisSystemException: connection
        // failures and command timeouts (RedisConnectionFailureException,
        // QueryTimeoutException) are SIBLINGS of RedisSystemException under
        // DataAccessException, not subclasses — catching only RedisSystemException
        // let exactly the most common failure (Redis unreachable) escape and
        // break tryLock's documented "returns null when unavailable" contract.
        LogThrottle.Counting.Attempt attempt = acquireFailureLog.record();
        if (!attempt.admitted()) {
          log.debug("Redis access error while acquiring lock '{}' ({} in current window)", key, attempt.count(), e);
        } else if (attempt.count() > 0) {
          log.error(
            "Redis access error while acquiring lock '{}' ({} suppressed in the last window)",
            key,
            attempt.count(),
            e
          );
        } else {
          log.error("Redis access error occurred while acquiring lock '{}'", key, e);
        }
      }
    }
    // tryLock is a try-style API: returning null because another caller holds
    // the lock is its NORMAL outcome under contention, not an operational risk —
    // log at DEBUG so a contended workload does not flood the log at try-rate.
    log.debug("Failed to acquire lock '{}' after {} attempts", key, lockCount);
    return null;
  }

  /**
   * Handle for an acquired Redis lock with watchdog auto-renewal.
   *
   * <p>Starts a watchdog upon construction that periodically extends the lock
   * TTL via Lua {@code GET + PEXPIRE}. The watchdog runs for as long as the
   * handle lives — every {@code max(expireMs / 3, 1s)} milliseconds, clamped
   * down to {@code expireMs / 2} for sub-second TTLs — and is cancelled when
   * {@link #close()} is called. Renewal is conditional on this handle's UUID,
   * so a stolen or already-released lock is never renewed (the watchdog then
   * silently stops extending). <b>Consequence:</b> a leaked handle (never
   * closed) keeps the lock alive indefinitely — always release in a
   * {@code finally} block or via try-with-resources.
   *
   * <p>Release uses Lua {@code GET + DEL} with UUID comparison for safe,
   * idempotent unlocking — no client-clock-based guard is needed.
   */
  @Slf4j
  public static class RedisLockHandle implements AutoReleaseLock {

    private final StringRedisTemplate redisTemplate;
    private final String lockKey;
    private final String uuid;
    private final int unlockCount;
    private final ScheduledExecutorService scheduler;
    private final long expireMs;

    /**
     * Rate-limits the release-path WARN. A Redis outage now reaches this branch
     * (widened catch) and every {@code close()} would otherwise emit one line per
     * unlock attempt plus the summary — see {@link #acquireFailureLog}.
     */
    private final LogThrottle.Counting unlockFailureLog = new LogThrottle.Counting();

    @SuppressWarnings("java:S3077")
    private volatile ScheduledFuture<?> watchdogTask;

    /**
     * Creates a new lock handle and starts the watchdog renewal.
     *
     * @param redisTemplate  the Redis template for unlock and renewal commands
     * @param lockKey        the full Redis key (with prefix)
     * @param uuid           the unique lock token (128-bit hex)
     * @param unlockCount    number of retries for the unlock Lua script
     * @param scheduler      executor for the watchdog task; may be null (watchdog disabled)
     * @param expireMs       lock TTL in milliseconds, used for watchdog renewal interval
     */
    public RedisLockHandle(
      StringRedisTemplate redisTemplate,
      String lockKey,
      String uuid,
      int unlockCount,
      ScheduledExecutorService scheduler,
      long expireMs
    ) {
      this.redisTemplate = redisTemplate;
      this.lockKey = lockKey;
      this.uuid = uuid;
      this.unlockCount = unlockCount;
      this.scheduler = scheduler;
      this.expireMs = expireMs;
      startWatchdog();
    }

    /**
     * Start a background watchdog that renews the lock TTL every
     * {@code expireMs / 3} (minimum 1 second).  The watchdog uses Lua
     * {@code GET + PEXPIRE} so it only extends the key when the UUID
     * matches — if the lock has been stolen (lost due to partition, etc.)
     * the renewal silently stops.
     *
     * <p>The watchdog is cancelled when {@link #close()} is called.
     */
    private void startWatchdog() {
      if (scheduler == null) {
        return;
      }
      // The renewal interval must be strictly shorter than the lock TTL, or
      // the lock expires before the first renewal and a second client can
      // acquire it while the first still holds its handle (broken mutual
      // exclusion for sub-second TTLs — the old max(expireMs/3, 1000) floor
      // exceeded the TTL whenever expireMs < 1000). The clamp keeps the
      // interval at expireMs/2 for very short TTLs instead of the 1s floor,
      // and the {@value #MIN_WATCHDOG_INTERVAL_MS}ms floor bounds the Redis
      // renewal rate (a 2ms floor used to renew a 4ms lock 500×/s). A TTL
      // below the floor cannot be renewed reliably and is warned about.
      long interval = Math.max(
        MIN_WATCHDOG_INTERVAL_MS,
        Math.min(Math.max(expireMs / 3, 1000L), expireMs / 2)
      );
      if (interval >= expireMs) {
        log.warn(
          "Lock TTL {}ms is below the {}ms watchdog renewal floor for key {}; the lock may expire before renewal",
          expireMs,
          interval,
          lockKey
        );
      }
      watchdogTask = scheduler.scheduleWithFixedDelay(
        () -> {
          try {
            redisTemplate.execute(RENEW_SCRIPT, List.of(lockKey), uuid, String.valueOf(expireMs));
          } catch (Exception e) {
            log.warn("Lock renew failed for {}: {}", lockKey, e.toString());
          }
        },
        interval,
        interval,
        TimeUnit.MILLISECONDS
      );
    }

    /**
     * Release the lock atomically.
     *
     * <p>Cancels the watchdog first, then uses Lua {@code GET + DEL} to
     * ensure only the lock owner can delete the key.  Retries up to
     * {@code unlockCount} times on transient Redis failures.  The Lua
     * script is idempotent (only deletes when {@code GET == uuid}), so
     * no client-clock-based guard is needed.
     */
    @Override
    @SuppressWarnings("all")
    public void close() {
      if (watchdogTask != null) {
        watchdogTask.cancel(false);
      }
      for (int i = 0; i < unlockCount; i++) {
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
          if (result != null) {
            // 1 = our lock was deleted; 0 = the lock was already gone or
            // stolen (UUID mismatch). Either way there is nothing left to
            // release, so this is idempotent success — retrying a 0 would
            // keep returning 0 and end in a misleading "Failed to release"
            // WARN. Only a null result (no information) falls through to
            // the retry loop.
            return;
          }
        } catch (DataAccessException e) {
          // Same widened catch as the acquire path: a connection-level failure on
          // release must fall into the retry loop (and finally the "lock may
          // persist until TTL" WARN), not escape close() — AutoReleaseLock's
          // contract is idempotent, best-effort release. Per-attempt detail is
          // DEBUG because a Redis outage would otherwise emit one WARN per attempt
          // per close(); the summary below is the rate-limited line.
          log.debug("Redis unlock attempt {}/{} failed for key {}", i + 1, unlockCount, lockKey, e);
        }
      }
      LogThrottle.Counting.Attempt releaseAttempt = unlockFailureLog.record();
      if (!releaseAttempt.admitted()) {
        log.debug("Failed to release lock '{}' after {} attempts (WARN suppressed this window)", lockKey, unlockCount);
      } else if (releaseAttempt.count() > 0) {
        log.warn(
          "Failed to release lock '{}' after {} attempts; lock may persist until TTL ({} suppressed since the last WARN)",
          lockKey,
          unlockCount,
          releaseAttempt.count()
        );
      } else {
        log.warn("Failed to release lock '{}' after {} attempts; lock may persist until TTL", lockKey, unlockCount);
      }
    }
  }
}
