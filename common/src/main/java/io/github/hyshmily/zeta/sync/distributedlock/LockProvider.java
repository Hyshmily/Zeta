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
package io.github.hyshmily.zeta.sync.distributedlock;

import io.github.hyshmily.zeta.Internal;
import java.util.concurrent.TimeUnit;

/**
 * Provider for distributed locks backed by a key-value store (typically Redis).
 *
 * <p>Two overloads of {@link #tryLock}: the basic variant uses provider-configured
 * defaults; the extended variant accepts explicit retry counts (the
 * implementation validates and falls back to defaults when parameters are
 * illegal).
 *
 * <p><b>Not reentrant.</b> Locks are keyed exclusively by the lock key: a
 * second {@code tryLock} on the same key <b>fails</b> even when the caller
 * (same thread, same JVM) already holds the lock. Callers that need nested or
 * repeated acquisition within one critical section must track their own
 * holding state around a single acquired handle.
 *
 * <p><b>Watchdog auto-renewal (implementation-specific).</b> The reference
 * {@link io.github.hyshmily.zeta.sync.distributedlock.impl.RedisLockProvider}
 * implementation starts a <b>watchdog</b> on every acquired handle that
 * periodically re-arms the lock TTL — the lock does <i>not</i> expire
 * unconditionally after {@code expire} while the handle is alive:
 * <ul>
 *   <li><b>Renewal cadence:</b> the TTL is renewed every
 *       {@code max(expireMs / 3, 1s)} milliseconds, clamped down to
 *       {@code expireMs / 2} for sub-second TTLs so the renewal always lands
 *       strictly before expiry, and floored at a small constant to bound the
 *       Redis renewal rate (a TTL below that floor is warned about). Renewal
 *       is conditional (Lua {@code GET +
 *       PEXPIRE} on the caller's token): if the lock was stolen or already
 *       released, the renewal silently stops.</li>
 *   <li><b>Lifetime:</b> the watchdog runs from handle acquisition until
 *       {@link AutoReleaseLock#close()} is called — it is cancelled as the
 *       first step of release.</li>
 *   <li><b>Leaked-handle consequence:</b> a handle that is never closed keeps
 *       the lock alive <em>indefinitely</em> (the watchdog renews past every
 *       TTL for the lifetime of the JVM; the watchdog thread is a daemon, so
 *       on JVM exit the lock expires after one final TTL). Callers MUST
 *       release in a {@code finally} block (or use try-with-resources —
 *       {@link AutoReleaseLock} extends {@link AutoCloseable}).</li>
 * </ul>
 *
 * <p>Because renewal semantics are part of the implementation contract, custom
 * {@code LockProvider} implementations should document their own watchdog
 * behaviour (or its absence) explicitly.
 */
@Internal
public interface LockProvider {
  /**
   * Attempt to acquire a distributed lock with the provider's default retry
   * counts.
   *
   * <p><b>Watchdog:</b> unless the implementation documents otherwise, the
   * returned handle renews its TTL in the background until
   * {@link AutoReleaseLock#close()} — always release the handle, otherwise the
   * lock never expires (see the interface Javadoc for the exact renewal
   * cadence and the leaked-handle consequence).
   *
   * @param key    the lock key (never {@code null})
   * @param expire the time-to-live for the lock
   * @param unit   the time unit for {@code expire}
   * @return a {@link AutoReleaseLock} if acquired, or {@code null} if the
   *         lock is held by another caller or the provider is unavailable
   */
  AutoReleaseLock tryLock(String key, long expire, TimeUnit unit);

  /**
   * Attempt to acquire a distributed lock with explicit retry counts.
   *
   * <p><b>Watchdog:</b> same semantics as {@link #tryLock(String, long,
   * TimeUnit)} — the returned handle renews its TTL in the background until
   * {@link AutoReleaseLock#close()}; always release the handle.
   *
   * <p>The default implementation ignores the extra parameters and delegates
   * to {@link #tryLock(String, long, TimeUnit)}.  Implementations that
   * support dynamic retry should override this method and apply their own
   * validation / fallback logic.
   *
   * @param key           the lock key (never {@code null})
   * @param expire        the time-to-live for the lock
   * @param unit          the time unit for {@code expire}
   * @param lockCount     the number of {@code SET NX} retries
   * @param inquiryCount  the number of {@code GET} inquiries after a transient failure
   * @param unlockCount   the number of {@code DEL} retries
   * @return a {@link AutoReleaseLock} if acquired, or {@code null} if the
   *         lock is held by another caller or the provider is unavailable
   */
  default AutoReleaseLock tryLock(
    String key,
    long expire,
    TimeUnit unit,
    int lockCount,
    int inquiryCount,
    int unlockCount
  ) {
    return tryLock(key, expire, unit);
  }
}
