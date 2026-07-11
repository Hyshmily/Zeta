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
 */
@Internal
public interface LockProvider {
  /**
   * Attempt to acquire a distributed lock with the provider's default retry
   * counts.
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
