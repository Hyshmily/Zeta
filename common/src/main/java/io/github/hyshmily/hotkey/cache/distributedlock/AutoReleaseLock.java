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
package io.github.hyshmily.hotkey.cache.distributedlock;

/**
 * A distributed lock handle that is automatically released when the
 * {@code try-with-resources} block exits.
 *
 * <p>Obtained from {@link LockProvider#tryLock}.  The lock is released
 * (via {@link #close}) on normal exit or exception — there is no
 * explicit {@code unlock()} method.
 *
 * <p>Usage:
 * <pre>{@code
 * try (AutoReleaseLock lock = hotKey.tryLock("my:key", 5, TimeUnit.SECONDS)) {
 *     if (lock != null) {
 *         // critical section
 *     }
 * }
 * }</pre>
 */
@FunctionalInterface
public interface AutoReleaseLock extends AutoCloseable {
  /**
   * Release the lock.  Idempotent — safe to call multiple times.
   * Implementations should retry on transient Redis failures.
   */
  @Override
  void close();
}
