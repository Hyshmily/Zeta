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

  /**
   * Whether the lock is still believed to be held by this handle.
   *
   * <p>A lease-based lock can be lost without the holder noticing: the TTL
   * lapses (a GC pause or scheduler starvation longer than the lease, a Redis
   * failover that drops the key, a network partition) and a peer acquires the
   * same key while this handle's critical section is still running. The
   * mutual-exclusion invariant is broken at that point, and no exception is
   * raised by {@link #close()} — releasing a lock this handle no longer owns
   * is an idempotent no-op that looks exactly like success.
   *
   * <p>Long critical sections should therefore re-check this before each
   * externally-visible step:
   *
   * <pre>{@code
   * try (AutoReleaseLock lock = hotKey.tryLock("my:key", 30, TimeUnit.SECONDS)) {
   *   if (lock == null) {
   *     return;                       // provider unavailable or contended
   *   }
   *   stepOne();
   *   if (!lock.isHeld()) {
   *     throw new IllegalStateException("lock lease lapsed mid-critical-section");
   *   }
   *   stepTwo();
   * }
   * }</pre>
   *
   * <p><b>Default:</b> {@code true} — an implementation that cannot observe
   * lease loss reports the handle as held. Callers must treat {@code false} as
   * the only authoritative answer; {@code true} means "no loss detected",
   * never "loss is impossible".
   *
   * @return {@code false} once the lease is known to have lapsed (or been
   *         stolen); {@code true} otherwise
   */
  default boolean isHeld() {
    return true;
  }
}
