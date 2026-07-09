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
package io.github.hyshmily.hotkey.cache.annotationsupporter;

import io.github.hyshmily.hotkey.Internal;
import jakarta.annotation.Nullable;

/**
 * Thread-bound context for propagating per-invocation cache parameters from
 * {@code @Cacheable} companion aspects into {@link HotKeySpringCache}.
 *
 * <p>Usage pattern (in aspect):
 *
 * <pre>{@code
 * ContextValues prev = HotKeyCacheContext.get().snapshot();
 * try {
 *   HotKeyCacheContext.get().apply(hardTtlMs, softTtlMs, allowNull);
 *   // proceed to CacheInterceptor
 * } finally {
 *   HotKeyCacheContext.get().restore(prev);
 * }
 * }</pre>
 *
 * @see HotKeySpringCache
 * @see NullValue
 */
@Internal
public final class HotKeyCacheContext {

  /**
   * Immutable snapshot of all thread-bound cache parameters captured at a
   * specific point in time by {@link #snapshot()}.
   * <p>
   * This recordReport carries the complete set of per-invocation overrides that
   * companion aspects (e.g., {@code @HotKeyCacheTTL}, {@code @NullCaching},
   * {@code @Broadcast}) apply before the Spring cache interceptor executes.
   * <p>
   * A value of {@code 0} for {@code hardTtlMs} or {@code softTtlMs} means
   * "use the global default" rather than "no expiry".
   *
   * @param hardTtlMs    hard TTL override in milliseconds (0 = use global default)
   * @param softTtlMs    soft TTL override in milliseconds (0 = use global default)
   * @param allowNull    whether {@code null} return values may be cached in L1
   * @param skipBroadcast whether to skip broadcasting sync messages to peers
   */
  public record ContextValues(long hardTtlMs, long softTtlMs, boolean allowNull, boolean skipBroadcast) {}

  private static final ThreadLocal<ContextValues> HOLDER = new ThreadLocal<>();
  private static final HotKeyCacheContext INSTANCE = new HotKeyCacheContext();

  private HotKeyCacheContext() {}

  /**
   * Returns the thread-bound singleton instance of the cache context.
   * <p>
   * Each thread maintains its own independent context via {@link ThreadLocal},
   * allowing concurrent cache operations with different per-invocation parameters
   * (TTL overrides, null-caching flag) without interference.
   *
   * @return the singleton {@link HotKeyCacheContext} instance
   */
  public static HotKeyCacheContext get() {
    return INSTANCE;
  }

  /**
   * Sets all cache parameters for the current thread's cache operation.
   * <p>
   * Typically called by companion aspects (e.g., {@code @HotKeyCacheTTL},
   * {@code @NullCaching}) before proceeding to the Spring cache interceptor.
   * Values of {@code 0} for TTL parameters mean "use the global default".
   * <p>
   * If all parameters are at their default values (TTLs are {@code 0} and
   * {@code allowNull} is {@code false}), the thread-local context is cleared
   * to avoid unnecessary storage.
   *
   * @param hardTtlMs    hard TTL override in milliseconds, or {@code 0} for default
   * @param softTtlMs    soft TTL override in milliseconds, or {@code 0} for default
   * @param allowNull    whether to allow caching null values in L1
   * @param skipBroadcast whether to skip broadcasting sync messages to peers
   */
  public void apply(long hardTtlMs, long softTtlMs, boolean allowNull, boolean skipBroadcast) {
    if (hardTtlMs > 0 || softTtlMs > 0 || allowNull || skipBroadcast) {
      HOLDER.set(new ContextValues(hardTtlMs, softTtlMs, allowNull, skipBroadcast));
    } else {
      HOLDER.remove();
    }
  }

  /**
   * Returns the current thread's context values, or {@code null} if no
   * overrides are active.
   * <p>
   * Prefer this single call over accessing individual getters when multiple
   * fields are needed — it avoids repeated {@link ThreadLocal} lookups.
   *
   * @return the current {@link ContextValues}, or {@code null} for defaults
   */
  @Nullable
  public ContextValues getValues() {
    return HOLDER.get();
  }

  /**
   * Returns the hard TTL override (in milliseconds) active for the current
   * thread's cache operation.
   * <p>
   * A return value of {@code 0} indicates that no override is active and the
   * globally configured default hard TTL should be used instead.
   *
   * @return the hard TTL override in milliseconds, or {@code 0} for default
   */
  public long getHardTtlMs() {
    ContextValues v = getValues();
    return v != null ? v.hardTtlMs() : 0L;
  }

  /**
   * Returns the soft TTL override (in milliseconds) active for the current
   * thread's cache operation.
   * <p>
   * A return value of {@code 0} indicates that no override is active and the
   * globally configured default soft TTL should be used instead.
   *
   * @return the soft TTL override in milliseconds, or {@code 0} for default
   */
  public long getSoftTtlMs() {
    ContextValues v = getValues();
    return v != null ? v.softTtlMs() : 0L;
  }

  /**
   * Returns whether null-value caching is enabled for the current thread's
   * cache operation.
   * <p>
   * When enabled ({@code true}), a cache miss that produces a {@code null} value
   * is stored via a {@link NullValue} sentinel in the L1 cache. Subsequent
   * lookups will return {@code null} without invoking the value loader again.
   *
   * @return {@code true} if null caching is enabled for this thread
   */
  public boolean isAllowNull() {
    ContextValues v = getValues();
    return v != null && v.allowNull();
  }

  /**
   * Returns whether send of sync messages is suppressed for the current
   * thread's cache operation.
   * <p>
   * When {@code true}, cache write/evict operations use local-only variants
   * ({@code putLocal()} / {@code invalidateLocal()}) and do not send sync messages
   * to peer instances via RabbitMQ.
   *
   * @return {@code true} if send is suppressed for this thread
   */
  public boolean isSkipBroadcast() {
    ContextValues v = getValues();
    return v != null && v.skipBroadcast();
  }

  /**
   * Captures a snapshot of the current thread's context values for later restoration.
   * <p>
   * Returns {@code null} if no override values are currently active, indicating
   * that global defaults will be used. This is typically called at the start
   * of an aspect's {@code proceed()} to preserve the caller's context.
   *
   * @return the current {@link ContextValues}, or {@code null} if no overrides are active
   */
  @Nullable
  public ContextValues snapshot() {
    return getValues();
  }

  /**
   * Restores a previously captured snapshot, returning the thread's cache
   * parameters to a prior state.
   * <p>
   * This is typically used in a {@code finally} block after an aspect's
   * {@code proceed()} call to ensure the caller's context is not polluted
   * with overrides applied by the aspect.
   * <p>
   * Pass {@code null} to clear all current overrides (reverting to global defaults).
   *
   * @param snapshot the snapshot to restore, or {@code null} to clear all overrides
   */
  public void restore(@Nullable ContextValues snapshot) {
    if (snapshot != null) {
      HOLDER.set(snapshot);
    } else {
      HOLDER.remove();
    }
  }
}
