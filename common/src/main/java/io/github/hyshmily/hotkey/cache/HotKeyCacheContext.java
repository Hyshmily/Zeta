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
package io.github.hyshmily.hotkey.cache;

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
 */
public final class HotKeyCacheContext {

  /**
   * Immutable snapshot of all thread-bound cache parameters.
   *
   * @param hardTtlMs hard TTL override (0 = use default)
   * @param softTtlMs soft TTL override (0 = use default)
   * @param allowNull whether null return values may be cached
   */
  public record ContextValues(long hardTtlMs, long softTtlMs, boolean allowNull) {}

  private static final ThreadLocal<ContextValues> HOLDER = new ThreadLocal<>();
  private static final HotKeyCacheContext INSTANCE = new HotKeyCacheContext();

  private HotKeyCacheContext() {}

  /** Returns the thread-bound singleton instance. */
  public static HotKeyCacheContext get() {
    return INSTANCE;
  }

  /**
   * Sets all cache parameters for the current thread's cache operation.
   * Values of 0 mean "use global default".
   *
   * @param hardTtlMs hard TTL in ms, or 0 for default
   * @param softTtlMs soft TTL in ms, or 0 for default
   * @param allowNull whether to allow caching null values
   */
  public void apply(long hardTtlMs, long softTtlMs, boolean allowNull) {
    if (hardTtlMs > 0 || softTtlMs > 0 || allowNull) {
      HOLDER.set(new ContextValues(hardTtlMs, softTtlMs, allowNull));
    } else {
      HOLDER.remove();
    }
  }

  /** Returns the hard TTL for the current thread, or 0 if no override is set. */
  public long getHardTtlMs() {
    ContextValues v = HOLDER.get();
    return v != null ? v.hardTtlMs() : 0L;
  }

  /** Returns the soft TTL for the current thread, or 0 if no override is set. */
  public long getSoftTtlMs() {
    ContextValues v = HOLDER.get();
    return v != null ? v.softTtlMs() : 0L;
  }

  /** Returns whether null caching is enabled for the current thread. */
  public boolean isAllowNull() {
    ContextValues v = HOLDER.get();
    return v != null && v.allowNull();
  }

  /**
   * Captures a snapshot of the current thread's context values.
   * Returns {@code null} if no values are set.
   */
  @Nullable
  public ContextValues snapshot() {
    return HOLDER.get();
  }

  /**
   * Restores a previously captured snapshot. Pass {@code null} to clear.
   *
   * @param snapshot the snapshot to restore, or {@code null} to clear
   */
  public void restore(@Nullable ContextValues snapshot) {
    if (snapshot != null) {
      HOLDER.set(snapshot);
    } else {
      HOLDER.remove();
    }
  }

}
