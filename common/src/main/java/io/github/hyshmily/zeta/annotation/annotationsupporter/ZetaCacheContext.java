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
package io.github.hyshmily.zeta.annotation.annotationsupporter;

import io.github.hyshmily.zeta.Internal;
import jakarta.annotation.Nullable;
import java.util.function.LongSupplier;

/**
 * Thread-bound context for propagating per-invocation cache parameters from
 * {@code @Cacheable} companion aspects into {@link ZetaSpringCache}.
 *
 * <p>Usage pattern (in aspect):
 *
 * <pre>{@code
 * ContextValues prev = ZetaCacheContext.get().snapshot();
 * try {
 *   ZetaCacheContext.get().apply(hardTtlMs, softTtlMs, allowNull, skipBroadcast, ...);
 *   // proceed to CacheInterceptor
 * } finally {
 *   ZetaCacheContext.get().restore(prev);
 * }
 * }</pre>
 *
 * @see ZetaSpringCache
 * @see NullValue
 */
@Internal
public final class ZetaCacheContext {

  /**
   * Immutable snapshot of all thread-bound cache parameters captured at a
   * specific point in time by {@link #snapshot()}.
   *
   * @param hardTtlMs       hard TTL override in milliseconds (0 = use global default)
   * @param softTtlMs       soft TTL override in milliseconds (0 = use global default)
   * @param allowNull       whether {@code null} return values may be cached in L1
   * @param skipBroadcast   whether to skip broadcasting sync messages to peers
   * @param skipDetection   whether to skip hot-key detection and Worker reporting
   */
  public record ContextValues(
    long hardTtlMs,
    long softTtlMs,
    boolean allowNull,
    boolean skipBroadcast,
    boolean skipDetection
  ) {}

  private static final ThreadLocal<ContextValues> HOLDER = new ThreadLocal<>();
  private static final ThreadLocal<LongSupplier> HARD_TTL_SUPPLIER = new ThreadLocal<>();
  private static final ThreadLocal<LongSupplier> SOFT_TTL_SUPPLIER = new ThreadLocal<>();
  private static final ZetaCacheContext INSTANCE = new ZetaCacheContext();

  private ZetaCacheContext() {}

  /**
   * Returns the thread-bound singleton instance of the cache context.
   *
   * @return the singleton {@link ZetaCacheContext} instance
   */
  public static ZetaCacheContext get() {
    return INSTANCE;
  }

  /**
   * Sets all cache parameters for the current thread's cache operation.
   * If all parameters are at their default values, the thread-local context
   * is cleared to avoid unnecessary storage.
   */
  public void apply(
    long hardTtlMs,
    long softTtlMs,
    boolean allowNull,
    boolean skipBroadcast,
    boolean skipDetection
  ) {
    HARD_TTL_SUPPLIER.remove();
    SOFT_TTL_SUPPLIER.remove();
    if (
      hardTtlMs > 0 ||
      softTtlMs > 0 ||
      allowNull ||
      skipBroadcast ||
      skipDetection
    ) {
      HOLDER.set(
        new ContextValues(hardTtlMs, softTtlMs, allowNull, skipBroadcast, skipDetection)
      );
    } else {
      HOLDER.remove();
    }
  }

  /**
   * Sets cache parameters with lazy SpEL evaluation. The suppliers are
   * evaluated only when {@link #getHardTtlMs()} / {@link #getSoftTtlMs()}
   * are actually called — which happens only on cache miss.
   */
  public void applyLazy(
    long hardTtlMs,
    long softTtlMs,
    @Nullable LongSupplier hardTtlSupplier,
    @Nullable LongSupplier softTtlSupplier,
    boolean allowNull,
    boolean skipBroadcast,
    boolean skipDetection
  ) {
    if (hardTtlSupplier != null) HARD_TTL_SUPPLIER.set(hardTtlSupplier);
    if (softTtlSupplier != null) SOFT_TTL_SUPPLIER.set(softTtlSupplier);
    boolean hasStatic = hardTtlMs > 0 || softTtlMs > 0;
    if (hasStatic || allowNull || skipBroadcast || skipDetection) {
      HOLDER.set(
        new ContextValues(hardTtlMs, softTtlMs, allowNull, skipBroadcast, skipDetection)
      );
    } else {
      HOLDER.remove();
    }
  }

  /**
   * Returns the current thread's context values, or {@code null} if no
   * overrides are active.
   */
  @Nullable
  public ContextValues getValues() {
    return HOLDER.get();
  }

  /** Returns the hard TTL override, or 0 if none is active. Lazily resolves SpEL on first call. */
  public long getHardTtlMs() {
    LongSupplier s = HARD_TTL_SUPPLIER.get();
    if (s != null) {
      long val = s.getAsLong();
      HARD_TTL_SUPPLIER.remove();
      return val;
    }
    ContextValues v = getValues();
    return v != null ? v.hardTtlMs() : 0L;
  }

  /** Returns the soft TTL override, or 0 if none is active. Lazily resolves SpEL on first call. */
  public long getSoftTtlMs() {
    LongSupplier s = SOFT_TTL_SUPPLIER.get();
    if (s != null) {
      long val = s.getAsLong();
      SOFT_TTL_SUPPLIER.remove();
      return val;
    }
    ContextValues v = getValues();
    return v != null ? v.softTtlMs() : 0L;
  }

  /** Returns whether null-value caching is enabled. */
  public boolean isAllowNull() {
    ContextValues v = getValues();
    return v != null && v.allowNull();
  }

  /** Returns whether broadcast sync messages are suppressed. */
  public boolean isSkipBroadcast() {
    ContextValues v = getValues();
    return v != null && v.skipBroadcast();
  }

  /** Returns whether hot-key detection and Worker reporting are skipped. */
  public boolean isSkipDetection() {
    ContextValues v = getValues();
    return v != null && v.skipDetection();
  }

  /** Captures a snapshot of the current thread's context values for later restoration. */
  @Nullable
  public ContextValues snapshot() {
    return getValues();
  }

  /** Restores a previously captured snapshot. */
  public void restore(@Nullable ContextValues snapshot) {
    HARD_TTL_SUPPLIER.remove();
    SOFT_TTL_SUPPLIER.remove();
    if (snapshot != null) {
      HOLDER.set(snapshot);
    } else {
      HOLDER.remove();
    }
  }
}
