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
package io.github.hyshmily.zeta.model;

import java.util.function.LongSupplier;

/**
 * Immutable per-invocation cache policy, carrying the resolved storage-side
 * decisions for a single cache operation.
 *
 * <p>This record is the single transport object between the annotation layer
 * ({@code CacheExtensionAspect} → {@code ZetaCacheContext} → {@code ZetaSpringCache})
 * and the {@code Zeta} facade / {@code HotKeyCache} read paths. It replaces the
 * previous bag of loosely-coupled ThreadLocal fields.
 *
 * <h3>Lazy TTL contract</h3>
 * The TTL suppliers are evaluated <b>at most once per cache call</b> and only
 * when the value is actually needed — that is, when an entry is created on a
 * cache miss, promoted to HOT, renewed, or a soft-expire background refresh is
 * scheduled. A plain cache hit on a NORMAL entry never evaluates the suppliers,
 * so SpEL-based TTL expressions cost nothing on the hit path.
 *
 * <h3>Null-caching contract</h3>
 * When {@link #nullCaching()} is {@code true} (the default), a {@code null}
 * loader result is stored as a {@code NullValue} sentinel with a short TTL
 * ({@code zeta.local.null-value-ttl-seconds}) for cache-penetration protection.
 * When {@code false}, a {@code null} loader result leaves no cache entry at
 * all, so the next call re-invokes the loader.
 *
 * @param hardTtlMs    lazy hard TTL override in milliseconds (0 = use configured
 *                     default); evaluated at most once per cache call
 * @param softTtlMs    lazy soft TTL override in milliseconds (0 = use configured
 *                     default); evaluated at most once per cache call
 * @param nullCaching  whether {@code null} loader results may be cached as a
 *                     sentinel entry
 * @param skipBroadcast whether cross-instance sync messages are suppressed for
 *                     write/evict operations (ignored on read paths)
 */
public record CachePolicy(
    LongSupplier hardTtlMs,
    LongSupplier softTtlMs,
    boolean nullCaching,
    boolean skipBroadcast
) {

  /** Shared zero supplier for "no TTL override". */
  private static final LongSupplier ZERO = () -> 0L;

  /** Singleton carrying all-default semantics. */
  private static final CachePolicy DEFAULTS = new CachePolicy(ZERO, ZERO, true, false);

  /**
   * Compact constructor: {@code null} suppliers are normalized to a zero
   * supplier so accessors never return {@code null}.
   */
  public CachePolicy {
    if (hardTtlMs == null) hardTtlMs = ZERO;
    if (softTtlMs == null) softTtlMs = ZERO;
  }

  /**
   * Returns the shared all-defaults policy: no TTL override, null caching
   * enabled, broadcast enabled.
   *
   * @return the default policy singleton
   */
  public static CachePolicy defaults() {
    return DEFAULTS;
  }

  /**
   * Builds a policy from static TTL values. The values are wrapped into
   * constant suppliers; the lazy-evaluation contract is unaffected.
   *
   * @param hardTtlMs     hard TTL override (0 = use configured default)
   * @param softTtlMs     soft TTL override (0 = use configured default)
   * @param nullCaching   whether {@code null} loader results may be cached
   * @param skipBroadcast whether to suppress cross-instance sync messages
   * @return a new policy instance
   */
  public static CachePolicy of(long hardTtlMs, long softTtlMs, boolean nullCaching, boolean skipBroadcast) {
    return new CachePolicy(() -> hardTtlMs, () -> softTtlMs, nullCaching, skipBroadcast);
  }
}
