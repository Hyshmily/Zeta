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
 * Immutable per-invocation write policy, carrying the resolved storage-side
 * decisions for a single cache <em>write</em> operation (ADR-0088).
 *
 * <p>One of three closed policy records replacing the former single
 * {@code CachePolicy}: each record carries exactly the knobs its call family
 * honors. The write family honors TTL overrides and broadcast control —
 * nothing else — so a write policy <em>cannot</em> name a reader, a
 * stale-policy, or failure semantics. The read side is {@link ReadPolicy},
 * the invalidation side {@link InvalidatePolicy}.
 *
 * @param hardTtlMs     lazy hard TTL override in milliseconds (0 = use configured
 *                      default); evaluated at most once per cache call
 * @param softTtlMs     lazy soft TTL override in milliseconds (0 = use configured
 *                      default); evaluated at most once per cache call
 * @param skipBroadcast whether cross-instance sync messages are suppressed for
 *                      this write
 */
public record WritePolicy(LongSupplier hardTtlMs, LongSupplier softTtlMs, boolean skipBroadcast) {
  /** Shared zero supplier for "no TTL override". */
  private static final LongSupplier ZERO = () -> 0L;

  private static LongSupplier ttlSupplier(long ttlMs) {
    return ttlMs == 0L ? ZERO : () -> ttlMs;
  }

  /** Singleton carrying all-default semantics (no TTL override, broadcast on). */
  private static final WritePolicy DEFAULTS = new WritePolicy(ZERO, ZERO, false);

  /**
   * Compact constructor: {@code null} suppliers are normalized to a zero
   * supplier so accessors never return {@code null}.
   */
  public WritePolicy {
    if (hardTtlMs == null) hardTtlMs = ZERO;
    if (softTtlMs == null) softTtlMs = ZERO;
  }

  /**
   * Returns the shared all-defaults policy: no TTL override, broadcast
   * enabled.
   *
   * @return the default policy singleton
   */
  public static WritePolicy defaults() {
    return DEFAULTS;
  }

  /**
   * Builds a write policy from static TTL overrides with broadcast enabled.
   *
   * @param hardTtlMs hard TTL override (0 = use configured default;
   *                  {@link Long#MAX_VALUE} for permanent entry)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @return a new policy instance
   */
  public static WritePolicy of(long hardTtlMs, long softTtlMs) {
    return new WritePolicy(ttlSupplier(hardTtlMs), ttlSupplier(softTtlMs), false);
  }

  /**
   * Builds a write policy from static TTL overrides with explicit broadcast
   * control.
   *
   * @param hardTtlMs     hard TTL override (0 = use configured default;
   *                      {@link Long#MAX_VALUE} for permanent entry)
   * @param softTtlMs     soft TTL override (0 = use configured default)
   * @param skipBroadcast whether to suppress cross-instance sync messages
   * @return a new policy instance
   */
  public static WritePolicy of(long hardTtlMs, long softTtlMs, boolean skipBroadcast) {
    return new WritePolicy(ttlSupplier(hardTtlMs), ttlSupplier(softTtlMs), skipBroadcast);
  }
}
