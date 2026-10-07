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

/**
 * Immutable per-invocation invalidation policy, carrying the resolved
 * storage-side decision for a single cache <em>invalidation</em> operation
 * (ADR-0088).
 *
 * <p>One of three closed policy records replacing the former single
 * {@code CachePolicy}: the invalidation family honors exactly one knob —
 * broadcast control. There is nothing else to misconfigure. The read side is
 * {@link ReadPolicy}, the write side {@link WritePolicy}.
 *
 * @param skipBroadcast whether cross-instance sync messages are suppressed
 *                      for this invalidation
 */
public record InvalidatePolicy(boolean skipBroadcast) {
  /** Singleton carrying all-default semantics (broadcast on). */
  private static final InvalidatePolicy DEFAULTS = new InvalidatePolicy(false);

  /**
   * Returns the shared all-defaults policy: broadcast enabled.
   *
   * @return the default policy singleton
   */
  public static InvalidatePolicy defaults() {
    return DEFAULTS;
  }

  /**
   * Builds an invalidation policy with explicit broadcast control.
   *
   * @param skipBroadcast whether to suppress cross-instance sync messages
   * @return a new policy instance
   */
  public static InvalidatePolicy of(boolean skipBroadcast) {
    return skipBroadcast ? new InvalidatePolicy(true) : DEFAULTS;
  }
}
