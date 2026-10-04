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
package io.github.hyshmily.zeta.cache.cachesupport;

import io.github.hyshmily.zeta.Internal;

/**
 * Read-only view of the runtime configuration the cache core consumes: TTL
 * arithmetic (normal/hot defaults and overrides), the null-sentinel TTL, and
 * cache-key normalization.
 *
 * <p>Implemented at the assembly layer by {@code ZetaProperties} (the
 * {@code zeta.local.*} binding). The cache packages never import the
 * autoconfigure package — the dependency points one way
 * (autoconfigure → cache, ADR-0082). Implementations are expected to be the
 * live properties bean, so every read reflects runtime configuration updates
 * exactly as before this view existed (the pre-extraction behavior that
 * {@link TtlPolicy} documents and {@code ZetaCacheTest} locks in).
 *
 * <p>Method names intentionally mirror the existing {@code ZetaProperties}
 * accessors so the implementation is signature-compatible without bridges
 * (except {@link #isStripQuery()}, which aggregates the nested cache-key
 * block).
 */
@Internal
public interface CacheCoreSettings {

  /** Effective hard TTL for normal keys: override if positive, else the default. */
  long effectiveHardTtlMs();

  /** Effective soft TTL for normal keys: override if positive, else the default (0 = disabled). */
  long effectiveSoftTtlMs();

  /** Effective hard TTL for hot keys: override if positive, else the default. */
  long effectiveHotHardTtlMs();

  /** Effective soft TTL for hot keys: override if positive, else the default (0 = disabled). */
  long effectiveHotSoftTtlMs();

  /**
   * Effective null-sentinel TTL in ms: positive seconds × 1000, or
   * {@link Long#MAX_VALUE} when disabled ({@code <= 0}).
   */
  long effectiveNullTtlMs();

  /** Raw null/cache-miss TTL in seconds (no {@code <= 0} guard — callers apply their own semantics). */
  int getNullValueTtlSeconds();

  /** Jitter ratio (0.0–1.0) applied to all TTL expiry timestamps. */
  double getTtlJitterRatio();

  /** Whether query parameters ({@code ?key=val}) are stripped from cache keys before operations. */
  boolean isStripQuery();
}
