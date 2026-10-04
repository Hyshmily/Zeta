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
import io.github.hyshmily.zeta.cache.cachesupport.impl.CircuitBreakerImpl;
import java.util.List;

/**
 * Read-only view of the sliding-window circuit breaker configuration the
 * breaker implementation reads live on every decision ({@link CircuitBreakerImpl}).
 *
 * <p>Implemented at the assembly layer by the nested circuit-breaker block of
 * {@code ZetaProperties} ({@code zeta.local.circuit-breaker.*}); the cache
 * packages never import the autoconfigure package — the dependency points one
 * way (autoconfigure → cache, ADR-0082). Method names mirror the existing
 * accessors, so the implementation is signature-compatible without bridges.
 *
 * <p>All reads are live: toggling {@link #isEnabled()} at runtime opens or
 * closes the breaker's fast paths exactly as before this view existed.
 */
@Internal
public interface CircuitBreakerSettings {
  /** Whether the breaker is enabled. Disabled = every call is allowed, windows still tracked. */
  boolean isEnabled();

  /** Sliding window duration in milliseconds. */
  long getWindowTimeMs();

  /** Number of buckets in the sliding window. */
  int getWindowBuckets();

  /** Failure rate threshold (0.0–1.0) — the breaker opens when exceeded. */
  double getFailThreshold();

  /** Minimum total requests in the window before the failure rate is evaluated. */
  long getRequestVolumeThreshold();

  /** How long to wait (ms) before allowing a half-open probe request. */
  long getSingleTestIntervalMs();

  /** Whether to log state transitions. */
  boolean isLogEnabled();

  /** Maximum number of probe requests allowed in the HALF_OPEN state. */
  int getHalfOpenMaxProbes();

  /** Consecutive successes in HALF_OPEN state required to close the breaker. */
  int getConsecutiveSuccessThreshold();

  /** Fully-qualified exception names that must NOT trip the breaker (client-error filter). */
  List<String> getExcludeExceptions();

  /** Fully-qualified exception names that SHOULD trip the breaker (allowlist filter). */
  List<String> getIncludeExceptions();
}
