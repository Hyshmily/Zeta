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

/**
 * Outcome classification for a {@link SingleFlight} load.
 *
 * <p>Replaces the overloaded {@code Optional.empty()}, which conflated "source
 * returned null" with "load suppressed by protection machinery" and "reader
 * threw". Every terminal state of the dedup execution maps to exactly one
 * member:
 *
 * <ul>
 *   <li>{@code LOADED} — reader completed with a non-null value.</li>
 *   <li>{@code SOURCE_EMPTY} — reader completed with {@code null}.</li>
 *   <li>{@code SUPPRESSED_BREAKER} — circuit breaker open, reader never ran.</li>
 *   <li>{@code SUPPRESSED_TIMEOUT} — reader outlived the per-load timeout
 *       (interrupted via {@code InterruptingAsync}).</li>
 *   <li>{@code SUPPRESSED_EXECUTOR} — executor saturated, task rejected before
 *       any data-source call (half-open probe reservation released).</li>
 *   <li>{@code FAILED} — reader threw; {@link LoaderOutcome#cause()} carries it.
 *       An {@code InterruptedException} cause maps here (with the thread's
 *       interrupt bit restored by both layers).</li>
 * </ul>
 */
public enum LoadStatus {
  /** Reader completed with a non-null value. */
  LOADED,
  /** Reader completed with {@code null} (source has no data). */
  SOURCE_EMPTY,
  /** Circuit breaker open — the reader was never executed. */
  SUPPRESSED_BREAKER,
  /** Reader exceeded the per-load timeout and was interrupted. */
  SUPPRESSED_TIMEOUT,
  /** Executor rejected the task before any data-source call. */
  SUPPRESSED_EXECUTOR,
  /** Reader threw — see {@link LoaderOutcome#cause()}. */
  FAILED,
}
