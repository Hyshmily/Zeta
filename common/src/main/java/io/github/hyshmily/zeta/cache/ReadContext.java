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
package io.github.hyshmily.zeta.cache;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.model.ReadPolicy;
import io.github.hyshmily.zeta.model.StalePolicy;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Per-call read context for the policy-based read paths: the resolved
 * {@link ReadPolicy} knobs plus the guard-derived {@code skipReport} flag and
 * the lazily memoized TTL overrides.
 *
 * <p>
 * The TTL suppliers are memoized on first use so the documented
 * at-most-once-per-call contract of {@link ReadPolicy} holds structurally,
 * even when several {@link HotKeyCache} methods consume the same override
 * within one call (e.g. the {@code getWithSoftExpire} SOFT_REFRESH branch
 * feeds both the background refresh and the promotion probe). The memoization
 * state lives here instead of per-supplier wrapper objects, so a call that
 * never consumes a TTL — a plain NORMAL-entry hit, per the
 * {@link ReadPolicy} contract — allocates no memoize wrappers at all.
 *
 * <p>
 * Single-threaded by design: one instance per read call, consumed entirely
 * on the calling thread (a scheduled background refresh consumes the reader
 * supplier, never the memoized TTLs, which are resolved before scheduling).
 */
@Internal
final class ReadContext {

  private final Supplier<?> reader;
  private final boolean nullCaching;
  private final StalePolicy stalePolicy;
  private final boolean skipReport;
  private final LongSupplier rawHardTtlMs;
  private final LongSupplier rawSoftTtlMs;
  private boolean hardResolved;
  private long hardTtl;
  private boolean softResolved;
  private long softTtl;

  private ReadContext(
    Supplier<?> reader,
    boolean nullCaching,
    StalePolicy stalePolicy,
    boolean skipReport,
    LongSupplier rawHardTtlMs,
    LongSupplier rawSoftTtlMs
  ) {
    this.reader = reader;
    this.nullCaching = nullCaching;
    this.stalePolicy = stalePolicy;
    this.skipReport = skipReport;
    this.rawHardTtlMs = rawHardTtlMs;
    this.rawSoftTtlMs = rawSoftTtlMs;
  }

  /**
   * Shared zero supplier for "no TTL override" (mirrors
   * {@code ReadPolicy}'s zero).
   */
  private static final LongSupplier ZERO = () -> 0L;

  /**
   * Wrap a static TTL override in a constant supplier, reusing {@link #ZERO} for
   * 0.
   */
  private static LongSupplier constant(long ttlMs) {
    return ttlMs == 0L ? ZERO : () -> ttlMs;
  }

  /**
   * Single-key read paths: the TTL suppliers stay lazy and are memoized once
   * per call, so SpEL-backed expressions are evaluated at most once regardless
   * of how many downstream methods consume them.
   */
  static ReadContext of(ReadPolicy policy, boolean skipReport) {
    return new ReadContext(
      policy.reader(),
      policy.nullCaching(),
      policy.stalePolicy(),
      skipReport,
      policy.hardTtlMs(),
      policy.softTtlMs()
    );
  }

  /**
   * Batch read helpers: the TTL overrides are already-resolved constants
   * carried by the caller, so no laziness is involved.
   */
  static ReadContext of(Supplier<?> reader, long hardTtlMs, long softTtlMs, boolean skipReport) {
    return new ReadContext(
      reader,
      true,
      StalePolicy.SOFT_REFRESH,
      skipReport,
      constant(hardTtlMs),
      constant(softTtlMs)
    );
  }

  Supplier<?> reader() {
    return reader;
  }

  boolean nullCaching() {
    return nullCaching;
  }

  StalePolicy stalePolicy() {
    return stalePolicy;
  }

  boolean skipReport() {
    return skipReport;
  }

  /**
   * Memoized lazy hard TTL override (0 = use configured default): the
   * underlying (possibly SpEL-backed) supplier evaluates at most once per call.
   */
  long hardTtlMs() {
    if (!hardResolved) {
      hardTtl = rawHardTtlMs.getAsLong();
      hardResolved = true;
    }
    return hardTtl;
  }

  /**
   * Memoized lazy soft TTL override (0 = use configured default), same contract
   * as {@link #hardTtlMs()}.
   */
  long softTtlMs() {
    if (!softResolved) {
      softTtl = rawSoftTtlMs.getAsLong();
      softResolved = true;
    }
    return softTtl;
  }
}
