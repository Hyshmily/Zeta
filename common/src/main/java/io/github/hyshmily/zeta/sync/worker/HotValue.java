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
package io.github.hyshmily.zeta.sync.worker;

import java.util.Objects;

/**
 * A value resolved for a HOT promotion plus the data version it carries.
 *
 * <p>{@code source} distinguishes the three ways a candidate reaches {@link WorkerPolicy#hot}:
 * loaded during this promotion ({@link Source#LOADED}), reused out of L1 as the source-outage
 * fallback ({@link Source#RESIDENT_FALLBACK}), or explicitly supplied by the policy after it
 * replaced the candidate ({@link Source#POLICY_SUPPLIED}). A loaded or policy-supplied value is
 * writable: it may create a new entry or replace a live resident's value, subject to the version
 * and invalidation-tombstone guards. A fallback is not writable: it may only promote the resident
 * it came from, otherwise it could resurrect a value a concurrent invalidation removed while the
 * load was in flight.
 *
 * <p>A writable candidate always carries a positive data stamp. A resident fallback may carry
 * {@code 0} (unknown): it is an existing value, never a new one.
 *
 * @param value       the value to promote; never {@code null}
 * @param dataVersion the data stamp the value carries ({@code 0} = unknown, fallback only)
 * @param source      where the candidate came from; never {@code null}
 */
public record HotValue(Object value, long dataVersion, Source source) {
  /** Where a HOT candidate came from, which decides whether its value may be written. */
  public enum Source {
    /** Loaded through the cluster loader during this promotion. */
    LOADED,
    /** Reused from the resident L1 entry after the source failed to load. */
    RESIDENT_FALLBACK,
    /** Explicitly supplied by {@link WorkerPolicy#hot} as a replacement. */
    POLICY_SUPPLIED,
  }

  public HotValue {
    Objects.requireNonNull(value, "value must not be null");
    Objects.requireNonNull(source, "source must not be null");
    if (source != Source.RESIDENT_FALLBACK && dataVersion <= 0L) {
      throw new IllegalArgumentException(source + " value requires a positive dataVersion, got " + dataVersion);
    }
  }

  /**
   * A value loaded during this promotion. Its data version is the pre-read stamp, so a write
   * that lands during the load carries a strictly greater stamp.
   *
   * @param value       the loaded value
   * @param dataVersion the positive pre-read data stamp
   * @return the loaded value
   */
  public static HotValue loaded(Object value, long dataVersion) {
    return new HotValue(value, dataVersion, Source.LOADED);
  }

  /**
   * A resident L1 value reused as the source-outage fallback. It keeps its own data version:
   * it is an existing value and must not be re-signed with a fresher stamp.
   *
   * @param value       the resident value
   * @param dataVersion the resident's data stamp, {@code 0} when unknown
   * @return the resident value
   */
  public static HotValue resident(Object value, long dataVersion) {
    return new HotValue(value, dataVersion, Source.RESIDENT_FALLBACK);
  }

  /**
   * A value explicitly supplied by {@link WorkerPolicy#hot} as a replacement for the resolved
   * candidate. It is writable, but it still passes the version and invalidation-tombstone
   * guards, exactly like a loaded value.
   *
   * @param value       the replacement value
   * @param dataVersion the positive data stamp the replacement carries
   * @return the policy-supplied value
   */
  public static HotValue supplied(Object value, long dataVersion) {
    return new HotValue(value, dataVersion, Source.POLICY_SUPPLIED);
  }

  /**
   * Whether this candidate's value may be written to L1. Only a resident fallback is excluded:
   * it represents an existing value and may not create an entry or overwrite the resident.
   *
   * @return {@code true} for a loaded or policy-supplied value
   */
  public boolean canWriteValue() {
    return source != Source.RESIDENT_FALLBACK;
  }
}
