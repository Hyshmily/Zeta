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
package io.github.hyshmily.hotkey.model;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * A value stored in the L1 cache together with its versions (dataVersion,
 * decisionVersion), TTL metadata,
 * and hot-key state.
 *
 * <p>{@code isVersionDegraded} indicates whether the {@code dataVersion} was obtained
 * from Redis INCR (normal) or fell back to node-local counter ({@code Long.MIN_VALUE + counter})
 * (degraded) — see {@code VersionResult} for the degraded-detection logic
 * used during broadcast reception.
 * <p>{@code decisionVersion} tracks Worker HOT/COOL decisions and is always
 * monotonically increasing (never degraded).
 * <p>The normal-state TTLs recorded at entry creation are preserved across state
 * transitions in {@code normalHardTtlMs} and {@code normalSoftTtlMs}.
 */
@Getter
@ToString
@EqualsAndHashCode
@Builder(toBuilder = true)
public class CacheEntry {

  /** The cached value (may be {@code null} if the entry represents a tombstone or a miss). */
  private final Object value;
  /** Monotonically increasing version from Redis INCR (normal) or node-local counter (degraded). */
  private final long dataVersion;
  /** {@code true} if {@link #dataVersion} was obtained from the local fallback instead of Redis. */
  private final boolean isVersionDegraded;
  /** Monotonically increasing version from Worker HOT/COOL decisions; never degraded. */
  private final long decisionVersion;
  /** Hard TTL duration in milliseconds for this specific entry. */
  private final long hardTtlMs;
  /** Absolute epoch-millis timestamp at which the entry should be evicted (hard expiry). */
  private final long hardExpireAtMs;
  /** Soft TTL duration in milliseconds for stale-while-revalidate behaviour. */
  private final long softTtlMs;
  /** Absolute epoch-millis timestamp at which the entry becomes stale (soft expiry). */
  private final long softExpireAtMs;
  /** Current hot-key state of this entry ({@link KeyState#NORMAL}, {@link KeyState#HOT}, or {@link KeyState#COOL}). */
  private final KeyState keyState;
  /** Normal-state hard TTL recorded at entry creation, preserved across HOT/COOL transitions. */
  private final long normalHardTtlMs;
  /** Normal-state soft TTL recorded at entry creation, preserved across HOT/COOL transitions. */
  private final long normalSoftTtlMs;
}
