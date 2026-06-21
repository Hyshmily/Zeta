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

import io.github.hyshmily.hotkey.sync.VersionGuard;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * A value stored in the L1 cache together with its version metadata,
 * TTL information, and hot-key state.
 *
 * <p>Each {@code CacheEntry} carries two orthogonal version spaces (see ADR-0008):
 * <ul>
 *   <li><b>{@code dataVersion}</b> — monotonically increasing counter obtained
 *       from Redis INCR (normal) or a node-local fallback (degraded). Used by
 *       the cache-sync broadcast to resolve concurrent updates across instances.
 *       When {@code isVersionDegraded} is {@code true}, the version originated
 *       from the local fallback ({@code Long.MIN_VALUE + counter}) and carries
 *       reduced authority.</li>
 *   <li><b>{@code decisionVersion}</b> — tracks Worker HOT/COOL decisions and is
 *       always monotonically increasing (never degraded). Orthogonal to
 *       {@code dataVersion}; used solely for ordering Worker decision broadcasts.</li>
 * </ul>
 *
 * <p>The normal-state TTLs ({@code normalHardTtlMs}, {@code normalSoftTtlMs}) are
 * recorded at entry creation and preserved across hot-key state transitions (HOT
 * extends TTL, COOL reverts to normal). This ensures the original expiry baseline
 * is never lost when the key's state changes.
 *
 * <p>Uses Lombok {@code @Builder(toBuilder = true)} — create new entries with
 * {@code CacheEntry.builder()} and produce modified copies via
 * {@code entry.toBuilder().field(newValue).build()}.
 */
@Getter
@ToString
@EqualsAndHashCode
@Builder(toBuilder = true)
public class CacheEntry {

  /**
   * The cached value. May be {@code null} if the entry represents a tombstone
   * (invalidated) or a placeholder for a cache miss.
   */
  private final Object value;
  /**
   * Monotonically increasing version obtained from Redis INCR (normal path)
   * or a node-local counter (degraded path). Used by the cache-sync broadcast
   * to resolve concurrent write conflicts across instances.
   */
  private final long dataVersion;
  /**
   * {@code true} if {@link #dataVersion} was obtained from the local fallback
   * (node-local counter) instead of Redis. A degraded version carries reduced
   * authority and triggers fallback comparison logic in
   * {@code CacheSyncListener}.
   */
  private final boolean isVersionDegraded;
  /**
   * Monotonically increasing version from Worker HOT/COOL decisions.
   * Never degraded (always originates from the Worker's {@code AtomicLong}).
   * Orthogonal to {@link #dataVersion} — used solely for ordering Worker
   * decision broadcasts (see ADR-0008).
   */
  private final long decisionVersion;
  /**
   * The Worker node ID that produced the {@link #decisionVersion}.
   * Used for per-Worker version partitioning in {@link VersionGuard#shouldSkipForWorker}.
   * May be {@code null} for entries created by local promotion (no Worker origin).
   */
  private final String decisionNodeId;

  /**
   * The epoch (restart counter) of the Worker that produced the {@link #decisionVersion}.
   * A higher epoch indicates a Worker restart; {@link VersionGuard} unconditionally
   * accepts decisions from a higher epoch (see ADR-0010).
   * May be {@code 0} for entries created by local promotion.
   */
  private final long decisionEpoch;
  /**
   * Hard TTL duration in milliseconds for this entry. The entry is evicted
   * unconditionally when {@link #hardExpireAtMs} is reached, regardless of
   * access patterns.
   */
  private final long hardTtlMs;
  /**
   * Absolute epoch-millis timestamp at which the entry should be evicted
   * (hard expiry). Compared against {@code System.currentTimeMillis()} on
   * each read.
   */
  private final long hardExpireAtMs;
  /**
   * Soft TTL duration in milliseconds for stale-while-revalidate behaviour.
   * After {@link #softExpireAtMs} the entry is considered stale; reads may
   * still return the stale value while a background refresh is triggered.
   */
  private final long softTtlMs;
  /**
   * Absolute epoch-millis timestamp at which the entry becomes stale
   * (soft expiry). Before this point the entry is considered fresh.
   */
  private final long softExpireAtMs;
  /**
   * Current hot-key state of this entry. Determines which TTL values
   * are active: {@link KeyState#HOT} uses extended TTLs,
   * {@link KeyState#COOL} reverts to normal TTLs.
   */
  private final KeyState keyState;
  /**
   * Normal-state hard TTL recorded at initial entry creation. Preserved
   * across HOT/COOL state transitions so the original hard expiry baseline
   * is always recoverable when the key returns to NORMAL state.
   */
  private final long normalHardTtlMs;
  /**
   * Normal-state soft TTL recorded at initial entry creation. Preserved
   * across HOT/COOL state transitions so the original soft expiry baseline
   * is always recoverable when the key returns to NORMAL state.
   */
  private final long normalSoftTtlMs;
}
