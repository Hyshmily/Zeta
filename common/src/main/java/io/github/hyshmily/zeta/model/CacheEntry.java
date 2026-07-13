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

import io.github.hyshmily.zeta.util.version.VersionGuard;
import jakarta.annotation.Nullable;
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
 *       the cache-sync send to resolve concurrent updates across instances.
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
 * <p>Uses Lombok {@code @Builder(toBuilder = true)} for initial construction.
 * For modified copies, prefer the {@code withXxx()} family of methods
 * (e.g. {@link #withValue}, {@link #withTtl}) which allocate a single new
 * instance directly — avoiding the intermediate Builder object created by
 * {@code toBuilder().field(v).build()}.
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
   * or a node-local counter (degraded path). Used by the cache-sync send
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

  /** Return a copy with a different {@link #value}. */
  public CacheEntry withValue(@Nullable Object value) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded,
      decisionVersion,
      decisionNodeId,
      decisionEpoch,
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      keyState,
      normalHardTtlMs,
      normalSoftTtlMs
    );
  }

  /** Return a copy with a different {@link #dataVersion}. */
  public CacheEntry withDataVersion(long dataVersion) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded,
      decisionVersion,
      decisionNodeId,
      decisionEpoch,
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      keyState,
      normalHardTtlMs,
      normalSoftTtlMs
    );
  }

  /** Return a copy with a different {@link #isVersionDegraded}. */
  public CacheEntry withIsVersionDegraded(boolean isVersionDegraded) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded,
      decisionVersion,
      decisionNodeId,
      decisionEpoch,
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      keyState,
      normalHardTtlMs,
      normalSoftTtlMs
    );
  }

  /** Return a copy with a different {@link #decisionVersion}. */
  public CacheEntry withDecisionVersion(long decisionVersion) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded,
      decisionVersion,
      decisionNodeId,
      decisionEpoch,
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      keyState,
      normalHardTtlMs,
      normalSoftTtlMs
    );
  }

  /** Return a copy with a different {@link #decisionNodeId}. */
  public CacheEntry withDecisionNodeId(@Nullable String decisionNodeId) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded,
      decisionVersion,
      decisionNodeId,
      decisionEpoch,
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      keyState,
      normalHardTtlMs,
      normalSoftTtlMs
    );
  }

  /** Return a copy with a different {@link #decisionEpoch}. */
  public CacheEntry withDecisionEpoch(long decisionEpoch) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded,
      decisionVersion,
      decisionNodeId,
      decisionEpoch,
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      keyState,
      normalHardTtlMs,
      normalSoftTtlMs
    );
  }

  /** Return a copy with a different {@link #hardTtlMs}. */
  public CacheEntry withHardTtlMs(long hardTtlMs) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded,
      decisionVersion,
      decisionNodeId,
      decisionEpoch,
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      keyState,
      normalHardTtlMs,
      normalSoftTtlMs
    );
  }

  /** Return a copy with a different {@link #hardExpireAtMs}. */
  public CacheEntry withHardExpireAtMs(long hardExpireAtMs) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded,
      decisionVersion,
      decisionNodeId,
      decisionEpoch,
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      keyState,
      normalHardTtlMs,
      normalSoftTtlMs
    );
  }

  /** Return a copy with a different {@link #softTtlMs}. */
  public CacheEntry withSoftTtlMs(long softTtlMs) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded,
      decisionVersion,
      decisionNodeId,
      decisionEpoch,
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      keyState,
      normalHardTtlMs,
      normalSoftTtlMs
    );
  }

  /** Return a copy with a different {@link #softExpireAtMs}. */
  public CacheEntry withSoftExpireAtMs(long softExpireAtMs) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded,
      decisionVersion,
      decisionNodeId,
      decisionEpoch,
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      keyState,
      normalHardTtlMs,
      normalSoftTtlMs
    );
  }

  /** Return a copy with a different {@link #keyState}. */
  public CacheEntry withKeyState(KeyState keyState) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded,
      decisionVersion,
      decisionNodeId,
      decisionEpoch,
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      keyState,
      normalHardTtlMs,
      normalSoftTtlMs
    );
  }

  /** Return a copy with a different {@link #normalHardTtlMs}. */
  public CacheEntry withNormalHardTtlMs(long normalHardTtlMs) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded,
      decisionVersion,
      decisionNodeId,
      decisionEpoch,
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      keyState,
      normalHardTtlMs,
      normalSoftTtlMs
    );
  }

  /** Return a copy with a different {@link #normalSoftTtlMs}. */
  public CacheEntry withNormalSoftTtlMs(long normalSoftTtlMs) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded,
      decisionVersion,
      decisionNodeId,
      decisionEpoch,
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      keyState,
      normalHardTtlMs,
      normalSoftTtlMs
    );
  }

  /** Return a copy with all four TTL fields updated at once. */
  public CacheEntry withTtl(long hardTtlMs, long softTtlMs, long hardExpireAtMs, long softExpireAtMs) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded,
      decisionVersion,
      decisionNodeId,
      decisionEpoch,
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      keyState,
      normalHardTtlMs,
      normalSoftTtlMs
    );
  }

  /** Return a copy with hard TTL and hard expire-at updated together. */
  public CacheEntry withHardTtl(long hardTtlMs, long hardExpireAtMs) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded,
      decisionVersion,
      decisionNodeId,
      decisionEpoch,
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      keyState,
      normalHardTtlMs,
      normalSoftTtlMs
    );
  }

  /** Return a copy with soft TTL and soft expire-at updated together. */
  public CacheEntry withSoftTtl(long softTtlMs, long softExpireAtMs) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded,
      decisionVersion,
      decisionNodeId,
      decisionEpoch,
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      keyState,
      normalHardTtlMs,
      normalSoftTtlMs
    );
  }

  /** Return a copy with both normal TTL fields updated together. */
  public CacheEntry withNormalTtl(long normalHardTtlMs, long normalSoftTtlMs) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded,
      decisionVersion,
      decisionNodeId,
      decisionEpoch,
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      keyState,
      normalHardTtlMs,
      normalSoftTtlMs
    );
  }

  /** Return a copy with all four TTL fields and keyState updated at once. */
  public CacheEntry withTtlAndKeyState(
    long hardTtlMs,
    long softTtlMs,
    long hardExpireAtMs,
    long softExpireAtMs,
    KeyState keyState
  ) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded,
      decisionVersion,
      decisionNodeId,
      decisionEpoch,
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      keyState,
      normalHardTtlMs,
      normalSoftTtlMs
    );
  }

  /**
   * Return a copy with decision metadata, TTL fields, and keyState updated
   * at once — the Worker COOL-decision pattern.
   */
  public CacheEntry withDecisionAndTtlAndState(
    long decisionVersion,
    String decisionNodeId,
    long decisionEpoch,
    long hardTtlMs,
    long softTtlMs,
    long hardExpireAtMs,
    long softExpireAtMs,
    KeyState keyState
  ) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded,
      decisionVersion,
      decisionNodeId,
      decisionEpoch,
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      keyState,
      normalHardTtlMs,
      normalSoftTtlMs
    );
  }

  /**
   * Return a copy with value, version metadata, and expire-at timestamps
   * updated at once — the cache-sync refresh pattern.
   */
  public CacheEntry withValueAndRefreshMeta(
    Object value,
    long dataVersion,
    boolean isVersionDegraded,
    long hardExpireAtMs,
    long softExpireAtMs
  ) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded,
      decisionVersion,
      decisionNodeId,
      decisionEpoch,
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      keyState,
      normalHardTtlMs,
      normalSoftTtlMs
    );
  }

  /** Return a copy with value and soft TTL fields updated at once — the refresh-task pattern. */
  public CacheEntry withValueAndSoftTtl(Object value, long softTtlMs, long softExpireAtMs) {
    return new CacheEntry(
      value,
      dataVersion,
      isVersionDegraded,
      decisionVersion,
      decisionNodeId,
      decisionEpoch,
      hardTtlMs,
      hardExpireAtMs,
      softTtlMs,
      softExpireAtMs,
      keyState,
      normalHardTtlMs,
      normalSoftTtlMs
    );
  }
}
