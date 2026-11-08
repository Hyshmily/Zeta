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
package io.github.hyshmily.zeta.cache.cachesupport.policy;

import java.util.function.LongSupplier;

/**
 * Immutable per-invocation write options, carrying the resolved storage-side
 * decisions for a single cache <em>write</em> operation.
 *
 * <p>One of three closed option records: each record carries exactly the knobs
 * its call family honors. The write family honors TTL overrides and cluster
 * sync control — nothing else — so write options <em>cannot</em> name a
 * reader, a stale-policy, or failure semantics. The read side is
 * {@link GetPolicy}, the invalidation side {@link InvalidatePolicy}.
 *
 * <h2>Sync semantics</h2>
 * {@code sync} controls cross-instance synchronization of this write
 * (fire-and-forget fan-out, lost messages tolerated by TTL convergence —
 * never a blocking wait for peer acknowledgement). It is <b>not</b> a local
 * per-key mutex around concurrent loads.
 *
 * <p>A {@code put} is pure cache maintenance: it carries no business writer.
 * "Mutate the source, then update the cache" orchestration belongs to the
 * caller. The default is local-only; {@link #synced()} upgrades a single call
 * to a version-stamped cluster write.
 *
 * @param hardTtlMs lazy hard TTL override in milliseconds (0 = use configured
 *                  default); evaluated at most once per cache call
 * @param softTtlMs lazy soft TTL override in milliseconds (0 = use configured
 *                  default); evaluated at most once per cache call
 * @param sync      whether this write is synchronized to peer instances
 */
public record PutPolicy(LongSupplier hardTtlMs, LongSupplier softTtlMs, boolean sync) {
  /** Singleton carrying all-default semantics (no TTL override, local-only). */
  private static final PutPolicy DEFAULTS = new PutPolicy(GetPolicy.ZERO, GetPolicy.ZERO, false);

  /** Singleton carrying cluster-sync semantics with no TTL override. */
  private static final PutPolicy SYNC = new PutPolicy(GetPolicy.ZERO, GetPolicy.ZERO, true);

  /**
   * Compact constructor: {@code null} suppliers are normalized to a zero
   * supplier so accessors never return {@code null}.
   */
  public PutPolicy {
    if (hardTtlMs == null) hardTtlMs = GetPolicy.ZERO;
    if (softTtlMs == null) softTtlMs = GetPolicy.ZERO;
  }

  /**
   * Returns the shared all-defaults options: no TTL override, local-only.
   *
   * @return the default options singleton
   */
  public static PutPolicy defaults() {
    return DEFAULTS;
  }

  /**
   * Returns the shared cluster-sync options: no TTL override, synchronized to
   * peer instances. Named {@code synced()} because the record component
   * {@code sync} already claims the {@code sync()} accessor name.
   *
   * @return the sync options singleton
   */
  public static PutPolicy synced() {
    return SYNC;
  }

  /**
   * Builds write options from static TTL overrides with explicit cluster sync
   * control.
   *
   * @param hardTtlMs hard TTL override (0 = use configured default;
   *                  {@link Long#MAX_VALUE} for permanent entry)
   * @param softTtlMs soft TTL override (0 = use configured default)
   * @param sync      whether this write is synchronized to peer instances
   * @return a new options instance
   * @throws IllegalArgumentException when a permanent hard TTL is combined with
   *                                  {@code sync}: peers can only drop an entry
   *                                  through its hard TTL, so a permanent synced
   *                                  entry pins a stale value on every peer
   */
  public static PutPolicy of(long hardTtlMs, long softTtlMs, boolean sync) {
    requireSyncable(hardTtlMs, sync);
    return new PutPolicy(GetPolicy.ttlSupplier(hardTtlMs), GetPolicy.ttlSupplier(softTtlMs), sync);
  }

  /**
   * Validates the permanent-entry/sync combination without building options.
   * The annotation layer calls this before a write body runs, so a dynamic
   * hard TTL that resolves to {@link Long#MAX_VALUE} fails before the business
   * side effect rather than after the transaction committed.
   *
   * @param hardTtlMs hard TTL override (0 = use configured default;
   *                  {@link Long#MAX_VALUE} for permanent entry)
   * @param sync      whether this write is synchronized to peer instances
   * @throws IllegalArgumentException when a permanent hard TTL is combined with
   *                                  {@code sync}
   */
  public static void requireSyncable(long hardTtlMs, boolean sync) {
    if (sync && hardTtlMs == Long.MAX_VALUE) {
      throw new IllegalArgumentException(
        "A permanent entry cannot be synced: peers expire entries by hard TTL, so a broadcast permanent " +
          "entry leaves every peer holding it forever"
      );
    }
  }
}
