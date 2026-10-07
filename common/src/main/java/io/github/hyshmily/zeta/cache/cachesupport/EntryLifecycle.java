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
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.EntryDraft;
import io.github.hyshmily.zeta.model.KeyState;
import org.jspecify.annotations.Nullable;

/**
 * The stateful side of the {@link CacheEntry} lifecycle: TOCTOU invalidation
 * guards, Decision-Validity demotion, the entry factory, and value wrapping.
 *
 * <p><b>Entry factory.</b> Every entry creation and modification flows through
 * the single {@link EntryDraft} API obtained here:
 * <ul>
 *   <li>{@link #newEntry()} — a blank draft for creating an entry;</li>
 *   <li>{@link #editEntry(CacheEntry)} — a draft seeded from an existing entry
 *       (copy-on-write: untouched fields carry over).</li>
 * </ul>
 * Both are wired to {@link #ttlPolicy()} for computed expire timestamps; wrap
 * raw values via {@link #wrapValue} <b>before</b> the Caffeine bin lock and
 * hand the stored form to {@link EntryDraft#value} (ADR-0030 discipline).
 *
 * <p>All stateless TTL arithmetic (resolve / compute / getEffective /
 * timestamp conversion with jitter / expiry predicates) lives in
 * {@link TtlPolicy}, exposed here via {@link #ttlPolicy()}.
 *
 * <p>Background soft-expire refresh scheduling lives behind the separate
 * {@link io.github.hyshmily.zeta.scheduler.BackgroundRefresher} seam — this
 * interface deliberately owns no executor and no scheduling state.
 */
@Internal
public interface EntryLifecycle {

  /**
   * The pure TTL and expiry policy backing this lifecycle: every stateless
   * lifecycle computation (resolve vs default, expire timestamps, jitter,
   * predicates, TTL transforms) is performed through this module.
   *
   * @return the TTL policy; never null
   */
  TtlPolicy ttlPolicy();

  /**
   * Check whether the given L1 entry is logically expired
   * and, if so, invalidate it and return {@code true}.
   */
  boolean invalidateIfIsLogicallyExpired(String cacheKey, @Nullable CacheEntry raw);

  /**
   * Demote a Worker-sourced {@link KeyState#HOT} entry whose issuing Worker
   * incarnation is no longer authoritative (Decision Validity — ADR-0035).
   *
   * <p>The demotion is an in-place rewrite performed on the read path: the
   * value is preserved and continues to be served, the entry reverts to the
   * ordinary NORMAL lifecycle (normal TTLs, decision stamp cleared), and the
   * local TopK re-decides on the next reload. Zero scan — only entries
   * actually read are visited.
   *
   * <p>Decision invalidity: the entry carries a {@code decisionNodeId} whose
   * health record is absent, whose incarnation is dead, or whose epoch no
   * longer matches the entry's {@code decisionEpoch} (Worker restart). The
   * predicate is re-verified inside the atomic write, so a concurrent fresh
   * broadcast (recovered Worker) is never clobbered.
   *
   * @param cacheKey the cache key
   * @param raw      the L1 entry (may be {@code null})
   * @return {@code true} if the entry was demoted
   */
  boolean demoteIfDecisionInvalid(String cacheKey, @Nullable CacheEntry raw);

  /**
   * Pure Decision-Validity demotion (ADR-0035): if the given Worker-sourced
   * HOT entry's issuing Worker incarnation is dead or restarted, return the
   * entry rewritten in place to the NORMAL lifecycle — value preserved, normal
   * TTLs, decision stamp cleared. Returns {@code null} when no demotion
   * applies.
   *
   * <p>Stateless and side-effect-free: the caller decides atomicity. Safe
   * inside a Caffeine {@code compute} callback, where the bin lock already
   * makes the check-and-rewrite atomic (see {@code HotKeyCache.computeInLock}).
   *
   * @param entry the Worker-sourced cache entry to inspect
   * @return the demoted entry, or {@code null} if no demotion applies
   */
  @Nullable
  CacheEntry demoteIfDecisionInvalidInPlace(CacheEntry entry);

  /**
   * Extract the Worker decision stamp from an L1 entry, or {@code null}
   * when the value carries no Worker origin — {@code null} or an
   * entry without a {@code decisionNodeId} (local promotion). This is the
   * single source of the carry-forward semantics shared by the null-sentinel
   * and put-through rebuild paths: a rebuild must not erase a Worker decision
   * (HOT/COOL stamp) ordering anchor. Callers that pass the result to
   * {@link EntryDraft#decision} get identical fields for {@code null} and for
   * an all-zero local stamp ({@code VERSION_DEFAULT} / 0), so the two shapes
   * are interchangeable.
   *
   * @param existing the L1 entry (may be {@code null})
   * @return the decision stamp, or {@code null} for a local origin
   */
  static @Nullable DecisionStamp decisionOf(@Nullable CacheEntry existing) {
    return existing == null ? null : existing.decisionStamp();
  }

  /**
   * A blank {@link EntryDraft} for creating a fresh {@link CacheEntry}, wired
   * to this lifecycle's TTL arithmetic — the single creation path. Shape the
   * draft (value in stored form, version, decision, key state, TTLs) and
   * {@link EntryDraft#build} it.
   *
   * @return a blank draft with TTL arithmetic attached
   */
  EntryDraft newEntry();

  /**
   * An {@link EntryDraft} seeded from an existing entry, wired to this
   * lifecycle's TTL arithmetic — the single copy-on-write modification path.
   * Untouched fields carry over from the source entry.
   *
   * @param source the entry to modify (not changed itself)
   * @return a seeded draft with TTL arithmetic attached
   */
  EntryDraft editEntry(CacheEntry source);

  /**
   * Wrap a raw value using the configured compressor, without allocating a new CacheEntry.
   * Used by callers that need the compressed value for the draft's
   * {@link EntryDraft#value} — compression must run BEFORE acquiring the
   * Caffeine bin lock (write-side ADR-0030 discipline).
   */
  Object wrapValue(@Nullable Object rawValue);
}
