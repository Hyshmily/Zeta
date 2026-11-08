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
package io.github.hyshmily.zeta.cache.cachesupport.lifecycle;

import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryMutations.Applied;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.util.Internal;
import java.util.Map;
import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;

/**
 * The stateful side of the {@link CacheEntry} lifecycle: TOCTOU invalidation
 * guards, Decision-Validity demotion and the entry factory.
 *
 * <p><b>Entry factory.</b> Every entry creation and modification flows through
 * the single {@link EntryDraft} API obtained here:
 * <ul>
 *   <li>{@link #newEntry()} — a blank draft for creating an entry;</li>
 *   <li>{@link #editEntry(CacheEntry)} — a draft seeded from an existing entry
 *       (copy-on-write: untouched fields carry over).</li>
 * </ul>
 * Both are wired to {@link #ttlPolicy()} for computed expire timestamps.
 * Values are stored as passed by the caller (no codec).
 *
 * <p>All stateless TTL arithmetic (override-vs-default resolution, configured
 * effective TTLs, timestamp conversion with jitter, expiry predicates) lives in
 * {@link TtlPolicy}, exposed here via {@link #ttlPolicy()}. Collaborators that
 * need the arithmetic read it once at construction and keep the reference:
 * the policy is stateless, so a per-call chain through the lifecycle only
 * repeats a dereference.
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
   * Judges hard expiry and removes the supplied snapshot only when it is still
   * the resident. A concurrent replacement is returned live: its expiry is not
   * this caller's judgment, so the read path must re-classify it rather than
   * treating the key as a miss.
   *
   * @param cacheKey the cache key
   * @param raw      the entry the caller read, or {@code null}
   * @param now      the per-call epoch-millis sample
   * @return the surviving resident, or {@code null} when the key is now absent
   */
  @Nullable
  CacheEntry evictIfExpired(String cacheKey, @Nullable CacheEntry raw, long now);

  /**
   * Demote a Worker-sourced {@link KeyState#HOT} entry whose issuing Worker
   * incarnation is no longer authoritative (Decision Validity — design decisions: hot state).
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
   * <p>A successful demotion fires the demoted sink registered via
   * {@link #setDemotedSink(java.util.function.Consumer)} — one owner of the
   * "re-queue the locally-hot key for the promoter" follow-up, shared by the
   * read paths and the background sweeper.
   *
   * @param cacheKey the cache key
   * @param raw      the L1 entry (may be {@code null})
   * @return the applied pre/post-images, or {@code null} when no demotion applied
   */
  @Nullable
  Applied demoteIfRemoteNodeIdInvalid(String cacheKey, @Nullable CacheEntry raw);

  /**
   * Read-only Decision-Validity check: whether {@code raw} is a Worker-sourced
   * {@link KeyState#HOT} entry whose issuing incarnation is no longer
   * authoritative. The read path uses it to decide whether a hit must enter
   * reconciliation; {@link #demoteIfRemoteNodeIdInvalid(String, CacheEntry)}
   * performs the rewrite.
   *
   * @param raw the L1 entry to inspect (never {@code null})
   * @return {@code true} when the entry carries a stale Worker decision
   */
  boolean needsDemotion(CacheEntry raw);

  /**
   * Registers the callback fired once per successful
   * {@link #demoteIfRemoteNodeIdInvalid} demotion — the single notification point
   * for "a Worker-HOT entry reverted to NORMAL", so the read paths and the
   * background sweeper cannot drift on whether the local promoter learns about
   * a demotion. {@code null} disables the notification.
   *
   * @param demotedSink the callback receiving the demoted key, or {@code null}
   */
  default void setDemotedSink(@Nullable Consumer<String> demotedSink) {}

  /**
   * Whether a write at {@code version} loses to an invalidation this node
   * already applied — the single anti-resurrection guard shared by every L1
   * write path (load fills, local writes) and by the sync receiver.
   *
   * <p>The tombstones are core L1 state, not a sync-plane detail: an
   * in-flight read that started before an {@code INVALIDATE}/{@code clearAll}
   * holds an older stamp, and on an absent entry the resident guard in
   * {@code HotKeyCache} accepts any stamp — this predicate is what refuses it.
   * Every path that removes entries must therefore record here, or a slow
   * read pins the pre-invalidation value back into L1 for its whole TTL.
   *
   * <p>Three watermarks participate, in cheapest-first order: the table-wide
   * clear, the namespace prefixes (a namespace clear), then the per-key
   * tombstone. A key may match more than one recorded prefix watermark, so
   * every recorded prefix is tested.
   *
   * <p><b>A version-less write is outside the stamp order.</b> For
   * {@code version <= 0} only the per-key tombstone is consulted: the clear and
   * prefix watermarks never expire, so ordering an unstamped write against them
   * would refuse every such fill for the rest of the process after a single
   * clear. The consequence is deliberate — a stamp-less in-flight read can
   * survive a {@code clearAll} / prefix clear, and is bounded only by its own
   * TTL — while the same read is refused after a per-key invalidation.
   *
   * @param cacheKey the key being written
   * @param version  the stamp the write would store ({@code 0} = unknown, which
   *                 loses to any recorded tombstone for the key)
   * @return {@code true} when the write predates an applied invalidation
   */
  boolean staleAgainstInvalidation(String cacheKey, long version);

  /**
   * Record a per-key invalidation at {@code version}: writes at or below it are
   * refused from now on. The tombstone is bounded in time and size — it only
   * has to outlive in-flight reads, not the entry's hard TTL; the bounds are
   * {@code zeta.local.cache.tombstone-maximum-size} /
   * {@code zeta.local.cache.tombstone-ttl-seconds}.
   *
   * @param cacheKey the invalidated key
   * @param version  the invalidation's stamp ({@code <= 0} records nothing)
   */
  void recordInvalidation(String cacheKey, long version);

  /**
   * Record a table-wide clear: every write stamped at or below
   * {@code clearStamp} is refused from now on, whichever key it targets.
   *
   * <p>A write carrying no stamp ({@code <= 0}) is outside that order: the
   * watermark cannot place it, so only a per-key tombstone can refuse it (see
   * {@link #staleAgainstInvalidation(String, long)}).
   *
   * @param clearStamp the clear's stamp ({@code <= 0} records nothing)
   */
  void recordClear(long clearStamp);

  /**
   * Record a namespace clear: every write stamped at or below
   * {@code clearStamp} whose key starts with {@code prefix} is refused from
   * now on. This is the scoped counterpart of {@link #recordClear(long)} — a
   * namespace clear must not flush the whole table.
   *
   * <p><b>The prefix must be namespace-complete</b> — a physical key prefix
   * ends with the namespace delimiter. Matching is a raw {@code startsWith}
   * over physical keys, so a bare name ("user") would also refuse writes to
   * sibling namespaces ("user_profile"); the delimiter makes the match exact.
   *
   * <p>Prefix watermarks are bounded in add and self-pruning: a stamp the
   * table-wide watermark covers is dropped, and past the cap the older entries
   * first shed the ones older than the tombstone TTL; if the cap is still
   * exceeded the whole live set folds into the table-wide watermark at the
   * highest stamp involved (a safe over-approximation) rather than growing
   * without bound.
   *
   * @param prefix     the key prefix (non-empty; a physical key prefix ends
   *                   with the namespace delimiter)
   * @param clearStamp the clear's stamp ({@code <= 0} records nothing)
   */
  void recordPrefixClear(String prefix, long clearStamp);

  /**
   * Batch form of {@link #recordPrefixClear(String, long)}: apply several
   * namespace clears as <b>one</b> snapshot swap — one lock acquisition and
   * one copy — instead of one per prefix.
   *
   * <p>Per-entry semantics match the single form: a null or empty prefix and a
   * non-positive stamp are skipped, a stamp the table-wide watermark covers is
   * skipped (that watermark already refuses every write it would), and an
   * equal-or-lower stamp for an already-recorded prefix is ignored. The cap is
   * enforced once for the whole batch, so a burst of clears folds or evicts at
   * most once.
   *
   * <p>The default implementation delegates each entry to
   * {@link #recordPrefixClear(String, long)}; an implementation may override it
   * to apply the batch as one snapshot swap.
   *
   * @param clears merged {@code prefix -> clear stamp} entries; callers that
   *               receive duplicate prefixes keep the highest stamp (never
   *               {@code null})
   */
  default void recordPrefixClears(Map<String, Long> clears) {
    clears.forEach(this::recordPrefixClear);
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
   * The one value-update tail shared by the local write, load and peer-sync
   * paths.
   *
   * <p>A logically expired resident is treated as absent: the new value gets a
   * fresh lifetime instead of inheriting the dead entry's deadline. An
   * existing elevated entry (HOT or COOL) keeps its state, decision lease and
   * hard deadline; only the value, data stamp, birth stamp and capped soft
   * window move. A NORMAL entry is rewritten with the caller's resolved TTLs,
   * optionally promoted to local HOT.
   *
   * @param existing     the resident entry, or {@code null}
   * @param value        the value to store (never {@code null})
   * @param version      the data stamp of the new value
   * @param hardTtlMs    the normal hard TTL for a fresh/NORMAL entry
   * @param softTtlMs    the normal soft TTL for a fresh/NORMAL entry
   * @param promoteHot   whether this write may promote a NORMAL entry to local HOT
   * @return the entry to store
   */
  CacheEntry writeValue(
    @Nullable CacheEntry existing,
    Object value,
    long version,
    long hardTtlMs,
    long softTtlMs,
    boolean promoteHot
  );

  /**
   * The COOL rewrite shared by the Worker decision handler and the read-path
   * lease reassertion. The caller owns the version guard.
   *
   * @param existing the entry to cool
   * @param stamp    the COOL decision lease to store
   * @return the COOL entry
   */
  CacheEntry cool(CacheEntry existing, DecisionStamp stamp);

  /**
   * Reverts an entry to the NORMAL lifecycle without restarting its lifetime:
   * normal TTL durations, the earlier of the normal deadline and the current
   * deadline, and a cleared decision lease.
   *
   * @param existing the entry to demote
   * @return the NORMAL entry
   */
  CacheEntry demoteToNormal(CacheEntry existing);
}
