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

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.TtlPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryMutations;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryMutations.Applied;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.sharding.ConsistentHashRing;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.DecisionPlaneSupport;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.clock.StampClock;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * Default implementation of {@link WorkerDecisionHandler} that performs loader-backed
 * HOT promotion and COOL downgrade with version guarding, and
 * {@link WorkerDecisionHook} dispatch.
 */
@Slf4j
public class DefaultWorkerDecisionHandler implements WorkerDecisionHandler {

  /** Loads the authoritative value for a key through the caller-supplied
   * cluster loader (optional: {@code null} when the application provides none).
   * Used during HOT promotion to fetch the authoritative value before writing
   * to L1.
   */
  @Nullable
  private final CacheLoader<String, Object> clusterLoader;

  /** Local Caffeine L1 cache — target for HOT promotion and COOL downgrade operations.
   * Accessed atomically via {@code asMap().compute()} for thread-safe updates.
   */
  private final Cache<String, CacheEntry> caffeineCache;

  /** Optional lifecycle hooks for Worker decision events. Never null. */
  private final List<WorkerDecisionHook> workerHooks;

  /** Computes hard and soft expiry timestamps for HOT-promoted and default-TTL entries. */
  private final EntryLifecycle entryLifecycle;

  /**
   * Consistent-hash ring for shard-ownership checks on the COOL path. {@code null}
   * disables the stale-cross-Worker COOL guard (fail-open to the pure version-guard
   * behaviour) — e.g. in app-only deployments without the AMQP sharding beans.
   */
  @Nullable
  private final ConsistentHashRing ring;

  /**
   * Cluster health view backing the ownership check: liveness of the entry's owning
   * Worker and the records the ring probes while routing. {@code null} disables the
   * stale-cross-Worker COOL guard (fail-open).
   */
  @Nullable
  private final HealthView healthView;

  /**
   * Source of the {@code dataVersion} stamp for a HOT-create entry (design decisions: version stamps).
   * The stamp is taken before the authoritative load, so the created entry is
   * a lower bound and cannot outrank a write that landed during the load.
   */
  private final StampClock stampClock;

  /** The stateless TTL/expiry arithmetic, resolved once from the lifecycle it belongs to. */
  private final TtlPolicy ttlPolicy;

  public DefaultWorkerDecisionHandler(
    Cache<String, CacheEntry> caffeineCache,
    @Nullable CacheLoader<String, Object> clusterLoader,
    EntryLifecycle entryLifecycle,
    StampClock stampClock,
    List<WorkerDecisionHook> workerHooks
  ) {
    this(caffeineCache, clusterLoader, entryLifecycle, stampClock, workerHooks, null, null);
  }

  /**
   * Creates a decision handler with shard-ownership awareness on the COOL path.
   *
   * <p>When {@code ring} and {@code healthView} are both non-null,
   * {@link #handleCool} additionally rejects a stale cross-Worker COOL that would
   * kill a live owner's in-force HOT while the ring still routes the key to that
   * owner (scale re-balance ghost-COOL kill — see
   * {@link #isStaleCrossNodeCool}). Either argument may be {@code null}
   * to disable that refinement (fail-open to the pure version-guard behaviour).
   *
   * @param caffeineCache     the local Caffeine L1 cache; must not be {@code null}
   * @param clusterLoader     the authoritative value loader; may be {@code null}
   *                          (loader-backed HOT warm-up is then skipped)
   * @param entryLifecycle    the entry lifecycle for TTL computation; must not be {@code null}
   * @param stampClock        the data-stamp clock for HOT-create entries; must not be {@code null}
   * @param workerHooks       the lifecycle hooks; may be {@code null} (treated as empty)
   * @param ring       the consistent-hash ring manager; may be {@code null}
   * @param healthView        the cluster health view; may be {@code null}
   */
  public DefaultWorkerDecisionHandler(
    Cache<String, CacheEntry> caffeineCache,
    @Nullable CacheLoader<String, Object> clusterLoader,
    EntryLifecycle entryLifecycle,
    StampClock stampClock,
    List<WorkerDecisionHook> workerHooks,
    @Nullable ConsistentHashRing ring,
    @Nullable HealthView healthView
  ) {
    this.ring = ring;
    this.stampClock = stampClock;
    this.healthView = healthView;
    this.caffeineCache = caffeineCache;
    this.clusterLoader = clusterLoader;
    this.entryLifecycle = entryLifecycle;
    this.ttlPolicy = entryLifecycle.ttlPolicy();
    this.workerHooks = workerHooks != null ? workerHooks : List.of();
  }

  /**
   * One HOT candidate: the value to promote, the data version it carries, and whether that
   * value was loaded by this call (as opposed to an L1 fallback).
   */
  private record HotCandidate(Object value, long stamp, boolean fresh) {}

  /**
   * Resolves the value a HOT decision promotes: the authoritative load when a loader is
   * configured, otherwise (or after a clean miss) the resident L1 entry's value. The
   * returned candidate is already checked for usability, so the caller has nothing left
   * to validate.
   *
   * <p>design decisions: version stamps: the create stamp is allocated BEFORE the
   * authoritative load. A pre-read stamp is a lower bound, so a write that lands during
   * the load carries a strictly greater stamp and is not outranked by this create. Its
   * independence from the DCL snapshot is the anti-resurrection guard: the snapshot's
   * entry can be removed by a concurrent INVALIDATE while the load is in flight, and a
   * stamp allocated after that invalidation would outrank the tombstone and resurrect
   * the pre-invalidation value.
   *
   * <p>A fallback reuses the resident value's own data version (0 = unknown): it is an
   * existing value, never a new one, so it must not be re-signed with a fresher stamp
   * after a concurrent invalidation removed it.
   *
   * @param key      the key being promoted
   * @param snapshot the DCL snapshot, reused when no loader I/O separates the read
   * @return the candidate, or {@code null} when neither source holds a usable value
   */
  @Nullable
  private HotCandidate resolveHotCandidate(String key, @Nullable CacheEntry snapshot) {
    if (clusterLoader != null) {
      long preReadStamp = stampClock.nextStamp();
      Object loaded = loadAuthoritative(key);
      if (loaded != null) {
        return new HotCandidate(loaded, preReadStamp, true);
      }
    }
    // Source outage fallback: any existing L1 entry value is promotable — get() serves
    // these values regardless of how they were stamped, so the HOT promotion must not be
    // stricter than the read path. With no loader there is no I/O between the DCL snapshot
    // and here, so the snapshot is reused as-is; with a loader the fallback is re-read
    // because that I/O can let a concurrent INVALIDATE remove the resident. A logically
    // expired resident is not served by the read path either (it is a miss there), so
    // promoting it would pin a dead value for the whole HOT TTL.
    CacheEntry fallback = clusterLoader != null ? caffeineCache.asMap().get(key) : snapshot;
    if (fallback != null && fallback.getValue() != null && !ttlPolicy.isLogicallyExpired(fallback)) {
      return new HotCandidate(fallback.getValue(), fallback.getDataVersion(), false);
    }
    return null;
  }

  /**
   * Promotes a cache key to {@link KeyState#HOT} with extended TTL and active soft
   * expiration, following a Worker HOT decision.
   *
   * <p><b>Promotion flow:</b>
   * <ol>
   *   <li><b>DCL check 1:</b> Fast-path version guard ({@link VersionGuard#shouldSkipForHot})
   *       against the existing L1 entry. If the resident's decision already outranks the
   *       incoming one, this is a no-op.</li>
   *   <li><b>Load:</b> Loads the authoritative value through the optional cluster loader.
   *       When the loader is absent, fails or returns no value, the existing L1 entry
   *       value is the fallback; a logically expired resident is not used.</li>
   *   <li><b>DCL check 2:</b> Second version guard inside the atomic {@code compute} to
   *       prevent overwriting a newer decision that arrived during the load.</li>
   *   <li><b>Write:</b> Rewrites the entry as {@code KeyState.HOT} with the loaded value
   *       or, when that value is not provably fresher, the decision metadata and HOT TTLs
   *       only (design decisions: hot state).</li>
   * </ol>
   *
   * <p>If no value is available from the cluster loader <em>and</em> no usable
   * entry exists in L1, the promotion is aborted — there is nothing to cache.
   *
   * <p>An L1 fallback promotes only the resident that supplied it. If that resident disappears
   * between the fallback read and the atomic write, the promotion is skipped: re-creating the
   * value would revive a hard-expired value under a fresh HOT TTL and a new birth stamp.
   *
   * @param message the Worker message containing the HOT decision; must not be null
   */
  @Override
  public void handleHot(WorkerMessage message) {
    String key = message.cacheKey();

    // DCL first check – cheap, outside the compute lock. With no cluster loader the
    // snapshot is also the L1 fallback value read below: no I/O separates the two,
    // so the second lookup is saved. With a loader the fallback is re-read after
    // the load, because that I/O can let a concurrent INVALIDATE remove the resident.
    // asMap().get: a decision-plane pre-check is not a caller read, so it must not
    // record a hit/miss. It still feeds Caffeine's read buffer and frequency sketch.
    CacheEntry snapshot = caffeineCache.asMap().get(key);
    if (snapshot != null && VersionGuard.shouldSkipForHot(snapshot, message.stamp())) {
      log.debug("handleHot: HotKey already up-to-date in L1: {}", key);
      fireOnHotSkipped(message, HotSkipReason.VERSION_STALE);
      return;
    }

    HotCandidate candidate = resolveHotCandidate(key, snapshot);
    if (candidate == null) {
      log.debug("handleHot: HotKey value not found and no usable L1 entry: {}", key);
      fireOnHotSkipped(message, HotSkipReason.VALUE_NOT_FOUND);
      return;
    }

    long hotHardTtl = ttlPolicy.effectiveHotHardTtlMs();
    long hotSoftTtl = ttlPolicy.effectiveHotSoftTtlMs();
    DecisionStamp stamp = message.stamp();
    // The compute has no second return value: the refusal reason travels in this
    // one-element holder so the terminal hook can name it.
    HotSkipReason[] skipReason = { HotSkipReason.VALUE_NOT_FOUND };
    Applied applied = EntryMutations.apply(caffeineCache, key, existing -> {
      // DCL second check – atomic with the write. A compute that observes the same
      // immutable snapshot the pre-check already cleared needs no second comparison.
      if (existing != null) {
        if (existing != snapshot && VersionGuard.shouldSkipForHot(existing, stamp)) {
          skipReason[0] = HotSkipReason.VERSION_STALE;
          return existing;
        }

        // The resident is unusable (no value or logically expired) when
        // promoteExisting hands it back unchanged: there is nothing to report as applied.
        return promoteExisting(key, existing, stamp, candidate, hotHardTtl, hotSoftTtl);
      }

      // The resident disappeared after the fallback read that selected its value.
      // Re-creating it here would resurrect the value under a fresh HOT TTL and a
      // new birth stamp, even though the source refused to load it again. Only a
      // value loaded during this call may create a new entry.
      return candidate.fresh() ? createHot(key, stamp, candidate, hotHardTtl, hotSoftTtl) : null;
    });
    // A refused promotion still owes the hook contract one terminal callback. The inner
    // DCL guard, the tombstone-refused create and the unusable-resident refusal all land
    // here; EntryMutations reports "changed" by reference identity, so no re-read is needed.
    if (applied == null) {
      fireOnHotSkipped(message, skipReason[0]);
      return;
    }
    log.debug("HotKey promoted by Worker: {}", key);
    fireAfterHotPromotion(message, applied.current());
  }

  /**
   * In-place promotion of an existing L1 entry (design decisions: hot state), guarded on
   * the <b>data</b> plane as well as the decision plane.
   *
   * <p>The loaded value is written only when it is provably fresh: a positive load stamp,
   * no newer resident and no invalidation tombstone at that stamp. Otherwise only the
   * decision metadata (state, decision stamp, hot TTLs) is promoted and the resident
   * value and version survive. The tombstone probe belongs to the value-rewrite guard
   * only, so a no-loader or L1-fallback promotion still promotes a live resident.
   *
   * @return the entry left in the table (a promoted resident, an untouched
   *         resident when promotion must not rewrite the value, or a fresh
   *         rewrite)
   */
  private CacheEntry promoteExisting(
    String key,
    CacheEntry entry,
    DecisionStamp stamp,
    HotCandidate candidate,
    long hotHardTtl,
    long hotSoftTtl
  ) {
    if (
      candidate.fresh() &&
      entry.getDataVersion() <= candidate.stamp() &&
      !entryLifecycle.staleAgainstInvalidation(key, candidate.stamp())
    ) {
      return entryLifecycle
        .editEntry(entry)
        .value(candidate.value())
        .version(candidate.stamp())
        .decision(stamp)
        .keyState(KeyState.HOT)
        .ttl(hotHardTtl, hotSoftTtl)
        .birthStampSec(TimeSource.monotonicSecondsSinceBoot())
        .build();
    }

    // The load is not provably fresher than the resident: promote the decision
    // only. An unusable resident (logically expired — its value is not served by
    // the read path either, and a load-refused promotion must not pin it at the
    // hot TTL) is left untouched — no rewrite, no hook.
    if (entry.getValue() != null && !ttlPolicy.isLogicallyExpired(entry)) {
      EntryDraft promoted = entryLifecycle.editEntry(entry).decision(stamp).keyState(KeyState.HOT);
      if (candidate.fresh() || clusterLoader == null) {
        // A loaded value, or a deployment with no loader (the resident is authoritative),
        // adopts the Worker's HOT TTLs. A loader failure fallback keeps its existing deadlines:
        // extending them would make stale data look freshly loaded and suppress its next soft
        // refresh for a whole hot window.
        promoted.ttl(hotHardTtl, hotSoftTtl);
      }
      return promoted.build();
    }
    return entry;
  }

  /**
   * Creates the HOT entry when the key has no resident and this call loaded a fresh value
   * (design decisions: hot state). The create stamp is the value's age lower bound, never a fresh
   * commit-time version: a freshly loaded value uses the pre-read stamp (allocated before the
   * load, hence at or below any write or invalidation that raced it). The tombstone check refuses
   * the pre-invalidation value when an INVALIDATE landed during the load.
   *
   * @return the created entry, or {@code null} when the promotion was refused
   */
  private CacheEntry createHot(
    String key,
    DecisionStamp stamp,
    HotCandidate candidate,
    long hotHardTtl,
    long hotSoftTtl
  ) {
    if (entryLifecycle.staleAgainstInvalidation(key, candidate.stamp())) {
      log.debug("handleHot: promotion refused, key was invalidated during the load: {}", key);
      return null;
    }
    // Preserve the decision identity on the create path: without nodeId/epoch,
    // VersionGuard.shouldSkipForWorker treats every subsequent decision as
    // cross-Worker (unconditional accept), so out-of-order replayed messages
    // could overwrite this entry.
    return entryLifecycle
      .newEntry()
      .value(candidate.value())
      .version(candidate.stamp())
      .decision(stamp)
      .ttl(
        hotHardTtl,
        hotSoftTtl,
        ttlPolicy.normalHardBaseline(ttlPolicy.effectiveHardTtlMs()),
        ttlPolicy.effectiveSoftTtlMs()
      )
      .keyState(KeyState.HOT)
      .build();
  }

  /**
   * Downgrades a cache key from {@link KeyState#HOT} to {@link KeyState#COOL},
   * following a Worker COOL decision.
   *
   * <p>This effectively reverts the HOT promotion:
   * <ul>
   *   <li>The cached value, {@code dataVersion}, key state, and
   *       {@code decisionVersion} are all preserved — the data remains available.</li>
   *   <li>The hard TTL is reset to the normal value (from
   *       {@code normalHardTtlMs}, or a 120s protection default).</li>
   *   <li>The soft TTL is reset to the normal value (from
   *       {@code normalSoftTtlMs}, or a 60s protection default), so the entry
   *       eventually soft-expires.</li>
   *   <li>Read paths still refresh COOL entries on soft expiry
   *       (SOFT_REFRESH, when a reader is present); a successful refresh
   *       downgrades the entry to NORMAL — the local read traffic shows the
   *       key is still active locally, so it returns to the ordinary local
   *       lifecycle. After that, the entry follows the NORMAL lifecycle:
   *       soft-expired accesses re-arm the same background refresh (the
   *       stale-while-revalidate contract of the soft TTL), the hard TTL
   *       stays the absolute bound (design decisions: expiry and refresh), and a Worker broadcast can
   *       still override at any time via {@code decisionVersion}.</li>
   * </ul>
   *
   * <p>If no existing entry is present in L1, the COOL decision is a no-op —
   * there is nothing to downgrade. The entry will be evicted naturally by
   * Caffeine's capacity policy or a subsequent invalidation.
   *
   * @param message the Worker message containing the COOL decision; must not be null
   */
  @Override
  public void handleCool(WorkerMessage message) {
    Applied applied = coolApply(message);
    // EntryMutations reports the rewrite by reference identity against the compute's
    // own result, so a concurrent INVALIDATE/eviction cannot deliver a stale entry.
    // COOL never removes, so a non-null Applied always carries the rewritten entry.
    if (applied == null) {
      fireOnCoolSkipped(message);
      return;
    }
    log.debug("HotKey cooled by Worker: {}", message.cacheKey());
    fireAfterCoolDowngrade(message, applied.current());
  }

  /**
   * Coalesces a run of consecutive COOL decisions for one key. COOL never loads, creates
   * or removes an entry, so only the last decision that passes the version guard can
   * affect the final state; the rest are reported as skipped. The fold starts from the
   * resident decision stamp and advances it with each accepted candidate, exactly as the
   * sequential applies would.
   *
   * <p>Only a run whose messages come from the same Worker node is coalesced. The
   * ghost-COOL guard depends on the resident's HOT owner and the sender's node: an
   * earlier same-owner COOL can legitimately demote the entry and disable the guard for a
   * later cross-node COOL, so dropping it could leave the entry HOT. A mixed-node run
   * therefore falls back to the sequential per-message applies.
   *
   * <p>The final winner still goes through {@link #handleCool}, which re-checks the guard
   * and the logical-expiry state inside the real entry snapshot. Every dropped message
   * receives one terminal {@code onCoolSkipped} callback; the winner receives
   * {@code afterCoolDowngrade} or {@code onCoolSkipped} from {@link #handleCool}.
   *
   * @param batch the COOL run, never empty, all messages for the same key
   */
  @Override
  public void handleCoolBatch(List<WorkerMessage> batch) {
    if (batch.size() == 1) {
      handleCool(batch.get(0));
      return;
    }

    if (!sameDecisionNode(batch)) {
      for (WorkerMessage message : batch) {
        handleCool(message);
      }
      return;
    }

    CacheEntry snapshot = caffeineCache.asMap().get(batch.get(0).cacheKey());
    DecisionStamp effective = snapshot != null ? snapshot.decision() : null;
    WorkerMessage winner = null;
    for (WorkerMessage candidate : batch) {
      if (VersionGuard.shouldSkipForWorker(effective, candidate.stamp())) {
        fireOnCoolSkipped(candidate);
        continue;
      }

      if (winner != null) {
        // A later decision supersedes the not-yet-applied winner; it is covered, not lost.
        fireOnCoolSkipped(winner);
      }
      winner = candidate;
      effective = candidate.stamp();
    }
    if (winner != null) {
      handleCool(winner);
    }
  }

  /** Whether every decision of the run was issued by the same Worker node. */
  private static boolean sameDecisionNode(List<WorkerMessage> batch) {
    String nodeId = batch.get(0).stamp().decisionNodeId();
    for (int i = 1; i < batch.size(); i++) {
      if (!Objects.equals(nodeId, batch.get(i).stamp().decisionNodeId())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Rejection-path COOL application (design decisions: decision delivery): the per-key
   * ordered dispatcher refused to enqueue the decision, and nothing re-sends a COOL — a dropped
   * one pins the entry at its HOT TTL until the hard expiry. The receiver therefore applies it inline on the AMQP
   * consumer thread instead of losing it.
   *
   * <p>Runs the same in-memory compute as {@link #handleCool} — pure memory, no
   * cluster-loader I/O (only the HOT path loads) — and deliberately skips the
   * hook dispatch: hooks are user code that may block, and the consumer thread
   * must never wait on it. One degraded decision fires no
   * {@code afterCoolDowngrade}/{@code onCoolSkipped} hook.
   *
   * @param message the Worker message containing the COOL decision; must not be null
   */
  @Override
  public void handleCoolInline(WorkerMessage message) {
    coolApply(message);
  }

  /**
   * The COOL downgrade compute shared by {@link #handleCool} (dispatcher task,
   * with hooks) and {@link #handleCoolInline} (rejection fallback, no hooks).
   *
   * <p>The shard-owner inputs of the ghost-COOL guard are resolved <b>outside</b>
   * the bin lock when possible: the ring lookup probes the health view through a
   * method reference ({@link ConsistentHashRing#locateNode(String, HealthView)}
   * passes {@code healthView::isAlive}, so no alive set is materialised), but a
   * slow-path probe walk can still touch several health records; doing that
   * inside the compute would serialise same-bin reads behind it. For the exact
   * entry reference the outer verdict is reused; the compute re-judges only
   * logical expiry. Any other observed entry is immutable but was not the input
   * the verdict was derived from, so the full guard runs again. Liveness and
   * ring routing are point-in-time inputs, not fields of the immutable entry;
   * they are not re-probed under the bin lock.
   *
   * @param message the incoming COOL message; must not be {@code null}
   * @return the applied downgrade, or {@code null} when the message was settled without one
   */
  @Nullable
  private Applied coolApply(WorkerMessage message) {
    String key = message.cacheKey();
    // DCL fast check, outside the bin lock: same-key Worker decisions are
    // serialized by the ordered dispatcher, so a snapshot that is absent,
    // decision-stale, or logically expired settles the message without the
    // compute — the compute's own guards would return the entry unchanged.
    // asMap().get keeps this decision-plane probe out of the caller hit/miss stats.
    CacheEntry snapshot = caffeineCache.asMap().get(key);
    if (snapshot == null || ttlPolicy.isLogicallyExpired(snapshot) || shouldSkipCool(snapshot, message)) {
      return null;
    }

    DecisionStamp stamp = message.stamp();
    return EntryMutations.apply(caffeineCache, key, existing -> {
      if (existing == null) {
        return null;
      }

      // The outer pre-check settled `snapshot`; both guards below depend on the
      // exact entry they were evaluated against. CacheEntry values are immutable
      // and replaced, never mutated, so any other reference needs a fresh
      // comparison under the bin lock. Expiry is re-judged for the same reason:
      // it is a function of the clock, not of the entry alone.
      if (ttlPolicy.isLogicallyExpired(existing) || (existing != snapshot && shouldSkipCool(existing, message))) {
        return existing;
      }

      // Decision re-stamp + normal-lifecycle TTLs + COOL state, with the COOL
      // protection window owned by TtlPolicy.
      return entryLifecycle.cool(existing, stamp);
    });
  }

  /**
   * The COOL admission guard, shared by the lock-free pre-check and the in-compute
   * re-check: a decision-stale entry or a stale cross-Worker kill is settled without
   * a rewrite. The ghost-COOL log lives here so both checks report the same line.
   *
   * @param entry   the entry the COOL would rewrite
   * @param message the incoming COOL message
   * @return {@code true} when the COOL must be skipped
   */
  private boolean shouldSkipCool(CacheEntry entry, WorkerMessage message) {
    if (VersionGuard.shouldSkipForWorker(entry, message.stamp())) {
      return true;
    }
    // Scale re-balance ghost-COOL kill: a delayed EvictStale COOL from the previous
    // owner must not downgrade the new owner's in-force HOT while the ring still
    // routes the key to that owner and the owner is alive.
    if (isStaleCrossNodeCool(entry, message)) {
      log.debug(
        "handleCool: rejecting stale cross-Worker COOL for key={} from nodeId={} (owner={} still owns the shard)",
        message.cacheKey(),
        message.stamp().decisionNodeId(),
        entry.getDecisionNodeId()
      );
      return true;
    }
    return false;
  }

  /**
   * Ownership check behind the ghost-COOL guard: a cross-Worker COOL is a stale
   * kill when the entry is an in-force HOT owned by a <b>different, still-live</b>
   * Worker <b>of the incarnation that issued the HOT</b>, and the consistent-hash
   * ring still routes the key to that owner.
   *
   * <p>Any missing signal fails open — the COOL applies exactly as the plain
   * version guard would allow it: a {@code null} ring or health view, a non-HOT
   * entry, an empty owner, the sender itself, a dead or restarted owner, or a ring
   * that no longer points at the owner. Liveness and incarnation are checked
   * before the ring probe: it is an O(1) record lookup, while the probe walks
   * health records, and a dead owner's HOT is legitimately coolable (design
   * decisions: version comparison).
   *
   * @param entry   the entry currently in L1; must not be {@code null}
   * @param message the incoming COOL message; must not be {@code null}
   * @return {@code true} when the COOL is a stale cross-Worker kill and must be skipped
   */
  private boolean isStaleCrossNodeCool(CacheEntry entry, WorkerMessage message) {
    if (ring == null || healthView == null || entry.getKeyState() != KeyState.HOT) {
      return false;
    }

    String owner = entry.getDecisionNodeId();
    String sender = message.stamp().decisionNodeId();
    if (owner == null || owner.isEmpty() || Objects.equals(owner, sender)) {
      return false;
    }
    if (!healthView.isCurrentIncarnation(owner, entry.getDecisionEpoch())) {
      return false;
    }
    // The published ring may predate the heartbeat that made the owner live: reconcile it
    // first, or a newly joined owner looks like a dead shard and its ghost-COOL guard is
    // bypassed.
    ring.rebuildIfPossible(healthView);
    return owner.equals(ring.locateNode(message.cacheKey(), healthView));
  }

  /**
   * Loads the authoritative value through the optional cluster loader. A clean miss, a
   * declined value and a loader failure all answer {@code null}, so the caller falls back
   * to the existing L1 entry value.
   *
   * @param key the cache key to load
   * @return the loaded value, or {@code null} on a clean miss or a loader failure
   */
  @Nullable
  private Object loadAuthoritative(String key) {
    return DecisionPlaneSupport.loadAuthoritative(
      clusterLoader,
      key,
      log,
      "handleHot: load failed for key={}, trying L1 entry fallback"
    );
  }

  /** Reports an applied HOT promotion to every registered hook. */
  private void fireAfterHotPromotion(WorkerMessage message, CacheEntry entry) {
    if (workerHooks.isEmpty()) {
      return;
    }
    fireHooks("afterHotPromotion", message.cacheKey(), hook ->
      hook.afterHotPromotion(message.cacheKey(), message, entry)
    );
  }

  /** Reports an applied COOL downgrade to every registered hook. */
  private void fireAfterCoolDowngrade(WorkerMessage message, CacheEntry entry) {
    if (workerHooks.isEmpty()) {
      return;
    }
    fireHooks("afterCoolDowngrade", message.cacheKey(), hook ->
      hook.afterCoolDowngrade(message.cacheKey(), message, entry)
    );
  }

  /** Reports a skipped HOT decision to every registered hook, with the refusal reason. */
  private void fireOnHotSkipped(WorkerMessage message, HotSkipReason reason) {
    if (workerHooks.isEmpty()) {
      return;
    }
    fireHooks("onHotSkipped", message.cacheKey(), hook -> hook.onHotSkipped(message.cacheKey(), message, reason));
  }

  /**
   * Reports a skipped COOL decision to every registered hook;
   * {@link WorkerDecisionHook#onCoolSkipped} owns the skip causes.
   */
  private void fireOnCoolSkipped(WorkerMessage message) {
    if (workerHooks.isEmpty()) {
      return;
    }
    fireHooks("onCoolSkipped", message.cacheKey(), hook -> hook.onCoolSkipped(message.cacheKey(), message));
  }

  /**
   * One owner of the hook-dispatch contract: every hook failure is caught and
   * WARN-logged with the key — a broken hook never breaks the decision apply.
   * The fire methods check for an empty list before building the capturing callback,
   * so the default no-hook deployment allocates nothing.
   *
   * @param hookName the callback name (for the WARN line)
   * @param key      the key the decision touched
   * @param callback the hook callback to invoke for each registered hook
   */
  private void fireHooks(String hookName, String key, Consumer<WorkerDecisionHook> callback) {
    DecisionPlaneSupport.fireHooks(workerHooks, "WorkerDecisionHook", hookName, key, callback, log);
  }
}
