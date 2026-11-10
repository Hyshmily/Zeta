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
 * Default {@link WorkerContext}: loader-backed HOT promotion and guarded COOL downgrade over
 * the one L1 table, with one terminal hook per settled decision (the inline rejection
 * fallback reports none).
 *
 * <p>HOT uses double-checked locking around the authoritative load: a lock-free pre-check
 * skips an already-outranked decision without I/O, and {@link #settleHot} re-checks the
 * decision stamp (and, for a value rewrite, the data version and the invalidation
 * tombstones) inside the atomic entry compute. COOL is pure in-memory: version guard,
 * ghost-COOL ownership, TTL rewrite.
 */
@Slf4j
public class DefaultWorkerContext implements WorkerContext, WorkerControl {

  /** Loads the authoritative value for a key through the caller-supplied cluster loader
   * (optional: {@code null} when the application provides none). */
  @Nullable
  private final CacheLoader<String, Object> clusterLoader;

  /** Local Caffeine L1 cache — target for HOT promotion and COOL downgrade operations. */
  private final Cache<String, CacheEntry> cache;

  /** Lifecycle hooks for Worker decision events. Never null. */
  private final List<WorkerDecisionHook> hooks;

  /** Computes hard and soft expiry timestamps for HOT-promoted and default-TTL entries. */
  private final EntryLifecycle entryLifecycle;

  /**
   * Consistent-hash ring for shard-ownership checks on the COOL path. {@code null} disables
   * the stale-cross-Worker COOL guard (fail-open to the pure version-guard behaviour).
   */
  @Nullable
  private final ConsistentHashRing ring;

  /**
   * Cluster health view backing the ownership check. {@code null} disables the stale
   * cross-Worker COOL guard (fail-open).
   */
  @Nullable
  private final HealthView healthView;

  /**
   * Source of the {@code dataVersion} stamp for a HOT-create entry (design decisions:
   * version stamps). The stamp is taken before the authoritative load, so the created entry
   * is a lower bound and cannot outrank a write that landed during the load.
   */
  private final StampClock stampClock;

  /** The stateless TTL/expiry arithmetic, resolved once from the lifecycle it belongs to. */
  private final TtlPolicy ttlPolicy;

  public DefaultWorkerContext(
    Cache<String, CacheEntry> cache,
    @Nullable CacheLoader<String, Object> clusterLoader,
    EntryLifecycle entryLifecycle,
    StampClock stampClock,
    List<WorkerDecisionHook> hooks
  ) {
    this(cache, clusterLoader, entryLifecycle, stampClock, hooks, null, null);
  }

  /**
   * Creates a context with shard-ownership awareness on the COOL path.
   *
   * <p>When {@code ring} and {@code healthView} are both non-null, {@link #cool} additionally
   * rejects a stale cross-Worker COOL that would kill a live owner's in-force HOT while the
   * ring still routes the key to that owner (see {@link #isStaleCrossNodeCool}). Either
   * argument may be {@code null} to disable that refinement.
   *
   * @param cache  the local Caffeine L1 cache; must not be {@code null}
   * @param clusterLoader  the authoritative value loader; may be {@code null}
   * @param entryLifecycle the entry lifecycle for TTL computation; must not be {@code null}
   * @param stampClock     the data-stamp clock for HOT-create entries; must not be {@code null}
   * @param hooks    the lifecycle hooks; may be {@code null} (treated as empty)
   * @param ring           the consistent-hash ring manager; may be {@code null}
   * @param healthView     the cluster health view; may be {@code null}
   */
  public DefaultWorkerContext(
    Cache<String, CacheEntry> cache,
    @Nullable CacheLoader<String, Object> clusterLoader,
    EntryLifecycle entryLifecycle,
    StampClock stampClock,
    @Nullable List<WorkerDecisionHook> hooks,
    @Nullable ConsistentHashRing ring,
    @Nullable HealthView healthView
  ) {
    this.ring = ring;
    this.stampClock = stampClock;
    this.healthView = healthView;
    this.cache = cache;
    this.clusterLoader = clusterLoader;
    this.entryLifecycle = entryLifecycle;
    this.ttlPolicy = entryLifecycle.ttlPolicy();
    this.hooks = hooks != null ? hooks : List.of();
  }

  @Override
  public HotAction resolveHot(WorkerDecision decision) {
    requireType(decision, WorkerType.HOT);
    String key = decision.cacheKey();

    // Lock-free pre-check: a decision the resident already outranks must not pay the
    // cluster-loader read. The authoritative guard runs again inside settleHot's compute.
    CacheEntry snapshot = cache.asMap().get(key);
    if (snapshot != null && VersionGuard.shouldSkipForHot(snapshot, decision.stamp())) {
      log.debug("resolveHot: HotKey already up-to-date in L1: {}", key);
      return new HotAction.Skip(HotSkipReason.VERSION_STALE);
    }

    HotValue candidate = resolveHotCandidate(key, snapshot);
    if (candidate == null) {
      log.debug("resolveHot: HotKey value not found and no usable L1 entry: {}", key);
      return new HotAction.Skip(HotSkipReason.VALUE_NOT_FOUND);
    }
    return new HotAction.Promote(candidate);
  }

  /**
   * Resolves the value a HOT decision promotes: the authoritative load when a loader is
   * configured, otherwise (or after a clean miss) the resident L1 entry's value.
   *
   * <p>design decisions: version stamps: the create stamp is allocated BEFORE the
   * authoritative load. A pre-read stamp is a lower bound, so a write that lands during the
   * load carries a strictly greater stamp and is not outranked by this create. Its
   * independence from the DCL snapshot is the anti-resurrection guard: the snapshot's entry
   * can be removed by a concurrent INVALIDATE while the load is in flight, and a stamp
   * allocated after that invalidation would outrank the tombstone and resurrect the
   * pre-invalidation value.
   *
   * <p>A fallback reuses the resident value's own data version (0 = unknown): it is an
   * existing value, never a new one, so it must not be re-signed with a fresher stamp after
   * a concurrent invalidation removed it.
   *
   * @param key      the key being promoted
   * @param snapshot the DCL snapshot, reused when no loader I/O separates the read
   * @return the candidate, or {@code null} when neither source holds a usable value
   */
  @Nullable
  private HotValue resolveHotCandidate(String key, @Nullable CacheEntry snapshot) {
    if (clusterLoader != null) {
      long preReadStamp = stampClock.nextStamp();
      Object loaded = loadAuthoritative(key);
      if (loaded != null) {
        return HotValue.loaded(loaded, preReadStamp);
      }
    }
    // Source outage fallback: any existing L1 entry value is promotable — get() serves
    // these values regardless of how they were stamped, so the HOT promotion must not be
    // stricter than the read path. With no loader there is no I/O between the DCL snapshot
    // and here, so the snapshot is reused as-is; with a loader the fallback is re-read
    // because that I/O can let a concurrent INVALIDATE remove the resident. A logically
    // expired resident is not served by the read path either (it is a miss there), so
    // promoting it would pin a dead value for the whole HOT TTL.
    CacheEntry fallback = clusterLoader != null ? cache.asMap().get(key) : snapshot;
    if (fallback != null && fallback.getValue() != null && !ttlPolicy.isLogicallyExpired(fallback)) {
      return HotValue.resident(fallback.getValue(), fallback.getDataVersion());
    }
    return null;
  }

  @Override
  public void settleHot(WorkerDecision decision, HotAction action) {
    requireType(decision, WorkerType.HOT);
    if (action instanceof HotAction.Skip skip) {
      fireOnHotSkipped(decision, skip.reason());
      return;
    }

    String key = decision.cacheKey();
    HotValue candidate = ((HotAction.Promote) action).value();
    long hotHardTtl = ttlPolicy.effectiveHotHardTtlMs();
    long hotSoftTtl = ttlPolicy.effectiveHotSoftTtlMs();
    DecisionStamp stamp = decision.stamp();
    Applied applied = EntryMutations.apply(cache, key, existing -> {
      // DCL second check — atomic with the write. A decision stamp on a logically
      // expired entry still orders a same-node replay, so this guard runs first.
      if (existing != null && VersionGuard.shouldSkipForHot(existing, stamp)) {
        return existing;
      }

      // A missing, value-less or logically expired resident is not a live entry: the
      // read path does not serve it, so it must never veto the freshly loaded value or
      // lend its stale TTL baseline to a new HOT entry. Only a value loaded during this
      // call may create one; a fallback value that selected the dead resident is not
      // re-signed with a fresh stamp after a concurrent invalidation.
      if (existing == null || existing.getValue() == null || ttlPolicy.isLogicallyExpired(existing)) {
        if (!candidate.loadedByThisCall()) {
          return existing;
        }
        CacheEntry created = createHot(key, stamp, candidate, hotHardTtl, hotSoftTtl);
        return created != null ? created : existing;
      }

      return promoteExisting(key, existing, stamp, candidate, hotHardTtl, hotSoftTtl);
    });
    // A refused promotion still owes the hook contract one terminal callback. The inner DCL
    // guard, the tombstone-refused create and the unusable-resident refusal all land here;
    // EntryMutations reports the rewrite by reference identity inside the compute.
    if (!applied.changed()) {
      CacheEntry current = applied.current();
      HotSkipReason reason =
        current != null && VersionGuard.shouldSkipForHot(current, stamp)
          ? HotSkipReason.VERSION_STALE
          : HotSkipReason.VALUE_NOT_FOUND;
      fireOnHotSkipped(decision, reason);
      return;
    }
    log.debug("HotKey promoted by Worker: {}", key);
    fireAfterHotPromotion(decision, applied.current());
  }

  /**
   * In-place promotion of a live resident (design decisions: hot state), guarded on the
   * <b>data</b> plane as well as the decision plane. The caller has already excluded a
   * missing, value-less or logically expired entry, so this method always has a value the
   * read path would serve and a valid TTL baseline to keep.
   *
   * <p>The loaded value is written only when it is provably fresh: a positive load stamp,
   * no newer resident and no invalidation tombstone at that stamp. Otherwise only the
   * decision metadata (state, decision stamp, hot TTLs) is promoted and the resident value
   * and version survive. The tombstone probe belongs to the value-rewrite guard only, so a
   * no-loader or L1-fallback promotion still promotes a live resident.
   *
   * @return the promoted entry
   */
  private CacheEntry promoteExisting(
    String key,
    CacheEntry entry,
    DecisionStamp stamp,
    HotValue candidate,
    long hotHardTtl,
    long hotSoftTtl
  ) {
    EntryDraft promoted = entryLifecycle.editEntry(entry).decision(stamp).keyState(KeyState.HOT);
    if (
      candidate.loadedByThisCall() &&
      entry.getDataVersion() <= candidate.dataVersion() &&
      !entryLifecycle.staleAgainstInvalidation(key, candidate.dataVersion())
    ) {
      return promoted
        .value(candidate.value())
        .version(candidate.dataVersion())
        .ttl(hotHardTtl, hotSoftTtl)
        .birthStampSec(TimeSource.monotonicSecondsSinceBoot())
        .build();
    }

    // The load is not provably fresher than the live resident: promote the decision only.
    // A loaded value, or a deployment with no loader (the resident is authoritative),
    // adopts the Worker's HOT TTLs. A loader failure fallback keeps its existing deadlines:
    // extending them would make stale data look freshly loaded and suppress its next soft
    // refresh for a whole hot window.
    if (candidate.loadedByThisCall() || clusterLoader == null) {
      promoted.ttl(hotHardTtl, hotSoftTtl);
    }
    return promoted.build();
  }

  /**
   * Creates the HOT entry when the key has no resident and this call loaded a fresh value
   * (design decisions: hot state). The create stamp is the value's age lower bound, never a
   * fresh commit-time version: a freshly loaded value uses the pre-read stamp (allocated
   * before the load, hence at or below any write or invalidation that raced it). The
   * tombstone check refuses the pre-invalidation value when an INVALIDATE landed during the
   * load.
   *
   * @return the created entry, or {@code null} when the promotion was refused
   */
  private CacheEntry createHot(String key, DecisionStamp stamp, HotValue candidate, long hotHardTtl, long hotSoftTtl) {
    if (entryLifecycle.staleAgainstInvalidation(key, candidate.dataVersion())) {
      log.debug("settleHot: promotion refused, key was invalidated during the load: {}", key);
      return null;
    }
    // Preserve the decision identity on the create path: without nodeId/epoch,
    // VersionGuard.shouldSkipForWorker treats every subsequent decision as
    // cross-Worker (unconditional accept), so out-of-order replayed messages
    // could overwrite this entry.
    return entryLifecycle
      .newEntry()
      .value(candidate.value())
      .version(candidate.dataVersion())
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

  @Override
  public List<WorkerDecision> reduceCool(List<WorkerDecision> batch) {
    if (batch.isEmpty()) {
      return List.of();
    }
    WorkerDecision first = batch.get(0);
    requireType(first, WorkerType.COOL);
    if (batch.size() == 1) {
      return List.of(first);
    }

    String nodeId = first.stamp().decisionNodeId();
    boolean sameNode = true;
    for (int i = 1; i < batch.size(); i++) {
      WorkerDecision decision = batch.get(i);
      requireType(decision, WorkerType.COOL);
      sameNode &= Objects.equals(nodeId, decision.stamp().decisionNodeId());
    }
    if (!sameNode) {
      return List.copyOf(batch);
    }

    CacheEntry snapshot = cache.asMap().get(first.cacheKey());
    DecisionStamp effective = snapshot != null ? snapshot.decision() : null;
    WorkerDecision winner = null;
    for (WorkerDecision candidate : batch) {
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
    return winner == null ? List.of() : List.of(winner);
  }

  @Override
  public void cool(WorkerDecision decision) {
    requireType(decision, WorkerType.COOL);
    Applied applied = coolApply(decision);
    // EntryMutations reports the rewrite by reference identity against the compute's
    // own result, so a concurrent INVALIDATE/eviction cannot deliver a stale entry.
    // COOL never removes, so a changed report always carries the rewritten entry.
    if (!applied.changed()) {
      fireOnCoolSkipped(decision);
      return;
    }
    log.debug("HotKey cooled by Worker: {}", decision.cacheKey());
    fireAfterCoolDowngrade(decision, applied.current());
  }

  @Override
  public void skip(WorkerDecision decision) {
    requireType(decision, WorkerType.COOL);
    fireOnCoolSkipped(decision);
  }

  @Override
  public void coolNow(WorkerDecision decision) {
    requireType(decision, WorkerType.COOL);
    coolApply(decision);
  }

  /**
   * The COOL downgrade compute shared by {@link #cool} (dispatcher task, with hooks) and
   * {@link #coolNow} (rejection fallback, no hooks).
   *
   * <p>The shard-owner inputs of the ghost-COOL guard are resolved <b>outside</b> the bin
   * lock when possible: the ring lookup probes the health view through a method reference
   * ({@link ConsistentHashRing#locateNode(String, HealthView)} passes
   * {@code healthView::isAlive}, so no alive set is materialised), but a slow-path probe walk
   * can still touch several health records; doing that inside the compute would serialise
   * same-bin reads behind it. For the exact entry reference the outer verdict is reused; the
   * compute re-judges only logical expiry. Any other observed entry is immutable but was not
   * the input the verdict was derived from, so the full guard runs again. Liveness and ring
   * routing are point-in-time inputs, not fields of the immutable entry; they are not
   * re-probed under the bin lock.
   *
   * @param decision the incoming COOL decision; must not be {@code null}
   * @return the images the bin lock observed; {@link Applied#changed()} is
   *         {@code false} when the message was settled without a downgrade
   */
  private Applied coolApply(WorkerDecision decision) {
    String key = decision.cacheKey();
    // DCL fast check, outside the bin lock: same-key Worker decisions are
    // serialized by the ordered dispatcher, so a snapshot that is absent,
    // decision-stale, or logically expired settles the message without the
    // compute — the compute's own guards would return the entry unchanged.
    // asMap().get keeps this decision-plane probe out of the caller hit/miss stats.
    CacheEntry snapshot = cache.asMap().get(key);
    if (snapshot == null || ttlPolicy.isLogicallyExpired(snapshot) || shouldSkipCool(snapshot, decision)) {
      return EntryMutations.unchanged(snapshot);
    }

    DecisionStamp stamp = decision.stamp();
    return EntryMutations.apply(cache, key, existing -> {
      if (existing == null) {
        return null;
      }

      // The outer pre-check settled `snapshot`; both guards below depend on the
      // exact entry they were evaluated against. CacheEntry values are immutable
      // and replaced, never mutated, so any other reference needs a fresh
      // comparison under the bin lock. Expiry is re-judged for the same reason:
      // it is a function of the clock, not of the entry alone.
      if (ttlPolicy.isLogicallyExpired(existing) || (existing != snapshot && shouldSkipCool(existing, decision))) {
        return existing;
      }

      // Decision re-stamp + normal-lifecycle TTLs + COOL state, with the COOL
      // protection window owned by TtlPolicy.
      return entryLifecycle.cool(existing, stamp);
    });
  }

  /**
   * The COOL admission guard, shared by the lock-free pre-check and the in-compute re-check:
   * a decision-stale entry or a stale cross-Worker kill is settled without a rewrite. The
   * ghost-COOL log lives here so both checks report the same line.
   *
   * @param entry    the entry the COOL would rewrite
   * @param decision the incoming COOL decision
   * @return {@code true} when the COOL must be skipped
   */
  private boolean shouldSkipCool(CacheEntry entry, WorkerDecision decision) {
    if (VersionGuard.shouldSkipForWorker(entry, decision.stamp())) {
      return true;
    }
    // Scale re-balance ghost-COOL kill: a delayed EvictStale COOL from the previous
    // owner must not downgrade the new owner's in-force HOT while the ring still
    // routes the key to that owner and the owner is alive.
    if (isStaleCrossNodeCool(entry, decision)) {
      log.debug(
        "cool: rejecting stale cross-Worker COOL for key={} from nodeId={} (owner={} still owns the shard)",
        decision.cacheKey(),
        decision.stamp().decisionNodeId(),
        entry.getDecisionNodeId()
      );
      return true;
    }
    return false;
  }

  /**
   * Ownership check behind the ghost-COOL guard: a cross-Worker COOL is a stale kill when the
   * entry is an in-force HOT owned by a <b>different, still-live</b> Worker <b>of the
   * incarnation that issued the HOT</b>, and the consistent-hash ring still routes the key to
   * that owner.
   *
   * <p>Any missing signal fails open — the COOL applies exactly as the plain version guard
   * would allow it: a {@code null} ring or health view, a non-HOT entry, an empty owner, the
   * sender itself, a dead or restarted owner, or a ring that no longer points at the owner.
   * Liveness and incarnation are checked before the ring probe: it is an O(1) record lookup,
   * while the probe walks health records, and a dead owner's HOT is legitimately coolable
   * (design decisions: version comparison).
   *
   * @param entry    the entry currently in L1; must not be {@code null}
   * @param decision the incoming COOL decision; must not be {@code null}
   * @return {@code true} when the COOL is a stale cross-Worker kill and must be skipped
   */
  private boolean isStaleCrossNodeCool(CacheEntry entry, WorkerDecision decision) {
    if (ring == null || healthView == null || entry.getKeyState() != KeyState.HOT) {
      return false;
    }

    String owner = entry.getDecisionNodeId();
    String sender = decision.stamp().decisionNodeId();
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
    return owner.equals(ring.locateNode(decision.cacheKey(), healthView));
  }

  /**
   * Loads the authoritative value through the optional cluster loader. A clean miss, a
   * declined value and a loader failure all answer {@code null}, so the caller falls back to
   * the existing L1 entry value.
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
      "resolveHot: load failed for key={}, trying L1 entry fallback"
    );
  }

  private static void requireType(WorkerDecision decision, WorkerType expected) {
    Objects.requireNonNull(decision, "decision must not be null");
    if (decision.type() != expected) {
      throw new IllegalArgumentException("expected a " + expected + " decision but got " + decision.type());
    }
  }

  /** Reports an applied HOT promotion to every registered hook. */
  private void fireAfterHotPromotion(WorkerDecision decision, CacheEntry entry) {
    if (hooks.isEmpty()) {
      return;
    }
    fireHooks("afterHotPromotion", decision.cacheKey(), hook -> hook.afterHotPromotion(decision, entry));
  }

  /** Reports an applied COOL downgrade to every registered hook. */
  private void fireAfterCoolDowngrade(WorkerDecision decision, CacheEntry entry) {
    if (hooks.isEmpty()) {
      return;
    }
    fireHooks("afterCoolDowngrade", decision.cacheKey(), hook -> hook.afterCoolDowngrade(decision, entry));
  }

  /** Reports a skipped HOT decision to every registered hook, with the refusal reason. */
  private void fireOnHotSkipped(WorkerDecision decision, HotSkipReason reason) {
    if (hooks.isEmpty()) {
      return;
    }
    fireHooks("onHotSkipped", decision.cacheKey(), hook -> hook.onHotSkipped(decision, reason));
  }

  /** Reports a skipped COOL decision to every registered hook. */
  private void fireOnCoolSkipped(WorkerDecision decision) {
    if (hooks.isEmpty()) {
      return;
    }
    fireHooks("onCoolSkipped", decision.cacheKey(), hook -> hook.onCoolSkipped(decision));
  }

  /**
   * One owner of the hook-dispatch contract: every hook failure is caught and WARN-logged
   * with the key — a broken hook never breaks the decision apply. The fire methods check for
   * an empty list before building the capturing callback, so the default no-hook deployment
   * allocates nothing.
   *
   * @param hookName the callback name (for the WARN line)
   * @param key      the key the decision touched
   * @param callback the hook callback to invoke for each registered hook
   */
  private void fireHooks(String hookName, String key, Consumer<WorkerDecisionHook> callback) {
    DecisionPlaneSupport.fireHooks(hooks, "WorkerDecisionHook", hookName, key, callback, log);
  }
}
