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
import io.github.hyshmily.zeta.cache.EntryView;
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

    CacheEntry snapshot = cache.asMap().get(key);
    if (snapshot != null && VersionGuard.shouldSkipForHot(snapshot, decision.stamp())) {
      log.debug("resolveHot: HotKey already up-to-date in L1: {}", key);
      return new HotAction.Skip(HotSkipReason.VERSION_STALE);
    }

    HotValue candidate = resolveHotCandidate(key, snapshot);
    if (candidate == null) {
      log.debug("resolveHot: HotKey value not found and no usable L1 entry: {}", key);
      return new HotAction.Skip(HotSkipReason.NO_PROMOTABLE_VALUE);
    }
    return new HotAction.Promote(candidate);
  }

  @Override
  public @Nullable EntryView view(String cacheKey) {
    return DecisionPlaneSupport.view(cache, cacheKey, ttlPolicy);
  }

  /**
   * Resolves the value a HOT decision promotes: the authoritative load when a loader is
   * configured, otherwise (or after a clean miss) the resident L1 entry's value.
   *
   * <p>The create stamp is allocated before the authoritative load (design decisions: version
   * stamps). A pre-read stamp is a lower bound, so a write that lands during the load carries a
   * strictly greater stamp and is not outranked by this create; a stamp allocated after a
   * concurrent invalidation would outrank the tombstone and resurrect the pre-invalidation
   * value.
   *
   * <p>A fallback reuses the resident value's own data version (0 = unknown): it is an existing
   * value, never a new one, so it must not be re-signed with a fresher stamp. Only a source
   * outage or a clean miss reaches the fallback; a loader that answered and then refused the
   * value through {@code isCacheable} has decided definitively.
   *
   * @param key      the key being promoted
   * @param snapshot the DCL snapshot, reused when no loader I/O separates the read
   * @return the candidate, or {@code null} when neither source holds a usable value
   */
  @Nullable
  private HotValue resolveHotCandidate(String key, @Nullable CacheEntry snapshot) {
    if (clusterLoader != null) {
      long preReadStamp = stampClock.nextStamp();
      DecisionPlaneSupport.LoadResult result = DecisionPlaneSupport.load(
        clusterLoader,
        key,
        log,
        "resolveHot: load failed for key={}, trying L1 entry fallback"
      );
      if (result.value() != null) {
        return HotValue.loaded(result.value(), preReadStamp);
      }
      if (result.declined()) {
        return null;
      }
    }

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
    if (candidate.source() == HotValue.Source.POLICY_SUPPLIED) {
      // A policy-supplied data version is adopted by this node, so it must be mergeable into the
      // local stamp floor: otherwise every later local write allocates a smaller stamp and loses
      // the version guard. The loader admission is re-checked for the same reason SyncPolicy
      // re-checks it — a replacement value never bypasses the deployment's own admission rule.
      if (!admitsValue(key, candidate.value())) {
        fireOnHotSkipped(decision, HotSkipReason.VALUE_DECLINED_BY_LOADER);
        return;
      }
      if (!stampClock.observe(candidate.dataVersion())) {
        log.warn(
          "settleHot: refusing policy-supplied dataVersion {} (outside the local clock's merge range): key={}",
          candidate.dataVersion(),
          key
        );
        fireOnHotSkipped(decision, HotSkipReason.VERSION_UNORDERABLE);
        return;
      }
    }
    long hotHardTtl = ttlPolicy.effectiveHotHardTtlMs();
    long hotSoftTtl = ttlPolicy.effectiveHotSoftTtlMs();
    DecisionStamp stamp = decision.stamp();
    Applied applied = EntryMutations.apply(cache, key, existing -> {
      // DCL second check — atomic with the write. A decision stamp on a logically
      // expired entry still orders a same-node replay, so this guard runs first.
      if (existing != null && VersionGuard.shouldSkipForHot(existing, stamp)) {
        return existing;
      }

      if (existing == null || existing.getValue() == null || ttlPolicy.isLogicallyExpired(existing)) {
        if (!candidate.canWriteValue()) {
          return existing;
        }
        CacheEntry created = createHot(key, stamp, candidate, hotHardTtl, hotSoftTtl);
        return created != null ? created : existing;
      }

      return promoteExisting(key, existing, stamp, candidate, hotHardTtl, hotSoftTtl);
    });

    if (!applied.changed()) {
      CacheEntry current = applied.current();
      HotSkipReason reason =
        current != null && VersionGuard.shouldSkipForHot(current, stamp)
          ? HotSkipReason.VERSION_STALE
          : HotSkipReason.NO_PROMOTABLE_VALUE;
      fireOnHotSkipped(decision, reason);
      return;
    }
    log.debug("HotKey promoted by Worker: {}", key);
    fireAfterHotPromotion(decision, applied.current());
  }

  /**
   * In-place promotion of a live resident (design decisions: hot state), guarded on the
   * <b>data</b> plane as well as the decision plane. The caller has already excluded a
   * missing, value-less or logically expired entry.
   *
   * <p>The writable value (loaded or policy-supplied) is written only when it is provably fresh:
   * a positive data stamp, no newer resident and no invalidation tombstone at that stamp.
   * Otherwise the decision metadata is promoted; the hot TTLs apply only when the soft window
   * is still open and the candidate can write a value or no loader is configured. A resident
   * whose soft window is already closed keeps that closed window: reopening it would hide the
   * pending reload for a whole hot TTL.
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
      candidate.canWriteValue() &&
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

    // Not provably fresher: promote the decision only. A writable candidate (or a no-loader
    // deployment) adopts the HOT TTLs; a loader-failure fallback keeps its deadlines so stale
    // data is not made to look freshly loaded, and a closed soft window stays closed.
    if ((candidate.canWriteValue() || clusterLoader == null) && !ttlPolicy.isSoftExpired(entry)) {
      promoted.ttl(hotHardTtl, hotSoftTtl);
    }
    return promoted.build();
  }

  /**
   * Creates the HOT entry when the key has no resident and the candidate is writable (design
   * decisions: hot state). The create stamp is the value's age lower bound, never a fresh
   * commit-time version; the tombstone check refuses the pre-invalidation value when an
   * INVALIDATE landed during the load.
   *
   * @return the created entry, or {@code null} when the promotion was refused
   */
  @Nullable
  private CacheEntry createHot(String key, DecisionStamp stamp, HotValue candidate, long hotHardTtl, long hotSoftTtl) {
    if (entryLifecycle.staleAgainstInvalidation(key, candidate.dataVersion())) {
      log.debug("settleHot: promotion refused, key was invalidated during the load: {}", key);
      return null;
    }
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
  public @Nullable CoolSkipReason preCheckCool(WorkerDecision decision) {
    requireType(decision, WorkerType.COOL);
    CacheEntry snapshot = cache.asMap().get(decision.cacheKey());
    return snapshot == null ? CoolSkipReason.NOT_FOUND_OR_EXPIRED : coolSkipReason(snapshot, decision, true);
  }

  /**
   * The cluster loader's admission check for a policy-supplied value. The loader SPI is treated as
   * fallible: an exception is a refusal, so the decision still takes one terminal skip hook instead
   * of escaping the applier.
   *
   * @param key   the affected key (for the WARN line)
   * @param value the replacement value
   * @return {@code true} when no loader is configured or the loader admits the value
   */
  private boolean admitsValue(String key, Object value) {
    if (clusterLoader == null) {
      return true;
    }
    try {
      return clusterLoader.isCacheable(value);
    } catch (Exception e) {
      log.warn("ClusterLoader.isCacheable failed for key={}, refusing the policy-supplied value", key, e);
      return false;
    }
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
    boolean sameNode = nodeId != null && !nodeId.isBlank();
    for (int i = 1; i < batch.size(); i++) {
      WorkerDecision decision = batch.get(i);
      requireType(decision, WorkerType.COOL);
      if (sameNode) {
        sameNode = nodeId.equals(decision.stamp().decisionNodeId());
      }
    }
    if (!sameNode) {
      return List.copyOf(batch);
    }

    CacheEntry snapshot = cache.asMap().get(first.cacheKey());
    // A logically expired resident is not a decision floor: the single-message path reports
    // NOT_FOUND_OR_EXPIRED before VERSION_STALE, so the folded path must start from the same state.
    DecisionStamp effective = snapshot != null && !ttlPolicy.isLogicallyExpired(snapshot) ? snapshot.decision() : null;
    WorkerDecision winner = null;
    for (WorkerDecision candidate : batch) {
      if (VersionGuard.shouldSkipForWorker(effective, candidate.stamp())) {
        fireOnCoolSkipped(candidate, CoolSkipReason.VERSION_STALE);
        continue;
      }
      if (winner != null) {
        fireOnCoolSkipped(winner, CoolSkipReason.COVERED_BY_NEWER_DECISION);
      }
      winner = candidate;
      effective = candidate.stamp();
    }
    return winner == null ? List.of() : List.of(winner);
  }

  @Override
  public void settleCool(WorkerDecision decision, CoolAction action) {
    requireType(decision, WorkerType.COOL);
    if (action instanceof CoolAction.Skip skip) {
      fireOnCoolSkipped(decision, skip.reason());
      return;
    }

    CoolOutcome outcome = coolApply(decision);
    if (!outcome.applied().changed()) {
      fireOnCoolSkipped(decision, outcome.reason());
      return;
    }
    log.debug("HotKey cooled by Worker: {}", decision.cacheKey());
    fireAfterCoolDowngrade(decision, outcome.applied().current());
  }

  @Override
  public void coolNow(WorkerDecision decision) {
    requireType(decision, WorkerType.COOL);
    coolApply(decision);
  }

  /**
   * The COOL downgrade compute shared by {@link #settleCool} (dispatcher task, with hooks) and
   * {@link #coolNow} (rejection fallback, no hooks).
   *
   * <p>The ghost-COOL guard's owner inputs are resolved outside the bin lock for the common
   * path; only that pre-check may reconcile the ring from the health view. For the exact entry
   * reference the outer verdict is reused and the compute re-judges only logical expiry; any
   * other observed entry runs the full guard again without a rebuild, so an owner absent from
   * the published ring fails open instead of stalling the bin behind a health-view walk.
   *
   * @param decision the incoming COOL decision; must not be {@code null}
   * @return the images the bin lock observed plus the skip reason when no downgrade applied
   */
  private CoolOutcome coolApply(WorkerDecision decision) {
    String key = decision.cacheKey();
    CacheEntry snapshot = cache.asMap().get(key);
    CoolSkipReason preCheck =
      snapshot == null ? CoolSkipReason.NOT_FOUND_OR_EXPIRED : coolSkipReason(snapshot, decision, true);
    if (preCheck != null) {
      return new CoolOutcome(EntryMutations.unchanged(snapshot), preCheck);
    }

    DecisionStamp stamp = decision.stamp();
    CoolSkipReason[] skipReason = new CoolSkipReason[1];
    Applied applied = EntryMutations.apply(cache, key, existing -> {
      if (existing == null) {
        skipReason[0] = CoolSkipReason.NOT_FOUND_OR_EXPIRED;
        return null;
      }

      CoolSkipReason reason =
        existing == snapshot
          ? (ttlPolicy.isLogicallyExpired(existing) ? CoolSkipReason.NOT_FOUND_OR_EXPIRED : null)
          : coolSkipReason(existing, decision, false);
      if (reason != null) {
        skipReason[0] = reason;
        return existing;
      }
      return entryLifecycle.cool(existing, stamp);
    });
    if (applied.changed()) {
      return new CoolOutcome(applied, null);
    }

    CoolSkipReason reason = skipReason[0];
    return new CoolOutcome(applied, reason != null ? reason : CoolSkipReason.NOT_FOUND_OR_EXPIRED);
  }

  /**
   * The skip reason for one COOL against one entry, or {@code null} when the downgrade may
   * apply. Shared by the lock-free pre-check, the in-compute re-check and the post-compute
   * attribution, so every path reports the same reason and the same ghost-COOL line.
   *
   * @param entry            the entry the COOL would rewrite
   * @param decision         the incoming COOL decision
   * @param allowRingRebuild whether the ghost-COOL guard may reconcile the ring from the
   *                         health view; only the lock-free pre-check passes {@code true}
   * @return the skip reason, or {@code null} when the downgrade is allowed
   */
  private @Nullable CoolSkipReason coolSkipReason(CacheEntry entry, WorkerDecision decision, boolean allowRingRebuild) {
    if (ttlPolicy.isLogicallyExpired(entry)) {
      return CoolSkipReason.NOT_FOUND_OR_EXPIRED;
    }
    if (VersionGuard.shouldSkipForWorker(entry, decision.stamp())) {
      return CoolSkipReason.VERSION_STALE;
    }
    if (!isStaleCrossNodeCool(entry, decision, allowRingRebuild)) {
      return null;
    }
    log.debug(
      "cool: rejecting stale cross-Worker COOL for key={} from nodeId={} (owner={} still owns the shard)",
      decision.cacheKey(),
      decision.stamp().decisionNodeId(),
      entry.getDecisionNodeId()
    );
    return CoolSkipReason.GHOST_COOL;
  }

  /** One COOL apply outcome: the mutation images plus the skip reason when nothing changed. */
  private record CoolOutcome(Applied applied, @Nullable CoolSkipReason reason) {}

  /**
   * Ownership check behind the ghost-COOL guard: a cross-Worker COOL is a stale kill when the
   * entry is an in-force HOT owned by a <b>different, still-live</b> Worker <b>of the
   * incarnation that issued the HOT</b>, and the consistent-hash ring still routes the key to
   * that owner.
   *
   * <p>Any missing signal fails open: a {@code null} ring or health view, a non-HOT entry, an
   * empty owner or sender, the sender itself, a dead or restarted owner, a ring that no longer
   * points at the owner, or an owner absent from the published ring when a rebuild is not
   * allowed. Liveness and incarnation are checked before the ring probe because the probe walks
   * health records, and a dead owner's HOT is legitimately coolable (design decisions: version
   * comparison).
   *
   * @param entry            the entry currently in L1; must not be {@code null}
   * @param decision         the incoming COOL decision; must not be {@code null}
   * @param allowRingRebuild whether a missing owner may be reconciled before routing; only
   *                         the lock-free pre-check passes {@code true}
   * @return {@code true} when the COOL is a stale cross-Worker kill and must be skipped
   */
  private boolean isStaleCrossNodeCool(CacheEntry entry, WorkerDecision decision, boolean allowRingRebuild) {
    if (ring == null || healthView == null || entry.getKeyState() != KeyState.HOT) {
      return false;
    }

    String owner = entry.getDecisionNodeId();
    String sender = decision.stamp().decisionNodeId();
    if (owner == null || owner.isEmpty() || sender == null || sender.isEmpty() || Objects.equals(owner, sender)) {
      return false;
    }
    if (!healthView.isCurrentIncarnation(owner, entry.getDecisionEpoch())) {
      return false;
    }
    // Reconcile only in the lock-free pre-check; under the bin lock the published ring is
    // used as-is, and a missing owner fails open because locateNode cannot return it.
    if (allowRingRebuild && !ring.getNodes().contains(owner)) {
      ring.rebuildIfPossible(healthView);
    }
    return owner.equals(ring.locateNode(decision.cacheKey(), healthView));
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

  /** Reports a skipped COOL decision to every registered hook, with the skip reason. */
  private void fireOnCoolSkipped(WorkerDecision decision, CoolSkipReason reason) {
    if (hooks.isEmpty()) {
      return;
    }
    fireHooks("onCoolSkipped", decision.cacheKey(), hook -> hook.onCoolSkipped(decision, reason));
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
