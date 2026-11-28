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
package io.github.hyshmily.zeta.sync.local;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.cache.EntryView;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.TtlContract;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.TtlPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryMutations;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryMutations.Applied;
import io.github.hyshmily.zeta.exception.ZetaExceptionHandler;
import io.github.hyshmily.zeta.heat.HeatView;
import io.github.hyshmily.zeta.policy.NamespacePolicyRegistry;
import io.github.hyshmily.zeta.policy.ResolvedNamespacePolicy;
import io.github.hyshmily.zeta.scheduler.WarmScheduler;
import io.github.hyshmily.zeta.scheduler.WarmSignal;
import io.github.hyshmily.zeta.sync.DecisionPlaneSupport;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.clock.StampClock;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * Applies granted sync batches through {@link SyncPolicy} decisions and framework-owned version,
 * tombstone, and clear-watermark guards.
 *
 * <p>{@link SyncPolicy} only observes {@link SyncEvent} and {@link EntryView}; all L1 writes,
 * tombstones, watermarks, and terminal {@link SyncHook} callbacks remain confined to this class.
 * PUT decoding, INVALIDATE, and clear sweeps run on the ordered dispatcher, whereas blocking peer
 * REFRESH loads are offloaded to {@link #refreshExecutor} and coalesced per key so slow sources
 * never stall O(1) invalidations.
 */
@Slf4j
@Internal
public class DefaultSyncContext {

  private final Cache<String, CacheEntry> cache;
  private final EntryLifecycle entryLifecycle;
  private final TtlPolicy ttlPolicy;
  private final List<SyncHook> hooks;
  private final Executor sweepExecutor;
  private final Executor refreshExecutor;

  private final @Nullable CacheLoader<String, Object> clusterLoader;
  private final @Nullable NamespacePolicyRegistry policies;
  private final @Nullable PutPayloadCodec payloadCodec;
  private final @Nullable StampClock stampClock;
  private final @Nullable SingleFlight singleFlight;
  private final @Nullable HeatView heatView;
  private final @Nullable WarmScheduler warmScheduler;

  /** Per-key async REFRESH coalescing: at most one active load plus one newest pending task. */
  private final ConcurrentHashMap<String, RefreshSlot> inFlightRefreshes = new ConcurrentHashMap<>();

  /**
   * Creates a sync application context wired with the L1 table, lifecycle guards, executors, and
   * optional peer-sync collaborators.
   *
   * @param cache           the L1 Caffeine table
   * @param clusterLoader   optional deployment-wide fallback loader
   * @param entryLifecycle  entry draft factory and invalidation watermark owner
   * @param hooks           optional terminal sync hooks ({@code null} means none)
   * @param singleFlight    optional deduplication layer to invalidate on writes/clears
   * @param payloadCodec    optional codec decoding value-carrying PUT payloads
   * @param sweepExecutor   executor running refused-clear sweeps (runs inline on rejection)
   * @param refreshExecutor dedicated executor for blocking peer REFRESH loads
   * @param stampClock      node-local data-stamp clock, or {@code null} to persist peer stamps as-is
   * @param heatView        optional heat query enabling hot soft-close degradation
   * @param warmScheduler   optional renewal scheduler to re-arm or cancel on mutations
   * @param policies        optional namespace policy registry resolving per-namespace loaders/TTLs
   * @throws NullPointerException if a required collaborator is {@code null}
   */
  public DefaultSyncContext(
    Cache<String, CacheEntry> cache,
    @Nullable CacheLoader<String, Object> clusterLoader,
    EntryLifecycle entryLifecycle,
    @Nullable List<SyncHook> hooks,
    @Nullable SingleFlight singleFlight,
    @Nullable PutPayloadCodec payloadCodec,
    Executor sweepExecutor,
    Executor refreshExecutor,
    @Nullable StampClock stampClock,
    @Nullable HeatView heatView,
    @Nullable WarmScheduler warmScheduler,
    @Nullable NamespacePolicyRegistry policies
  ) {
    this.cache = Objects.requireNonNull(cache, "cache must not be null");
    this.entryLifecycle = Objects.requireNonNull(entryLifecycle, "entryLifecycle must not be null");
    this.sweepExecutor = Objects.requireNonNull(sweepExecutor, "sweepExecutor must not be null");
    this.refreshExecutor = Objects.requireNonNull(refreshExecutor, "refreshExecutor must not be null");
    this.ttlPolicy = entryLifecycle.ttlPolicy();
    this.hooks = hooks != null ? List.copyOf(hooks) : List.of();
    this.clusterLoader = clusterLoader;
    this.singleFlight = singleFlight;
    this.payloadCodec = payloadCodec;
    this.stampClock = stampClock;
    this.heatView = heatView;
    this.warmScheduler = warmScheduler;
    this.policies = policies;
  }

  /**
   * Applies one reduced sync batch through {@link SyncPolicy} and framework guards, reporting all
   * covered messages via terminal skip hooks.
   *
   * @param batch  the reduced batch to apply
   * @param policy the sync decision policy
   */
  public void apply(Batch batch, SyncPolicy policy) {
    Objects.requireNonNull(batch, "batch must not be null");
    Objects.requireNonNull(policy, "policy must not be null");
    if (batch instanceof Batch.Value value) {
      applyValueBatch(value, policy);
    } else {
      // Batch is sealed to Value and Clear, so the remaining case needs no runtime check.
      applyClearBatch((Batch.Clear) batch, policy);
    }
  }

  /**
   * Consumer-thread fallback for a message the dispatcher refused. The refusal was already reported
   * as a skip, so this path only heals local state: it re-stamps the peer version and applies the
   * removal under that stamp without invoking user hooks. A tombstone recorded during eviction
   * prevents a late fill from reintroducing the invalidated value; a clear records its watermarks
   * first and offloads the table sweep to {@link #sweepExecutor} (running inline on rejection).
   *
   * @param message the refused message
   */
  public void fallbackEvict(SyncMessage message) {
    long version = resolveEffectiveVersion(message.version());
    switch (message.type()) {
      case INVALIDATE_ALL -> dispatchClearFallback(version, Map.of());
      case INVALIDATE_PREFIX -> dispatchClearFallback(0L, Map.of(message.cacheKey(), version));
      default -> evict(message.cacheKey(), version, false);
    }
  }

  /**
   * Applies a single-key value/invalidation batch: reports every covered message as superseded,
   * then routes the effective winner to the INVALIDATE or PUT/REFRESH path.
   *
   * @param batch  the reduced value batch
   * @param policy the sync policy
   */
  private void applyValueBatch(Batch.Value batch, SyncPolicy policy) {
    SyncMessage winner = batch.winner();
    for (SyncMessage covered : batch.covered()) {
      fireOnSkipped(SyncEvent.from(covered), SyncSkipReason.COVERED_BY_NEWER_EVENT);
    }
    // Batch reduction ran on peer versions; only the applied winner is re-stamped on drift. The
    // covered INVALIDATE is resolved first, in ascending version order, so a clock-ahead peer's
    // fallback stamp cannot later outrank a write issued after this batch was admitted.
    SyncMessage covered = batch.coveredInvalidate();
    long coveredVersion =
      winner.type() == SyncType.INVALIDATE || covered == null ? 0L : resolveEffectiveVersion(covered.version());
    SyncEvent event = SyncEvent.from(winner, resolveEffectiveVersion(winner.version()));
    if (winner.type() == SyncType.INVALIDATE) {
      applyInvalidate(event, policy);
    } else {
      applyValue(batch, event, policy, coveredVersion);
    }
  }

  /**
   * Applies a winning INVALIDATE through version guard, policy admission, and local eviction.
   *
   * @param event  the winning invalidation event
   * @param policy the sync policy
   */
  private void applyInvalidate(SyncEvent event, SyncPolicy policy) {
    String key = event.key();
    long version = event.version();
    EntryView current = view(key);
    // The tombstone only decides the no-resident case: with a live resident the version guard
    // already orders the event against it, and a tombstone at or above the resident cannot exist.
    SyncSkipReason stale = staleReason(key, current, version, false);
    if (stale != null) {
      fireOnSkipped(event, stale);
      return;
    }

    SyncAdmission admission = callPolicy(event, "admit", () -> policy.admit(event, current), null);
    if (admission == null) {
      return;
    }
    if (admission == SyncAdmission.SKIP) {
      fireOnSkipped(event, SyncSkipReason.POLICY_REJECTED);
      return;
    }
    // INVALIDATE_LOCAL is redundant for an INVALIDATE: the event already removes the local copy,
    // so it proceeds instead of settling as a policy failure that would leave the entry behind.

    int outcome = evict(key, version, false);
    if (outcome == EVICT_STALE) {
      fireOnSkipped(event, SyncSkipReason.VERSION_STALE);
      return;
    }
    if (outcome == EVICT_STALE_TOMBSTONE) {
      fireOnSkipped(event, SyncSkipReason.INVALIDATION_TOMBSTONE);
      return;
    }
    fireAfterInvalidate(event);
  }

  /**
   * Applies a winning PUT or REFRESH through admission, cold-entry prewarmability checks, payload
   * decoding (PUT) or async reload scheduling (REFRESH).
   *
   * @param batch  the reduced value batch
   * @param event  the winning value event
   * @param policy the sync policy
   * @param coveredVersion the effective version of the batch's covered INVALIDATE, or {@code 0}
   */
  private void applyValue(Batch.Value batch, SyncEvent event, SyncPolicy policy, long coveredVersion) {
    String key = event.key();
    long version = event.version();
    boolean isPut = event.type() == SyncType.PUT;
    EntryView current = view(key);
    SyncSkipReason stale = staleReason(key, current, version, true);
    if (stale != null) {
      // A tombstone refuses the value, not the removal the batch carries: the covered INVALIDATE
      // still has to land as a hard eviction.
      if (stale == SyncSkipReason.INVALIDATION_TOMBSTONE) {
        rejectValue(event, key, stale, coveredVersion);
      } else {
        fireOnSkipped(event, stale);
      }
      return;
    }

    SyncAdmission admission = callPolicy(
      event,
      "admit",
      () -> policy.admit(event, current),
      () -> applyCoveredInvalidateFallback(key, coveredVersion)
    );
    if (admission == null) {
      return;
    }
    switch (admission) {
      case SKIP -> {
        rejectValue(event, key, SyncSkipReason.POLICY_REJECTED, coveredVersion);
        return;
      }
      case INVALIDATE_LOCAL -> {
        degradeToInvalidate(batch, event, key, version, SyncSkipReason.DEGRADED_TO_INVALIDATE);
        return;
      }
      case PROCEED -> {
        // Proceed to namespace policy and value resolution.
      }
    }

    ResolvedNamespacePolicy resolvedPolicy = policies == null ? null : policies.resolve(key);
    if (!EntryView.isLive(current)) {
      SyncSkipReason refusal = coldRefusal(resolvedPolicy, key, isPut);
      if (refusal != null) {
        degradeToInvalidate(batch, event, key, version, refusal);
        return;
      }
    }

    // A namespace's own loader wins over the deployment-wide fallback.
    CacheLoader<String, Object> loader = resolvedPolicy != null ? resolvedPolicy.loader() : clusterLoader;
    if (isPut) {
      Object decoded = payloadCodec == null ? null : payloadCodec.decode(batch.winner().payload());
      if (decoded == null) {
        degradeToInvalidate(batch, event, key, version, SyncSkipReason.PAYLOAD_UNREADABLE);
        return;
      }
      finishValue(batch, event, key, version, true, decoded, resolvedPolicy, loader, policy, coveredVersion, null);
      return;
    }
    if (loader == null) {
      degradeToInvalidate(batch, event, key, version, SyncSkipReason.VALUE_NOT_FOUND);
      return;
    }
    scheduleAsyncRefresh(new RefreshTask(batch, event, resolvedPolicy, loader, policy, coveredVersion));
  }

  /**
   * Settles a decoded PUT or loaded REFRESH value through {@link SyncPolicy#settle}, loader
   * admission, a guarded L1 write, and terminal hook dispatch. Staleness is re-checked after any
   * loader work, so a value read during a long REFRESH cannot overwrite a newer resident.
   *
   * @param isPut          {@code true} for a payload-bearing PUT, {@code false} for a REFRESH
   * @param coveredVersion effective stamp of a covered INVALIDATE, or {@code 0}
   * @param slot           the coalescing slot when the settle runs inside a refresh task, else
   *                       {@code null}
   * @return the highest covered stamp this settle satisfied: the write version when the value
   *         landed or lost to a newer resident, the covered stamp after a hard removal the settle
   *         performed for the batch, and {@code 0} when a replacement task owns the outcome and the
   *         obligation must travel with the slot
   */
  private long finishValue(
    Batch.Value batch,
    SyncEvent event,
    String key,
    long version,
    boolean isPut,
    Object value,
    @Nullable ResolvedNamespacePolicy resolvedPolicy,
    @Nullable CacheLoader<String, Object> loader,
    SyncPolicy policy,
    long coveredVersion,
    @Nullable RefreshSlot slot
  ) {
    EntryView resolvedView = view(key);
    SyncSkipReason staleAfterResolution = staleReason(key, resolvedView, version, true);
    if (staleAfterResolution != null) {
      return settleStaleValue(event, key, version, staleAfterResolution, coveredVersion, slot);
    }

    long[] rejectedCovered = { 0L };
    SyncValueAction action = callPolicy(
      event,
      "settle",
      () -> policy.settle(event, value, resolvedView),
      () -> {
        if (slot == null || !isSuperseded(slot, version)) {
          long covered = effectiveCovered(coveredVersion, slot);
          applyCoveredInvalidateFallback(key, covered);
          rejectedCovered[0] = covered;
        }
      }
    );
    if (action == null) {
      return rejectedCovered[0];
    }
    if (action instanceof SyncValueAction.Skip) {
      if (slot != null && isSuperseded(slot, version)) {
        // The replacement task owns the key's outcome; the covered removal travels with the slot.
        fireOnSkipped(event, SyncSkipReason.POLICY_REJECTED);
        return 0L;
      }
      long covered = effectiveCovered(coveredVersion, slot);
      rejectValue(event, key, SyncSkipReason.POLICY_REJECTED, covered);
      return covered;
    }
    if (action instanceof SyncValueAction.Invalidate) {
      return degradeOrYieldIfSuperseded(
        batch,
        event,
        key,
        version,
        SyncSkipReason.DEGRADED_TO_INVALIDATE,
        coveredVersion,
        slot
      );
    }

    // SyncValueAction is sealed to Skip, Invalidate and Apply, so this is the apply case.
    Object resolved = ((SyncValueAction.Apply) action).value();
    // Skip redundant loader admission on an unchanged REFRESH value (already admitted by the
    // refresh task before it resolved the value).
    if ((isPut || resolved != value) && !admitsValue(loader, key, resolved)) {
      return degradeOrYieldIfSuperseded(
        batch,
        event,
        key,
        version,
        SyncSkipReason.VALUE_DECLINED_BY_LOADER,
        coveredVersion,
        slot
      );
    }

    SyncSkipReason[] refuseReason = new SyncSkipReason[1];
    Applied applied = writePeerValue(key, version, resolved, resolvedPolicy, refuseReason);
    if (!applied.changed()) {
      SyncSkipReason reason = refuseReason[0] != null ? refuseReason[0] : SyncSkipReason.VERSION_STALE;
      if (reason == SyncSkipReason.TTL_NOT_PREWARMABLE) {
        // Resident expired mid-load; settle identically to cold-path degradation.
        return degradeOrYieldIfSuperseded(batch, event, key, version, reason, coveredVersion, slot);
      }
      return settleStaleValue(event, key, version, reason, coveredVersion, slot);
    }
    fireAfterValueApplied(event, applied.current());
    return version;
  }

  /**
   * Settles a value that lost a guard after its loader work. A version-stale value needs nothing
   * further (the resident already outranks it, and every covered stamp is below it), while a
   * tombstone refuses the value but not the removal the same batch carried: the covered INVALIDATE
   * is hard-removed here, exactly as a standalone INVALIDATE would have been.
   *
   * @return the highest covered stamp this settle satisfied
   */
  private long settleStaleValue(
    SyncEvent event,
    String key,
    long version,
    SyncSkipReason reason,
    long coveredVersion,
    @Nullable RefreshSlot slot
  ) {
    if (reason != SyncSkipReason.INVALIDATION_TOMBSTONE) {
      fireOnSkipped(event, reason);
      return version;
    }
    if (slot != null && isSuperseded(slot, version)) {
      // The replacement task owns the key's outcome; the covered removal travels with the slot.
      fireOnSkipped(event, reason);
      return 0L;
    }
    long covered = effectiveCovered(coveredVersion, slot);
    rejectValue(event, key, reason, covered);
    return covered;
  }

  /**
   * Degrades a refused value to a versioned eviction unless a strictly newer task is parked in
   * {@code slot}. Evicting on behalf of a superseded task would remove the live resident its
   * replacement is about to renew, turning that replacement into a refused cold load.
   *
   * @return the stamp the eviction satisfied, or {@code 0} when the replacement inherits the duty
   */
  private long degradeOrYieldIfSuperseded(
    Batch.Value batch,
    SyncEvent event,
    String key,
    long version,
    SyncSkipReason reason,
    long coveredVersion,
    @Nullable RefreshSlot slot
  ) {
    if (slot != null && isSuperseded(slot, version)) {
      fireOnSkipped(event, SyncSkipReason.COVERED_BY_NEWER_EVENT);
      return 0L;
    }
    long covered = effectiveCovered(coveredVersion, slot);
    return degradeToInvalidate(batch, event, key, version, reason, covered == 0L);
  }

  /**
   * Schedules a peer REFRESH onto {@link #refreshExecutor} with per-key coalescing (one active
   * load plus one newest pending replacement). Lower or equal versions are skipped immediately.
   *
   * <p>A superseded task never evicts on the spot: its covered INVALIDATE is merged into the slot
   * and honoured by whichever task settles last. Evicting when the task is replaced would remove a
   * live resident before its replacement has loaded, turning a successful refresh into a refused
   * cold load for a namespace that forbids one.
   *
   * @param task the refresh task to execute
   */
  @SuppressWarnings("all")
  private void scheduleAsyncRefresh(RefreshTask task) {
    RefreshTask[] superseded = new RefreshTask[1];
    boolean[] shouldSubmit = { false };
    RefreshSlot slot = inFlightRefreshes.compute(task.key(), (key, existing) -> {
      superseded[0] = null;
      shouldSubmit[0] = false;
      if (existing == null) {
        shouldSubmit[0] = true;
        return new RefreshSlot(task.version(), task.coveredVersion());
      }
      if (task.version() <= existing.activeVersion) {
        // Same or older version: its removal still has to land, so merge it into the slot.
        superseded[0] = task;
        existing.activeCovered = Math.max(existing.activeCovered, task.coveredVersion());
      } else if (existing.next == null) {
        existing.next = task;
        existing.nextCovered = task.coveredVersion();
      } else if (task.version() > existing.next.version()) {
        superseded[0] = existing.next;
        existing.nextCovered = Math.max(existing.nextCovered, task.coveredVersion());
        existing.next = task;
      } else {
        superseded[0] = task;
        existing.nextCovered = Math.max(existing.nextCovered, task.coveredVersion());
      }
      return existing;
    });

    if (superseded[0] != null) {
      onRefreshSuperseded(superseded[0]);
    }
    if (!shouldSubmit[0]) {
      return;
    }

    try {
      refreshExecutor.execute(() -> drainAsyncRefresh(task, slot));
    } catch (RejectedExecutionException e) {
      ReleasedSlot released = releaseSlotOnRejection(task.key(), slot);
      boolean allowSoft = released.coveredVersion() == 0L;
      degradeToInvalidate(task, SyncSkipReason.REFRESH_REJECTED, allowSoft);
      if (released.pending() != null) {
        degradeToInvalidate(released.pending(), SyncSkipReason.REFRESH_REJECTED, allowSoft);
      }
    } catch (RuntimeException | Error failure) {
      ReleasedSlot released = releaseSlotOnRejection(task.key(), slot);
      boolean allowSoft = released.coveredVersion() == 0L;
      degradeToInvalidate(task, SyncSkipReason.REFRESH_FAILED, allowSoft);
      if (released.pending() != null) {
        degradeToInvalidate(released.pending(), SyncSkipReason.REFRESH_FAILED, allowSoft);
      }
      throw failure;
    }
  }

  /**
   * Drains the active refresh task and, in the same executor turn, the newest replacement queued
   * behind it. Each settle reports the highest covered stamp it satisfied; whatever remains is
   * handed to the replacement, and applied in place when the slot closes with no replacement left,
   * so a removal covered by a task that lost its value is never lost. Failures are degraded
   * individually so one bad load cannot strand later tasks. The finally block covers abnormal exit
   * (an {@link Error} outside the per-task catch) by releasing the slot and degrading the task that
   * was still running; the slot is released here rather than in the caller because only this loop
   * knows whether a replacement became active.
   */
  private void drainAsyncRefresh(RefreshTask first, RefreshSlot slot) {
    RefreshTask current = first;
    long activeCovered = first.coveredVersion();
    boolean completedNormally = false;
    try {
      while (current != null) {
        RefreshTask task = current;
        long satisfiedUpTo;
        try {
          satisfiedUpTo = executeRefreshTask(task, slot, activeCovered);
        } catch (RuntimeException e) {
          if (isSuperseded(slot, task.version())) {
            onRefreshSuperseded(task);
            satisfiedUpTo = 0L;
          } else {
            log.warn("Async refresh failed: key={}", task.key(), e);
            satisfiedUpTo = degradeToInvalidate(
              task,
              SyncSkipReason.REFRESH_FAILED,
              effectiveCovered(activeCovered, slot) == 0L
            );
          }
        }
        // The task above is settled either way: clearing the cursor before the poll keeps the
        // finally block from degrading it a second time if the poll itself throws.
        current = null;
        NextRefresh next = pollNextRefresh(first.key(), slot, satisfiedUpTo);
        current = next.task();
        activeCovered = next.activeCovered();
        if (current == null && activeCovered > 0L) {
          // No task is left to carry the removal the slot still owed; land it now.
          applyCoveredInvalidateFallback(first.key(), activeCovered);
        }
      }
      completedNormally = true;
    } finally {
      if (!completedNormally) {
        ReleasedSlot released = releaseSlotOnRejection(first.key(), slot);
        boolean allowSoft = activeCovered == 0L && released.coveredVersion() == 0L;
        if (current != null) {
          degradeToInvalidate(current, SyncSkipReason.REFRESH_FAILED, allowSoft);
        }
        if (released.pending() != null) {
          degradeToInvalidate(released.pending(), SyncSkipReason.REFRESH_FAILED, allowSoft);
        }
      }
    }
  }

  /**
   * Executes one offloaded REFRESH task. Supersession and version/tombstone guards are checked both
   * before and after the blocking loader call: the task may have been parked behind a newer one, or a
   * peer write may have landed while the source was being read. The cold-load policy is re-checked as
   * well, since an entry that was live at enqueue time can expire before the task runs.
   *
   * @return the highest covered stamp this settle satisfied; {@code 0} when a replacement is parked
   *         and inherits the obligation
   */
  private long executeRefreshTask(RefreshTask task, RefreshSlot slot, long activeCovered) {
    if (isSuperseded(slot, task.version())) {
      onRefreshSuperseded(task);
      return 0L;
    }

    EntryView preLoad = view(task.key());
    SyncSkipReason stale = staleReason(task.key(), preLoad, task.version(), true);
    if (stale != null) {
      return settleStaleValue(task.event(), task.key(), task.version(), stale, activeCovered, slot);
    }

    long covered = Math.max(activeCovered, slot.activeCovered);
    if (!EntryView.isLive(preLoad)) {
      // The entry may have expired or been evicted while this task waited in the refresh queue, so
      // the cold-load policy is re-checked here instead of only at apply time.
      SyncSkipReason refusal = coldRefusal(task.resolvedPolicy(), task.key(), false);
      if (refusal != null) {
        return degradeToInvalidate(task, refusal, covered == 0L);
      }
    }

    Object loaded;
    try {
      // A refresh always runs outside the caller's stack, resident or not, so it uses the
      // background entry point and never the foreground-only load.
      loaded = task.loader().reload(task.key());
    } catch (RuntimeException e) {
      // A task superseded while its load was in flight is reported as covered, not as a failure:
      // the newer task owns the key's outcome.
      if (isSuperseded(slot, task.version())) {
        onRefreshSuperseded(task);
        return 0L;
      }
      log.warn("refresh: load failed for key={}", task.key(), e);
      return degradeToInvalidate(task, SyncSkipReason.REFRESH_FAILED, covered == 0L);
    }

    if (isSuperseded(slot, task.version())) {
      onRefreshSuperseded(task);
      return 0L;
    }
    if (loaded == null) {
      return degradeToInvalidate(task, SyncSkipReason.VALUE_NOT_FOUND, covered == 0L);
    }
    if (!admitsValue(task.loader(), task.key(), loaded)) {
      return degradeToInvalidate(task, SyncSkipReason.VALUE_DECLINED_BY_LOADER, covered == 0L);
    }
    return finishValue(
      task.batch(),
      task.event(),
      task.key(),
      task.version(),
      false,
      loaded,
      task.resolvedPolicy(),
      task.loader(),
      task.policy(),
      covered,
      slot
    );
  }

  /**
   * Writes a peer value into L1 under the tombstone guard and an idempotent equal-version guard,
   * preserving resident TTLs or using a prewarmable namespace contract for absent entries, then
   * drops dedup and best-effort re-arms {@link #warmScheduler}.
   *
   * @param key            the affected physical key
   * @param version        the effective data version stamp
   * @param value          the resolved value to store
   * @param resolvedPolicy pre-resolved namespace policy, or {@code null}
   * @param refuseReason   single-element holder populated if a guard refuses the write
   * @return the {@link Applied} before/after mutation result
   */
  @SuppressWarnings("all")
  private Applied writePeerValue(
    String key,
    long version,
    Object value,
    @Nullable ResolvedNamespacePolicy resolvedPolicy,
    SyncSkipReason[] refuseReason
  ) {
    Applied applied = EntryMutations.apply(cache, key, existing -> {
      if (entryLifecycle.staleAgainstInvalidation(key, version)) {
        refuseReason[0] = SyncSkipReason.INVALIDATION_TOMBSTONE;
        return existing;
      }
      CacheEntry resident = liveResident(existing);
      // A value event is idempotent at an equal stamp: the resident already carries that version,
      // so a redelivered PUT/REFRESH must not re-fetch the source or restart the entry's TTLs.
      if (resident != null && VersionGuard.shouldSkipForSync(resident, version)) {
        refuseReason[0] = SyncSkipReason.VERSION_STALE;
        return existing;
      }
      long hardTtl;
      long softTtl;
      if (resident != null) {
        hardTtl = resident.getHardTtlMs();
        softTtl = resident.getSoftTtlMs();
      } else if (resolvedPolicy != null) {
        // Peer broadcasts carry no TTL; an absent entry uses its namespace's static TTL contract.
        TtlContract contract = resolvedPolicy.ttl();
        if (!contract.prewarmable()) {
          refuseReason[0] = SyncSkipReason.TTL_NOT_PREWARMABLE;
          return existing;
        }
        hardTtl = contract.hardTtlMs();
        softTtl = contract.softTtlMs();
      } else {
        hardTtl = ttlPolicy.effectiveHardTtlMs();
        softTtl = ttlPolicy.effectiveSoftTtlMs();
      }
      return entryLifecycle.writeValue(value, version, hardTtl, softTtl);
    });

    if (applied.changed()) {
      // Kill the key's in-flight load: it started before this write and could otherwise commit an
      // older value afterwards.
      if (singleFlight != null) {
        singleFlight.invalidate(key);
      }
      // Re-arm renewal in case the new entry's lifetime is shorter than an earlier warm task.
      CacheEntry current = applied.current();
      if (warmScheduler != null && current != null) {
        try {
          warmScheduler.scheduleIfEligible(key, current, WarmSignal.WRITE);
        } catch (RuntimeException e) {
          log.debug("Warm re-arm skipped after peer value write for key={}", key, e);
        }
      }
    }
    return applied;
  }

  /** {@code evict} outcome: the resident outranked the stamp; nothing was removed. */
  private static final int EVICT_STALE = 0;

  /** {@code evict} outcome: the stamp was already covered and no resident was live. */
  private static final int EVICT_STALE_TOMBSTONE = 1;

  /** {@code evict} outcome: the soft window was closed and the entry still serves as stale. */
  private static final int EVICT_SOFT_EXPIRED = 2;

  /** {@code evict} outcome: the entry was hard-removed, or was already absent. */
  private static final int EVICT_HARD = 3;

  /**
   * Performs version-guarded local eviction inside the key's map bin: records the invalidation
   * tombstone, drops SingleFlight dedup, and either closes the soft window (when
   * {@code softExpireHot} applies to a live hot resident with soft TTL) or removes the entry and
   * cancels its warm task.
   *
   * <p>A stamp an earlier removal already covered cannot condemn anything: with no live resident
   * there is nothing to remove, and invalidating the key's flight unconditionally would kill a load
   * that started <em>after</em> that removal. Such a late message (a reordered peer INVALIDATE, or a
   * delayed degradation) settles as {@link #EVICT_STALE_TOMBSTONE} without touching the flight.
   * A live resident is exempt from that short-circuit: a resident that is merely soft-expired still
   * has to be hard-removed when the same stamp arrives again.
   *
   * @param key           the affected physical key
   * @param version       the invalidation version stamp
   * @param softExpireHot {@code true} to soft-expire eligible hot entries instead of deleting them
   * @return one of {@link #EVICT_STALE}, {@link #EVICT_STALE_TOMBSTONE}, {@link #EVICT_SOFT_EXPIRED}
   *         or {@link #EVICT_HARD}
   */
  @SuppressWarnings("all")
  private int evict(String key, long version, boolean softExpireHot) {
    int[] outcome = { EVICT_HARD };
    boolean[] removed = { false };
    cache
      .asMap()
      .compute(key, (k, existing) -> {
        outcome[0] = EVICT_HARD;
        removed[0] = false;
        CacheEntry resident = liveResident(existing);
        if (resident != null && VersionGuard.shouldSkipForRefresh(resident, version)) {
          outcome[0] = EVICT_STALE;
          return existing;
        }
        boolean alreadyCovered = entryLifecycle.staleAgainstInvalidation(key, version);
        if (resident == null && alreadyCovered) {
          outcome[0] = EVICT_STALE_TOMBSTONE;
          removed[0] = existing != null;
          return null;
        }
        if (!alreadyCovered) {
          entryLifecycle.recordInvalidation(key, version);
          if (singleFlight != null) {
            singleFlight.invalidate(key);
          }
        }
        if (existing == null) {
          // Entry is already absent, but a scheduled warm task may still be armed.
          removed[0] = true;
          return null;
        }
        if (
          resident != null && softExpireHot && existing.getSoftTtlMs() > 0L && heatView != null && heatView.isHot(key)
        ) {
          outcome[0] = EVICT_SOFT_EXPIRED;
          if (ttlPolicy.isSoftExpired(existing)) {
            return existing;
          }
          // Set soft deadline 1ms in the past so reads in the same millisecond trigger soft refresh.
          return entryLifecycle.editEntry(existing).softExpiryAt(TimeSource.monotonicMillis() - 1L).build();
        }
        removed[0] = true;
        return null;
      });
    if (removed[0] && warmScheduler != null) {
      warmScheduler.cancel(key);
    }
    return outcome[0];
  }

  /**
   * Applies a granted clear batch in one pass: admits each clear independently, records coalesced
   * watermarks, invalidates pre-clear flights, sweeps L1 once, and fires {@code afterClear}.
   *
   * @param batch  the granted clear batch
   * @param policy the sync policy
   */
  private void applyClearBatch(Batch.Clear batch, SyncPolicy policy) {
    // Resolve replacement stamps once in ascending peer order so policy and hooks stay consistent.
    Map<Long, Long> effective = resolveClearVersions(batch.messages());
    List<SyncMessage> accepted = new ArrayList<>(batch.messages().size());
    for (SyncMessage message : batch.messages()) {
      long resolvedVersion = effective.getOrDefault(message.version(), message.version());
      SyncEvent event = SyncEvent.from(message, resolvedVersion);
      SyncAdmission admission = callPolicy(event, "admit", () -> policy.admit(event, null), null);
      if (admission == null) {
        continue;
      }

      if (admission == SyncAdmission.SKIP) {
        fireOnSkipped(event, SyncSkipReason.POLICY_REJECTED);
        continue;
      }
      // INVALIDATE_LOCAL is redundant for a clear; treat as PROCEED.
      accepted.add(message);
    }
    if (accepted.isEmpty()) {
      return;
    }

    ClearMarks marks = collectMarks(accepted, effective);
    long fullStamp = marks.fullStamp();
    Map<String, Long> prefixStamps = marks.prefixStamps();
    recordWatermarks(fullStamp, prefixStamps);
    killClearedFlights(fullStamp, prefixStamps);

    int removed = sweep(fullStamp, prefixStamps);
    if (hooks.isEmpty()) {
      // Building the event list is only worth it when a hook will observe it.
      return;
    }
    fireHooks("afterClear", "clear", hook -> hook.afterClear(clearEvents(accepted, marks), removed));
  }

  /** Per-key sweep result: the resident outranked the clear and was left in place. */
  private static final int SWEEP_KEPT = 0;

  /** Per-key sweep result: an already-expired resident was removed but no servable value was lost. */
  private static final int SWEEP_EXPIRED = 1;

  /** Per-key sweep result: a live resident was removed by this clear. */
  private static final int SWEEP_LIVE = 2;

  /**
   * Sweeps L1 entries targeted by {@code fullStamp} or {@code prefixStamps}, removing residents
   * at or below the applicable stamp and cancelling their warm tasks.
   *
   * <p>One {@code outcome} holder is reused across the walk and reset before every mutation, so a
   * contended bin that re-runs the mapping function against a different image cannot carry the
   * previous key's result into this key's count.
   *
   * @return number of live entries removed by this sweep
   */
  @SuppressWarnings("all")
  private int sweep(long fullStamp, Map<String, Long> prefixStamps) {
    int removed = 0;
    int[] outcome = new int[1];
    for (Map.Entry<String, CacheEntry> entry : cache.asMap().entrySet()) {
      String key = entry.getKey();
      long stamp = stampFor(key, fullStamp, prefixStamps);
      if (stamp <= 0L) {
        continue;
      }

      // Lock-free pre-filter: skip bin locks for live residents already newer than the clear.
      CacheEntry resident = liveResident(entry.getValue());
      if (resident != null && VersionGuard.shouldSkipForRefresh(resident, stamp)) {
        continue;
      }
      outcome[0] = SWEEP_KEPT;
      // Pre-clear flights were already invalidated above; watermarks block stale fills.
      cache
        .asMap()
        .computeIfPresent(key, (k, existing) -> {
          // Reset on every attempt: a contended bin can re-run this function against a different image.
          outcome[0] = SWEEP_KEPT;
          boolean live = liveResident(existing) != null;
          if (live && VersionGuard.shouldSkipForRefresh(existing, stamp)) {
            return existing;
          }
          // An entry past its hard deadline is already unservable: it still has to be removed (and
          // its warm task cancelled), but it was not an entry this clear took away.
          outcome[0] = live ? SWEEP_LIVE : SWEEP_EXPIRED;
          return null;
        });

      if (outcome[0] == SWEEP_LIVE) {
        removed++;
      }
      if (outcome[0] != SWEEP_KEPT && warmScheduler != null) {
        warmScheduler.cancel(key);
      }
    }
    return removed;
  }

  /**
   * Invokes a {@link SyncPolicy} decision step, converting a thrown exception or a null return into
   * {@link SyncSkipReason#POLICY_FAILED} after running optional refusal cleanup. Cleanup runs before
   * the skip hook so a covered INVALIDATE still lands when the policy is broken.
   *
   * @param step      the decision name used in warnings
   * @param call      the policy decision
   * @param onFailure optional cleanup applied when the decision fails
   * @return the decision result, or {@code null} when the caller must stop
   */
  private <R> @Nullable R callPolicy(SyncEvent event, String step, Supplier<R> call, @Nullable Runnable onFailure) {
    R result;
    try {
      result = call.get();
    } catch (Exception e) {
      log.warn("SyncPolicy.{} failed: type={}, key={}", step, event.type(), eventKey(event), e);
      if (onFailure != null) {
        onFailure.run();
      }
      fireOnSkipped(event, SyncSkipReason.POLICY_FAILED);
      return null;
    }
    if (result == null) {
      log.warn(
        "SyncPolicy.{} returned null; settling as POLICY_FAILED: type={}, key={}",
        step,
        event.type(),
        eventKey(event)
      );
      if (onFailure != null) {
        onFailure.run();
      }
      fireOnSkipped(event, SyncSkipReason.POLICY_FAILED);
    }
    return result;
  }

  /**
   * Checks the version guard and, on the paths that need it, the invalidation watermarks before
   * policy invocation or write-back. A live resident carrying the event's own stamp or a higher one
   * settles the event: a value event is idempotent at an equal stamp, while an INVALIDATE or a clear
   * sweep must still win at that stamp, so only a strictly newer resident outranks them. The
   * tombstone is consulted for value events always, and for removals only when no live resident
   * exists — a live (possibly soft-expired) resident is condemned by {@code evict} itself, whereas a
   * tombstone would otherwise settle the event without touching it. Returning a reason here avoids
   * running a loader for an event that cannot win.
   *
   * @param isValue whether the event writes a value ({@code PUT}/{@code REFRESH}) rather than
   *                removing one
   * @return the skip reason, or {@code null} when the event may proceed
   */
  @SuppressWarnings("all")
  private @Nullable SyncSkipReason staleReason(String key, @Nullable EntryView current, long version, boolean isValue) {
    boolean live = EntryView.isLive(current);
    if (live && (isValue ? current.dataVersion() >= version : current.dataVersion() > version)) {
      return SyncSkipReason.VERSION_STALE;
    }
    if ((isValue || !live) && entryLifecycle.staleAgainstInvalidation(key, version)) {
      return SyncSkipReason.INVALIDATION_TOMBSTONE;
    }
    return null;
  }

  /**
   * Settles a refused value event. The covered INVALIDATE is applied first so a same-batch removal
   * still takes effect when the winning value is rejected; only then is the skip reported. Reversing
   * the order would let a hook observe a value that the fallback is about to remove.
   */
  @SuppressWarnings("all")
  private void rejectValue(SyncEvent event, String key, SyncSkipReason reason, long coveredVersion) {
    applyCoveredInvalidateFallback(key, coveredVersion);
    fireOnSkipped(event, reason);
  }

  /**
   * Degrades a non-writable value event to versioned eviction and reports the degradation hook.
   * Eviction carries the event's version so the local entry cannot be resurrected by an older peer
   * value; if the eviction itself loses that version check, the event is reported as stale instead
   * of degraded. Soft expiry is preserved when the batch allows it.
   *
   * @param reason the original refusal, reported through {@code afterValueDegraded}
   * @return the stamp the eviction satisfied: the event version after a hard removal, otherwise
   *         {@code 0}
   */
  private long degradeToInvalidate(
    Batch.Value batch,
    SyncEvent event,
    String key,
    long version,
    SyncSkipReason reason
  ) {
    return degradeToInvalidate(batch, event, key, version, reason, true);
  }

  /**
   * Degrades a value event to a versioned eviction, choosing between a hard removal and a
   * soft-expire close.
   *
   * @param allowSoftExpire whether the settle may leave a hot entry soft-expired instead of removing
   *                        it; forced to {@code false} when the batch carried an INVALIDATE
   * @return the stamp the eviction satisfied: the event version after a hard removal, otherwise
   *         {@code 0}
   */
  private long degradeToInvalidate(
    Batch.Value batch,
    SyncEvent event,
    String key,
    long version,
    SyncSkipReason reason,
    boolean allowSoftExpire
  ) {
    int outcome = evict(key, version, allowSoftExpire && batch.allowSoftExpire());
    if (outcome == EVICT_STALE) {
      fireOnSkipped(event, SyncSkipReason.VERSION_STALE);
      return version;
    }
    if (outcome == EVICT_STALE_TOMBSTONE) {
      fireOnSkipped(event, SyncSkipReason.INVALIDATION_TOMBSTONE);
      return version;
    }
    fireAfterValueDegraded(event, reason);
    return outcome == EVICT_HARD ? version : 0L;
  }

  /**
   * Degrades a captured refresh task to a versioned local invalidation. Used by the executor
   * rejection path, the drain loop and its cleanup: in every case the load produced no value to
   * settle, and evicting at the task's version still removes the resident the refresh was meant to
   * renew instead of dropping the event silently.
   */
  private long degradeToInvalidate(RefreshTask task, SyncSkipReason reason, boolean allowSoftExpire) {
    return degradeToInvalidate(task.batch(), task.event(), task.key(), task.version(), reason, allowSoftExpire);
  }

  /**
   * Applies the batch's covered INVALIDATE when its winning value message did not write. A value can
   * be refused by policy, by a loader or by a guard; in every case the covered removal is still
   * granted and must land at its effective stamp. Without this fallback a batch carrying both a value
   * and a removal would leave the removed key resident.
   */
  private void applyCoveredInvalidateFallback(String key, long coveredVersion) {
    if (coveredVersion > 0L) {
      evict(key, coveredVersion, false);
    }
  }

  /**
   * Returns the peer stamp if accepted by {@link #stampClock}, otherwise allocates a local stamp.
   * Observing keeps this node's future local versions ahead of every peer stamp it has seen; when a
   * peer stamp is behind or equal to a locally issued one, the peer value must not be written under
   * that stale version, so a fresh local stamp is used for the eviction instead.
   */
  private long resolveEffectiveVersion(long peerVersion) {
    if (stampClock == null || stampClock.observe(peerVersion)) {
      return peerVersion;
    }
    return stampClock.nextStamp();
  }

  /**
   * Merges accepted clears into the watermarks used by the sweep. A full clear takes the maximum
   * effective stamp and each prefix takes its own maximum. A full clear at or above a prefix stamp
   * subsumes that prefix, so the prefix is dropped — sweeping it again would double-count and fire a
   * redundant hook event.
   */
  private static ClearMarks collectMarks(List<SyncMessage> batch, Map<Long, Long> effective) {
    long rawFull = 0L;
    Map<String, Long> prefixStamps = new HashMap<>(2);
    for (SyncMessage message : batch) {
      switch (message.type()) {
        case INVALIDATE_ALL -> rawFull = Math.max(rawFull, message.version());
        case INVALIDATE_PREFIX -> prefixStamps.merge(message.cacheKey(), message.version(), Math::max);
        default -> {
        }
      }
    }

    long fullStamp = rawFull > 0L ? effective.getOrDefault(rawFull, rawFull) : 0L;
    prefixStamps.replaceAll((prefix, stamp) -> effective.getOrDefault(stamp, stamp));
    if (fullStamp > 0L) {
      prefixStamps.values().removeIf(prefixStamp -> prefixStamp <= fullStamp);
    }
    return new ClearMarks(fullStamp, Map.copyOf(prefixStamps));
  }

  /**
   * Maps each distinct clear peer stamp to its effective local stamp. Ascending peer order matters
   * when the stamp clock re-stamps a clock-ahead peer: later clears must still receive later local
   * stamps, otherwise a subsequent clear could look older than one already folded into a watermark
   * and be skipped.
   */
  private Map<Long, Long> resolveClearVersions(List<SyncMessage> batch) {
    TreeSet<Long> ordered = new TreeSet<>();
    for (SyncMessage message : batch) {
      if (message.type().isClear()) {
        ordered.add(message.version());
      }
    }
    Map<Long, Long> effective = new HashMap<>(ordered.size());
    for (Long stamp : ordered) {
      effective.put(stamp, resolveEffectiveVersion(stamp));
    }
    return effective;
  }

  /**
   * Builds hook events using the watermark actually applied at sweep time, not the peer stamp. Hooks
   * therefore observe the same version that guards subsequent fills, and a prefix clear subsumed by a
   * full clear reports the full stamp.
   */
  private static List<SyncEvent> clearEvents(List<SyncMessage> clears, ClearMarks marks) {
    List<SyncEvent> events = new ArrayList<>(clears.size());
    for (SyncMessage clear : clears) {
      long version =
        clear.type() == SyncType.INVALIDATE_ALL
          ? marks.fullStamp()
          : stampFor(clear.cacheKey(), marks.fullStamp(), marks.prefixStamps());
      events.add(SyncEvent.from(clear, version));
    }
    return events;
  }

  /**
   * Records clear watermarks before the sweep. The order is a guard, not a convenience: an in-flight
   * peer fill that checks staleness after the watermark is recorded is rejected, whereas recording it
   * afterwards would let that fill repopulate a key the clear just removed.
   */
  private void recordWatermarks(long fullStamp, Map<String, Long> prefixStamps) {
    if (fullStamp > 0L) {
      entryLifecycle.recordClear(fullStamp);
    }
    if (!prefixStamps.isEmpty()) {
      entryLifecycle.recordPrefixClears(prefixStamps);
    }
  }

  /**
   * Invalidates SingleFlight entries that a clear covers so an in-flight load cannot commit after the
   * sweep. A full clear kills every flight; a prefix clear kills only matching keys. The flag is
   * monotonic on the flight, so a leader already past its guard discards the result.
   */
  private void killClearedFlights(long fullStamp, Map<String, Long> prefixStamps) {
    if (singleFlight == null) {
      return;
    }
    if (fullStamp > 0L) {
      singleFlight.invalidateAll();
    } else {
      for (String prefix : prefixStamps.keySet()) {
        singleFlight.invalidatePrefix(prefix);
      }
    }
  }

  /**
   * Applies a refused clear on the consumer thread: records the watermarks, kills pre-clear flights,
   * then sweeps off the dispatcher. A rejected sweep submission runs inline so the clear still lands
   * before the delivery is acknowledged.
   */
  private void dispatchClearFallback(long fullStamp, Map<String, Long> prefixStamps) {
    recordWatermarks(fullStamp, prefixStamps);
    killClearedFlights(fullStamp, prefixStamps);
    Runnable task = () -> {
      try {
        sweep(fullStamp, prefixStamps);
      } catch (RuntimeException e) {
        ZetaExceptionHandler.handleException("clear sweep failed", e);
      }
    };
    try {
      sweepExecutor.execute(task);
    } catch (RejectedExecutionException ignored) {
      task.run();
    }
  }

  /**
   * Returns the highest clear stamp covering {@code key}. The full-clear stamp is the floor; a prefix
   * clear contributes only when its prefix is a string prefix of the key. Zero means no clear applies
   * and the key is left untouched by the sweep.
   */
  private static long stampFor(String key, long fullStamp, Map<String, Long> prefixStamps) {
    long stamp = fullStamp;
    for (Map.Entry<String, Long> entry : prefixStamps.entrySet()) {
      if (entry.getValue() > stamp && key.startsWith(entry.getKey())) {
        stamp = entry.getValue();
      }
    }
    return stamp;
  }

  /**
   * Returns the degradation reason when an absent or expired key may not be cold-populated on this
   * node, or {@code null} when a cold load is allowed.
   *
   * <p>Shared by apply time and the deferred refresh task: an entry that was live when the task was
   * queued can expire before the task runs, and the policy that forbids cold loads has to hold at
   * the moment of the load, not only at enqueue time.
   *
   * @param policy the namespace policy, or {@code null} when none is registered
   * @param key    the physical key
   * @param isPut  {@code true} when the incoming event carries its own value
   * @return the skip reason, or {@code null} to proceed
   */
  private @Nullable SyncSkipReason coldRefusal(@Nullable ResolvedNamespacePolicy policy, String key, boolean isPut) {
    // A dynamic-TTL namespace has no static lifetime baseline for creating an absent entry.
    if (policy != null && !policy.ttl().prewarmable()) {
      return SyncSkipReason.TTL_NOT_PREWARMABLE;
    }
    // Avoid cluster-wide cold reloads that multiply source load and evict locally hot keys.
    if (!isPut && !canColdRefresh(policy, key)) {
      return SyncSkipReason.ABSENT_NOT_PREWARMABLE;
    }
    return null;
  }

  /**
   * Whether a peer REFRESH for an absent key may query the source here. Without a namespace policy
   * only a locally hot key qualifies; with one, the namespace's prewarm trigger decides. The gate
   * keeps every node from triggering a simultaneous cold reload of the same absent key.
   */
  private boolean canColdRefresh(@Nullable ResolvedNamespacePolicy policy, String key) {
    if (policy == null) {
      return heatView == null || heatView.isHot(key);
    }
    return heatView != null && heatView.matches(policy.prewarm(), key);
  }

  /**
   * Runs {@link CacheLoader#isCacheable} defensively. A null loader admits unconditionally; a throwing
   * loader is treated as a refusal so a faulty policy cannot write an uncacheable value. The exception
   * is logged here and the caller degrades to an invalidation.
   */
  @SuppressWarnings("all")
  private boolean admitsValue(@Nullable CacheLoader<String, Object> loader, String key, Object value) {
    if (loader == null) {
      return true;
    }
    try {
      return loader.isCacheable(value);
    } catch (Exception e) {
      log.warn("CacheLoader.isCacheable failed for key={}, degrading to INVALIDATE", key, e);
      return false;
    }
  }

  /**
   * Whether a strictly newer replacement is parked in {@code slot}. Only a strictly greater version
   * supersedes: an equal version keeps the first task, so duplicate broadcasts do not cascade into
   * repeated reloads.
   */
  private static boolean isSuperseded(RefreshSlot slot, long version) {
    RefreshTask next = slot.next;
    return next != null && next.version() > version;
  }

  /**
   * Highest covered stamp a settle has to honour: the task's own batch coverage merged with
   * everything {@link #scheduleAsyncRefresh} folded into the slot while the task ran. The slot's
   * read is lock-free; a merge that lands after this snapshot is still picked up by the drain loop
   * through {@code remaining}.
   */
  private static long effectiveCovered(long coveredVersion, @Nullable RefreshSlot slot) {
    return slot == null ? coveredVersion : Math.max(coveredVersion, slot.activeCovered);
  }

  /**
   * Settles a REFRESH that lost to a newer task. The covered removal is not applied here: it is
   * merged into the slot and honoured by whichever task settles last, so a replacement that writes
   * a newer value is not preceded by an eviction of the resident it was about to renew. The skip is
   * reported as {@code COVERED_BY_NEWER_EVENT} rather than as a failure.
   */
  private void onRefreshSuperseded(RefreshTask task) {
    fireOnSkipped(task.event(), SyncSkipReason.COVERED_BY_NEWER_EVENT);
  }

  /**
   * Promotes the queued replacement to active under the map's compute, returning it together with
   * the covered stamp it inherits: whatever the settled task did not satisfy. An unsatisfied stamp
   * is never dropped with the slot — with a replacement parked it is folded into the task it hands
   * over, and without one it is returned to the caller, which applies it before the slot disappears.
   * The {@code existing != slot} identity check keeps a replaced slot's task from being mutated.
   *
   * @param satisfiedUpTo the highest covered stamp the settled task satisfied
   */
  @SuppressWarnings("all")
  private NextRefresh pollNextRefresh(String key, RefreshSlot slot, long satisfiedUpTo) {
    NextRefresh[] result = { new NextRefresh(null, 0L) };
    inFlightRefreshes.computeIfPresent(key, (k, existing) -> {
      if (existing != slot) {
        result[0] = new NextRefresh(null, 0L);
        return existing;
      }
      long remaining = existing.activeCovered > satisfiedUpTo ? existing.activeCovered : 0L;
      if (existing.next == null) {
        result[0] = new NextRefresh(null, remaining);
        return null;
      }
      RefreshTask next = existing.next;
      long nextActiveCovered = Math.max(existing.nextCovered, remaining);
      existing.activeVersion = next.version();
      existing.activeCovered = nextActiveCovered;
      existing.next = null;
      existing.nextCovered = 0L;
      result[0] = new NextRefresh(next, nextActiveCovered);
      return existing;
    });
    return result[0];
  }

  /**
   * Removes a rejected or failed slot and hands back its parked replacement plus the highest covered
   * stamp the slot still owed, so the caller can degrade both tasks at a version that also honours
   * the removal. The identity check prevents removing a slot that a newer submission has already
   * installed under the same key.
   */
  private ReleasedSlot releaseSlotOnRejection(String key, RefreshSlot slot) {
    ReleasedSlot[] result = { new ReleasedSlot(null, 0L) };
    inFlightRefreshes.computeIfPresent(key, (k, existing) -> {
      if (existing != slot) {
        result[0] = new ReleasedSlot(null, 0L);
        return existing;
      }
      result[0] = new ReleasedSlot(existing.next, Math.max(existing.activeCovered, existing.nextCovered));
      existing.next = null;
      existing.nextCovered = 0L;
      return null;
    });
    return result[0];
  }

  /**
   * Returns the entry only when it is non-null and not logically expired. Version and soft-expiry
   * guards must not resurrect an entry Caffeine would no longer serve; treating it as absent keeps a
   * stale resident from blocking a peer value.
   */
  private @Nullable CacheEntry liveResident(@Nullable CacheEntry entry) {
    return ttlPolicy.isLive(entry) ? entry : null;
  }

  /**
   * Captures an immutable {@link EntryView} for policy decisions. Policies see data version and TTL
   * state but cannot mutate the L1 entry, and the snapshot stays stable for the whole decision even
   * if a concurrent write changes the resident.
   */
  private @Nullable EntryView view(String key) {
    return EntryView.snapshot(cache, key, ttlPolicy);
  }

  /**
   * Reports a successfully applied peer value after the L1 write settles. The entry is snapshotted at
   * call time so hooks observe the value that was written, not a later concurrent mutation.
   */
  private void fireAfterValueApplied(SyncEvent event, @Nullable CacheEntry entry) {
    if (hooks.isEmpty() || entry == null) {
      return;
    }
    EntryView view = EntryView.of(entry, ttlPolicy, TimeSource.monotonicMillis());
    fireHooks("afterValueApplied", eventKey(event), hook -> hook.afterValueApplied(event, view));
  }

  /**
   * Reports a completed local eviction. Called only after the version guard and the eviction itself
   * succeeded, so hooks never see an invalidation that did not take effect.
   */
  private void fireAfterInvalidate(SyncEvent event) {
    if (hooks.isEmpty()) {
      return;
    }
    fireHooks("afterInvalidate", eventKey(event), hook -> hook.afterInvalidate(event));
  }

  /**
   * Reports that a value event was downgraded to a versioned eviction. The reason is the original
   * refusal; a stale eviction is reported as a skip instead and does not reach this hook.
   */
  private void fireAfterValueDegraded(SyncEvent event, SyncSkipReason reason) {
    if (hooks.isEmpty()) {
      return;
    }
    fireHooks("afterValueDegraded", eventKey(event), hook -> hook.afterValueDegraded(event, reason));
  }

  /**
   * Reports a message that the pipeline settled without a write. Every skip reason flows through this
   * one path so hooks have a complete, ordered view of what the batch did and did not apply.
   */
  private void fireOnSkipped(SyncEvent event, SyncSkipReason reason) {
    if (hooks.isEmpty()) {
      return;
    }
    fireHooks("onSkipped", eventKey(event), hook -> hook.onSkipped(event, reason));
  }

  /**
   * Dispatches one hook callback through the shared guard. A throwing hook is logged and contained so
   * a user callback cannot abort the L1 mutation that just committed or prevent later hooks from
   * running.
   */
  private void fireHooks(String hookName, String key, Consumer<SyncHook> callback) {
    DecisionPlaneSupport.fireHooks(hooks, "SyncHook", hookName, key, callback, log);
  }

  /**
   * Returns the event key for logs and hook diagnostics, substituting {@code "clear"} for clear
   * events whose key is null.
   */
  private static String eventKey(SyncEvent event) {
    return event.key() != null ? event.key() : "clear";
  }

  /** Captured context for one asynchronously executed peer REFRESH task. */
  private record RefreshTask(
    Batch.Value batch,
    SyncEvent event,
    @Nullable ResolvedNamespacePolicy resolvedPolicy,
    CacheLoader<String, Object> loader,
    SyncPolicy policy,
    long coveredVersion
  ) {
    /**
     * The target key, read from the event so the task and its batch cannot drift apart.
     */
    private String key() {
      return event.key();
    }

    /**
     * The effective version used for coalescing and guards, already re-stamped by the caller.
     */
    private long version() {
      return event.version();
    }
  }

  /**
   * Per-key coalescing slot: the executing task's version and outstanding covered stamp, plus at
   * most one newest replacement.
   *
   * <p>Every field is written under the {@code inFlightRefreshes} per-key bin lock (inside the
   * {@code compute} of {@link #scheduleAsyncRefresh}, {@link #pollNextRefresh} and
   * {@link #releaseSlotOnRejection}). {@link #activeCovered} is additionally {@code volatile}
   * because a settling task reads it without the lock. {@link #nextCovered} stays plain: it is
   * only ever read under that lock.
   */
  private static final class RefreshSlot {

    volatile long activeVersion;
    volatile long activeCovered;
    volatile @Nullable RefreshTask next;
    long nextCovered;

    /**
     * Creates a slot for a task about to run. The active version and its covered stamp are seeded
     * before submission so a concurrent newer task is parked as a replacement rather than starting
     * a second load.
     */
    private RefreshSlot(long activeVersion, long activeCovered) {
      this.activeVersion = activeVersion;
      this.activeCovered = activeCovered;
    }
  }

  /** The promoted replacement and the covered stamp it inherits. */
  private record NextRefresh(@Nullable RefreshTask task, long activeCovered) {}

  /** A released slot's parked replacement and the highest covered stamp the slot still owed. */
  private record ReleasedSlot(@Nullable RefreshTask pending, long coveredVersion) {}

  /** Immutable table-wide and prefix-scoped clear watermarks resolved for one clear batch. */
  private record ClearMarks(long fullStamp, Map<String, Long> prefixStamps) {}
}
