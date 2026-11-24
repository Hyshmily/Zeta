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
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;
import java.util.function.Supplier;

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

    if (!evict(key, version, false)) {
      fireOnSkipped(event, SyncSkipReason.VERSION_STALE);
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
      fireOnSkipped(event, stale);
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
      finishValue(batch, event, key, version, true, decoded, resolvedPolicy, loader, policy, coveredVersion);
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
   */
  private void finishValue(
    Batch.Value batch,
    SyncEvent event,
    String key,
    long version,
    boolean isPut,
    Object value,
    @Nullable ResolvedNamespacePolicy resolvedPolicy,
    @Nullable CacheLoader<String, Object> loader,
    SyncPolicy policy,
    long coveredVersion
  ) {
    EntryView resolvedView = view(key);
    SyncSkipReason staleAfterResolution = staleReason(key, resolvedView, version, true);
    if (staleAfterResolution != null) {
      fireOnSkipped(event, staleAfterResolution);
      return;
    }

    SyncValueAction action = callPolicy(
      event,
      "settle",
      () -> policy.settle(event, value, resolvedView),
      () -> applyCoveredInvalidateFallback(key, coveredVersion)
    );
    if (action == null) {
      return;
    }
    if (action instanceof SyncValueAction.Skip) {
      rejectValue(event, key, SyncSkipReason.POLICY_REJECTED, coveredVersion);
      return;
    }
    if (action instanceof SyncValueAction.Invalidate) {
      degradeToInvalidate(batch, event, key, version, SyncSkipReason.DEGRADED_TO_INVALIDATE);
      return;
    }

    // SyncValueAction is sealed to Skip, Invalidate and Apply, so this is the apply case.
    Object resolved = ((SyncValueAction.Apply) action).value();
    // Skip redundant loader admission on an unchanged REFRESH value (already admitted by the
    // refresh task before it resolved the value).
    if ((isPut || resolved != value) && !admitsValue(loader, key, resolved)) {
      degradeToInvalidate(batch, event, key, version, SyncSkipReason.VALUE_DECLINED_BY_LOADER);
      return;
    }

    SyncSkipReason[] refuseReason = new SyncSkipReason[1];
    Applied applied = writePeerValue(key, version, resolved, resolvedPolicy, refuseReason);
    if (!applied.changed()) {
      SyncSkipReason reason = refuseReason[0] != null ? refuseReason[0] : SyncSkipReason.VERSION_STALE;
      if (reason == SyncSkipReason.TTL_NOT_PREWARMABLE) {
        // Resident expired mid-load; settle identically to cold-path degradation.
        degradeToInvalidate(batch, event, key, version, reason);
        return;
      }
      fireOnSkipped(event, reason);
      return;
    }
    fireAfterValueApplied(event, applied.current());
  }

  /**
   * Schedules a peer REFRESH onto {@link #refreshExecutor} with per-key coalescing (one active
   * load plus one newest pending replacement). Lower or equal versions are skipped immediately.
   *
   * @param task the refresh task to execute
   */
  @SuppressWarnings("all")
  private void scheduleAsyncRefresh(RefreshTask task) {
    RefreshTask[] superseded = new RefreshTask[1];
    boolean[] shouldSubmit = { false };
    RefreshSlot slot = inFlightRefreshes.compute(task.key(), (key, existing) -> {
      if (existing == null) {
        shouldSubmit[0] = true;
        return new RefreshSlot(task.version());
      }
      if (task.version() <= existing.activeVersion) {
        superseded[0] = task;
      } else if (existing.next == null) {
        existing.next = task;
      } else if (task.version() > existing.next.version()) {
        superseded[0] = existing.next;
        existing.next = task;
      } else {
        superseded[0] = task;
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
      RefreshTask pending = releaseSlotOnRejection(task.key(), slot);
      degradeToInvalidate(task, SyncSkipReason.REFRESH_REJECTED);
      if (pending != null) {
        degradeToInvalidate(pending, SyncSkipReason.REFRESH_REJECTED);
      }
    } catch (RuntimeException | Error failure) {
      RefreshTask pending = releaseSlotOnRejection(task.key(), slot);
      degradeToInvalidate(task, SyncSkipReason.REFRESH_FAILED);
      if (pending != null) {
        degradeToInvalidate(pending, SyncSkipReason.REFRESH_FAILED);
      }
      throw failure;
    }
  }

  /**
   * Drains the active refresh task and, in the same executor turn, the newest replacement queued
   * behind it. Failures are degraded individually so one bad load cannot strand later tasks. The
   * finally block covers abnormal exit (an {@link Error} outside the per-task catch) by releasing the
   * slot and degrading the task that was still running; the slot is released here rather than in the
   * caller because only this loop knows whether a replacement became active.
   */
  private void drainAsyncRefresh(RefreshTask first, RefreshSlot slot) {
    RefreshTask current = first;
    boolean completedNormally = false;
    try {
      while (current != null) {
        RefreshTask task = current;
        try {
          executeRefreshTask(task, slot);
        } catch (RuntimeException e) {
          log.warn("Async refresh failed: key={}", task.key(), e);
          degradeToInvalidate(task, SyncSkipReason.REFRESH_FAILED);
        }
        // The task above is settled either way: clearing the cursor before the poll keeps the
        // finally block from degrading it a second time if the poll itself throws.
        current = null;
        current = pollNextRefresh(first.key(), slot);
      }
      completedNormally = true;
    } finally {
      if (!completedNormally) {
        RefreshTask pending = releaseSlotOnRejection(first.key(), slot);
        if (current != null) {
          degradeToInvalidate(current, SyncSkipReason.REFRESH_FAILED);
        }
        if (pending != null) {
          degradeToInvalidate(pending, SyncSkipReason.REFRESH_FAILED);
        }
      }
    }
  }

  /**
   * Executes one offloaded REFRESH task. Supersession and version/tombstone guards are checked both
   * before and after the blocking loader call: the task may have been parked behind a newer one, or a
   * peer write may have landed while the source was being read. The cold-load policy is re-checked as
   * well, since an entry that was live at enqueue time can expire before the task runs.
   */
  private void executeRefreshTask(RefreshTask task, RefreshSlot slot) {
    if (isSuperseded(slot, task.version())) {
      onRefreshSuperseded(task);
      return;
    }

    EntryView preLoad = view(task.key());
    SyncSkipReason stale = staleReason(task.key(), preLoad, task.version(), true);
    if (stale != null) {
      fireOnSkipped(task.event(), stale);
      return;
    }

    boolean hasResident = EntryView.isLive(preLoad);
    if (!hasResident) {
      // The entry may have expired or been evicted while this task waited in the refresh queue, so
      // the cold-load policy is re-checked here instead of only at apply time.
      SyncSkipReason refusal = coldRefusal(task.resolvedPolicy(), task.key(), false);
      if (refusal != null) {
        degradeToInvalidate(task, refusal);
        return;
      }
    }

    Object loaded;
    try {
      loaded = hasResident ? task.loader().reload(task.key()) : task.loader().load(task.key());
    } catch (RuntimeException e) {
      // A task superseded while its load was in flight is reported as covered, not as a failure:
      // the newer task owns the key's outcome.
      if (isSuperseded(slot, task.version())) {
        onRefreshSuperseded(task);
        return;
      }
      log.warn("refresh: load failed for key={}", task.key(), e);
      degradeToInvalidate(task, SyncSkipReason.REFRESH_FAILED);
      return;
    }

    if (isSuperseded(slot, task.version())) {
      onRefreshSuperseded(task);
      return;
    }
    if (loaded == null) {
      degradeToInvalidate(task, SyncSkipReason.VALUE_NOT_FOUND);
      return;
    }
    if (!admitsValue(task.loader(), task.key(), loaded)) {
      degradeToInvalidate(task, SyncSkipReason.VALUE_DECLINED_BY_LOADER);
      return;
    }
    finishValue(
      task.batch(),
      task.event(),
      task.key(),
      task.version(),
      false,
      loaded,
      task.resolvedPolicy(),
      task.loader(),
      task.policy(),
      task.coveredVersion()
    );
  }

  /**
   * Writes a peer value into L1 under tombstone and refresh version guards, preserving resident
   * TTLs or using a prewarmable namespace contract for absent entries, then drops dedup and
   * best-effort re-arms {@link #warmScheduler}.
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
      if (resident != null && VersionGuard.shouldSkipForRefresh(resident, version)) {
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

  /**
   * Performs version-guarded local eviction inside the key's map bin: records the invalidation
   * tombstone, drops SingleFlight dedup, and either closes the soft window (when
   * {@code softExpireHot} applies to a live hot resident with soft TTL) or removes the entry and
   * cancels its warm task.
   *
   * @param key           the affected physical key
   * @param version       the invalidation version stamp
   * @param softExpireHot {@code true} to soft-expire eligible hot entries instead of deleting them
   * @return {@code true} if the tombstone and eviction applied; {@code false} if a newer resident
   *         outranked {@code version}
   */
  @SuppressWarnings("all")
  private boolean evict(String key, long version, boolean softExpireHot) {
    boolean[] stale = { false };
    boolean[] removed = { false };
    cache
      .asMap()
      .compute(key, (k, existing) -> {
        CacheEntry resident = liveResident(existing);
        if (resident != null && VersionGuard.shouldSkipForRefresh(resident, version)) {
          stale[0] = true;
          return existing;
        }

        entryLifecycle.recordInvalidation(key, version);
        if (singleFlight != null) {
          singleFlight.invalidate(key);
        }
        if (existing == null) {
          // Entry is already absent, but a scheduled warm task may still be armed.
          removed[0] = true;
          return null;
        }
        if (
          resident != null && softExpireHot && existing.getSoftTtlMs() > 0L && heatView != null && heatView.isHot(key)
        ) {
          if (ttlPolicy.isSoftExpired(existing)) {
            return existing;
          }
          // Set soft deadline 1ms in the past so reads in the same millisecond trigger soft refresh.
          return entryLifecycle.editEntry(existing).softExpiryAt(TimeSource.currentTimeMillis() - 1L).build();
        }
        removed[0] = true;
        return null;
      });
    if (removed[0] && warmScheduler != null) {
      warmScheduler.cancel(key);
    }
    return !stale[0];
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

  /**
   * Sweeps L1 entries targeted by {@code fullStamp} or {@code prefixStamps}, removing residents
   * at or below the applicable stamp and cancelling their warm tasks.
   *
   * @return number of live entries removed by this sweep
   */
  @SuppressWarnings("all")
  private int sweep(long fullStamp, Map<String, Long> prefixStamps) {
    int removed = 0;
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
      // Pre-clear flights were already invalidated above; watermarks block stale fills.
      boolean[] liveRemoved = { false };
      Applied removal = EntryMutations.apply(cache, key, existing -> {
        // Reset on every attempt: a contended bin can re-run this function against a different image.
        liveRemoved[0] = false;
        if (
          existing == null || (liveResident(existing) != null && VersionGuard.shouldSkipForRefresh(existing, stamp))
        ) {
          return existing;
        }

        // An entry past its hard deadline is already unservable: it still has to be removed (and its
        // warm task cancelled), but it was not an entry this clear took away.
        liveRemoved[0] = liveResident(existing) != null;
        return null;
      });

      if (liveRemoved[0]) {
        removed++;
      }
      if (removal.changed() && warmScheduler != null) {
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
   * Checks version and optional tombstone guards before policy invocation or write-back. A live
   * resident with a strictly newer data version makes the event stale; a tombstone check is
   * requested only for value paths, where a prior invalidation or clear at an equal or newer stamp
   * must suppress a late peer value. Returning a reason here avoids running a loader for an event
   * that cannot win.
   *
   * @param checkTombstone whether to consult the invalidation/prefix-clear watermarks
   * @return the skip reason, or {@code null} when the event may proceed
   */
  @SuppressWarnings("all")
  private @Nullable SyncSkipReason staleReason(
    String key,
    @Nullable EntryView current,
    long version,
    boolean checkTombstone
  ) {
    if (EntryView.isLive(current) && current.dataVersion() > version) {
      return SyncSkipReason.VERSION_STALE;
    }
    if (checkTombstone && entryLifecycle.staleAgainstInvalidation(key, version)) {
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
   */
  private void degradeToInvalidate(
    Batch.Value batch,
    SyncEvent event,
    String key,
    long version,
    SyncSkipReason reason
  ) {
    if (!evict(key, version, batch.allowSoftExpire())) {
      fireOnSkipped(event, SyncSkipReason.VERSION_STALE);
      return;
    }
    fireAfterValueDegraded(event, reason);
  }

  /**
   * Degrades a captured refresh task to a versioned local invalidation. Used by the executor
   * rejection path, the drain loop and its cleanup: in every case the load produced no value to
   * settle, and evicting at the task's version still removes the resident the refresh was meant to
   * renew instead of dropping the event silently.
   */
  private void degradeToInvalidate(RefreshTask task, SyncSkipReason reason) {
    degradeToInvalidate(task.batch(), task.event(), task.key(), task.version(), reason);
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
   * Settles a REFRESH that lost to a newer task. Its covered INVALIDATE still applies, so removal
   * intent carried in the batch is not lost with the superseded value; the skip is reported as
   * {@code COVERED_BY_NEWER_EVENT} rather than as a failure.
   */
  private void onRefreshSuperseded(RefreshTask task) {
    applyCoveredInvalidateFallback(task.key(), task.coveredVersion());
    fireOnSkipped(task.event(), SyncSkipReason.COVERED_BY_NEWER_EVENT);
  }

  /**
   * Promotes the queued replacement to active under the map's compute, returning it for the drain
   * loop to execute. The identity check ({@code existing != slot}) is essential: a rejected slot may
   * already have been removed and replaced, and mutating the newer slot would strand its task.
   */
  @SuppressWarnings("all")
  private @Nullable RefreshTask pollNextRefresh(String key, RefreshSlot slot) {
    RefreshTask[] next = new RefreshTask[1];
    inFlightRefreshes.computeIfPresent(key, (k, existing) -> {
      if (existing != slot) {
        return existing;
      }
      if (existing.next != null) {
        next[0] = existing.next;
        existing.activeVersion = existing.next.version();
        existing.next = null;
        return existing;
      }
      return null;
    });
    return next[0];
  }

  /**
   * Removes a rejected or failed slot and hands back its parked replacement so the caller can degrade
   * both tasks. The identity check prevents removing a slot that a newer submission has already
   * installed under the same key.
   */
  private @Nullable RefreshTask releaseSlotOnRejection(String key, RefreshSlot slot) {
    RefreshTask[] pending = new RefreshTask[1];
    inFlightRefreshes.computeIfPresent(key, (k, existing) -> {
      if (existing != slot) {
        return existing;
      }
      pending[0] = existing.next;
      existing.next = null;
      return null;
    });
    return pending[0];
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
    EntryView view = EntryView.of(entry, ttlPolicy, TimeSource.currentTimeMillis());
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

  /** Per-key coalescing slot holding the active refresh version and at most one newest replacement. */
  private static final class RefreshSlot {

    volatile long activeVersion;
    volatile @Nullable RefreshTask next;

    /**
     * Creates a slot for a task about to run. The active version is seeded before submission so a
     * concurrent higher-version task is parked as a replacement rather than starting a second load.
     */
    private RefreshSlot(long activeVersion) {
      this.activeVersion = activeVersion;
    }
  }

  /** Immutable table-wide and prefix-scoped clear watermarks resolved for one clear batch. */
  private record ClearMarks(long fullStamp, Map<String, Long> prefixStamps) {}
}
