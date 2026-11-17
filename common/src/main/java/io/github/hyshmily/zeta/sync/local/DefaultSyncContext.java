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
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.TtlPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryMutations;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryMutations.Applied;
import io.github.hyshmily.zeta.sync.DecisionPlaneSupport;
import io.github.hyshmily.zeta.sync.DecisionPlaneSupport.LoadResult;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.clock.StampClock;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Applies one granted sync batch through the {@link SyncPolicy} decision points and the
 * framework-owned guards. The policy sees {@link SyncEvent} and {@link EntryView} only; every
 * write, tombstone, clear watermark and terminal hook stays here.
 *
 * <p>The policy is called for the effective value event only (once per granted clear).
 * Covered batch messages are reported as {@link SyncSkipReason#COVERED_BY_NEWER_EVENT}; a
 * winner that refuses or fails still gets exactly one terminal callback, and a covered
 * INVALIDATE still takes effect when the winning value message is refused.
 */
@Slf4j
@Internal
public class DefaultSyncContext {

  @Nullable
  private final CacheLoader<String, Object> clusterLoader;

  private final Cache<String, CacheEntry> cache;

  @Nullable
  private final PutPayloadCodec payloadCodec;

  /** The node's data-stamp clock, or {@code null} when this context is used without one. */
  @Nullable
  private final StampClock stampClock;

  private final EntryLifecycle entryLifecycle;

  @Nullable
  private final SingleFlight singleFlight;

  private final List<SyncHook> hooks;

  private final TtlPolicy ttlPolicy;

  /**
   * Runs the coalesced sweep of a clear the dispatcher refused. Inline by default so a
   * direct construction (tests, non-Spring embedding) still applies the clear.
   */
  private final Executor sweepExecutor;

  /** The merged clear marks waiting for the next sweep; {@code null} when none is pending. */
  private final AtomicReference<ClearMarks> pendingSweep = new AtomicReference<>();

  /** One in-flight sweep at a time; the drain re-arms when a clear lands during the sweep. */
  private final AtomicBoolean sweepScheduled = new AtomicBoolean();

  /**
   * Creates a context whose refused-clear sweep runs inline and with no stamp clock, for
   * direct construction in tests and non-Spring embedding.
   *
   * @param cache          the L1 table
   * @param clusterLoader  the optional authoritative loader
   * @param entryLifecycle the entry lifecycle and TTL owner
   * @param hooks          the terminal hooks; {@code null} means none
   * @param singleFlight   the optional dedup layer
   * @param payloadCodec   the optional PUT payload codec
   */
  public DefaultSyncContext(
    Cache<String, CacheEntry> cache,
    @Nullable CacheLoader<String, Object> clusterLoader,
    EntryLifecycle entryLifecycle,
    @Nullable List<SyncHook> hooks,
    @Nullable SingleFlight singleFlight,
    @Nullable PutPayloadCodec payloadCodec
  ) {
    this(cache, clusterLoader, entryLifecycle, hooks, singleFlight, payloadCodec, Runnable::run, null);
  }

  /**
   * Creates a context with an explicit executor for the coalesced sweep of a refused clear
   * and no stamp clock: a refused peer version is persisted as received.
   *
   * @param cache          the L1 table
   * @param clusterLoader  the optional authoritative loader
   * @param entryLifecycle the entry lifecycle and TTL owner
   * @param hooks          the terminal hooks; {@code null} means none
   * @param singleFlight   the optional dedup layer
   * @param payloadCodec   the optional PUT payload codec
   * @param sweepExecutor  the executor for the refused-clear sweep; rejection runs it inline
   */
  public DefaultSyncContext(
    Cache<String, CacheEntry> cache,
    @Nullable CacheLoader<String, Object> clusterLoader,
    EntryLifecycle entryLifecycle,
    @Nullable List<SyncHook> hooks,
    @Nullable SingleFlight singleFlight,
    @Nullable PutPayloadCodec payloadCodec,
    Executor sweepExecutor
  ) {
    this(cache, clusterLoader, entryLifecycle, hooks, singleFlight, payloadCodec, sweepExecutor, null);
  }

  /**
   * Creates a context with the node's stamp clock. A peer version the anti-drift guard
   * refuses is replaced with a fresh local allocation when the reduced per-key batch is
   * applied, so the replacement order is the apply order (design decisions: version stamps).
   *
   * @param cache          the L1 table
   * @param clusterLoader  the optional authoritative loader
   * @param entryLifecycle the entry lifecycle and TTL owner
   * @param hooks          the terminal hooks; {@code null} means none
   * @param singleFlight   the optional dedup layer
   * @param payloadCodec   the optional PUT payload codec
   * @param sweepExecutor  the executor for the refused-clear sweep; rejection runs it inline
   * @param stampClock     the node's data-stamp clock; {@code null} persists the peer version
   */
  public DefaultSyncContext(
    Cache<String, CacheEntry> cache,
    @Nullable CacheLoader<String, Object> clusterLoader,
    EntryLifecycle entryLifecycle,
    @Nullable List<SyncHook> hooks,
    @Nullable SingleFlight singleFlight,
    @Nullable PutPayloadCodec payloadCodec,
    Executor sweepExecutor,
    @Nullable StampClock stampClock
  ) {
    this.singleFlight = singleFlight;
    this.cache = cache;
    this.clusterLoader = clusterLoader;
    this.entryLifecycle = entryLifecycle;
    this.payloadCodec = payloadCodec;
    this.stampClock = stampClock;
    this.ttlPolicy = entryLifecycle.ttlPolicy();
    this.hooks = hooks != null ? hooks : List.of();
    this.sweepExecutor = sweepExecutor;
  }

  /**
   * Applies one granted batch: the reduced winner through the policy and guards, every
   * covered message through the terminal skip hook.
   *
   * @param batch  the reduced batch; never {@code null}
   * @param policy the event policy; never {@code null}
   */
  public void apply(Batch batch, SyncPolicy policy) {
    if (batch instanceof Batch.Value value) {
      applyValueBatch(value, policy);
    } else {
      applyClearBatch((Batch.Clear) batch, policy);
    }
  }

  /**
   * Consumer-thread fallback for a refused INVALIDATE: records the tombstone and removes the
   * resident without firing user hooks.
   *
   * @param message the invalidation message
   */
  public void invalidateNow(SyncMessage message) {
    evict(message.cacheKey(), resolveEffectiveVersion(message.version()), false);
  }

  /**
   * Consumer-thread fallback for a refused clear: records the watermarks and kills the
   * pre-clear flights, then hands the table sweep to the configured executor.
   *
   * @param batch the clear batch
   */
  public void clearNow(Batch.Clear batch) {
    ClearMarks marks = collectMarks(batch.messages());
    if (marks.isEmpty()) {
      return;
    }
    long fullStamp = marks.fullStamp();
    Map<String, Long> prefixStamps = marks.prefixStamps();
    recordWatermarks(fullStamp, prefixStamps);
    killClearedFlights(fullStamp, prefixStamps);
    requestSweep(marks);
  }

  /**
   * Applies a reduced single-key batch: every covered message receives its terminal skip
   * callback first, then the winner follows the INVALIDATE or value path.
   *
   * @param batch  the reduced value batch
   * @param policy the event policy
   */
  private void applyValueBatch(Batch.Value batch, SyncPolicy policy) {
    SyncMessage winner = batch.winner();
    for (SyncMessage covered : batch.covered()) {
      fireOnSkipped(SyncEvent.from(covered), SyncSkipReason.COVERED_BY_NEWER_EVENT);
    }
    // Reduce on peer versions first; only the applied winner is re-stamped.
    SyncEvent event = SyncEvent.from(winner, resolveEffectiveVersion(winner.version()));
    if (winner.type() == SyncType.INVALIDATE) {
      applyInvalidate(event, policy);
    } else {
      applyValue(batch, event, policy);
    }
  }

  /**
   * Applies the winning INVALIDATE through the policy and the version guard. A stale or
   * refused event reports its skip; an applied one records the tombstone, removes the resident
   * and fires {@code afterInvalidate}.
   *
   * @param event  the winning key event
   * @param policy the event policy
   */
  private void applyInvalidate(SyncEvent event, SyncPolicy policy) {
    SyncEvent.KeyEvent keyEvent = (SyncEvent.KeyEvent) event;
    String key = keyEvent.cacheKey();
    long version = keyEvent.version();
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
    if (admission instanceof SyncAdmission.Skip) {
      fireOnSkipped(event, SyncSkipReason.POLICY_REJECTED);
      return;
    }
    if (admission instanceof SyncAdmission.InvalidateLocal) {
      // An INVALIDATE is already a local invalidation; the action is meaningless here.
      fireOnSkipped(event, SyncSkipReason.POLICY_FAILED);
      return;
    }

    EvictOutcome outcome = evict(key, version, false);
    if (outcome.stale()) {
      fireOnSkipped(event, SyncSkipReason.VERSION_STALE);
      return;
    }
    fireAfterInvalidate(event);
  }

  /**
   * Applies the winning PUT/REFRESH through the policy, value resolution and guarded write. A
   * refused or failed winner still receives one terminal callback, and a covered INVALIDATE
   * that the batch carried still evicts at its own version before that callback.
   *
   * @param batch  the reduced value batch
   * @param event  the winner's key event
   * @param policy the event policy
   */
  private void applyValue(Batch.Value batch, SyncEvent event, SyncPolicy policy) {
    SyncEvent.KeyEvent keyEvent = (SyncEvent.KeyEvent) event;
    String key = keyEvent.cacheKey();
    long version = keyEvent.version();
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
      () -> applyCoveredInvalidateFallback(batch, key)
    );
    if (admission == null) {
      return;
    }
    if (admission instanceof SyncAdmission.Skip) {
      applyCoveredInvalidateFallback(batch, key);
      fireOnSkipped(event, SyncSkipReason.POLICY_REJECTED);
      return;
    }
    if (admission instanceof SyncAdmission.InvalidateLocal) {
      degradeToInvalidate(batch, event, key, version, SyncSkipReason.DEGRADED_TO_INVALIDATE);
      return;
    }
    if (!(admission instanceof SyncAdmission.Proceed)) {
      applyCoveredInvalidateFallback(batch, key);
      fireOnSkipped(event, SyncSkipReason.POLICY_FAILED);
      return;
    }

    Object value;
    SyncSkipReason noValueReason;
    if (isPut) {
      value = payloadCodec == null ? null : payloadCodec.decode(batch.winner().payload());
      noValueReason = SyncSkipReason.PAYLOAD_UNREADABLE;
    } else {
      LoadResult loaded = load(key);
      value = loaded.value();
      noValueReason = loaded.declined() ? SyncSkipReason.VALUE_DECLINED_BY_LOADER : SyncSkipReason.VALUE_NOT_FOUND;
    }
    if (value == null) {
      degradeToInvalidate(batch, event, key, version, noValueReason);
      return;
    }

    // Resolution is the long window (a REFRESH load or a PUT decode): a concurrent INVALIDATE
    // or clear can land here. The policy sees the post-resolution resident, and an event the
    // race made stale settles before the policy is called.
    EntryView resolvedView = view(key);
    SyncSkipReason staleAfterResolution = staleReason(key, resolvedView, version, true);
    if (staleAfterResolution != null) {
      fireOnSkipped(event, staleAfterResolution);
      return;
    }

    SyncValueAction action = callPolicy(
      event,
      "onValueResolved",
      () -> policy.onValueResolved(event, value, resolvedView),
      () -> applyCoveredInvalidateFallback(batch, key)
    );
    if (action == null) {
      return;
    }
    if (action instanceof SyncValueAction.Skip) {
      applyCoveredInvalidateFallback(batch, key);
      fireOnSkipped(event, SyncSkipReason.POLICY_REJECTED);
      return;
    }
    if (action instanceof SyncValueAction.Invalidate) {
      degradeToInvalidate(batch, event, key, version, SyncSkipReason.DEGRADED_TO_INVALIDATE);
      return;
    }
    if (!(action instanceof SyncValueAction.Apply apply)) {
      applyCoveredInvalidateFallback(batch, key);
      fireOnSkipped(event, SyncSkipReason.POLICY_FAILED);
      return;
    }

    Object resolved = apply.value();
    if (!admitsValue(key, resolved)) {
      degradeToInvalidate(batch, event, key, version, SyncSkipReason.VALUE_DECLINED_BY_LOADER);
      return;
    }

    SyncSkipReason[] refuseReason = new SyncSkipReason[1];
    Applied applied = writePeerValue(key, version, resolved, refuseReason);
    if (!applied.changed()) {
      SyncSkipReason reason = refuseReason[0] != null ? refuseReason[0] : SyncSkipReason.VERSION_STALE;
      fireOnSkipped(event, reason);
      return;
    }
    log.debug("Applied peer sync value: {}", key);
    fireAfterValueApplied(event, applied.current());
  }

  /**
   * Runs one policy decision and settles a thrown exception or {@code null} result as
   * {@link SyncSkipReason#POLICY_FAILED}. A failure action runs before the skip callback so
   * a covered INVALIDATE can still take effect.
   *
   * @param event     the event being settled
   * @param step      the policy method name for the WARN line
   * @param call      the policy invocation
   * @param onFailure extra refusal cleanup, or {@code null}
   * @param <R>       the policy result type
   * @return the result, or {@code null} when the policy failed
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
   * Applies a granted clear batch in one pass: each clear is admitted independently, refused
   * clears report their skip, and the accepted watermarks are coalesced into one table sweep.
   *
   * @param batch  the granted clear batch
   * @param policy the event policy
   */
  private void applyClearBatch(Batch.Clear batch, SyncPolicy policy) {
    List<SyncMessage> accepted = new ArrayList<>(batch.messages().size());
    for (SyncMessage message : batch.messages()) {
      SyncEvent event = SyncEvent.from(message);
      SyncAdmission admission = callPolicy(event, "admit", () -> policy.admit(event, null), null);
      if (admission == null) {
        continue;
      }
      if (admission instanceof SyncAdmission.Skip) {
        fireOnSkipped(event, SyncSkipReason.POLICY_REJECTED);
        continue;
      }
      // InvalidateLocal is redundant for a clear; treat it as Proceed.
      accepted.add(message);
    }
    if (accepted.isEmpty()) {
      return;
    }

    ClearMarks marks = collectMarks(accepted);
    if (marks.isEmpty()) {
      return;
    }

    long fullStamp = marks.fullStamp();
    HashMap<String, Long> prefixStamps = new HashMap<>(marks.prefixStamps());
    recordWatermarks(fullStamp, prefixStamps);
    killClearedFlights(fullStamp, prefixStamps);

    int removed = sweep(fullStamp, prefixStamps);
    logSweep("applied", marks, removed);
    fireAfterClear(clearEvents(accepted, marks), removed);
  }

  /**
   * The hook's clear events, each at the highest watermark the sweep applies to its key set:
   * a refused peer stamp is reported at its local replacement, and a prefix clear covered by
   * a newer table-wide clear at that table-wide stamp.
   *
   * @param clears the accepted clear messages, in submission order
   * @param marks  the watermarks resolved for the batch
   * @return one event per accepted clear, in submission order
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
   * The version/tombstone pre-check shared by value and invalidate events. A newer live
   * resident or an applied tombstone settles the event before any policy call or I/O; a value
   * event runs it again after its resolution, because a load or decode is a window in which a
   * concurrent invalidation can land.
   *
   * @param key            the affected key
   * @param current        the snapshot, or {@code null}
   * @param version        the incoming data stamp
   * @param checkTombstone whether an applied tombstone/watermark outranks this event; an
   *                       INVALIDATE stays false so a resident below the tombstone is still
   *                       removed by the eviction
   * @return the skip reason, or {@code null} when the event may proceed
   */
  private @Nullable SyncSkipReason staleReason(
    String key,
    @Nullable EntryView current,
    long version,
    boolean checkTombstone
  ) {
    if (current != null && !current.logicallyExpired() && current.dataVersion() > version) {
      return SyncSkipReason.VERSION_STALE;
    }
    if (checkTombstone && entryLifecycle.staleAgainstInvalidation(key, version)) {
      return SyncSkipReason.INVALIDATION_TOMBSTONE;
    }
    return null;
  }

  /**
   * The framework's fallback for a value that produced no writable value: apply the same
   * versioned invalidation as an INVALIDATE, then report the value event as degraded.
   */
  private void degradeToInvalidate(
    Batch.Value batch,
    SyncEvent event,
    String key,
    long version,
    SyncSkipReason reason
  ) {
    EvictOutcome outcome = evict(key, version, batch.allowSoftExpire());
    if (outcome.stale()) {
      fireOnSkipped(event, SyncSkipReason.VERSION_STALE);
      return;
    }
    fireAfterValueDegraded(event, reason);
  }

  /**
   * Applies the highest covered INVALIDATE when the winning value message never wrote a
   * value. The covered instruction was already reported as superseded, so this fallback is
   * the removal its version still demands — not a second terminal callback.
   *
   * @param batch the value batch whose winner was refused or failed
   * @param key   the affected key
   */
  private void applyCoveredInvalidateFallback(Batch.Value batch, String key) {
    SyncMessage covered = batch.coveredInvalidate();
    if (covered != null) {
      // The covered removal applies after the winner was refused; if its peer stamp was
      // over-drift it still needs a persistable local version.
      evict(key, resolveEffectiveVersion(covered.version()), false);
    }
  }

  /**
   * The guarded peer-value write. Inside the atomic compute: the invalidation tombstones
   * (a concurrent INVALIDATE or clear while the value was resolved refuses the write), the
   * refresh guard (only a strictly newer resident skips; an equal version applies), then a
   * value+version rewrite that keeps the resident TTLs and Worker decision metadata — or a
   * fresh NORMAL entry, because only a Worker decision may create a HOT one.
   *
   * @param key        the affected key
   * @param version    the incoming data stamp
   * @param value      the resolved value
   * @param refuseReason receives the guard that refused the write, when one did
   * @return the images the bin lock observed; {@link Applied#changed()} is
   *         {@code false} when the guards refused the value
   */
  @SuppressWarnings("all")
  private Applied writePeerValue(String key, long version, Object value, SyncSkipReason[] refuseReason) {
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
      long hardTtl = resident != null ? resident.getHardTtlMs() : ttlPolicy.effectiveHardTtlMs();
      long softTtl = resident != null ? resident.getSoftTtlMs() : ttlPolicy.effectiveSoftTtlMs();
      return entryLifecycle.writeValue(resident, value, version, hardTtl, softTtl, false);
    });
    if (applied.changed()) {
      invalidateDedupEntry(key);
    }
    return applied;
  }

  /**
   * The shared versioned eviction. The tombstone is recorded in the same compute as the
   * removal, so a concurrent refresh cannot slip between them and re-warm the key. The
   * key's dedup flight is dropped immediately after the tombstone — including an absent
   * key — so a read that starts after the invalidation cannot join a pre-invalidation load;
   * a version guard skip leaves the newer flight alone.
   *
   * <p>With {@code softExpireWorkerManaged}, a live Worker-managed (HOT/COOL) entry survives:
   * it keeps its decision stamp and TTL regime and only its soft window is closed — the next
   * read reloads through the application read path. A logically expired entry is removed
   * instead of being rewritten with a closed window.
   *
   * @return whether a newer resident refused the eviction; when {@code false}, the tombstone
   *         was recorded and any resident the batch targeted is gone
   */
  @SuppressWarnings("all")
  private EvictOutcome evict(String key, long version, boolean softExpireWorkerManaged) {
    boolean[] stale = { false };
    cache
      .asMap()
      .compute(key, (k, existing) -> {
        CacheEntry resident = liveResident(existing);
        if (resident != null && VersionGuard.shouldSkipForRefresh(resident, version)) {
          stale[0] = true;
          return existing;
        }

        entryLifecycle.recordInvalidation(key, version);
        invalidateDedupEntry(key);
        if (existing == null) {
          return null;
        }
        if (resident != null && softExpireWorkerManaged && existing.isWorkerManaged()) {
          if (ttlPolicy.isSoftExpired(existing)) {
            return existing;
          }
          // One millisecond in the past on the wall-clock axis TtlPolicy.isSoftExpired reads, so
          // a read in this same millisecond already sees the closed soft window.
          return entryLifecycle.editEntry(existing).softExpiryAt(TimeSource.currentTimeMillis() - 1L).build();
        }
        return null;
      });
    return new EvictOutcome(stale[0]);
  }

  /** One eviction outcome: whether a newer resident refused the eviction. */
  private record EvictOutcome(boolean stale) {}

  /**
   * Reduces one granted clear batch to its effective watermarks. A prefix clear at or below
   * the table-wide stamp is dropped: the table-wide stamp already covers every key it could
   * target.
   *
   * <p>Marks are resolved in ascending peer order so replacement stamps preserve peer order;
   * otherwise an older full clear could outrank a newer prefix clear.
   */
  private ClearMarks collectMarks(List<SyncMessage> batch) {
    long rawFull = 0L;
    Map<String, Long> rawPrefixes = new HashMap<>(2);
    for (SyncMessage message : batch) {
      switch (message.type()) {
        case INVALIDATE_ALL -> rawFull = Math.max(rawFull, message.version());
        case INVALIDATE_PREFIX -> rawPrefixes.merge(message.cacheKey(), message.version(), Math::max);
        default -> {
        }
      }
    }

    TreeSet<Long> ordered = new TreeSet<>(rawPrefixes.values());
    if (rawFull > 0L) {
      ordered.add(rawFull);
    }
    Map<Long, Long> effective = new HashMap<>(ordered.size());
    for (Long stamp : ordered) {
      effective.put(stamp, resolveEffectiveVersion(stamp));
    }

    long fullStamp = rawFull > 0L ? effective.get(rawFull) : 0L;
    Map<String, Long> prefixStamps = new HashMap<>(rawPrefixes.size());
    rawPrefixes.forEach((prefix, stamp) -> prefixStamps.put(prefix, effective.get(stamp)));
    if (fullStamp > 0L) {
      prefixStamps.values().removeIf(prefixStamp -> prefixStamp <= fullStamp);
    }
    return new ClearMarks(fullStamp, Map.copyOf(prefixStamps));
  }

  /**
   * The version a peer mutation is persisted at: the peer stamp when the anti-drift guard
   * accepts it, otherwise a fresh local stamp allocated at apply time (design decisions:
   * version stamps).
   *
   * @param peerVersion the decoded message version; always positive
   * @return the peer version when accepted, otherwise a fresh local stamp
   */
  private long resolveEffectiveVersion(long peerVersion) {
    if (stampClock == null || stampClock.observe(peerVersion)) {
      return peerVersion;
    }
    return stampClock.nextStamp();
  }

  /**
   * Schedules one coalesced sweep for the marks. The consumer thread only records the
   * watermark and the request; the table walk runs on the configured executor. When the
   * executor refuses (shutdown or saturation), the sweep runs inline so the clear still
   * reaches the resident entries.
   */
  private void requestSweep(ClearMarks marks) {
    pendingSweep.updateAndGet(current -> current == null ? marks : current.merge(marks));
    if (sweepScheduled.compareAndSet(false, true)) {
      submitSweep();
    }
  }

  /** Submits the drain, falling back to an inline sweep when the executor refuses. */
  private void submitSweep() {
    try {
      sweepExecutor.execute(this::drainSweep);
    } catch (RejectedExecutionException ignored) {
      drainSweep();
    }
  }

  /** Drains every pending mark, then re-arms when a clear landed during the sweep. */
  private void drainSweep() {
    try {
      for (;;) {
        ClearMarks marks = pendingSweep.getAndSet(null);
        if (marks == null) {
          return;
        }
        int removed = sweep(marks.fullStamp(), marks.prefixStamps());
        if (removed > 0) {
          logSweep("swept", marks, removed);
        }
      }
    } finally {
      sweepScheduled.set(false);
      // A clear that landed after the last getAndSet but before the flag reset found the
      // flag set and did not schedule; re-arm here so it is not stranded.
      if (pendingSweep.get() != null && sweepScheduled.compareAndSet(false, true)) {
        submitSweep();
      }
    }
  }

  /** One clear-sweep DEBUG line, shared by the dispatcher apply and the deferred drain. */
  private void logSweep(String label, ClearMarks marks, int removed) {
    log.debug(
      "Clear {}: fullStamp={}, prefixes={}, removed={}",
      label,
      marks.fullStamp(),
      marks.prefixStamps().keySet(),
      removed
    );
  }

  /**
   * The effective watermarks of one clear batch: the table-wide stamp plus the scoped
   * prefix stamps that outrank it. Immutable; merged by taking the highest stamp per scope.
   */
  private record ClearMarks(long fullStamp, Map<String, Long> prefixStamps) {
    boolean isEmpty() {
      return fullStamp == 0L && prefixStamps.isEmpty();
    }

    ClearMarks merge(ClearMarks other) {
      long mergedFull = Math.max(fullStamp, other.fullStamp);
      Map<String, Long> mergedPrefixes = new HashMap<>(prefixStamps);
      other.prefixStamps.forEach((prefix, stamp) -> mergedPrefixes.merge(prefix, stamp, Math::max));
      if (mergedFull > 0L) {
        mergedPrefixes.values().removeIf(stamp -> stamp <= mergedFull);
      }
      return new ClearMarks(mergedFull, Map.copyOf(mergedPrefixes));
    }
  }

  /** Records the batch's clear watermarks before the sweep touches a single entry. */
  private void recordWatermarks(long fullStamp, Map<String, Long> prefixStamps) {
    if (fullStamp > 0L) {
      entryLifecycle.recordClear(fullStamp);
    }
    if (!prefixStamps.isEmpty()) {
      entryLifecycle.recordPrefixClears(prefixStamps);
    }
  }

  /**
   * Kills pre-clear flights too: the watermark refuses their fills, but a read that starts
   * after the clear must not join one and receive the pre-clear value. A table-wide kill
   * already covers every prefix, so the per-prefix walk only runs without one.
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
   * Removes every resident a clear of the batch targets, each at the highest stamp that reaches
   * it. The count is committed per key, not per compute attempt: the remapping function can run
   * again when the bin is contended.
   *
   * @return the number of entries the sweep removed
   */
  @SuppressWarnings("all")
  private int sweep(long fullStamp, Map<String, Long> prefixStamps) {
    int removed = 0;
    boolean[] didRemove = new boolean[1];
    for (Map.Entry<String, CacheEntry> entry : cache.asMap().entrySet()) {
      String key = entry.getKey();
      long stamp = stampFor(key, fullStamp, prefixStamps);
      if (stamp <= 0L) {
        continue;
      }
      // Lock-free pre-filter on the value the entry set already carries: a live resident
      // stamped above the clear is preserved by the compute, so its bin lock is skipped.
      // A logically expired resident is absent and is removed instead of vetoing the clear.
      CacheEntry resident = liveResident(entry.getValue());
      if (resident != null && VersionGuard.shouldSkipForRefresh(resident, stamp)) {
        continue;
      }
      // No per-key dedup kill here (design decisions: invalidation): the clear family already
      // marked every pre-clear flight above, and the watermark refuses any pre-clear fill.
      EntryMutations.mutate(cache, key, existing -> {
        if (
          existing == null || (liveResident(existing) != null && VersionGuard.shouldSkipForRefresh(existing, stamp))
        ) {
          didRemove[0] = false;
          return existing;
        }
        didRemove[0] = true;
        return null;
      });
      if (didRemove[0]) {
        removed++;
      }
    }
    return removed;
  }

  /** The stamp a key must be removed at, or {@code 0} when no clear of the batch targets it. */
  private static long stampFor(String key, long fullStamp, Map<String, Long> prefixStamps) {
    long stamp = fullStamp;
    for (Map.Entry<String, Long> entry : prefixStamps.entrySet()) {
      if (entry.getValue() > stamp && key.startsWith(entry.getKey())) {
        stamp = entry.getValue();
      }
    }
    return stamp;
  }

  /** The entry the read path would serve, or {@code null} when it is absent or logically expired. */
  @Nullable
  private CacheEntry liveResident(@Nullable CacheEntry entry) {
    return entry != null && !ttlPolicy.isLogicallyExpired(entry) ? entry : null;
  }

  /** The policy/hook snapshot of a key, or {@code null} when no entry is resident. */
  @Nullable
  private EntryView view(String key) {
    return DecisionPlaneSupport.view(cache, key, ttlPolicy);
  }

  /** Loads through the optional cluster loader, keeping the declined-vs-missing reason. */
  private LoadResult load(String key) {
    return DecisionPlaneSupport.load(clusterLoader, key, log, "refresh: load failed for key={}");
  }

  /**
   * The cluster loader's admission check, with the loader SPI treated as fallible: an exception
   * is a refusal, so the event still takes the versioned invalidation path and its one terminal
   * hook instead of escaping the applier.
   *
   * @param key   the affected key (for the WARN line)
   * @param value the resolved value
   * @return {@code true} when no loader is configured or the loader admits the value
   */
  private boolean admitsValue(String key, Object value) {
    if (clusterLoader == null) {
      return true;
    }
    try {
      return clusterLoader.isCacheable(value);
    } catch (Exception e) {
      log.warn("ClusterLoader.isCacheable failed for key={}, degrading to INVALIDATE", key, e);
      return false;
    }
  }

  /** Drops the applied key's dedup entry: a pre-apply load result must not be replayed. */
  private void invalidateDedupEntry(String key) {
    if (singleFlight != null) {
      singleFlight.invalidate(key);
    }
  }

  private void fireAfterValueApplied(SyncEvent event, @Nullable CacheEntry entry) {
    if (hooks.isEmpty() || entry == null) {
      return;
    }
    EntryView view = EntryView.of(entry, ttlPolicy, TimeSource.currentTimeMillis());
    fireHooks("afterValueApplied", eventKey(event), hook -> hook.afterValueApplied(event, view));
  }

  private void fireAfterInvalidate(SyncEvent event) {
    if (hooks.isEmpty()) {
      return;
    }
    fireHooks("afterInvalidate", eventKey(event), hook -> hook.afterInvalidate(event));
  }

  private void fireAfterValueDegraded(SyncEvent event, SyncSkipReason reason) {
    if (hooks.isEmpty()) {
      return;
    }
    fireHooks("afterValueDegraded", eventKey(event), hook -> hook.afterValueDegraded(event, reason));
  }

  private void fireAfterClear(List<SyncEvent> events, int affectedEntries) {
    if (hooks.isEmpty()) {
      return;
    }
    List<SyncEvent> clears = List.copyOf(events);
    fireHooks("afterClear", "clear", hook -> hook.afterClear(clears, affectedEntries));
  }

  private void fireOnSkipped(SyncEvent event, SyncSkipReason reason) {
    if (hooks.isEmpty()) {
      return;
    }
    fireHooks("onSkipped", eventKey(event), hook -> hook.onSkipped(event, reason));
  }

  /**
   * One owner of the hook-dispatch contract: a broken hook never breaks the decision
   * apply. The empty check runs before the capturing callback is built.
   *
   * @param hookName the callback name (for the WARN line)
   * @param key      the key the event touched, or {@code "clear"} for the clear family
   * @param callback the hook callback to invoke for each registered hook
   */
  private void fireHooks(String hookName, String key, Consumer<SyncHook> callback) {
    DecisionPlaneSupport.fireHooks(hooks, "SyncHook", hookName, key, callback, log);
  }

  /** The key used in the hook-failure log line; the clear family has no single key. */
  private static String eventKey(SyncEvent event) {
    return event instanceof SyncEvent.KeyEvent keyEvent ? keyEvent.cacheKey() : "clear";
  }
}
