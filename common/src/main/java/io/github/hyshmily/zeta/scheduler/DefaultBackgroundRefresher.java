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
package io.github.hyshmily.zeta.scheduler;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.cache.cachesupport.TtlPolicy;
import io.github.hyshmily.zeta.cache.codec.CacheCompressor;
import io.github.hyshmily.zeta.model.*;
import io.github.hyshmily.zeta.util.InterruptingAsync;
import io.github.hyshmily.zeta.util.TimeSource;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.*;
import java.util.function.Supplier;

import static io.github.hyshmily.zeta.constants.ZetaConstants.Version.VERSION_DEFAULT;

/**
 * Default {@link BackgroundRefresher}: soft-expire refresh scheduling with
 * per-key dedup, a global refresh limiter, timeout protection, version-guarded
 * merge, and lease-on-failure degradation (ADR-0036).
 *
 * <p>Extracted from the former {@code ExpireManagerImpl} so the entry
 * lifecycle (factory, guards, demotion) and the background refresh executor
 * each own their own state behind their own seam.
 */
@Getter
@Slf4j
@Internal
public class DefaultBackgroundRefresher implements BackgroundRefresher {

  /** The underlying L1 Caffeine cache instance. */
  private final Cache<String, CacheEntry> caffeineCache;
  /** Async executor for background refresh tasks. */
  private final Executor executor;
  /** Pure TTL/expiry policy — shared with the entry lifecycle. */
  private final TtlPolicy ttlPolicy;
  /** Compressor for L1 cache values. */
  private final CacheCompressor compressor;
  /** Semaphore limiting concurrent background refresh operations. */
  private final Semaphore refreshLimiter;
  /** Per-key dedup for background refreshes — prevents concurrent refresh for the same key. */
  private final ConcurrentHashMap<String, CompletableFuture<?>> pendingRefreshes = new ConcurrentHashMap<>();

  @SuppressWarnings("all")
  private static final long REFRESH_TIMEOUT_SECONDS = 30;

  /** Lease-on-failure (ADR-0036): TTL halving divisor applied to the remaining budget. */
  @SuppressWarnings("all")
  private static final long LEASE_DIVISOR = 1;

  /** Lease-on-failure (ADR-0036): minimum lease duration in milliseconds — the decay floor. */
  @SuppressWarnings("all")
  private static final long LEASE_MIN_TTL_MS = 120_000;

  /**
   * Immutable snapshot of an entry's decision-relevant metadata, taken at
   * refresh-creation time and re-judged at completion time. The Worker
   * decision travels as one {@link DecisionStamp} ({@code null} = local
   * origin) so the merge paths pass it through or drop it as a unit.
   */
  @SuppressWarnings("all")
  private record SnapshotEntry(
    long dataVersion,
    @Nullable DecisionStamp decision,
    KeyState keyState,
    long hardExpireAtMs,
    long hardTtlMs
  ) {
    static final SnapshotEntry DEFAULT = new SnapshotEntry(VERSION_DEFAULT, null, KeyState.NORMAL, Long.MAX_VALUE, 0L);
  }

  private static SnapshotEntry snapshotEntry(CacheEntry raw) {
    if (raw != null) {
      return new SnapshotEntry(
        raw.getDataVersion(),
        raw.decisionStamp(),
        raw.getKeyState(),
        raw.getHardExpireAtMs(),
        raw.getHardTtlMs()
      );
    }
    return SnapshotEntry.DEFAULT;
  }

  /**
   * Creates a refresher over the given Caffeine cache, executor, TTL policy,
   * compressor, and refresh pool bound.
   *
   * @param caffeineCache   the underlying L1 Caffeine cache
   * @param executor        async executor for background refresh
   * @param ttlPolicy       the pure TTL policy shared with the entry lifecycle
   * @param compressor      compressor for L1 cache values
   * @param refreshMaxPools maximum concurrent background refreshes (capped at 100)
   */
  public DefaultBackgroundRefresher(
    Cache<String, CacheEntry> caffeineCache,
    Executor executor,
    TtlPolicy ttlPolicy,
    CacheCompressor compressor,
    int refreshMaxPools
  ) {
    this.caffeineCache = caffeineCache;
    this.executor = executor;
    this.ttlPolicy = ttlPolicy;
    this.compressor = compressor;
    this.refreshLimiter = new Semaphore(refreshMaxPools > 0 ? refreshMaxPools : 100);
  }

  /**
   * Triggers an asynchronous background refresh for the given cache key. The
   * caller (typically the soft-expire read path) has already returned the
   * stale value to the client, so this method executes entirely in the
   * background without blocking the caller.
   *
   * <p><b>Concurrency:</b> a two-phase reservation. The {@link
   * ConcurrentHashMap#compute} on {@code pendingRefreshes} atomically decides
   * whether a refresh should start (deduped against an in-flight marker, gated by
   * the limiter) and stores an uncompleted <i>reservation marker</i>; the actual
   * task is then created <b>outside</b> the map. Creating it inside the compute —
   * as earlier designs did — lets a synchronous executor run the task's
   * completion callback (and its same-key map removal) inside the mapping
   * function, which is an unsupported recursive update that
   * {@code ConcurrentHashMap} answers with {@code IllegalStateException}. The
   * marker is identity-removed by the task's completion callback, so the
   * deduped key is always free again once its refresh settles.
   *
   * @param cacheKey  the key whose value should be refreshed
   * @param reader    the data-source supplier
   * @param softTtlMs the soft TTL to set on the refreshed entry (milliseconds)
   */
  @Override
  @SuppressWarnings("java:S1181")
  public void triggerBackgroundRefresh(String cacheKey, Supplier<?> reader, long softTtlMs) {
    boolean[] reserved = new boolean[1];
    CompletableFuture<?> marker = pendingRefreshes.compute(cacheKey, (k, existing) -> {
      // If there is already an in-flight refresh for this key, keep the
      // existing marker and do nothing.
      if (existing != null && !existing.isDone()) {
        return existing;
      }

      // Try to acquire a permit from the global refresh limiter.
      if (!refreshLimiter.tryAcquire()) {
        log.debug("Refresh limiter blocked, skip background refresh: {}", cacheKey);
        // Returning null removes any previous entry, leaving no stale marker.
        return null;
      }

      // Reserve the key atomically; the task is created outside the map (see
      // the method Javadoc for why the compute must not submit it).
      reserved[0] = true;
      return new CompletableFuture<>();
    });

    if (marker == null || !reserved[0]) {
      return; // limiter blocked, or a refresh is already in flight
    }

    CompletableFuture<?> task;
    try {
      task = createRefreshTask(cacheKey, reader, softTtlMs, marker);
    } catch (Throwable t) {
      // Unexpected failure before task creation (getIfPresent NPE, supplyAsync Error,
      // etc.) — release the semaphore so the refresh limiter does not permanently
      // lose a slot, and drop the reservation so the next read can retry.
      log.warn("Unexpected error during background refresh scheduling: {}", cacheKey, t);
      refreshLimiter.release();
      pendingRefreshes.remove(cacheKey, marker);
      throw t;
    }
    if (task == null) {
      // Executor rejected (createRefreshTask already released the permit):
      // leave no pending marker so the next read can retry immediately.
      pendingRefreshes.remove(cacheKey, marker);
    }
  }

  /**
   * Creates a background async refresh task for the given cache key, with
   * soft-TTL semantics, version-guarded merge, and rate-limited concurrency.
   *
   * <p><b>Flow:</b>
   * <ol>
   *   <li>Snapshot the current entry's metadata (dataVersion, decisionVersion,
   *       decisionNodeId, decisionEpoch, keyState) at call time, so that
   *       {@link #applyRefreshTask} can detect superseding writes and
   *       preserve Worker decision state across the refresh boundary.</li>
   *   <li>Submit the supplier to a bounded executor; if the executor rejects,
   *       release the limiter permit immediately and return null so the next
   *       read can retry without waiting.</li>
   *   <li>On completion (success or failure): release the limiter permit,
   *       remove the in-flight marker from {@code pendingRefreshes}, and
   *       call {@link #applyRefreshTask} only when the value is non-null and
   *       no error occurred. On failure, {@link #leaseOnFailure} extends the
   *       existing entry's expire timestamps instead (ADR-0036) — the stale
   *       value stays servable and the next soft-expiry read re-arms the
   *       refresh.</li>
   * </ol>
   *
   * <p><b>Version guard:</b> Inside {@code applyRefreshTask}, the stale
   * refresh result is discarded (keeping the existing entry) whenever the
   * current {@code dataVersion} is strictly newer than the snapshot taken at
   * creation time.
   *
   * <p><b>Limiter:</b> Controlled by the {@code refreshLimiter} semaphore
   * acquired in the calling method. One permit is held until the future
   * completes (success, error, timeout) and is released inside the
   * {@code whenComplete} callback.
   *
   * @param cacheKey  the key being refreshed
   * @param reader    the async value supplier (executed on the bounded pool)
   * @param softTtlMs soft-TTL in milliseconds applied to the resulting entry
   * @param marker    the reservation marker stored in {@code pendingRefreshes} by
   *                  {@link #triggerBackgroundRefresh}; identity-removed by the
   *                  completion callback. The callback always runs outside any
   *                  {@code pendingRefreshes} compute — the marker is created
   *                  inside the compute but the task is wired outside it — so a
   *                  synchronous executor cannot trigger a recursive update.
   * @return the {@link CompletableFuture} representing the refresh, or
   *         {@code null} if the executor rejected the task
   */
  private CompletableFuture<?> createRefreshTask(
    String cacheKey,
    Supplier<?> reader,
    long softTtlMs,
    CompletableFuture<?> marker
  ) {
    // Snapshot the current entry metadata so we can detect superseding
    // writes and preserve Worker decision state across the refresh.
    SnapshotEntry snap = snapshotEntry(caffeineCache.getIfPresent(cacheKey));

    // Build the async refresh task with timeout protection. Unlike a bare
    // orTimeout, a timeout also interrupts the reader thread (via
    // {@link InterruptingAsync}) — otherwise a hung data source would pin the
    // shared refresh executor until the reader finished on its own.
    CompletableFuture<?> task;
    try {
      task = InterruptingAsync.supplyAsync(reader, executor, REFRESH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (RejectedExecutionException e) {
      // Executor saturated – release the limiter permit and leave no
      // pending marker so the next read can retry immediately.
      refreshLimiter.release();
      log.warn("Background refresh rejected by executor (saturated), key={}", cacheKey);
      return null;
    }

    task.whenComplete((value, error) -> {
      try {
        if (error != null) {
          if (error instanceof TimeoutException) {
            log.warn("Background soft refresh timed out after {}s: {}", REFRESH_TIMEOUT_SECONDS, cacheKey);
          } else {
            log.warn("Background soft refresh failed: {}", cacheKey, error);
          }
          // ADR-0036: keep the stale entry servable instead of letting it run
          // into the hard TTL and stampede the failing source on every read.
          leaseOnFailure(cacheKey, snap);
          return;
        }
        if (value instanceof VersionedValue vv && vv.value() != null) {
          applyRefreshTask(cacheKey, vv, softTtlMs, snap);
        }
      } finally {
        // Always release the limiter permit and remove the in-flight
        // marker so that a future refresh can be scheduled. Conditional
        // removal by object identity against the RESERVATION marker this
        // task owns: a concurrent trigger that replaced the marker with a
        // newer one is left untouched (ABA).
        refreshLimiter.release();
        pendingRefreshes.remove(cacheKey, marker);
      }
    });

    return task;
  }

  /**
   * Lease-on-Failure (ADR-0036): keep a stale entry servable after its
   * background refresh failed, by extending its expire timestamps.
   *
   * <p><b>Semantics:</b> the lease is a <i>provisional keep-alive</i>, not an
   * authoritative state transition — the value, all metadata (dataVersion,
   * decision stamps, keyState) and the {@code hardTtlMs}/{@code softTtlMs}
   * duration fields are preserved verbatim; only the expire timestamps move.
   * The hard timestamp extends to {@code now + max(remaining/2, 120s)} — the
   * halved remaining budget gives soft exponential decay to the 120s floor,
   * so repeated failure is graceful degradation rather than an entry clearing.
   * The soft timestamp extends to the lease midpoint ({@code now + lease/2}):
   * the entry is soft-expired during the second half of every lease while
   * still hard-valid, which is the read-triggered retry window — the next
   * read re-arms the refresh. {@code soft == hard} would close the window:
   * the read path checks hard expiry before soft expiry, so the entry would
   * die at lease end without ever retrying.
   *
   * <p><b>Identity guard:</b> mirrors {@link #applyRefreshTask}'s version
   * guard — the lease applies only to the same logical entry that failed the
   * refresh ({@code dataVersion} and {@code hardExpireAtMs} both equal to the
   * creation-time snapshot). A write, broadcast, or promotion that landed
   * in-flight already granted fresh TTLs; leasing it would shorten them. An
   * absent (evicted) entry is never recreated — the failure path carries no
   * new value. Permanent entries ({@code hardExpireAtMs == Long.MAX_VALUE})
   * are never leased: there is nothing to extend and the halving arithmetic
   * would overflow.
   *
   * @param cacheKey the key whose entry to lease
   * @param snap     the entry metadata snapshot taken at refresh-creation time
   */
  private void leaseOnFailure(String cacheKey, SnapshotEntry snap) {
    caffeineCache
      .asMap()
      .compute(cacheKey, (key, existing) -> {
        if (
          existing == null ||
          existing.getDataVersion() != snap.dataVersion() ||
          existing.getHardExpireAtMs() != snap.hardExpireAtMs() ||
          existing.getHardExpireAtMs() == Long.MAX_VALUE
        ) {
          return existing;
        }

        long now = TimeSource.currentTimeMillis();
        long remainingMs = existing.getHardExpireAtMs() - now;
        long leaseTtlMs = Math.max(LEASE_MIN_TTL_MS, Math.max(1, remainingMs) >> LEASE_DIVISOR);
        long leaseExpireAtMs = now + leaseTtlMs;
        // The soft timestamp sits at the midpoint of the lease: the entry spends
        // the second half of every lease soft-expired while still hard-valid, so
        // the next read re-arms the refresh (retry window). soft == hard would
        // close the window entirely — the read path checks hard expiry before
        // soft expiry, so the entry would die at lease end without ever retrying.
        // NB: the halving applies to the lease DURATION only — shifting the full
        // epoch timestamp (now + lease) would land it decades in the past.
        long leaseSoftExpireAtMs = now + (leaseTtlMs >> LEASE_DIVISOR);
        log.debug(
          "Lease-on-failure: extending stale entry for key={} by {}ms (floor {}ms)",
          cacheKey,
          leaseTtlMs,
          LEASE_MIN_TTL_MS
        );
        // Provisional keep-alive: durations, value, and all metadata preserved
        // verbatim; only the two expire timestamps move (explicit lease
        // arithmetic — the draft's computed expiry must not run here).
        return EntryDraft.of(existing, ttlPolicy).expiryAt(leaseExpireAtMs, leaseSoftExpireAtMs).build();
      });
  }

  /**
   * Atomically applies a soft-refresh result to the cache, guarded by
   * a data-version check to prevent stale overwrites.
   *
   * <p><b>Logic:</b>
   * <ol>
   *   <li>Extract version metadata and keyState from the snapshot taken at
   *       refresh-creation time.</li>
   *   <li>Call Caffeine {@code compute} on the map:
   *       <ul>
    *       <li><b>Stamped refresh (ADR-0033):</b> the result carries the
    *             {@code dataVersion} probed from Redis after the value read.
    *             Acceptance is decided against the creation-time
    *             <i>snapshot</i>, not the probe: the refresh applies unless
    *             a newer write landed in flight (entry version strictly above
    *             the snapshot, or the degraded boundary was crossed). A probe
    *             comparison is wrong in both directions — equal-skip discards
    *             every ordinary refresh (entry == probe with no concurrent
    *             write, so SWR would never extend TTLs), while equal-apply
    *             re-applies a stale value over a concurrent writer's fresh
    *             entry when the probe was taken after the write. Same-version
    *             apply also heals: a snapshot-degraded entry with no in-flight
    *             write is overwritten by the normal probe (case 4 of the sync
    *             matrix), so a recovered Redis heals degraded entries.</li>
   *         <li><b>Unstamped refresh (fail-open):</b> the probe was withheld.
   *             Legacy L1-internal guards apply: discard if the entry is
   *             degraded, or if its current {@code dataVersion} exceeds the
   *             snapshot value (a newer write arrived while the refresh was
   *             in-flight).</li>
   *         <li><b>Entry exists, version not superseded:</b> replace the
   *             entry value, stamp the probed version (when present), and
   *             apply the soft TTL.</li>
   *         <li><b>Entry absent:</b> create a fresh entry preserving the
   *             snapshot's decision stamp and key state (see
   *             {@link #rebuildEvictedEntry}) and the snapshot's own hard TTL
   *             (so a per-call TTL override survives eviction+refresh instead
   *             of reverting to the configured default), stamped with the
   *             probed version when present, and the given soft TTL.</li>
   *       </ul>
   *   </li>
   * </ol>
   *
   * <p>The value is wrapped/compressed <i>before</i> entering the
   * {@code compute} callback: compression cost is linear in the value size
   * (ADR-0015 caps scratch residency at 1 MiB), and holding the Caffeine bin
   * lock for it would stall every same-bin read/write for the duration. The
   * price is one wasted compression when a version guard discards the result.
   * This method is only called from {@link #createRefreshTask}'s
   * {@code whenComplete} callback on the async-thread pool, so {@code compute}
   * ensures safe atomic visibility under concurrent reads and writes.
   *
   * @param cacheKey  the key whose value to update
   * @param vv        the freshly-loaded value paired with the probed
   *                  {@code dataVersion} (ADR-0033); unstamped when the probe
   *                  failed
   * @param softTtlMs soft-TTL in milliseconds applied to the resulting entry
   * @param snap      the entry metadata snapshot taken at refresh-creation time
   */
  @SuppressWarnings("all")
  private void applyRefreshTask(String cacheKey, VersionedValue vv, long softTtlMs, SnapshotEntry snap) {
    // Compress outside the bin lock — see the method Javadoc. The drafts below
    // take the value in stored form (already wrapped; re-wrapping an LZ4
    // byte[] would corrupt the stored format).
    final Object wrappedValue = compressor.wrap(vv.value());

    caffeineCache
      .asMap()
      .compute(cacheKey, (key, existingEntry) -> {
        if (existingEntry != null) {
          CacheEntry entry = existingEntry;
          if (vv.stamped()) {
            // Snapshot guard (not probe guard): discard only when a newer
            // write landed while the refresh was in flight. Comparing the
            // entry against the PROBE version is wrong in both directions —
            // with no concurrent write entry == probe, so an equal-skip
            // (shouldSkipForSync) discards every ordinary refresh and SWR
            // never extends TTLs; with equal-apply (shouldSkipForRefresh) a
            // probe taken after a concurrent write re-applies the stale value
            // over the writer's fresh entry. The snapshot taken at creation
            // time is the correct baseline in both cases.
            // Degraded states are derived from the version sign (negative =
            // degraded, the CacheEntry invariant): crossing the boundary in
            // flight means a write landed after the snapshot, so it wins.
            boolean snapDegraded = snap.dataVersion() < 0;
            if (entry.isVersionDegraded() != snapDegraded) {
              log.debug("Async refresh discarded: degraded boundary crossed in flight for key={}", cacheKey);
              return entry;
            }
            if (entry.getDataVersion() > snap.dataVersion()) {
              log.debug("Async refresh discarded: newer version exists: {}", cacheKey);
              return entry;
            }
            // Same-version apply also heals: a snapshot-degraded entry with
            // no in-flight write is overwritten by the normal probe (case 4
            // of the sync matrix), so a recovered Redis heals degraded
            // entries through the refresh path. The stamp keeps version
            // monotonicity: a probe older than the entry (version-key
            // wraparound, ADR-0022 — the floor cache normally prevents this
            // from ever reaching here) must not regress L1's watermark, so
            // the entry's own version wins ties against the past while the
            // fresh value and extended TTLs still apply.
            return refreshedEntry(entry, wrappedValue, Math.max(entry.getDataVersion(), vv.dataVersion()), softTtlMs);
          }
          // Degraded version: a Redis-outage write occurred during refresh.
          // Discard the refresh result unconditionally — it is older than
          // the degraded write and must not supersede it.
          if (entry.isVersionDegraded()) {
            log.debug("Async refresh discarded: degraded version write superseded, key={}", cacheKey);
            return entry;
          }
          // Version guard: if a newer write has
          // arrived while we were refreshing,
          // discard the stale refresh result.
          if (entry.getDataVersion() > snap.dataVersion()) {
            log.debug("Async refresh discarded: newer version exists: {}", cacheKey);
            return entry;
          }
          // Unstamped (fail-open) merge: preserve the entry's own version
          // identity — no probe authority to stamp with.
          return refreshedEntry(entry, wrappedValue, entry.getDataVersion(), softTtlMs);
        }
        return rebuildEvictedEntry(vv, softTtlMs, snap, wrappedValue);
      });
  }

  /**
   * Merge a successful refresh result into an existing entry — the shared tail
   * of both merge branches in {@link #applyRefreshTask}: fresh value, the
   * given data version, and the refresh soft TTL. A successful refresh of a
   * COOL entry downgrades it to NORMAL: the local read that triggered the
   * refresh shows the key is still active locally, so it returns to the
   * ordinary local lifecycle (promotion-eligible, hard-TTL reload). Decision
   * metadata is preserved, so a later Worker broadcast still overrides via
   * {@code decisionVersion}. NORMAL/HOT entries keep state.
   *
   * @param entry        the existing entry to merge into (not modified)
   * @param wrappedValue the refreshed value in stored (already wrapped) form
   * @param dataVersion  the data version to stamp (ADR-0033); the entry's own
   *                     version for an unstamped (fail-open) merge
   * @param softTtlMs    the soft TTL for the refreshed entry
   * @return the merged entry
   */
  private CacheEntry refreshedEntry(CacheEntry entry, Object wrappedValue, long dataVersion, long softTtlMs) {
    EntryDraft refreshed = EntryDraft.of(entry, ttlPolicy).value(wrappedValue).version(dataVersion).softTtl(softTtlMs);
    if (entry.getKeyState() == KeyState.COOL) {
      refreshed.keyState(KeyState.NORMAL);
    }
    return refreshed.build();
  }

  /**
   * Rebuild an entry after the snapshot it was snapshotted from was evicted
   * while the refresh was in flight — the absent-entry branch of
   * {@link #applyRefreshTask}.
   *
   * <p>The rebuild uses the snapshot's OWN hard TTL so a per-call TTL override
   * (stamped on the entry at load time) survives eviction+refresh instead of
   * silently reverting to the configured default; a snapshot taken with no
   * entry present ({@code DEFAULT}, {@code hardTtlMs = 0}) falls back to the
   * configured default. The snapshot's decision stamp is carried forward and a
   * successful refresh rebuilds a COOL entry as NORMAL (same downgrade as the
   * existing-entry branch).
   */
  private CacheEntry rebuildEvictedEntry(VersionedValue vv, long softTtlMs, SnapshotEntry snap, Object wrappedValue) {
    long effectiveHardTtl = ttlPolicy.resolveEffectiveHardTtl(snap.hardTtlMs());
    // ADR-0033: stamp the probed version when present; fail-open
    // rebuilds with VERSION_DEFAULT.
    long stampedVersion = vv.stamped() ? vv.dataVersion() : VERSION_DEFAULT;
    EntryDraft rebuilt = EntryDraft.blank(ttlPolicy)
      .value(wrappedValue)
      .version(stampedVersion)
      .ttl(effectiveHardTtl, softTtlMs, ttlPolicy.getEffectiveHardTtlMs(), ttlPolicy.getEffectiveSoftTtlMs())
      .keyState(snap.keyState() == KeyState.COOL ? KeyState.NORMAL : snap.keyState());
    if (snap.decision() != null) {
      rebuilt.decision(snap.decision());
    }

    return rebuilt.build();
  }
}
