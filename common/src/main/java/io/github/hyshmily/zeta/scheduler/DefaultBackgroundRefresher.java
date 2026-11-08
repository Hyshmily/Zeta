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

import static io.github.hyshmily.zeta.constants.ZetaConstants.Version.VERSION_DEFAULT;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.TtlPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.cache.cachesupport.raw.VersionedValue;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.InterruptingAsync;
import io.github.hyshmily.zeta.util.TimeSource;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * Default {@link BackgroundRefresher}: soft-expire refresh scheduling with
 * per-key dedup, a global refresh limiter, timeout protection, a merge guarded
 * by both the version snapshot and the invalidation tombstones, and
 * lease-on-failure degradation (design decisions: expiry and refresh).
 *
 * <p>The entry lifecycle (factory, guards, demotion) and the background refresh
 * executor each own their own state behind their own seam.
 */
@Slf4j
@Internal
public class DefaultBackgroundRefresher implements BackgroundRefresher {

  /** Budget of one background refresh before its reader thread is interrupted, in seconds. */
  private static final long REFRESH_TIMEOUT_SECONDS = 30;

  /** Lease-on-failure (design decisions: expiry and refresh): minimum lease duration in milliseconds — the decay floor. */
  private static final long LEASE_MIN_TTL_MS = 120_000;

  /** Upper bound of the {@code staleDebt} episode view; oldest entries are never evicted. */
  private static final int MAX_LEASED_EPISODES = 1024;

  private final Cache<String, CacheEntry> cache;
  private final EntryLifecycle entryLifecycle;
  private final Semaphore limiter;
  private final TtlPolicy ttlPolicy;
  private final Executor executor;

  /**
   * Per-key dedup for background refreshes. A ticket exists only while its
   * refresh holds a limiter permit, so the map is bounded by
   * {@code refreshMaxPools}. The ticket is both the reservation token and the
   * completion callback: its identity makes the completion-time removal
   * ABA-safe, and it carries the creation-time snapshot without a second object.
   */
  private final ConcurrentHashMap<String, RefreshTicket> refreshes = new ConcurrentHashMap<>();

  /** Cumulative leases granted; source of the {@code zeta.expire.refresh.failure.leased.total} counter. */
  private final AtomicLong leaseGrants = new AtomicLong();

  /** Failure classifier gating the lease (never {@code null}). */
  private final LeaseClassifier classifier;

  /**
   * Currently-leased keys for the {@code staleDebt} diagnostic view:
   * key to first-lease wall time. Insertion-ordered so iteration is
   * oldest-first without sorting; at {@link #MAX_LEASED_EPISODES} new keys are
   * refused rather than evicting the oldest debt. Every access is guarded by
   * this map's monitor; {@link #episodeCount} is the lock-free empty
   * probe that keeps the common no-lease success path off that monitor.
   */
  @GuardedBy("leasedEpisodes")
  private final Map<String, Long> leasedEpisodes = new LinkedHashMap<>(64, 0.75f, false);

  /** Live episode count, maintained under {@code leasedEpisodes}' monitor. */
  private volatile int episodeCount;

  /**
   * Decides whether a failed background refresh may keep its stale entry
   * servable. The default leases every failure except JVM-level {@code Error}s
   * (fatal, never a reason to serve older data longer); applications with
   * definitive "not found" error types supply their own predicate to also
   * refuse those. Evaluated against the peeled cause (one
   * {@code CompletionException} layer removed).
   */
  @FunctionalInterface
  public interface LeaseClassifier extends Predicate<Throwable> {
    /**
     * Default: lease everything but {@code Error}.
     *
     * @return a classifier that refuses only {@code Error} causes
     */
    static LeaseClassifier defaults() {
      return cause -> !(cause instanceof Error);
    }
  }

  /** Outcome of the lease decision made inside one refresh's compute block. */
  private enum LeaseOutcome {
    NONE,
    GRANTED,
    CAPPED,
    CLOSED,
    EXPIRED,
  }

  /**
   * One background refresh, from reservation to settlement: the instance is the dedup token in
   * {@link #refreshes}, the creation-time snapshot and the {@code whenComplete} callback.
   *
   * <p>The snapshot fields are written once by {@link #captureSnapshot} before
   * submission and read by the completion path. {@link #leaseOutcome} and
   * {@link #leasedAtMs} are the settlement written inside the failure compute;
   * a dedicated outcome keeps the snapshot fields immutable for the whole call.
   */
  private final class RefreshTicket implements BiConsumer<Object, Throwable> {

    private final String key;
    private final long softTtlMs;
    private long dataVersion;
    private long hardExpireAtMs;
    private long hardTtlMs;
    private long leasedAtMs;
    private KeyState keyState;
    private @Nullable DecisionStamp decision;
    private LeaseOutcome leaseOutcome = LeaseOutcome.NONE;

    private RefreshTicket(String key, long softTtlMs) {
      this.key = key;
      this.softTtlMs = softTtlMs;
    }

    private void captureSnapshot(CacheEntry raw) {
      if (raw == null) {
        dataVersion = VERSION_DEFAULT;
        decision = null;
        keyState = KeyState.NORMAL;
        hardExpireAtMs = Long.MAX_VALUE;
        hardTtlMs = 0L;
        return;
      }
      dataVersion = raw.getDataVersion();
      decision = raw.decision();
      keyState = raw.getKeyState();
      hardExpireAtMs = raw.getHardExpireAtMs();
      hardTtlMs = raw.getHardTtlMs();
    }

    /**
     * Whether {@code raw} still carries the identity this ticket snapshotted:
     * same data stamp, same hard deadline, and not permanent. A lease applies to
     * its own snapshot only — a write, broadcast or promotion that landed in
     * flight already installed a newer value or regime, and a lease must not
     * extend that different entry under the old refresh.
     */
    private boolean sameEntry(@Nullable CacheEntry raw) {
      return (
        raw != null &&
        raw.getDataVersion() == dataVersion &&
        raw.getHardExpireAtMs() == hardExpireAtMs &&
        hardExpireAtMs != Long.MAX_VALUE
      );
    }

    @Override
    public void accept(Object value, Throwable failure) {
      try {
        if (failure != null) {
          // Peel one CompletionException layer: supplier failures arrive
          // wrapped (supplyAsync encodeThrowable semantics) while orTimeout
          // timeouts arrive bare — the classifier must see the real cause.
          Throwable cause = firstCause(failure);
          if (cause instanceof TimeoutException) {
            log.warn("Background soft refresh timed out after {}s: {}", REFRESH_TIMEOUT_SECONDS, key);
          } else {
            log.warn("Background soft refresh failed: {}", key, failure);
          }

          // design decisions: expiry and refresh: keep the stale entry servable instead of letting it run
          // into the hard TTL and stampede the failing source on every read.
          leaseOnFailure(this, cause);
          return;
        }

        if (value instanceof VersionedValue loaded && loaded.value() != null) {
          applyRefreshTask(loaded, this);
          // Healed: the fresh value ends the lease episode.
          removeLeaseEpisode(key);
        } else {
          // A clean refresh that produced no value leaves a soft-expired, hard-valid entry:
          // without closing the retry window every later read re-arms the refresh and the
          // source is hammered for the rest of the hard TTL. Keep the value servable, but
          // stop re-arming it; the next explicit write or hard expiry is the recovery point.
          closeRetryWindowAfterEmptyRefresh(this);
        }
      } finally {
        // Always remove the ticket and release the limiter permit so a future
        // refresh can be scheduled. Conditional removal by object identity
        // against the RESERVATION token this task owns: a concurrent trigger
        // that installed a newer ticket is left untouched (ABA).
        refreshes.remove(key, this);
        limiter.release();
      }
    }
  }

  /**
   * Creates a refresher over the given Caffeine cache, executor, entry
   * lifecycle and refresh pool bound.
   *
   * @param caffeineCache   the underlying L1 Caffeine cache
   * @param executor        async executor for background refresh
   * @param entryLifecycle  the entry lifecycle (TTL policy and tombstones)
   * @param refreshMaxPools maximum concurrent background refreshes; must be positive and
   *                        should stay below the executor's pool size (see
   *                        {@code zeta.local.refresh-max-pools})
   */
  public DefaultBackgroundRefresher(
    Cache<String, CacheEntry> caffeineCache,
    Executor executor,
    EntryLifecycle entryLifecycle,
    int refreshMaxPools
  ) {
    this(caffeineCache, executor, entryLifecycle, refreshMaxPools, LeaseClassifier.defaults());
  }

  /**
   * Creates a refresher with an explicit lease classifier (see
   * {@link LeaseClassifier}): applications with definitive "not found" error
   * types refuse the lease for those and let the entry hard-expire instead of
   * serving known-stale data.
   *
   * @param caffeineCache   the underlying L1 Caffeine cache
   * @param executor        async executor for background refresh
   * @param entryLifecycle  the entry lifecycle (TTL policy and tombstones)
   * @param refreshMaxPools maximum concurrent background refreshes; must be positive and
   *                        should stay below the executor's pool size (see
   *                        {@code zeta.local.refresh-max-pools})
   * @param leaseClassifier the failure classifier gating the lease; {@code null} selects
   *                        {@link LeaseClassifier#defaults()}
   */
  public DefaultBackgroundRefresher(
    Cache<String, CacheEntry> caffeineCache,
    Executor executor,
    EntryLifecycle entryLifecycle,
    int refreshMaxPools,
    LeaseClassifier leaseClassifier
  ) {
    this.executor = executor;
    this.cache = caffeineCache;
    this.entryLifecycle = entryLifecycle;
    this.ttlPolicy = entryLifecycle.ttlPolicy();
    if (refreshMaxPools <= 0) {
      // A zero-permit semaphore would block every refresh forever. The
      // configuration layer derives the bound from the executor; a direct
      // caller must pass a real one.
      throw new IllegalArgumentException("refreshMaxPools must be positive, but got: " + refreshMaxPools);
    }
    this.limiter = new Semaphore(refreshMaxPools);
    this.classifier = leaseClassifier != null ? leaseClassifier : LeaseClassifier.defaults();
  }

  /**
   * Exposes the refresh limiter semaphore for monitoring purposes — the only
   * instance state intentionally surfaced beyond the seam's counters.
   */
  @Override
  public Semaphore getRefreshLimiter() {
    return limiter;
  }

  /** Cumulative leases granted (see {@link BackgroundRefresher#getRefreshFailureLeased()}). */
  @Override
  public long getRefreshFailureLeased() {
    return leaseGrants.get();
  }

  /**
   * O(1) count of currently tracked lease episodes for gauges. May include keys
   * whose L1 value already left (they are lazily reaped by the next
   * {@link #leasedStaleness(int)} read).
   */
  @Override
  public int leasedKeyCount() {
    return episodeCount;
  }

  /**
   * Oldest currently-leased keys, oldest-first, capped at {@code limit} — the
   * {@code staleDebt} diagnostic view. A key leaves the view when a refresh
   * applies a fresh value (healed), a refusal closes/expires its lease, or its
   * entry leaves L1.
   */
  @Override
  public Map<String, Long> leasedStaleness(int limit) {
    if (limit <= 0) {
      return Map.of();
    }
    long now = TimeSource.currentTimeMillis();
    List<Map.Entry<String, Long>> episodes;
    synchronized (leasedEpisodes) {
      episodes = new ArrayList<>(leasedEpisodes.entrySet());
    }
    Map<String, Long> view = new LinkedHashMap<>();
    for (Map.Entry<String, Long> episode : episodes) {
      if (view.size() >= limit) {
        break;
      }
      String key = episode.getKey();
      long leasedAt = episode.getValue();
      // asMap().containsKey is the quiet probe: unlike getIfPresent it neither
      // records a hit/miss nor promotes the key in the access-order queue, so a
      // diagnostics scrape cannot make a leased entry look hotter than it is.
      if (!cache.asMap().containsKey(key)) {
        // Conditional removal: a concurrent failure may already have recorded a
        // new lease episode for the same key after this snapshot was taken.
        removeLeaseEpisode(key, leasedAt);
        continue;
      }
      view.put(key, Math.max(0L, now - leasedAt));
    }
    return Collections.unmodifiableMap(view);
  }

  /**
   * Triggers an asynchronous background refresh for the given cache key. The
   * caller (typically the soft-expire read path) has already returned the
   * stale value to the client, so this method executes entirely in the
   * background without blocking the caller.
   *
   * <p><b>Concurrency:</b> the read-side dedup is a single lock-free
   * {@link ConcurrentHashMap#containsKey} — a soft-expired read for a key whose
   * refresh is already in flight serves the stale value and returns. Only a
   * read that finds no ticket enters the reservation path: acquire a limiter
   * permit, install the ticket with {@code putIfAbsent} (losing a race releases
   * the permit immediately), then create the task <b>outside</b> the map.
   * Creating it inside a map update would let a synchronous executor run the
   * ticket's completion callback (and its same-key map removal) inside the
   * mapping function, an unsupported recursive update that
   * {@code ConcurrentHashMap} answers with {@code IllegalStateException}. The
   * ticket is identity-removed by its own completion callback, so the deduped
   * key is always free again once its refresh settles.
   *
   * @param cacheKey  the key whose value should be refreshed
   * @param reader    the data-source supplier; must return a
   *                  {@link VersionedValue} with a
   *                  non-null value — anything else is treated as "no new value"
   *                  and leaves the entry untouched (the production read path
   *                  wraps the reader result with a version probe, design decisions: version stamps)
   * @param softTtlMs the soft TTL to set on the refreshed entry (milliseconds)
   */
  @Override
  @SuppressWarnings("java:S1181")
  public void triggerBackgroundRefresh(String cacheKey, Supplier<?> reader, long softTtlMs) {
    // Lock-free dedup fast path: an in-flight refresh for this key means every
    // later read just serves the stale value. A plain containsKey keeps the
    // read path off the CHM bin lock. Presence, not a separate flag, is the
    // state: a completion that is still settling leaves the ticket in place, so
    // this read correctly dedups instead of racing a reservation it would lose.
    if (refreshes.containsKey(cacheKey)) {
      return;
    }

    // Try to acquire a permit from the global refresh limiter.
    if (!limiter.tryAcquire()) {
      log.debug("Refresh limiter blocked, skip background refresh: {}", cacheKey);
      return;
    }

    // Reserve the key atomically; the task is created outside the map (see
    // the method Javadoc for why the reservation must not submit it).
    // The ticket is its own identity token: the completion removes by
    // reference, so a ticket installed by a later reservation can never be
    // removed by an earlier task.
    RefreshTicket ticket = new RefreshTicket(cacheKey, softTtlMs);
    if (refreshes.putIfAbsent(cacheKey, ticket) != null) {
      // Another trigger owns the key, or its just-completed ticket is still
      // waiting for identity removal. Either way this attempt adds nothing:
      // release the permit and let the next soft-expired read retry.
      limiter.release();
      return;
    }

    CompletableFuture<?> task;
    try {
      task = createRefreshTask(ticket, reader);
    } catch (Throwable t) {
      // Unexpected failure before task creation (snapshot lookup, supplyAsync
      // Error, etc.) — release the semaphore so the refresh limiter does not
      // permanently lose a slot, and drop the reservation so the next read can
      // retry.
      log.warn("Unexpected error during background refresh scheduling: {}", cacheKey, t);
      limiter.release();
      refreshes.remove(cacheKey, ticket);
      throw t;
    }
    if (task == null) {
      // Executor rejected (createRefreshTask already released the permit):
      // leave no pending ticket so the next read can retry immediately.
      refreshes.remove(cacheKey, ticket);
    }
  }

  /**
   * Creates a background async refresh task for the given ticket, with
   * soft-TTL semantics, version-guarded merge, and rate-limited concurrency.
   *
   * <p><b>Flow:</b>
   * <ol>
   *   <li>Capture the current entry's metadata on the ticket
   *       ({@code dataVersion}, {@link DecisionStamp}, {@code keyState}, hard
   *       expiry) at call time, so {@link #applyRefreshTask} can detect
   *       superseding writes and preserve Worker decision state across the
   *       refresh boundary.</li>
   *   <li>Submit the supplier to a bounded executor; if the executor rejects,
   *       release the limiter permit immediately and return null so the next
   *       read can retry without waiting.</li>
   *   <li>Register the ticket as the completion callback. It identity-removes
   *       itself from {@code refreshes}, releases the permit, and either
   *       applies the value or leases the failed entry.</li>
   * </ol>
   *
   * <p><b>Version guard:</b> Inside {@code applyRefreshTask}, the stale
   * refresh result is discarded (keeping the existing entry) whenever the
   * current {@code dataVersion} is strictly newer than the snapshot taken at
   * creation time.
   *
   * <p><b>Limiter:</b> Controlled by the {@code limiter} semaphore
   * acquired in the calling method. One permit is held until the future
   * completes (success, error, timeout) and is released by the ticket.
   *
   * @param ticket the reservation, snapshot carrier and completion callback
   * @param reader the async value supplier (executed on the bounded pool)
   * @return the {@link CompletableFuture} representing the refresh, or
   *         {@code null} if the executor rejected the task
   */
  private CompletableFuture<?> createRefreshTask(RefreshTicket ticket, Supplier<?> reader) {
    // asMap().get is stats-quiet: a background refresh must not record a cache
    // hit/miss, or every soft-expired read would count twice (the read path's own
    // hit plus this snapshot) and inflate the L1 hit rate. It still feeds the
    // eviction policy's read buffer, which is harmless for a key already being
    // refreshed.
    ticket.captureSnapshot(cache.asMap().get(ticket.key));

    // Build the async refresh task with timeout protection. Unlike a bare
    // orTimeout, a timeout also interrupts the reader thread (via
    // {@link InterruptingAsync}) — otherwise a hung data source would pin the
    // shared refresh executor until the reader finished on its own.
    CompletableFuture<?> task;
    try {
      task = InterruptingAsync.supplyAsync(reader, executor, REFRESH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (RejectedExecutionException e) {
      // Executor saturated – release the limiter permit and leave no
      // pending ticket so the next read can retry immediately. The pool's own
      // rejection handler already reported the refusal.
      limiter.release();
      log.debug("Background refresh rejected by executor (saturated), key={}", ticket.key);
      return null;
    }

    task.whenComplete(ticket);
    return task;
  }

  /**
   * Unwrap one {@code CompletionException}/{@code ExecutionException} layer so
   * the classifier and the timeout log see the real cause: supplier failures
   * arrive wrapped (supplyAsync encodeThrowable semantics) while orTimeout
   * timeouts arrive bare. Mirrors {@code SingleFlightImpl}'s join-side
   * {@code getCause()} handling.
   *
   * @param error the raw {@code whenComplete} error (never {@code null} here)
   * @return the peeled cause, or the error itself when it carries no cause
   */
  private static Throwable firstCause(Throwable error) {
    if ((error instanceof CompletionException || error instanceof ExecutionException) && error.getCause() != null) {
      return error.getCause();
    }
    return error;
  }

  /**
   * Lease-on-Failure (design decisions: expiry and refresh): keep a stale entry servable after its
   * background refresh failed, by extending its expire timestamps.
   *
   * <p><b>Semantics:</b> the lease is a <i>provisional keep-alive</i>, not an
   * authoritative state transition — the value, all metadata (dataVersion,
   * decision stamps, keyState) and the {@code hardTtlMs}/{@code softTtlMs}
   * duration fields are preserved verbatim; only the expire timestamps move.
   * The hard timestamp extends to {@code now + max(remaining, floor)} — never
   * shorter than the remaining budget (a failure must not punish a healthy
   * long TTL), decaying to the floor on repeated failure, so repeated failure
   * is graceful degradation rather than an entry clearing. The floor follows the entry's own scale
   * ({@code min(120s, hardTtlMs)}): a single lease never exceeds one declared
   * lifetime. The soft timestamp extends to the lease midpoint
   * ({@code now + lease/2}): the entry is soft-expired during the second half
   * of every lease while still hard-valid, which is the read-triggered retry
   * window — the next read re-arms the refresh. {@code soft == hard} would
   * close the window: the read path checks hard expiry before soft expiry, so
   * the entry would die at lease end without ever retrying.
   *
   * <p><b>Refusals:</b> the classifier may refuse definitive/fatal failures; such
   * an entry is hard-expired immediately (both deadlines moved into the past)
   * instead of serving known-stale data. Entries older than the staleness cap
   * ({@code birthStampSec}, default 30 min), and entries whose declared hard TTL
   * is below twice the refresh timeout, cannot justify a retry: their soft
   * deadline moves to the existing hard deadline, so they stay servable until
   * that bound but no read re-arms the failing refresh in between. Every grant is
   * counted, and the {@code staleDebt} view tracks the oldest up to its cap, so
   * "bounded staleness" is verifiable, not a belief.
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
   * @param ticket the refresh whose entry may be leased
   * @param cause  the peeled refresh failure (one wrapper layer removed)
   */
  private void leaseOnFailure(RefreshTicket ticket, Throwable cause) {
    boolean mayLease = classifier.test(cause);
    if (!mayLease) {
      log.debug(
        "Lease-on-failure: classifier refused lease for key={} (cause {}), hard-expiring the entry",
        ticket.key,
        cause.toString()
      );
    }

    // Decided under the bin lock, published after it: the ticket's outcome
    // records the grant or refusal. The diagnostic map (and its monitor) is
    // updated outside the compute so the Caffeine bin lock is never held while
    // waiting for it — a diagnostics scrape copying the map must not stall L1
    // writers on this bin.
    cache
      .asMap()
      .compute(ticket.key, (key, entry) -> {
        if (!ticket.sameEntry(entry)) {
          return entry;
        }
        // A peer invalidation may have closed the soft window or removed the entry while this
        // refresh was in flight. The lease must not re-open a retry window an explicit
        // invalidation just condemned.
        if (entryLifecycle.staleAgainstInvalidation(ticket.key, ticket.dataVersion)) {
          ticket.leaseOutcome = LeaseOutcome.NONE;
          return entry;
        }

        long now = TimeSource.currentTimeMillis();
        if (!mayLease) {
          // Both deadlines move into the past; the read path removes the entry on its next
          // pass, so an in-flight reader is never raced by an explicit removal.
          ticket.leaseOutcome = LeaseOutcome.EXPIRED;
          return EntryDraft.of(entry, ttlPolicy).expiryAt(now - 1L, now - 1L).build();
        }

        // A refused lease must not leave the entry soft-expired while hard-valid:
        // every read would re-arm the refresh and hammer a failing source for the
        // rest of the hard window. Close the retry window at the hard deadline and
        // let the entry expire naturally (the 30-minute cap's documented response).
        if (entry.isLeaseStaleCapped(TimeSource.monotonicSecondsSinceBoot())) {
          ticket.leaseOutcome = LeaseOutcome.CAPPED;
          log.debug("Lease-on-failure: staleness cap refused lease for key={}, closing retry window", ticket.key);
          return closeRetryWindow(entry);
        }

        // No retry window fits below twice the refresh timeout: same response —
        // keep the value until its original hard deadline, but do not retry.
        long scaleMs = Math.max(1L, entry.getHardTtlMs());
        if (scaleMs < 2 * TimeUnit.SECONDS.toMillis(REFRESH_TIMEOUT_SECONDS)) {
          ticket.leaseOutcome = LeaseOutcome.CLOSED;
          log.debug("Lease-on-failure: retry window cannot fit, closing it for key={} until hard expiry", ticket.key);
          return closeRetryWindow(entry);
        }

        long remainingMs = Math.max(0L, entry.getHardExpireAtMs() - now);
        long floorMs = Math.min(LEASE_MIN_TTL_MS, scaleMs);
        // Never shorter than the remaining budget: a failure never shortens a
        // healthy budget; decay toward the floor happens through wall-time
        // passage, not halving.
        long leaseTtlMs = Math.max(remainingMs, floorMs);
        long leaseExpireAtMs = now + leaseTtlMs;
        // The soft timestamp sits at the midpoint of the lease: the entry spends
        // the second half of every lease soft-expired while still hard-valid, so
        // the next read re-arms the refresh (retry window). soft == hard would
        // close the window entirely — the read path checks hard expiry before
        // soft expiry, so the entry would die at lease end without ever retrying.
        // NB: the halving applies to the lease DURATION only — shifting the full
        // epoch timestamp (now + lease) would land it decades in the past.
        long leaseSoftExpireAtMs = now + (leaseTtlMs >> 1);
        log.debug(
          "Lease-on-failure: extending stale entry for key={} by {}ms (floor {}ms)",
          ticket.key,
          leaseTtlMs,
          floorMs
        );

        ticket.leaseOutcome = LeaseOutcome.GRANTED;
        ticket.leasedAtMs = now;
        // Provisional keep-alive: durations, value, and all metadata preserved
        // verbatim; only the two expire timestamps move (explicit lease
        // arithmetic — the draft's computed expiry must not run here).
        return EntryDraft.of(entry, ttlPolicy).expiryAt(leaseExpireAtMs, leaseSoftExpireAtMs).build();
      });

    switch (ticket.leaseOutcome) {
      case GRANTED -> {
        leaseGrants.incrementAndGet();
        recordLeaseEpisode(ticket.key, ticket.leasedAtMs);
      }
      case CAPPED, CLOSED, EXPIRED -> removeLeaseEpisode(ticket.key);
      case NONE -> {
        // The entry changed or vanished while the refresh was in flight: no lease.
      }
    }
  }

  /**
   * Move the soft deadline up to the existing hard deadline: the entry stays servable
   * until its already-scheduled hard expiry, but a read no longer re-arms the failing
   * refresh in between.
   */
  private CacheEntry closeRetryWindow(CacheEntry entry) {
    return EntryDraft.of(entry, ttlPolicy).softExpiryAt(entry.getHardExpireAtMs()).build();
  }

  /**
   * Close the retry window after a successful refresh returned no value. The identity guard is
   * the ticket's snapshot (data stamp and hard deadline, permanent included): a concurrent write
   * or promotion already granted fresh TTLs and must keep them. A matched close also ends the
   * key's lease episode, because the source has now answered cleanly.
   *
   * @param ticket the refresh whose empty result settles
   */
  private void closeRetryWindowAfterEmptyRefresh(RefreshTicket ticket) {
    boolean[] closed = { false };
    cache
      .asMap()
      .compute(ticket.key, (key, entry) -> {
        if (
          entry == null ||
          entry.getDataVersion() != ticket.dataVersion ||
          entry.getHardExpireAtMs() != ticket.hardExpireAtMs
        ) {
          return entry;
        }
        closed[0] = true;
        return closeRetryWindow(entry);
      });
    if (closed[0]) {
      removeLeaseEpisode(ticket.key);
      log.debug("Background soft refresh returned no value; closed the retry window for key={}", ticket.key);
    }
  }

  /**
   * Admit a lease episode while preserving the diagnostic view's oldest-first meaning:
   * once the cap is reached, new keys are refused rather than evicting the oldest (and
   * therefore most stale) debt. The check, insert and count move as one critical
   * section under the map's monitor.
   */
  private void recordLeaseEpisode(String key, long leasedAt) {
    synchronized (leasedEpisodes) {
      if (leasedEpisodes.size() >= MAX_LEASED_EPISODES && !leasedEpisodes.containsKey(key)) {
        return;
      }
      if (leasedEpisodes.putIfAbsent(key, leasedAt) == null) {
        episodeCount++;
      }
    }
  }

  /** Removes a key's episode unconditionally; the lock-free count keeps the common path out of the monitor. */
  private void removeLeaseEpisode(String key) {
    if (episodeCount == 0) {
      return;
    }
    synchronized (leasedEpisodes) {
      if (leasedEpisodes.remove(key) != null) {
        episodeCount--;
      }
    }
  }

  /** Conditional removal for the staleDebt reap: never drops a newer episode for the same key. */
  private void removeLeaseEpisode(String key, long leasedAt) {
    if (episodeCount == 0) {
      return;
    }
    synchronized (leasedEpisodes) {
      if (leasedEpisodes.remove(key, leasedAt)) {
        episodeCount--;
      }
    }
  }

  /**
   * Atomically applies a soft-refresh result to the cache, guarded by
   * a data-stamp check to prevent stale overwrites.
   *
   * <p><b>Logic:</b>
   * <ol>
   *   <li>Extract version metadata and keyState from the snapshot taken at
   *       refresh-creation time.</li>
   *   <li>Call Caffeine {@code compute} on the map:
   *       <ul>
   *         <li><b>Snapshot guard (design decisions: version stamps):</b> the result carries the stamp
   *             taken before the value read. Acceptance is decided against the
   *             creation-time <i>snapshot</i>, not the incoming stamp: the
   *             refresh applies unless a newer write landed in flight (entry
   *             stamp strictly above the snapshot). A stamp comparison is wrong
   *             in both directions — equal-skip discards every ordinary refresh
   *             (entry == incoming with no concurrent write, so SWR would never
   *             extend TTLs), while equal-apply re-applies a stale value over a
   *             concurrent writer's fresh entry when the stamp was taken after
   *             the write.</li>
   *         <li><b>Entry exists, version not superseded:</b> replace the entry
   *             value, keep the higher of the entry's and the incoming stamp,
   *             and apply the soft TTL.</li>
   *         <li><b>Entry absent:</b> create a fresh entry preserving the
   *             snapshot's decision stamp and key state (see
   *             {@link #rebuildEvictedEntry}) and the snapshot's own hard TTL
   *             (so a per-call TTL override survives eviction+refresh instead
   *             of reverting to the configured default), stamped with the
   *             incoming stamp, and the given soft TTL.</li>
   *       </ul>
   *   </li>
   * </ol>
   *
   * <p>Values are stored as passed by the caller (no codec).
   * This method is only called from {@link #createRefreshTask}'s
   * {@code whenComplete} callback on the async-thread pool, so {@code compute}
   * ensures safe atomic visibility under concurrent reads and writes.
   *
   * @param loaded the freshly-loaded value paired with the pre-read
   *               {@code dataVersion} stamp (design decisions: version stamps)
   * @param ticket the refresh whose snapshot and soft TTL apply
   */
  private void applyRefreshTask(VersionedValue loaded, RefreshTicket ticket) {
    // Values are stored as passed (no codec): the drafts below take the raw value directly.
    Object value = loaded.value();
    String key = ticket.key;

    cache
      .asMap()
      .compute(key, (k, entry) -> {
        // Anti-resurrection: the refresh read its value before this compute, so
        // an INVALIDATE / clearAll that landed in between outranks it. The
        // entry may be absent for two very different reasons — capacity
        // eviction (rebuild is correct) or an explicit removal (rebuild would
        // revive the pre-invalidation value) — and only the tombstone tells
        // them apart. Returning the resident (possibly null) keeps both
        // outcomes: a newer entry survives, an absent one stays absent.
        if (entryLifecycle.staleAgainstInvalidation(key, loaded.dataVersion())) {
          log.debug("Async refresh discarded: key was invalidated during the refresh: {}", key);
          return entry;
        }

        if (entry != null) {
          // Snapshot guard (not incoming-stamp guard): discard only when a
          // newer write landed while the refresh was in flight. Comparing the
          // entry against the INCOMING stamp is wrong in both directions —
          // with no concurrent write entry == incoming, so an equal-skip
          // (shouldSkipForSync) discards every ordinary refresh and SWR never
          // extends TTLs; with equal-apply (shouldSkipForRefresh) a stamp taken
          // after a concurrent write re-applies the stale value over the
          // writer's fresh entry. The snapshot taken at creation time is the
          // correct baseline in both cases.
          if (entry.getDataVersion() > ticket.dataVersion) {
            log.debug("Async refresh discarded: newer version exists: {}", key);
            return entry;
          }

          // Keep L1's stamp monotonic: the entry's own stamp wins ties, while
          // the fresh value and extended TTLs still apply.
          return refreshedEntry(entry, value, Math.max(entry.getDataVersion(), loaded.dataVersion()), ticket.softTtlMs);
        }
        return rebuildEvictedEntry(loaded, ticket, value);
      });
  }

  /**
   * Merge a successful refresh result into an existing entry — the rebuild
   * tail of {@link #applyRefreshTask}'s compute: fresh value, the
   * given data version, and the refresh soft TTL. A successful refresh of a
   * COOL entry downgrades it to NORMAL: the local read that triggered the
   * refresh shows the key is still active locally, so it returns to the
   * ordinary local lifecycle (promotion-eligible, hard-TTL reload). Decision
   * metadata is preserved, so a later Worker broadcast still overrides via
   * {@code decisionVersion}. NORMAL/HOT entries keep state.
   *
   * @param entry       the existing entry to merge into (not modified)
   * @param value       the refreshed value (stored as-is, no codec)
   * @param dataVersion  the data stamp to store
   * @param softTtlMs    the soft TTL for the refreshed entry
   * @return the merged entry
   */
  private CacheEntry refreshedEntry(CacheEntry entry, Object value, long dataVersion, long softTtlMs) {
    // Fresh value ⇒ reborn: the birth stamp resets so the lease staleness cap
    // measures the current value's age, not the superseded entry's.
    EntryDraft refreshed = EntryDraft.of(entry, ttlPolicy)
      .value(value)
      .version(dataVersion)
      .softTtl(softTtlMs)
      .birthStampSec(TimeSource.monotonicSecondsSinceBoot());
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
   * <p>The rebuild uses the ticket's OWN hard TTL so a per-call TTL override
   * (stamped on the entry at load time) survives eviction+refresh instead of
   * silently reverting to the configured default; a ticket captured with no
   * entry present ({@code hardTtlMs = 0}) falls back to the configured default.
   * The snapshot's decision stamp is carried forward and a successful refresh
   * rebuilds a COOL entry as NORMAL (same downgrade as the existing-entry
   * branch).
   */
  private CacheEntry rebuildEvictedEntry(VersionedValue loaded, RefreshTicket ticket, Object value) {
    long hardTtlMs = ttlPolicy.resolveEffectiveHardTtl(ticket.hardTtlMs);
    // design decisions: version stamps: the rebuilt entry carries the pre-read stamp taken before the
    // refresh load began.
    EntryDraft rebuilt = EntryDraft.blank(ttlPolicy)
      .value(value)
      .version(loaded.dataVersion())
      .ttl(
        hardTtlMs,
        ticket.softTtlMs,
        ttlPolicy.normalHardBaseline(ttlPolicy.effectiveHardTtlMs()),
        ttlPolicy.effectiveSoftTtlMs()
      )
      .keyState(ticket.keyState == KeyState.COOL ? KeyState.NORMAL : ticket.keyState)
      .birthStampSec(TimeSource.monotonicSecondsSinceBoot());
    if (ticket.decision != null) {
      rebuilt.decision(ticket.decision);
    }

    return rebuilt.build();
  }
}
