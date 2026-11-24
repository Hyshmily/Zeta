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
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.TtlPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.cache.cachesupport.raw.QuietCache;
import io.github.hyshmily.zeta.cache.cachesupport.raw.VersionedValue;
import io.github.hyshmily.zeta.exception.ZetaExceptionHandler;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.InterruptingAsync;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * Default {@link BackgroundRefresher} coordinating per-key soft refreshes and hard renewals behind
 * a single concurrency limiter and deduplication table.
 *
 * <p><b>Soft refresh vs. hard renewal.</b> A soft refresh replaces the value and re-arms only the
 * soft window while keeping the caller's hard deadline intact. A hard renewal replaces the value
 * and re-arms both soft and hard deadlines from the entry's stored durations. Neither operation
 * resurrects an evicted or invalidated entry.
 *
 * <p><b>In-flight soft-to-hard upgrade.</b> A hard renewal arriving while a soft refresh is active
 * upgrades the ticket atomically via {@link #TICKET_STATE} ({@code SOFT -> UPGRADED_HARD}) and
 * shares its load; if soft settlement already claimed {@code SOFT -> SETTLING_SOFT}, the renewal
 * shares the ticket's completion future and observes {@link RenewOutcome.Skipped#SOFT_REFRESH} once
 * the write-back lands. Soft-only tickets allocate no {@link CompletableFuture}.
 */
@Slf4j
@Internal
public class DefaultBackgroundRefresher implements BackgroundRefresher {

  private static final int FLAG_HARD = 1;
  private static final int FLAG_SETTLING = 1 << 1;
  private static final int FLAG_FROM_SOFT = 1 << 2;

  private static final int STATE_HARD = FLAG_HARD;
  private static final int STATE_SOFT = FLAG_FROM_SOFT;
  private static final int STATE_UPGRADED_HARD = FLAG_FROM_SOFT | FLAG_HARD;
  private static final int STATE_SETTLING_SOFT = FLAG_FROM_SOFT | FLAG_SETTLING;

  private static final CompletableFuture<RenewOutcome> LIMITER_SKIPPED = CompletableFuture.completedFuture(
    RenewOutcome.Skipped.LIMITER
  );
  private static final CompletableFuture<RenewOutcome> ABSENT_SKIPPED = CompletableFuture.completedFuture(
    RenewOutcome.Skipped.ABSENT_OR_EXPIRED
  );

  private static final VarHandle TICKET_STATE;
  private static final VarHandle TICKET_COMPLETION;

  static {
    try {
      MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(RefreshTicket.class, MethodHandles.lookup());
      TICKET_STATE = lookup.findVarHandle(RefreshTicket.class, "state", int.class);
      TICKET_COMPLETION = lookup.findVarHandle(RefreshTicket.class, "completion", Object.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private final Cache<String, CacheEntry> cache;
  private final EntryLifecycle entryLifecycle;
  private final TtlPolicy ttlPolicy;
  private final Executor executor;
  private final Semaphore limiter;
  private final long refreshTimeoutMs;
  private final ConcurrentHashMap<String, RefreshTicket> refreshes = new ConcurrentHashMap<>();

  /**
   * Creates a background refresher bounded by {@code refreshMaxPools} concurrent loads.
   *
   * @param caffeineCache    the L1 Caffeine cache
   * @param executor         the interruptible executor running background loads
   * @param entryLifecycle   entry draft factory and invalidation guard manager
   * @param refreshMaxPools  maximum concurrent refreshes allowed across all keys; must be positive
   * @param refreshTimeoutMs per-refresh execution timeout in milliseconds; must be positive
   * @throws IllegalArgumentException if {@code refreshMaxPools} or {@code refreshTimeoutMs} is non-positive
   * @throws NullPointerException     if any required collaborator is {@code null}
   */
  public DefaultBackgroundRefresher(
    Cache<String, CacheEntry> caffeineCache,
    Executor executor,
    EntryLifecycle entryLifecycle,
    int refreshMaxPools,
    long refreshTimeoutMs
  ) {
    this.cache = Objects.requireNonNull(caffeineCache, "caffeineCache must not be null");
    this.executor = Objects.requireNonNull(executor, "executor must not be null");
    this.entryLifecycle = Objects.requireNonNull(entryLifecycle, "entryLifecycle must not be null");
    if (refreshMaxPools <= 0) {
      throw new IllegalArgumentException("refreshMaxPools must be positive, but got: " + refreshMaxPools);
    }
    if (refreshTimeoutMs <= 0L) {
      throw new IllegalArgumentException("refreshTimeoutMs must be positive, but got: " + refreshTimeoutMs);
    }
    this.ttlPolicy = entryLifecycle.ttlPolicy();
    this.limiter = new Semaphore(refreshMaxPools);
    this.refreshTimeoutMs = refreshTimeoutMs;
  }

  /**
   * Returns the semaphore bounding concurrent background refresh executions.
   *
   * @return the shared refresh concurrency limiter
   */
  @Override
  public Semaphore getRefreshLimiter() {
    return limiter;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Deduplicates against any in-flight refresh for {@code cacheKey} and skips silently when the
   * entry is absent/expired or the concurrency limiter is saturated.
   *
   * @param cacheKey  the physical cache key to refresh
   * @param reader    the value loader producing a stamped {@link VersionedValue}
   * @param softTtlMs the resolved soft TTL in milliseconds for the refreshed entry
   */
  @Override
  public void triggerBackgroundRefresh(String cacheKey, Supplier<VersionedValue> reader, long softTtlMs) {
    if (refreshes.containsKey(cacheKey)) {
      return;
    }
    submitSoft(cacheKey, softTtlMs, reader);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Upgrades an existing in-flight soft ticket in place when present; otherwise reserves and
   * submits a new hard-renewal ticket.
   *
   * @param cacheKey the physical cache key to renew
   * @param reader   the value loader producing a stamped {@link VersionedValue}
   * @return a future completing with the {@link RenewOutcome} once settlement finishes
   */
  @Override
  public CompletableFuture<RenewOutcome> renewHard(String cacheKey, Supplier<VersionedValue> reader) {
    RefreshTicket existing = refreshes.get(cacheKey);
    return existing != null ? existing.upgradeToHard() : submitHard(cacheKey, reader);
  }

  /**
   * Reserves and submits a soft-refresh ticket without allocating a caller future.
   *
   * @param cacheKey  the physical cache key
   * @param softTtlMs the soft TTL to apply upon settlement
   * @param reader    the stamped value loader
   */
  private void submitSoft(String cacheKey, long softTtlMs, Supplier<VersionedValue> reader) {
    CacheEntry snapshot = liveSnapshot(cacheKey);
    if (snapshot == null || !limiter.tryAcquire()) {
      return;
    }
    RefreshTicket ticket = new RefreshTicket(
      cacheKey,
      softTtlMs,
      STATE_SOFT,
      null,
      snapshot.getDataVersion(),
      snapshot.getHardExpireAtMs()
    );
    if (refreshes.putIfAbsent(cacheKey, ticket) != null) {
      limiter.release();
      return;
    }
    dispatchTicket(cacheKey, ticket, reader, false);
  }

  /**
   * Reserves and submits a hard-renewal ticket, upgrading a racing ticket if another thread
   * installed one concurrently.
   *
   * @param cacheKey the physical cache key
   * @param reader   the stamped value loader
   * @return a future completing with the renewal outcome
   */
  private CompletableFuture<RenewOutcome> submitHard(String cacheKey, Supplier<VersionedValue> reader) {
    CacheEntry snapshot = liveSnapshot(cacheKey);
    if (snapshot == null) {
      return ABSENT_SKIPPED;
    }
    if (!limiter.tryAcquire()) {
      RefreshTicket raced = refreshes.get(cacheKey);
      return raced != null ? raced.upgradeToHard() : LIMITER_SKIPPED;
    }
    CompletableFuture<RenewOutcome> hardFuture = new CompletableFuture<>();
    RefreshTicket ticket = new RefreshTicket(
      cacheKey,
      0L,
      STATE_HARD,
      hardFuture,
      snapshot.getDataVersion(),
      snapshot.getHardExpireAtMs()
    );
    RefreshTicket raced = refreshes.putIfAbsent(cacheKey, ticket);
    if (raced != null) {
      limiter.release();
      return raced.upgradeToHard();
    }
    dispatchTicket(cacheKey, ticket, reader, true);
    return hardFuture;
  }

  /**
   * Samples the current live resident entry for {@code cacheKey}, returning {@code null} when
   * absent or already logically expired.
   *
   * @param cacheKey the physical cache key
   * @return the live {@link CacheEntry}, or {@code null}
   */
  private @Nullable CacheEntry liveSnapshot(String cacheKey) {
    CacheEntry snapshot = QuietCache.getIfPresent(cache, cacheKey);
    return snapshot == null || ttlPolicy.isLogicallyExpired(snapshot) ? null : snapshot;
  }

  /**
   * Submits a reserved {@link RefreshTicket} to {@link #executor} with {@link #refreshTimeoutMs},
   * retiring the ticket and releasing the limiter permit if submission throws.
   *
   * @param cacheKey the physical cache key
   * @param ticket   the installed reservation ticket
   * @param reader   the stamped value loader
   * @param hard     {@code true} for a hard renewal, {@code false} for a soft refresh
   */
  private void dispatchTicket(String cacheKey, RefreshTicket ticket, Supplier<VersionedValue> reader, boolean hard) {
    try {
      InterruptingAsync.supplyAsync(reader, executor, refreshTimeoutMs, TimeUnit.MILLISECONDS).whenComplete(ticket);
    } catch (Throwable t) {
      // Retire before releasing the permit so the next acquirer cannot adopt this failed ticket.
      boolean retired = refreshes.remove(cacheKey, ticket);
      if (retired) {
        limiter.release();
      }
      boolean rejected = t instanceof RejectedExecutionException;
      if (rejected) {
        log.debug("Background refresh rejected by executor (saturated), key={}", cacheKey);
      }
      ticket.complete(rejected ? RenewOutcome.Skipped.EXECUTOR_REJECTED : new RenewOutcome.Failed(t));
      if (!hard && !rejected) {
        throw t;
      }
    }
  }

  /**
   * Unwraps a single {@link CompletionException} layer so timeouts and root causes are classified
   * directly.
   *
   * @param failure the async completion failure
   * @return the unwrapped cause, or {@code failure} itself
   */
  private static Throwable unwrapCause(Throwable failure) {
    return failure instanceof CompletionException && failure.getCause() != null ? failure.getCause() : failure;
  }

  /**
   * Per-key refresh reservation, creation-time baseline snapshot, atomic upgrade state machine,
   * and async completion callback.
   */
  private final class RefreshTicket implements BiConsumer<VersionedValue, Throwable> {

    private final String key;
    private final long refreshSoftTtlMs;
    private final long dataVersion;
    private final long hardExpireAtMs;

    @SuppressWarnings("all")
    private int state;

    /** {@code null} (no waiter) | {@code CompletableFuture<RenewOutcome>} | terminal {@link RenewOutcome}. */
    @SuppressWarnings("all")
    private @Nullable Object completion;

    /**
     * Creates a refresh ticket bound to a live resident snapshot.
     *
     * @param key              the physical cache key
     * @param refreshSoftTtlMs the soft TTL to apply on soft settlement
     * @param initialState     {@link #STATE_SOFT} or {@link #STATE_HARD}
     * @param hardFuture       pre-allocated future for hard renewals, or {@code null} for soft
     * @param dataVersion      the resident's version at reservation time
     * @param hardExpireAtMs   the resident's hard deadline at reservation time
     */
    private RefreshTicket(
      String key,
      long refreshSoftTtlMs,
      int initialState,
      @Nullable CompletableFuture<RenewOutcome> hardFuture,
      long dataVersion,
      long hardExpireAtMs
    ) {
      this.key = key;
      this.refreshSoftTtlMs = refreshSoftTtlMs;
      this.state = initialState;
      this.completion = hardFuture;
      this.dataVersion = dataVersion;
      this.hardExpireAtMs = hardExpireAtMs;
    }

    /**
     * Atomically upgrades a soft ticket to hard renewal if not yet settling, and returns a
     * {@link CompletableFuture} that completes once this ticket's settlement has finished.
     *
     * @return the shared completion future
     */
    @SuppressWarnings("unchecked")
    private CompletableFuture<RenewOutcome> upgradeToHard() {
      TICKET_STATE.compareAndSet(this, STATE_SOFT, STATE_UPGRADED_HARD);
      for (;;) {
        Object current = TICKET_COMPLETION.getVolatile(this);
        if (current instanceof CompletableFuture<?> future) {
          return (CompletableFuture<RenewOutcome>) future;
        }
        if (current instanceof RenewOutcome done) {
          return CompletableFuture.completedFuture(done);
        }
        CompletableFuture<RenewOutcome> created = new CompletableFuture<>();
        if (TICKET_COMPLETION.compareAndSet(this, null, created)) {
          return created;
        }
      }
    }

    /**
     * Publishes the terminal {@link RenewOutcome} and completes any attached waiter future.
     *
     * @param outcome the terminal settlement outcome
     */
    @SuppressWarnings("unchecked")
    private void complete(RenewOutcome outcome) {
      Object previous = TICKET_COMPLETION.getAndSet(this, outcome);
      if (previous instanceof CompletableFuture<?> future) {
        ((CompletableFuture<RenewOutcome>) future).complete(outcome);
      }
    }

    /**
     * Settles the async refresh result or failure, retires the ticket and releases the limiter
     * permit before completing any waiting future so callbacks can safely re-enter.
     *
     * @param loaded  the loaded {@link VersionedValue}, or {@code null}
     * @param failure the execution or timeout failure, or {@code null}
     */
    @Override
    public void accept(@Nullable VersionedValue loaded, @Nullable Throwable failure) {
      RenewOutcome outcome;
      try {
        if (failure != null) {
          Throwable root = unwrapCause(failure);
          settleFailure(root);
          outcome = new RenewOutcome.Failed(root);
        } else if (loaded != null && loaded.value() != null) {
          boolean hard = !TICKET_STATE.compareAndSet(this, STATE_SOFT, STATE_SETTLING_SOFT);
          outcome = settleLoaded(loaded, hard);
        } else {
          settleFailure(null);
          outcome = RenewOutcome.Failed.EMPTY;
        }
      } catch (Throwable t) {
        ZetaExceptionHandler.handleException("background refresh settlement failed: key=" + key, t);
        outcome = new RenewOutcome.Failed(t);
      } finally {
        // Retire and release before completing so chained callbacks observe a clean reservation slot.
        // The successful removal owns the permit, keeping retirement and release idempotent.
        if (refreshes.remove(key, this)) {
          limiter.release();
        }
      }
      complete(outcome);
    }

    /**
     * Logs a failure and closes the soft retry window if this ticket originated as a soft refresh.
     *
     * @param failure the root failure, or {@code null} when the loader returned empty
     */
    private void settleFailure(@Nullable Throwable failure) {
      if (failure instanceof TimeoutException) {
        log.warn("Background refresh timed out after {}ms: {}", refreshTimeoutMs, key);
      } else if (failure != null) {
        log.warn("Background refresh failed: {}", key, failure);
      }
      if (((int) TICKET_STATE.getVolatile(this) & FLAG_FROM_SOFT) != 0) {
        closeRetryWindow();
      }
    }

    /**
     * Applies a loaded value inside the key's map bin under invalidation, residency, version, and
     * expiry guards, re-arming either both deadlines (hard) or only the soft window (soft).
     *
     * @param loaded the loaded value and its pre-load version stamp
     * @param hard   {@code true} to perform hard renewal, {@code false} for soft refresh
     * @return the settlement {@link RenewOutcome}
     */
    private RenewOutcome settleLoaded(VersionedValue loaded, boolean hard) {
      RenewOutcome[] outcome = { RenewOutcome.Skipped.SOFT_REFRESH };
      cache
        .asMap()
        .compute(key, (k, entry) -> {
          if (entryLifecycle.staleAgainstInvalidation(key, loaded.dataVersion())) {
            outcome[0] = RenewOutcome.Skipped.INVALIDATED;
            return entry;
          }
          if (entry == null) {
            outcome[0] = RenewOutcome.Skipped.ABSENT_OR_EXPIRED;
            return null;
          }
          if (entry.getDataVersion() > dataVersion) {
            outcome[0] = RenewOutcome.Skipped.NEWER_VALUE;
            return entry;
          }
          if (!hard && ttlPolicy.isLogicallyExpired(entry)) {
            outcome[0] = RenewOutcome.Skipped.ABSENT_OR_EXPIRED;
            return entry;
          }
          EntryDraft draft = entryLifecycle
            .editEntry(entry)
            .value(loaded.value())
            .version(Math.max(entry.getDataVersion(), loaded.dataVersion()));
          CacheEntry updated = (hard ? draft.rearmExpiry() : draft.softTtl(refreshSoftTtlMs)).build();
          if (hard) {
            outcome[0] = new RenewOutcome.Applied(updated);
          }
          return updated;
        });
      return outcome[0];
    }

    /**
     * Moves the soft deadline to the existing hard deadline after a failed soft refresh so
     * subsequent reads do not hammer a failing source, guarded by snapshot identity and tombstones.
     */
    private void closeRetryWindow() {
      boolean[] closed = { false };
      cache
        .asMap()
        .compute(key, (k, entry) -> {
          if (
            entry == null ||
            entry.getDataVersion() != dataVersion ||
            entry.getHardExpireAtMs() != hardExpireAtMs ||
            entryLifecycle.staleAgainstInvalidation(key, dataVersion)
          ) {
            return entry;
          }
          closed[0] = true;
          return entryLifecycle.editEntry(entry).softExpiryAt(entry.getHardExpireAtMs()).build();
        });
      if (closed[0]) {
        log.debug("Background soft refresh settled without a value; closed the retry window for key={}", key);
      }
    }
  }
}
