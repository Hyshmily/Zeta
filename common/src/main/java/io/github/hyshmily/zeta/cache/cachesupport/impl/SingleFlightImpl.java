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
package io.github.hyshmily.zeta.cache.cachesupport.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.cache.cachesupport.CircuitBreaker;
import io.github.hyshmily.zeta.cache.cachesupport.CircuitBreakerRegistry;
import io.github.hyshmily.zeta.cache.cachesupport.LoadStatus;
import io.github.hyshmily.zeta.cache.cachesupport.LoaderOutcome;
import io.github.hyshmily.zeta.cache.cachesupport.SingleFlight;
import io.github.hyshmily.zeta.util.InterruptingAsync;
import io.github.hyshmily.zeta.util.LogThrottle;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * Deduplicates concurrent in-flight loads for the same key.
 * <p>
 * Only the first caller executes the supplier; subsequent callers wait for
 * the same {@link CompletableFuture}. On normal completion, the future
 * remains cached (TTL-based expiry) so that late-arriving callers reuse the
 * completed result (see ADR-0002). On timeout or exception, the entry is
 * evicted immediately to allow a subsequent retry.
 * <p>
 * The internal dedup cache is bounded by {@code maxSize} (LRU eviction) and
 * entries expire after the configured {@code ttlSec} seconds from write.
 * This class is thread-safe.
 */
@Slf4j
@Internal
public class SingleFlightImpl implements SingleFlight {

  /** Caffeine cache tracking currently in-flight loads (key -> CompletableFuture). */
  private final Cache<String, CompletableFuture<Object>> inflightLoads;
  /** Async executor for running the supplier. */
  private final Executor executor;
  /** Timeout in seconds before a supplier future is completed exceptionally. */
  private final int timeoutSeconds;
  /** Maximum number of in-flight keys tracked simultaneously. */
  private final int inflightMaxSize;
  /**
   * Per-prefix circuit-breaker shards protecting remote calls from cascading
   * failures — one sick datasource trips only its own prefix bucket, never
   * the whole key space.
   */
  private final CircuitBreakerRegistry breakerRegistry;

  /**
   * Rate-limiter for the join-failure WARN: one per window — a data-source
   * outage with a high miss rate would otherwise flood the log with one
   * stack-trace WARN per read (the project's "never WARN on hot path" rule).
   * Admission is strict — {@link LogThrottle}
   * claims the window with a compare-and-set, so exactly one caller per window logs.
   * The atomicity and the monotonic clock are provided by {@link LogThrottle}.
   */
  private final LogThrottle failureLogThrottle = LogThrottle.perDefaultWindow();

  /**
   * Rate-limiter for the high-inflight WARN (same window pattern). Admission is strict — {@link LogThrottle}
   * claims the window with a compare-and-set, so exactly one caller per window logs.
   * The atomicity and the monotonic clock are provided by {@link LogThrottle}.
   */
  private final LogThrottle inflightLogThrottle = LogThrottle.perDefaultWindow();

  /**
   * Cumulative count of dedup loads resolved empty by a reader timeout —
   * the Redis-degraded stall signal behind {@code getLoadTimeoutCount()} /
   * {@code zeta.stall.redis_degraded.timeouts.total}. A timeout is the one
   * failure shape the breaker already counts toward OPEN, so this counter
   * exists for attribution, not protection.
   */
  private final AtomicLong timeoutCounter = new AtomicLong();

  /**
   * Live count of reader suppliers currently executing on the dedup executor —
   * gauges real in-flight loads without the per-call {@code estimatedSize()}
   * segment scan on the admission path. Entries retained for the dedup TTL
   * after completion are excluded: this answers "how many remote calls are
   * running right now", which is what the high-inflight WARN is about.
   */
  private final AtomicInteger runningLoads = new AtomicInteger();

  /**
   * Creates a SingleFlightImpl deduplicator that prevents concurrent in-flight loads
   * for the same key.
   *
   * @param maxSize        maximum number of concurrent in-flight keys tracked
   * @param ttlSec         time-to-live for dedup entries after write
   * @param timeoutSeconds per-supplier timeout before the future is completed exceptionally
   * @param executor       async executor for supplier execution
   * @param circuitBreaker circuit breaker for protecting remote calls
   */
  public SingleFlightImpl(
    int maxSize,
    int ttlSec,
    int timeoutSeconds,
    Executor executor,
    CircuitBreaker circuitBreaker
  ) {
    this(maxSize, ttlSec, timeoutSeconds, executor, CircuitBreakerRegistry.fixed(circuitBreaker));
  }

  /**
   * Creates a SingleFlightImpl deduplicator fronted by a per-prefix breaker
   * registry: each key's admission, success, and failure accounting routes to
   * its own prefix shard.
   *
   * @param maxSize         maximum number of concurrent in-flight keys tracked
   * @param ttlSec          time-to-live for dedup entries after write
   * @param timeoutSeconds  per-supplier timeout before the future is completed exceptionally
   * @param executor        async executor for supplier execution
   * @param breakerRegistry per-prefix breaker shards (never {@code null})
   */
  public SingleFlightImpl(
    int maxSize,
    int ttlSec,
    int timeoutSeconds,
    Executor executor,
    CircuitBreakerRegistry breakerRegistry
  ) {
    this.inflightLoads = Caffeine.newBuilder().maximumSize(maxSize).expireAfterWrite(ttlSec, TimeUnit.SECONDS).build();
    this.executor = executor;
    this.timeoutSeconds = timeoutSeconds;
    this.inflightMaxSize = maxSize;
    this.breakerRegistry = breakerRegistry;
  }

  /**
   * Whether any breaker shard is currently open.
   * The no-key aggregate behind the stall gauges — per-key callers should
   * prefer {@link #isBreakerOpen(String)}.
   *
   * @return {@code true} if at least one shard is open
   */
  @Override
  public boolean isBreakerOpen() {
    return breakerRegistry.isAnyOpen();
  }

  /**
   * Whether the given key's breaker shard is currently open.
   * Used by {@code HotKeyCache} to decide whether to return stale cache on miss.
   *
   * @param cacheKey the key whose shard to check
   * @return {@code true} if the key's shard breaker is open
   */
  @Override
  public boolean isBreakerOpen(String cacheKey) {
    return breakerRegistry.isOpen(cacheKey);
  }

  /**
   * Approximate number of keys currently tracked for dedup.
   * Useful for monitoring and diagnostics.
   *
   * @return the estimated number of in-flight keys
   */
  @Override
  public long estimatedInflightSize() {
    return inflightLoads.estimatedSize();
  }

  /** Cumulative reader-timeout count across the single-key and batch paths. */
  @Override
  public long getLoadTimeoutCount() {
    return timeoutCounter.get();
  }

  /**
   * Explicit-outcome single-key load — the primary. Never throws for reader
   * failures; they surface as {@code FAILED} (or a {@code SUPPRESSED_*} state
   * for transient degradation) so the caller owns the {@code failOnError}
   * decision. The legacy {@link SingleFlight#load(String, Supplier)} bridge
   * rethrows {@code FAILED} to preserve the historical fail-fast behavior.
   *
   * @param cacheKey the key to load
   * @param reader   the value supplier (should not return {@code null})
   * @param <T>      the value type
   * @return the explicit outcome (never {@code null})
   */
  @SuppressWarnings("all")
  @Override
  public <T> LoaderOutcome<T> loadOutcome(String cacheKey, Supplier<T> reader) {
    CircuitBreaker breaker = breakerRegistry.breakerFor(cacheKey);
    if (tryAdmitUnderBreaker(breaker)) {
      log.debug("CB open, skip load for key={}", cacheKey);
      return LoaderOutcome.suppressed(LoadStatus.SUPPRESSED_BREAKER);
    }

    CompletableFuture<Object> future;
    try {
      future = inflightLoads.asMap().computeIfAbsent(cacheKey, k -> submitReader(reader::get));
    } catch (RejectedExecutionException e) {
      // The executor rejected the task before any data-source call: no reader
      // outcome exists, so report suppression (parity with the timeout path) and
      // release the half-open probe reservation taken by tryAdmitUnderBreaker() — without
      // this callback the reserved slot leaks and the breaker sticks in
      // HALF_OPEN once the quota is drained by repeated rejections.
      log.debug("SingleFlight executor rejected load, key={}", cacheKey);
      breaker.onAbandoned();
      return LoaderOutcome.suppressed(LoadStatus.SUPPRESSED_EXECUTOR);
    }

    try {
      T result = (T) future.join();
      breaker.onSuccess();
      // ADR-0002 catch-only semantics: keep the completed future cached until
      // expireAfterWrite(ttlSec) naturally evicts it, so late-arriving callers
      // reuse the result instead of re-running the supplier.
      return result == null ? LoaderOutcome.sourceEmpty() : LoaderOutcome.loaded(result);
    } catch (CompletionException e) {
      // Record EVERY failure against the key's own shard — the exception filter inside
      // CircuitBreakerImpl decides whether the cause is ignorable. Previously
      // only TimeoutException was recorded, so a fast-failing data source
      // (connection refused, serialization error) never opened the breaker and
      // the stale-entry fallback in HotKeyCache never engaged.
      breaker.onFailure(e.getCause());
      return failedOutcome(cacheKey, e);
    }
  }

  /**
   * Legacy batch bridge over {@link #loadOutcome(Iterable, Function)}: outcomes
   * mapped to {@code Optional}, {@code FAILED} swallowed (the historical
   * {@code failOnError=false} behavior). The fail-fast variant lives as the
   * interface default {@link SingleFlight#load(Iterable, Function, boolean)}.
   *
   * <p>
   * Implementation note: submits all reader suppliers to the executor in a single
   * pass via {@code computeIfAbsent}, then collects results.  This avoids nested
   * thread blocking — the calling thread only blocks during Phase 2 (collect),
   * while all readers execute concurrently on executor threads.
   */
  @SuppressWarnings("all")
  @Override
  public <T> Map<String, Optional<T>> load(Iterable<String> cacheKeys, Function<? super String, ? extends T> reader) {
    Map<String, LoaderOutcome<T>> outcomes = loadOutcome(cacheKeys, reader);
    Map<String, Optional<T>> results = new LinkedHashMap<>();
    for (Map.Entry<String, LoaderOutcome<T>> e : outcomes.entrySet()) {
      results.put(e.getKey(), e.getValue().toOptional());
    }
    return results;
  }

  /**
   * Batch explicit-outcome primary with the two-phase submit/collect shape:
   * Phase 1 submits every reader in one pass, capturing the dedup future per
   * key; Phase 2 joins the captured references. Never throws for reader
   * failures — they surface per key as {@code FAILED} and the caller applies
   * its own fail-fast policy.
   */
  @SuppressWarnings("all")
  @Override
  public <T> Map<String, LoaderOutcome<T>> loadOutcome(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader
  ) {
    List<String> keys = new ArrayList<>();
    cacheKeys.forEach(keys::add);
    if (keys.isEmpty()) {
      return Collections.emptyMap();
    }

    // Phase 0: admit every key under its own prefix shard. A tripped shard
    // suppresses only its own keys — siblings behind healthy shards still load.
    Map<String, CircuitBreaker> admitted = new LinkedHashMap<>();
    Map<String, LoaderOutcome<T>> suppressed = new LinkedHashMap<>();
    for (String key : keys) {
      CircuitBreaker breaker = breakerRegistry.breakerFor(key);
      if (tryAdmitUnderBreaker(breaker)) {
        suppressed.put(key, LoaderOutcome.suppressed(LoadStatus.SUPPRESSED_BREAKER));
      } else {
        admitted.put(key, breaker);
      }
    }

    // Phase 1: submit every admitted reader in one pass, capturing the dedup future per
    // key. The captured reference (not a second map lookup) is joined in phase
    // 2 — one hash round trip fewer, and the captured future always completes
    // even if a concurrent failure removes it from the dedup cache, so the
    // null-future branch of the former get()-based collect is gone.
    List<String> admittedKeys = new ArrayList<>(admitted.keySet());
    List<CompletableFuture<Object>> futures = new ArrayList<>(admittedKeys.size());
    try {
      for (String key : admittedKeys) {
        futures.add(inflightLoads.asMap().computeIfAbsent(key, ignored -> submitReader(() -> reader.apply(key))));
      }
    } catch (RejectedExecutionException e) {
      // Executor saturation is not a reader failure: report suppression for
      // every admitted key. Release each key's half-open probe reservation taken
      // by tryAdmitUnderBreaker(); readers already submitted keep running and
      // their results stay in the dedup cache for late callers (ADR-0002).
      log.debug("SingleFlight executor rejected batch load: {} keys", admittedKeys.size());
      for (String key : admittedKeys) {
        admitted.get(key).onAbandoned();
        suppressed.put(key, LoaderOutcome.suppressed(LoadStatus.SUPPRESSED_EXECUTOR));
      }
      return suppressed;
    }

    Map<String, LoaderOutcome<T>> outcomes = new LinkedHashMap<>(suppressed);
    for (int i = 0; i < admittedKeys.size(); i++) {
      String key = admittedKeys.get(i);
      CircuitBreaker breaker = admitted.get(key);
      CompletableFuture<Object> future = futures.get(i);
      try {
        T result = (T) future.join();
        breaker.onSuccess();
        // ADR-0002 catch-only semantics: keep the completed future cached until
        // expireAfterWrite(ttlSec) naturally evicts it (late callers reuse).
        outcomes.put(key, result == null ? LoaderOutcome.sourceEmpty() : LoaderOutcome.loaded(result));
      } catch (CompletionException e) {
        // Every failure is recorded against the key's own shard (the filter inside
        // CircuitBreakerImpl decides ignorability) — see the single-key path.
        // Joining the captured reference means a waiter observes the same
        // failure as the caller that caused it, instead of silently
        // resolving empty when the dedup entry was invalidated mid-flight.
        breaker.onFailure(e.getCause());
        outcomes.put(key, failedOutcome(key, e));
      }
    }
    // Re-assemble in caller input order (suppressed keys were collected first).
    Map<String, LoaderOutcome<T>> results = new LinkedHashMap<>(outcomes.size());
    for (String key : keys) {
      results.put(key, outcomes.get(key));
    }
    return results;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Removes the dedup entry unconditionally (in-flight or completed). An
   * in-flight future is left to finish for its waiters, but the next caller
   * starts a fresh load instead of replaying its result — the write-path
   * companion to the catch-only invalidation: a value invalidation (local or
   * received, ADR-0067) must also invalidate the load result that produced it.
   * <p>
   * A call arriving from inside the same-key load's own compute (e.g. a reader
   * that performs a synchronous putThrough) is a re-entrant map modification:
   * the future is not yet inserted, so the eviction is impossible AND
   * unnecessary — the load-path version guard refuses the stale store. The
   * exception is therefore absorbed at DEBUG, never propagated to the write
   * path.
   */
  @Override
  public void invalidate(String cacheKey) {
    try {
      inflightLoads.invalidate(cacheKey);
    } catch (RuntimeException e) {
      log.debug(
        "SingleFlight invalidate raced the same-key load compute for key={} (load guard covers the store): {}",
        cacheKey,
        e.toString()
      );
    }
  }

  /**
   * {@inheritDoc}
   * <p>
   * Clears the whole dedup cache (in-flight futures included — waiters keep
   * their future references and complete independently, but no new caller can
   * join or replay them). The bulk companion to {@link #invalidate(String)},
   * wired to {@code HotKeyCache.invalidateAllLocal} so an emergency L1 flush
   * cannot be undone by a completed future replayed within the dedup TTL
   * (ADR-0067).
   */
  @Override
  public void invalidateAll() {
    inflightLoads.invalidateAll();
  }

  /**
   * Check the key's breaker shard and log a warning if the inflight queue is high.
   *
   * @param breaker the responsible shard (resolved by the caller, so admission
   *                and settlement always pair on the same instance)
   * @return {@code true} if the request was <b>intercepted</b> (the shard breaker is open
   *         and the caller must resolve to empty), {@code false} if it may proceed
   */
  private boolean tryAdmitUnderBreaker(CircuitBreaker breaker) {
    if (!breaker.allowRequest()) {
      return true;
    }
    long inflight = runningLoads.get();
    if (inflight > inflightMaxSize * 0.8 && inflightLogThrottle.tryAcquire()) {
      log.warn("SingleFlight inflight queue is high: {}/{}", inflight, inflightMaxSize);
    }
    return false;
  }

  /**
   * Submit a reader supplier to the async executor with timeout.
   * Wraps the executor to capture the running thread reference, so timeout
   * can interrupt it via {@link Thread#interrupt()}, preventing thread-pool
   * starvation from timed-out-but-still-running tasks.
   * <p>
   * <b>Interrupt safety:</b> the captured thread reference is cleared and a
   * {@code stillRunning} latch is lowered when the task finishes, so a
   * timeout that arrives after completion never interrupts a reused pool
   * thread running an unrelated task. The interrupt flag left behind by a
   * timed-out task (which may ignore the interrupt, or re-interrupt itself
   * from a {@code catch (InterruptedException)}) is also cleared on task
   * exit, so the next task on the same pool thread starts clean. Threads are
   * never exposed outside this class, so the only source of the flag is our
   * own timeout machinery.
   * <p>
   * Uses {@link CompletableFuture#supplyAsync} internally so that exception
   * propagation (including {@link Error}) matches CompletableFuture's
   * standard {@code encodeThrowable} semantics.
   *
   * @param reader the supplier to execute asynchronously
   * @return a {@link CompletableFuture} that will complete with the result
   *         or a {@link TimeoutException}
   */
  private CompletableFuture<Object> submitReader(Supplier<Object> reader) {
    // Timeout machinery shared with the background-refresh path: the shared
    // logic (thread capture, race-once latch, interrupt-flag cleanup) lives in
    // {@link InterruptingAsync}.
    return InterruptingAsync.supplyAsync(() -> {
        runningLoads.incrementAndGet();
        try {
          return reader.get();
        } finally {
          runningLoads.decrementAndGet();
        }
      }, executor, timeoutSeconds, TimeUnit.SECONDS);
  }

  /**
   * Classify a {@link CompletionException} from a future join into an explicit
   * outcome: log once (rate-limited), evict the dedup entry so a retry
   * re-runs the reader, count timeouts for the stall gauge, and restore the
   * interrupt bit when the cause is an {@code InterruptedException} (which —
   * six-state design — maps to {@code FAILED}, never to a suppression state).
   *
   * @param cacheKey the key whose load failed
   * @param e        the caught {@link CompletionException}
   * @return the classified outcome (never {@code null})
   */
  private <T> LoaderOutcome<T> failedOutcome(String cacheKey, CompletionException e) {
    logFailure(cacheKey, e);
    inflightLoads.invalidate(cacheKey);

    Throwable cause = e.getCause() != null ? e.getCause() : e;
    if (cause instanceof InterruptedException) {
      Thread.currentThread().interrupt();
      return LoaderOutcome.failed(cause);
    }
    if (cause instanceof TimeoutException) {
      timeoutCounter.incrementAndGet();
      return LoaderOutcome.suppressed(LoadStatus.SUPPRESSED_TIMEOUT);
    }
    return LoaderOutcome.failed(cause);
  }

  /**
   * Rate-limited join-failure WARN: at most one per {@value LogThrottle#DEFAULT_WINDOW_MS}ms
   * window, keeping the first exception. A failing data source must not flood
   * the log with one stack-trace WARN per read.
   *
   * @param cacheKey the key whose load failed
   * @param e        the caught {@link CompletionException}
   */
  private void logFailure(String cacheKey, CompletionException e) {
    if (failureLogThrottle.tryAcquire()) {
      log.warn("singleflight join failed: key={}", cacheKey, e);
    }
  }
}
