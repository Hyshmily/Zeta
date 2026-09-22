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
import io.github.hyshmily.zeta.cache.cachesupport.SingleFlight;
import io.github.hyshmily.zeta.util.InterruptingAsync;
import io.github.hyshmily.zeta.util.LogThrottle;
import java.util.*;
import java.util.concurrent.*;
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
  /** Circuit breaker for protecting remote calls from cascading failures. */
  private final CircuitBreaker circuitBreaker;

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
    this.inflightLoads = Caffeine.newBuilder().maximumSize(maxSize).expireAfterWrite(ttlSec, TimeUnit.SECONDS).build();
    this.executor = executor;
    this.timeoutSeconds = timeoutSeconds;
    this.inflightMaxSize = maxSize;
    this.circuitBreaker = circuitBreaker;
  }

  /**
   * Whether the circuit breaker is currently open.
   * Used by {@code HotKeyCache} to decide whether to return stale cache on miss.
   *
   * @return {@code true} if the breaker is open
   */
  @Override
  public boolean isBreakerOpen() {
    return circuitBreaker.isOpen();
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
   * Load a value via the supplier, deduplicating concurrent requests for the same key.
   * Thread-safe: concurrent calls for the same key share a single future.
   *
   * @param cacheKey the key to load
   * @param reader   the value supplier (should not return {@code null})
   * @param <T>      the value type
   * @return the loaded value, or empty if the load failed or timed out
   */
  @SuppressWarnings("all")
  @Override
  public <T> Optional<T> load(String cacheKey, Supplier<T> reader) {
    if (tryAdmitUnderBreaker()) {
      log.debug("CB open, skip load for key={}", cacheKey);
      return Optional.empty();
    }

    CompletableFuture<Object> future;
    try {
      future = inflightLoads.asMap().computeIfAbsent(cacheKey, k -> submitReader(reader::get));
    } catch (RejectedExecutionException e) {
      // The executor rejected the task before any data-source call: no reader
      // outcome exists, so resolve empty (parity with the timeout path) and
      // release the half-open probe reservation taken by tryAdmitUnderBreaker() — without
      // this callback the reserved slot leaks and the breaker sticks in
      // HALF_OPEN once the quota is drained by repeated rejections.
      log.debug("SingleFlight executor rejected load, resolving empty: key={}", cacheKey);
      circuitBreaker.onAbandoned();
      return Optional.empty();
    }

    try {
      T result = (T) future.join();
      circuitBreaker.onSuccess();
      // ADR-0002 catch-only semantics: keep the completed future cached until
      // expireAfterWrite(ttlSec) naturally evicts it, so late-arriving callers
      // reuse the result instead of re-running the supplier.
      return Optional.ofNullable(result);
    } catch (CompletionException e) {
      // Record EVERY failure against the breaker — the exception filter inside
      // CircuitBreakerImpl decides whether the cause is ignorable. Previously
      // only TimeoutException was recorded, so a fast-failing data source
      // (connection refused, serialization error) never opened the breaker and
      // the stale-entry fallback in HotKeyCache never engaged.
      circuitBreaker.onFailure(e.getCause());
      handleFailure(cacheKey, e);
      return Optional.empty();
    }
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implementation note: submits all reader suppliers to the executor in a single
   * pass via {@code computeIfAbsent}, then collects results.  This avoids nested
   * thread blocking — the calling thread only blocks during Phase 2 (collect),
   * while all readers execute concurrently on executor threads.
   */
  @SuppressWarnings("all")
  @Override
  public <T> Map<String, Optional<T>> load(Iterable<String> cacheKeys, Function<? super String, ? extends T> reader) {
    return load(cacheKeys, reader, false);
  }

  /**
   * Batch variant with an explicit failure policy — see
   * {@link SingleFlight#load(Iterable, Function, boolean)} for the contract.
   */
  @SuppressWarnings("all")
  @Override
  public <T> Map<String, Optional<T>> load(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader,
    boolean failOnError
  ) {
    List<String> keys = new ArrayList<>();
    cacheKeys.forEach(keys::add);
    if (keys.isEmpty()) {
      return Collections.emptyMap();
    }

    if (tryAdmitUnderBreaker()) {
      Map<String, Optional<T>> empty = new LinkedHashMap<>();
      for (String key : keys) empty.put(key, Optional.empty());
      return empty;
    }

    // Phase 1: submit every reader in one pass, capturing the dedup future per
    // key. The captured reference (not a second map lookup) is joined in phase
    // 2 — one hash round trip fewer, and the captured future always completes
    // even if a concurrent failure removes it from the dedup cache, so the
    // null-future branch of the former get()-based collect is gone.
    List<CompletableFuture<Object>> futures = new ArrayList<>(keys.size());
    try {
      for (String key : keys) {
        futures.add(inflightLoads.asMap().computeIfAbsent(key, ignored -> submitReader(() -> reader.apply(key))));
      }
    } catch (RejectedExecutionException e) {
      // Executor saturation is not a reader failure: resolve empty for every
      // key in both failure modes (mirrors the breaker-interception path,
      // which resolves empty even under failOnError). Release the half-open
      // probe reservation taken by tryAdmitUnderBreaker(); readers already submitted
      // keep running and their results stay in the dedup cache for late
      // callers (ADR-0002).
      log.debug("SingleFlight executor rejected batch load, resolving empty: {} keys", keys.size());
      circuitBreaker.onAbandoned();
      Map<String, Optional<T>> empty = new LinkedHashMap<>();
      for (String key : keys) {
        empty.put(key, Optional.empty());
      }
      return empty;
    }

    Map<String, Optional<T>> results = new LinkedHashMap<>();
    for (int i = 0; i < keys.size(); i++) {
      String key = keys.get(i);
      CompletableFuture<Object> future = futures.get(i);
      try {
        T result = (T) future.join();
        circuitBreaker.onSuccess();
        // ADR-0002 catch-only semantics: keep the completed future cached until
        // expireAfterWrite(ttlSec) naturally evicts it (late callers reuse).
        results.put(key, Optional.ofNullable(result));
      } catch (CompletionException e) {
        // Every failure is recorded against the breaker (the filter inside
        // CircuitBreakerImpl decides ignorability) — see the single-key path.
        // Joining the captured reference means a waiter observes the same
        // failure as the caller that caused it, instead of silently
        // resolving empty when the dedup entry was invalidated mid-flight.
        circuitBreaker.onFailure(e.getCause());
        Throwable cause = e.getCause();
        // The contract resolves timeouts to empty in both failure modes; the
        // InterruptedException branch is defensive (CompletableFuture.join is
        // uninterruptible, so a join cause is only ever a reader failure).
        boolean resolveEmpty = cause instanceof TimeoutException || cause instanceof InterruptedException;
        if (cause instanceof TimeoutException) {
          timeoutCounter.incrementAndGet();
        }
        if (failOnError && !resolveEmpty) {
          // Fail-fast batch: handleFailure invalidates the dedup future so a
          // retry re-runs the reader, then rethrows the cause — the first real
          // reader failure aborts the collection and propagates to the caller.
          handleFailure(key, e);
        }
        logFailure(key, e);

        inflightLoads.invalidate(key);
        results.put(key, Optional.empty());

        if (cause instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
      }
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
   * Check the circuit breaker and log a warning if the inflight queue is high.
   *
   * @return {@code true} if the request was <b>intercepted</b> (the breaker is open
   *         and the caller must resolve to empty), {@code false} if it may proceed
   */
  private boolean tryAdmitUnderBreaker() {
    if (!circuitBreaker.allowRequest()) {
      return true;
    }
    long inflight = estimatedInflightSize();
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
    return InterruptingAsync.supplyAsync(reader, executor, timeoutSeconds, TimeUnit.SECONDS);
  }

  /**
   * Handle a {@link CompletionException} from a future join by logging,
   * invalidating the cache entry, and rethrowing an appropriate exception.
   *
   * @param cacheKey the key whose load failed
   * @param e        the caught {@link CompletionException}
   * @throws RuntimeException wrapping the actual cause
   * @throws Error          if the cause is an {@link Error}
   */
  private void handleFailure(String cacheKey, CompletionException e) {
    logFailure(cacheKey, e);
    inflightLoads.invalidate(cacheKey);

    Throwable cause = e.getCause();
    if (cause instanceof InterruptedException) {
      Thread.currentThread().interrupt();
      return;
    }
    if (cause instanceof TimeoutException) {
      timeoutCounter.incrementAndGet();
      return;
    }
    propagateCause(e);
  }

  /**
   * Rethrow a {@link CompletionException}'s cause in its natural form: a
   * {@code RuntimeException} as-is, an {@code Error} as-is, anything else wrapped
   * back in a {@code CompletionException}. Used by the single-key failure path and
   * the fail-fast batch path (where the interface contract requires every
   * non-timeout reader failure to propagate).
   *
   * @param e the caught {@link CompletionException}
   */
  private static void propagateCause(CompletionException e) {
    Throwable cause = e.getCause();
    if (cause instanceof RuntimeException re) {
      throw re;
    }
    if (cause instanceof Error err) {
      throw err;
    }
    throw new CompletionException(cause);
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
