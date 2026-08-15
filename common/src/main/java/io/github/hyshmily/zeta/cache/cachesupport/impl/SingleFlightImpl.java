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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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
   * Rate-limits the per-key join-failure WARN to one per window. A data-source
   * outage with a high miss rate would otherwise flood the log with one
   * stack-trace WARN per read (the project's "never WARN on hot path" rule).
   */
  private static final long FAILURE_LOG_WINDOW_MS = 10_000;
  private volatile long lastFailureLoggedAtMs = 0L;

  /** Rate-limits the high-inflight WARN (same window pattern). */
  private static final long INFLIGHT_LOG_WINDOW_MS = 10_000;
  private volatile long lastInflightLoggedAtMs = 0L;

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
    if (intercept()) {
      log.debug("CB open, skip load for key={}", cacheKey);
      return Optional.empty();
    }

    CompletableFuture<Object> future = inflightLoads.asMap().computeIfAbsent(cacheKey, k -> submitReader(reader::get));

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

    if (intercept()) {
      Map<String, Optional<T>> empty = new LinkedHashMap<>();
      for (String key : keys) empty.put(key, Optional.empty());
      return empty;
    }

    for (String key : keys) {
      inflightLoads.asMap().computeIfAbsent(key, ignored -> submitReader(() -> reader.apply(key)));
    }

    Map<String, Optional<T>> results = new LinkedHashMap<>();
    for (String key : keys) {
      CompletableFuture<Object> future = inflightLoads.asMap().get(key);
      // May be null if another thread invalidated the future due to a load failure
      if (future == null) {
        results.put(key, Optional.empty());
        continue;
      }
      try {
        T result = (T) future.join();
        circuitBreaker.onSuccess();
        // ADR-0002 catch-only semantics: keep the completed future cached until
        // expireAfterWrite(ttlSec) naturally evicts it (late callers reuse).
        results.put(key, Optional.ofNullable(result));
      } catch (CompletionException e) {
        // Every failure is recorded against the breaker (the filter inside
        // CircuitBreakerImpl decides ignorability) — see the single-key path.
        circuitBreaker.onFailure(e.getCause());
        if (failOnError) {
          // Fail-fast batch: mirror the single-key flow — handleFailure
          // invalidates the dedup future and rethrows the cause
          // (timeouts/interrupts still resolve to empty).
          handleFailure(key, e);
          results.put(key, Optional.empty());
          continue;
        }
        logFailure(key, e);

        inflightLoads.invalidate(key);
        results.put(key, Optional.empty());

        Throwable cause = e.getCause();
        if (cause instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
      }
    }
    return results;
  }

  /**
   * Check the circuit breaker and log a warning if the inflight queue is high.
   *
   * @return {@code true} if the request can proceed, {@code false} if the breaker is open
   */
  private boolean intercept() {
    if (!circuitBreaker.allowRequest()) {
      return true;
    }
    long inflight = estimatedInflightSize();
    if (inflight > inflightMaxSize * 0.8 && tryAcquireInflightLog()) {
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
  @SuppressWarnings("")
  private CompletableFuture<Object> submitReader(Supplier<Object> reader) {
    AtomicReference<Thread> runningThread = new AtomicReference<>();
    AtomicBoolean stillRunning = new AtomicBoolean(true);
    Executor wrapped = task ->
      executor.execute(() -> {
        runningThread.set(Thread.currentThread());
        try {
          task.run();
        } finally {
          // Task finished: forbid any late timeout interrupt (the captured
          // thread reference may be reused for an unrelated task) and clear
          // the interrupt flag so the next task on this pool thread starts
          // clean. The flag can only have come from our own timeout
          // machinery — threads are never exposed outside this class.
          stillRunning.set(false);
          runningThread.set(null);
          Thread.interrupted();
        }
      });

    CompletableFuture<Object> future = CompletableFuture.supplyAsync(reader, wrapped);
    future.orTimeout(timeoutSeconds, TimeUnit.SECONDS);
    future.whenComplete((r, ex) -> {
      // The latch makes "interrupt" and "task finished" race exactly once:
      // finished → no interrupt; timeout first → interrupt the thread while
      // it is still executing this task (runningThread was cleared on exit).
      if (ex instanceof TimeoutException && stillRunning.getAndSet(false)) {
        Thread t = runningThread.get();
        if (t != null) {
          t.interrupt();
        }
      }
    });
    return future;
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
      return;
    }
    if (cause instanceof RuntimeException re) {
      throw re;
    }
    if (cause instanceof Error err) {
      throw err;
    }
    throw new CompletionException(cause);
  }

  /**
   * Rate-limited join-failure WARN: at most one per {@value #FAILURE_LOG_WINDOW_MS}ms
   * window, keeping the first exception. A failing data source must not flood
   * the log with one stack-trace WARN per read.
   *
   * @param cacheKey the key whose load failed
   * @param e        the caught {@link CompletionException}
   */
  private void logFailure(String cacheKey, CompletionException e) {
    if (tryAcquireFailureLog()) {
      log.warn("singleflight join failed: key={}", cacheKey, e);
    }
  }

  /**
   * Rate-limiter for the join-failure WARN (see {@link #logFailure}). Thread-safe
   * via {@link #lastFailureLoggedAtMs} being volatile; a concurrent double-log in
   * the same window is benign (approximate throttle, mirrors BroadcastBuffer).
   *
   * @return {@code true} if the caller may log now
   */
  private boolean tryAcquireFailureLog() {
    long now = System.currentTimeMillis();
    if (now - lastFailureLoggedAtMs < FAILURE_LOG_WINDOW_MS) {
      return false;
    }
    lastFailureLoggedAtMs = now;
    return true;
  }

  /**
   * Rate-limiter for the high-inflight WARN (see {@link #intercept()}).
   *
   * @return {@code true} if the caller may log now
   */
  private boolean tryAcquireInflightLog() {
    long now = System.currentTimeMillis();
    if (now - lastInflightLoggedAtMs < INFLIGHT_LOG_WINDOW_MS) {
      return false;
    }
    lastInflightLoggedAtMs = now;
    return true;
  }
}
