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
package io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import io.github.hyshmily.zeta.cache.cachesupport.circuitbreaker.CircuitBreaker;
import io.github.hyshmily.zeta.cache.cachesupport.circuitbreaker.CircuitBreakerRegistry;
import io.github.hyshmily.zeta.cache.cachesupport.loader.LoadStatus;
import io.github.hyshmily.zeta.cache.cachesupport.loader.LoaderOutcome;
import io.github.hyshmily.zeta.util.CollectionSizing;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.InterruptingAsync;
import jakarta.annotation.PreDestroy;
import java.io.Serial;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.util.Assert;

/**
 * Default {@link SingleFlight} implementation: a bounded Caffeine table of in-flight elections, a
 * per-prefix {@link CircuitBreakerRegistry} for admission control, and an {@link Executor} running
 * elected readers under an interrupting timeout budget.
 *
 * <p>The elected leader owns the entire load lifecycle — breaker admission, reader execution,
 * L1 commit, and breaker settlement — and retires its flight from the table <em>before</em>
 * completing the shared future so woken waiters never re-elect a settled flight. Followers simply
 * join the shared future. Write-path invalidations flag live flights so superseded leaders still
 * return a value to waiting callers without committing stale data to L1.
 */
@Slf4j
@Internal
public class LoadFlights implements SingleFlight {

  /** High-water ratio of {@code inFlightMaxSize} above which backlog debug warnings are emitted. */
  private static final double HIGH_IN_FLIGHT_RATIO = 0.8d;

  /** Default map capacity when a batch {@link Iterable} cannot report its size upfront. */
  private static final int DEFAULT_BATCH_CAPACITY = 16;

  /** Sentinel indicating the executor rejected the leader's submission. */
  private static final InFlight REJECTED = new InFlight();

  /** Sentinel indicating the circuit breaker denied admission before creating a flight. */
  private static final InFlight DENIED = new InFlight();

  /** Sentinel indicating this {@link LoadFlights} instance has been closed. */
  private static final InFlight CLOSED = new InFlight();

  private final CircuitBreakerRegistry breakerRegistry;

  /** Maps each physical cache key to the {@link InFlight} currently serving it. */
  private final Cache<String, InFlight> inFlightLoads;

  /**
   * Flights that left {@link #inFlightLoads} while still unsettled — evicted, expired, or removed
   * by an explicit invalidation. {@link #close()} still cancels their executor task, and each
   * flight removes itself on completion so this set cannot retain a finished load.
   */
  private final Set<InFlight> detachedActive = ConcurrentHashMap.newKeySet();

  private final int inFlightMaxSize;

  /** Per-reader execution timeout budget in seconds (queue wait time is excluded). */
  private final int timeoutSeconds;

  private final Executor executor;

  /** Count of readers actively executing right now. */
  private final AtomicInteger runningLoads = new AtomicInteger();

  /** Count of executor-submitted loads not yet settled (observed by the high-water debug guard). */
  private final AtomicInteger pendingLoads = new AtomicInteger();

  /** Cumulative leader execution timeouts (source-degradation signal). */
  private final AtomicLong timeoutCounter = new AtomicLong();

  /** Backlog warning threshold ({@code max(1, (long) (maxSize * 0.8))}). */
  private final long highInFlightThreshold;

  /** Set before draining flights during shutdown; subsequent elections are suppressed. */
  private final AtomicBoolean closed = new AtomicBoolean();

  /**
   * Creates a {@link LoadFlights} coordinator.
   *
   * @param maxSize         maximum number of concurrent deduplication keys tracked
   * @param ttlSec          retention bound for an unsettled flight in seconds (raised to at least
   *                        {@code timeoutSeconds + 1} so active leaders are not evicted prematurely)
   * @param timeoutSeconds  per-reader execution budget in seconds
   * @param executor        executor running non-caller-affine readers
   * @param breakerRegistry per-prefix circuit breaker registry
   * @throws IllegalArgumentException if any numeric parameter is non-positive
   * @throws NullPointerException     if {@code executor} or {@code breakerRegistry} is {@code null}
   */
  public LoadFlights(
    int maxSize,
    int ttlSec,
    int timeoutSeconds,
    Executor executor,
    CircuitBreakerRegistry breakerRegistry
  ) {
    Assert.isTrue(maxSize > 0, "maxSize must be > 0, but got: " + maxSize);
    Assert.isTrue(ttlSec > 0, "ttlSec must be > 0, but got: " + ttlSec);
    Assert.isTrue(timeoutSeconds > 0, "timeoutSeconds must be > 0, but got: " + timeoutSeconds);
    this.executor = Objects.requireNonNull(executor, "executor must not be null");
    this.breakerRegistry = Objects.requireNonNull(breakerRegistry, "breakerRegistry must not be null");
    this.inFlightMaxSize = maxSize;
    this.timeoutSeconds = timeoutSeconds;
    this.highInFlightThreshold = Math.max(1L, (long) (maxSize * HIGH_IN_FLIGHT_RATIO));
    this.inFlightLoads = Caffeine.newBuilder()
      .maximumSize(maxSize)
      .expireAfterWrite(Math.max(ttlSec, timeoutSeconds + 1), TimeUnit.SECONDS)
      .evictionListener((String key, InFlight flight, RemovalCause cause) -> detach(flight))
      .build();
  }

  /**
   * Tracks a flight removed from the deduplication table while still unsettled, so {@link #close()}
   * can cancel a task the table no longer references. The completion callback removes it again, so
   * a flight that settles between the check and the registration cannot linger.
   *
   * <p>The trailing {@code closed} check mirrors {@code submit}: a flight that left the table just
   * before {@code close()} took its snapshot is in neither collection, so this is the only place
   * that can still retire it. Without it a refresh of the table that loses the race leaves a flight
   * (and every thread waiting on it) unsettled until the executor discards the queued task, which
   * {@code shutdownNow} does without completing the future.
   *
   * @param flight the detached flight, or {@code null}
   */
  private void detach(@Nullable InFlight flight) {
    if (flight == null || isSentinel(flight) || flight.isDone()) {
      return;
    }
    detachedActive.add(flight);
    flight.whenComplete((ignored, failure) -> detachedActive.remove(flight));
    if (closed.get()) {
      // Every step is idempotent: close() may already have processed this flight from either
      // snapshot, and the settlement bit keeps the breaker and the backlog count single-owner.
      detachedActive.remove(flight);
      releasePending(flight);
      abandonFlight(flight, new RejectedExecutionException("SingleFlight closed"));
      flight.cancelTask();
    }
  }

  /**
   * Returns whether any circuit breaker shard is currently degraded.
   *
   * @return {@code true} if any shard is open or half-open
   */
  @Override
  public boolean isBreakerOpen() {
    return breakerRegistry.isAnyDegraded();
  }

  /**
   * Returns whether the circuit breaker shard owning {@code cacheKey} is currently degraded.
   *
   * @param cacheKey the physical cache key
   * @return {@code true} if the key's shard is open or half-open
   */
  @Override
  public boolean isBreakerOpen(String cacheKey) {
    return breakerRegistry.isDegraded(cacheKey);
  }

  /**
   * Returns the number of readers actively executing at this moment (excluding queued tasks).
   *
   * @return the current count of running readers
   */
  @Override
  public long estimatedInFlightSize() {
    return runningLoads.get();
  }

  /**
   * Returns the cumulative count of leader reader timeouts observed since startup.
   *
   * @return the total leader timeout count
   */
  @Override
  public long loadTimeoutCount() {
    return timeoutCounter.get();
  }

  /**
   * Deduplicates and loads a single key: elects a leader outside any cache lock, executes its
   * reader either inline ({@code callerRuns == true}) or on {@link #executor}, and classifies the
   * resulting {@link LoaderOutcome}.
   *
   * @param cacheKey     the physical key to load
   * @param reader       the value supplier executed at most once per election
   * @param callerRuns   {@code true} to execute the leader's reader on the calling thread
   * @param leaderCommit callback storing the leader's loaded value before followers wake
   * @param <T>          the value type
   * @return the classified {@link LoaderOutcome}; reader failures surface as
   *         {@link LoadStatus#FAILED} rather than throwing
   */
  @Override
  public <T> LoaderOutcome<T> load(
    String cacheKey,
    Supplier<T> reader,
    boolean callerRuns,
    Consumer<? super T> leaderCommit
  ) {
    Election election = elect(cacheKey);
    InFlight flight = election.flight();
    // close() may have drained and failed the flight between election and dispatch; fall through
    // to settle() so the caller observes the rejection instead of running an orphaned reader.
    if (election.leader() && !flight.isDone()) {
      if (callerRuns) {
        return runInline(cacheKey, flight, reader, leaderCommit);
      }
      flight = submit(cacheKey, flight, reader, leaderCommit);
    }
    return settle(cacheKey, flight);
  }

  /**
   * Executes a caller-isolated load directly on the calling thread without participating in
   * SingleFlight deduplication, used for transactional loads whose uncommitted values must not be
   * shared with concurrent followers.
   *
   * <p>Circuit breaker admission and settlement still apply.
   *
   * @param cacheKey     the physical key to load
   * @param reader       the value supplier
   * @param leaderCommit callback committing a non-null loaded value
   * @param <T>          the value type
   * @return the classified {@link LoaderOutcome}
   */
  @Override
  public <T> LoaderOutcome<T> loadIsolated(String cacheKey, Supplier<T> reader, Consumer<? super T> leaderCommit) {
    if (closed.get()) {
      log.debug("SingleFlight closed, skip isolated load for key={}", cacheKey);
      return LoaderOutcome.suppressed(LoadStatus.SUPPRESSED_EXECUTOR);
    }
    CircuitBreaker breaker = breakerRegistry.breakerFor(cacheKey);
    long stamp = breaker.tryAcquireStamp();
    if (stamp == CircuitBreaker.STAMP_REJECTED) {
      log.debug("CB open, skip isolated load for key={}", cacheKey);
      return LoaderOutcome.suppressed(LoadStatus.SUPPRESSED_BREAKER);
    }

    runningLoads.incrementAndGet();
    boolean settled = false;
    try {
      T value = reader.get();
      // The breaker is settled once per admission: a leaderCommit failure is a local commit
      // failure, not a second data-source outcome for the same stamp.
      settled = true;
      breaker.onSuccess(stamp);
      if (value != null) {
        leaderCommit.accept(value);
      }
      return LoaderOutcome.ofNullable(value);
    } catch (Throwable t) {
      Throwable cause = causeOf(t);
      if (!settled) {
        breaker.onFailure(cause, stamp);
      }
      if (t instanceof Error err) {
        throw err;
      }
      if (cause instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      return LoaderOutcome.failed(cause);
    } finally {
      runningLoads.decrementAndGet();
    }
  }

  /**
   * Performs a non-blocking deduplicated load: elects and submits on {@link #executor} and returns
   * a {@link CompletableFuture} translating the flight's completion into a {@link LoaderOutcome}.
   *
   * @param cacheKey     the physical key to load
   * @param reader       the value supplier executed at most once per election
   * @param leaderCommit callback storing the leader's loaded value before waiters wake
   * @param <T>          the value type
   * @return a future completing with the classified {@link LoaderOutcome} (never exceptionally for
   *         reader failures)
   */
  @Override
  public <T> CompletableFuture<LoaderOutcome<T>> loadAsync(
    String cacheKey,
    Supplier<T> reader,
    Consumer<? super T> leaderCommit
  ) {
    Election election = elect(cacheKey);
    InFlight flight = election.flight();
    if (election.leader() && !flight.isDone()) {
      flight = submit(cacheKey, flight, reader, leaderCommit);
    }
    return settleAsync(cacheKey, flight);
  }

  /**
   * Deduplicates and loads a batch of keys in parallel: elects and submits every distinct key
   * before joining any flight so cache misses overlap. Duplicate keys reuse their first election.
   *
   * @param cacheKeys    the physical keys to load
   * @param reader       the per-key value supplier function
   * @param leaderCommit the per-key leader commit callback
   * @param <T>          the value type
   * @return map from each distinct key to its {@link LoaderOutcome}, preserving first-seen order
   */
  @Override
  public <T> Map<String, LoaderOutcome<T>> load(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader,
    BiConsumer<String, ? super T> leaderCommit
  ) {
    Map<String, InFlight> elections = new LinkedHashMap<>(mapCapacity(cacheKeys));
    for (String key : cacheKeys) {
      if (elections.containsKey(key)) {
        continue;
      }
      Election election = elect(key);
      InFlight flight = election.flight();
      if (election.leader() && !flight.isDone()) {
        flight = submit(key, flight, () -> reader.apply(key), value -> leaderCommit.accept(key, value));
      }
      elections.put(key, flight);
    }

    Map<String, LoaderOutcome<T>> outcomes = new LinkedHashMap<>(CollectionSizing.capacity(elections.size()));
    for (Map.Entry<String, InFlight> entry : elections.entrySet()) {
      outcomes.put(entry.getKey(), settle(entry.getKey(), entry.getValue()));
    }
    return outcomes;
  }

  /**
   * Removes a key's active flight from the deduplication table and flags it as invalidated so a
   * running leader completes for its waiters without committing stale data to L1.
   *
   * @param cacheKey the physical key to invalidate
   */
  @Override
  public void invalidate(String cacheKey) {
    InFlight flight = inFlightLoads.asMap().remove(cacheKey);
    if (flight != null) {
      flight.markInvalidated();
      detach(flight);
    }
  }

  /**
   * Flags every flight in the deduplication table as invalidated and removes it. Marking matters:
   * a leader that has captured its flight but not yet committed sees the invalidated bit and drops
   * its result, while a new load after the removal starts a fresh flight. Existing joiners settle
   * against the marked flight rather than waiting for a commit that will never land.
   *
   * <p>Each key is removed through {@link #invalidate(String)} rather than one table-wide clear: a
   * clear removes a live flight as an explicit removal, which Caffeine reports through the removal
   * listener, never the eviction listener, so the flight it removed would reach neither
   * {@link #detachedActive} nor {@link #close()}'s snapshot. A load elected after the traversal
   * passed its key is left running — its stamp outranks the clear's — and stays visible to
   * {@link #close()}.
   */
  @Override
  public void invalidateAll() {
    for (String key : inFlightLoads.asMap().keySet()) {
      invalidate(key);
    }
  }

  /**
   * Invalidates every active flight whose key starts with {@code prefix}, leaving flights elected
   * after traversal begins untouched.
   *
   * @param prefix the physical namespace prefix
   */
  @Override
  public void invalidatePrefix(String prefix) {
    for (String key : inFlightLoads.asMap().keySet()) {
      if (key.startsWith(prefix)) {
        invalidate(key);
      }
    }
  }

  /**
   * Marks this coordinator closed, cancels every queued or running executor task, and fails every
   * remaining in-flight entry (including one already detached from the table) with
   * {@link RejectedExecutionException} so waiting threads unblock cleanly before executor shutdown
   * drains queued tasks.
   */
  @PreDestroy
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    // A flight can leave the table while an executor task is still queued; close() must cancel
    // those too, or a shutdownNow that discards queued work leaves every waiter blocked forever.
    Set<InFlight> stranded = new LinkedHashSet<>(inFlightLoads.asMap().values());
    stranded.addAll(detachedActive);
    inFlightLoads.invalidateAll();
    detachedActive.clear();
    for (InFlight flight : stranded) {
      // Fail the waiters first: cancelling the task completes the flight with a cancellation, which
      // would classify a coordinated shutdown as a failed load instead of a suppression.
      releasePending(flight);
      abandonFlight(flight, new RejectedExecutionException("SingleFlight closed"));
      flight.cancelTask();
    }
  }

  /**
   * Elects a new leader flight for {@code key} under circuit breaker admission, or joins an
   * existing in-flight entry via a lock-free fast path.
   *
   * @param key the physical cache key
   * @return the {@link Election} indicating the flight (or sentinel) and whether this caller won
   */
  private Election elect(String key) {
    if (closed.get()) {
      return new Election(CLOSED, false);
    }
    InFlight reusable = inFlightLoads.getIfPresent(key);
    if (reusable != null) {
      return new Election(reusable, false);
    }

    CircuitBreaker breaker = breakerRegistry.breakerFor(key);
    InFlight[] created = new InFlight[1];
    InFlight flight;
    try {
      flight = inFlightLoads
        .asMap()
        .computeIfAbsent(key, k -> {
          long stamp = breaker.tryAcquireStamp();
          if (stamp == CircuitBreaker.STAMP_REJECTED) {
            return null;
          }
          InFlight candidate = new InFlight(breaker, stamp);
          created[0] = candidate;
          return candidate;
        });
    } catch (RuntimeException | Error e) {
      InFlight candidate = created[0];
      if (candidate != null) {
        breaker.onAbandoned(candidate.stamp());
      }
      throw e;
    }

    boolean leader = flight != null && flight == created[0];
    if (leader && closed.get()) {
      // Unconditionally retire and fail: close() may have already swept the table or a concurrent
      // invalidate(key) may have removed the entry while a follower already joined.
      abortLeaderElection(key, flight, new RejectedExecutionException("SingleFlight closed"));
      return new Election(CLOSED, false);
    }
    return flight == null ? new Election(DENIED, false) : new Election(flight, leader);
  }

  /**
   * Submits an elected leader's reader task to {@link #executor} with an interrupting timeout and
   * wires completion settlement. A task that reaches the executor after the flight was settled —
   * closed, abandoned, or cancelled while queued — exits without calling the reader, so no
   * downstream source call is spent on a result that can no longer be committed.
   *
   * @param key    the physical cache key
   * @param flight the elected leader flight
   * @param reader the value supplier
   * @param commit the leader L1 commit callback
   * @param <T>    the value type
   * @return {@code flight} on submission success, or {@link #REJECTED} on executor rejection
   */
  private <T> InFlight submit(String key, InFlight flight, Supplier<T> reader, Consumer<? super T> commit) {
    CompletableFuture<Object> submitted;
    try {
      submitted = submitReader(flight, () -> flight.isDone() ? null : executeLeaderTask(flight, reader, commit));
    } catch (RuntimeException | Error e) {
      abortLeaderElection(key, flight, e);
      if (e instanceof RejectedExecutionException) {
        return REJECTED;
      }
      throw e;
    }
    submitted.whenComplete((result, error) -> finishFlight(key, flight, result, error));
    flight.attachTask(submitted);
    // close() may have drained the table between elect() and this submission; retiring the flight
    // here (before cancelling the task) keeps its waiters classified as a suppression instead of
    // letting the cancellation settle the breaker as a failure. Abandoning is idempotent, so a
    // close() that already took this flight leaves nothing behind.
    if (closed.get()) {
      abortLeaderElection(key, flight, new RejectedExecutionException("SingleFlight closed"));
      flight.cancelTask();
    }
    logHighBacklog();
    return flight;
  }

  /**
   * Submits a reader task to {@link #executor} under {@link InterruptingAsync} and accounts for
   * its backlog slot in {@link #pendingLoads}.
   *
   * @param flight the leader flight
   * @param reader the wrapped reader task
   * @return the underlying execution future
   */
  private CompletableFuture<Object> submitReader(InFlight flight, Supplier<Object> reader) {
    pendingLoads.incrementAndGet();
    try {
      CompletableFuture<Object> submitted = InterruptingAsync.supplyAsync(
        reader,
        executor,
        timeoutSeconds,
        TimeUnit.SECONDS
      );
      flight.claimPending();
      return submitted;
    } catch (RuntimeException | Error e) {
      pendingLoads.decrementAndGet();
      throw e;
    }
  }

  /**
   * Executes the leader's reader on the calling thread without an executor timeout so caller
   * transaction, security context, and MDC remain intact.
   *
   * @param key    the physical cache key
   * @param flight the elected leader flight
   * @param reader the value supplier
   * @param commit the leader L1 commit callback
   * @param <T>    the value type
   * @return the classified {@link LoaderOutcome}
   */
  private <T> LoaderOutcome<T> runInline(String key, InFlight flight, Supplier<T> reader, Consumer<? super T> commit) {
    T value;
    try {
      value = executeLeaderTask(flight, reader, commit);
    } catch (Throwable t) {
      CompletionException wrapped = t instanceof CompletionException ce ? ce : new CompletionException(t);
      if (causeOf(wrapped) instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      finishFlight(key, flight, null, wrapped);
      return failureOutcome(wrapped);
    }
    finishFlight(key, flight, value, null);
    return LoaderOutcome.ofNullable(value);
  }

  /**
   * Executes the leader's reader, settles the circuit breaker once, and invokes {@code commit}
   * when this thread owns settlement and the flight was not invalidated.
   *
   * <p>The circuit breaker settles before {@code commit} so local admission or TTL failures never
   * masquerade as downstream data-source failures.
   *
   * @param flight the leader flight
   * @param reader the value supplier
   * @param commit the leader L1 commit callback
   * @param <T>    the value type
   * @return the loaded value, which may be {@code null}
   */
  private <T> @Nullable T executeLeaderTask(InFlight flight, Supplier<T> reader, Consumer<? super T> commit) {
    runningLoads.incrementAndGet();
    flight.runningThreadId = Thread.currentThread().getId();
    try {
      T value = reader.get();
      if (flight.trySettle()) {
        CircuitBreaker breaker = flight.breaker();
        if (breaker != null) {
          breaker.onSuccess(flight.stamp());
        }
        if (value != null && !flight.isInvalidated()) {
          commit.accept(value);
        }
      }
      return value;
    } catch (Throwable t) {
      if (flight.trySettle()) {
        CircuitBreaker breaker = flight.breaker();
        if (breaker != null) {
          breaker.onFailure(causeOf(t), flight.stamp());
        }
      }
      if (t instanceof Error err) {
        throw err;
      }
      // Tag all reader failures so join-side classification distinguishes reader-thrown
      // TimeoutException/RejectedExecutionException from coordinator-originated suppressions.
      throw new ReaderFailure(t);
    } finally {
      flight.runningThreadId = 0L;
      runningLoads.decrementAndGet();
    }
  }

  /**
   * Retires {@code flight} from the deduplication table, releases its backlog slot, settles any
   * timeout failure not observed by the reader thread, and completes the shared future.
   *
   * <p>Only the first caller finalizes a flight. A later one is the cancellation callback of a
   * flight {@link #close()} or {@link #detach(InFlight)} already settled — a running leader that
   * ignores its interrupt, or a queued task the executor drops — and has nothing left to release.
   * It must also stay out of the table (the detach hook runs inside Caffeine's eviction
   * notification for the same key, so a nested removal re-enters the removal it is part of) and out
   * of the metrics (a leader timeout that lands after a shutdown already answered the waiters with a
   * suppression must not raise the source-degradation signal).
   *
   * <p>The error is classified after unwrapping transport envelopes, so a coordinator timeout that
   * arrives wrapped in a {@link CompletionException} still reaches the timeout counter and is not
   * logged as a join failure. A {@link ReaderFailure}-wrapped timeout is a reader failure and is
   * counted as one, matching {@link #failureOutcome(Throwable)}.
   *
   * @param key    the physical cache key
   * @param flight the leader flight being finalized
   * @param result the loaded value, or {@code null}
   * @param error  the execution error or timeout, or {@code null} on success
   */
  private void finishFlight(String key, InFlight flight, @Nullable Object result, @Nullable Throwable error) {
    if (flight.isDone()) {
      return;
    }

    boolean retired = inFlightLoads.asMap().remove(key, flight);
    releasePending(flight);
    if (error == null) {
      flight.complete(result);
      return;
    }

    Throwable failure = unwrapTransport(error);
    boolean timedOut = failure instanceof TimeoutException;
    if (flight.trySettle()) {
      CircuitBreaker breaker = flight.breaker();
      if (breaker != null) {
        breaker.onFailure(causeOf(failure), flight.stamp());
      }
    }
    if (timedOut) {
      timeoutCounter.incrementAndGet();
    }
    flight.completeExceptionally(error);
    if (retired && !timedOut) {
      log.warn("singleflight join failed: key={}", key, error);
    }
  }

  /**
   * Undoes a leader election that failed before executor submission by retiring the flight,
   * releasing any reserved probe, and failing waiting followers.
   *
   * @param key    the physical cache key
   * @param flight the aborted leader flight
   * @param cause  the exception preventing submission
   */
  private void abortLeaderElection(String key, InFlight flight, Throwable cause) {
    inFlightLoads.asMap().remove(key, flight);
    releasePending(flight);
    abandonFlight(flight, cause);
  }

  /**
   * Releases the breaker probe (at most once) and completes {@code flight} exceptionally.
   *
   * @param flight the flight being abandoned
   * @param cause  the failure to propagate to joined waiters
   */
  private static void abandonFlight(InFlight flight, Throwable cause) {
    if (flight.trySettle()) {
      CircuitBreaker breaker = flight.breaker();
      if (breaker != null) {
        breaker.onAbandoned(flight.stamp());
      }
    }
    flight.completeExceptionally(cause);
  }

  /**
   * Decrements {@link #pendingLoads} at most once if {@code flight} claimed a backlog slot.
   *
   * @param flight the flight whose backlog slot should be released
   */
  private void releasePending(InFlight flight) {
    if (flight.tryReleasePending()) {
      pendingLoads.decrementAndGet();
    }
  }

  /**
   * Joins a flight interruptibly and translates its outcome into a {@link LoaderOutcome}.
   *
   * @param key    the physical cache key
   * @param flight the flight or sentinel to join
   * @param <T>    the value type
   * @return the classified {@link LoaderOutcome}
   * @throws IllegalStateException if the calling thread is already executing this key's leader task
   */
  @SuppressWarnings("unchecked")
  private <T> LoaderOutcome<T> settle(String key, InFlight flight) {
    if (isSentinel(flight)) {
      return suppressedOutcome(key, flight);
    }
    if (flight.isRunningOnCurrentThread()) {
      throw new IllegalStateException("re-entrant SingleFlight load for key: " + key);
    }
    try {
      T result = (T) flight.get();
      return LoaderOutcome.ofNullable(result);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return LoaderOutcome.failed(e);
    } catch (ExecutionException | CancellationException e) {
      return failureOutcome(e);
    }
  }

  /**
   * Translates a flight into a non-blocking {@link CompletableFuture} of {@link LoaderOutcome}.
   *
   * @param key    the physical cache key
   * @param flight the flight or sentinel to observe
   * @param <T>    the value type
   * @return a future completing with the classified {@link LoaderOutcome}
   * @throws IllegalStateException if the calling thread is already executing this key's leader task
   */
  @SuppressWarnings("unchecked")
  private <T> CompletableFuture<LoaderOutcome<T>> settleAsync(String key, InFlight flight) {
    if (isSentinel(flight)) {
      return CompletableFuture.completedFuture(suppressedOutcome(key, flight));
    }
    if (flight.isRunningOnCurrentThread()) {
      throw new IllegalStateException("re-entrant SingleFlight load for key: " + key);
    }
    return flight.handle((result, error) -> {
      if (error != null) {
        return failureOutcome(error);
      }
      return LoaderOutcome.ofNullable((T) result);
    });
  }

  /**
   * Maps a sentinel flight ({@link #DENIED}, {@link #REJECTED}, or {@link #CLOSED}) to its
   * corresponding suppressed {@link LoaderOutcome}.
   *
   * @param key    the physical cache key
   * @param flight the sentinel flight
   * @param <T>    the value type
   * @return the suppressed {@link LoaderOutcome}
   */
  private <T> LoaderOutcome<T> suppressedOutcome(String key, InFlight flight) {
    if (flight == DENIED) {
      log.debug("CB open, skip load for key={}", key);
      return LoaderOutcome.suppressed(LoadStatus.SUPPRESSED_BREAKER);
    }
    if (flight == CLOSED) {
      log.debug("SingleFlight closed, skip load for key={}", key);
    } else {
      log.debug("SingleFlight executor rejected load, key={}", key);
    }
    return LoaderOutcome.suppressed(LoadStatus.SUPPRESSED_EXECUTOR);
  }

  /**
   * Classifies a failed flight into either a coordinator suppression
   * ({@link LoadStatus#SUPPRESSED_EXECUTOR} / {@link LoadStatus#SUPPRESSED_TIMEOUT}) or a
   * {@link LoadStatus#FAILED} outcome wrapping the underlying reader cause.
   *
   * @param error the exception carried by the failed flight
   * @param <T>   the value type
   * @return the classified {@link LoaderOutcome}
   */
  @SuppressWarnings("all")
  private <T> LoaderOutcome<T> failureOutcome(Throwable error) {
    Throwable failure = unwrapTransport(error);
    if (failure instanceof ReaderFailure readerFailure) {
      return LoaderOutcome.failed(causeOf(readerFailure));
    }
    if (failure instanceof RejectedExecutionException) {
      return LoaderOutcome.suppressed(LoadStatus.SUPPRESSED_EXECUTOR);
    }
    return failure instanceof TimeoutException
      ? LoaderOutcome.suppressed(LoadStatus.SUPPRESSED_TIMEOUT)
      : LoaderOutcome.failed(causeOf(failure));
  }

  /**
   * Emits a debug warning when the submitted executor backlog exceeds
   * {@link #highInFlightThreshold}.
   */
  private void logHighBacklog() {
    long backlog = pendingLoads.get();
    if (backlog > highInFlightThreshold) {
      log.debug("SingleFlight in-flight queue is high: {}/{}", backlog, inFlightMaxSize);
    }
  }

  /**
   * Returns whether {@code flight} is one of the static suppression sentinels.
   *
   * @param flight the flight reference to test
   * @return {@code true} if {@code flight} is {@link #DENIED}, {@link #REJECTED}, or {@link #CLOSED}
   */
  private static boolean isSentinel(InFlight flight) {
    return flight == DENIED || flight == REJECTED || flight == CLOSED;
  }

  /**
   * Unwraps transport envelopes ({@link CompletionException}, {@link ExecutionException}) down to
   * the first failure that is not an envelope, stopping at a {@link ReaderFailure} tag.
   *
   * <p>The stop is what keeps classification honest: a coordinator timeout fails bare and is a
   * suppression, while a reader-thrown {@link TimeoutException} arrives wrapped in
   * {@link ReaderFailure} and is a reader failure like any other.
   *
   * @param error the wrapped or bare throwable
   * @return the transport-free failure
   */
  private static Throwable unwrapTransport(Throwable error) {
    Throwable failure = error;
    while (
      !(failure instanceof ReaderFailure) &&
      (failure instanceof CompletionException || failure instanceof ExecutionException)
    ) {
      Throwable next = failure.getCause();
      if (next == null) {
        break;
      }
      failure = next;
    }
    return failure;
  }

  /**
   * Unwraps transport envelopes ({@link CompletionException}, {@link ExecutionException}) and
   * {@link ReaderFailure} tags to expose the underlying root cause.
   *
   * @param error the wrapped or bare throwable
   * @return the unwrapped cause
   */
  private static Throwable causeOf(Throwable error) {
    Throwable current = error;
    while (
      (current instanceof CompletionException ||
        current instanceof ExecutionException ||
        current instanceof ReaderFailure) &&
      current.getCause() != null
    ) {
      current = current.getCause();
    }
    return current;
  }

  /**
   * Computes an initial {@link LinkedHashMap} capacity for a batch key source, falling back to
   * {@link #DEFAULT_BATCH_CAPACITY} when the iterable is not a {@link Collection}.
   *
   * @param cacheKeys the batch key source
   * @return the initial map capacity
   */
  private static int mapCapacity(Iterable<String> cacheKeys) {
    return cacheKeys instanceof Collection<?> collection
      ? CollectionSizing.capacity(collection.size())
      : DEFAULT_BATCH_CAPACITY;
  }

  /**
   * Deduplication entry for one in-flight key, extending {@link CompletableFuture} directly so a
   * cache miss allocates one shared object.
   *
   * <p>Packs the write-path invalidation flag, single-settlement bit, backlog-counted bit, and
   * 61-bit circuit breaker stamp into one {@code long} field manipulated via {@link VarHandle}.
   */
  private static final class InFlight extends CompletableFuture<Object> {

    /** 61-bit mask for circuit breaker probe stamps. */
    private static final long STAMP_MASK = (1L << 61) - 1;

    /** Sign bit flagging a write-path invalidation so {@link #isInvalidated()} is one compare. */
    private static final long BIT_INVALIDATED = 1L << 63;

    /** Single-settlement bit ensuring breaker outcome and leader commit run at most once. */
    private static final long BIT_SETTLED = 1L << 62;

    /** Bit set once {@link #pendingLoads} is incremented and cleared by the sole releaser. */
    private static final long BIT_BACKLOG = 1L << 61;

    private static final VarHandle STATE;

    static {
      try {
        STATE = MethodHandles.lookup().findVarHandle(InFlight.class, "state", long.class);
      } catch (ReflectiveOperationException e) {
        throw new ExceptionInInitializerError(e);
      }
    }

    private final @Nullable CircuitBreaker breaker;

    /** Thread ID currently executing the leader reader for re-entrancy detection, or {@code 0}. */
    private volatile long runningThreadId;

    /** The submitted executor task, retained so {@link #close()} can cancel a queued load. */
    private volatile @Nullable CompletableFuture<Object> task;

    /** Bit layout: [63]=invalidated | [62]=settled | [61]=backlog | [0..60]=breaker stamp. */
    private volatile long state;

    /**
     * Creates a live flight bound to a circuit breaker and its admitted stamp.
     *
     * @param breaker the owning circuit breaker shard
     * @param stamp   the admitted breaker stamp
     */
    private InFlight(@Nullable CircuitBreaker breaker, long stamp) {
      this.breaker = breaker;
      this.state = stamp & STAMP_MASK;
    }

    /**
     * Creates a sentinel flight carrying no breaker, running thread, or stamp.
     */
    private InFlight() {
      this.breaker = null;
      this.state = 0L;
      this.runningThreadId = 0L;
    }

    /**
     * Records the executor task submitted for this flight so a concurrent {@link #close()} can
     * cancel queued work instead of leaving its waiters blocked.
     *
     * @param submitted the task future returned by the executor submission
     */
    private void attachTask(CompletableFuture<Object> submitted) {
      task = submitted;
    }

    /** Cancels the submitted task if it has not completed yet. */
    private void cancelTask() {
      CompletableFuture<Object> submitted = task;
      if (submitted != null) {
        submitted.cancel(true);
      }
    }

    /**
     * Records that this flight incremented {@link #pendingLoads}. The single bit makes the
     * corresponding decrement idempotent: only the caller that clears the bit during settlement
     * decrements the counter, so racing joiners cannot double-release the same backlog unit.
     */
    private void claimPending() {
      STATE.getAndBitwiseOr(this, BIT_BACKLOG);
    }

    /**
     * Atomically clears the backlog bit at most once.
     *
     * @return {@code true} if this call cleared the bit and must decrement {@link #pendingLoads}
     */
    private boolean tryReleasePending() {
      return ((long) STATE.getAndBitwiseAnd(this, ~BIT_BACKLOG) & BIT_BACKLOG) != 0L;
    }

    /**
     * Returns whether the leader's reader is currently executing on the calling thread.
     *
     * @return {@code true} on a re-entrant call from inside the leader's reader
     */
    private boolean isRunningOnCurrentThread() {
      return runningThreadId == Thread.currentThread().getId();
    }

    /**
     * Returns the circuit breaker shard bound to this flight, or {@code null} for sentinels.
     *
     * @return the owning circuit breaker, or {@code null}
     */
    private @Nullable CircuitBreaker breaker() {
      return breaker;
    }

    /**
     * Returns the 61-bit circuit breaker admission stamp stored in {@link #state}.
     *
     * @return the breaker stamp
     */
    private long stamp() {
      return state & STAMP_MASK;
    }

    /**
     * Returns whether this flight was marked invalidated by a concurrent write-path clear.
     *
     * @return {@code true} if invalidated
     */
    private boolean isInvalidated() {
      return state < 0L;
    }

    /**
     * Atomically claims the single settlement right for this flight so concurrent timeout and
     * completion paths never double-settle the breaker or commit stale data.
     *
     * @return {@code true} if this caller won the settlement CAS
     */
    private boolean trySettle() {
      return ((long) STATE.getAndBitwiseOr(this, BIT_SETTLED) & BIT_SETTLED) == 0L;
    }

    /**
     * Permanently flags this flight as invalidated by a write-path operation. The bit is monotonic:
     * once set, the leader's commit and every joiner's settlement observe it and discard the load
     * result, so a write that raced an in-flight read always wins.
     */
    private void markInvalidated() {
      STATE.getAndBitwiseOr(this, BIT_INVALIDATED);
    }
  }

  /**
   * Outcome of electing a flight for a key.
   *
   * @param flight the elected or joined flight (or a suppression sentinel)
   * @param leader {@code true} only for the caller that inserted {@code flight}
   */
  private record Election(InFlight flight, boolean leader) {}

  /**
   * Stackless unchecked tag wrapping exceptions thrown by the user reader so join-side
   * classification never confuses a reader-thrown {@link TimeoutException} or
   * {@link RejectedExecutionException} with a coordinator suppression.
   *
   * <p>Extends {@link RuntimeException} rather than {@link CompletionException} so
   * {@link CompletableFuture#get()} preserves the tag inside {@link ExecutionException}.
   */
  private static final class ReaderFailure extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Wraps a reader failure without capturing a redundant stack trace.
     *
     * @param cause the exception thrown by the leader reader
     */
    private ReaderFailure(Throwable cause) {
      super(null, cause, false, false);
    }
  }
}
