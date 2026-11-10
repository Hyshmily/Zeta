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
import io.github.hyshmily.zeta.cache.cachesupport.circuitbreaker.CircuitBreaker;
import io.github.hyshmily.zeta.cache.cachesupport.circuitbreaker.CircuitBreakerRegistry;
import io.github.hyshmily.zeta.cache.cachesupport.loader.LoadStatus;
import io.github.hyshmily.zeta.cache.cachesupport.loader.LoaderOutcome;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.InterruptingAsync;
import jakarta.annotation.PreDestroy;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;

/**
 * Default {@link SingleFlight}: one Caffeine table of in-flight elections, a per-prefix
 * breaker registry for admission, and an executor that runs elected readers with an
 * interrupting timeout.
 *
 * <p>The leader owns its whole lifecycle — breaker admission, the reader, the commit and
 * the breaker settlement — and retires its flight <em>before</em> completing it, so a
 * waiter woken by the completion cannot re-elect the settled flight. Followers only
 * join. A write-path {@link #invalidate(String)} marks the flight, so a leader whose
 * value was superseded finishes for its waiters but commits nothing to L1.
 */
@Slf4j
@Internal
public class SingleFlightImpl implements SingleFlight {

  private final CircuitBreakerRegistry breakerRegistry;
  /** Key → the flight currently serving it. */
  private final Cache<String, InFlight> inFlightLoads;
  private final int inFlightMaxSize;
  /** Per-load execution budget; queue wait before the task starts is not counted. */
  private final int timeoutSeconds;
  private final Executor executor;

  /** Readers executing right now — the in-flight gauge. */
  private final AtomicInteger runningLoads = new AtomicInteger();
  /** Loads submitted but not yet settled — the backlog the high-water WARN watches. */
  private final AtomicInteger pendingLoads = new AtomicInteger();
  /** Leader-only reader timeouts, the source-degraded stall signal. */
  private final AtomicLong timeoutCounter = new AtomicLong();
  /** {@code maxSize × 0.8}, floored at 1 so a single-key table never warns. */
  private final long highInFlightThreshold;
  /** Set before the shutdown drain; a load that elects afterwards is suppressed, not queued. */
  private final AtomicBoolean closed = new AtomicBoolean();

  /**
   * @param maxSize         maximum number of dedup keys tracked at once
   * @param ttlSec          retention bound for an unsettled flight; the table expiry is
   *                        raised to at least {@code timeoutSeconds + 1}
   * @param timeoutSeconds  per-reader execution budget
   * @param executor        executor running non-caller-affine readers
   * @param breakerRegistry per-prefix breaker shards
   */
  public SingleFlightImpl(
    int maxSize,
    int ttlSec,
    int timeoutSeconds,
    Executor executor,
    CircuitBreakerRegistry breakerRegistry
  ) {
    Assert.isTrue(maxSize > 0, "maxSize must be > 0, but got: " + maxSize);
    Assert.isTrue(ttlSec > 0, "ttlSec must be > 0, but got: " + ttlSec);
    Assert.isTrue(timeoutSeconds > 0, "timeoutSeconds must be > 0, but got: " + timeoutSeconds);
    Objects.requireNonNull(executor, "executor must not be null");
    Objects.requireNonNull(breakerRegistry, "breakerRegistry must not be null");
    this.executor = executor;
    this.inFlightMaxSize = maxSize;
    this.timeoutSeconds = timeoutSeconds;
    this.breakerRegistry = breakerRegistry;
    this.highInFlightThreshold = Math.max(1L, (long) (maxSize * 0.8));
    // A dedup entry must outlive one load attempt, or the next caller misses it while
    // its leader is still loading and starts a second concurrent load.
    this.inFlightLoads = Caffeine.newBuilder()
      .maximumSize(maxSize)
      .expireAfterWrite(Math.max(ttlSec, timeoutSeconds + 1), TimeUnit.SECONDS)
      .build();
  }

  /** Any-shard view: the read path's degraded-mode gate. */
  @Override
  public boolean isBreakerOpen() {
    return breakerRegistry.isAnyDegraded();
  }

  /** Per-key view: only the key's own shard decides. */
  @Override
  public boolean isBreakerOpen(String cacheKey) {
    return breakerRegistry.isDegraded(cacheKey);
  }

  /** Readers executing right now — not the dedup table's size. */
  @Override
  public long estimatedInFlightSize() {
    return runningLoads.get();
  }

  /** Cumulative leader timeouts — the source-degradation signal. */
  @Override
  public long loadTimeoutCount() {
    return timeoutCounter.get();
  }

  /**
   * One key's dedup entry: the future every waiter joins, the write-path kill
   * flag and the breaker settlement packed into one word. The flight is the
   * future, so a miss allocates one {@code InFlight} and a caller that joins an
   * existing flight allocates only the small election result.
   *
   * <p>The thread executing the leader's reader is recorded for re-entrancy
   * detection only: a same-key call from inside that reader must fail fast
   * instead of joining a future it is itself responsible for completing.
   */
  private static final class InFlight extends CompletableFuture<Object> {

    /** Probe stamps are breaker generations; 61 bits outlast any JVM incarnation. */
    private static final long STAMP_MASK = (1L << 61) - 1;
    /** The write-path kill bit; the sign bit makes {@link #isInvalidated()} one compare. */
    private static final long BIT_INVALIDATED = 1L << 63;
    /** The single settlement bit (leader commit + breaker outcome). */
    private static final long BIT_SETTLED = 1L << 62;
    /** Set once the load has incremented the backlog; cleared by the one releaser. */
    private static final long BIT_BACKLOG = 1L << 61;
    private static final VarHandle STATE;

    static {
      try {
        STATE = MethodHandles.lookup().findVarHandle(InFlight.class, "state", long.class);
      } catch (ReflectiveOperationException e) {
        throw new ExceptionInInitializerError(e);
      }
    }

    private final CircuitBreaker breaker;
    /** The thread running the leader's reader, or {@code 0} when none is running. */
    private volatile long runningThreadId;
    /** [63]: invalidated | [62]: settled | [61]: backlog counted | [0..60]: circuit-breaker stamp. */
    private volatile long state;

    InFlight(CircuitBreaker breaker, long stamp) {
      this.breaker = breaker;
      this.state = stamp & STAMP_MASK;
    }

    /** Sentinel constructor: no breaker, no running reader, no stamp. */
    private InFlight() {
      this.state = 0L;
      this.breaker = null;
      this.runningThreadId = 0L;
    }

    /**
     * Marks this flight's backlog slot as counted. Called after
     * {@code pendingLoads.incrementAndGet()}, so close() can release exactly the
     * slots that were actually counted.
     */
    void claimPending() {
      STATE.getAndBitwiseOr(this, BIT_BACKLOG);
    }

    /**
     * Releases this flight's backlog count once. Returns {@code false} when the
     * slot was never claimed or was already released, so the decrement happens
     * exactly once per counted load.
     */
    boolean tryReleasePending() {
      for (;;) {
        long current = state;
        if ((current & BIT_BACKLOG) == 0L) {
          return false;
        }
        if (STATE.compareAndSet(this, current, current & ~BIT_BACKLOG)) {
          return true;
        }
      }
    }

    /** Whether this flight's reader is executing on the calling thread. */
    boolean isRunningOnCurrentThread() {
      return runningThreadId == Thread.currentThread().getId();
    }

    CircuitBreaker breaker() {
      return breaker;
    }

    long stamp() {
      return state & STAMP_MASK;
    }

    boolean isInvalidated() {
      return state < 0L;
    }

    /**
     * Claims this flight's single settlement: the breaker outcome and the leader
     * commit. A timeout competes with the reader's normal completion, so a result
     * that arrives after the timeout loses this CAS and neither commits to L1 nor
     * counts as a breaker success.
     */
    boolean trySettle() {
      for (;;) {
        long current = state;
        if ((current & BIT_SETTLED) != 0L) {
          return false;
        }
        if (STATE.compareAndSet(this, current, current | BIT_SETTLED)) {
          return true;
        }
      }
    }

    /** Set only by {@link #invalidate}/{@link #invalidateAll}; never cleared. */
    void markInvalidated() {
      STATE.getAndBitwiseOr(this, BIT_INVALIDATED);
    }
  }

  /** The executor refused the reader: the flight was aborted. */
  private static final InFlight REJECTED = new InFlight();
  /** The breaker refused the load: no flight exists. */
  private static final InFlight DENIED = new InFlight();
  /** The dedup layer is closed: no new flight may be elected. */
  private static final InFlight CLOSED = new InFlight();

  /**
   * One election result. {@code leader} is true only for the call that inserted
   * the flight, so a re-entrant same-thread call that reads the same flight is a
   * follower, not a second leader.
   */
  private record Election(InFlight flight, boolean leader) {}

  /**
   * Load one value: elect a leader outside any cache lock, run its reader (on this
   * thread under {@code callerRuns}, otherwise on the executor), then classify this
   * caller's role. A follower joins the flight's shared future; an inline leader
   * returns its outcome directly.
   *
   * @param cacheKey     the key to load
   * @param reader       the value supplier, run at most once per election
   * @param callerRuns   {@code true} keeps the reader on the calling thread
   * @param leaderCommit stores the leader's value before any waiter wakes (or registers
   *                     that store for the caller transaction's afterCommit)
   * @param <T>          the value type
   * @return the explicit outcome; reader failures surface as {@code FAILED}
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
    // close() may have drained and failed the flight between the election and this line; the
    // join below then reports the rejection instead of running a reader nobody waits for.
    if (election.leader() && !flight.isDone()) {
      if (callerRuns) {
        return runInline(cacheKey, flight, reader, leaderCommit);
      }
      flight = submit(cacheKey, flight, reader, leaderCommit);
    }
    return settle(cacheKey, flight);
  }

  /**
   * Runs the reader on the calling thread without electing a flight. Used by a caller-affine
   * transactional load: followers must never observe a value that may still roll back, and a
   * concurrent caller's own load is cheaper than a shared future that has to be withheld.
   *
   * <p>Breaker admission still applies, so a rejected probe maps to
   * {@link LoadStatus#SUPPRESSED_BREAKER} and the read path can serve its stale fallback.
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
    try {
      T value = reader.get();
      if (value != null) {
        leaderCommit.accept(value);
      }
      breaker.onSuccess(stamp);
      return value == null ? LoaderOutcome.sourceEmpty() : LoaderOutcome.loaded(value);
    } catch (Throwable t) {
      breaker.onFailure(causeOf(t), stamp);
      if (t instanceof Error err) {
        throw err;
      }
      return LoaderOutcome.failed(causeOf(t));
    } finally {
      runningLoads.decrementAndGet();
    }
  }

  /**
   * Batch form: elect and submit every distinct key before joining any, so a batch of
   * misses overlaps its loads. A duplicated key reuses its first election and outcome.
   *
   * @param cacheKeys    the keys to load
   * @param reader       the per-key value function
   * @param leaderCommit the per-key leader commit
   * @param <T>          the value type
   * @return key → outcome, in first-seen order
   */
  @Override
  public <T> Map<String, LoaderOutcome<T>> load(
    Iterable<String> cacheKeys,
    Function<? super String, ? extends T> reader,
    BiConsumer<String, ? super T> leaderCommit
  ) {
    // Elect (and submit) every key before joining any: a batch of misses must load in
    // parallel. A duplicated key reuses its first election.
    Map<String, InFlight> elections = new LinkedHashMap<>(mapCapacity(cacheKeys));
    for (String key : cacheKeys) {
      if (elections.containsKey(key)) {
        continue;
      }
      Election election = elect(key);
      InFlight flight = election.flight();
      if (election.leader() && !flight.isDone()) {
        flight = submit(key, flight, () -> reader.apply(key), v -> leaderCommit.accept(key, v));
      }
      elections.put(key, flight);
    }
    Map<String, LoaderOutcome<T>> outcomes = new LinkedHashMap<>(mapCapacity(elections.size()));
    for (Map.Entry<String, InFlight> entry : elections.entrySet()) {
      outcomes.put(entry.getKey(), settle(entry.getKey(), entry.getValue()));
    }
    return outcomes;
  }

  /**
   * Initial capacity that holds {@code expectedEntries} without an intermediate
   * rehash at the default 0.75 load factor.
   *
   * @param expectedEntries the number of entries the map must hold
   * @return the constructor capacity
   */
  private static int mapCapacity(int expectedEntries) {
    return (int) (expectedEntries / 0.75f) + 1;
  }

  /**
   * The batch-map capacity for a key source: its exact size when the source can
   * report one, otherwise the {@link LinkedHashMap} default.
   *
   * @param cacheKeys the keys the batch will visit
   * @return the constructor capacity
   */
  private static int mapCapacity(Iterable<String> cacheKeys) {
    return cacheKeys instanceof Collection<?> c ? mapCapacity(c.size()) : 16;
  }

  /**
   * Elect a leader for the key, or join the flight already serving it.
   *
   * <p>The fast path answers from a lock-free {@code getIfPresent}: an in-flight
   * flight is the overwhelmingly common case on a hot key, and the mapping
   * function below would have observed the same reference and run nothing.
   *
   * <p>The slow path admits under the breaker <em>inside</em> the map's atomic insertion,
   * so a loser never consumes a probe; only the placeholder is created under the bin lock
   * — the reader is submitted afterwards.
   */
  private Election elect(String key) {
    if (closed.get()) {
      return new Election(CLOSED, false);
    }
    InFlight reusable = inFlightLoads.getIfPresent(key);
    if (reusable != null) {
      // Followers short-circuit before the breaker lookup: the hot-key contention
      // path is mostly followers, and they never consult the breaker.
      return new Election(reusable, false);
    }
    CircuitBreaker breaker = breakerRegistry.breakerFor(key);

    // The flight is created only after breaker admission, inside the atomic
    // insertion: a rejected load allocates nothing beyond the small holder. The
    // holder publishes the created instance back to this call; identity with the
    // returned flight is the election result, so a loser is a follower without a
    // second atomic read-modify-write.
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
      // The mapping stored nothing, so a probe it took before failing must not stay
      // reserved in HALF_OPEN.
      InFlight candidate = created[0];
      if (candidate != null) {
        breaker.onAbandoned(candidate.stamp());
      }
      throw e;
    }
    boolean leader = flight != null && flight == created[0];
    if (leader) {
      if (closed.get()) {
        // close() may have drained the table before this insertion became visible, and a
        // concurrent invalidate(key) may have removed the flight in the same window. Settle and
        // fail the flight unconditionally: a withdrawal gated on "the removal won" would leak the
        // breaker probe and leave a follower that already joined waiting forever.
        inFlightLoads.asMap().remove(key, flight);
        if (flight.trySettle() && flight.breaker() != null) {
          flight.breaker().onAbandoned(flight.stamp());
        }
        flight.completeExceptionally(new RejectedExecutionException("SingleFlight closed"));
        return new Election(CLOSED, false);
      }
      logHighBacklog();
    }
    return flight == null ? new Election(DENIED, false) : new Election(flight, leader);
  }

  /**
   * Submit the elected leader's reader to the executor, wrapped so that it commits the
   * value (unless the load was superseded) and settles the breaker on the thread that
   * actually performed the downstream call.
   */
  private <T> InFlight submit(String key, InFlight flight, Supplier<T> reader, Consumer<? super T> commit) {
    CompletableFuture<Object> submitted;
    try {
      submitted = submitReader(flight, () -> executeLeaderTask(flight, reader, commit));
    } catch (RuntimeException | Error e) {
      abortLeaderElection(key, flight, e);
      if (e instanceof RejectedExecutionException) {
        return REJECTED;
      }
      throw e;
    }
    submitted.whenComplete((result, error) -> finishFlight(key, flight, result, error));
    return flight;
  }

  /**
   * The leader's reader execution and settlement, shared by the inline and submitted
   * paths. The flight is settled exactly once; the leader commits only when it owns
   * that settlement and the value was not superseded.
   *
   * <p>The breaker is settled as soon as the reader returns, before the local commit.
   * The commit runs the admission predicate and the TTL encoding, none of which
   * passed through the data source: counting a commit failure as a source failure
   * would fail a HALF_OPEN probe (reopening the breaker) or trip it on a bad
   * {@code isCacheable} decision.
   */
  private <T> T executeLeaderTask(InFlight flight, Supplier<T> reader, Consumer<? super T> commit) {
    runningLoads.incrementAndGet();
    flight.runningThreadId = Thread.currentThread().getId();
    try {
      T value = reader.get();
      if (flight.trySettle()) {
        flight.breaker().onSuccess(flight.stamp());
        if (value != null && !flight.isInvalidated()) {
          commit.accept(value);
        }
      }
      return value;
    } catch (Throwable t) {
      // A reader failure settles only if no external timeout won the flight first. A commit
      // failure finds the flight already settled here, so the source breaker stays untouched.
      if (flight.trySettle()) {
        flight.breaker().onFailure(causeOf(t), flight.stamp());
      }
      if (t instanceof Error err) {
        throw err;
      }
      // Tag every reader failure, including a RuntimeException such as a nested
      // RejectedExecutionException: only the dedup layer's own rejection and timeout
      // stay bare, so the join-side classifier can tell a failed reader from a
      // suppressed submission (design decisions: executors).
      throw new ReaderFailure(t);
    } finally {
      flight.runningThreadId = 0L;
      runningLoads.decrementAndGet();
    }
  }

  /**
   * Retires the flight, completes its future and, on failure, settles any outcome the
   * executor's interrupting timeout did not. Retirement precedes completion, so a woken
   * waiter never re-elects a settled flight.
   */
  private void finishFlight(String key, InFlight flight, Object result, Throwable error) {
    boolean retired = inFlightLoads.asMap().remove(key, flight);
    // Decrement on settlement, not on return: a task queued behind a saturated
    // executor must stay visible to the backlog WARN for its whole life. The
    // shutdown drain may have released it first.
    if (flight.tryReleasePending()) {
      pendingLoads.decrementAndGet();
    }
    if (error == null) {
      flight.complete(result);
      return;
    }

    Throwable cause = causeOf(error);
    // The interrupting timeout completes the future exceptionally without the reader
    // seeing the TimeoutException, so settle the breaker here: a reader that ignores the
    // interrupt must not keep a failed flight out of the window.
    if (flight.trySettle()) {
      flight.breaker().onFailure(cause, flight.stamp());
    }
    // Only the dedup layer's own budget timeout stays bare; a reader-thrown TimeoutException
    // was wrapped by executeLeaderTask and is a supplier failure, not a leader timeout.
    // Count that timeout once, on the leader that actually ran the call.
    if (error instanceof TimeoutException) {
      timeoutCounter.incrementAndGet();
    }
    flight.completeExceptionally(error);
    // A budget timeout is an operational suppression the caller logs with its fallback
    // decision; only a reader failure needs the owner-side stack here.
    if (retired && !(error instanceof TimeoutException)) {
      log.warn("singleflight join failed: key={}", key, error);
    }
  }

  /**
   * Run the leader's reader on this thread — the caller-affine path, where the reader
   * must observe the caller's transaction, security context and MDC. No timeout: a
   * timeout would interrupt the caller's own work and there is no pool slot to release.
   */
  private <T> LoaderOutcome<T> runInline(String key, InFlight flight, Supplier<T> reader, Consumer<? super T> commit) {
    pendingLoads.incrementAndGet();
    flight.claimPending();
    T value;
    try {
      value = executeLeaderTask(flight, reader, commit);
    } catch (Throwable t) {
      CompletionException wrapped = t instanceof CompletionException ce ? ce : new CompletionException(t);
      if (causeOf(wrapped) instanceof InterruptedException) {
        // The only thread allowed to restore the flag: this one cleared it.
        Thread.currentThread().interrupt();
      }
      finishFlight(key, flight, null, wrapped);
      return failureOutcome(wrapped);
    }
    finishFlight(key, flight, value, null);
    return value == null ? LoaderOutcome.sourceEmpty() : LoaderOutcome.loaded(value);
  }

  /** Join a flight and classify the result. Never throws for a reader failure. */
  @SuppressWarnings("unchecked")
  private <T> LoaderOutcome<T> settle(String key, InFlight flight) {
    if (flight == DENIED || flight == REJECTED || flight == CLOSED) {
      return suppressedOutcome(key, flight);
    }
    if (flight.isRunningOnCurrentThread()) {
      throw new IllegalStateException("re-entrant SingleFlight load for key: " + key);
    }
    try {
      T result = (T) flight.join();
      return result == null ? LoaderOutcome.sourceEmpty() : LoaderOutcome.loaded(result);
    } catch (CompletionException | CancellationException e) {
      return failureOutcome(e);
    }
  }

  /** Map a denial/rejection to its explicit outcome. */
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
   * Classify a failed join. The leader already retired the flight before completing the
   * future, so a retry re-runs the reader; the failure WARN is owner-side too, which keeps
   * one line per failed flight instead of one per woken waiter.
   *
   * @param error the reader or timeout failure carried by the flight
   * @return the classified outcome
   */
  private <T> LoaderOutcome<T> failureOutcome(Throwable error) {
    // Unwrap only the join envelope: a reader failure is tagged by executeLeaderTask
    // (ReaderFailure), while the dedup layer's own timeout and submission rejection stay
    // bare. Classifying the fully unwrapped cause would turn a reader-thrown
    // RejectedExecutionException into a suppression the reader never saw.
    Throwable failure = error;
    if (!(failure instanceof ReaderFailure) && failure instanceof CompletionException && failure.getCause() != null) {
      failure = failure.getCause();
    }
    if (failure instanceof ReaderFailure readerFailure) {
      return LoaderOutcome.failed(causeOf(readerFailure));
    }
    if (failure instanceof RejectedExecutionException) {
      // A follower joined the leader's flight before the executor refused it;
      // it must resolve to the same suppressed outcome as the leader.
      return LoaderOutcome.suppressed(LoadStatus.SUPPRESSED_EXECUTOR);
    }
    return failure instanceof TimeoutException
      ? LoaderOutcome.suppressed(LoadStatus.SUPPRESSED_TIMEOUT)
      : LoaderOutcome.failed(causeOf(failure));
  }

  /** A reader failure, tagged so it is never mistaken for a suppressed submission. */
  private static final class ReaderFailure extends CompletionException {

    ReaderFailure(Throwable cause) {
      super(cause);
    }
  }

  /**
   * Fail every flight still in the table so a waiter cannot outlive the context.
   *
   * <p>Spring destroys this bean before {@code interruptibleLoadExecutor}: the
   * executor's {@code shutdownNow} drains queued tasks without completing their
   * futures, so without this drain a caller already joining one of those flights
   * blocks forever. The task that never ran is classified as
   * {@code SUPPRESSED_EXECUTOR}, the same outcome a rejected submission gets.
   */
  @PreDestroy
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    for (String key : List.copyOf(inFlightLoads.asMap().keySet())) {
      InFlight flight = inFlightLoads.asMap().remove(key);
      if (flight == null) {
        continue;
      }
      if (flight.tryReleasePending()) {
        pendingLoads.decrementAndGet();
      }
      if (flight.trySettle() && flight.breaker() != null) {
        flight.breaker().onAbandoned(flight.stamp());
      }
      flight.completeExceptionally(new RejectedExecutionException("SingleFlight closed"));
    }
  }

  /** The real cause behind transport envelopes ({@code CompletionException}/{@code ExecutionException}). */
  private static Throwable causeOf(Throwable error) {
    Throwable current = error;
    while (
      (current instanceof CompletionException || current instanceof ExecutionException) && current.getCause() != null
    ) {
      current = current.getCause();
    }
    return current;
  }

  /**
   * Reports a newly elected leader that takes the backlog over its threshold. Runs
   * after the dedup insertion, outside the Caffeine bin lock, and before this load's
   * own submission is counted, so the reading excludes it.
   */
  private void logHighBacklog() {
    long backlogAfterThisLoad = pendingLoads.get() + 1;
    if (backlogAfterThisLoad > highInFlightThreshold) {
      log.debug("SingleFlight in-flight queue is high: {}/{}", backlogAfterThisLoad, inFlightMaxSize);
    }
  }

  /**
   * Undo a leader election that never reached the executor: retire the placeholder,
   * release the breaker probe it reserved, and fail any waiter that joined it.
   */
  private void abortLeaderElection(String key, InFlight flight, Throwable cause) {
    inFlightLoads.asMap().remove(key, flight);
    // The shutdown drain may have settled this flight first; release the probe
    // exactly once.
    if (flight.trySettle() && flight.breaker() != null) {
      flight.breaker().onAbandoned(flight.stamp());
    }
    flight.completeExceptionally(cause);
  }

  /**
   * Submit a reader with an interrupting timeout: the budget is armed when a pool thread
   * starts the task (queue wait is free), and expiry interrupts that thread so a wedged
   * call cannot shrink the pool one batch at a time.
   *
   * <p>The caller's completion callback owns the backlog decrement and the flight
   * retirement — one callback per load, not one here plus one at the caller.
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
      // Mark after a successful submission: a failed submit decrements below, and
      // close() must not release a slot this load never held.
      flight.claimPending();
      return submitted;
    } catch (RuntimeException | Error e) {
      pendingLoads.decrementAndGet();
      throw e;
    }
  }

  /**
   * Drop the key's flight and mark it superseded: the leader still completes for its
   * waiters but commits nothing, and a later caller starts a fresh load.
   *
   * <p>The removal and the flag are separate on purpose — the flag must reach a
   * flight a leader already holds, which removing the map entry cannot do.
   */
  @Override
  public void invalidate(String cacheKey) {
    InFlight flight = inFlightLoads.asMap().remove(cacheKey);
    if (flight != null) {
      flight.markInvalidated();
    }
  }

  /** Whole-table kill: flag every live flight first, then drop the table. */
  @Override
  public void invalidateAll() {
    inFlightLoads.asMap().values().forEach(InFlight::markInvalidated);
    inFlightLoads.invalidateAll();
  }

  /**
   * Scoped kill: same contract as {@link #invalidate(String)} for every flight under the
   * prefix. Weakly consistent, like the cache sweep that calls it: a flight inserted after
   * the iteration starts belongs to a read that began after the clear and is left alone.
   */
  @Override
  public void invalidatePrefix(String prefix) {
    for (String key : inFlightLoads.asMap().keySet()) {
      if (key.startsWith(prefix)) {
        invalidate(key);
      }
    }
  }
}
