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
package io.github.hyshmily.zeta.reporting.impl;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.github.hyshmily.zeta.reporting.CadenceTunable;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.reporting.ReportMessage;
import io.github.hyshmily.zeta.reporting.ReportPublisher;
import io.github.hyshmily.zeta.sharding.ConsistentHashRing;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.util.CollectionSizing;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.executor.ZetaThreadFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * Publishes per-key access-count batches to Workers via {@link ReportPublisher}.
 *
 * <p>Each reporting cycle executes in three stages:
 * <ol>
 *   <li><b>Admission</b> on the caller thread: validates lifecycle state, reconciles the hash ring
 *       once, and snapshots alive Worker epochs.</li>
 *   <li><b>Routing</b> on the caller thread: shards counts per target Worker and offers batches
 *       under a single shared queue-offer budget.</li>
 *   <li><b>Publishing</b> on dedicated consumer threads: sends one shard batch at a time with a
 *       watchdog-driven interrupting timeout.</li>
 * </ol>
 */
@Slf4j
@Internal
public class KeyReporterImpl implements KeyReporter, CadenceTunable {

  private final ReportPublisher reportPublisher;
  private final ConsistentHashRing ring;
  private final HealthView healthView;
  private final String appName;
  private final int queueCapacity;
  private final int queueOfferTimeoutMs;
  private final int consumerCount;

  /** Optional flush-cadence tuner; {@code null} keeps a fixed reporting interval. */
  private final @Nullable ReportFeedLoop intervalFeedLoop;

  /** Callback through which the cadence tuner updates the upstream tide interval. */
  private volatile @Nullable Consumer<Long> cadenceAdjuster;

  /** Keys dropped because no routable target with a captured epoch remained. */
  private final AtomicLong unroutableDropCounter = new AtomicLong();

  /** Keys dropped because no alive Worker with a valid epoch was available. */
  private final AtomicLong workerDeadDropCounter = new AtomicLong();

  /** Keys dropped because the dispatcher queue was full or shutting down. */
  private final AtomicLong droppedCounter = new AtomicLong();

  private final AtomicReference<State> state = new AtomicReference<>(State.NEW);

  /** Active dispatcher instance created on {@link #start()} and shut down on {@link #stop()}. */
  private volatile @Nullable Dispatcher dispatcher;

  /**
   * Creates a reporter with a fixed flush cadence.
   *
   * @param reportPublisher     publisher used to send report messages
   * @param appName             application instance name stamped on every message
   * @param queueCapacity       bounded dispatcher work-queue capacity
   * @param queueOfferTimeoutMs shared whole-flush offer timeout budget in milliseconds
   * @param consumerCount       number of consumer threads draining the dispatcher queue
   * @param ring                consistent-hash ring for Worker target routing
   * @param healthView          cluster health view supplying liveness and Worker epochs
   * @throws IllegalArgumentException if capacity/consumers are non-positive or timeout is negative
   * @throws NullPointerException     if any required collaborator is {@code null}
   */
  public KeyReporterImpl(
    ReportPublisher reportPublisher,
    String appName,
    int queueCapacity,
    int queueOfferTimeoutMs,
    int consumerCount,
    ConsistentHashRing ring,
    HealthView healthView
  ) {
    this(reportPublisher, appName, queueCapacity, queueOfferTimeoutMs, consumerCount, ring, healthView, null);
  }

  /**
   * Creates a reporter with an optional adaptive flush-cadence tuner.
   *
   * @param reportPublisher     publisher used to send report messages
   * @param appName             application instance name stamped on every message
   * @param queueCapacity       bounded dispatcher work-queue capacity
   * @param queueOfferTimeoutMs shared whole-flush offer timeout budget in milliseconds
   * @param consumerCount       number of consumer threads draining the dispatcher queue
   * @param ring                consistent-hash ring for Worker target routing
   * @param healthView          cluster health view supplying liveness and Worker epochs
   * @param intervalFeedLoop    optional flush-cadence tuner, or {@code null} for fixed cadence
   * @throws IllegalArgumentException if capacity/consumers are non-positive or timeout is negative
   * @throws NullPointerException     if any required collaborator is {@code null}
   */
  public KeyReporterImpl(
    ReportPublisher reportPublisher,
    String appName,
    int queueCapacity,
    int queueOfferTimeoutMs,
    int consumerCount,
    ConsistentHashRing ring,
    HealthView healthView,
    @Nullable ReportFeedLoop intervalFeedLoop
  ) {
    this.reportPublisher = Objects.requireNonNull(reportPublisher, "reportPublisher must not be null");
    this.appName = Objects.requireNonNull(appName, "appName must not be null");
    this.ring = Objects.requireNonNull(ring, "ring must not be null");
    this.healthView = Objects.requireNonNull(healthView, "healthView must not be null");
    if (queueCapacity <= 0) {
      throw new IllegalArgumentException("queueCapacity must be > 0, but got: " + queueCapacity);
    }
    if (queueOfferTimeoutMs < 0) {
      throw new IllegalArgumentException("queueOfferTimeoutMs must be >= 0, but got: " + queueOfferTimeoutMs);
    }
    if (consumerCount <= 0) {
      throw new IllegalArgumentException("consumerCount must be > 0, but got: " + consumerCount);
    }
    this.queueCapacity = queueCapacity;
    this.queueOfferTimeoutMs = queueOfferTimeoutMs;
    this.consumerCount = consumerCount;
    this.intervalFeedLoop = intervalFeedLoop;
  }

  /**
   * Registers the callback used by {@link #intervalFeedLoop} to adjust the tide interval.
   *
   * @param adjuster callback accepting the computed interval in milliseconds, or {@code null}
   */
  @Override
  public void setCadenceAdjuster(@Nullable Consumer<Long> adjuster) {
    this.cadenceAdjuster = adjuster;
  }

  /**
   * Starts the reporter and its background {@link Dispatcher}. Safe to invoke repeatedly; a
   * previously failed start may be retried, while a stopped reporter ignores subsequent starts.
   */
  @Override
  public void start() {
    for (;;) {
      State current = state.get();
      if (current == State.RUNNING || current == State.STARTING) {
        log.debug("KeyReporterImpl already started, skip");
        return;
      }
      if (current == State.STOPPED) {
        log.debug("KeyReporterImpl is stopped; start() ignored");
        return;
      }
      if (state.compareAndSet(current, State.STARTING)) {
        break;
      }
    }

    Dispatcher created = null;
    try {
      created = new Dispatcher();
      created.start();
      // Publish before transitioning to RUNNING so a concurrent stop() can observe and close it.
      dispatcher = created;
      if (!state.compareAndSet(State.STARTING, State.RUNNING)) {
        dispatcher = null;
        created.shutdown();
        return;
      }
      ReportFeedLoop feedLoop = intervalFeedLoop;
      log.info(
        "KeyReporterImpl started: appName={}, cadence={}, queueCapacity={}, consumers={}",
        appName,
        feedLoop == null ? "fixed" : "feed-loop/" + feedLoop.mode() + " seed=" + feedLoop.intervalMs() + "ms",
        queueCapacity,
        consumerCount
      );
    } catch (Throwable t) {
      dispatcher = null;
      if (created != null) {
        created.shutdown();
      }
      // Preserve STOPPED if stop() raced ahead of this failure.
      state.compareAndSet(State.STARTING, State.FAILED);
      log.error(
        "Failed to start KeyReporterImpl; per-key counts will not be flushed to Worker. " +
          "Application continues but Worker hot-key detection will be blind to this instance. " +
          "start() may be retried.",
        t
      );
    }
  }

  /**
   * Permanently stops the reporter and shuts down its active {@link Dispatcher}.
   */
  @Override
  public void stop() {
    State previous = state.getAndSet(State.STOPPED);
    if (previous == State.STOPPED) {
      return;
    }
    Dispatcher dispatcherToStop = dispatcher;
    if (dispatcherToStop != null) {
      dispatcherToStop.shutdown();
    }
  }

  /**
   * Publishes one tide batch of key counts, feeding both empty and admitted non-empty windows into
   * the optional cadence tuner before routing to Workers.
   *
   * @param counts   map of key counts accumulated over {@code windowMs} (may be empty)
   * @param windowMs measured monotonic accumulation window in milliseconds
   */
  @Override
  public void publishBatch(Map<String, Long> counts, long windowMs) {
    if (state.get() != State.RUNNING) {
      return;
    }
    try {
      if (counts.isEmpty()) {
        // Sample idle windows at rate zero so the cadence tuner can back off.
        sampleFeedLoop(0, windowMs);
        return;
      }
      Admission admission = admit(counts);
      if (admission == null) {
        return;
      }
      sampleFeedLoop(counts.size(), windowMs);
      routeAndEnqueue(counts, admission);
    } catch (Exception e) {
      log.error("Flush callback failed", e);
    }
  }

  /**
   * Publishes a batch directly without sampling the cadence tuner.
   *
   * @param counts map of key counts to route and enqueue
   */
  @Override
  public void publishDirect(Map<String, Long> counts) {
    try {
      Admission admission = admit(counts);
      if (admission != null) {
        routeAndEnqueue(counts, admission);
      }
    } catch (Exception e) {
      log.error("Direct report failed", e);
    }
  }

  /**
   * Returns a snapshot of dispatcher queue and consumer metrics, or {@code null} before start.
   *
   * @return current {@link DispatcherStats}, or {@code null} if not started
   */
  @Override
  public @Nullable DispatcherStats dispatcherStats() {
    Dispatcher current = dispatcher;
    return current == null
      ? null
      : new DispatcherStats(
          current.depth(),
          current.expiredDeadTarget(),
          current.expiredStale(),
          current.publishFailed(),
          current.dropped()
        );
  }

  /**
   * Returns a snapshot of routing-stage drop counters, or {@code null} before start.
   *
   * @return current {@link RoutingStats}, or {@code null} if not started
   */
  @Override
  public @Nullable RoutingStats routingStats() {
    if (dispatcher == null) {
      return null;
    }
    return new RoutingStats(unroutableDropCounter.get(), workerDeadDropCounter.get(), droppedCounter.get());
  }

  /**
   * Returns the configured adaptive cadence tuner, or {@code null} if fixed cadence is used.
   *
   * @return the {@link ReportFeedLoop}, or {@code null}
   */
  @Override
  public @Nullable ReportFeedLoop feedLoop() {
    return intervalFeedLoop;
  }

  /**
   * Performs per-flush admission: validates non-empty running state, reconciles the hash ring once,
   * and captures current epochs for all alive Workers.
   *
   * @param counts the flush key counts
   * @return the captured {@link Admission}, or {@code null} if the flush must be dropped
   */
  private @Nullable Admission admit(Map<String, Long> counts) {
    if (counts.isEmpty() || state.get() != State.RUNNING) {
      return null;
    }

    Set<String> aliveNodes = ring.rebuildIfPossible(healthView);
    if (aliveNodes.isEmpty()) {
      dropUnroutable(counts, "No alive Worker nodes for routing");
      return null;
    }

    Map<String, Long> epochs = new HashMap<>(CollectionSizing.capacity(aliveNodes.size()));
    for (String node : aliveNodes) {
      long epoch = healthView.epochOf(node);
      if (epoch != HealthView.UNKNOWN_EPOCH && healthView.isCurrentIncarnation(node, epoch)) {
        epochs.put(node, epoch);
      }
    }
    if (epochs.isEmpty()) {
      dropUnroutable(counts, "No Worker node has a routable epoch");
      return null;
    }
    return new Admission(TimeSource.currentTimeMillis(), Map.copyOf(epochs));
  }

  /**
   * Accounts for a dropped flush when no routable Worker exists and logs at DEBUG level.
   *
   * @param counts the dropped key counts
   * @param reason operator-facing drop reason
   */
  private void dropUnroutable(Map<String, Long> counts, String reason) {
    long total = workerDeadDropCounter.addAndGet(counts.size());
    log.debug("{}; dropping {} keys in this flush (cumulative: {})", reason, counts.size(), total);
  }

  /**
   * Samples one completed flush into {@link #intervalFeedLoop} and applies the adjusted tide
   * interval when operating in {@link ReportFeedLoop.Mode#ON}.
   *
   * @param batchSize number of distinct keys in the flush
   * @param windowMs  measured monotonic window in milliseconds
   */
  private void sampleFeedLoop(int batchSize, long windowMs) {
    ReportFeedLoop feedLoop = intervalFeedLoop;
    if (feedLoop == null || windowMs <= 0L) {
      return;
    }
    try {
      ReportFeedLoop.State feedState = feedLoop.onFlushCompleted(batchSize, windowMs);
      Consumer<Long> adjuster = cadenceAdjuster;
      if (feedLoop.mode() == ReportFeedLoop.Mode.ON && adjuster != null) {
        adjuster.accept(feedState.intervalMs());
      }
    } catch (RuntimeException e) {
      log.debug("feed-loop sample failed; flush proceeds at the unchanged cadence", e);
    }
  }

  /**
   * Shards {@code counts} across admitted Worker targets and offers each shard batch to the active
   * {@link Dispatcher}.
   *
   * @param counts    the flush key counts
   * @param admission the captured routing timestamp and target epochs
   */
  private void routeAndEnqueue(Map<String, Long> counts, Admission admission) {
    Dispatcher current = dispatcher;
    if (current == null) {
      return;
    }
    Map<String, Map<String, Long>> sharded = shardCounts(counts, admission.epochs());
    enqueueShards(current, sharded, admission);
  }

  /**
   * Consistent-hashes each valid key in {@code counts} onto a target Worker present in
   * {@code epochs}, falling back to predicate-filtered lookup when a concurrent ring rebuild moves
   * a key onto a node without a captured epoch.
   *
   * @param counts the flush key counts
   * @param epochs map of admitted Worker IDs to their captured epochs
   * @return map from target Worker ID to its per-key counts
   */
  private Map<String, Map<String, Long>> shardCounts(Map<String, Long> counts, Map<String, Long> epochs) {
    Predicate<String> admittedTarget = epochs::containsKey;
    Map<String, Map<String, Long>> sharded = new HashMap<>(CollectionSizing.capacity(epochs.size()));
    int perShardCapacity = CollectionSizing.capacity(Math.max(1, counts.size() / Math.max(1, epochs.size())));
    for (Map.Entry<String, Long> entry : counts.entrySet()) {
      String key = entry.getKey();
      Long count = entry.getValue();
      if (key == null || count == null || count <= 0L) {
        continue;
      }

      String target;
      try {
        target = ring.locateNode(key);
        if (target == null || !epochs.containsKey(target)) {
          target = ring.locateNode(key, admittedTarget);
        }
        if (target == null) {
          noteUnroutable(key, 1, "locateNode returned null");
          continue;
        }
      } catch (RuntimeException e) {
        noteUnroutable(key, 1, "locateNode failed: " + e.getMessage());
        continue;
      }

      // A plain get/put avoids a capturing lambda per entry; the per-shard capacity is shared.
      Map<String, Long> bucket = sharded.get(target);
      if (bucket == null) {
        bucket = new HashMap<>(perShardCapacity);
        sharded.put(target, bucket);
      }
      bucket.put(key, count);
    }
    return sharded;
  }

  /**
   * Offers all sharded batches to {@code current} under one shared whole-flush timeout budget,
   * degrading to non-blocking offers once the budget is exhausted.
   *
   * @param current   the active dispatcher
   * @param sharded   per-target key-count maps
   * @param admission the captured flush timestamp and Worker epochs
   */
  private void enqueueShards(Dispatcher current, Map<String, Map<String, Long>> sharded, Admission admission) {
    long timestamp = admission.timestamp();
    long enqueuedAt = TimeSource.monotonicMillis();
    boolean hasBudget = queueOfferTimeoutMs > 0;
    long deadlineNanos = hasBudget ? System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(queueOfferTimeoutMs) : 0L;

    for (Map.Entry<String, Map<String, Long>> entry : sharded.entrySet()) {
      String target = entry.getKey();
      Map<String, Long> countsForTarget = entry.getValue();
      Long targetEpoch = admission.epochs().get(target);
      if (targetEpoch == null) {
        noteUnroutable(target, countsForTarget.size(), "no epoch for routed target");
        continue;
      }

      long remainingNanos = hasBudget ? Math.max(0L, deadlineNanos - System.nanoTime()) : 0L;
      ShardBatch batch = new ShardBatch(target, targetEpoch, timestamp, countsForTarget, enqueuedAt);
      if (!current.enqueue(batch, remainingNanos)) {
        long dropped = droppedCounter.addAndGet(countsForTarget.size());
        log.debug(
          "report queue full, dropped target={} keys={}, depth={}/{}, cumulativeDrops={}",
          target,
          countsForTarget.size(),
          current.depth(),
          queueCapacity,
          dropped
        );
      }
    }
  }

  /**
   * Increments {@link #unroutableDropCounter} and logs the unroutable key or target at DEBUG level.
   *
   * @param key    the key or target identifier
   * @param count  number of keys dropped
   * @param reason diagnostic reason
   */
  private void noteUnroutable(String key, int count, String reason) {
    long total = unroutableDropCounter.addAndGet(count);
    log.debug("{} for key={}, cumulativeUnroutable={}", reason, key, total);
  }

  /**
   * Reporter lifecycle state; {@link #FAILED} may retry {@link #start()}, whereas {@link #STOPPED}
   * is terminal.
   */
  private enum State {
    NEW,
    STARTING,
    RUNNING,
    FAILED,
    STOPPED,
  }

  /**
   * Per-flush admission snapshot capturing the wire wall-clock timestamp and target Worker epochs.
   *
   * @param timestamp wall-clock milliseconds stamped on every shard batch
   * @param epochs    immutable map from admitted Worker node ID to captured epoch
   */
  private record Admission(long timestamp, Map<String, Long> epochs) {}

  /**
   * Batch of per-key counts destined for one Worker target.
   *
   * @param target      the target Worker node ID
   * @param targetEpoch the target Worker's epoch captured at routing time
   * @param timestamp   wall-clock milliseconds when the batch was assembled (sent on the wire)
   * @param counts      positive per-key access counts accumulated since the previous flush
   * @param enqueuedAt  monotonic milliseconds at enqueue time for staleness checks
   */
  record ShardBatch(String target, long targetEpoch, long timestamp, Map<String, Long> counts, long enqueuedAt) {}

  /**
   * Bounded work queue, consumer pool, and watchdog timer executing synchronous report publishes.
   */
  final class Dispatcher {

    private static final long PUBLISH_TIMEOUT_MS = 5_000L;
    private static final long WATCHDOG_SWEEP_MS = 100L;
    private static final long STALE_BATCH_MS = 5_000L;
    private static final long SHUTDOWN_AWAIT_SECONDS = 2L;

    private final BlockingQueue<ShardBatch> queue = new LinkedBlockingQueue<>(queueCapacity);
    private final AtomicLong expiredDeadTargetCount = new AtomicLong();
    private final AtomicLong expiredStaleCount = new AtomicLong();
    private final AtomicLong droppedCount = new AtomicLong();
    private final AtomicLong publishFailedCount = new AtomicLong();
    private final ScheduledExecutorService publishTimer;
    private final ConsumerWatchdogSlot[] watchdogs;
    private final ExecutorService consumers;
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile boolean running;

    /**
     * Allocates the consumer thread pool, watchdog slots, and single-threaded watchdog timer.
     */
    Dispatcher() {
      this.watchdogs = new ConsumerWatchdogSlot[consumerCount];
      for (int i = 0; i < consumerCount; i++) {
        watchdogs[i] = new ConsumerWatchdogSlot();
      }
      ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(
        new ZetaThreadFactory("zeta-report-timer")
      );
      ExecutorService pool;
      try {
        pool = Executors.newFixedThreadPool(consumerCount, new ZetaThreadFactory("zeta-report-consumer"));
      } catch (Throwable t) {
        timer.shutdownNow();
        throw t;
      }
      this.publishTimer = timer;
      this.consumers = pool;
    }

    /**
     * Starts all consumer threads and schedules the periodic watchdog timeout sweep.
     */
    void start() {
      running = true;
      for (ConsumerWatchdogSlot slot : watchdogs) {
        consumers.submit(() -> consumeLoop(slot));
      }
      publishTimer.scheduleWithFixedDelay(
        this::sweepWatchdogs,
        WATCHDOG_SWEEP_MS,
        WATCHDOG_SWEEP_MS,
        TimeUnit.MILLISECONDS
      );
    }

    /**
     * Offers {@code batch} to the bounded work queue, waiting up to {@code timeoutNanos} for space.
     *
     * @param batch        the shard batch to enqueue
     * @param timeoutNanos remaining shared offer budget in nanoseconds ({@code <= 0} for non-blocking)
     * @return {@code true} if the batch was accepted
     */
    boolean enqueue(ShardBatch batch, long timeoutNanos) {
      if (!running) {
        droppedCount.incrementAndGet();
        return false;
      }
      try {
        boolean accepted =
          timeoutNanos > 0L ? queue.offer(batch, timeoutNanos, TimeUnit.NANOSECONDS) : queue.offer(batch);
        // Withdraw if shutdown flipped running concurrently after offer succeeded.
        if (!accepted || (!running && queue.remove(batch))) {
          droppedCount.incrementAndGet();
          return false;
        }
        return true;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        droppedCount.incrementAndGet();
        return false;
      }
    }

    /**
     * Stops the watchdog timer and consumer pool idempotently, waiting briefly for termination and
     * clearing any abandoned queued batches.
     */
    void shutdown() {
      if (!closed.compareAndSet(false, true)) {
        return;
      }
      running = false;
      publishTimer.shutdownNow();
      consumers.shutdownNow();
      try {
        if (!consumers.awaitTermination(SHUTDOWN_AWAIT_SECONDS, TimeUnit.SECONDS)) {
          log.debug("ReportDispatcher consumers did not terminate within {}s", SHUTDOWN_AWAIT_SECONDS);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.warn("ReportDispatcher shutdown interrupted while waiting for consumers to terminate");
      }

      int abandoned = queue.size();
      queue.clear();
      log.info(
        "ReportDispatcher stopped, abandonedQueue={}, expiredDeadTarget={}, expiredStale={}, dropped={}",
        abandoned,
        expiredDeadTargetCount.get(),
        expiredStaleCount.get(),
        droppedCount.get()
      );
    }

    /**
     * Returns the current number of queued shard batches.
     *
     * @return queue depth
     */
    int depth() {
      return queue.size();
    }

    /**
     * Returns the count of batches discarded because their target Worker incarnation died.
     *
     * @return dead-target expiry count
     */
    long expiredDeadTarget() {
      return expiredDeadTargetCount.get();
    }

    /**
     * Returns the count of batches discarded after exceeding {@link #STALE_BATCH_MS} in the queue.
     *
     * @return stale-batch expiry count
     */
    long expiredStale() {
      return expiredStaleCount.get();
    }

    /**
     * Returns the count of batches dropped due to queue saturation, interruption, or shutdown.
     *
     * @return dropped batch count
     */
    long dropped() {
      return droppedCount.get();
    }

    /**
     * Returns the count of batches whose {@link ReportPublisher#publish} call threw an exception.
     *
     * @return publish failure count
     */
    long publishFailed() {
      return publishFailedCount.get();
    }

    /**
     * Records batches dropped by the routing layer prior to offering to the queue.
     *
     * @param batches number of batches dropped
     */
    void recordDropped(long batches) {
      if (batches > 0L) {
        droppedCount.addAndGet(batches);
      }
    }

    /**
     * Interrupts any consumer thread whose in-flight publish has exceeded its deadline.
     */
    private void sweepWatchdogs() {
      long now = TimeSource.monotonicMillis();
      for (ConsumerWatchdogSlot slot : watchdogs) {
        slot.interruptIfExpired(now);
      }
    }

    /**
     * Main consumer loop draining {@link #queue} until {@link #running} becomes {@code false}.
     *
     * @param watchdog this consumer thread's dedicated timeout slot
     */
    private void consumeLoop(ConsumerWatchdogSlot watchdog) {
      watchdog.bind(Thread.currentThread());
      while (running) {
        ShardBatch batch;
        try {
          batch = queue.take();
        } catch (InterruptedException e) {
          if (running) {
            // Discard a late publish-timeout interrupt that arrived after disarm.
            continue;
          }
          log.debug("Report consumer interrupted, shutting down");
          Thread.currentThread().interrupt();
          break;
        }
        processBatch(batch, watchdog);
      }
    }

    /**
     * Validates target incarnation and staleness for one {@link ShardBatch} and publishes it under
     * the consumer's watchdog deadline.
     *
     * @param batch    the dequeued shard batch
     * @param watchdog this consumer thread's timeout slot
     */
    private void processBatch(ShardBatch batch, ConsumerWatchdogSlot watchdog) {
      boolean deadTarget;
      try {
        deadTarget = !healthView.isCurrentIncarnation(batch.target(), batch.targetEpoch());
      } catch (RuntimeException e) {
        droppedCount.incrementAndGet();
        log.warn("Health check failed for target={}, dropping report batch", batch.target(), e);
        return;
      }

      boolean stale = TimeSource.monotonicMillis() - batch.enqueuedAt() > STALE_BATCH_MS;
      if (deadTarget || stale) {
        (deadTarget ? expiredDeadTargetCount : expiredStaleCount).incrementAndGet();
        return;
      }

      watchdog.arm(TimeSource.monotonicMillis() + PUBLISH_TIMEOUT_MS);
      try {
        reportPublisher.publish(
          batch.target(),
          new ReportMessage(
            TimeSource.currentTimeMillis(),
            appName,
            batch.timestamp(),
            batch.targetEpoch(),
            batch.counts()
          )
        );
      } catch (RuntimeException e) {
        publishFailedCount.incrementAndGet();
        log.warn("Failed to publish report batch for target={}, keys={}", batch.target(), batch.counts().size(), e);
      } finally {
        // Synchronized disarm prevents a concurrent timer interrupt from leaking into the next take().
        watchdog.disarm();
        if (Thread.interrupted()) {
          log.debug("Cleared publish-timeout interrupt flag for target={}", batch.target());
        }
      }
    }

    /**
     * Per-consumer synchronized timeout slot preventing watchdog interrupts from leaking across
     * batches after {@link #disarm()} completes.
     */
    private static final class ConsumerWatchdogSlot {

      @GuardedBy("this")
      private @Nullable Thread consumer;

      /** {@code 0} when idle; otherwise the monotonic deadline in milliseconds. */
      @GuardedBy("this")
      private long deadlineMs;

      /**
       * Binds this slot to the consumer thread that owns it.
       *
       * @param thread the consumer thread
       */
      synchronized void bind(Thread thread) {
        this.consumer = thread;
      }

      /**
       * Arms the publish deadline for the current batch.
       *
       * @param deadlineMs monotonic deadline in milliseconds
       */
      synchronized void arm(long deadlineMs) {
        this.deadlineMs = deadlineMs;
      }

      /**
       * Disarms the active deadline so the watchdog will no longer interrupt the consumer.
       */
      synchronized void disarm() {
        this.deadlineMs = 0L;
      }

      /**
       * Interrupts the bound consumer thread if an armed deadline has expired by {@code now}.
       *
       * @param now current monotonic timestamp in milliseconds
       */
      synchronized void interruptIfExpired(long now) {
        if (deadlineMs > 0L && now >= deadlineMs) {
          deadlineMs = 0L;
          if (consumer != null) {
            consumer.interrupt();
          }
        }
      }
    }
  }
}
