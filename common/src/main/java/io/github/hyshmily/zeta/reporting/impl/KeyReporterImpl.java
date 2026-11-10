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
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.reporting.ReportMessage;
import io.github.hyshmily.zeta.reporting.ReportPublisher;
import io.github.hyshmily.zeta.sharding.ConsistentHashRing;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.executor.ZetaThreadFactory;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * Publishes per-key access-add batches to the Worker via {@link ReportPublisher}.
 *
 * <p>One producer call runs three stages, each with exactly one owner (design decisions:
 * reporting):
 * <ol>
 *   <li><b>Admission</b> on the caller's thread ({@code publishBatch}/{@code publishDirect}):
 *       empty batch, no alive Worker; at most one ring reconcile.</li>
 *   <li><b>Routing</b> on the same thread: consistent-hash the counts into per-target
 *       batches and offer each to the bounded dispatcher queue, whose offer is the only
 *       backpressure.</li>
 *   <li><b>Publishing</b> on the consumer threads: one batch per thread, synchronously,
 *       with a scheduler-armed interrupt as the timeout.</li>
 * </ol>
 */
@Slf4j
@Internal
public class KeyReporterImpl implements KeyReporter {

  private final ReportPublisher reportPublisher;
  private final int queueOfferTimeoutMs;
  private final ConsistentHashRing ring;
  private final HealthView healthView;
  private final int queueCapacity;
  private final int consumerCount;
  private final String appName;

  /** Optional flush-cadence tuner; {@code null} keeps the configured interval. */
  @Setter
  private volatile ReportFeedLoop intervalFeedLoop;

  /** Where the tuner applies a computed tide interval (the access counter registers here). */
  @Setter
  private volatile Consumer<Long> cadenceAdjuster;

  /** Keys dropped because {@code locateNode} returned null or the target had no epoch. */
  private final AtomicLong unroutableDropCounter = new AtomicLong();
  /** Keys dropped because no Worker was alive. */
  private final AtomicLong workerDeadDropCounter = new AtomicLong();
  /** Keys dropped because the dispatcher refused the batch (queue full, or shutting down). */
  private final AtomicLong droppedCounter = new AtomicLong();

  /** Reporter lifecycle; a failed start is retryable and a stopped reporter is final. */
  private enum State {
    NEW,
    STARTING,
    RUNNING,
    FAILED,
    STOPPED,
  }

  private final AtomicReference<State> state = new AtomicReference<>(State.NEW);

  /** The dispatcher; created on {@link #start()}. */
  private volatile @Nullable Dispatcher dispatcher;

  /**
   * @param reportPublisher     publisher used to send report messages
   * @param appName             this application instance's name, stamped on every message
   * @param queueCapacity       dispatcher work-queue capacity
   * @param queueOfferTimeoutMs how long a routed batch waits for queue space
   * @param consumerCount       threads draining the queue
   * @param ring                consistent-hash ring for Worker routing
   * @param healthView          cluster health view (liveness and epoch)
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
    this.ring = ring;
    this.appName = appName;
    this.healthView = healthView;
    this.queueCapacity = queueCapacity;
    this.consumerCount = consumerCount;
    this.reportPublisher = reportPublisher;
    this.queueOfferTimeoutMs = queueOfferTimeoutMs;
  }

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
      // Publish the dispatcher before RUNNING: a stop() that observes RUNNING
      // must be able to find and close it. A stop() that wins the CAS below
      // closes this instance; the failed CAS then only clears the reference.
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
      // A concurrent stop() owns the final state: never overwrite STOPPED with
      // FAILED and accidentally make the reporter restartable.
      state.compareAndSet(State.STARTING, State.FAILED);
      log.error(
        "Failed to start KeyReporterImpl; per-key counts will not be flushed to Worker. " +
          "Application continues but Worker hot-key detection will be blind to this instance. " +
          "start() may be retried.",
        t
      );
    }
  }

  @Override
  public void stop() {
    State current = state.getAndSet(State.STOPPED);
    if (current == State.STOPPED) {
      return;
    }

    Dispatcher dispatcherToStop = dispatcher;
    if (dispatcherToStop != null) {
      dispatcherToStop.shutdown();
    }
  }

  @Override
  public void publishBatch(Map<String, Long> counts, long windowMs) {
    if (state.get() != State.RUNNING) {
      return;
    }
    try {
      // An empty window is a measured zero arrival rate, not a censored cycle: the
      // reporter did flush, nothing arrived. It must feed the cadence tuner or SHADOW
      // can never predict an idle ON deployment and ON never backs off to maxInterval.
      // A non-empty cycle with no alive Worker returns below without sampling.
      if (counts.isEmpty()) {
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
   * The admission gates: empty batch, no alive Worker.
   *
   * @return the routing inputs, or {@code null} when this cycle is dropped
   */
  @Nullable
  private Admission admit(Map<String, Long> counts) {
    // publishBatch handles an empty window before this point (it is a zero-rate sample,
    // not an admission decision); this guard remains for publishDirect.
    if (counts.isEmpty() || state.get() != State.RUNNING) {
      return null;
    }

    // One reconcile per flush. The alive set and epochs are captured from that reconcile;
    // per-key routing reads the ring's current published snapshot. A concurrent rebuild can
    // therefore route a key to a node absent from this capture, and the key is dropped as
    // unroutable rather than re-based onto an epoch it was never counted under.
    Set<String> aliveNodes = ring.rebuildIfPossible(healthView);
    if (aliveNodes.isEmpty()) {
      dropUnroutable(counts, "No alive Worker nodes for routing");
      return null;
    }

    // Capture the incarnation with the ring snapshot the batch is routed on. The fused
    // liveness+epoch probe rejects a Worker that restarted into a cold start between the
    // alive-set snapshot and this read: its accumulated counts must not be stamped with the
    // new incarnation before it is ready to serve.
    Map<String, Long> epochs = new HashMap<>(Math.max(1, aliveNodes.size() * 2));
    for (String node : aliveNodes) {
      long epoch = healthView.epochOf(node);
      if (epoch != HealthView.UNKNOWN_EPOCH && healthView.isCurrentIncarnation(node, epoch)) {
        epochs.put(node, epoch);
      }
    }
    if (epochs.isEmpty()) {
      // Every live Worker is still cold-starting (no epoch captured). This cycle is censored
      // exactly like a no-alive-Worker one: routing every key through an always-false predicate
      // and counting each as unroutable would only burn the flush.
      dropUnroutable(counts, "No Worker node has a routable epoch");
      return null;
    }
    return new Admission(TimeSource.currentTimeMillis(), Map.copyOf(epochs));
  }

  /**
   * Drops a flush that cannot be routed and counts every key as a dead-Worker drop.
   *
   * @param counts the flush counts
   * @param reason the operator-facing cause, without a trailing separator
   */
  private void dropUnroutable(Map<String, Long> counts, String reason) {
    long total = workerDeadDropCounter.addAndGet(counts.size());
    log.warn("{}; dropping {} keys in this flush (cumulative: {})", reason, counts.size(), total);
  }

  /**
   * Feeds a completed flush to the cadence tuner. Empty snapshots are sampled at rate zero
   * (a flush happened and nothing arrived); a non-empty no-alive-Worker cycle returns before
   * this point, so the loop never learns from a censored delivery. Control-path failures never
   * cost a report.
   */
  private void sampleFeedLoop(int batchSize, long windowMs) {
    ReportFeedLoop feedLoop = intervalFeedLoop;
    if (feedLoop == null) {
      return;
    }
    // Two flushes inside one millisecond (or a manual flush) carry no measurable rate; dividing
    // by zero would poison the EMA with NaN/Infinity for the rest of the process.
    if (windowMs <= 0L) {
      return;
    }
    try {
      ReportFeedLoop.State state = feedLoop.onFlushCompleted(batchSize, windowMs);
      Consumer<Long> adjuster = cadenceAdjuster;
      if (feedLoop.mode() == ReportFeedLoop.Mode.ON && adjuster != null) {
        adjuster.accept(state.intervalMs());
      }
    } catch (RuntimeException e) {
      log.debug("feed-loop sample failed; flush proceeds at the unchanged cadence", e);
    }
  }

  /**
   * Routes the flush's keys to their Worker targets and offers one batch per target to the
   * dispatcher queue. The comparison predicate is built once per flush, not once per key.
   */
  private void routeAndEnqueue(Map<String, Long> counts, Admission admission) {
    Dispatcher current = dispatcher;
    if (current == null) {
      return;
    }

    long timestamp = admission.timestamp();
    long enqueuedAt = TimeSource.monotonicMillis();
    // Route only to nodes whose epoch this admission captured. The ring can be rebuilt by a
    // concurrent heartbeat between the reconcile and this pass; the current snapshot's new owner
    // has no captured epoch, and walking past it to a captured node keeps the counts instead of
    // dropping the key as unroutable.
    Predicate<String> admittedTarget = admission.epochs()::containsKey;
    Map<String, Map<String, Long>> sharded = new HashMap<>();
    for (Map.Entry<String, Long> entry : counts.entrySet()) {
      Long count = entry.getValue();
      if (entry.getKey() == null || count == null || count <= 0L) {
        // One malformed record must not discard the whole flush.
        continue;
      }

      String target;
      try {
        target = ring.locateNode(entry.getKey(), admittedTarget);
        if (target == null) {
          noteUnroutable(entry.getKey(), 1, "locateNode returned null");
          continue;
        }
      } catch (RuntimeException e) {
        // One bad key must not discard the whole flush: count it, keep routing.
        noteUnroutable(entry.getKey(), 1, "locateNode failed: " + e.getMessage());
        continue;
      }

      sharded.computeIfAbsent(target, t -> new HashMap<>()).put(entry.getKey(), count);
    }

    // The offer timeout is one budget for the whole flush, not one per routed target.
    // A broker stall with N alive Workers would otherwise block the tide thread for
    // N × queueOfferTimeoutMs and delay the local sketch merge behind it.
    long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(queueOfferTimeoutMs);
    Iterator<Map.Entry<String, Map<String, Long>>> targets = sharded.entrySet().iterator();
    while (targets.hasNext()) {
      Map.Entry<String, Map<String, Long>> entry = targets.next();
      String target = entry.getKey();
      Map<String, Long> countsForTarget = entry.getValue();
      // Reports are addressed to an incarnation captured with the routing snapshot: a
      // batch routed while the Worker was this epoch must never be re-based onto a new
      // one after a restart.
      Long targetEpoch = admission.epochs().get(target);
      if (targetEpoch == null) {
        noteUnroutable(target, countsForTarget.size(), "no epoch for routed target");
        continue;
      }

      long remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos <= 0) {
        long remainingKeys = countsForTarget.size();
        int remainingTargets = 1;
        while (targets.hasNext()) {
          remainingKeys += targets.next().getValue().size();
          remainingTargets++;
        }
        long dropped = droppedCounter.addAndGet(remainingKeys);
        current.recordDropped(remainingTargets);
        log.debug(
          "report flush offer budget exhausted, dropped {} keys across remaining targets, depth={}/{}, " +
            "cumulativeDrops={}",
          remainingKeys,
          current.depth(),
          queueCapacity,
          dropped
        );
        break;
      }

      long offerTimeoutMs = Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
      boolean accepted = current.enqueue(
        new ShardBatch(target, targetEpoch, timestamp, countsForTarget, enqueuedAt),
        offerTimeoutMs
      );
      if (!accepted) {
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

  private void noteUnroutable(String key, int count, String reason) {
    long total = unroutableDropCounter.addAndGet(count);
    log.debug("{} for key={}, cumulativeUnroutable={}", reason, key, total);
  }

  /**
   * The admission outcome: the wall-clock timestamp stamped on every shard of this
   * flush and the epoch captured for each routed Worker.
   */
  private record Admission(long timestamp, Map<String, Long> epochs) {}

  @Override
  public @Nullable DispatcherStats dispatcherStats() {
    Dispatcher current = dispatcher;
    return current == null
      ? null
      : new DispatcherStats(current.depth(), current.expiredDeadTarget(), current.expiredStale(), current.dropped());
  }

  @Override
  public @Nullable RoutingStats routingStats() {
    if (dispatcher == null) {
      return null;
    }
    return new RoutingStats(unroutableDropCounter.get(), workerDeadDropCounter.get(), droppedCounter.get());
  }

  @Override
  public @Nullable ReportFeedLoop feedLoop() {
    return intervalFeedLoop;
  }

  /**
   * A batch of key-add mappings destined for a single Worker target.
   *
   * @param target      the Worker nodeId for this batch
   * @param targetEpoch the target's epoch at routing time — captured before the batch waits
   *                    in the queue, so a restart supersedes the whole batch instead of
   *                    silently re-basing it onto the new incarnation
   * @param timestamp   wall-clock ms when the batch was assembled; travels on the wire
   * @param counts      non-zero per-key counts accumulated since the last flush
   * @param enqueuedAt  monotonic ms at enqueue — the only field used for batch-age
   *                    arithmetic, which must not be subject to NTP steps
   */
  record ShardBatch(String target, long targetEpoch, long timestamp, Map<String, Long> counts, long enqueuedAt) {}

  /**
   * Bounded work queue plus the consumer threads that drain it, and the one timer that bounds
   * a publish (design decisions: reporting).
   */
  final class Dispatcher {

    /** One publish call's budget before the consumer thread is interrupted. */
    private static final long PUBLISH_TIMEOUT_MS = 5_000;
    /** Watchdog scan cadence; bounds the overrun past {@link #PUBLISH_TIMEOUT_MS}. */
    private static final long WATCHDOG_SWEEP_MS = 100;
    /** Queue wait after which a batch is discarded as stale rather than published. */
    private static final long STALE_BATCH_MS = 5_000;

    private final BlockingQueue<ShardBatch> queue = new LinkedBlockingQueue<>(queueCapacity);
    private final AtomicLong expiredDeadTargetCount = new AtomicLong();
    private final AtomicLong expiredStaleCount = new AtomicLong();
    private final AtomicLong droppedCount = new AtomicLong();
    private final ScheduledExecutorService publishTimer;
    /** One permanent slot per consumer thread, swept by the single timer. */
    private final ConsumerWatchdogSlot[] watchdogs;
    private final ExecutorService consumers;

    /** Lifecycle flag; false after shutdown. */
    private volatile boolean running;

    /** Makes a concurrent or repeated shutdown a no-op after the first close. */
    private final AtomicBoolean closed = new AtomicBoolean();

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

    void start() {
      running = true;
      for (int i = 0; i < consumerCount; i++) {
        ConsumerWatchdogSlot slot = watchdogs[i];
        consumers.submit(() -> consumeLoop(slot));
      }
      publishTimer.scheduleWithFixedDelay(
        this::sweepWatchdogs,
        WATCHDOG_SWEEP_MS,
        WATCHDOG_SWEEP_MS,
        TimeUnit.MILLISECONDS
      );
    }

    private void sweepWatchdogs() {
      long now = TimeSource.monotonicMillis();
      for (ConsumerWatchdogSlot slot : watchdogs) {
        slot.interruptIfExpired(now);
      }
    }

    /**
     * Offer a batch to the bounded queue, waiting up to {@code timeoutMs} for space.
     * An interrupt discards the batch and restores the flag.
     *
     * @param batch     the batch to enqueue
     * @param timeoutMs the remaining shared flush budget
     * @return {@code true} when the batch was accepted
     */
    boolean enqueue(ShardBatch batch, long timeoutMs) {
      if (!running) {
        return false;
      }
      try {
        boolean accepted = queue.offer(batch, timeoutMs, TimeUnit.MILLISECONDS);
        // shutdown() flips running and then drains the queue. A batch offered after that
        // drain would sit forever with no consumer, so withdraw it and report the same
        // rejection the caller would have seen before the offer.
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

    @SuppressWarnings("all")
    void shutdown() {
      if (!closed.compareAndSet(false, true)) {
        return;
      }
      // Clear the flag first: a consumer blocked on a publish then re-tests it and exits
      // instead of taking another item only to be interrupted again. The timer stops
      // before the consumers, so shutdown's own interrupt is the only one in flight.
      running = false;
      publishTimer.shutdownNow();
      consumers.shutdownNow();
      try {
        consumers.awaitTermination(2, TimeUnit.SECONDS);
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

    int depth() {
      return queue.size();
    }

    long expiredDeadTarget() {
      return expiredDeadTargetCount.get();
    }

    long expiredStale() {
      return expiredStaleCount.get();
    }

    long dropped() {
      return droppedCount.get();
    }

    /**
     * Records batches the routing layer refused before an offer: a shared offer budget that
     * expired between targets, or a queue offer that lost to shutdown. The queue's own
     * {@link #enqueue} path counts its internal withdrawal separately.
     *
     * @param batches the number of batches not offered
     */
    void recordDropped(long batches) {
      if (batches > 0L) {
        droppedCount.addAndGet(batches);
      }
    }

    /**
     * A consumer thread's permanent timeout slot. {@code arm}/{@code disarm} and the
     * timer's {@code interruptIfExpired} synchronize on this object, so an interrupt
     * cannot be delivered after {@code disarm} returns — otherwise it would leak into the
     * next {@code take()}.
     */
    private static final class ConsumerWatchdogSlot {

      @GuardedBy("this")
      private Thread consumer;

      /** 0 = idle; otherwise the monotonic deadline of the publish in flight. */
      @GuardedBy("this")
      private long deadlineMs;

      synchronized void bind(Thread thread) {
        this.consumer = thread;
      }

      synchronized void arm(long deadlineMs) {
        this.deadlineMs = deadlineMs;
      }

      synchronized void disarm() {
        this.deadlineMs = 0L;
      }

      synchronized void interruptIfExpired(long now) {
        if (deadlineMs > 0L && now >= deadlineMs) {
          deadlineMs = 0L;
          consumer.interrupt();
        }
      }
    }

    /**
     * Consumer body: take one batch, discard it when its target died or it waited past the
     * staleness bound, otherwise publish it synchronously with an interrupting timeout.
     */
    @SuppressWarnings("all")
    private void consumeLoop(ConsumerWatchdogSlot watchdog) {
      watchdog.bind(Thread.currentThread());
      while (running) {
        ShardBatch batch;
        try {
          batch = queue.take();
        } catch (InterruptedException e) {
          if (running) {
            // A publish-timeout interrupt can arrive just after the publish returned and
            // the finally cleared the flag. That is not a shutdown signal; discarding it
            // here keeps the consumer alive.
            continue;
          }
          log.debug("Report consumer interrupted, shutting down");
          Thread.currentThread().interrupt();
          break;
        }

        boolean deadTarget = !healthView.isCurrentIncarnation(batch.target(), batch.targetEpoch());
        boolean stale = TimeSource.monotonicMillis() - batch.enqueuedAt() > STALE_BATCH_MS;
        if (deadTarget || stale) {
          (deadTarget ? expiredDeadTargetCount : expiredStaleCount).incrementAndGet();
          continue;
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
          log.warn("Failed to publish report batch for target={}, keys={}", batch.target(), batch.counts().size(), e);
        } finally {
          // Disarm waits for an in-flight interrupt: once it returns, no timer task can
          // interrupt this thread before the flag is cleared below.
          watchdog.disarm();
          Thread.interrupted();
        }
      }
    }
  }
}
