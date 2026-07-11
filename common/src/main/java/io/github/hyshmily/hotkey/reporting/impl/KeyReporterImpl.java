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
package io.github.hyshmily.hotkey.reporting.impl;

import static io.github.hyshmily.hotkey.constants.HotKeyConstants.TOPK_INCR;
import static io.github.hyshmily.hotkey.util.TimeSource.currentTimeMillis;

import io.github.hyshmily.hotkey.Internal;
import io.github.hyshmily.hotkey.hotkeydetector.doublebuffer.BufferedCounter;
import io.github.hyshmily.hotkey.reporting.BbrRateLimiter;
import io.github.hyshmily.hotkey.reporting.KeyReporter;
import io.github.hyshmily.hotkey.reporting.ReportMessage;
import io.github.hyshmily.hotkey.reporting.ReportPublisher;
import io.github.hyshmily.hotkey.sharding.HealthView;
import io.github.hyshmily.hotkey.sharding.RingManager;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Periodically aggregates per-key access counts and publishes them
 * to the Worker via {@link ReportPublisher}.
 *
 * <p>Uses a {@link BufferedCounter} double-buffer as a temporary counter store;
 * the active buffer accepts lock-free writes while the standby buffer is drained
 * and reset on each flush cycle. Flushed to the appropriate shard at a fixed
 * interval.
 *
 * <p>Keys are routed via the {@link RingManager} consistent-hash ring, ensuring the
 * same key always maps to the same Worker node even as the cluster scales.
 * The RingManager also tracks Worker liveness via heartbeat, so dead shards are
 * automatically excluded from routing.
 *
 * <p>Burst absorption and backpressure are provided by a bounded
 * {@link LinkedBlockingQueue} between the flush callback and the
 * RabbitMQ publisher.  When the queue is full, {@code onFlush()} drops
 * batches after a configurable timeout.
 *
 * <p>When a {@link io.github.hyshmily.hotkey.reporting.BbrRateLimiter} is configured, each flush cycle is submitted
 * to the BBR for admission control.  If the pipeline is saturated (high CPU
 * and/or excessive in-flight batches), the flushed batch is dropped and
 * the counts are lost — at most one {@code reportIntervalMs} window.
 */
@Slf4j
@Internal
public class KeyReporterImpl implements KeyReporter {

  /** Maximum distinct keys in the BufferedCounter before eager swap. */
  private static final int MAX_BUFFER_SIZE = 100_000;

  /** Fraction of maxBufferSize that triggers an eager buffer swap. */
  private static final double EAGER_SWAP_RATIO = 0.8;

  /** Double-buffered counter aggregating per-key access counts between flushes. */
  private final BufferedCounter bufferedCounter;

  /** Publishes aggregated reports to RabbitMQ. */
  private final ReportPublisher reportPublisher;

  /** Scheduler for the periodic flush loop. */
  @Getter
  private final ScheduledExecutorService scheduler;

  /** Fixed delay between report flushes in milliseconds. */
  private final long reportIntervalMs;
  /** Name of this application instance, included in report messages. */
  private final String appName;
  /** Maximum capacity of the dispatcher work queue. */
  private final int queueCapacity;
  /** Timeout (ms) for offering a batch to the dispatcher queue before dropping. */
  private final int queueOfferTimeoutMs;
  /** Number of consumer threads draining the dispatcher queue. */
  private final int consumerCount;
  /** Consistent-hashing ring manager for Worker node routing. */
  private final RingManager ringManager;
  /** Cluster health view for filtering dead Workers. */
  private final HealthView healthView;

  private volatile int lastNodeCount = -1;

  /** Optional BBR adaptive rate limiter; null disables BBR gating. */
  @Setter
  @SuppressWarnings("java:S3077") // BBR is thread-safe
  private volatile BbrRateLimiterImpl bbrRateLimiter;

  /** Guards start() idempotency. */
  private final AtomicBoolean started = new AtomicBoolean(false);
  /** The report dispatcher instance; created on start(). */
  private ReportDispatcher dispatcher;

  /**
   * Creates a new reporter that periodically flushes access counts to RabbitMQ.
   *
   * @param reportPublisher     the publisher used to send report messages to RabbitMQ
   * @param scheduler           the scheduler for periodic flush cycles
   * @param reportIntervalMs    fixed delay between consecutive flushes in milliseconds
   * @param appName             name of this application instance, included in report messages
   * @param queueCapacity       maximum capacity of the dispatcher work queue
   * @param queueOfferTimeoutMs timeout for offering a batch to the dispatcher queue before dropping
   * @param consumerCount       number of consumer threads draining the dispatcher queue
   * @param ringManager         consistent-hashing ring manager for Worker node routing
   * @param healthView          cluster health view for filtering dead Workers
   */
  @SuppressWarnings("java:S107") // too many constructor args, but all are required for proper initialization
  public KeyReporterImpl(
    ReportPublisher reportPublisher,
    ScheduledExecutorService scheduler,
    long reportIntervalMs,
    String appName,
    int queueCapacity,
    int queueOfferTimeoutMs,
    int consumerCount,
    RingManager ringManager,
    HealthView healthView
  ) {
    this.reportPublisher = reportPublisher;
    this.scheduler = scheduler;
    this.reportIntervalMs = reportIntervalMs;
    this.appName = appName;
    this.queueCapacity = queueCapacity;
    this.queueOfferTimeoutMs = queueOfferTimeoutMs;
    this.consumerCount = consumerCount;
    this.ringManager = ringManager;
    this.healthView = healthView;
    this.bufferedCounter = new BufferedCounter(
      this::onFlush,
      MAX_BUFFER_SIZE,
      reportIntervalMs,
      EAGER_SWAP_RATIO,
      scheduler
    );
  }

  /**
   * A batch of key-count mappings destined for a single Worker target.
   *
   * @param target    the Worker nodeId for this batch
   * @param timestamp wall-clock ms when the batch was assembled
   * @param counts    non-zero per-key counts accumulated since the last flush
   */
  record ShardBatch(String target, long timestamp, Map<String, Long> counts) {}

  /**
   * Record one access for the given cache key.
   *
   * <p>Idempotent per-key local counter increment.  The counter is stored in
   * a double-buffered {@link BufferedCounter} and flushed periodically to the
   * configured Workers.
   *
   * @param cacheKey the accessed key
   */
  public void recordReport(String cacheKey) {
    bufferedCounter.count(cacheKey, TOPK_INCR);
  }

  /**
   * Start the periodic flush scheduler and the report dispatcher.
   * Idempotent — subsequent calls are silently ignored.
   *
   * <p>The flush callback drains the {@link BufferedCounter}, groups entries by
   * target (shard index or nodeId), and enqueues them.  Actual publishing
   * to RabbitMQ runs on dedicated consumer threads, decoupling the flush
   * callback from network I/O.
   */
  public void start() {
    if (!started.compareAndSet(false, true)) {
      log.debug("KeyReporterImpl already started, skip");
      return;
    }
    try {
      dispatcher = new ReportDispatcher();
      dispatcher.start();

      bufferedCounter.afterPropertiesSet();

      log.info(
        "KeyReporterImpl started: appName={}, intervalMs={}, queueCapacity={}, consumers={}",
        appName,
        reportIntervalMs,
        queueCapacity,
        dispatcher.consumerCount()
      );
    } catch (Exception e) {
      log.error(
        "Failed to start KeyReporterImpl; per-key counts will not be flushed to Worker. " +
          "Application continues but Worker hot-key detection will be blind to this instance.",
        e
      );
    }
  }

  /**
   * Gracefully shut down the report dispatcher.
   *
   * <p>Interrupts all consumer threads and waits for them to finish
   * (with a 2-second timeout per thread). Any batches remaining in the
   * work queue are discarded. This method is typically registered as
   * the Spring bean {@code destroyMethod}.
   *
   * <p>After shutdown, the reporter no longer publishes reports.
   * The periodic flush loop continues to run but its output is silently
   * dropped because the dispatcher queue is no longer being consumed.
   *
   * <p>Idempotent — safe to call multiple times.
   */
  public void stop() {
    bufferedCounter.destroy();
    if (dispatcher != null) {
      dispatcher.shutdown();
    }
  }

  /**
   * Callback invoked by the {@link BufferedCounter} on each scheduled flush.
   * Groups the drained key-count map by target Worker via consistent-hash
   * routing and enqueues one {@link ShardBatch} per target.
   *
   * <p>When BBR rate limiting is active, the batch is dropped if the
   * limiter rejects the cycle.  Because the double-buffer has already
   * drained the counts, at most one {@code reportIntervalMs} window of
   * counts is lost — an acceptable trade-off for the simpler and more
   * predictable double-buffer design.
   *
   * <p>If no Workers are alive, the batch is silently discarded.
   */
  @SuppressWarnings("all")
  private void onFlush(Map<String, Long> keyCounts) {
    if (keyCounts.isEmpty()) {
      return;
    }

    try {
      ringManager.reconcileFromHealthView(healthView);

      Set<String> aliveNodes = healthView.getAliveWorkerIds();
      if (aliveNodes.isEmpty()) {
        log.warn("No alive Worker nodes for routing; dropping {} keys in this flush", keyCounts.size());
        return;
      }

      if (bbrRateLimiter != null) {
        // sync minInFlight floor to current Worker count; only updates when count changes
        int currentCount = ringManager.nodeCount();
        if (currentCount != lastNodeCount) {
          lastNodeCount = currentCount;
          bbrRateLimiter.setMinInFlight(currentCount);
        }
        if (!bbrRateLimiter.tryAcquire()) {
          bbrRateLimiter.onGateDrop();
          return;
        }
      }

      long now = currentTimeMillis();
      Map<String, Map<String, Long>> sharded = new HashMap<>();

      keyCounts.forEach((key, val) -> {
        if (val > 0) {
          String target = ringManager.routeNode(key, aliveNodes);
          if (target != null) {
            sharded.computeIfAbsent(target, t -> new HashMap<>()).put(key, val);
          }
        }
      });

      sharded.forEach((target, counts) -> {
        if (!dispatcher.enqueue(new ShardBatch(target, now, counts))) {
          long dropped = dispatcher.dropped();
          if (dropped % 100 == 0 || dropped == 1) {
            log.warn(
              "report queue full, dropped target={} keys={}, depth={}/{}, cumulativeDrops={}",
              target,
              counts.size(),
              dispatcher.depth(),
              queueCapacity,
              dropped
            );
          }
        } else if (bbrRateLimiter != null) {
          bbrRateLimiter.onEnqueue();
        }
      });
    } catch (Exception e) {
      log.error("Flush callback failed", e);
    }
  }

  /**
   * Return the current number of batches waiting in the dispatcher work queue.
   *
   * @return queue depth (number of enqueued but not yet consumed batches),
   *         or {@code -1} if the dispatcher has not been started
   */
  public int dispatcherDepth() {
    return dispatcher == null ? -1 : dispatcher.depth();
  }

  /**
   * Return the maximum capacity of the dispatcher work queue.
   *
   * @return the queue capacity as configured via {@code queueCapacity},
   *         or {@code -1} if the dispatcher has not been started
   */
  public int dispatcherCapacity() {
    return dispatcher == null ? -1 : dispatcher.capacity();
  }

  /**
   * Return the total number of batches that were discarded because they
   * waited longer than 5 seconds in the dispatcher queue (staleness expiry).
   *
   * @return total expired batch count since startup, or {@code -1} if the
   *         dispatcher has not been started
   */
  public long dispatcherExpired() {
    return dispatcher == null ? -1 : dispatcher.expired();
  }

  /**
   * Return the total number of batches rejected because the dispatcher
   * queue was full (back-pressure drops from the flush loop).
   *
   * @return total dropped batch count since startup, or {@code -1} if the
   *         dispatcher has not been started
   */
  public long dispatcherDropped() {
    return dispatcher == null ? -1 : dispatcher.dropped();
  }

  /**
   * Return the approximate number of unique keys currently buffered in the
   * local double-buffer counter store.
   *
   * <p>This is the sum of distinct keys in both the active and standby
   * buffers and may not reflect the exact count under concurrent access.
   *
   * @return estimated number of unique keys with pending access counts
   */
  public long getPendingKeyCount() {
    return bufferedCounter.estimatedSizeOfKeysCount();
  }

  /**
   * Return the total number of flush cycles that were permitted by the BBR
   * rate limiter since startup.
   *
   * @return total passed count, or {@code -1} if BBR rate limiting is disabled
   *         ({@link #bbrRateLimiter} is {@code null})
   */
  public long bbrPassed() {
    return bbrRateLimiter == null ? -1 : bbrRateLimiter.getTotalPassed();
  }

  /**
   * Return the total number of flush cycles that were dropped by the BBR
   * rate limiter since startup (both gate drops and consumer drops).
   *
   * @return total dropped count, or {@code -1} if BBR rate limiting is disabled
   */
  public long bbrDropped() {
    return bbrRateLimiter == null ? -1 : bbrRateLimiter.getTotalDropped();
  }

  /**
   * Return the current number of in-flight batches tracked by the BBR rate
   * limiter — those enqueued for publishing but not yet completed or dropped.
   *
   * @return current in-flight count (non-negative), or {@code -1} if BBR
   *         rate limiting is disabled
   */
  public long bbrInFlight() {
    return bbrRateLimiter == null ? -1 : bbrRateLimiter.getInFlight();
  }

  /**
   * Return the current BBR-computed maximum concurrency budget (max in-flight).
   *
   * <p>This value is derived from the sliding-window maxPASS and minRT
   * metrics via Little's Law. It represents the limiter's estimate of the
   * optimal number of concurrent in-flight batches before the pipeline
   * becomes congested.
   *
   * @return the computed max in-flight budget, or {@code -1} if BBR rate
   *         limiting is disabled
   */
  public long bbrMaxInFlight() {
    return bbrRateLimiter == null ? -1 : bbrRateLimiter.getCurrentMaxInFlight();
  }

  /**
   * Manages a bounded work queue and a fixed pool of consumer threads that
   * drain batches and publish them via {@link ReportPublisher}.
   *
   * <p>Burst absorption: the bound on {@code LinkedBlockingQueue} prevents
   * the flush callback from overwhelming RabbitMQ.  When the queue is
   * persistently full, batches are dropped after a configurable timeout.
   */
  class ReportDispatcher {

    /** Bounded work queue between the flush loop and the consumer threads. */
    private final BlockingQueue<ShardBatch> queue = new LinkedBlockingQueue<>(queueCapacity);
    /** Count of batches discarded due to staleness (>5 s wait in queue). */
    private final AtomicLong expiredCount = new AtomicLong();
    /** Count of batches rejected because the queue was full. */
    private final AtomicLong droppedCount = new AtomicLong();
    /** Active consumer threads draining the work queue. */
    private final List<Thread> consumers = new ArrayList<>();
    /** Lifecycle flag; false after shutdown. */
    private volatile boolean running;

    /**
     * Start the consumer threads that drain batches from the bounded work
     * queue and publish them via {@link ReportPublisher}.
     *
     * <p>Each consumer runs in a named daemon thread
     * ({@code "report-consumer-N"}) so they do not prevent JVM shutdown.
     * The number of consumer threads is determined by the
     * {@code consumerCount} configuration.
     */
    void start() {
      running = true;
      for (int i = 0; i < consumerCount; i++) {
        Thread t = new Thread(this::consumeLoop, "hotkey-report-consumer-" + i);
        t.setDaemon(true);
        t.start();
        consumers.add(t);
      }
    }

    /**
     * Offer a batch to the bounded work queue with a timeout.
     *
     * <p>Blocks for up to {@code queueOfferTimeoutMs} milliseconds waiting
     * for space to become available. If the queue remains full after the
     * timeout, the batch is rejected and the drop counter is incremented.
     *
     * <p>If the calling thread is interrupted while waiting, the batch is
     * discarded and {@code false} is returned.
     *
     * @param batch the sharded batch to enqueue for publishing
     * @return {@code true} if the batch was accepted into the queue;
     *         {@code false} if the queue was full or the thread was
     *         interrupted
     */
    boolean enqueue(ShardBatch batch) {
      try {
        boolean accepted = queue.offer(batch, queueOfferTimeoutMs, TimeUnit.MILLISECONDS);
        if (!accepted) {
          droppedCount.incrementAndGet();
        }
        return accepted;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }

    /**
     * Gracefully shut down all consumer threads.
     *
     * <p>Signals all consumers to stop via the {@code running} flag and
     * thread interruption, then waits up to 2 seconds for each thread to
     * finish. After shutdown, the queue may still contain unconsumed
     * batches — they are discarded.
     *
     * <p>This method is called from {@link KeyReporterImpl#stop()} and is
     * idempotent. However, after shutdown the dispatcher cannot be
     * restarted.
     */
    void shutdown() {
      running = false;
      for (Thread t : consumers) {
        t.interrupt();
      }
      for (Thread t : consumers) {
        try {
          t.join(2000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      consumers.clear();
      log.info(
        "ReportDispatcher stopped, remaining queue={}, expired={}, dropped={}",
        queue.size(),
        expiredCount.get(),
        droppedCount.get()
      );
    }

    /**
     * Return the current number of batches waiting in the work queue.
     *
     * @return current queue depth (non-negative)
     */
    int depth() {
      return queue.size();
    }

    /**
     * Return the maximum number of batches the work queue can hold.
     *
     * @return the configured queue capacity
     */
    int capacity() {
      return queueCapacity;
    }

    /**
     * Return the number of actively running consumer threads.
     *
     * @return current consumer thread count
     */
    int consumerCount() {
      return consumers.size();
    }

    /**
     * Return the total number of batches that were discarded due to
     * staleness (waited longer than 5 seconds in the queue) since startup.
     *
     * @return total expired batch count
     */
    long expired() {
      return expiredCount.get();
    }

    /**
     * Return the total number of batches that were rejected because the
     * queue was full since startup.
     *
     * @return total dropped batch count
     */
    long dropped() {
      return droppedCount.get();
    }

    /**
     * Consumer thread body: poll batches from the queue and publish them.
     *
     * <p>Staleness guard: batches that waited longer than 5 s in the queue
     * are discarded rather than published, preventing the Worker from receiving
     * stale access patterns during prolonged backpressure.
     *
     * <p>When BBR rate limiting is active:
     * <ul>
     *   <li>Successful publish records round-trip time via {@link BbrRateLimiter#onSuccess(long)}</li>
     *   <li>Stale batches (5s+ wait) or publish failures trigger ,
     *       allowing BBR to back off the send rate and prevent cascading overload</li>
     *   <li>InterruptedException is handled separately (break, not logged as error)
     *       to avoid noise during orderly shutdown</li>
     * </ul>
     */
    @SuppressWarnings("all")
    private void consumeLoop() {
      while (running) {
        ShardBatch batch;
        try {
          batch = queue.take();
        } catch (InterruptedException e) {
          log.warn("ReportDispatcher consumer interrupted, shutting down");
          Thread.currentThread().interrupt();
          break;
        }

        // 5s stale check — discard data that waited too long in the queue
        if (currentTimeMillis() - batch.timestamp() > 5_000) {
          expiredCount.incrementAndGet();
          if (bbrRateLimiter != null) {
            bbrRateLimiter.onConsumerDrop();
          }
          continue;
        }

        try {
          reportPublisher.publish(batch.target(), new ReportMessage(appName, batch.timestamp(), batch.counts()));
          if (bbrRateLimiter != null) {
            bbrRateLimiter.onSuccess(currentTimeMillis() - batch.timestamp());
          }
        } catch (Exception e) {
          log.error("Failed to publish report batch for target={}, keys={}", batch.target(), batch.counts().size(), e);
          if (bbrRateLimiter != null) {
            bbrRateLimiter.onConsumerDrop();
          }
        }
      }
    }
  }
}
