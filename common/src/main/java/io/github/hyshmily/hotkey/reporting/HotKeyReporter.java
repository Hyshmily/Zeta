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
package io.github.hyshmily.hotkey.reporting;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.hotkey.sharding.RingManager;
import io.github.hyshmily.hotkey.sharding.ClusterHealthView;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


/**
 * Periodically aggregates per-key access counts and publishes them
 * to the Worker via {@link ReportPublisher}.
 *
 * <p>Uses a Caffeine cache as a temporary counter store; entries are
 * evicted after 30 seconds of inactivity to bound memory usage.
 * Flushed to the appropriate shard at a fixed interval.
 *
 * <p>Keys are routed via the {@link RingManager} consistent-hash ring, ensuring the
 * same key always maps to the same Worker node even as the cluster scales.
 * The RingManager also tracks Worker liveness via heartbeat, so dead shards are
 * automatically excluded from routing.
 *
 * <p>Burst absorption and backpressure are provided by a bounded
 * {@link LinkedBlockingQueue} between the flush loop and the
 * RabbitMQ publisher.  When the queue is full, {@code flush()} drops
 * batches after a configurable timeout, and the Caffeine eviction
 * provides natural rate-limiting.
 *
 * <p>When a {@link BbrRateLimiter} is configured, each flush cycle is submitted
 * to the BBR for admission control.  If the pipeline is saturated (high CPU
 * and/or excessive in-flight batches), the flush is skipped and counters
 * remain in the local Caffeine cache for the next cycle.  This provides
 * adaptive back-pressure proportional to system load.
 */
@Slf4j
public class HotKeyReporter {

  /** Caffeine cache acting as a temporary counter store; entries evict after 30 s of inactivity. */
  private final Cache<String, LongAdder> counters = Caffeine.newBuilder()
    .expireAfterAccess(30, TimeUnit.SECONDS)
    .maximumSize(100_000)
    .build();
  /** Publishes aggregated reports to RabbitMQ. */
  private final ReportPublisher reportPublisher;
  /** Scheduler for the periodic flush loop. */
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
  private final ClusterHealthView healthView;

  /** Optional BBR adaptive rate limiter; null disables BBR gating. */
  @Setter
  private volatile BbrRateLimiter bbrRateLimiter;

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
  public HotKeyReporter(
    ReportPublisher reportPublisher,
    ScheduledExecutorService scheduler,
    long reportIntervalMs,
    String appName,
    int queueCapacity,
    int queueOfferTimeoutMs,
    int consumerCount,
    RingManager ringManager,
    ClusterHealthView healthView
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
   * a Caffeine cache that evicts after 30 s of inactivity, so low-frequency
   * keys are naturally forgotten without explicit cleanup.
   *
   * @param cacheKey the accessed key
   */
  public void record(String cacheKey) {
    counters.get(cacheKey, k -> new LongAdder()).increment();
  }

  /**
   * Start the periodic flush scheduler and the report dispatcher.
   * Idempotent — subsequent calls are silently ignored.
   *
   * <p>The flush loop drains the Caffeine counter map, groups entries by
   * target (shard index or nodeId), and enqueues them.  Actual publishing
   * to RabbitMQ runs on dedicated consumer threads, decoupling the flush
   * loop from network I/O.
   */
  public void start() {
    if (!started.compareAndSet(false, true)) {
      log.debug("HotKeyReporter already started, skip");
      return;
    }
    try {
      dispatcher = new ReportDispatcher();
      dispatcher.start();
      scheduler.scheduleAtFixedRate(this::flush, reportIntervalMs, reportIntervalMs, TimeUnit.MILLISECONDS);
      log.info(
        "HotKeyReporter started: appName={}, intervalMs={}, queueCapacity={}, consumers={}",
        appName,
        reportIntervalMs,
        queueCapacity,
        dispatcher.consumerCount()
      );
    } catch (Exception e) {
      log.error("Failed to start HotKeyReporter; per-key counts will not be flushed to Worker. " +
          "Application continues but Worker hot-key detection will be blind to this instance.", e);
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
    if (dispatcher != null) {
      dispatcher.shutdown();
    }
  }

  /**
   * Drain all locally accumulated counters, group by target, and enqueue
   * one {@link ShardBatch} per target for the consumer threads to publish.
   *
   * <p>In consistent-hashing mode, the ring is reconciled with the current
   * set of alive Worker nodes before grouping.
   *
   * <p>If the dispatcher queue is full, the batch for that target is
   * dropped (logged at WARN).  Caffeine eviction provides additional
   * backpressure — counters for cold keys are silently discarded.
   *
   * <p>When BBR rate limiting is active, {@code flush()} first checks
   * {@link BbrRateLimiter#tryAcquire()}.  If the limiter rejects the cycle,
   * the counters remain in the local cache and are merged with the next
   * flush.  This provides adaptive back-pressure proportional to system load.
   *
   * <p>Called periodically by the scheduler at {@code reportIntervalMs}.
   */
  private void flush() {
    try {
      if (counters.estimatedSize() == 0) {
        log.trace("Reporter flush tick: counters empty, no-op");
        return;
      }

      // Reconcile ring with alive Worker nodes (from heartbeat state), then route via consistent hash
      ringManager.reconcileFromHealthView(healthView);

      // Set BBR minInFlight to the current number of Workers so that multi-Worker deployments
      // don't trigger false-positive drops (each Worker produces one ShardBatch per flush).
      if (bbrRateLimiter != null) {
        bbrRateLimiter.setMinInFlight(ringManager.nodeCount());
        if (!bbrRateLimiter.tryAcquire()) {
          bbrRateLimiter.onGateDrop();
          return;
        }
      }

      Map<String, Map<String, Long>> sharded = new HashMap<>();
      long now = System.currentTimeMillis();

      counters
        .asMap()
        .forEach((key, adder) -> {
          long val = adder.sum();

          if (val > 0) {
            String target = ringManager.routeNode(key, healthView);

            if (target != null) {
              adder.sumThenReset();
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
      log.error("Scheduled reporter flush failed", e);
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
   * local Caffeine counter store.
   *
   * <p>This is an estimate provided by {@link Cache#estimatedSize()} and
   * may not reflect the exact count due to the concurrent nature of the
   * underlying data structure.
   *
   * @return estimated number of unique cache keys with pending access counts
   */
  public long getPendingKeyCount() {
    return counters.estimatedSize();
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
   * the flush loop from overwhelming RabbitMQ.  Backpressure propagates to
   * Caffeine eviction when the queue is persistently full.
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
        Thread t = new Thread(this::consumeLoop, "report-consumer-" + i);
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
     * <p>This method is called from {@link HotKeyReporter#stop()} and is
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
        if (System.currentTimeMillis() - batch.timestamp() > 5_000) {
          expiredCount.incrementAndGet();
          if (bbrRateLimiter != null) {
            bbrRateLimiter.onConsumerDrop();
          }
          continue;
        }

        try {
          reportPublisher.publish(batch.target(), new ReportMessage(appName, batch.timestamp(), batch.counts()));
          if (bbrRateLimiter != null) {
            bbrRateLimiter.onSuccess(System.currentTimeMillis() - batch.timestamp());
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
