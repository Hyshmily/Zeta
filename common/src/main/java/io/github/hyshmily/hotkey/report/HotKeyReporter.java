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
package io.github.hyshmily.hotkey.report;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
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
import lombok.RequiredArgsConstructor;
import io.github.hyshmily.hotkey.log.DefaultLogger;
import io.github.hyshmily.hotkey.log.HotKeyLogger;

/**
 * Periodically aggregates per-key access counts and publishes them
 * to the Worker via {@link ReportPublisher}.
 *
 * <p>Uses a Caffeine cache as a temporary counter store; entries are
 * evicted after 30 seconds of inactivity to bound memory usage.
 * Flushed to the appropriate shard at a fixed interval.
 *
 * <p>Burst absorption and backpressure are provided by a bounded
 * {@link LinkedBlockingQueue} between the flush loop and the
 * RabbitMQ publisher.  When the queue is full, {@code flush()} drops
 * batches after a configurable timeout, and the Caffeine eviction
 * provides natural rate-limiting.
 */
@RequiredArgsConstructor
public class HotKeyReporter {

  private static final HotKeyLogger log = new DefaultLogger(HotKeyReporter.class);

  private final Cache<String, LongAdder> counters = Caffeine.newBuilder()
    .expireAfterAccess(30, TimeUnit.SECONDS)
    .maximumSize(100_000)
    .build();
  private final ReportPublisher reportPublisher;
  private final ScheduledExecutorService scheduler;
  private final long reportIntervalMs;
  private final int shardCount;
  private final String appName;
  private final int queueCapacity;
  private final int queueOfferTimeoutMs;
  private final int consumerCount;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private ReportDispatcher dispatcher;

  /**
   * A batch of key-count mappings destined for a single Worker shard.
   *
   * @param shard     target shard index
   * @param timestamp wall-clock ms when the batch was assembled
   * @param counts    non-zero per-key counts accumulated since the last flush
   */
  record ShardBatch(int shard, long timestamp, Map<String, Long> counts) {}

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
    counters.get(cacheKey, _ -> new LongAdder()).increment();
  }

  /**
   * Start the periodic flush scheduler and the report dispatcher.
   * Idempotent — subsequent calls are silently ignored.
   *
   * <p>The flush loop drains the Caffeine counter map, groups entries by
   * shard, and enqueues them.  Actual publishing to RabbitMQ runs on
   * dedicated consumer threads, decoupling the flush loop from network I/O.
   */
  public void start() {
    if (!started.compareAndSet(false, true)) {
      log.debug("HotKeyReporter already started, skip");
      return;
    }
    dispatcher = new ReportDispatcher();
    dispatcher.start();
    scheduler.scheduleAtFixedRate(this::flush, reportIntervalMs, reportIntervalMs, TimeUnit.MILLISECONDS);
    log.info(
      "HotKeyReporter started: appName={}, shardCount={}, intervalMs={}, queueCapacity={}, consumers={}",
      appName,
      shardCount,
      reportIntervalMs,
      queueCapacity,
      dispatcher.consumerCount()
    );
  }

  /** Stop the dispatcher. Called via destroyMethod on the bean. */
  public void stop() {
    if (dispatcher != null) {
      dispatcher.shutdown();
    }
  }

  /**
   * Drain all locally accumulated counters, group by shard, and enqueue
   * one {@link ShardBatch} per shard for the consumer threads to publish.
   *
   * <p>If the dispatcher queue is full, the batch for that shard is
   * dropped (logged at WARN).  Caffeine eviction provides additional
   * backpressure — counters for cold keys are silently discarded.
   *
   * <p>Called periodically by the scheduler at {@code reportIntervalMs}.
   */
  private void flush() {
    if (counters.estimatedSize() == 0) {
      return;
    }

    Map<Integer, Map<String, Long>> sharded = new HashMap<>();
    long now = System.currentTimeMillis();

    counters
      .asMap()
      .forEach((key, adder) -> {
        long val = adder.sumThenReset();
        if (val > 0) {
          int shard = Math.floorMod(key.hashCode(), shardCount);
          sharded.computeIfAbsent(shard, _ -> new HashMap<>()).put(key, val);
        }
      });

    sharded.forEach((shard, counts) -> {
      if (!dispatcher.enqueue(new ShardBatch(shard, now, counts))) {
        log.warn(
          "report queue full, dropped shard={} keys={}, depth={}/{}",
          shard,
          counts.size(),
          dispatcher.depth(),
          queueCapacity
        );
      }
    });
  }

  /**
   * @return current backlog depth in the dispatcher queue, or {@code -1} before start
   */
  public int dispatcherDepth() {
    return dispatcher == null ? -1 : dispatcher.depth();
  }

  /**
   * @return capacity of the dispatcher queue
   */
  public int dispatcherCapacity() {
    return dispatcher == null ? -1 : dispatcher.capacity();
  }

  /**
   * @return total batches discarded because they waited longer than 5 s in the queue
   */
  public long dispatcherExpired() {
    return dispatcher == null ? -1 : dispatcher.expired();
  }

  /**
   * @return total batches rejected because the queue was full (backpressure drops)
   */
  public long dispatcherDropped() {
    return dispatcher == null ? -1 : dispatcher.dropped();
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

    private final BlockingQueue<ShardBatch> queue = new LinkedBlockingQueue<>(queueCapacity);
    private final AtomicLong expiredCount = new AtomicLong();
    private final AtomicLong droppedCount = new AtomicLong();
    private final List<Thread> consumers = new ArrayList<>();
    private volatile boolean running;

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
     * Offer a batch to the queue.  Blocks for up to {@code queueOfferTimeoutMs}.
     *
     * @param batch the sharded batch to publish
     * @return {@code true} if accepted, {@code false} if the queue was full
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

    /** Interrupt all consumer threads and wait for them to finish. */
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

    /** Current number of batches waiting in the queue. */
    int depth() {
      return queue.size();
    }

    /** Maximum number of batches the queue can hold. */
    int capacity() {
      return queueCapacity;
    }

    /** Number of active consumer threads. */
    int consumerCount() {
      return consumers.size();
    }

    /** Total batches expired (removed after TTL) since startup. */
    long expired() {
      return expiredCount.get();
    }

    /** Total batches dropped (queue full) since startup. */
    long dropped() {
      return droppedCount.get();
    }

    /**
     * Consumer thread body: poll batches from the queue and publish them.
     *
     * <p>Staleness guard: batches that waited longer than 5 s in the queue
     * are discarded rather than published, preventing the Worker from receiving
     * stale access patterns during prolonged backpressure.
     */
    private void consumeLoop() {
      while (running) {
        try {
          ShardBatch batch = queue.poll(1, TimeUnit.SECONDS);
          if (batch == null) {
            continue;
          }

          // 5s stale check — discard data that waited too long in the queue
          if (System.currentTimeMillis() - batch.timestamp() > 5_000) {
            expiredCount.incrementAndGet();
            continue;
          }

          reportPublisher.publish(batch.shard(), new ReportMessage(appName, batch.timestamp(), batch.counts()));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
          log.error("Report publish failed, continuing", e);
        }
      }
    }
  }
}
