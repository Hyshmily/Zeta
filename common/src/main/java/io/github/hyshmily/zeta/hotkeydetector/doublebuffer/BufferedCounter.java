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
package io.github.hyshmily.zeta.hotkeydetector.doublebuffer;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.util.ZetaThreadFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import javax.security.auth.Destroyable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;

/**
 * Double-buffered counter that aggregates high-frequency single-key increments
 * and flushes them in batch to a downstream consumer.
 *
 * <p><b>Design:</b> One active {@link CounterBuffer} accepts incoming
 * {@link #count(String, long)} calls via a lock-free {@code ConcurrentHashMap}.
 * When the buffer is saturated the active reference is atomically swapped and
 * the old buffer is enqueued into a {@link ConcurrentLinkedQueue}. A scheduled
 * flusher periodically drains both the swapped-out active buffer and the queue
 * into the downstream consumer (see {@link #flushStandby()}).
 *
 * <p><b>Eager swap:</b> When the active buffer exceeds 80 % of
 * {@link #DEFAULT_MAX_BUFFER_SIZE}, the buffers are swapped eagerly to prevent any
 * single buffer from growing unbounded under a traffic spike. The hot path
 * (the {@code count} call) remains lock-free — it only does an atomic
 * {@code getAndSet} on the active reference.
 *
 * <p><b>Lifecycle:</b> Implements {@link InitializingBean} to start the
 * periodic flush scheduler, and {@link Destroyable} to perform a final drain
 * on shutdown. The scheduler is either self-created (and owned) or externally
 * provided (shared), controlling whether {@link #destroy()} shuts it down.
 *
 * <p>Thread-safe. All public methods can be called concurrently from
 * multiple threads.
 */
@Slf4j
@Internal
public class BufferedCounter implements InitializingBean, Destroyable {

  /** Default maximum distinct keys in one buffer before forced swap ({@value}). */
  static final int DEFAULT_MAX_BUFFER_SIZE = 10_000;

  /** Default flush interval in milliseconds ({@value}). */
  static final long DEFAULT_FLUSH_INTERVAL_MS = 500;

  /** Default eager swap ratio ({@value}). */
  static final double DEFAULT_EAGER_SWAP_RATIO = 0.8;

  private final int maxBufferSize;

  private final long flushIntervalMs;

  private final double eagerSwapRatio;

  private final AtomicReference<CounterBuffer> active;

  private final ConcurrentLinkedQueue<CounterBuffer> flushQueue;

  private final Consumer<Map<String, Long>> batchConsumer;

  private final ScheduledExecutorService scheduler;

  private final boolean ownsScheduler;

  private static final int MAX_STANDBY_BUFFERS = 3;

  private volatile boolean shutdown;

  /**
   * Creates a buffered counter that flushes aggregated counts to the given consumer.
   * Creates its own single-thread scheduler with default parameters ({@value DEFAULT_MAX_BUFFER_SIZE}
   * max keys, {@value DEFAULT_FLUSH_INTERVAL_MS} ms interval, {@value DEFAULT_EAGER_SWAP_RATIO} swap ratio).
   *
   * @param batchConsumer callback receiving the aggregated key-count map on each flush
   */
  public BufferedCounter(Consumer<Map<String, Long>> batchConsumer) {
    this(batchConsumer, DEFAULT_MAX_BUFFER_SIZE, DEFAULT_FLUSH_INTERVAL_MS, DEFAULT_EAGER_SWAP_RATIO, true, null);
  }

  /**
   * Creates a buffered counter with an externally provided shared scheduler and default parameters.
   *
   * @param batchConsumer callback receiving the aggregated key-count map on each flush
   * @param scheduler     the shared scheduler (not shut down on destroy)
   */
  public BufferedCounter(Consumer<Map<String, Long>> batchConsumer, ScheduledExecutorService scheduler) {
    this(batchConsumer, DEFAULT_MAX_BUFFER_SIZE, DEFAULT_FLUSH_INTERVAL_MS, DEFAULT_EAGER_SWAP_RATIO, false, scheduler);
  }

  /**
   * Creates a buffered counter with custom parameters and an externally provided shared scheduler.
   *
   * @param batchConsumer   callback receiving the aggregated key-count map on each flush
   * @param maxBufferSize   maximum distinct keys in one buffer before forced eager swap
   * @param flushIntervalMs fixed delay between consecutive flushes in milliseconds
   * @param eagerSwapRatio  fraction of {@code maxBufferSize} that triggers an eager buffer swap (0.0 – 1.0)
   * @param scheduler       the shared scheduler (not shut down on destroy)
   */
  public BufferedCounter(
    Consumer<Map<String, Long>> batchConsumer,
    int maxBufferSize,
    long flushIntervalMs,
    double eagerSwapRatio,
    ScheduledExecutorService scheduler
  ) {
    this(batchConsumer, maxBufferSize, flushIntervalMs, eagerSwapRatio, false, scheduler);
  }

  private BufferedCounter(
    Consumer<Map<String, Long>> batchConsumer,
    int maxBufferSize,
    long flushIntervalMs,
    double eagerSwapRatio,
    boolean ownsScheduler,
    ScheduledExecutorService scheduler
  ) {
    this.batchConsumer = batchConsumer;
    this.maxBufferSize = maxBufferSize;
    this.flushIntervalMs = flushIntervalMs;
    this.eagerSwapRatio = eagerSwapRatio;
    this.active = new AtomicReference<>(new CounterBuffer());
    this.flushQueue = new ConcurrentLinkedQueue<>();
    this.ownsScheduler = ownsScheduler;
    this.scheduler = ownsScheduler
      ? Executors.newSingleThreadScheduledExecutor(new ZetaThreadFactory("zeta-buffered-counter-flusher"))
      : scheduler;
  }

  /**
   * Record one or more accesses for the given key into the active buffer.
   *
   * <p>This is the hot path — it performs a lock-free update on the active
   * {@link CounterBuffer}. If the active buffer exceeds 80 % capacity
   * after this increment, an eager swap is triggered to keep the buffer
   * from overflowing before the next scheduled flush.
   *
   * @param key   the accessed key (must not be {@code null})
   * @param delta the number of accesses to reportToWorker (must be positive)
   */
  public void count(String key, long delta) {
    if (shutdown) {
      return;
    }
    CounterBuffer buffer = active.get();
    buffer.add(key, delta);

    if (buffer.size() >= maxBufferSize * eagerSwapRatio) {
      trySwitch();
    }
  }

  /**
   * Return an approximate count of distinct keys request in the active buffer.
   *
   * @return number of distinct keys in the active buffer
   */
  public long estimatedSizeOfKeysCount() {
    return active.get().size();
  }

  /**
   * Drain all remaining counts from both buffers without calling the consumer.
   * After this call both buffers are empty and ready for reuse.
   */
  public void clear() {
    active.getAndSet(new CounterBuffer()).drain();
    CounterBuffer buf;
    while ((buf = flushQueue.poll()) != null) {
      buf.drain();
    }
  }

  /**
   * try to switch the active buffer with a new one and move the old one to standby for flushing.
   */
  private void trySwitch() {
    CounterBuffer newBuffer = new CounterBuffer();
    CounterBuffer oldBuffer = active.getAndSet(newBuffer);
    if (oldBuffer != null) {
      if (flushQueue.size() >= MAX_STANDBY_BUFFERS) {
        CounterBuffer victim = flushQueue.poll();
        if (victim != null) {
          victim.drain();
          log.warn("Flush queue is full, dropping buffer with {} keys", victim.size());
        }
      }
      flushQueue.offer(oldBuffer);
    }
  }

  private void flushStandby() {
    try {
      CounterBuffer flushedActive = active.getAndSet(new CounterBuffer());
      drainBuffer(flushedActive);

      CounterBuffer buf;
      while ((buf = flushQueue.poll()) != null) {
        drainBuffer(buf);
      }
    } catch (Exception e) {
      log.error("Scheduled flushStandby failed", e);
    }
  }

  private void drainBuffer(CounterBuffer buf) {
    if (!buf.isEmpty()) {
      Map<String, Long> snapshot = buf.drain();
      if (!snapshot.isEmpty()) {
        batchConsumer.accept(snapshot);
      }
    }
  }

  /**
   * Start the periodic flush scheduler.  Called by the Spring container
   * after all bean properties have been set.
   */
  @Override
  public void afterPropertiesSet() {
    try {
      scheduler.scheduleAtFixedRate(this::flushStandby, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      log.error(
        "Failed to start BufferedCounter flush scheduler; buffered counts will not " +
          "be flushed to HeavyKeeper. Hot-key detection may be impaired.",
        e
      );
    }
  }

  /**
   * Perform a final drain of any remaining buffered counts and shut down
   * the scheduler only if owned (self-created). For a shared scheduler
   * the task is simply cancelled.
   *
   * <p>Called by the Spring container during context close.
   */
  @Override
  public void destroy() {
    shutdown = true;

    if (ownsScheduler) {
      scheduler.shutdown();
      try {
        if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
          scheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        scheduler.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    for (int i = 0; i < 3; i++) {
      trySwitch();
      flushStandby();
      if (active.get().isEmpty() && flushQueue.isEmpty()) {
        break;
      }
    }
  }

  /**
   * Returns the ratio of the active buffer's current distinct-key count
   * to {@code maxBufferSize}. A value {@code >= 0.8} (the default
   * {@code eagerSwapRatio}) indicates the buffer is close to triggering
   * an eager swap.
   *
   * @return saturation ratio in the {@code [0, 1+)} range
   */
  public double activeBufferSaturation() {
    return (double) active.get().size() / maxBufferSize;
  }

  private static class CounterBuffer {

    private final ConcurrentHashMap<String, LongAdder> counters = new ConcurrentHashMap<>();

    /** Reusable result map — avoids allocation on every drain cycle. */
    private Map<String, Long> reusableResult;

    /**
     * Record one or more accesses for the given key in this buffer.
     *
     * @param key   the accessed key
     * @param delta the number of accesses to reportToWorker
     */
    void add(String key, long delta) {
      counters.computeIfAbsent(key, k -> new LongAdder()).add(delta);
    }

    /**
     * Return the number of distinct keys held in this buffer.
     *
     * @return the number of distinct keys
     */
    int size() {
      return counters.size();
    }

    /**
     * Return whether this buffer holds no entries.
     *
     * @return {@code true} if the buffer is empty
     */
    boolean isEmpty() {
      return size() == 0;
    }

    /**
     * Atomically drain all counters and return a snapshot of the accumulated
     * counts. Each LongAdder is zeroed ({@code sumThenReset}),
     * but the key entries remain in the map ({@code size()} unchanged).
     * The caller must discard this instance after draining — it is not
     * reused in place.
     *
     * <p>The returned map is reused between calls to reduce GC pressure.
     * The caller must not retain a reference beyond the synchronous
     * {@code batchConsumer.accept()} callback.
     *
     * @return a map of keys to their accumulated counts, never {@code null}
     */
    Map<String, Long> drain() {
      Map<String, LongAdder> oldCounters = counters;

      if (reusableResult == null) {
        reusableResult = new HashMap<>(oldCounters.size());
      } else {
        reusableResult.clear();
      }
      oldCounters.forEach((key, adder) -> {
        long val = adder.sumThenReset();
        if (val > 0) {
          reusableResult.put(key, val);
        }
      });
      return reusableResult;
    }
  }
}
