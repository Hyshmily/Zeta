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
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;

import javax.security.auth.Destroyable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

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
 * {@link #DEFAULT_MAX_CEIL_SIZE}, the buffers are swapped eagerly to prevent any
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
  private static final int DEFAULT_MAX_CEIL_SIZE = 10_000;

  /** Default flush interval in milliseconds ({@value}). */
  private static final long DEFAULT_FLUSH_INTERVAL_MS = 500;

  /** Default eager swap ratio ({@value}). */
  private static final double DEFAULT_EAGER_SWAP_RATIO = 0.8;

  /** Maximum number of standby buffers ({@value}). */
  private static final int MAXS_CEILS = 64;

  private final int ceilMaxCapacity;

  private final long flushIntervalMs;

  /** Active hash bucket count (power of 2); volatile for count() visibility on expand/shrink. */
  private volatile int ceilCount = 8;

  private final AtomicReference<CounterBuffer>[] activeCeils;

  private final ConcurrentLinkedQueue<CounterBuffer> flushQueue;

  private final Consumer<Map<String, Long>> batchConsumer;

  private final ScheduledExecutorService scheduler;

  private final boolean ownsScheduler;

  private static final int MAX_STANDBY_BUFFERS = 3;

  /** Counter of ceil-swap events across all ceils, used as load signal for adjust. */
  private final LongAdder swapCounter = new LongAdder();

  /** Number of flush cycles accumulated in the current sample window. */
  private int sampleWindows;

  /** Accumulated swap counts within the sample window (read+reset from swapCounter each flush). */
  private int sampleHits;

  private volatile boolean shutdown;

  private final int eagerSwapThreshold;

  /**
   * Creates a buffered counter that flushes aggregated counts to the given consumer.
   * Creates its own single-thread scheduler with default parameters ({@value DEFAULT_MAX_CEIL_SIZE}
   * max keys, {@value DEFAULT_FLUSH_INTERVAL_MS} ms interval, {@value DEFAULT_EAGER_SWAP_RATIO} swap ratio).
   *
   * @param batchConsumer callback receiving the aggregated key-count map on each flush
   */
  public BufferedCounter(Consumer<Map<String, Long>> batchConsumer) {
    this(batchConsumer, DEFAULT_MAX_CEIL_SIZE, DEFAULT_FLUSH_INTERVAL_MS, DEFAULT_EAGER_SWAP_RATIO, true, null);
  }

  /**
   * Creates a buffered counter with an externally provided shared scheduler and default parameters.
   *
   * @param batchConsumer callback receiving the aggregated key-count map on each flush
   * @param scheduler     the shared scheduler (not shut down on destroy)
   */
  public BufferedCounter(Consumer<Map<String, Long>> batchConsumer, ScheduledExecutorService scheduler) {
    this(batchConsumer, DEFAULT_MAX_CEIL_SIZE, DEFAULT_FLUSH_INTERVAL_MS, DEFAULT_EAGER_SWAP_RATIO, false, scheduler);
  }

  /**
   * Creates a buffered counter with custom parameters and an externally provided shared scheduler.
   *
   * @param batchConsumer   callback receiving the aggregated key-count map on each flush
   * @param ceilMaxCapacity   maximum distinct keys in one buffer before forced eager swap
   * @param flushIntervalMs fixed delay between consecutive flushes in milliseconds
   * @param eagerSwapRatio  fraction of {@code maxBufferSize} that triggers an eager buffer swap (0.0 – 1.0)
   * @param scheduler       the shared scheduler (not shut down on destroy)
   */
  public BufferedCounter(
    Consumer<Map<String, Long>> batchConsumer,
    int ceilMaxCapacity,
    long flushIntervalMs,
    double eagerSwapRatio,
    ScheduledExecutorService scheduler
  ) {
    this(batchConsumer, ceilMaxCapacity, flushIntervalMs, eagerSwapRatio, false, scheduler);
  }

  @SuppressWarnings("unchecked")
  private BufferedCounter(
    Consumer<Map<String, Long>> batchConsumer,
    int ceilMaxCapacity,
    long flushIntervalMs,
    double eagerSwapRatio,
    boolean ownsScheduler,
    ScheduledExecutorService scheduler
  ) {
    this.batchConsumer = batchConsumer;
    this.ceilMaxCapacity = ceilMaxCapacity;
    this.flushIntervalMs = flushIntervalMs;
    eagerSwapThreshold = (int) (ceilMaxCapacity * eagerSwapRatio);

    this.activeCeils = new AtomicReference[MAXS_CEILS];
    for (int i = 0; i < MAXS_CEILS; i++) {
      this.activeCeils[i] = new AtomicReference<>(new CounterBuffer());
    }
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

    int idx = key.hashCode() & (ceilCount - 1);
    CounterBuffer seg = activeCeils[idx].get();
    seg.add(key, delta);

    if (seg.size() >= eagerSwapThreshold) {
      trySwap(idx, seg);
      swapCounter.increment(); // signal for ceil-count adjust
    }
  }

  /**
   * Return an approximate count of distinct keys request in the active buffer.
   *
   * @return number of distinct keys in the active buffer
   */
  public long estimatedSizeOfKeysCount() {
    int count = 0;
    for (int i = 0; i < ceilCount; i++) {
      count += activeCeils[i].get().size();
    }
    return count;
  }

  /**
   * Drain all remaining counts from both buffers without calling the consumer.
   * After this call both buffers are empty and ready for reuse.
   */
  public void clear() {
    for (int i = 0; i < ceilCount; i++) {
      activeCeils[i].getAndSet(new CounterBuffer()).drain();
    }
    CounterBuffer buf;
    while ((buf = flushQueue.poll()) != null) {
      buf.drain();
    }
  }

  /**
   * Try to switch the active buffer with a new one and move the old one to standby for flushing.
   *
   * <p>If the standby queue is full, all enqueued buffers are compacted into one
   * (the oldest) and the rest are drained and merged into it.  This preserves all
   * counts, reclaims queue slots, and avoids discarding entire buffers.  The
   * overhead is O(total standby keys) and occurs on the hot path only when the
   * queue is persistently backed up — i.e. switch rate > flush rate.
   */
  private void trySwap(int idx, CounterBuffer buffer) {
    if (buffer != activeCeils[idx].get() || !activeCeils[idx].compareAndSet(buffer, new CounterBuffer())) {
      return; // another thread already swapped
    }

    if (flushQueue.size() >= MAX_STANDBY_BUFFERS) {
      CounterBuffer oldest = flushQueue.poll();
      if (oldest != null) {
        drainBuffer(oldest);
      }
    }

    flushQueue.offer(buffer);
  }

  private void flushStandby() {
    try {
      // Drain standby queue first — these are buffers eagerly swapped on the hot path
      CounterBuffer buf;
      while ((buf = flushQueue.poll()) != null) {
        drainBuffer(buf);
      }

      // Sample swap events since last flush; window = current ceilCount
      int current = ceilCount;
      sampleWindows++;
      sampleHits += (int) swapCounter.sumThenReset();

      // Adjust ceil count if sample window is complete
      if (sampleWindows >= current) {
        double hitRatio = (double) sampleHits / current;
        if (hitRatio >= 0.75 && current < MAXS_CEILS) {
          ceilCount = current << 1; // expand: more than 75% of flushes had swaps
        } else if (hitRatio < 0.25 && current > 1) {
          ceilCount = current >> 1; // shrink: fewer than 25%
        }
        sampleWindows = 0;
        sampleHits = 0;
      }

      // Drain active ceils (respects the updated ceilCount after expand/shrink)
      for (int i = 0; i < ceilCount; i++) {
        drainBuffer(activeCeils[i].getAndSet(new CounterBuffer()));
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

    for (int i = 0; i < ceilCount; i++) {
      drainBuffer(activeCeils[i].getAndSet(new CounterBuffer()));
    }
    CounterBuffer buf;
    while ((buf = flushQueue.poll()) != null) {
      drainBuffer(buf);
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
    int totalSize = 0;
    for (int i = 0; i < ceilCount; i++) {
      totalSize += activeCeils[i].get().size();
    }
    return (double) totalSize / (ceilMaxCapacity * ceilCount);
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
    public void add(String key, long delta) {
      counters.computeIfAbsent(key, k -> new LongAdder()).add(delta);
    }

    /**
     * Return the number of distinct keys held in this buffer.
     *
     * @return the number of distinct keys
     */
    public int size() {
      return counters.size();
    }

    /**
     * Return whether this buffer holds no entries.
     *
     * @return {@code true} if the buffer is empty
     */
    public boolean isEmpty() {
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
    public Map<String, Long> drain() {
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
