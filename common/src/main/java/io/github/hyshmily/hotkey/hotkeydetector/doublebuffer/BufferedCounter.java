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
package io.github.hyshmily.hotkey.hotkeydetector.doublebuffer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
 * <p>An active buffer accepts incoming {@link #count(String, long)} calls while
 * a standby buffer is drained by a scheduled flusher.  When the active buffer
 * exceeds 80 % capacity the buffers are swapped eagerly, keeping the hot path
 * lock-free.
 */
@Slf4j
public class BufferedCounter implements InitializingBean, Destroyable {

  /** Maximum number of distinct keys held in one buffer before forced swap ({@value}). */
  private static final int MAX_BUFFER_SIZE = 10_000;

  /** Fixed flush interval in milliseconds ({@value}). */
  private static final long FLUSH_INTERVAL_MS = 500;

  private final AtomicReference<CounterBuffer> active;

  private final AtomicReference<CounterBuffer> standbyRef;

  private final Consumer<Map<String, Long>> batchConsumer;

  private final ScheduledExecutorService scheduler;

  private final boolean ownsScheduler;

  /**
   * Creates a buffered counter that flushes aggregated counts to the given consumer.
   * Creates its own single-thread scheduler (backward-compatible).
   *
   * @param batchConsumer callback receiving the aggregated key-count map on each flush
   */
  public BufferedCounter(Consumer<Map<String, Long>> batchConsumer) {
    this(
      batchConsumer,
      Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "buffered-counter-flusher")),
      true
    );
  }

  /**
   * Creates a buffered counter with an externally provided scheduler.
   *
   * @param batchConsumer callback receiving the aggregated key-count map on each flush
   * @param scheduler     the shared scheduler (not shut down on destroy)
   */
  public BufferedCounter(Consumer<Map<String, Long>> batchConsumer, ScheduledExecutorService scheduler) {
    this(batchConsumer, scheduler, false);
  }

  private BufferedCounter(
    Consumer<Map<String, Long>> batchConsumer,
    ScheduledExecutorService scheduler,
    boolean ownsScheduler
  ) {
    this.batchConsumer = batchConsumer;
    this.active = new AtomicReference<>(new CounterBuffer());
    this.standbyRef = new AtomicReference<>(new CounterBuffer());
    this.scheduler = scheduler;
    this.ownsScheduler = ownsScheduler;
  }

  /**
   * Record one or more accesses for the given key.
   *
   * @param key   the accessed key
   * @param delta the number of accesses to record
   */
  public void count(String key, long delta) {
    CounterBuffer buffer = active.get();
    buffer.add(key, delta);

    if (buffer.size() >= MAX_BUFFER_SIZE * 0.8) {
      trySwitch();
    }
  }

  private void trySwitch() {
    CounterBuffer newBuffer = new CounterBuffer();
    CounterBuffer oldBuffer = active.getAndSet(newBuffer);
    if (oldBuffer != null) {
      standbyRef.set(oldBuffer);
    }
  }

  private void flushStandby() {
    try {
      // Swap active and standby independently to avoid races between
      // count()->trySwitch() and the scheduled flush.
      CounterBuffer flushedActive = active.getAndSet(new CounterBuffer());
      CounterBuffer flushedStandby = standbyRef.getAndSet(new CounterBuffer());

      drainBuffer(flushedActive);
      drainBuffer(flushedStandby);
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
    scheduler.scheduleAtFixedRate(this::flushStandby, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
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
    if (ownsScheduler) {
      scheduler.shutdown();
    }
    // Swap any remaining active data into standby and flush both
    trySwitch();
    flushStandby();
  }

  private static class CounterBuffer {

    private final ConcurrentHashMap<String, LongAdder> counters = new ConcurrentHashMap<>();
    private final LongAdder totalSize = new LongAdder();

    /**
     * Record one or more accesses for the given key in this buffer.
     *
     * @param key   the accessed key
     * @param delta the number of accesses to record
     */
    void add(String key, long delta) {
      counters.computeIfAbsent(key, k -> new LongAdder()).add(delta);
      totalSize.add(delta);
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
      return counters.isEmpty();
    }

    /**
     * Atomically drain all counters and return a snapshot of the accumulated
     * counts. After this call the buffer is empty and ready for reuse.
     *
     * @return a map of keys to their accumulated counts, never {@code null}
     */
    Map<String, Long> drain() {
      Map<String, LongAdder> oldCounters = counters;

      Map<String, Long> result = new HashMap<>(oldCounters.size());
      oldCounters.forEach((key, adder) -> {
        long val = adder.sumThenReset();
        if (val > 0) {
          result.put(key, val);
        }
      });
      return result;
    }
  }
}
