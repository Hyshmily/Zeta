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

import org.springframework.beans.factory.InitializingBean;
import org.springframework.scheduling.annotation.Scheduled;

import javax.security.auth.Destroyable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * Double-buffered counter that aggregates high-frequency single-key increments
 * and flushes them in batch to a downstream consumer.
 *
 * <p>An active buffer accepts incoming {@link #count(String, long)} calls while
 * a standby buffer is drained by a scheduled flusher.  When the active buffer
 * exceeds 80 % capacity the buffers are swapped eagerly, keeping the hot path
 * lock-free.
 */
public class BufferedCounter implements InitializingBean, Destroyable {
    /** Maximum number of distinct keys held in one buffer before forced swap ({@value}). */
    private static final int MAX_BUFFER_SIZE = 10_000;

    /** Fixed flush interval in milliseconds ({@value}). */
    private static final long FLUSH_INTERVAL_MS = 500;

    private final AtomicReference<CounterBuffer> active;

    private volatile CounterBuffer standby;

    private final Consumer<Map<String, Long>> batchConsumer;

    private final ScheduledExecutorService scheduler;

    /**
     * Creates a buffered counter that flushes aggregated counts to the given consumer.
     *
     * @param batchConsumer callback receiving the aggregated key-count map on each flush
     */
    public BufferedCounter(Consumer<Map<String, Long>> batchConsumer) {
        this.batchConsumer = batchConsumer;
        this.active = new AtomicReference<>(new CounterBuffer());
        this.standby = new CounterBuffer();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                r -> new Thread(r, "buffered-counter-flusher"));
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

    private CounterBuffer trySwitch() {
        CounterBuffer newBuffer = new CounterBuffer();
        CounterBuffer oldBuffer = active.getAndSet(newBuffer);
        if (oldBuffer != null) {
            this.standby = oldBuffer;
        }
        return oldBuffer;
    }

    @Scheduled(fixedRate = FLUSH_INTERVAL_MS)
    private void flushStandby() {
        CounterBuffer readyTOFlush = trySwitch();
        if (readyTOFlush.isEmpty()) {
            return;
        }

        Map<String, Long> snapshot = readyTOFlush.Cohesion();
        if (!snapshot.isEmpty()) {
            batchConsumer.accept(snapshot);
        }
    }

    /**
     * Start the periodic flush scheduler.  Called by the Spring container
     * after all bean properties have been set.
     */
    @Override
    public void afterPropertiesSet() {
        scheduler.scheduleAtFixedRate(this::flushStandby,
                FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Shut down the flush scheduler and perform a final drain of any
     * remaining buffered counts.  Called by the Spring container during
     * context close.
     */
    @Override
    public void destroy() {
        scheduler.shutdown();
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
        Map<String, Long> Cohesion() {

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
