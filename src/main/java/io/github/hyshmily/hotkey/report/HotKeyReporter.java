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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * Periodically aggregates per-key access counts and publishes them
 * to the Worker via {@link ReportPublisher}.
 *
 * <p>Uses a Caffeine cache as a temporary counter store; entries are
 * evicted after 30 seconds of inactivity to bound memory usage.
 * Flushed to the appropriate shard at a fixed interval.
 */
@Slf4j
@RequiredArgsConstructor
public class HotKeyReporter {

  private final Cache<String, LongAdder> counters = Caffeine.newBuilder()
    .expireAfterAccess(30, TimeUnit.SECONDS)
    .maximumSize(100_000)
    .build();
  private final ReportPublisher reportPublisher;
  private final ScheduledExecutorService scheduler;
  private final long reportIntervalMs;
  private final int shardCount;
  private final String appName;
  private final AtomicBoolean started = new AtomicBoolean(false);

  /**
   * Record one access for the given cache key.
   *
   * @param cacheKey the accessed key
   */
  public void record(String cacheKey) {
    counters.get(cacheKey, _ -> new LongAdder()).increment();
  }

  /**
   * Start the periodic flush scheduler.  Idempotent — subsequent calls
   * are silently ignored.
   */
  public void start() {
    if (!started.compareAndSet(false, true)) {
      log.debug("HotKeyReporter already started, skip");
      return;
    }
    scheduler.scheduleAtFixedRate(this::flush, reportIntervalMs, reportIntervalMs, TimeUnit.MILLISECONDS);
    log.info("HotKeyReporter started: appName={}, shardCount={}, intervalMs={}", appName, shardCount, reportIntervalMs);
  }

  private void flush() {
    if (counters.estimatedSize() == 0) {
      return;
    }

    Map<Integer, Map<String, Long>> sharded = new HashMap<>();
    counters
      .asMap()
      .forEach((key, adder) -> {
        long val = adder.sumThenReset();

        if (val > 0) {
          int shard = Math.abs(key.hashCode()) % shardCount;
          sharded.computeIfAbsent(shard, _ -> new HashMap<>()).put(key, val);
        }
      });

    sharded.forEach((shard, counts) ->
      reportPublisher.publish(shard, new ReportMessage(appName, System.currentTimeMillis(), counts))
    );
  }
}
