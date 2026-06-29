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
package io.github.hyshmily.hotkey.cache;

import static io.github.hyshmily.hotkey.util.TimeSource.currentTimeMillis;

import io.github.hyshmily.hotkey.autoconfigure.HotKeyProperties;
import io.github.hyshmily.hotkey.util.HotKeyThreadFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Sliding-window circuit breaker for protecting remote calls from cascading failures.
 *
 * <p>Uses a rolling time window divided into buckets ({@link LongAdder}s) to count
 * successes and failures. When the failure rate exceeds {@code failThreshold} and the
 * total request volume meets {@code requestVolumeThreshold}, the breaker opens.
 * After {@code singleTestIntervalMs} in the open state, a single probe request is
 * allowed through (half-open). If that probe succeeds, the breaker closes; if it
 * fails, the breaker stays open.
 *
 * <p>This class is thread-safe.
 */
@Slf4j
public class HotKeyCircuitBreaker implements AutoCloseable {

  private static final ScheduledExecutorService SCHEDULER = new ScheduledThreadPoolExecutor(
    Runtime.getRuntime().availableProcessors(),
    new HotKeyThreadFactory("hotkey-cb")
  );

  private final HotKeyProperties.CircuitBreaker config;
  private final int bucketSize;
  private final LongAdder[] successBuckets;
  private final LongAdder[] failBuckets;
  private volatile int currentIndex;
  private final AtomicBoolean open = new AtomicBoolean(false);
  private volatile long openTimestamp;
  private final AtomicLong lastHalfOpenAttempt = new AtomicLong(0L);
  private final ScheduledFuture<?> slideFuture;

  /**
   * Creates a new circuit breaker with sliding-window failure tracking.
   * <p>
   * Initialises the success/failure bucket ring and schedules a periodic
   * slide task on the shared {@link #SCHEDULER} to advance the window.
   *
   * @param config the circuit breaker configuration (window size, thresholds, etc.)
   */
  public HotKeyCircuitBreaker(HotKeyProperties.CircuitBreaker config) {
    this.config = config;
    this.bucketSize = config.getWindowBuckets();
    this.successBuckets = new LongAdder[bucketSize];
    this.failBuckets = new LongAdder[bucketSize];
    for (int i = 0; i < bucketSize; i++) {
      successBuckets[i] = new LongAdder();
      failBuckets[i] = new LongAdder();
    }
    long slideMs = config.getWindowTimeMs() / bucketSize;
    this.slideFuture = SCHEDULER.scheduleAtFixedRate(this::slide, slideMs, slideMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Check whether a request is allowed through.
   * When closed: always allowed. When open: allowed only for half-open probe.
   *
   * @return {@code true} if the request may proceed
   */
  public boolean allowRequest() {
    if (!config.isEnabled()) {
      if (open.get()) {
        open.set(false);
      }
      return true;
    }
    if (!open.get()) {
      return true;
    }

    long now = currentTimeMillis();
    long lastTest = lastHalfOpenAttempt.get();
    if (
      now - openTimestamp > config.getSingleTestIntervalMs() &&
      now - lastTest > config.getSingleTestIntervalMs() &&
      lastHalfOpenAttempt.compareAndSet(lastTest, now)
    ) {
      if (config.isLogEnabled()) {
        log.info("CB half-open probe");
      }
      return true;
    }
    return false;
  }

  /** Record a successful call. If the breaker was open, attempt to close it. */
  public void onSuccess() {
    if (!config.isEnabled()) {
      return;
    }
    successBuckets[currentIndex].increment();
    if (open.get() && open.compareAndSet(true, false)) {
      resetAllBuckets();
      if (config.isLogEnabled()) {
        log.info("CB closed");
      }
    }
  }

  /** Record a failed call. */
  public void onFailure() {
    if (config.isEnabled()) {
      failBuckets[currentIndex].increment();
      evaluateThreshold();
    }
  }

  /** Whether the breaker is currently open. */
  public boolean isOpen() {
    return open.get() && config.isEnabled();
  }

  private void slide() {
    int next = (currentIndex + 1) % bucketSize;
    successBuckets[next].reset();
    failBuckets[next].reset();
    currentIndex = next;
    evaluateThreshold();
  }

  private void evaluateThreshold() {
    long totalSuccess = 0, totalFail = 0;

    for (LongAdder a : successBuckets) {
      totalSuccess += a.sum();
    }
    for (LongAdder a : failBuckets) {
      totalFail += a.sum();
    }

    if (totalFail > 0 && totalSuccess + totalFail >= config.getRequestVolumeThreshold()) {
      double rate = (double) totalFail / (totalSuccess + totalFail);

      if (rate > config.getFailThreshold() && open.compareAndSet(false, true)) {
        openTimestamp = currentTimeMillis();

        if (config.isLogEnabled()) {
          log.info("CB OPEN (failRate={}, total={})", rate, totalSuccess + totalFail);
        }
      }
    }
  }

  private void resetAllBuckets() {
    for (LongAdder a : successBuckets) {
      a.reset();
    }
    for (LongAdder a : failBuckets) {
      a.reset();
    }
  }

  @Override
  public void close() {
    if (slideFuture != null) {
      slideFuture.cancel(false);
    }
  }
}
