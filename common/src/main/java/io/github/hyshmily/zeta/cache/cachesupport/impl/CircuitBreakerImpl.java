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
package io.github.hyshmily.zeta.cache.cachesupport.impl;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.CircuitBreaker;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import static io.github.hyshmily.zeta.util.TimeSource.currentTimeMillis;

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
@Internal
public class CircuitBreakerImpl implements CircuitBreaker {

  private static final ScheduledExecutorService SCHEDULER = new ScheduledThreadPoolExecutor(
    Runtime.getRuntime().availableProcessors(),
    r -> {
      Thread t = new Thread(r, "zeta-cb");
      t.setDaemon(true);
      return t;
    }
  );

  private final ZetaProperties.CircuitBreaker config;
  private final int bucketSize;
  private final LongAdder[] successBuckets;
  private final LongAdder[] failBuckets;
  private volatile int currentIndex;
  private final AtomicBoolean open = new AtomicBoolean(false);

  private static final class OpenTimestampField extends CbPadding.OpenTimestampRef {}

  private final OpenTimestampField openTimestampField = new OpenTimestampField();
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
  public CircuitBreakerImpl(ZetaProperties.CircuitBreaker config) {
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
  @Override
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
      now - openTimestampField.value > config.getSingleTestIntervalMs() &&
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
  @Override
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
  @Override
  public void onFailure() {
    if (config.isEnabled()) {
      failBuckets[currentIndex].increment();
      evaluateThreshold();
    }
  }

  /** Whether the breaker is currently open. */
  @Override
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
    long totalSuccess = 0;
    long totalFail = 0;

    for (LongAdder a : successBuckets) {
      totalSuccess += a.sum();
    }
    for (LongAdder a : failBuckets) {
      totalFail += a.sum();
    }

    if (totalFail > 0 && totalSuccess + totalFail >= config.getRequestVolumeThreshold()) {
      double rate = (double) totalFail / (totalSuccess + totalFail);

      if (rate > config.getFailThreshold() && open.compareAndSet(false, true)) {
        openTimestampField.value = currentTimeMillis();

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

/** Cache-line padding namespace — adapted from Caffeine. */
final class CbPadding {

  private CbPadding() {}

  @SuppressWarnings("all")
  abstract static class PadOpenTimestamp {

    byte p000, p001, p002, p003, p004, p005, p006, p007;
    byte p008, p009, p010, p011, p012, p013, p014, p015;
    byte p016, p017, p018, p019, p020, p021, p022, p023;
    byte p024, p025, p026, p027, p028, p029, p030, p031;
    byte p032, p033, p034, p035, p036, p037, p038, p039;
    byte p040, p041, p042, p043, p044, p045, p046, p047;
    byte p048, p049, p050, p051, p052, p053, p054, p055;
    byte p056, p057, p058, p059, p060, p061, p062, p063;
    byte p064, p065, p066, p067, p068, p069, p070, p071;
    byte p072, p073, p074, p075, p076, p077, p078, p079;
    byte p080, p081, p082, p083, p084, p085, p086, p087;
    byte p088, p089, p090, p091, p092, p093, p094, p095;
    byte p096, p097, p098, p099, p100, p101, p102, p103;
    byte p104, p105, p106, p107, p108, p109, p110, p111;
    byte p112, p113, p114, p115, p116, p117, p118, p119;
  }

  abstract static class OpenTimestampRef extends PadOpenTimestamp {

    /** Circuit-breaker open timestamp — isolated on its own cache line. */
    volatile long value;
  }
}
