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
package io.github.hyshmily.zeta.worker.detection.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the state-machine transition log throttle: the aggregate
 * INFO summary must fire at most once per window, carry every recorded
 * transition exactly once, and stay consistent under concurrent report
 * consumers.
 */
class TransitionLogThrottleTest {

  @Test
  void record_firstCallSummarizesImmediately_thenThrottlesWithinWindow() {
    TransitionLogThrottle throttle = new TransitionLogThrottle();
    // First record: the -WINDOW initialization makes the first transition
    // always emit a summary (the ReportConsumer first-log-always convention).
    assertThat(throttle.record(1_000)).isEqualTo(1);
    assertThat(throttle.record(1_001)).isEqualTo(-1);
    assertThat(throttle.record(10_999)).isEqualTo(-1);
  }

  @Test
  void record_windowElapsed_returnsAccumulatedCountAndResets() {
    TransitionLogThrottle throttle = new TransitionLogThrottle();
    assertThat(throttle.record(0)).isEqualTo(1);

    assertThat(throttle.record(1)).isEqualTo(-1);
    assertThat(throttle.record(2)).isEqualTo(-1);
    assertThat(throttle.record(3)).isEqualTo(-1);

    // Exactly at the window boundary the summary fires with the count of
    // every transition since the previous summary.
    assertThat(throttle.record(TransitionLogThrottle.WINDOW_MS)).isEqualTo(4);

    // And the counter restarts from zero for the next window.
    assertThat(throttle.record(TransitionLogThrottle.WINDOW_MS + 1)).isEqualTo(-1);
    assertThat(throttle.record(2 * TransitionLogThrottle.WINDOW_MS)).isEqualTo(2);
  }

  @Test
  void record_isConcurrencySafe_noLostCountsAcrossSummaries() throws Exception {
    TransitionLogThrottle throttle = new TransitionLogThrottle();
    int threads = 4;
    int recordsPerThread = 1_000;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(threads);
      AtomicLong accounted = new AtomicLong();

      for (int t = 0; t < threads; t++) {
        pool.submit(() -> {
          try {
            start.await();
            for (int i = 0; i < recordsPerThread; i++) {
              long due = throttle.record(500); // fixed "now" inside the first window
              if (due > 0) {
                accounted.addAndGet(due);
              }
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            done.countDown();
          }
        });
      }
      start.countDown();
      assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();

      // The first record's summary (1) may have been consumed by whichever
      // thread recorded first; drain the remainder with a summary due now.
      // The drain call is itself a record — the throttle's invariant is that
      // the sum of all returned counts equals the total number of records.
      long remaining = throttle.record(500 + TransitionLogThrottle.WINDOW_MS);
      accounted.addAndGet(remaining);

      assertThat(accounted.get()).isEqualTo((long) threads * recordsPerThread + 1);
    } finally {
      pool.shutdownNow();
    }
  }
}
