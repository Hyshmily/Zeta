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

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.hotkey.cache.SingleFlight;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SingleFlight}, verifying request deduplication, error handling, and inflight tracking.
 */
class SingleFlightTest {

  private SingleFlight singleFlight;
  private Executor executor;

  @BeforeEach
  void setUp() {
    executor = Executors.newCachedThreadPool();
    singleFlight = new SingleFlight(1000, 10, 5, executor);
  }

  @Test
  void load_shouldReturnValue() {
    Optional<String> result = singleFlight.load("key", () -> "value");
    assertThat(result).contains("value");
  }

  @Test
  void load_shouldDeduplicateConcurrentRequests() throws InterruptedException {
    AtomicInteger counter = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(5);

    for (int i = 0; i < 5; i++) {
      executor.execute(() -> {
        singleFlight.load("key", () -> {
          counter.incrementAndGet();
          try {
            Thread.sleep(100);
          } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
          }
          return "deduped";
        });
        latch.countDown();
      });
    }

    assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(counter.get()).isLessThan(5);
  }

  @Test
  void load_shouldReturnEmptyOnSupplierException() {
    Optional<String> result = singleFlight.load("key", () -> { throw new RuntimeException("fail"); });
    assertThat(result).isEmpty();
  }

  @Test
  void load_shouldReturnEmptyOnNullSupplier() {
    Optional<String> result = singleFlight.load("key", () -> null);
    assertThat(result).isEmpty();
  }

  @Test
  void estimatedInflightSize_shouldBeNonNegative() {
    assertThat(singleFlight.estimatedInflightSize()).isNotNegative();
    singleFlight.load("k1", () -> "v1");
    assertThat(singleFlight.estimatedInflightSize()).isGreaterThanOrEqualTo(0);
  }
}
