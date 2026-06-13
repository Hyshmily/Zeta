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
import java.util.concurrent.atomic.AtomicReference;
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

  /**
   * Verifies that a basic load operation returns the value supplied by the loader.
   */
  @Test
  void load_shouldReturnValue() {
    Optional<String> result = singleFlight.load("key", () -> "value");
    assertThat(result).contains("value");
  }

  /**
   * Verifies that concurrent requests for the same key are deduplicated so the loader executes only once (or fewer
   * times than the request count).
   */
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

  /**
   * Verifies that a supplier exception results in an empty Optional.
   */
  @Test
  void load_shouldReturnEmptyOnSupplierException() {
    Optional<String> result = singleFlight.load("key", () -> { throw new RuntimeException("fail"); });
    assertThat(result).isEmpty();
  }

  /**
   * Verifies that a supplier returning null results in an empty Optional.
   */
  @Test
  void load_shouldReturnEmptyOnNullSupplier() {
    Optional<String> result = singleFlight.load("key", () -> null);
    assertThat(result).isEmpty();
  }

  /**
   * Verifies that the estimated inflight request size is never negative.
   */
  @Test
  void estimatedInflightSize_shouldBeNonNegative() {
    assertThat(singleFlight.estimatedInflightSize()).isNotNegative();
    singleFlight.load("k1", () -> "v1");
    assertThat(singleFlight.estimatedInflightSize()).isGreaterThanOrEqualTo(0);
  }

  // ── New tests ──

  /**
   * Verifies that two loads for different keys execute their suppliers independently (no deduplication across keys).
   */
  @Test
  void load_withDifferentKeys_shouldNotDeduplicate() throws InterruptedException {
    AtomicInteger counter = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(2);

    executor.execute(() -> {
      singleFlight.load("key1", () -> {
        counter.incrementAndGet();
        return "v1";
      });
      latch.countDown();
    });

    executor.execute(() -> {
      singleFlight.load("key2", () -> {
        counter.incrementAndGet();
        return "v2";
      });
      latch.countDown();
    });

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(counter.get()).isEqualTo(2);
  }

  /**
   * Verifies that after the dedup TTL expires, a subsequent load for the same key executes the supplier again.
   */
  @Test
  void load_withTTLExpiry_shouldAllowNewLoadAfterExpiry() throws InterruptedException {
    SingleFlight shortTtl = new SingleFlight(1000, 1, 5, executor);
    AtomicInteger counter = new AtomicInteger(0);

    shortTtl.load("key", () -> {
      counter.incrementAndGet();
      return "first";
    });
    assertThat(counter.get()).isEqualTo(1);

    Thread.sleep(1100);

    shortTtl.load("key", () -> {
      counter.incrementAndGet();
      return "second";
    });
    assertThat(counter.get()).isEqualTo(2);
  }

  /**
   * Verifies that when {@code maxSize} is small, the inflight cache does not grow unboundedly under concurrent
   * loads for different keys.
   */
  @Test
  void load_withMaxInflight_shouldLimit() throws InterruptedException {
    SingleFlight limited = new SingleFlight(1, 10, 5, executor);
    AtomicInteger callCounter = new AtomicInteger(0);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(5);

    for (int i = 0; i < 5; i++) {
      executor.execute(() -> {
        try {
          startLatch.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        limited.load("k" + callCounter.incrementAndGet(), () -> {
          try {
            Thread.sleep(300);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          return "v";
        });
        doneLatch.countDown();
      });
    }

    startLatch.countDown();
    assertThat(doneLatch.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(limited.estimatedInflightSize()).isLessThanOrEqualTo(2L);
  }

  /**
   * Verifies that concurrent loads for different keys each return the correct value to their respective caller.
   */
  @Test
  void load_withInterleavedKeys_callerGetsResult() throws InterruptedException {
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(2);
    AtomicReference<String> result1 = new AtomicReference<>();
    AtomicReference<String> result2 = new AtomicReference<>();

    executor.execute(() -> {
      try {
        startLatch.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      result1.set(singleFlight.load("keyA", () -> "valueA").orElse(null));
      doneLatch.countDown();
    });

    executor.execute(() -> {
      try {
        startLatch.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      result2.set(singleFlight.load("keyB", () -> "valueB").orElse(null));
      doneLatch.countDown();
    });

    startLatch.countDown();
    assertThat(doneLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(result1.get()).isEqualTo("valueA");
    assertThat(result2.get()).isEqualTo("valueB");
  }

  /**
   * Verifies that after all in-flight loads complete (via exception which triggers invalidation), the estimated inflight
   * size drops to zero.
   */
  @Test
  void estimatedInflightSize_shouldReturnZeroAfterAllComplete() throws InterruptedException {
    // Load with exception — the catch block invalidates the entry synchronously
    singleFlight.load("to-evict", () -> { throw new RuntimeException("fail"); });
    assertThat(singleFlight.estimatedInflightSize()).isZero();
  }
}
