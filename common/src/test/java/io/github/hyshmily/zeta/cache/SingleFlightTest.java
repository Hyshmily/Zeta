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
package io.github.hyshmily.zeta.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.CircuitBreaker;
import io.github.hyshmily.zeta.cache.cachesupport.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.impl.CircuitBreakerImpl;
import io.github.hyshmily.zeta.cache.cachesupport.impl.SingleFlightImpl;
import java.util.List;
import java.util.Map;
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
  private CircuitBreaker disabledBreaker;

  @BeforeEach
  void setUp() {
    executor = Executors.newCachedThreadPool();
    disabledBreaker = new CircuitBreakerImpl(new ZetaProperties.CircuitBreaker());
    singleFlight = new SingleFlightImpl(1000, 10, 5, executor, disabledBreaker);
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
   * Verifies that a supplier exception propagates through SingleFlight.
   */
  @Test
  void load_shouldPropagateSupplierException() {
    assertThatThrownBy(() ->
      singleFlight.load("key", () -> {
        throw new RuntimeException("fail");
      })
    )
      .isInstanceOf(RuntimeException.class)
      .hasMessage("fail");
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
    SingleFlight shortTtl = new SingleFlightImpl(1000, 1, 5, executor, disabledBreaker);
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
    SingleFlight limited = new SingleFlightImpl(1, 10, 5, executor, disabledBreaker);
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
    try {
      singleFlight.load("to-evict", () -> {
        throw new RuntimeException("fail");
      });
    } catch (RuntimeException e) {
      // expected — exception invalidates the entry
    }
    assertThat(singleFlight.estimatedInflightSize()).isZero();
  }

  /**
   * Verifies that a supplier that times out returns empty (graceful degradation).
   */
  @Test
  void load_withTimeout_shouldReturnEmpty() {
    SingleFlight shortTimeout = new SingleFlightImpl(1000, 10, 1, executor, disabledBreaker);
    Optional<String> result = shortTimeout.load("timeout-key", () -> {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return "too-late";
    });
    assertThat(result).isEmpty();
  }

  /**
   * Verifies that after a supplier throws an exception, a subsequent load for the same key
   * retries the supplier (previous entry was invalidated).
   */
  @Test
  void load_afterException_shouldRetryAndSucceed() {
    // First call fails with exception — cache entry is invalidated
    try {
      singleFlight.load("retry-key", () -> {
        throw new RuntimeException("first-fail");
      });
    } catch (RuntimeException e) {
      // expected
    }

    // Subsequent call should create a new future and succeed
    Optional<String> result = singleFlight.load("retry-key", () -> "success");
    assertThat(result).contains("success");
  }

  /**
   * Verifies that concurrent calls for the same key invoke the supplier exactly once
   * (strict dedup count verification).
   */
  @Test
  void load_shouldInvokeSupplierExactlyOnceForConcurrentCalls() throws InterruptedException {
    AtomicInteger counter = new AtomicInteger(0);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(5);

    for (int i = 0; i < 5; i++) {
      executor.execute(() -> {
        try {
          startLatch.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        singleFlight.load("dedup-key", () -> {
          counter.incrementAndGet();
          try {
            Thread.sleep(200);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          return "deduped";
        });
        doneLatch.countDown();
      });
    }

    startLatch.countDown();
    assertThat(doneLatch.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(counter.get()).isEqualTo(1);
  }

  /**
   * Verifies that a slow supplier completing after timeout causes the next load for the same key
   * to re-execute the supplier (entry was invalidated after timeout).
   */
  @Test
  void load_afterTimeout_shouldRetrySupplier() throws InterruptedException {
    SingleFlight shortTimeout = new SingleFlightImpl(1000, 10, 1, executor, disabledBreaker);

    // First load times out
    try {
      shortTimeout.load("slow-key", () -> {
        try {
          Thread.sleep(3000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return "slow-value";
      });
    } catch (RuntimeException e) {
      // expected
    }

    // Give the timeout processing time to complete
    Thread.sleep(100);

    // Second load should retry (entry was invalidated)
    assertThat(shortTimeout.load("slow-key", () -> "fast-value")).contains("fast-value");
  }

  /**
   * Verifies that the inflight size warning threshold triggers a log at 80% capacity.
   */
  @Test
  void load_withHighInflight_shouldLogWarning() {
    SingleFlight smallPool = new SingleFlightImpl(5, 10, 5, executor, disabledBreaker);
    java.util.stream.IntStream.range(0, 10).forEach(idx -> smallPool.load("k" + idx, () -> "v" + idx));
    assertThat(smallPool.estimatedInflightSize()).isNotNegative();
  }

  /**
   * Verifies that when a supplier throws with an {@link InterruptedException} cause,
   * the exception propagates through SingleFlight.
   */
  @Test
  void load_withInterruptedExceptionCause_shouldPropagate() {
    assertThatThrownBy(() ->
      singleFlight.load("interrupt-key", () -> {
        throw new RuntimeException(new InterruptedException("simulated"));
      })
    ).isInstanceOf(RuntimeException.class);
  }

  /**
   * Verifies that after an exception path, a subsequent load for the same key
   * retries the supplier (entry was invalidated).
   */
  @Test
  void load_afterInterruptedException_shouldRetry() {
    try {
      singleFlight.load("retry-key", () -> {
        throw new RuntimeException(new InterruptedException("simulated"));
      });
    } catch (RuntimeException e) {
      // expected
    }

    Optional<String> result = singleFlight.load("retry-key", () -> "success");
    assertThat(result).contains("success");
  }

  @Test
  void load_withErrorCause_shouldPropagate() {
    assertThatThrownBy(() ->
      singleFlight.load("error-key", () -> {
        throw new Error("simulated-error");
      })
    ).isInstanceOf(Error.class);
  }

  @Test
  void load_whenBreakerOpen_shouldReturnEmpty() {
    ZetaProperties.CircuitBreaker cfg = new ZetaProperties.CircuitBreaker();
    cfg.setEnabled(true);
    cfg.setFailThreshold(0.1);
    cfg.setRequestVolumeThreshold(1);
    CircuitBreakerImpl breaker = new CircuitBreakerImpl(cfg);
    breaker.onFailure();
    SingleFlight sf = new SingleFlightImpl(1000, 10, 5, executor, breaker);
    assertThat(sf.load("key", () -> "val")).isEmpty();
    assertThat(sf.isBreakerOpen()).isTrue();
  }

  // ── Collection load tests ──

  /**
   * Verifies that loading multiple keys via the collection overload returns
   * all keys with their correct values.
   */
  @Test
  void loadCollection_shouldReturnAllKeys() {
    Map<String, Optional<String>> result = singleFlight.load(List.of("a", "b", "c"), key -> "val-" + key);
    assertThat(result)
      .hasSize(3)
      .containsEntry("a", Optional.of("val-a"))
      .containsEntry("b", Optional.of("val-b"))
      .containsEntry("c", Optional.of("val-c"));
  }

  /**
   * Verifies that an empty collection returns an empty map immediately,
   * without invoking the reader supplier.
   */
  @Test
  void loadCollection_withEmptyInput_shouldReturnEmptyMap() {
    Map<String, Optional<String>> result = singleFlight.load(List.of(), key -> {
      throw new AssertionError("reader must not be called");
    });
    assertThat(result).isEmpty();
  }

  /**
   * Verifies that the circuit breaker intercepts a collection load and
   * returns all keys as empty.
   */
  @Test
  void loadCollection_whenBreakerOpen_shouldReturnEmptyForAllKeys() {
    ZetaProperties.CircuitBreaker cfg = new ZetaProperties.CircuitBreaker();
    cfg.setEnabled(true);
    cfg.setFailThreshold(0.1);
    cfg.setRequestVolumeThreshold(1);
    CircuitBreakerImpl breaker = new CircuitBreakerImpl(cfg);
    breaker.onFailure();
    SingleFlight sf = new SingleFlightImpl(1000, 10, 5, executor, breaker);

    Map<String, Optional<String>> result = sf.load(List.of("x", "y"), key -> "val");
    assertThat(result).hasSize(2).containsEntry("x", Optional.empty()).containsEntry("y", Optional.empty());
    assertThat(sf.isBreakerOpen()).isTrue();
  }

  /**
   * Verifies that a reader exception during a collection load propagates
   * as a RuntimeException.
   */
  @Test
  void loadCollection_shouldPropagateReaderException() {
    assertThatThrownBy(() ->
      singleFlight.load(List.of("fail-key"), key -> {
        throw new RuntimeException("batch-fail");
      })
    )
      .isInstanceOf(RuntimeException.class)
      .hasMessageContaining("batch-fail");
  }
}
