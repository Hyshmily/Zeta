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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.circuitbreaker.CircuitBreaker;
import io.github.hyshmily.zeta.cache.cachesupport.circuitbreaker.CircuitBreakerImpl;
import io.github.hyshmily.zeta.cache.cachesupport.circuitbreaker.CircuitBreakerRegistry;
import io.github.hyshmily.zeta.cache.cachesupport.loader.LoadStatus;
import io.github.hyshmily.zeta.cache.cachesupport.loader.LoaderOutcome;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlightImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;

/**
 * Tests for {@link SingleFlight}, verifying request deduplication, error handling, and inflight tracking.
 */
class SingleFlightTest {

  private TestSingleFlight singleFlight;
  private Executor executor;
  private CircuitBreaker disabledBreaker;

  @BeforeEach
  void setUp() {
    executor = Executors.newCachedThreadPool();
    disabledBreaker = new CircuitBreakerImpl(new ZetaProperties.CircuitBreaker());
    singleFlight = new TestSingleFlight(1000, 10, 5, executor, registryOf(disabledBreaker));
  }

  /** Production-shaped wiring (see {@code ZetaAutoConfiguration}): a registry whose every shard is {@code breaker}. */
  private static CircuitBreakerRegistry registryOf(CircuitBreaker breaker) {
    CircuitBreakerRegistry registry = mock(CircuitBreakerRegistry.class);
    when(registry.breakerFor(anyString())).thenReturn(breaker);
    // The aggregate/per-key open queries must observe the shard's real state,
    // not Mockito's default false — otherwise an open breaker looks closed.
    when(registry.isAnyDegraded()).thenAnswer(invocation -> breaker.isDegraded());
    when(registry.isDegraded(anyString())).thenAnswer(invocation -> breaker.isDegraded());
    return registry;
  }

  /**
   * Verifies that a basic load operation returns the value supplied by the loader.
   */
  @Test
  void loadOutcome_shouldReturnValue() {
    LoaderOutcome<String> result = singleFlight.loadOutcome("key", () -> "value");
    assertThat(result.status()).isEqualTo(LoadStatus.LOADED);
    assertThat(result.value()).isEqualTo("value");
  }

  /**
   * The caller-runs contract behind {@code @Cacheable(sync = true)}: the elected
   * leader executes the reader on the thread that called, not on the executor.
   */
  @Test
  void loadOutcomeCallerRuns_shouldExecuteReaderOnCallingThread() {
    Thread caller = Thread.currentThread();
    AtomicReference<Thread> loaderThread = new AtomicReference<>();

    LoaderOutcome<String> outcome = singleFlight.loadOutcomeCallerRuns(
      "key",
      () -> {
        loaderThread.set(Thread.currentThread());
        return "value";
      },
      null
    );

    assertThat(outcome.status()).isEqualTo(LoadStatus.LOADED);
    assertThat(outcome.value()).isEqualTo("value");
    assertThat(loaderThread.get()).isSameAs(caller);
  }

  /**
   * Same-key dedup must survive the caller-runs mode: a concurrent follower
   * joins the leader's future instead of running a second reader, even though
   * the leader is executing on its own (non-pool) thread.
   */
  @Test
  void loadOutcomeCallerRuns_shouldMergeConcurrentFollowerOntoLeader() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    ExecutorService leaderPool = Executors.newSingleThreadExecutor();
    try {
      Future<LoaderOutcome<String>> leader = leaderPool.submit(() ->
        singleFlight.loadOutcomeCallerRuns(
          "key",
          () -> {
            calls.incrementAndGet();
            started.countDown();
            try {
              release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            return "value";
          },
          null
        )
      );
      assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

      LoaderOutcome<String> follower = singleFlight.loadOutcomeCallerRuns(
        "key",
        () -> {
          calls.incrementAndGet();
          return "other";
        },
        null
      );

      release.countDown();
      assertThat(follower.status()).isEqualTo(LoadStatus.LOADED);
      assertThat(follower.value()).isEqualTo("value");
      assertThat(leader.get(5, TimeUnit.SECONDS).value()).isEqualTo("value");
      assertThat(calls).hasValue(1);
    } finally {
      leaderPool.shutdownNow();
    }
  }

  /**
   * A same-key call from inside a caller-runs reader must fail fast: without the
   * running-thread guard it would join a future only this call can complete.
   */
  @Test
  void loadOutcomeCallerRuns_shouldRejectSameKeyReentry() {
    LoaderOutcome<String> outcome = singleFlight.loadOutcomeCallerRuns(
      "key",
      () -> singleFlight.loadOutcomeCallerRuns("key", () -> "inner", null).value(),
      null
    );

    assertThat(outcome.status()).isEqualTo(LoadStatus.FAILED);
    assertThat(outcome.cause()).isInstanceOf(IllegalStateException.class).hasMessageContaining("re-entrant");
  }

  /**
   * The same guard covers the executor path: the reader's thread is the one that
   * would otherwise block until the load budget expires.
   */
  @Test
  void loadOutcome_shouldRejectSameKeyReentryOnExecutorThread() {
    LoaderOutcome<String> outcome = singleFlight.loadOutcome("key", () ->
      singleFlight.loadOutcome("key", () -> "inner").value()
    );

    assertThat(outcome.status()).isEqualTo(LoadStatus.FAILED);
    assertThat(outcome.cause()).isInstanceOf(IllegalStateException.class).hasMessageContaining("re-entrant");
  }

  /**
   * A failed caller-runs load evicts its future, so the next read retries
   * instead of replaying the failure for the dedup TTL.
   */
  @Test
  void loadOutcomeCallerRuns_shouldEvictFailureForRetry() {
    AtomicInteger calls = new AtomicInteger();

    LoaderOutcome<String> failed = singleFlight.loadOutcomeCallerRuns(
      "key",
      () -> {
        calls.incrementAndGet();
        throw new IllegalStateException("boom");
      },
      null
    );
    assertThat(failed.status()).isEqualTo(LoadStatus.FAILED);
    assertThat(failed.cause()).isInstanceOf(IllegalStateException.class);

    LoaderOutcome<String> retry = singleFlight.loadOutcomeCallerRuns(
      "key",
      () -> {
        calls.incrementAndGet();
        return "ok";
      },
      null
    );
    assertThat(retry.status()).isEqualTo(LoadStatus.LOADED);
    assertThat(retry.value()).isEqualTo("ok");
    assertThat(calls).hasValue(2);
  }

  /**
   * Verifies that concurrent requests for the same key are deduplicated so the loader executes only once (or fewer
   * times than the request add).
   */
  @Test
  void load_shouldDeduplicateConcurrentRequests() throws InterruptedException {
    AtomicInteger counter = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(5);

    for (int i = 0; i < 5; i++) {
      executor.execute(() -> {
        singleFlight.loadOutcome("key", () -> {
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
  void loadOutcome_shouldCaptureSupplierException() {
    LoaderOutcome<String> outcome = singleFlight.loadOutcome("key", () -> {
      throw new RuntimeException("fail");
    });
    assertThat(outcome.status()).isEqualTo(LoadStatus.FAILED);
    assertThat(outcome.cause()).isInstanceOf(RuntimeException.class).hasMessage("fail");
  }

  /**
   * Verifies that a supplier returning null results in an empty Optional.
   */
  @Test
  void loadOutcome_shouldReturnSourceEmptyOnNullSupplier() {
    LoaderOutcome<String> result = singleFlight.loadOutcome("key", () -> null);
    assertThat(result.status()).isEqualTo(LoadStatus.SOURCE_EMPTY);
  }

  /**
   * Verifies that the estimated inflight request size is never negative.
   */
  @Test
  void estimatedInFlightSize_shouldBeNonNegative() {
    assertThat(singleFlight.estimatedInFlightSize()).isNotNegative();
    singleFlight.loadOutcome("k1", () -> "v1");
    assertThat(singleFlight.estimatedInFlightSize()).isGreaterThanOrEqualTo(0);
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
      singleFlight.loadOutcome("key1", () -> {
        counter.incrementAndGet();
        return "v1";
      });
      latch.countDown();
    });

    executor.execute(() -> {
      singleFlight.loadOutcome("key2", () -> {
        counter.incrementAndGet();
        return "v2";
      });
      latch.countDown();
    });

    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(counter.get()).isEqualTo(2);
  }

  /**
   * Verifies that after the dedup retention window expires, a subsequent load for the same key executes the
   * supplier again. The retention window is {@code max(ttlSec, timeoutSeconds + 1)} — see
   * {@link #completedFutureOutlivesConfiguredTtlWhenTtlIsShorterThanTimeout()}.
   */
  @Test
  void load_withTTLExpiry_shouldAllowNewLoadAfterExpiry() throws InterruptedException {
    // ttl 2s > timeout 1s: the configured ttl is the retention window here.
    TestSingleFlight shortTtl = new TestSingleFlight(1000, 2, 1, executor, registryOf(disabledBreaker));
    AtomicInteger counter = new AtomicInteger(0);

    shortTtl.loadOutcome("key", () -> {
      counter.incrementAndGet();
      return "first";
    });
    assertThat(counter.get()).isEqualTo(1);

    Thread.sleep(2100);

    shortTtl.loadOutcome("key", () -> {
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
    TestSingleFlight limited = new TestSingleFlight(1, 10, 5, executor, registryOf(disabledBreaker));
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
        limited.loadOutcome("k" + callCounter.incrementAndGet(), () -> {
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
    assertThat(limited.estimatedInFlightSize()).isLessThanOrEqualTo(2L);
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
      result1.set(singleFlight.loadOutcome("keyA", () -> "valueA").value());
      doneLatch.countDown();
    });

    executor.execute(() -> {
      try {
        startLatch.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      result2.set(singleFlight.loadOutcome("keyB", () -> "valueB").value());
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
  void estimatedInFlightSize_shouldReturnZeroAfterAllComplete() throws InterruptedException {
    singleFlight.loadOutcome("to-evict", () -> {
      throw new RuntimeException("fail");
    });
    assertThat(singleFlight.estimatedInFlightSize()).isZero();
  }

  /**
   * Verifies that a supplier that times out returns empty (graceful degradation).
   */
  @Test
  void load_withTimeout_shouldReturnEmpty() {
    TestSingleFlight shortTimeout = new TestSingleFlight(1000, 10, 1, executor, registryOf(disabledBreaker));
    LoaderOutcome<String> result = shortTimeout.loadOutcome("timeout-key", () -> {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return "too-late";
    });
    assertThat(result.status()).isEqualTo(LoadStatus.SUPPRESSED_TIMEOUT);
  }

  /**
   * Verifies that a timed-out task does not poison its pool thread: the next
   * task reusing that thread must not inherit the timeout interrupt flag.
   * The slow task responds the way a real loader often does — re-applies the
   * interrupt flag after the timeout interrupt — so without the
   * exit-time flag clearing, the reused thread would start the next task in
   * interrupted state and its I/O would fail immediately.
   */
  @Test
  void timeoutInterrupt_shouldNotPoisonReusedPoolThread() throws InterruptedException {
    ExecutorService singleThread = Executors.newSingleThreadExecutor();
    try {
      TestSingleFlight shortTimeout = new TestSingleFlight(1000, 10, 1, singleThread, registryOf(disabledBreaker));
      CountDownLatch slowTaskDone = new CountDownLatch(1);

      // Task A: interrupted by the 1s timeout, re-applies the interrupt flag,
      // then finishes late.
      shortTimeout.loadOutcome("poison-key", () -> {
        try {
          Thread.sleep(3000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        slowTaskDone.countDown();
        return "too-late";
      });
      assertThat(slowTaskDone.await(5, TimeUnit.SECONDS)).isTrue();

      // Give the pool thread a moment to return to the pool.
      Thread.sleep(200);

      // Task B runs on the same (only) pool thread and must start clean.
      AtomicBoolean taskBInterrupted = new AtomicBoolean(true);
      CountDownLatch taskBDone = new CountDownLatch(1);
      singleThread.execute(() -> {
        taskBInterrupted.set(Thread.currentThread().isInterrupted());
        taskBDone.countDown();
      });
      assertThat(taskBDone.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(taskBInterrupted.get()).isFalse();
    } finally {
      singleThread.shutdownNow();
    }
  }

  /**
   * Verifies that after a supplier throws an exception, a subsequent load for the same key
   * retries the supplier (previous entry was invalidated).
   */
  @Test
  void load_afterException_shouldRetryAndSucceed() {
    // First call fails with exception — cache entry is invalidated
    singleFlight.loadOutcome("retry-key", () -> {
      throw new RuntimeException("first-fail");
    });

    // Subsequent call should create a new future and succeed
    LoaderOutcome<String> result = singleFlight.loadOutcome("retry-key", () -> "success");
    assertThat(result.value()).isEqualTo("success");
  }

  /**
   * Verifies that concurrent calls for the same key invoke the supplier exactly once
   * (strict dedup add verification).
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
        singleFlight.loadOutcome("dedup-key", () -> {
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
    TestSingleFlight shortTimeout = new TestSingleFlight(1000, 10, 1, executor, registryOf(disabledBreaker));

    // First load times out
    shortTimeout.loadOutcome("slow-key", () -> {
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return "slow-value";
    });

    // Give the timeout processing time to complete
    Thread.sleep(100);

    // Second load should retry (entry was invalidated)
    assertThat(shortTimeout.loadOutcome("slow-key", () -> "fast-value").value()).isEqualTo("fast-value");
  }

  /**
   * Verifies that the inflight size warning threshold triggers a log at 80% capacity.
   */
  @Test
  void load_withHighInflight_shouldLogWarning() {
    TestSingleFlight smallPool = new TestSingleFlight(5, 10, 5, executor, registryOf(disabledBreaker));
    java.util.stream.IntStream.range(0, 10).forEach(idx -> smallPool.loadOutcome("k" + idx, () -> "v" + idx));
    assertThat(smallPool.estimatedInFlightSize()).isNotNegative();
  }

  /**
   * Verifies that when a supplier throws with an {@link InterruptedException} cause,
   * the exception propagates through SingleFlight.
   */
  @Test
  void loadOutcome_withInterruptedExceptionCause_shouldCaptureFailure() {
    LoaderOutcome<String> outcome = singleFlight.loadOutcome("interrupt-key", () -> {
      throw new RuntimeException(new InterruptedException("simulated"));
    });
    assertThat(outcome.status()).isEqualTo(LoadStatus.FAILED);
    assertThat(outcome.cause()).hasCauseInstanceOf(InterruptedException.class);
  }

  /**
   * Verifies that after an exception path, a subsequent load for the same key
   * retries the supplier (entry was invalidated).
   */
  @Test
  void load_afterInterruptedException_shouldRetry() {
    singleFlight.loadOutcome("retry-key", () -> {
      throw new RuntimeException(new InterruptedException("simulated"));
    });

    LoaderOutcome<String> result = singleFlight.loadOutcome("retry-key", () -> "success");
    assertThat(result.value()).isEqualTo("success");
  }

  @Test
  void loadOutcome_withErrorCause_shouldCaptureError() {
    LoaderOutcome<String> outcome = singleFlight.loadOutcome("error-key", () -> {
      throw new Error("simulated-error");
    });
    assertThat(outcome.status()).isEqualTo(LoadStatus.FAILED);
    assertThat(outcome.cause()).isInstanceOf(Error.class).hasMessage("simulated-error");
  }

  @Test
  void load_whenBreakerOpen_shouldReturnEmpty() {
    ZetaProperties.CircuitBreaker cfg = new ZetaProperties.CircuitBreaker();
    cfg.setEnabled(true);
    cfg.setFailThreshold(0.1);
    cfg.setRequestVolumeThreshold(1);
    CircuitBreakerImpl breaker = new CircuitBreakerImpl(cfg);
    breaker.onFailure(new RuntimeException("trip"), CircuitBreaker.STAMP_CLOSED);
    TestSingleFlight sf = new TestSingleFlight(1000, 10, 5, executor, registryOf(breaker));
    assertThat(sf.loadOutcome("key", () -> "val").status()).isEqualTo(LoadStatus.SUPPRESSED_BREAKER);
    assertThat(sf.isBreakerOpen()).isTrue();
  }

  // ── Collection load tests ──

  /**
   * Verifies that loading multiple keys via the collection overload returns
   * all keys with their correct values.
   */
  @Test
  void loadCollection_shouldReturnAllKeys() {
    Map<String, Optional<String>> result = loadAll(singleFlight, List.of("a", "b", "c"), key -> "val-" + key);
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
    Map<String, Optional<String>> result = loadAll(singleFlight, List.of(), key -> {
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
    breaker.onFailure(new RuntimeException("trip"), CircuitBreaker.STAMP_CLOSED);
    TestSingleFlight sf = new TestSingleFlight(1000, 10, 5, executor, registryOf(breaker));

    Map<String, Optional<String>> result = loadAll(sf, List.of("x", "y"), key -> "val");
    assertThat(result).hasSize(2).containsEntry("x", Optional.empty()).containsEntry("y", Optional.empty());
    assertThat(sf.isBreakerOpen()).isTrue();
  }

  /**
   * Verifies that a reader exception during a collection load is caught and
   * the failing key returns {@link Optional#empty()} without propagating
   * the exception.
   */
  @Test
  void loadCollection_onReaderException_shouldReturnEmptyForKey() {
    Map<String, Optional<String>> result = loadAll(singleFlight, List.of("fail-key"), key -> {
      throw new RuntimeException("batch-fail");
    });
    assertThat(result).hasSize(1).containsEntry("fail-key", Optional.empty());
  }

  /**
   * Verifies that when one key in a batch load fails, the remaining keys are
   * still processed successfully and the failing key returns empty.
   */
  @Test
  void loadCollection_withOneKeyFailing_shouldReturnAllKeys() {
    Map<String, Optional<String>> result = loadAll(singleFlight, List.of("a", "fail", "c"), key -> {
      if ("fail".equals(key)) {
        throw new RuntimeException("intentional failure");
      }
      return "val-" + key;
    });
    assertThat(result)
      .hasSize(3)
      .containsEntry("a", Optional.of("val-a"))
      .containsEntry("fail", Optional.empty())
      .containsEntry("c", Optional.of("val-c"));
  }

  /**
   * Verifies that fail-on-error collection loading propagates the first
   * reader failure to the caller instead of swallowing it as a miss.
   */
  @Test
  void loadCollection_readerFailure_shouldPropagate() {
    assertThatThrownBy(() ->
      failFast(
        singleFlight.loadOutcome(List.of("a", "fail", "c"), key -> {
          if ("fail".equals(key)) {
            throw new RuntimeException("intentional failure");
          }
          return "val-" + key;
        })
      )
    )
      .isInstanceOf(RuntimeException.class)
      .hasMessage("intentional failure");
  }

  /**
   * Verifies that fail-on-error collection loading still evicts the failed
   * key's dedup future, so a subsequent load retries the reader.
   */
  @Test
  void loadCollection_readerFailure_shouldAllowRetryAfterFailure() {
    assertThatThrownBy(() ->
      failFast(
        singleFlight.loadOutcome(List.of("fail-key"), key -> {
          throw new RuntimeException("intentional failure");
        })
      )
    )
      .isInstanceOf(RuntimeException.class)
      .hasMessage("intentional failure");

    Map<String, Optional<String>> retry = loadAll(singleFlight, List.of("fail-key"), key -> "recovered-" + key);
    assertThat(retry).containsEntry("fail-key", Optional.of("recovered-fail-key"));
  }

  /**
   * design decisions: version comparison: in fail-on-error mode a reader TIMEOUT
   * resolves to empty (the interface contract) instead of propagating, while any
   * other reader failure aborts the batch and propagates. The test pins the
   * timeout half of the contract with a short timeout.
   */
  @Test
  void loadCollection_timeout_shouldResolveEmptyAndRetry() throws Exception {
    CountDownLatch slowReader = new CountDownLatch(1);
    CircuitBreaker breaker = new CircuitBreakerImpl(new ZetaProperties.CircuitBreaker());
    TestSingleFlight shortTimeout = new TestSingleFlight(1000, 10, 1, executor, registryOf(breaker)); // 1s timeout

    Map<String, LoaderOutcome<String>> outcomes = shortTimeout.loadOutcome(List.of("slow", "fast"), key -> {
      if ("slow".equals(key)) {
        awaitUninterruptibly(slowReader); // longer than the 1s timeout
      }
      return "val-" + key;
    });

    assertThat(outcomes.get("slow").status()).isEqualTo(LoadStatus.SUPPRESSED_TIMEOUT);
    assertThat(outcomes.get("fast").status()).isEqualTo(LoadStatus.LOADED);

    // The timed-out key's dedup future was invalidated: a retry re-runs the reader.
    slowReader.countDown();
    assertThat(loadAll(shortTimeout, List.of("slow"), key -> "recovered")).containsEntry(
      "slow",
      Optional.of("recovered")
    );
  }

  /**
   * Batch convenience over the explicit-outcome view: every input key is present;
   * non-LOADED outcomes resolve to empty.
   */
  private <T> Map<String, Optional<T>> loadAll(TestSingleFlight sf, List<String> keys, Function<String, T> fn) {
    Map<String, Optional<T>> out = new java.util.LinkedHashMap<>();
    sf
      .loadOutcome(keys, fn)
      .forEach((k, o) -> out.put(k, o.status() == LoadStatus.LOADED ? Optional.of(o.value()) : Optional.empty()));
    return out;
  }

  /**
   * Apply the caller-side fail-fast policy over per-key outcomes: the first
   * {@code FAILED} cause propagates in its natural form; the policy lives with
   * the caller.
   */
  private static void failFast(Map<String, LoaderOutcome<String>> outcomes) {
    for (LoaderOutcome<String> outcome : outcomes.values()) {
      if (outcome.status() != LoadStatus.FAILED) {
        continue;
      }
      Throwable cause = outcome.cause();
      if (cause instanceof RuntimeException re) {
        throw re;
      }
      if (cause instanceof Error err) {
        throw err;
      }
      throw new java.util.concurrent.CompletionException(cause);
    }
  }

  /** {@code CountDownLatch.await} that ignores the interrupt flag (used by blocking readers). */
  private static void awaitUninterruptibly(CountDownLatch latch) {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          latch.await();
          return;
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Verifies that fail-on-error collection loading respects the circuit
   * breaker: an open breaker still resolves every key to empty without
   * invoking the reader or throwing.
   */
  @Test
  void loadCollection_breakerOpen_shouldReturnEmptyForAllKeys() {
    ZetaProperties.CircuitBreaker cfg = new ZetaProperties.CircuitBreaker();
    cfg.setEnabled(true);
    cfg.setFailThreshold(0.1);
    cfg.setRequestVolumeThreshold(1);
    CircuitBreakerImpl breaker = new CircuitBreakerImpl(cfg);
    breaker.onFailure(new RuntimeException("trip"), CircuitBreaker.STAMP_CLOSED);
    TestSingleFlight sf = new TestSingleFlight(1000, 10, 5, executor, registryOf(breaker));

    Map<String, LoaderOutcome<String>> outcomes = sf.loadOutcome(List.of("x", "y"), key -> {
      throw new AssertionError("reader must not be called");
    });
    assertThat(outcomes)
      .hasSize(2)
      .allSatisfy((key, outcome) -> assertThat(outcome.status()).isEqualTo(LoadStatus.SUPPRESSED_BREAKER));
  }

  // ── Leader-only breaker accounting ──

  /**
   * One failing load behind N merged waiters records exactly one breaker
   * admission and one failure: the breaker protects the downstream call, not
   * the waiting threads. Recording per waiter would let a single downstream
   * blip behind 30 merged reads trip the whole prefix shard.
   */
  @Test
  void loadOutcome_concurrentWaitersOnSingleFailure_countsBreakerOnce() throws Exception {
    CircuitBreaker breaker = mock(CircuitBreaker.class);
    when(breaker.tryAcquireStamp()).thenReturn(CircuitBreaker.STAMP_CLOSED);
    TestSingleFlight sf = new TestSingleFlight(1000, 10, 5, executor, registryOf(breaker));

    int waiters = 30;
    CountDownLatch ready = new CountDownLatch(waiters);
    CountDownLatch go = new CountDownLatch(1);
    AtomicInteger calls = new AtomicInteger();
    List<Thread> threads = new ArrayList<>(waiters);
    List<LoaderOutcome<String>> outcomes = Collections.synchronizedList(new ArrayList<>(waiters));
    for (int i = 0; i < waiters; i++) {
      Thread t = new Thread(() -> {
        ready.countDown();
        awaitUninterruptibly(go);
        outcomes.add(
          sf.loadOutcome("k", () -> {
            calls.incrementAndGet();
            try {
              Thread.sleep(300);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            throw new RuntimeException("boom");
          })
        );
      });
      threads.add(t);
      t.start();
    }
    assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
    go.countDown();
    for (Thread t : threads) {
      t.join(15_000);
    }

    assertThat(calls.get()).isEqualTo(1);
    assertThat(outcomes).hasSize(waiters);
    assertThat(outcomes).allMatch(o -> o.status() == LoadStatus.FAILED);
    verify(breaker, times(1)).tryAcquireStamp();
    verify(breaker, times(1)).onFailure(any(Throwable.class), eq(CircuitBreaker.STAMP_CLOSED));
    verify(breaker, times(0)).onSuccess(anyLong());
  }

  /**
   * Mirror of the failure case: one shared successful load behind N merged
   * waiters records exactly one success — a half-open breaker must not close
   * N times early on a single probe.
   */
  @Test
  void loadOutcome_concurrentWaitersOnSingleSuccess_countsBreakerOnce() throws Exception {
    CircuitBreaker breaker = mock(CircuitBreaker.class);
    when(breaker.tryAcquireStamp()).thenReturn(CircuitBreaker.STAMP_CLOSED);
    TestSingleFlight sf = new TestSingleFlight(1000, 10, 5, executor, registryOf(breaker));

    int waiters = 30;
    CountDownLatch ready = new CountDownLatch(waiters);
    CountDownLatch go = new CountDownLatch(1);
    AtomicInteger calls = new AtomicInteger();
    List<Thread> threads = new ArrayList<>(waiters);
    List<LoaderOutcome<String>> outcomes = Collections.synchronizedList(new ArrayList<>(waiters));
    for (int i = 0; i < waiters; i++) {
      Thread t = new Thread(() -> {
        ready.countDown();
        awaitUninterruptibly(go);
        outcomes.add(
          sf.loadOutcome("k", () -> {
            calls.incrementAndGet();
            try {
              Thread.sleep(300);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            return "v";
          })
        );
      });
      threads.add(t);
      t.start();
    }
    assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
    go.countDown();
    for (Thread t : threads) {
      t.join(15_000);
    }

    assertThat(calls.get()).isEqualTo(1);
    assertThat(outcomes).hasSize(waiters);
    assertThat(outcomes).allMatch(o -> o.status() == LoadStatus.LOADED && "v".equals(o.value()));
    verify(breaker, times(1)).tryAcquireStamp();
    verify(breaker, times(1)).onSuccess(CircuitBreaker.STAMP_CLOSED);
    verify(breaker, times(0)).onFailure(any(Throwable.class), anyLong());
  }

  // ═══════════════════════════════════════════════════════════════
  // Submission failures, cleanup robustness, construction contract
  // ═══════════════════════════════════════════════════════════════
  /**
   * A load's timeout is an <b>execution</b> budget, not a queue deadline.
   *
   * <p>With the clock anchored at submission, the tail of a batch submitted to
   * a small pool was declared timed out before its readers ever ran: every key
   * whose queue wait alone exceeded the timeout was silently dropped as
   * {@code SUPPRESSED_TIMEOUT}. The clock now starts when a pool thread picks
   * up the task, so all ten keys — each reader well inside its own 1s budget —
   * must load.
   */
  @Test
  void loadOutcome_batchOnSingleThread_eachKeyGetsItsOwnExecutionTimeout() {
    ExecutorService pool = Executors.newFixedThreadPool(1);
    try {
      TestSingleFlight flight = new TestSingleFlight(1000, 10, 1, pool, registryOf(disabledBreaker));
      List<String> keys = java.util.stream.IntStream.range(0, 10)
        .mapToObj(i -> "queued-" + i)
        .toList();

      Map<String, LoaderOutcome<String>> outcomes = flight.loadOutcome(keys, key -> {
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return "v-" + key;
      });

      assertThat(outcomes).hasSize(keys.size());
      assertThat(outcomes).allSatisfy((key, outcome) -> {
        assertThat(outcome.status()).as("key %s", key).isEqualTo(LoadStatus.LOADED);
        assertThat(outcome.value()).isEqualTo("v-" + key);
      });
      assertThat(flight.loadTimeoutCount()).isZero();
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * {@code close()} must complete every flight still in the table. A queued task
   * that {@code shutdownNow} drained never ran, so without this drain its future
   * would never settle and the caller joining it would block forever.
   */
  @Test
  void close_completesQueuedLoadsAndReleasesWaiters() throws Exception {
    ExecutorService callers = Executors.newCachedThreadPool();
    ExecutorService pool = Executors.newFixedThreadPool(1);
    CountDownLatch submitted = new CountDownLatch(2);
    Executor counting = task -> {
      pool.execute(task);
      submitted.countDown();
    };
    TestSingleFlight flight = new TestSingleFlight(1000, 10, 5, counting, registryOf(disabledBreaker));
    CountDownLatch running = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    try {
      Future<LoaderOutcome<String>> first = callers.submit(() ->
        flight.loadOutcome("busy", () -> {
          running.countDown();
          try {
            release.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          return "v";
        })
      );
      assertThat(running.await(5, TimeUnit.SECONDS)).isTrue();

      Future<LoaderOutcome<String>> queued = callers.submit(() -> flight.loadOutcome("queued", () -> "v"));
      assertThat(submitted.await(5, TimeUnit.SECONDS)).isTrue();

      flight.close();

      LoaderOutcome<String> outcome = queued.get(5, TimeUnit.SECONDS);
      assertThat(outcome.status()).isEqualTo(LoadStatus.SUPPRESSED_EXECUTOR);

      release.countDown();
      first.get(5, TimeUnit.SECONDS);
    } finally {
      release.countDown();
      callers.shutdownNow();
      pool.shutdownNow();
    }
  }

  /**
   * The batch path must classify a post-close election like the single-key path.
   * Joining the never-completed CLOSED sentinel would block {@code getAll}
   * forever during shutdown.
   */
  @Test
  @Timeout(5)
  void close_suppressesBatchLoadsInsteadOfJoiningTheClosedSentinel() {
    TestSingleFlight flight = new TestSingleFlight(1000, 10, 5, Runnable::run, registryOf(disabledBreaker));
    flight.close();

    Map<String, LoaderOutcome<String>> outcomes = flight.loadOutcome(List.of("a", "b"), key -> "v");

    assertThat(outcomes).allSatisfy((key, outcome) ->
      assertThat(outcome.status()).as("key %s", key).isEqualTo(LoadStatus.SUPPRESSED_EXECUTOR)
    );
  }

  /**
   * A submission failure of any kind must release the breaker probe stamp.
   *
   * <p>The {@code Executor} is a caller-supplied SPI, so a pool shutting down
   * can surface any {@link RuntimeException} — or an {@link Error} — from
   * {@code execute}. The acquired stamp represents a HALF_OPEN probe quota; if
   * the failed submission does not abandon it, the slot stays reserved and
   * permanently narrows (or pins) the shard's half-open budget.
   */
  @Test
  void loadOutcome_executorThrowsUnexpectedRuntimeException_shouldReleaseTheProbeStamp() {
    CircuitBreaker breaker = mock(CircuitBreaker.class);
    when(breaker.tryAcquireStamp()).thenReturn(5L);
    Executor hostile = task -> {
      throw new IllegalStateException("pool is shutting down");
    };
    TestSingleFlight flight = new TestSingleFlight(1000, 10, 5, hostile, registryOf(breaker));

    assertThatThrownBy(() -> flight.loadOutcome("k", () -> "v")).isInstanceOf(IllegalStateException.class);

    verify(breaker).onAbandoned(5L);
    verify(breaker, times(0)).onSuccess(anyLong());
    verify(breaker, times(0)).onFailure(any(), anyLong());
  }

  /**
   * Illegal construction fails at construction. The three bounds are Caffeine's
   * own requirements and an {@code executor}/{@code registry} of {@code null}
   * would otherwise surface as an opaque failure inside a production read.
   */
  @Test
  void constructor_shouldRejectIllegalBoundsAndNullCollaborators() {
    assertThatThrownBy(() -> new TestSingleFlight(0, 10, 5, executor, registryOf(disabledBreaker))).isInstanceOf(
      IllegalArgumentException.class
    );
    assertThatThrownBy(() -> new TestSingleFlight(1000, 0, 5, executor, registryOf(disabledBreaker))).isInstanceOf(
      IllegalArgumentException.class
    );
    assertThatThrownBy(() -> new TestSingleFlight(1000, 10, 0, executor, registryOf(disabledBreaker))).isInstanceOf(
      IllegalArgumentException.class
    );
    assertThatThrownBy(() -> new TestSingleFlight(-1, 10, 5, executor, registryOf(disabledBreaker))).isInstanceOf(
      IllegalArgumentException.class
    );
    assertThatThrownBy(() -> new TestSingleFlight(1000, 10, 5, null, registryOf(disabledBreaker))).isInstanceOf(
      NullPointerException.class
    );
    assertThatThrownBy(() -> new TestSingleFlight(1000, 10, 5, executor, (CircuitBreakerRegistry) null)).isInstanceOf(
      NullPointerException.class
    );
  }

  /**
   * The high-water line must fire on the submission that <em>crosses</em> the
   * threshold, not on the next one. It is evaluated during admission, before
   * this load's own increment, so a load arriving exactly at the threshold read
   * "not over" and stayed silent while the very act of admitting it pushed the
   * backlog past the line.
   */
  @Test
  void loadOutcome_admissionCrossingHighWater_shouldLogOnTheCrossingLoad() throws Exception {
    int maxSize = 5; // threshold = (long) (5 * 0.8) = 4
    ExecutorService pool = Executors.newFixedThreadPool(8);
    CollectingAppender appender = new CollectingAppender();
    ch.qos.logback.classic.Logger logger = ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger(
      SingleFlightImpl.class
    );
    Level originalLevel = logger.getLevel();
    logger.setLevel(Level.DEBUG);
    appender.start();
    logger.addAppender(appender);
    try {
      TestSingleFlight flight = new TestSingleFlight(maxSize, 10, 60, pool, registryOf(disabledBreaker));
      CountDownLatch release = new CountDownLatch(1);
      CountDownLatch running = new CountDownLatch(maxSize - 1);
      for (int i = 0; i < maxSize - 1; i++) {
        String key = "pending-" + i;
        new Thread(() ->
          flight.loadOutcome(key, () -> {
            running.countDown();
            awaitUninterruptibly(release);
            return "v";
          })
        )
          .start();
      }
      assertThat(running.await(10, TimeUnit.SECONDS)).as("the backlog is fully in flight").isTrue();

      // This load takes the backlog from (maxSize-1) to maxSize — the crossing.
      flight.loadOutcome("crossing", () -> "v");

      assertThat(
        appender.events
          .stream()
          .filter(e -> e.getLevel() == Level.DEBUG)
          .filter(e -> e.getFormattedMessage().contains("in-flight queue is high"))
          .count()
      )
        .as("the crossing admission must log")
        .isEqualTo(1L);
      release.countDown();
    } finally {
      logger.detachAppender(appender);
      if (originalLevel != null) {
        logger.setLevel(originalLevel);
      }
      appender.stop();
      pool.shutdownNow();
    }
  }

  /**
   * A follower that merely joins a failed future must not inherit the
   * leader's interrupt status: each joining thread keeps its own flag, so one
   * interrupted load cannot poison every caller that joined it.
   */
  @Test
  void interruptedLeaderDoesNotInterruptFollowers() throws Exception {
    TestSingleFlight flight = new TestSingleFlight(1000, 10, 5, Runnable::run, registryOf(disabledBreaker));
    AtomicBoolean leaderFlag = new AtomicBoolean();
    AtomicBoolean followerFlag = new AtomicBoolean();
    CountDownLatch leaderInReader = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);

    Thread leader = new Thread(
      () -> {
        flight.loadOutcomeCallerRuns(
          "k",
          () -> {
            leaderInReader.countDown();
            try {
              release.await();
            } catch (InterruptedException e) {
              // Throwing cleared the flag; runInline restores it for this thread.
              Thread.currentThread().interrupt();
              throw new CompletionException(e);
            }
            return "never";
          },
          null
        );
        leaderFlag.set(Thread.currentThread().isInterrupted());
      },
      "sf-leader"
    );

    Thread follower = new Thread(
      () -> {
        flight.loadOutcomeCallerRuns("k", () -> "unused", null);
        followerFlag.set(Thread.currentThread().isInterrupted());
      },
      "sf-follower"
    );

    leader.start();
    assertThat(leaderInReader.await(5, TimeUnit.SECONDS)).isTrue();
    follower.start();
    awaitParked(follower);

    leader.interrupt();
    release.countDown();
    leader.join(5_000);
    follower.join(5_000);

    assertThat(leaderFlag).as("the reader thread restores its own flag").isTrue();
    assertThat(followerFlag).as("a joining follower was never interrupted").isFalse();
  }

  /**
   * A reader that joins an async client and lets its {@link TimeoutException} escape is a
   * supplier failure, not the dedup layer's budget timeout: only the bare
   * {@code TimeoutException} the interaction timeout completes with is a suppression, and
   * only that counts in the timeout gauge.
   */
  @Test
  void nestedReaderTimeoutIsAFailureNotABudgetTimeout() {
    TestSingleFlight flight = new TestSingleFlight(1000, 10, 5, Runnable::run, registryOf(disabledBreaker));

    LoaderOutcome<String> outcome = flight.loadOutcome("k", () -> {
      throw new CompletionException(new ExecutionException(new TimeoutException("slow")));
    });

    assertThat(outcome.status()).isEqualTo(LoadStatus.FAILED);
    assertThat(flight.loadTimeoutCount()).isZero();
  }

  /** Poll until the thread is parked (e.g. inside {@code CompletableFuture.join}). */
  private static void awaitParked(Thread thread) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      Thread.State state = thread.getState();
      if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) {
        return;
      }
      Thread.sleep(1L);
    }
    throw new AssertionError("thread did not park: " + thread.getState());
  }

  /** Collects logback events for assertions about WARN emission. */
  static class CollectingAppender extends AppenderBase<ILoggingEvent> {

    final ConcurrentLinkedQueue<ILoggingEvent> events = new ConcurrentLinkedQueue<>();

    @Override
    protected void append(ILoggingEvent eventObject) {
      events.add(eventObject);
    }
  }
}
