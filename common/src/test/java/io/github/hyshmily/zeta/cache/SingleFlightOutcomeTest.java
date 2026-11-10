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
import static org.mockito.Mockito.*;

import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.circuitbreaker.CircuitBreaker;
import io.github.hyshmily.zeta.cache.cachesupport.circuitbreaker.CircuitBreakerImpl;
import io.github.hyshmily.zeta.cache.cachesupport.circuitbreaker.CircuitBreakerRegistry;
import io.github.hyshmily.zeta.cache.cachesupport.loader.LoadStatus;
import io.github.hyshmily.zeta.cache.cachesupport.loader.LoaderOutcome;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlightImpl;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Tests for the explicit {@link LoaderOutcome} load paths: every terminal
 * state of the dedup execution maps to exactly one {@link LoadStatus}, and
 * the legacy {@code Optional} bridges preserve the historical
 * throw-on-failure behavior.
 */
class SingleFlightOutcomeTest {

  private static TestSingleFlight directFlight() {
    return new TestSingleFlight(1000, 10, 5, Runnable::run, disabledRegistry());
  }

  /** A registry wired the production way, whose every shard is a disabled (force-closed) breaker. */
  private static CircuitBreakerRegistry disabledRegistry() {
    return registryOf(new CircuitBreakerImpl(new ZetaProperties.CircuitBreaker()));
  }

  /** Production-shaped wiring (see {@code ZetaAutoConfiguration}): a registry whose every shard is {@code breaker}. */
  private static CircuitBreakerRegistry registryOf(CircuitBreaker breaker) {
    CircuitBreakerRegistry registry = mock(CircuitBreakerRegistry.class);
    when(registry.breakerFor(anyString())).thenReturn(breaker);
    return registry;
  }

  @Test
  void loadOutcome_loadedAndSourceEmpty() {
    TestSingleFlight sf = directFlight();

    LoaderOutcome<String> loaded = sf.loadOutcome("k1", () -> "v");
    assertThat(loaded.status()).isEqualTo(LoadStatus.LOADED);
    assertThat(loaded.value()).isEqualTo("v");

    LoaderOutcome<String> empty = sf.loadOutcome("k2", () -> null);
    assertThat(empty.status()).isEqualTo(LoadStatus.SOURCE_EMPTY);
    assertThat(empty.value()).isNull();
  }

  @Test
  void loadOutcome_readerFailure_isFailedWithCause() {
    TestSingleFlight sf = directFlight();
    IllegalStateException boom = new IllegalStateException("boom");

    LoaderOutcome<String> outcome = sf.loadOutcome("k", () -> {
      throw boom;
    });

    assertThat(outcome.status()).isEqualTo(LoadStatus.FAILED);
    assertThat(outcome.cause()).isSameAs(boom);
  }

  /**
   * A reader that submits to a saturated pool of its own throws
   * {@link RejectedExecutionException}; that is a reader failure, not a suppressed
   * submission by the dedup layer.
   */
  @Test
  void loadOutcome_readerRejectedExecution_isFailedNotSuppressed() {
    TestSingleFlight sf = directFlight();
    RejectedExecutionException rejected = new RejectedExecutionException("reader pool saturated");

    LoaderOutcome<String> outcome = sf.loadOutcome("k", () -> {
      throw rejected;
    });

    assertThat(outcome.status()).isEqualTo(LoadStatus.FAILED);
    assertThat(outcome.cause()).isSameAs(rejected);
  }

  /** A reader-thrown timeout is a supplier failure, not the dedup layer's own budget timeout. */
  @Test
  void loadOutcome_readerTimeout_isFailedNotSuppressed() {
    TestSingleFlight sf = directFlight();
    java.util.concurrent.TimeoutException timeout = new java.util.concurrent.TimeoutException("downstream");

    LoaderOutcome<String> outcome = sf.loadOutcome("k", () -> {
      throw new java.util.concurrent.CompletionException(timeout);
    });

    assertThat(outcome.status()).isEqualTo(LoadStatus.FAILED);
    assertThat(outcome.cause()).isSameAs(timeout);
  }

  @Test
  void loadOutcome_breakerOpen_isSuppressedBreakerWithoutRunningReader() {
    CircuitBreaker openBreaker = mock(CircuitBreaker.class);
    when(openBreaker.tryAcquireStamp()).thenReturn(CircuitBreaker.STAMP_REJECTED);
    when(openBreaker.isOpen()).thenReturn(true);
    TestSingleFlight sf = new TestSingleFlight(1000, 10, 5, Runnable::run, registryOf(openBreaker));

    LoaderOutcome<String> outcome = sf.loadOutcome("k", () -> {
      throw new AssertionError("reader must not run under an open breaker");
    });

    assertThat(outcome.status()).isEqualTo(LoadStatus.SUPPRESSED_BREAKER);
  }

  /**
   * A rejection that races the shutdown drain must not be mistaken for a leader:
   * the created-flight holder is null on both sides. The load settles as a
   * suppression instead of touching a null flight.
   */
  @Test
  void loadOutcome_breakerRejectsWhileCloseRuns_doesNotThrow() {
    CircuitBreaker openBreaker = mock(CircuitBreaker.class);
    AtomicReference<TestSingleFlight> flight = new AtomicReference<>();
    when(openBreaker.tryAcquireStamp()).thenAnswer(inv -> {
      flight.get().close();
      return CircuitBreaker.STAMP_REJECTED;
    });
    TestSingleFlight sf = new TestSingleFlight(1000, 10, 5, Runnable::run, registryOf(openBreaker));
    flight.set(sf);

    LoaderOutcome<String> outcome = sf.loadOutcome("k", () -> {
      throw new AssertionError("reader must not run after close");
    });

    assertThat(outcome.status()).isEqualTo(LoadStatus.SUPPRESSED_BREAKER);
  }

  @Test
  void loadIsolated_runsInlineAndCommitsWithoutPublishingAFlight() {
    TestSingleFlight sf = directFlight();
    AtomicInteger commits = new AtomicInteger();

    LoaderOutcome<String> outcome = sf.loadIsolated("k", () -> "v", value -> commits.incrementAndGet());

    assertThat(outcome.status()).isEqualTo(LoadStatus.LOADED);
    assertThat(outcome.value()).isEqualTo("v");
    assertThat(commits).hasValue(1);
    assertThat(sf.estimatedInFlightSize()).isZero();
  }

  @Test
  void loadIsolated_rejectedProbe_isSuppressedBreakerWithoutRunningReader() {
    CircuitBreaker openBreaker = mock(CircuitBreaker.class);
    when(openBreaker.tryAcquireStamp()).thenReturn(CircuitBreaker.STAMP_REJECTED);
    TestSingleFlight sf = new TestSingleFlight(1000, 10, 5, Runnable::run, registryOf(openBreaker));

    LoaderOutcome<String> outcome = sf.loadIsolated(
      "k",
      () -> {
        throw new AssertionError("reader must not run under an open breaker");
      },
      value -> {}
    );

    assertThat(outcome.status()).isEqualTo(LoadStatus.SUPPRESSED_BREAKER);
  }

  @Test
  void loadOutcome_executorReject_isSuppressedExecutor() {
    Executor rejecting = command -> {
      throw new RejectedExecutionException("saturated");
    };
    TestSingleFlight sf = new TestSingleFlight(1000, 10, 5, rejecting, disabledRegistry());

    LoaderOutcome<String> outcome = sf.loadOutcome("k", () -> "v");

    assertThat(outcome.status()).isEqualTo(LoadStatus.SUPPRESSED_EXECUTOR);
  }

  @Test
  void loadOutcome_timeout_isSuppressedTimeout() {
    Executor async = Executors.newCachedThreadPool();
    try {
      TestSingleFlight sf = new TestSingleFlight(1000, 10, 1, async, disabledRegistry());

      LoaderOutcome<String> outcome = sf.loadOutcome("k", () -> {
        try {
          Thread.sleep(5_000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return "too-late";
      });

      assertThat(outcome.status()).isEqualTo(LoadStatus.SUPPRESSED_TIMEOUT);
      assertThat(sf.loadTimeoutCount()).isEqualTo(1);
    } finally {
      ((ExecutorService) async).shutdownNow();
    }
  }

  @Test
  void loadOutcome_batch_reportsPerKeyWithoutThrowing() {
    TestSingleFlight sf = directFlight();

    Map<String, LoaderOutcome<String>> outcomes = sf.loadOutcome(List.of("a", "b", "c"), key -> {
      if (key.equals("b")) {
        throw new IllegalStateException("boom");
      }
      return key.equals("a") ? "v" : null;
    });

    assertThat(outcomes.get("a").status()).isEqualTo(LoadStatus.LOADED);
    assertThat(outcomes.get("b").status()).isEqualTo(LoadStatus.FAILED);
    assertThat(outcomes.get("b").cause()).isInstanceOf(IllegalStateException.class);
    assertThat(outcomes.get("c").status()).isEqualTo(LoadStatus.SOURCE_EMPTY);
  }

  @Test
  void loadOutcome_batch_breakerOpen_suppressesEveryKey() {
    CircuitBreaker openBreaker = mock(CircuitBreaker.class);
    when(openBreaker.tryAcquireStamp()).thenReturn(CircuitBreaker.STAMP_REJECTED);
    when(openBreaker.isOpen()).thenReturn(true);
    TestSingleFlight sf = new TestSingleFlight(1000, 10, 5, Runnable::run, registryOf(openBreaker));

    Map<String, LoaderOutcome<String>> outcomes = sf.loadOutcome(List.of("a", "b"), k -> "v");

    assertThat(outcomes.values()).allMatch(o -> o.status() == LoadStatus.SUPPRESSED_BREAKER);
  }

  @Test
  void loadOutcome_batchFailure_carriesFirstFailedCause() {
    TestSingleFlight sf = directFlight();

    Map<String, LoaderOutcome<String>> outcomes = sf.loadOutcome(List.of("a", "b"), k -> {
      if (k.equals("a")) {
        throw new IllegalStateException("boom");
      }
      return "v";
    });
    assertThat(outcomes.get("a").status()).isEqualTo(LoadStatus.FAILED);
    assertThat(outcomes.get("a").cause()).hasMessage("boom");
    assertThat(outcomes.get("b").status()).isEqualTo(LoadStatus.LOADED);
  }

  @Test
  void outcome_invariants_rejectBadCombinations() {
    assertThatThrownBy(() -> LoaderOutcome.loaded(null)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> LoaderOutcome.failed(null)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> LoaderOutcome.suppressed(LoadStatus.LOADED)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new LoaderOutcome<>("v", LoadStatus.FAILED, null)).isInstanceOf(
      IllegalArgumentException.class
    );
    assertThatThrownBy(() -> new LoaderOutcome<>("v", LoadStatus.FAILED, new RuntimeException("boom"))).isInstanceOf(
      IllegalArgumentException.class
    );
    assertThat(LoadStatus.SUPPRESSED_BREAKER.isSuppressed()).isTrue();
    assertThat(LoadStatus.FAILED.isSuppressed()).isFalse();
    assertThat(suppressedWithCause().status()).isEqualTo(LoadStatus.SUPPRESSED_TIMEOUT);
  }

  /**
   * Duplicate keys in one batch must elect exactly once: the reader runs a
   * single time for the duplicated key, one stamped breaker admission is
   * consumed and settled, and the key produces a single outcome entry.
   *
   * <p>Regression for the {@code putIfAbsent}-eager-evaluation defect: the
   * previous batch loop ran {@code elect} for every duplicate (method
   * arguments are evaluated before {@code putIfAbsent} can deduplicate), so
   * in the window where the first election's future had already failed out of
   * the dedup cache, the discarded second election consumed a second
   * {@code HALF_OPEN} probe stamp that was never settled — a permanently
   * leaked probe slot. {@code computeIfAbsent} never elects twice.
   */
  @Test
  void loadOutcome_duplicateKeys_electExactlyOnce() {
    CircuitBreaker breaker = mock(CircuitBreaker.class);
    when(breaker.tryAcquireStamp()).thenReturn(1L);
    TestSingleFlight sf = new TestSingleFlight(1000, 10, 5, Runnable::run, registryOf(breaker));

    AtomicInteger readerCalls = new AtomicInteger();
    Map<String, LoaderOutcome<String>> outcomes = sf.loadOutcome(
      List.of("dup", "dup", "dup"),
      k -> "v" + readerCalls.incrementAndGet()
    );

    assertThat(outcomes).containsOnlyKeys("dup");
    assertThat(outcomes.get("dup").status()).isEqualTo(LoadStatus.LOADED);
    assertThat(outcomes.get("dup").value()).isEqualTo("v1");
    assertThat(readerCalls.get()).isEqualTo(1);
    verify(breaker, times(1)).tryAcquireStamp();
    // The single consumed admission was settled exactly once (leader-only).
    verify(breaker, times(1)).onSuccess(1L);
    verify(breaker, never()).onFailure(any(Throwable.class), anyLong());
    verify(breaker, never()).onAbandoned(anyLong());
  }

  private static LoaderOutcome<String> suppressedWithCause() {
    return LoaderOutcome.suppressed(LoadStatus.SUPPRESSED_TIMEOUT);
  }
}
