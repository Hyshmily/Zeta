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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.CircuitBreaker;
import io.github.hyshmily.zeta.cache.cachesupport.LoadStatus;
import io.github.hyshmily.zeta.cache.cachesupport.LoaderOutcome;
import io.github.hyshmily.zeta.cache.cachesupport.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.impl.CircuitBreakerImpl;
import io.github.hyshmily.zeta.cache.cachesupport.impl.SingleFlightImpl;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import org.junit.jupiter.api.Test;

/**
 * Tests for the explicit {@link LoaderOutcome} load paths: every terminal
 * state of the dedup execution maps to exactly one {@link LoadStatus}, and
 * the legacy {@code Optional} bridges preserve the historical
 * throw-on-failure behavior.
 */
class SingleFlightOutcomeTest {

  private static SingleFlight directFlight() {
    return new SingleFlightImpl(
      1000,
      10,
      5,
      Runnable::run,
      new CircuitBreakerImpl(new ZetaProperties.CircuitBreaker())
    );
  }

  @Test
  void loadOutcome_loadedAndSourceEmpty() {
    SingleFlight sf = directFlight();

    LoaderOutcome<String> loaded = sf.loadOutcome("k1", () -> "v");
    assertThat(loaded.status()).isEqualTo(LoadStatus.LOADED);
    assertThat(loaded.value()).isEqualTo("v");
    assertThat(loaded.toOptional()).contains("v");

    LoaderOutcome<String> empty = sf.loadOutcome("k2", () -> null);
    assertThat(empty.status()).isEqualTo(LoadStatus.SOURCE_EMPTY);
    assertThat(empty.toOptional()).isEmpty();
  }

  @Test
  void loadOutcome_readerFailure_isFailedWithCause() {
    SingleFlight sf = directFlight();
    IllegalStateException boom = new IllegalStateException("boom");

    LoaderOutcome<String> outcome = sf.loadOutcome("k", () -> {
      throw boom;
    });

    assertThat(outcome.status()).isEqualTo(LoadStatus.FAILED);
    assertThat(outcome.cause()).isSameAs(boom);
    assertThat(outcome.toOptional()).isEmpty();
  }

  @Test
  void loadOutcome_breakerOpen_isSuppressedBreakerWithoutRunningReader() {
    CircuitBreaker openBreaker = mock(CircuitBreaker.class);
    when(openBreaker.allowRequest()).thenReturn(false);
    when(openBreaker.isOpen()).thenReturn(true);
    SingleFlight sf = new SingleFlightImpl(1000, 10, 5, Runnable::run, openBreaker);

    LoaderOutcome<String> outcome = sf.loadOutcome("k", () -> {
      throw new AssertionError("reader must not run under an open breaker");
    });

    assertThat(outcome.status()).isEqualTo(LoadStatus.SUPPRESSED_BREAKER);
  }

  @Test
  void loadOutcome_executorReject_isSuppressedExecutor() {
    Executor rejecting = command -> {
      throw new RejectedExecutionException("saturated");
    };
    SingleFlight sf = new SingleFlightImpl(
      1000,
      10,
      5,
      rejecting,
      new CircuitBreakerImpl(new ZetaProperties.CircuitBreaker())
    );

    LoaderOutcome<String> outcome = sf.loadOutcome("k", () -> "v");

    assertThat(outcome.status()).isEqualTo(LoadStatus.SUPPRESSED_EXECUTOR);
  }

  @Test
  void loadOutcome_timeout_isSuppressedTimeout() {
    Executor async = Executors.newCachedThreadPool();
    try {
      SingleFlight sf = new SingleFlightImpl(
        1000,
        10,
        1,
        async,
        new CircuitBreakerImpl(new ZetaProperties.CircuitBreaker())
      );

      LoaderOutcome<String> outcome = sf.loadOutcome("k", () -> {
        try {
          Thread.sleep(5_000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return "too-late";
      });

      assertThat(outcome.status()).isEqualTo(LoadStatus.SUPPRESSED_TIMEOUT);
      assertThat(sf.getLoadTimeoutCount()).isEqualTo(1);
    } finally {
      ((ExecutorService) async).shutdownNow();
    }
  }

  @Test
  void loadOutcome_batch_reportsPerKeyWithoutThrowing() {
    SingleFlight sf = directFlight();

    Map<String, LoaderOutcome<String>> outcomes = sf.loadOutcome(
      List.of("a", "b", "c"),
      key -> {
        if (key.equals("b")) {
          throw new IllegalStateException("boom");
        }
        return key.equals("a") ? "v" : null;
      }
    );

    assertThat(outcomes.get("a").status()).isEqualTo(LoadStatus.LOADED);
    assertThat(outcomes.get("b").status()).isEqualTo(LoadStatus.FAILED);
    assertThat(outcomes.get("b").cause()).isInstanceOf(IllegalStateException.class);
    assertThat(outcomes.get("c").status()).isEqualTo(LoadStatus.SOURCE_EMPTY);
  }

  @Test
  void loadOutcome_batch_breakerOpen_suppressesEveryKey() {
    CircuitBreaker openBreaker = mock(CircuitBreaker.class);
    when(openBreaker.allowRequest()).thenReturn(false);
    when(openBreaker.isOpen()).thenReturn(true);
    SingleFlight sf = new SingleFlightImpl(1000, 10, 5, Runnable::run, openBreaker);

    Map<String, LoaderOutcome<String>> outcomes = sf.loadOutcome(List.of("a", "b"), k -> "v");

    assertThat(outcomes.values()).allMatch(o -> o.status() == LoadStatus.SUPPRESSED_BREAKER);
  }

  @Test
  void load_legacyBridge_rethrowsFailedAndEmptiesSuppressed() {
    SingleFlight sf = directFlight();

    assertThatThrownBy(() ->
      sf.load("k", () -> {
        throw new IllegalStateException("boom");
      })
    )
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("boom");

    CircuitBreaker openBreaker = mock(CircuitBreaker.class);
    when(openBreaker.allowRequest()).thenReturn(false);
    SingleFlight suppressed = new SingleFlightImpl(1000, 10, 5, Runnable::run, openBreaker);
    assertThat(suppressed.load("k", () -> "v")).isEmpty();
    assertThat(suppressed.load("k", () -> null)).isEmpty();
  }

  @Test
  void load_legacyBatchFailFast_propagatesFirstFailed() {
    SingleFlight sf = directFlight();

    assertThatThrownBy(() ->
      sf.load(
        List.of("a", "b"),
        k -> {
          if (k.equals("a")) {
            throw new IllegalStateException("boom");
          }
          return "v";
        },
        true
      )
    )
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("boom");
  }

  @Test
  void outcome_invariants_rejectBadCombinations() {
    assertThatThrownBy(() -> LoaderOutcome.loaded(null)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> LoaderOutcome.failed(null)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> LoaderOutcome.suppressed(LoadStatus.LOADED)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new LoaderOutcome<>("v", LoadStatus.FAILED, null))
      .isInstanceOf(IllegalArgumentException.class);
    assertThat(suppressedWithCause().isSuppressed()).isTrue();
  }

  private static LoaderOutcome<String> suppressedWithCause() {
    return LoaderOutcome.suppressed(LoadStatus.SUPPRESSED_TIMEOUT);
  }
}
