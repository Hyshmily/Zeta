package io.github.hyshmily.hotkey.hotkeycache;

import static org.assertj.core.api.Assertions.assertThat;

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
