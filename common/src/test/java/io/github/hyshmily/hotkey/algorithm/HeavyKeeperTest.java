package io.github.hyshmily.hotkey.algorithm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link HeavyKeeper}, covering construction validation, single-key and multi-key tracking,
 * expulsion behavior, minimum-count thresholds, ordering, containment queries, total accumulation,
 * decay fading, and concurrency safety.
 */
class HeavyKeeperTest {

  private static final int TOP_K = 3;
  private static final int WIDTH = 1000;
  private static final int DEPTH = 4;
  private static final double DECAY = 0.9;
  private static final int MIN_COUNT = 5;

  private HeavyKeeper keeper;

  @BeforeEach
  void setUp() {
    keeper = new HeavyKeeper(TOP_K, WIDTH, DEPTH, DECAY, MIN_COUNT);
  }

  @Test
  void constructor_shouldRejectInvalidK() {
    assertThatThrownBy(() -> new HeavyKeeper(0, WIDTH, DEPTH, DECAY, MIN_COUNT))
      .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new HeavyKeeper(-1, WIDTH, DEPTH, DECAY, MIN_COUNT))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void add_shouldTrackSingleKey() {
    AddResult result = keeper.add("key1", 10);
    assertThat(result.isHotKey()).isTrue();
    assertThat(result.currentKey()).isEqualTo("key1");
    assertThat(result.expelledKey()).isNull();
  }

  @Test
  void add_shouldReturnExpelledKeyWhenTopKFull() {
    for (int i = 0; i < TOP_K; i++) {
      AddResult result = keeper.add("key" + i, 50);
      assertThat(result.isHotKey()).isTrue();
      assertThat(result.expelledKey()).isNull();
    }

    AddResult result = keeper.add("newKey", 100);
    assertThat(result.isHotKey()).isTrue();
    assertThat(result.expelledKey()).isNotNull();
  }

  @Test
  void add_keysBelowMinCountShouldNotBeHot() {
    keeper = new HeavyKeeper(TOP_K, WIDTH, DEPTH, DECAY, 100);
    AddResult result = keeper.add("lowFreqKey", 1);
    assertThat(result.isHotKey()).isFalse();
    assertThat(result.expelledKey()).isNull();
  }

  @Test
  void list_shouldReturnKeysInDescendingOrder() {
    keeper.add("keyA", 5);
    keeper.add("keyB", 50);
    keeper.add("keyC", 500);
    keeper.add("keyD", 500);

    List<Item> items = keeper.list();
    assertThat(items).isNotEmpty();
    for (int i = 1; i < items.size(); i++) {
      assertThat(items.get(i - 1).count()).isGreaterThanOrEqualTo(items.get(i).count());
    }
  }

  @Test
  void listTopN_shouldReturnAtMostNItems() {
    for (int i = 0; i < 10; i++) {
      keeper.add("key" + i, 20 + i);
    }
    assertThat(keeper.listTopN(2)).hasSize(2);
    assertThat(keeper.listTopN(100)).hasSizeLessThanOrEqualTo(TOP_K);
  }

  @Test
  void contains_shouldReturnTrueForHotKey() {
    keeper.add("hotKey", 50);
    assertThat(keeper.contains("hotKey")).isTrue();
  }

  @Test
  void contains_shouldReturnFalseForUnknownKey() {
    assertThat(keeper.contains("unknown")).isFalse();
  }

  @Test
  void total_shouldTrackAllAdditions() {
    keeper.add("k1", 10);
    keeper.add("k2", 20);
    keeper.add("k1", 30);
    assertThat(keeper.total()).isEqualTo(60);
  }

  @Test
  void expelled_shouldReturnNonEmptyQueueAfterEviction() {
    for (int i = 0; i < TOP_K + 5; i++) {
      keeper.add("key" + i, 50);
    }
    assertThat(keeper.expelled()).isNotEmpty();
  }

  @Test
  void fading_shouldHalveCounts() {
    keeper.add("k1", 100);
    long before = keeper.total();
    keeper.fading();
    assertThat(keeper.total()).isLessThan(before);
  }

  @Test
  void fading_shouldClearZeroCountKeys() {
    keeper.add("k1", 1);
    keeper.fading();
    assertThat(keeper.contains("k1")).isFalse();
  }

  @Test
  void add_shouldBeThreadSafe() throws InterruptedException {
    int threadCount = 10;
    int iterations = 100;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);
    AtomicInteger errors = new AtomicInteger(0);

    for (int t = 0; t < threadCount; t++) {
      final int threadId = t;
      executor.submit(() -> {
        try {
          for (int i = 0; i < iterations; i++) {
            try {
              keeper.add("key" + threadId, 1);
            } catch (Exception e) {
              errors.incrementAndGet();
            }
          }
        } finally {
          latch.countDown();
        }
      });
    }

    assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
    executor.shutdown();
    assertThat(errors.get()).isZero();
    assertThat(keeper.total()).isEqualTo((long) threadCount * iterations);
  }
}
