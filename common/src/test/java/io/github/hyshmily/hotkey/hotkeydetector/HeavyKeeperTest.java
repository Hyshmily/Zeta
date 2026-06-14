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
package io.github.hyshmily.hotkey.hotkeydetector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.AddResult;
import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.HeavyKeeper;
import io.github.hyshmily.hotkey.hotkeydetector.heavykepper.Item;
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

  /**
   * Verifies that the constructor rejects zero or negative values for topK.
   */
  @Test
  void constructor_shouldRejectInvalidK() {
    assertThatThrownBy(() -> new HeavyKeeper(0, WIDTH, DEPTH, DECAY, MIN_COUNT)).isInstanceOf(
      IllegalArgumentException.class
    );
    assertThatThrownBy(() -> new HeavyKeeper(-1, WIDTH, DEPTH, DECAY, MIN_COUNT)).isInstanceOf(
      IllegalArgumentException.class
    );
  }

  /**
   * Verifies that adding a single key with sufficient count marks it as hot.
   */
  @Test
  void add_Direct_shouldTrackSingleKey() {
    AddResult result = keeper.addDirect("key1", 10);
    assertThat(result.isHotKey()).isTrue();
    assertThat(result.currentKey()).isEqualTo("key1");
    assertThat(result.expelledKey()).isNull();
  }

  /**
   * Verifies that when the TopK set is full, adding a high-count key expels the least frequent key.
   */
  @Test
  void add_Direct_shouldReturnExpelledKeyWhenTopKFull() {
    for (int i = 0; i < TOP_K; i++) {
      AddResult result = keeper.addDirect("key" + i, 50);
      assertThat(result.isHotKey()).isTrue();
      assertThat(result.expelledKey()).isNull();
    }

    AddResult result = keeper.addDirect("newKey", 100);
    assertThat(result.isHotKey()).isTrue();
    assertThat(result.expelledKey()).isNotNull();
  }

  /**
   * Verifies that keys added with a count below the minimum threshold are not promoted to hot.
   */
  @Test
  void add_Direct_keysBelowMinCountShouldNotBeHot() {
    keeper = new HeavyKeeper(TOP_K, WIDTH, DEPTH, DECAY, 100);
    AddResult result = keeper.addDirect("lowFreqKey", 1);
    assertThat(result.isHotKey()).isFalse();
    assertThat(result.expelledKey()).isNull();
  }

  /**
   * Verifies that the list of hot keys is returned in descending order of count.
   */
  @Test
  void list_shouldReturnKeysInDescendingOrder() {
    keeper.addDirect("keyA", 5);
    keeper.addDirect("keyB", 50);
    keeper.addDirect("keyC", 500);
    keeper.addDirect("keyD", 500);

    List<Item> items = keeper.list();
    assertThat(items).isNotEmpty();
    for (int i = 1; i < items.size(); i++) {
      assertThat(items.get(i - 1).count()).isGreaterThanOrEqualTo(items.get(i).count());
    }
  }

  /**
   * Verifies that listTopN returns at most N items and never exceeds the configured TopK size.
   */
  @Test
  void listTopN_shouldReturnAtMostNItems() {
    for (int i = 0; i < 10; i++) {
      keeper.addDirect("key" + i, 20 + i);
    }
    assertThat(keeper.listTopN(2)).hasSize(2);
    assertThat(keeper.listTopN(100)).hasSizeLessThanOrEqualTo(TOP_K);
  }

  /**
   * Verifies that contains returns true for a key that has been added and promoted to hot.
   */
  @Test
  void contains_shouldReturnTrueForHotKey() {
    keeper.addDirect("hotKey", 50);
    assertThat(keeper.contains("hotKey")).isTrue();
  }

  /**
   * Verifies that contains returns false for a key that has never been added.
   */
  @Test
  void contains_shouldReturnFalseForUnknownKey() {
    assertThat(keeper.contains("unknown")).isFalse();
  }

  /**
   * Verifies that the total count reflects the sum of all additions across keys.
   */
  @Test
  void total_shouldTrackAllAdditions() {
    keeper.addDirect("k1", 10);
    keeper.addDirect("k2", 20);
    keeper.addDirect("k1", 30);
    assertThat(keeper.total()).isEqualTo(60);
  }

  /**
   * Verifies that the expelled queue contains items after eviction from a full TopK set.
   */
  @Test
  void expelled_shouldReturnNonEmptyQueueAfterEviction() {
    for (int i = 0; i < TOP_K + 5; i++) {
      keeper.addDirect("key" + i, 50);
    }
    assertThat(keeper.expelled()).isNotEmpty();
  }

  /**
   * Verifies that the fading operation reduces the total accumulated count.
   */
  @Test
  void fading_shouldHalveCounts() {
    keeper.addDirect("k1", 100);
    long before = keeper.total();
    keeper.fading();
    assertThat(keeper.total()).isLessThan(before);
  }

  /**
   * Verifies that after fading, keys whose count drops to zero are removed from the tracking set.
   */
  @Test
  void fading_shouldClearZeroCountKeys() {
    keeper.addDirect("k1", 1);
    keeper.fading();
    assertThat(keeper.contains("k1")).isFalse();
  }

  /**
   * Verifies that concurrent additions from multiple threads are safe and all counts are accurately tracked.
   */
  @Test
  void add_Direct_shouldBeThreadSafe() throws InterruptedException {
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
              keeper.addDirect("key" + threadId, 1);
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
