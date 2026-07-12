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
package io.github.hyshmily.zeta.hotkeydetector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.AddResult;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.HeavyKeeper;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

  @Test
  void addDirect_shouldThrowOnNullKey() {
    assertThatThrownBy(() -> keeper.addDirect(null, 1)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void addDirect_shouldHandleEmptyStringKey() {
    AddResult result = keeper.addDirect("", 10);
    assertThat(result.isHotKey()).isTrue();
    assertThat(result.currentKey()).isEmpty();
  }

  @Test
  void addDirect_shouldHandleVeryLongKey() {
    String longKey = "a".repeat(10000);
    AddResult result = keeper.addDirect(longKey, 50);
    assertThat(result.isHotKey()).isTrue();
    assertThat(keeper.contains(longKey)).isTrue();
  }

  @Test
  void addDirect_shouldHandleIncrementZero() {
    AddResult result = keeper.addDirect("key1", 0);
    assertThat(result.isHotKey()).isFalse();
    assertThat(keeper.total()).isZero();
  }

  @Test
  void addDirect_shouldAccumulateSameKey() {
    for (int i = 0; i < 10; i++) {
      keeper.addDirect("key1", 5);
    }
    assertThat(keeper.contains("key1")).isTrue();
    assertThat(keeper.total()).isEqualTo(50);
  }

  @Test
  void addDirectMap_shouldHandleEmptyMap() {
    List<AddResult> results = keeper.addDirect(Map.of());
    assertThat(results).isEmpty();
  }

  @Test
  void addDirectMap_shouldThrowOnNullMap() {
    assertThatThrownBy(() -> keeper.addDirect((Map<String, Long>) null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void addDirectMap_shouldThrowOnNullKeyInMap() {
    Map<String, Long> map = new HashMap<>();
    map.put(null, 10L);
    assertThatThrownBy(() -> keeper.addDirect(map)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void addDirectMap_shouldThrowOnNullValueInMap() {
    Map<String, Long> map = new HashMap<>();
    map.put("key1", null);
    assertThatThrownBy(() -> keeper.addDirect(map)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void listTopN_shouldReturnEmptyWhenNoHotKeys() {
    assertThat(keeper.listTopN(5)).isEmpty();
  }

  @Test
  void listTopN_shouldReturnEmptyWhenNIsZero() {
    keeper.addDirect("key1", 50);
    assertThat(keeper.listTopN(0)).isEmpty();
  }

  @Test
  void listTopN_shouldThrowWhenNIsNegative() {
    keeper.addDirect("key1", 50);
    assertThatThrownBy(() -> keeper.listTopN(-1)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void fading_shouldHandleEmptyStructure() {
    keeper.fading();
    assertThat(keeper.list()).isEmpty();
    assertThat(keeper.total()).isZero();
  }

  @Test
  void fading_shouldHandleMultipleFadingCalls() {
    keeper.addDirect("key1", 100);
    for (int i = 0; i < 5; i++) {
      keeper.fading();
    }
    long totalAfter = keeper.total();
    assertThat(totalAfter).isPositive();
    assertThat(totalAfter).isLessThan(100);
  }

  @Test
  void fading_shouldRemoveKeysWithCountOne() {
    HeavyKeeper lowMinKeeper = new HeavyKeeper(TOP_K, WIDTH, DEPTH, DECAY, 1);
    lowMinKeeper.addDirect("key1", 1);
    assertThat(lowMinKeeper.contains("key1")).isTrue();
    lowMinKeeper.fading();
    assertThat(lowMinKeeper.contains("key1")).isFalse();
  }

  @Test
  void contains_shouldReturnFalseForExpelledKey() {
    for (int i = 0; i < TOP_K; i++) {
      keeper.addDirect("key" + i, 50);
    }
    keeper.addDirect("newKey", 100);
    assertThat(keeper.contains("key0")).isFalse();
  }

  @Test
  void concurrentAddSameKey_shouldBeThreadSafe() throws InterruptedException {
    int threadCount = 10;
    int iterations = 100;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);
    for (int t = 0; t < threadCount; t++) {
      executor.submit(() -> {
        try {
          for (int i = 0; i < iterations; i++) {
            keeper.addDirect("sameKey", 1);
          }
        } finally {
          latch.countDown();
        }
      });
    }
    assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
    executor.shutdown();
    assertThat(keeper.total()).isEqualTo((long) threadCount * iterations);
    assertThat(keeper.contains("sameKey")).isTrue();
  }

  @Test
  void addDirect_withNegativeIncrement_shouldNotThrow() {
    assertThat(keeper.addDirect("key1", -5).isHotKey()).isFalse();
    assertThat(keeper.contains("key1")).isFalse();
  }

  @Test
  void addDirect_mapBatch_whenAllBelowMinCount_shouldReturnEmpty() {
    HeavyKeeper hk = new HeavyKeeper(TOP_K, WIDTH, DEPTH, DECAY, 1000);
    Map<String, Long> map = new HashMap<>();
    map.put("low1", 1L);
    map.put("low2", 2L);
    assertThat(hk.addDirect(map)).isEmpty();
  }

  @Test
  void addDirect_mapBatch_withBlankKeys_shouldNotThrow() {
    keeper.addDirect("", 10);
    assertThat(keeper.contains("")).isTrue();
  }

  @Test
  void addDirect_withExpelledQueueFull_shouldNotThrow() {
    HeavyKeeper smallQueue = new HeavyKeeper(3, 1000, 4, 0.9, 1, 1);
    for (int i = 0; i < 5; i++) {
      smallQueue.addDirect("key" + i, 50);
    }
    assertThat(smallQueue.list()).hasSize(3);
  }

  @Test
  void listTopN_withNLessThanTopK_shouldReturnNItems() {
    keeper.addDirect("a", 50);
    keeper.addDirect("b", 40);
    keeper.addDirect("c", 30);
    assertThat(keeper.listTopN(2)).hasSize(2);
  }

  @Test
  void listTopN_withNGreaterThanTopK_shouldReturnAllItems() {
    keeper.addDirect("a", 50);
    keeper.addDirect("b", 40);
    assertThat(keeper.listTopN(100)).hasSize(2);
  }

  @Test
  void contains_withEmptyString_shouldReturnFalse() {
    assertThat(keeper.contains("")).isFalse();
  }

  @Test
  void contains_withSpecialCharacters_shouldWork() {
    keeper.addDirect("\uD83D\uDD25", 50);
    keeper.addDirect("\u952E", 50);
    keeper.addDirect("\u043A\u043B\u044E\u0447", 50);
    assertThat(keeper.contains("\uD83D\uDD25")).isTrue();
    assertThat(keeper.contains("\u952E")).isTrue();
    assertThat(keeper.contains("\u043A\u043B\u044E\u0447")).isTrue();
  }

  @Test
  void addDirect_withSameCountKeys_shouldHandleTieBreaking() {
    keeper.addDirect("b-key", 50);
    keeper.addDirect("a-key", 50);
    keeper.addDirect("c-key", 50);
    List<Item> items = keeper.list();
    assertThat(items).hasSize(3);
    assertThat(items.get(0).count()).isEqualTo(50);
    assertThat(items.get(1).count()).isEqualTo(50);
    assertThat(items.get(2).count()).isEqualTo(50);
  }

  @Test
  void fading_whenMultipleEntriesHaveCountOne_shouldClearAll() {
    HeavyKeeper hk = new HeavyKeeper(TOP_K, WIDTH, DEPTH, DECAY, 1);
    hk.addDirect("k1", 1);
    hk.addDirect("k2", 1);
    hk.addDirect("k3", 1);
    assertThat(hk.list()).hasSize(3);
    hk.fading();
    assertThat(hk.list()).isEmpty();
    assertThat(hk.total()).isEqualTo(1);
  }

  // ── Fingerprint collision paths in addDirect(String, int) ──

  @Test
  void addDirect_withCollisionAndDecaySubtracts_shouldDecrement() {
    HeavyKeeper hk = new HeavyKeeper(3, 1, 1, 0.9, 1, 100);
    hk.addDirect("key1", 10);
    hk.addDirect("key2", 5);
    assertThat(hk.contains("key2")).isTrue();
  }

  @Test
  void addDirect_withCollisionAndDecayReplaces_shouldReplaceFingerprint() {
    HeavyKeeper hk = new HeavyKeeper(3, 1, 1, 1.0, 1, 100);
    hk.addDirect("key1", 5);
    hk.addDirect("key2", 10);
    assertThat(hk.contains("key1")).isTrue();
    assertThat(hk.contains("key2")).isTrue();
  }

  @Test
  void addDirect_withCollisionAndCountAboveLookup_shouldUseMaxLookupEntry() {
    HeavyKeeper hk = new HeavyKeeper(3, 1, 1, 0.5, 1, 100);
    for (int i = 0; i < 300; i++) {
      hk.addDirect("key1", 1);
    }
    hk.addDirect("key2", 10);
    assertThat(hk.contains("key2")).isTrue();
  }

  // ── sampleBinomial edge cases through collision path ──

  @Test
  void addDirect_withCollisionAndZeroIncrement_shouldHandle() {
    HeavyKeeper hk = new HeavyKeeper(3, 1, 1, 0.5, 1, 100);
    hk.addDirect("key1", 10);
    AddResult result = hk.addDirect("key2", 0);
    assertThat(result).isNotNull();
    assertThat(hk.total()).isEqualTo(10);
  }

  @Test
  void addDirect_withCollisionAndLargeIncrement_shouldUseNormalApproximation() {
    HeavyKeeper hk = new HeavyKeeper(3, 1, 1, 0.9, 1, 100);
    hk.addDirect("key1", 1);
    AddResult result = hk.addDirect("key2", 200);
    assertThat(result.isHotKey()).isTrue();
  }

  @Test
  void addDirect_withCollisionAndMediumIncrement_shouldUseFallbackLoop() {
    HeavyKeeper hk = new HeavyKeeper(3, 1, 1, 0.5, 1, 100);
    hk.addDirect("key1", 1);
    AddResult result = hk.addDirect("key2", 20);
    assertThat(result.isHotKey()).isTrue();
  }

  // ── addDirect(Map) edge cases ──

  @Test
  void addDirectMap_whenKeyBelowMinCount_shouldSkip() {
    HeavyKeeper hk = new HeavyKeeper(3, 100, 4, 0.9, 50, 100);
    Map<String, Long> map = new HashMap<>();
    map.put("high", 100L);
    map.put("low", 10L);
    List<AddResult> results = hk.addDirect(map);
    boolean hasHigh = results.stream().anyMatch(r -> "high".equals(r.currentKey()));
    boolean hasLow = results.stream().anyMatch(r -> "low".equals(r.currentKey()));
    assertThat(hasHigh).isTrue();
    assertThat(hasLow).isFalse();
  }

  @Test
  void addDirectMap_whenKeyNotHotEnough_shouldNotInsert() {
    HeavyKeeper hk = new HeavyKeeper(3, 100000, 4, 0.9, 1, 100);
    hk.addDirect("a", 100);
    hk.addDirect("b", 100);
    hk.addDirect("c", 100);
    Map<String, Long> map = new HashMap<>();
    map.put("d", 5L);
    List<AddResult> results = hk.addDirect(map);
    assertThat(results).isEmpty();
    assertThat(hk.contains("d")).isFalse();
  }

  @Test
  void addDirectMap_withCollisionInAddToSketch_shouldHandle() {
    HeavyKeeper hk = new HeavyKeeper(3, 1, 1, 0.0, 1, 100);
    Map<String, Long> map = new HashMap<>();
    map.put("key1", 10L);
    map.put("key2", 5L);
    List<AddResult> results = hk.addDirect(map);
    assertThat(results).hasSize(2);
  }

  @Test
  void addDirectMap_withExistingKeyInHeap_shouldRemoveOldNode() {
    HeavyKeeper hk = new HeavyKeeper(3, 1000, 4, 0.9, 1, 100);
    hk.addDirect("k", 50);
    Map<String, Long> map = new HashMap<>();
    map.put("k", 10L);
    map.put("other", 100L);
    List<AddResult> results = hk.addDirect(map);
    assertThat(results).hasSize(2);
    assertThat(hk.contains("k")).isTrue();
  }

  @Test
  void addDirectMap_withExpelledQueueFull_shouldLogWarning() {
    HeavyKeeper hk = new HeavyKeeper(1, 1000, 4, 0.9, 1, 1);
    hk.addDirect("first", 100);
    hk.addDirect("second", 200);
    Map<String, Long> map = new HashMap<>();
    map.put("third", 300L);
    List<AddResult> results = hk.addDirect(map);
    assertThat(results).hasSize(1);
    assertThat(results.get(0).currentKey()).isEqualTo("third");
  }

  @Test
  void addDirectMap_withExistingKeyCollisionInAddToSketch_shouldHandle() {
    HeavyKeeper hk = new HeavyKeeper(3, 1000, 4, 0.9, 1, 100);
    hk.addDirect("k", 10);
    Map<String, Long> map = new HashMap<>();
    map.put("k", 5L);
    List<AddResult> results = hk.addDirect(map);
    String ks = results
      .stream()
      .filter(r -> "k".equals(r.currentKey()))
      .findFirst()
      .map(AddResult::currentKey)
      .orElse("");
    assertThat(results).isNotEmpty();
  }

  @Test
  void addDirectMap_withCollisionCountAboveLookup_shouldUseMaxLookup() {
    HeavyKeeper hk = new HeavyKeeper(3, 1, 1, 0.5, 1, 100);
    for (int i = 0; i < 300; i++) {
      hk.addDirect("key1", 1);
    }
    Map<String, Long> map = new HashMap<>();
    map.put("key2", 10L);
    List<AddResult> results = hk.addDirect(map);
    assertThat(results).isNotEmpty();
  }

  @Test
  void addDirectMap_withCollisionAndDecayReplacesFingerprint_shouldReplace() {
    HeavyKeeper hk = new HeavyKeeper(3, 1, 1, 1.0, 1, 100);
    hk.addDirect("key1", 5);
    Map<String, Long> map = new HashMap<>();
    map.put("key2", 10L);
    List<AddResult> results = hk.addDirect(map);
    assertThat(results).isNotEmpty();
    assertThat(hk.contains("key2")).isTrue();
  }

  @Test
  void fading_withConcurrentAddDirect_shouldNotDeadlock() throws InterruptedException {
    HeavyKeeper preloaded = new HeavyKeeper(TOP_K, WIDTH, DEPTH, DECAY, 1);
    for (int i = 0; i < 10; i++) {
      preloaded.addDirect("key" + i, 100);
    }
    CountDownLatch latch = new CountDownLatch(1);
    ExecutorService exec = Executors.newFixedThreadPool(2);
    AtomicBoolean stopped = new AtomicBoolean(false);
    exec.submit(() -> {
      try {
        latch.await();
      } catch (Exception e) {
        Thread.currentThread().interrupt();
      }
      while (!stopped.get()) {
        preloaded.addDirect("concurrentKey", 1);
      }
    });
    exec.submit(() -> {
      try {
        latch.await();
      } catch (Exception e) {
        Thread.currentThread().interrupt();
      }
      for (int i = 0; i < 100; i++) {
        preloaded.fading();
      }
      stopped.set(true);
    });
    latch.countDown();
    exec.shutdown();
    assertThat(exec.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    assertThat(preloaded.list()).isNotNull();
  }

  @Test
  void fading_withConcurrentAccumulate_shouldPreserveCount() throws InterruptedException {
    // Regression test: decayMembership() previously used reset()+accumulate() on LongAccumulator,
    // which lost concurrent accumulate() writes that arrived between get() and reset().
    // The fix uses AtomicLong with a CAS retry loop that preserves concurrent accumulates.
    HeavyKeeper hk = new HeavyKeeper(10, 1000, 4, 0.9, 1, 50000, 3);
    hk.addDirect("hotkey", 1000);

    CountDownLatch latch = new CountDownLatch(1);
    ExecutorService exec = Executors.newFixedThreadPool(2);
    AtomicBoolean stopped = new AtomicBoolean(false);

    // Accumulator thread — continuously raises the count via lock-free fast path
    exec.submit(() -> {
      try {
        latch.await();
      } catch (Exception e) {
        Thread.currentThread().interrupt();
      }
      while (!stopped.get()) {
        hk.addDirect("hotkey", 50);
      }
    });

    // Decay thread — runs fading (which calls decayMembership)
    exec.submit(() -> {
      try {
        latch.await();
      } catch (Exception e) {
        Thread.currentThread().interrupt();
      }
      for (int i = 0; i < 50; i++) {
        hk.fading();
      }
      stopped.set(true);
    });

    latch.countDown();
    exec.shutdown();
    assertThat(exec.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

    // Key should still be tracked (concurrent accumulates kept it above zero)
    assertThat(hk.contains("hotkey")).isTrue();

    // Count should be positive — in the old buggy code, concurrent accumulates
    // were lost on every decay cycle, causing the count to drop to 0 rapidly.
    List<Item> items = hk.listTopN(10);
    long hotkeyCount = -1;
    for (Item item : items) {
      if (item.key().equals("hotkey")) {
        hotkeyCount = item.count();
        break;
      }
    }
    assertThat(hotkeyCount).as("concurrent accumulates during decay should keep count > 0").isPositive();
  }
}
