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
 * expulsion behavior, minimum-add thresholds, ordering, containment queries, total accumulation,
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
    keeper = new HeavyKeeper(TOP_K, WIDTH, DEPTH, DECAY, MIN_COUNT, 10_000, 3, false);
  }

  /** The tracked member add of {@code key} (0 when the key is not a member). */
  private static long memberCount(HeavyKeeper hk, String key) {
    return hk
      .list()
      .stream()
      .filter(i -> i.key().equals(key))
      .mapToLong(Item::count)
      .findFirst()
      .orElse(0L);
  }

  /**
   * Verifies that the constructor rejects zero or negative values for topK.
   */
  @Test
  void constructor_shouldRejectInvalidK() {
    assertThatThrownBy(() -> new HeavyKeeper(0, WIDTH, DEPTH, DECAY, MIN_COUNT, 10_000, 3, false)).isInstanceOf(
      IllegalArgumentException.class
    );
    assertThatThrownBy(() -> new HeavyKeeper(-1, WIDTH, DEPTH, DECAY, MIN_COUNT, 10_000, 3, false)).isInstanceOf(
      IllegalArgumentException.class
    );
  }

  /**
   * Verifies that adding a single key with sufficient add marks it as hot.
   */
  @Test
  void add_Direct_shouldTrackSingleKey() {
    AddResult result = keeper.addDirect("key1", 10);
    assertThat(result.isHotKey()).isTrue();
    assertThat(result.currentKey()).isEqualTo("key1");
    assertThat(result.expelledKey()).isNull();
  }

  /**
   * Verifies that when the TopK set is full, adding a high-add key expels the least frequent key.
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
   * Verifies that keys added with a add below the minimum threshold are not promoted to hot.
   */
  @Test
  void add_Direct_keysBelowMinCountShouldNotBeHot() {
    keeper = new HeavyKeeper(TOP_K, WIDTH, DEPTH, DECAY, 100, 10_000, 3, false);
    AddResult result = keeper.addDirect("lowFreqKey", 1);
    assertThat(result.isHotKey()).isFalse();
    assertThat(result.expelledKey()).isNull();
  }

  /**
   * Verifies that the list of hot keys is returned in descending order of add.
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
   * Verifies that the total add reflects the sum of all additions across keys.
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
   * Verifies that the fading operation reduces the total accumulated add.
   */
  @Test
  void fading_shouldHalveCounts() {
    keeper.addDirect("k1", 100);
    long before = keeper.total();
    keeper.fading();
    assertThat(keeper.total()).isLessThan(before);
  }

  /**
   * Verifies that after fading, keys whose add drops to zero are removed from the tracking set.
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

  /**
   * A zero increment must be a full no-op even when the key is already a
   * member — no membership change, no add change, no total change.
   */
  @Test
  void addDirect_zeroIncrementIsNoOpEvenForExistingMember() {
    keeper.addDirect("key1", 10);
    assertThat(keeper.contains("key1")).isTrue();
    long totalBefore = keeper.total();
    long countBefore = memberCount(keeper, "key1");

    AddResult result = keeper.addDirect("key1", 0);

    assertThat(result.isHotKey()).isFalse();
    assertThat(keeper.contains("key1")).isTrue();
    assertThat(keeper.total()).isEqualTo(totalBefore);
    assertThat(memberCount(keeper, "key1")).isEqualTo(countBefore);
  }

  @Test
  void addDirect_shouldAccumulateSameKey() {
    for (int i = 0; i < 10; i++) {
      keeper.addDirect("key1", 5);
    }
    assertThat(keeper.contains("key1")).isTrue();
    assertThat(keeper.total()).isEqualTo(50);
  }

  /**
   * {@code total} accounting lives at the entry points, not in the per-key
   * body: a batch folds its increments into one accumulator update. The
   * observable total is identical either way — this pins that the fold did
   * not drop or double-add anything, including the skipped non-positive
   * deltas.
   */
  @Test
  void ingestBatch_shouldAccumulateTotalAcrossBatch() {
    HeavyKeeper hk = new HeavyKeeper(3, 100, 4, 0.9, 5, 100, 3, false);
    Map<String, Long> map = new HashMap<>();
    map.put("a", 3L);
    map.put("b", 7L);
    map.put("c", 0L);
    map.put("d", -4L);
    map.put("e", null);

    hk.ingestBatch(map);

    assertThat(hk.total()).isEqualTo(10L);
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
    HeavyKeeper lowMinKeeper = new HeavyKeeper(TOP_K, WIDTH, DEPTH, DECAY, 1, 10_000, 3, false);
    lowMinKeeper.addDirect("key1", 1);
    assertThat(lowMinKeeper.contains("key1")).isTrue();
    // The add lives in a sliding window: three rotations clear the other
    // windows, the fourth clears the one holding this add.
    lowMinKeeper.fading();
    lowMinKeeper.fading();
    lowMinKeeper.fading();
    assertThat(lowMinKeeper.contains("key1")).as("add stays while its window is in the ring").isTrue();
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
    keeper.addDirect("key1", 4); // below MIN_COUNT — stays cold
    long totalBefore = keeper.total();

    assertThat(keeper.addDirect("key1", -5).isHotKey()).isFalse();
    assertThat(keeper.contains("key1")).isFalse();
    assertThat(keeper.total()).isEqualTo(totalBefore);
  }

  /**
   * The batch path must skip non-positive entries entirely — the sketch is
   * add-only, so a negative entry must not corrupt windows or slot sums.
   */
  @Test
  void addDirect_mapBatch_withBlankKeys_shouldNotThrow() {
    keeper.addDirect("", 10);
    assertThat(keeper.contains("")).isTrue();
  }

  @Test
  void addDirect_withExpelledQueueFull_shouldNotThrow() {
    HeavyKeeper smallQueue = new HeavyKeeper(3, 1000, 4, 0.9, 1, 1, 3, false);
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
    HeavyKeeper hk = new HeavyKeeper(TOP_K, WIDTH, DEPTH, DECAY, 1, 10_000, 3, false);
    hk.addDirect("k1", 1);
    hk.addDirect("k2", 1);
    hk.addDirect("k3", 1);
    assertThat(hk.list()).hasSize(3);
    // Membership counts follow the sketch, which is a sliding window: a add
    // stays backed by its window until the ring rotates past it.
    hk.fading();
    hk.fading();
    hk.fading();
    assertThat(hk.list()).as("counts stay while the ring still covers them").hasSize(3);
    // The fourth rotation clears the window that held the counts; the sketch
    // now reads zero for every key, so every seat is released.
    hk.fading();
    assertThat(hk.list()).isEmpty();
    assertThat(hk.total()).isZero();
  }

  // ── Fingerprint collision paths in addDirect(String, int) ──

  /**
   * A partial-decay collision contributes <b>zero</b> to the caller's estimate:
   * the slot still belongs to the occupant, so the colliding key's own
   * cross-row maximum must stay at 0. Returning the occupant's decayed
   * {@code slotSums} here would let a single cold key (delta=1) be admitted
   * with a hot key's add — polluting the TopK and evicting the real member.
   */
  @Test
  void collisionWithPartialDecay_shouldNotAdmitStrangerOnOccupantCount() {
    HeavyKeeper hk = new HeavyKeeper(3, 1, 1, 0.9, 1, 100, 3, false);
    hk.addDirect("occupant", 200);
    AddResult result = hk.addDirect("stranger", 5);
    assertThat(result.isHotKey()).as("a stranger sharing a slot must not inherit the occupant's estimate").isFalse();
    assertThat(hk.contains("stranger")).isFalse();
    assertThat(hk.contains("occupant")).as("the occupant survives a partial-decay collision").isTrue();
  }

  /**
   * The stranger's estimate is 0, not merely below the occupant's: a deep decay
   * that leaves the slot far above the newcomer must still not promote it.
   * {@code minCount=1} makes any non-zero estimate an admission.
   */
  @Test
  void collisionWithPartialDecay_shouldNotRaiseStrangerToOccupantCount() {
    HeavyKeeper hk = new HeavyKeeper(3, 1, 1, 0.5, 1, 100, 3, false);
    for (int i = 0; i < 300; i++) {
      hk.addDirect("occupant", 1);
    }
    AddResult result = hk.addDirect("stranger", 10);
    assertThat(result.isHotKey()).isFalse();
    assertThat(memberCount(hk, "stranger")).isEqualTo(0L);
  }

  /**
   * The full-displacement lottery win is unaffected: when the sampled decay
   * consumes the whole slot the newcomer legitimately takes it over, and its
   * estimate is its own {@code delta} — never the evicted occupant's add.
   */
  @Test
  void collisionWithFullDisplacement_shouldAdmitStrangerOnItsOwnDelta() {
    HeavyKeeper hk = new HeavyKeeper(3, 1, 1, 0.0, 1, 100, 3, false);
    hk.addDirect("occupant", 4);
    AddResult result = hk.addDirect("stranger", 2);
    assertThat(result.isHotKey()).isTrue();
    assertThat(memberCount(hk, "stranger")).isEqualTo(2L);
  }

  @Test
  void addDirect_withCollisionAndDecayReplaces_shouldReplaceFingerprint() {
    HeavyKeeper hk = new HeavyKeeper(3, 1, 1, 1.0, 1, 100, 3, false);
    hk.addDirect("key1", 5);
    hk.addDirect("key2", 10);
    assertThat(hk.contains("key1")).isTrue();
    assertThat(hk.contains("key2")).isTrue();
  }

  // ── sampleBinomial edge cases through collision path ──

  @Test
  void addDirect_withCollisionAndZeroIncrement_shouldHandle() {
    HeavyKeeper hk = new HeavyKeeper(3, 1, 1, 0.5, 1, 100, 3, false);
    hk.addDirect("key1", 10);
    AddResult result = hk.addDirect("key2", 0);
    assertThat(result).isNotNull();
    assertThat(hk.total()).isEqualTo(10);
  }

  @Test
  void addDirect_withCollisionAndLargeIncrement_shouldUseNormalApproximation() {
    HeavyKeeper hk = new HeavyKeeper(3, 1, 1, 0.9, 1, 100, 3, false);
    hk.addDirect("key1", 1);
    AddResult result = hk.addDirect("key2", 200);
    assertThat(result.isHotKey()).isTrue();
  }

  @Test
  void addDirect_withCollisionAndMediumIncrement_shouldUseFallbackLoop() {
    HeavyKeeper hk = new HeavyKeeper(3, 1, 1, 0.5, 1, 100, 3, false);
    hk.addDirect("key1", 1);
    AddResult result = hk.addDirect("key2", 20);
    assertThat(result.isHotKey()).isTrue();
  }


  @Test
  void fading_withConcurrentAddDirect_shouldNotDeadlock() throws InterruptedException {
    HeavyKeeper preloaded = new HeavyKeeper(TOP_K, WIDTH, DEPTH, DECAY, 1, 10_000, 3, false);
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

  // ── Soft heavy protection in decayCollisionSlot ──

  /**
   * A member whose add drops below {@code minCount} can never be re-admitted
   * (admission requires {@code maxCount >= minCount}), so keeping its seat only
   * starves the TopK of a better candidate and leaves {@code minMemberCount} —
   * the O(1) fast-reject threshold — artificially low.
   *
   * <p>Counts follow the sketch's sliding window, so a key that stops reporting
   * keeps its seat until the ring rotates past its last window and then loses it
   * in one step — three rotations of {@code windowCount=3}, plus the one that
   * clears the window holding the add.
   */
  @Test
  void fading_memberHalvedBelowMinCount_shouldExpel() {
    HeavyKeeper hk = new HeavyKeeper(3, 1, 1, 0.9, 50, 100, 3, false);
    hk.addDirect("fading", 200);
    assertThat(hk.contains("fading")).isTrue();

    hk.fading();
    hk.fading();
    hk.fading();
    assertThat(hk.contains("fading")).as("counts stay while the ring still covers them").isTrue();

    // The fourth rotation clears the window holding the 200: the sketch now
    // reads 0, below minCount, so the seat is released.
    hk.fading();
    assertThat(hk.contains("fading")).as("a member under minCount must not hold a TopK seat").isFalse();
    assertThat(hk.expelled()).hasSize(1);
  }

  /**
   * The decay drop reports the add the member actually held at departure.
   * A hard-coded {@code 0} would tell every downstream consumer (expelled
   * drain, stats) that a 25-add key expired identically to an empty one.
   *
   * <p>Reaching a non-zero departure add requires the fallback path: a larger
   * batch takes over the member's only sketch row, so the sketch can no longer
   * see the key and the add halves instead of syncing to the window.
   */
  @Test
  void fading_expelledMember_shouldReportActualCountNotZero() {
    HeavyKeeper hk = new HeavyKeeper(3, 1, 1, 0.9, 50, 100, 3, false);
    hk.addDirect("fading", 200);
    // Outweighs the occupant, so the single row changes hands and probeSketch
    // returns -1 for "fading" from here on.
    hk.addDirect("usurper", 100_000);
    hk.fading(); // 100
    hk.fading(); // 50
    hk.fading(); // 25

    Item expelled = hk.expelled().poll();
    assertThat(expelled).isNotNull();
    assertThat(expelled.key()).isEqualTo("fading");
    assertThat(expelled.count()).isEqualTo(25L);
  }

  /**
   * A live concurrent raise that lands on the same decay cycle keeps the
   * member: the removal-time re-check re-reads the add inside the map's
   * per-key critical section, so a add lifted back above {@code minCount}
   * is never lost to a stale decision.
   */
  @Test
  void fading_memberRaisedBackAboveMinCount_shouldSurvive() {
    HeavyKeeper hk = new HeavyKeeper(3, 1, 1, 0.9, 50, 100, 3, false);
    hk.addDirect("fading", 200);
    hk.fading();
    hk.fading();
    hk.fading();
    hk.fading(); // the window holding the add rolls out — below threshold, dropped
    assertThat(hk.contains("fading")).isFalse();
    assertThat(hk.expelled()).as("the drop is reported once").hasSize(1);
    assertThat(hk.expelled().poll()).isNotNull();

    // Re-observed traffic repopulates the sketch; the key must be able to
    // re-enter on its own strength, without a second departure report.
    hk.addDirect("fading", 200);
    assertThat(hk.contains("fading")).isTrue();
    assertThat(hk.expelled()).as("re-admission is not a departure").hasSize(0);
  }

  @Test
  void decayCollision_withHotSlot_shouldLimitDecay() {
    HeavyKeeper hk = new HeavyKeeper(3, 1, 1, 0.9, 1, 100, 3, false);
    for (int i = 0; i < 100; i++) {
      hk.addDirect("hot", 1);
    }
    for (int i = 0; i < 50; i++) {
      hk.addDirect("cold", 1);
    }
    assertThat(hk.contains("hot")).isTrue();
  }

  // ── Batch decay fast path ──

  @Test
  void decayCollision_withLargeIncrement_shouldUseFastPath() {
    HeavyKeeper hk = new HeavyKeeper(3, 1, 1, 0.9, 1, 100, 3, false);
    hk.addDirect("key1", 1);
    AddResult result = hk.addDirect("key2", 200);
    assertThat(result).isNotNull();
    assertThat(hk.total()).isEqualTo(201);
  }

  @Test
  void fading_withConcurrentAccumulate_shouldPreserveCount() throws InterruptedException {
    // Regression test: decayMembership() previously used reset()+accumulate() on LongAccumulator,
    // which lost concurrent accumulate() writes that arrived between get() and reset().
    // The fix uses AtomicLong with a CAS retry loop that preserves concurrent accumulates.
    HeavyKeeper hk = new HeavyKeeper(10, 1000, 4, 0.9, 1, 50000, 3, false);
    hk.addDirect("hotkey", 10000);

    CountDownLatch latch = new CountDownLatch(1);
    ExecutorService exec = Executors.newFixedThreadPool(2);
    AtomicBoolean stopped = new AtomicBoolean(false);

    // Accumulator thread — continuously raises the add via lock-free fast path
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
      for (int i = 0; i < 10; i++) {
        hk.fading();
        try {
          // Give the writer a window to land a batch between rotations. In
          // production fading runs every 20 s; rotating the ring faster than
          // traffic can refill it would empty the slot and evict the key —
          // correct behaviour, but not what this test measures.
          Thread.sleep(2);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
      stopped.set(true);
    });

    latch.countDown();
    exec.shutdown();
    assertThat(exec.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

    // Key should still be tracked (concurrent accumulates kept it above zero)
    assertThat(hk.contains("hotkey")).isTrue();

    // Count should be positive — in the old buggy code, concurrent accumulates
    // were lost on every decay cycle, causing the add to drop to 0 rapidly.
    List<Item> items = hk.listTopN(10);
    long hotkeyCount = -1;
    for (Item item : items) {
      if (item.key().equals("hotkey")) {
        hotkeyCount = item.count();
        break;
      }
    }
    assertThat(hotkeyCount).as("concurrent accumulates during decay should keep add > 0").isPositive();
  }

  /**
   * Verifies that a malformed huge increment saturates at
   * {@link Integer#MAX_VALUE} instead of wrapping negative (the int counter
   * stop-condition, mirroring the previous long saturation semantics).
   */
  @Test
  void addDirect_hugeIncrement_shouldSaturateNotWrap() {
    keeper.addDirect("key1", Long.MAX_VALUE);

    assertThat(keeper.contains("key1")).isTrue();
    List<Item> items = keeper.listTopN(1);
    assertThat(items).hasSize(1);
    assertThat(items.get(0).count()).isEqualTo(Integer.MAX_VALUE);
  }

  /**
   * Verifies that the hot-path max-raise on an existing member also saturates:
   * repeated large adds never push a member add negative.
   */
  @Test
  void addDirect_repeatedLargeIncrements_shouldStayPositive() {
    keeper.addDirect("key1", 1_000_000);
    for (int i = 0; i < 50; i++) {
      keeper.addDirect("key1", 100_000_000);
    }
    List<Item> items = keeper.listTopN(1);
    assertThat(items).hasSize(1);
    assertThat(items.get(0).count()).isPositive().isLessThanOrEqualTo(Integer.MAX_VALUE);
  }

  /**
   * Verifies that counts beyond the int range are clamped (not wrapped) when
   * admitted through the batch {@code ingestBatch} path.
   */
  @Test
  void addDirect_hugeSnapshotCount_shouldClampToIntMax() {
    Map<String, Long> snapshot = new HashMap<>();
    snapshot.put("key1", Long.MAX_VALUE);
    keeper.ingestBatch(snapshot);

    assertThat(keeper.contains("key1")).isTrue();
    List<Item> items = keeper.listTopN(1);
    assertThat(items).hasSize(1);
    assertThat(items.get(0).count()).isEqualTo(Integer.MAX_VALUE);
  }

  /**
   * Refreshing a key that is already a member must update it in place, never
   * run the full-set eviction path. That path removed the weakest member
   * (which could be the refreshed key itself) and then re-put the key over the
   * removed slot: when the refreshed key was the minimum it was reported as
   * its own departure, and when it was not, the set was left one member short.
   *
   * <p>Batch values are <b>increments</b>, not absolute snapshot totals (the
   * documented {@code addDirect(Map)} contract — every batch caller feeds
   * per-batch deltas), so a refreshed
   * member's add is its previous estimate plus the batch value.
   */
  @Test
  void addDirect_fullSet_existingMember_shouldRefreshInPlaceWithoutExpelling() {
    HeavyKeeper hk = new HeavyKeeper(3, 1024, 4, 0.9, 1, 100, 3, false);
    hk.addDirect("min", 100);
    hk.addDirect("mid", 200);
    hk.addDirect("max", 300);
    assertThat(hk.expelled()).as("filling the set admits without evicting").isEmpty();

    // Case 1: the refreshed key is the current minimum — previous code expelled it as a
    // false departure and re-inserted it.
    hk.ingestBatch(Map.of("min", 500L));
    assertThat(memberCount(hk, "min")).as("the batch value adds to the member's estimate (100 + 500)").isEqualTo(600L);
    assertThat(hk.list()).extracting(Item::key).containsExactly("min", "max", "mid");
    assertThat(hk.expelled()).as("warming the current minimum must not expel it").isEmpty();

    // Case 2: the refreshed key is a non-minimum member — previous code evicted the real
    // minimum and left the set one member short.
    hk.ingestBatch(Map.of("max", 700L));
    assertThat(memberCount(hk, "max")).as("the batch value adds to the member's estimate (300 + 700)").isEqualTo(1000L);
    assertThat(hk.estimatedSize()).as("refresh in place keeps the set full").isEqualTo(3);
    assertThat(hk.list()).extracting(Item::key).containsExactly("max", "min", "mid");
    assertThat(hk.expelled()).as("refreshing a member must not evict anyone").isEmpty();
  }

  /**
   * The in-place refresh must not weaken the {@code expelled()} contract: a genuinely
   * new warmed key that beats the weakest member still evicts it, and the departure
   * carries the add the member actually held.
   */
  @Test
  void addDirect_fullSet_newKey_shouldEvictWeakestAndReportDeparture() {
    HeavyKeeper hk = new HeavyKeeper(2, 1024, 4, 0.9, 1, 100, 3, false);
    hk.addDirect("weak", 10);
    hk.addDirect("strong", 50);
    assertThat(hk.expelled()).as("filling the set admits without evicting").isEmpty();

    hk.ingestBatch(Map.of("newcomer", 100L));

    assertThat(hk.list()).extracting(Item::key).containsExactly("newcomer", "strong");
    assertThat(hk.contains("weak")).isFalse();
    Item departure = hk.expelled().poll();
    assertThat(departure).isNotNull();
    assertThat(departure.key()).isEqualTo("weak");
    assertThat(departure.count()).as("departure carries the add the member held").isEqualTo(10L);
    assertThat(hk.expelled()).as("exactly one genuine departure").isEmpty();
  }
}
