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
package io.github.hyshmily.hotkey.worker.detection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLongArray;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link SlidingWindowDetector}.
 */
class SlidingWindowDetectorTest {

  /**
   * Verifies that the {@link SlidingWindowDetector} is constructed with the expected parameter values.
   */
  @Test
  void shouldConstructWithValidParameters() {
    SlidingWindowDetector detector = new SlidingWindowDetector(1000, 10, 500);
    assertThat(detector.getWindowSize()).isEqualTo(10);
    assertThat(detector.getTimeMillisPerSlice()).isEqualTo(100);
    assertThat(detector.getThreshold()).isEqualTo(500);
  }

  /**
   * Verifies that {@code addCount} returns {@code true} when the window sum exceeds the threshold.
   */
  @Test
  void shouldReturnTrueWhenWindowSumExceedsThreshold() {
    SlidingWindowDetector detector = new SlidingWindowDetector(1000, 10, 3);
    assertThat(detector.addCount("key1", 5)).isTrue();
  }

  /**
   * Verifies that {@code addCount} returns {@code false} when the window sum is below the threshold.
   */
  @Test
  void shouldReturnFalseWhenWindowSumBelowThreshold() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 100);
    assertThat(detector.addCount("key2", 1)).isFalse();
  }

  /**
   * Verifies that {@code getWindowSum} returns a positive value for a key that has been tracked.
   */
  @Test
  void shouldReturnWindowSumForTrackedKey() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 1000);
    detector.addCount("key3", 42);
    assertThat(detector.getWindowSum("key3")).isPositive();
  }

  /**
   * Verifies that {@code getWindowSum} returns zero for a key that has never been tracked.
   */
  @Test
  void shouldReturnZeroForUnknownKey() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 1000);
    assertThat(detector.getWindowSum("unknown")).isZero();
  }

  /**
   * Verifies that stale keys are evicted after calling {@code evictStale}.
   */
  @Test
  void shouldEvictStaleKeys() throws InterruptedException {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 1000);
    detector.addCount("staleKey", 1);
    assertThat(detector.getActiveKeyCount()).isEqualTo(1);
    Thread.sleep(10);
    detector.evictStale(1);
    assertThat(detector.getActiveKeyCount()).isZero();
  }

  /**
   * Verifies that the active key count changes as keys are added to the detector.
   */
  @Test
  void shouldReportActiveKeyCount() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 1000);
    assertThat(detector.getActiveKeyCount()).isZero();
    detector.addCount("a", 1);
    detector.addCount("b", 1);
    detector.addCount("c", 1);
    assertThat(detector.getActiveKeyCount()).isEqualTo(3);
  }

  /**
   * Verifies that addCount returns true when the window sum exactly equals the threshold.
   * Boundary: {@code >= threshold} includes equality.
   */
  @Test
  void shouldReturnTrueWhenWindowSumEqualsThreshold() {
    SlidingWindowDetector detector = new SlidingWindowDetector(1000, 10, 5);
    assertThat(detector.addCount("key", 5)).isTrue();
  }

  /**
   * Verifies that adding zero count does not change the hot verdict for a tracked key,
   * and the window sum remains unchanged.
   */
  @Test
  void shouldHandleZeroCount() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 100);
    detector.addCount("key", 50);
    long sumBefore = detector.getWindowSum("key");
    assertThat(detector.addCount("key", 0)).isFalse();
    assertThat(detector.getWindowSum("key")).isEqualTo(sumBefore);
  }

  /**
   * Verifies that adding a negative count does not cause failures and decreases the window sum.
   */
  @Test
  void shouldHandleNegativeCount() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 100);
    detector.addCount("key", 50);
    assertThat(detector.addCount("key", -20)).isFalse();
    assertThat(detector.getWindowSum("key")).isEqualTo(30);
  }

  /**
   * Verifies that {@code getWindowSum} returns zero for a key that has been evicted.
   */
  @Test
  void shouldReturnZeroForEvictedKey() throws InterruptedException {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 100);
    detector.addCount("ephemeral", 42);
    assertThat(detector.getWindowSum("ephemeral")).isPositive();
    Thread.sleep(10);
    detector.evictStale(1);
    assertThat(detector.getWindowSum("ephemeral")).isZero();
  }

  /**
   * Verifies that getWindowSum correctly aggregates across multiple time slices when
   * time advances enough to rotate into a new slice.
   */
  @Test
  void shouldAggregateAcrossMultipleSlices() throws InterruptedException {
    // 10 second window, 1000ms per slice — long slices so rotation doesn't lose data
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 100);
    detector.addCount("key", 30);
    Thread.sleep(150); // advance past at least one slice boundary
    detector.addCount("key", 20);
    // the sum should include both additions (within the window)
    assertThat(detector.getWindowSum("key")).isEqualTo(50);
  }

  /**
   * Verifies that evictStale with staleAfterMs = 0 eventually evicts all keys
   * that were accessed in the past (boundary: zero stale timeout).
   */
  @Test
  void shouldEvictAllWithZeroStaleTimeout() throws InterruptedException {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 1000);
    detector.addCount("key", 1);
    Thread.sleep(1);
    detector.evictStale(0);
    assertThat(detector.getActiveKeyCount()).isZero();
  }

  /**
   * Verifies that getWindowSum handles Long.MAX_VALUE without overflow.
   */
  @Test
  void shouldHandleMaxLongCount() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, Long.MAX_VALUE);
    boolean firstResult = detector.addCount("key", Long.MAX_VALUE);
    assertThat(firstResult).isTrue(); // Long.MAX_VALUE >= Long.MAX_VALUE (threshold)
    assertThat(detector.getWindowSum("key")).isPositive();
    // adding zero should not change sum
    detector.addCount("key", 0);
    assertThat(detector.getWindowSum("key")).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Exhaustively verifies that {@code clearStaleSlices} does not clear any slot
   * still within the current sliding window, for every possible {@code currentIndex}
   * in the circular buffer.
   *
   * <p>A previous implementation iterated <em>forward</em> from
   * {@code currentIndex + windowSize}, which incorrectly cleared up to
   * {@code windowSize - 1} slots still inside the window.
   */
  @Test
  void clearStaleSlices_shouldNotOverlapWithWindowAtEveryIndex() throws Exception {
    SlidingWindowDetector detector = new SlidingWindowDetector(5000, 5, 1000);

    Method clearMethod = SlidingWindowDetector.class.getDeclaredMethod(
      "clearStaleSlices",
      AtomicLongArray.class,
      int.class
    );
    clearMethod.setAccessible(true);

    Method sumMethod = SlidingWindowDetector.class.getDeclaredMethod("getWindowSum", AtomicLongArray.class, int.class);
    sumMethod.setAccessible(true);

    int arrayLen = 10; // 2 * windowSize
    for (int ci = 0; ci < arrayLen; ci++) {
      AtomicLongArray slices = new AtomicLongArray(arrayLen);
      for (int i = 0; i < arrayLen; i++) {
        slices.set(i, i * 10L);
      }

      clearMethod.invoke(detector, slices, ci);
      long sum = (long) sumMethod.invoke(detector, slices, ci);

      // Window = {ci, ci-1, ci-2, ci-3, ci-4} modulo arrayLen
      long expectedSum = 0;
      for (int i = 0; i < 5; i++) {
        int idx = (ci - i + arrayLen) % arrayLen;
        expectedSum += idx * 10L;
      }

      assertThat(sum).as("currentIndex=" + ci).isEqualTo(expectedSum);
    }
  }

  /**
   * Verifies that counts survive when time advances past one or more slice
   * boundaries (non‑contiguous access pattern). The old slice must remain
   * within the window and must not be cleared.
   */
  @Test
  void addCount_shouldPreserveCountsAfterSliceAdvance() throws InterruptedException {
    SlidingWindowDetector detector = new SlidingWindowDetector(5000, 5, 1000);
    detector.addCount("gap-key", 100);
    Thread.sleep(1500); // advance at least 1 slice, still well within 5000ms window
    detector.addCount("gap-key", 50);
    long sum = detector.getWindowSum("gap-key");
    assertThat(sum).isEqualTo(150);
  }

  /**
   * Verifies that after multiple non‑contiguous accesses spanning several
   * slices, the window sum includes exactly the still‑active slices and
   * nothing more.
   */
  @Test
  void addCount_shouldMaintainCorrectSumAfterMultipleSliceAdvances() throws InterruptedException {
    SlidingWindowDetector detector = new SlidingWindowDetector(5000, 5, 1000);
    detector.addCount("rot-key", 100);
    Thread.sleep(800);
    detector.addCount("rot-key", 20);
    Thread.sleep(900);
    detector.addCount("rot-key", 5);
    long sum = detector.getWindowSum("rot-key");
    assertThat(sum).isEqualTo(125);
  }

  /**
   * Verifies that {@code addCount} throws {@link NullPointerException} when the key is null,
   * as specified by the method contract.
   */
  @Test
  void shouldHandleNullKey() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 100);
    assertThatThrownBy(() -> detector.addCount(null, 1)).isInstanceOf(NullPointerException.class);
  }

  /**
   * Verifies that {@code clearStaleSlices} handles a fresh (all-zero) array
   * without throwing any exception.  With {@link AtomicLongArray} there are
   * no null entries — all elements are pre-initialised to zero.
   */
  @Test
  void clearStaleSlices_shouldHandleAllZeroedSlices() throws Exception {
    SlidingWindowDetector detector = new SlidingWindowDetector(5000, 5, 1000);
    Method clearMethod = SlidingWindowDetector.class.getDeclaredMethod(
      "clearStaleSlices",
      AtomicLongArray.class,
      int.class
    );
    clearMethod.setAccessible(true);

    AtomicLongArray slices = new AtomicLongArray(10); // all entries are 0
    clearMethod.invoke(detector, slices, 3);
    // No exception expected
  }

  /**
   * Verifies that a recently accessed key survives eviction by {@code evictStale}
   * when its last access time is well within the stale timeout.
   */
  @Test
  void evictStale_shouldNotRemoveRecentlyAccessedKey() throws InterruptedException {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 1000);
    detector.addCount("keepMe", 1);
    Thread.sleep(1);
    detector.evictStale(3600_000); // 1 hour stale timeout — key is far newer
    assertThat(detector.getActiveKeyCount()).isOne();
  }

  /**
   * Verifies that {@code evictStale} cleans up orphaned entries in
   * {@code lastAccessTime} whose corresponding window array has been removed
   * (line 215 of the source).  This path is normally exercised by the
   * race-condition guard inside {@code windows.compute()}, but we simulate it
   * directly by injecting a dangling timestamp via reflection.
   *
   * <p>A normal key is also added via {@code addCount} to exercise the
   * {@code false} branch of the {@code removeIf} predicate (keys that are
   * in both {@code lastAccessTime} and {@code windows} must not be removed).
   */
  @Test
  void evictStale_shouldCleanupOrphanedLastAccessTimeEntries() throws Exception {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 1000);

    // Add a normal key that IS in windows — exercises the false branch of removeIf.
    detector.addCount("normal", 1);

    Field latField = SlidingWindowDetector.class.getDeclaredField("lastAccessTime");
    latField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Long> lastAccessTime = (Map<String, Long>) latField.get(detector);

    // Inject a key that exists only in lastAccessTime, not in windows.
    lastAccessTime.put("orphan", System.currentTimeMillis() - 100_000);

    // Call evictStale with a stale timeout large enough that "orphan" is NOT a
    // candidate for the normal eviction path.  Line 215 will still clean it up.
    detector.evictStale(3600_000);

    assertThat(lastAccessTime).doesNotContainKey("orphan");
    assertThat(lastAccessTime).containsKey("normal");
  }

  /**
   * Verifies that the threshold can be updated at runtime and read back correctly.
   */
  @Test
  void setThreshold_shouldUpdateAndGetThreshold() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 100);
    assertThat(detector.getThreshold()).isEqualTo(100);
    detector.setThreshold(500);
    assertThat(detector.getThreshold()).isEqualTo(500);
    detector.setThreshold(0);
    assertThat(detector.getThreshold()).isZero();
    detector.setThreshold(Long.MAX_VALUE);
    assertThat(detector.getThreshold()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Verifies that {@code getWindowSum} treats zero-valued slice entries in the
   * circular buffer as zero when computing the window sum.
   */
  @Test
  void getWindowSum_shouldReturnZeroForZeroedSliceEntry() throws Exception {
    SlidingWindowDetector detector = new SlidingWindowDetector(5000, 5, 1000);
    Method sumMethod = SlidingWindowDetector.class.getDeclaredMethod("getWindowSum", AtomicLongArray.class, int.class);
    sumMethod.setAccessible(true);

    int arrayLen = 10; // 2 * windowSize
    AtomicLongArray slices = new AtomicLongArray(arrayLen); // all zero by default
    slices.set(0, 10);
    slices.set(1, 20);

    // currentIndex = 9, window covers 9,8,7,6,5 — all zero
    long sumForZeroRange = (long) sumMethod.invoke(detector, slices, 9);
    assertThat(sumForZeroRange).isZero();

    // currentIndex = 1, window covers 1,0,9,8,7 — indices 1 and 0 are set, rest are zero
    long sumWithPartialValues = (long) sumMethod.invoke(detector, slices, 1);
    assertThat(sumWithPartialValues).isEqualTo(30);
  }
}
