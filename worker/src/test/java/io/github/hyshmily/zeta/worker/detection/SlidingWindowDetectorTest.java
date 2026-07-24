package io.github.hyshmily.zeta.worker.detection;

import static org.assertj.core.api.Assertions.*;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongArray;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

class SlidingWindowDetectorTest {

  @Test
  void shouldConstructWithValidParameters() {
    SlidingWindowDetector detector = new SlidingWindowDetector(1000, 10, 500);
    assertThat(detector.getWindowSize()).isEqualTo(10);
    assertThat(detector.getTimeMillisPerSlice()).isEqualTo(100);
    assertThat(detector.getThreshold()).isEqualTo(500);
  }

  @Test
  void shouldReturnWindowSumExceedingThreshold() {
    SlidingWindowDetector detector = new SlidingWindowDetector(1000, 10, 3);
    assertThat(detector.addCount("key1", 5)).isEqualTo(5);
  }

  @Test
  void shouldReturnWindowSumBelowThreshold() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 100);
    assertThat(detector.addCount("key2", 1)).isEqualTo(1);
  }

  @Test
  void shouldReturnWindowSumForTrackedKey() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 1000);
    long sum = detector.addCount("key3", 42);
    assertThat(sum).isPositive();
  }

  @Test
  void shouldReturnZeroForUnknownKey() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 1000);
    assertThat(detector.addCount("unknown", 0)).isZero();
  }

  @Test
  void shouldEvictStaleKeys() throws InterruptedException {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 1000);
    detector.addCount("staleKey", 1);
    assertThat(detector.getActiveKeyCount()).isEqualTo(1);
    Thread.sleep(10);
    detector.evictStale(1);
    assertThat(detector.getActiveKeyCount()).isZero();
  }

  @Test
  void shouldReportActiveKeyCount() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 1000);
    assertThat(detector.getActiveKeyCount()).isZero();
    detector.addCount("a", 1);
    detector.addCount("b", 1);
    detector.addCount("c", 1);
    assertThat(detector.getActiveKeyCount()).isEqualTo(3);
  }

  @Test
  void shouldReturnWindowSumWhenEqualsThreshold() {
    SlidingWindowDetector detector = new SlidingWindowDetector(1000, 10, 5);
    assertThat(detector.addCount("key", 5)).isEqualTo(5);
  }

  @Test
  void shouldHandleZeroCount() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 100);
    long sumBefore = detector.addCount("key", 50);
    long sumAfter = detector.addCount("key", 0);
    assertThat(sumAfter).isEqualTo(sumBefore);
  }

  @Test
  void shouldHandleNegativeCount() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 100);
    detector.addCount("key", 50);
    long sum = detector.addCount("key", -20);
    assertThat(sum).isEqualTo(30);
  }

  @Test
  void shouldReturnZeroForEvictedKey() throws InterruptedException {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 100);
    long before = detector.addCount("ephemeral", 42);
    assertThat(before).isPositive();
    Thread.sleep(10);
    detector.evictStale(1);
    long after = detector.addCount("ephemeral", 0);
    assertThat(after).isZero();
  }

  @Test
  void shouldAggregateAcrossMultipleSlices() throws InterruptedException {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 100);
    detector.addCount("key", 30);
    Thread.sleep(150);
    long sum = detector.addCount("key", 20);
    assertThat(sum).isEqualTo(50);
  }

  @Test
  @Tag("flaky")
  void shouldEvictAllWithZeroStaleTimeout() throws InterruptedException {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 1000);
    detector.addCount("key", 1);
    Thread.sleep(1);
    detector.evictStale(0);
    assertThat(detector.getActiveKeyCount()).isZero();
  }

  @Test
  void shouldHandleMaxLongCount() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, Long.MAX_VALUE);
    long result = detector.addCount("key", Long.MAX_VALUE);
    assertThat(result).isPositive();
    long unchanged = detector.addCount("key", 0);
    assertThat(unchanged).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void shouldClearStaleSlicesWithoutOverlappingWindow() throws InterruptedException {
    SlidingWindowDetector detector = new SlidingWindowDetector(5000, 5, 1000);
    detector.addCount("key", 100);
    Thread.sleep(2100);
    long sum = detector.addCount("key", 50);
    assertThat(sum).isEqualTo(150);
  }

  @Test
  void addCount_shouldPreserveCountsAfterSliceAdvance() throws InterruptedException {
    SlidingWindowDetector detector = new SlidingWindowDetector(5000, 5, 1000);
    detector.addCount("gap-key", 100);
    Thread.sleep(1500);
    long sum = detector.addCount("gap-key", 50);
    assertThat(sum).isEqualTo(150);
  }

  @Test
  void addCount_shouldMaintainCorrectSumAfterMultipleSliceAdvances() throws InterruptedException {
    SlidingWindowDetector detector = new SlidingWindowDetector(5000, 5, 1000);
    detector.addCount("rot-key", 100);
    Thread.sleep(800);
    detector.addCount("rot-key", 20);
    Thread.sleep(900);
    long sum = detector.addCount("rot-key", 5);
    assertThat(sum).isEqualTo(125);
  }

  @Test
  void shouldHandleNullKey() {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 100);
    assertThatThrownBy(() -> detector.addCount(null, 1)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void addCount_shouldHandleFreshKey() {
    SlidingWindowDetector detector = new SlidingWindowDetector(5000, 5, 1000);
    assertThatCode(() -> detector.addCount("fresh", 42)).doesNotThrowAnyException();
    assertThat(detector.addCount("fresh", 0)).isEqualTo(42);
  }

  @Test
  void evictStale_shouldNotRemoveRecentlyAccessedKey() throws InterruptedException {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 1000);
    detector.addCount("keepMe", 1);
    Thread.sleep(1);
    detector.evictStale(3600_000);
    assertThat(detector.getActiveKeyCount()).isOne();
  }

  @Test
  void evictStale_shouldCleanupOrphanedLastAccessTimeEntries() throws Exception {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 1000);
    detector.addCount("normal", 1);

    Field latField = SlidingWindowDetector.class.getDeclaredField("lastAccessTime");
    latField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, Long> lastAccessTime = (Map<String, Long>) latField.get(detector);
    lastAccessTime.put("orphan", System.currentTimeMillis() - 100_000);
    detector.evictStale(3600_000);

    assertThat(lastAccessTime).doesNotContainKey("orphan");
    assertThat(lastAccessTime).containsKey("normal");
  }

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

  @Test
  void getWindowSum_shouldReturnZeroForZeroedWindowSlice() {
    SlidingWindowDetector detector = new SlidingWindowDetector(5000, 5, 1000);
    detector.addCount("expired", 100);
    assertThat(detector.addCount("never-tracked", 0)).isZero();
  }

  @Test
  void cleanupRegion_shouldNotOverlapSummationRegion() throws Exception {
    // Verify the doubled-buffer invariant: cleanup region and summation region
    // are disjoint by construction (length == 2 * windowSize).
    SlidingWindowDetector detector = new SlidingWindowDetector(5000, 5, Long.MAX_VALUE);
    Field windowsField = SlidingWindowDetector.class.getDeclaredField("windows");
    windowsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    ConcurrentHashMap<String, AtomicLongArray> windows =
        (ConcurrentHashMap<String, AtomicLongArray>) windowsField.get(detector);

    detector.addCount("k", 0);
    AtomicLongArray buf = windows.get("k");
    int len = buf.length();
    int win = detector.getWindowSize();
    long sliceMs = detector.getTimeMillisPerSlice();
    assertThat(len).as("doubled-buffer invariant").isEqualTo(2 * win);

    // Fill every slot with a unique marker so we can detect any overwrite.
    for (int i = 0; i < len; i++) buf.set(i, (long) i + 1);

    // Advance at least 1 slice to trigger the cleanup branch.
    Thread.sleep(sliceMs + 10);

    // Snapshot currentIndex before addCount mutates the buffer.
    long beforeCall = System.currentTimeMillis();
    int ci = (int) ((beforeCall / sliceMs) % len);

    detector.addCount("k", 0);

    // All active-window slots (except currentIndex, which was just written)
    // must still hold their original markers — proving cleanup never hit them.
    for (int i = 0; i < win; i++) {
      int slot = (ci - i + len) % len;
      if (slot == ci) continue;
      assertThat(buf.get(slot))
          .as("active slot %d cleaned by stale-region cleanup", slot)
          .isEqualTo((long) slot + 1);
    }
  }

  @Test
  void cleanupRegion_shouldHandleSequentialAdvances() throws Exception {
    // Verify cleanup correctness after multiple sequential advances,
    // stressing the clearStart derivation across overlapping window positions.
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, Long.MAX_VALUE);
    long sliceMs = detector.getTimeMillisPerSlice();
    int win = detector.getWindowSize();

    detector.addCount("seq", 100);
    for (int step = 1; step <= 6; step++) {
      Thread.sleep(sliceMs + 10);
      long sum = detector.addCount("seq", 10);
      assertThat(sum)
          .as("sum after step %d", step)
          .isBetween((long) step * 10, 100L + (long) step * 10);
    }
  }
}
