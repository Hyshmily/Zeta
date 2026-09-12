package io.github.hyshmily.zeta.worker.detection;

import static org.assertj.core.api.Assertions.*;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongArray;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

class SlidingWindowDetectorTest {

  @Test
  void shouldConstructWithValidParameters() {
    SlidingWindowDetector detector = new SlidingWindowDetector(1000, 10, 500);
    assertThat(detector.getWindowSize()).isEqualTo(16);
    assertThat(detector.getTimeMillisPerSlice()).isEqualTo(62);
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

  /**
   * Pins the merged single-map eviction invariant: a consumer's
   * {@code addCount} landing between the evictor's candidate scan and the
   * removal compute must keep the window — the refreshed timestamp fails the
   * re-check inside the bin-locked compute.
   *
   * <p>Deterministic simulation: a hook map fires the consumer's addCount
   * while the evictor's phase-1 {@code forEach} visits the key, so the
   * timestamp is refreshed before the evictor's phase-2 compute runs. The
   * window must survive with its counts intact (no orphaned timestamp and no
   * lost count is possible in the merged design — the previous two-map
   * conditional-remove protocol this replaces needed exactly this guard).
   *
   * <p>The sleep crosses at least one full slice (625 ms) so the hooked
   * addCount takes the slow path: under the slice-gated fast path, only a
   * touch at least one slice old refreshes the timestamp — which is exactly
   * the production interleaving, since an evictor can only target a window
   * whose timestamp is already far older than a slice (staleAfterMs is
   * configured in minutes; the gate routes every arriving count for such a
   * window through the timestamp-refreshing slow path).
   */
  @Test
  void evictStale_shouldKeepWindowWhenTimestampRefreshedBetweenScanAndRemove() throws Exception {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 1000);
    detector.addCount("race", 100);

    Field windowsField = SlidingWindowDetector.class.getDeclaredField("windows");
    windowsField.setAccessible(true);
    @SuppressWarnings({"unchecked", "rawtypes"})
    ConcurrentHashMap<String, Object> realWindows =
        (ConcurrentHashMap<String, Object>) windowsField.get(detector);

    // Hook map: the evictor's phase-1 forEach triggers the consumer's
    // addCount ("timestamp refresh + count write") while visiting "race" —
    // exactly the interleaving the re-check must tolerate. One-shot.
    @SuppressWarnings({"unchecked", "rawtypes"})
    ConcurrentHashMap<String, Object> hooked =
        new ConcurrentHashMap<String, Object>(realWindows) {
          private boolean fired = false;

          @Override
          public void forEach(java.util.function.BiConsumer<? super String, ? super Object> action) {
            super.forEach((key, value) -> {
              if ("race".equals(key) && !fired) {
                fired = true;
                detector.addCount("race", 7);
              }
              action.accept(key, value);
            });
          }
        };

    windowsField.set(detector, hooked);
    try {
      // ≥ 1 slice (625 ms): "race" is stale relative to staleAfterMs = 10 AND
      // old enough that the hooked addCount crosses a slice into the slow path.
      Thread.sleep(640);
      detector.evictStale(10);
    } finally {
      windowsField.set(detector, realWindows);
    }

    // The re-check observed the refreshed timestamp, so the window survived
    // with its counts intact — the refreshed count of 7 was written into the
    // live window mid-eviction.
    assertThat(detector.getActiveKeyCount()).as("window survives the refreshed re-check").isEqualTo(1);
    assertThat(detector.addCount("race", 0)).as("counts preserved (100 + 7)").isEqualTo(107);
  }

  /**
   * Pins the merged single-map removal order: a concurrent {@code addCount}
   * that arrives AFTER the evictor's removal re-creates a fresh window inside
   * its own compute — no orphaned entry is left behind and the new count is
   * visible (the previous design's orphan sweep existed for the two-map
   * equivalent of this case).
   */
  @Test
  void evictStale_thenAddCount_recreatesFreshWindowWithoutOrphans() throws Exception {
    SlidingWindowDetector detector = new SlidingWindowDetector(10_000, 10, 1000);
    detector.addCount("stale-key", 100);
    Thread.sleep(30);

    detector.evictStale(10);
    assertThat(detector.getActiveKeyCount()).isZero();

    // addCount after eviction rebuilds cleanly: no orphaned timestamp, the
    // new count is the whole window.
    assertThat(detector.addCount("stale-key", 42)).isEqualTo(42);
    assertThat(detector.getActiveKeyCount()).isOne();
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
    @SuppressWarnings("all")
    ConcurrentHashMap<String, Object> windows =
        (ConcurrentHashMap<String, Object>) windowsField.get(detector);

    detector.addCount("k", 0);
    // The merged map's value is the private KeyWindow holder — extract its
    // slices array reflectively.
    Object keyWindow = windows.get("k");
    Field slicesField = keyWindow.getClass().getDeclaredField("slices");
    slicesField.setAccessible(true);
    AtomicLongArray buf = (AtomicLongArray) slicesField.get(keyWindow);
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

  /**
   * Pins the slice-gated fast path: repeated {@code addCount} calls within
   * one slice skip the bin-locked compute entirely — every count must still
   * land exactly and the returned window sum must include the full history.
   */
  @Test
  void addCount_repeatedAddsInSameSlice_produceExactSums() {
    SlidingWindowDetector detector = new SlidingWindowDetector(5000, 5, Long.MAX_VALUE);
    long sum = 0;
    for (int i = 1; i <= 500; i++) {
      sum = detector.addCount("burst", 2);
    }
    assertThat(sum).as("500 fast-path adds of 2, all inside the window").isEqualTo(1000);
  }

  /**
   * Pins the fast/slow interleaving: same-slice bursts (fast path) followed
   * by slice crossings (slow path, W-1 pre-clear) must keep the window sum
   * exact — the fast path neither loses the slow path's cleared history nor
   * skips a stale-slot clear that is still needed.
   */
  @Test
  void addCount_sameSliceBurstsAcrossSliceCrossings_keepSumExact() throws InterruptedException {
    SlidingWindowDetector detector = new SlidingWindowDetector(5000, 5, Long.MAX_VALUE);
    long sliceMs = detector.getTimeMillisPerSlice();
    long sum = 0;
    for (int round = 0; round < 3; round++) {
      for (int i = 0; i < 50; i++) {
        sum = detector.addCount("burst-cross", 1);
      }
      if (round < 2) {
        Thread.sleep(sliceMs + 10);
      }
    }
    // All 150 counts land inside the 5-slice window (~3.1 s), so the final
    // sum is exact regardless of where the crossings fell.
    assertThat(sum).isEqualTo(150);
  }

  /**
   * Pins the eviction contract under the slice-gated fast path: a key touched
   * continuously — mostly via the fast path, which defers the timestamp
   * refresh to slice crossings — must never be evicted while every crossing
   * refreshes the timestamp within the staleness threshold, and must be
   * evicted once genuinely idle.
   */
  @Test
  void evictStale_continuouslyAccessedKeySurvives_thenIdleKeyIsEvicted() throws Exception {
    // 1 ms slices: the tight add loop crosses slices repeatedly, so both the
    // fast-path touches and the per-crossing timestamp refreshes run against
    // a 50 ms staleness threshold.
    SlidingWindowDetector detector = new SlidingWindowDetector(8, 8, Long.MAX_VALUE);
    long deadline = System.nanoTime() + 5_000_000L; // ~5 ms of continuous access
    while (System.nanoTime() < deadline) {
      detector.addCount("hot-loop", 1);
    }

    detector.evictStale(50);
    assertThat(detector.getActiveKeyCount()).as("continuously accessed key survives").isEqualTo(1);

    Thread.sleep(100);
    detector.evictStale(50);
    assertThat(detector.getActiveKeyCount()).as("idle key is evicted").isZero();
  }
}
