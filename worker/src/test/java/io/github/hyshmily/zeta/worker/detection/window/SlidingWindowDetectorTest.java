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
package io.github.hyshmily.zeta.worker.detection.window;

import static org.assertj.core.api.Assertions.*;

import io.github.hyshmily.zeta.util.TimeSource;
import java.lang.reflect.Field;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class SlidingWindowDetectorTest {

  private static SlidingWindowDetector detector(long windowMs, int slices, long maxTrackedKeys) {
    return new SlidingWindowDetector(WindowGeometry.of(windowMs, slices), maxTrackedKeys);
  }

  private static long addCount(SlidingWindowDetector detector, String key, long count) {
    return detector.addCountAtSlice(key, count, detector.sliceIndex(TimeSource.monotonicMillis()));
  }

  private static void recordCount(SlidingWindowDetector detector, String key, long count) {
    detector.recordCountAtSlice(key, count, detector.sliceIndex(TimeSource.monotonicMillis()));
  }

  @Test
  void geometryAlignsSliceCountAndSliceLength() {
    WindowGeometry geometry = WindowGeometry.of(1_000, 10);

    assertThat(geometry.alignedSlices()).isEqualTo(16);
    assertThat(geometry.sliceMs()).isEqualTo(62);
  }

  @Test
  void shouldReturnWindowSumExceedingThreshold() {
    SlidingWindowDetector detector = detector(1000, 10, 0);
    assertThat(addCount(detector, "key1", 5)).isEqualTo(5);
  }

  @Test
  void shouldReturnWindowSumBelowThreshold() {
    SlidingWindowDetector detector = detector(10_000, 10, 0);
    assertThat(addCount(detector, "key2", 1)).isEqualTo(1);
  }

  @Test
  void shouldReturnWindowSumForTrackedKey() {
    SlidingWindowDetector detector = detector(10_000, 10, 0);
    long sum = addCount(detector, "key3", 42);
    assertThat(sum).isPositive();
  }

  @Test
  void shouldReturnZeroForUnknownKey() {
    SlidingWindowDetector detector = detector(10_000, 10, 0);
    assertThat(addCount(detector, "unknown", 0)).isZero();
  }

  @Test
  void shouldEvictStaleKeys() {
    SlidingWindowDetector detector = detector(10_000, 10, 0);
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      addCount(detector, "staleKey", 1);
      assertThat(detector.activeWindows()).isEqualTo(1);
      // Retention is quantized to slices, so cross one boundary instead of sleeping 1 ms.
      TimeSource.setTimeOffsetForTest(0, -base + 1_000);
      detector.evictStale(1);
      assertThat(detector.activeWindows()).isZero();
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  @Test
  void shouldReportActiveKeyCount() {
    SlidingWindowDetector detector = detector(10_000, 10, 0);
    assertThat(detector.activeWindows()).isZero();
    addCount(detector, "a", 1);
    addCount(detector, "b", 1);
    addCount(detector, "c", 1);
    assertThat(detector.activeWindows()).isEqualTo(3);
  }

  @Test
  void shouldReturnWindowSumWhenEqualsThreshold() {
    SlidingWindowDetector detector = detector(1000, 10, 0);
    assertThat(addCount(detector, "key", 5)).isEqualTo(5);
  }

  @Test
  void shouldHandleZeroCount() {
    SlidingWindowDetector detector = detector(10_000, 10, 0);
    long sumBefore = addCount(detector, "key", 50);
    long sumAfter = addCount(detector, "key", 0);
    assertThat(sumAfter).isEqualTo(sumBefore);
  }

  @Test
  void shouldIgnoreNegativeCount() {
    SlidingWindowDetector detector = detector(10_000, 10, 0);
    addCount(detector, "key", 50);
    long sum = addCount(detector, "key", -20);
    assertThat(sum).as("a non-positive report cannot subtract from the window").isEqualTo(50);
  }

  @Test
  void shouldReturnZeroForEvictedKey() {
    SlidingWindowDetector detector = detector(10_000, 10, 0);
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      long before = addCount(detector, "ephemeral", 42);
      assertThat(before).isPositive();
      TimeSource.setTimeOffsetForTest(0, -base + 1_000);
      detector.evictStale(1);
      long after = addCount(detector, "ephemeral", 0);
      assertThat(after).isZero();
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  @Test
  void shouldAggregateAcrossMultipleSlices() throws InterruptedException {
    SlidingWindowDetector detector = detector(10_000, 10, 0);
    addCount(detector, "key", 30);
    Thread.sleep(150);
    long sum = addCount(detector, "key", 20);
    assertThat(sum).isEqualTo(50);
  }

  @Test
  void shouldEvictAllWithZeroStaleTimeout() {
    SlidingWindowDetector detector = detector(10_000, 10, 0);
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      addCount(detector, "key", 1);
      // A zero threshold evicts on the next slice boundary; the current slice is still live.
      TimeSource.setTimeOffsetForTest(0, -base + 1_000);
      detector.evictStale(0);
      assertThat(detector.activeWindows()).isZero();
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  @Test
  void shouldSaturateAtIntRange() {
    SlidingWindowDetector detector = detector(10_000, 10, 0);
    long result = addCount(detector, "key", Long.MAX_VALUE);
    assertThat(result).as("the per-slice counter saturates instead of wrapping").isEqualTo(Integer.MAX_VALUE);
    long unchanged = addCount(detector, "key", 0);
    assertThat(unchanged).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  void shouldExcludeTrafficOlderThanTheWindow() throws InterruptedException {
    SlidingWindowDetector detector = detector(5000, 5, 0);
    addCount(detector, "key", 100);
    Thread.sleep(2100);
    long sum = addCount(detector, "key", 50);
    assertThat(sum).isEqualTo(150);
  }

  @Test
  void addCount_shouldPreserveCountsAfterSliceAdvance() throws InterruptedException {
    SlidingWindowDetector detector = detector(5000, 5, 0);
    addCount(detector, "gap-key", 100);
    Thread.sleep(1500);
    long sum = addCount(detector, "gap-key", 50);
    assertThat(sum).isEqualTo(150);
  }

  @Test
  void addCount_shouldMaintainCorrectSumAfterMultipleSliceAdvances() throws InterruptedException {
    SlidingWindowDetector detector = detector(5000, 5, 0);
    addCount(detector, "rot-key", 100);
    Thread.sleep(800);
    addCount(detector, "rot-key", 20);
    Thread.sleep(900);
    long sum = addCount(detector, "rot-key", 5);
    assertThat(sum).isEqualTo(125);
  }

  @Test
  void shouldHandleNullKey() {
    SlidingWindowDetector detector = detector(10_000, 10, 0);
    assertThatThrownBy(() -> addCount(detector, null, 1)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void addCount_shouldHandleFreshKey() {
    SlidingWindowDetector detector = detector(5000, 5, 0);
    assertThatCode(() -> addCount(detector, "fresh", 42)).doesNotThrowAnyException();
    assertThat(addCount(detector, "fresh", 0)).isEqualTo(42);
  }

  @Test
  void evictStale_shouldNotRemoveRecentlyAccessedKey() throws InterruptedException {
    SlidingWindowDetector detector = detector(10_000, 10, 0);
    addCount(detector, "keepMe", 1);
    Thread.sleep(1);
    detector.evictStale(3600_000);
    assertThat(detector.activeWindows()).isOne();
  }

  /**
   * A consumer's {@code addCountAtSlice} landing between the evictor's candidate scan and the removal
   * compute keeps the window: the refreshed timestamp fails the re-check under the entry monitor.
   *
   * <p>The hook map fires the consumer's add while the evictor's phase-1 {@code forEach} visits the
   * key, so the timestamp is refreshed before the removal compute runs. The window must survive
   * with its counts intact.
   *
   * <p>The sleep crosses at least one full slice (625 ms) so the hooked add takes the slow
   * path: under the slice-gated fast path, only a touch at least one slice old refreshes the
   * timestamp. An evictor can only target a window whose timestamp is already far older than a
   * slice (staleAfterMs is configured in minutes).
   */
  @Test
  void evictStale_shouldKeepWindowWhenTimestampRefreshedBetweenScanAndRemove() throws Exception {
    SlidingWindowDetector detector = detector(10_000, 10, 0);
    addCount(detector, "race", 100);

    Field windowsField = SlidingWindowDetector.class.getDeclaredField("windows");
    windowsField.setAccessible(true);
    @SuppressWarnings({ "unchecked", "rawtypes" })
    ConcurrentHashMap<String, Object> realWindows = (ConcurrentHashMap<String, Object>) windowsField.get(detector);

    // Hook map: the evictor's phase-1 forEach triggers the consumer's
    // addCountAtSlice ("timestamp refresh + add write") while visiting "race" —
    // exactly the interleaving the re-check must tolerate. One-shot.
    @SuppressWarnings({ "unchecked", "rawtypes" })
    ConcurrentHashMap<String, Object> hooked = new ConcurrentHashMap<String, Object>(realWindows) {
      private boolean fired = false;

      @Override
      public void forEach(java.util.function.BiConsumer<? super String, ? super Object> action) {
        super.forEach((key, value) -> {
          if ("race".equals(key) && !fired) {
            fired = true;
            addCount(detector, "race", 7);
          }
          action.accept(key, value);
        });
      }
    };

    windowsField.set(detector, hooked);
    try {
      // ≥ 1 slice (625 ms): "race" is stale relative to staleAfterMs = 10 AND
      // old enough that the hooked add crosses a slice into the slow path.
      Thread.sleep(640);
      detector.evictStale(10);
    } finally {
      windowsField.set(detector, realWindows);
    }

    // The re-check observed the refreshed timestamp, so the window survived
    // with its counts intact — the refreshed add of 7 was written into the
    // live window mid-eviction.
    assertThat(detector.activeWindows()).as("window survives the refreshed re-check").isEqualTo(1);
    assertThat(addCount(detector, "race", 0)).as("counts preserved (100 + 7)").isEqualTo(107);
  }

  /**
   * An {@code addCountAtSlice} that arrives after the evictor's removal re-creates a fresh window inside
   * its own compute: no orphaned entry is left behind and the new add is visible.
   */
  @Test
  void evictStale_thenAddCount_recreatesFreshWindowWithoutOrphans() {
    SlidingWindowDetector detector = detector(10_000, 10, 0);
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      addCount(detector, "stale-key", 100);
      TimeSource.setTimeOffsetForTest(0, -base + 1_000);

      detector.evictStale(10);
      assertThat(detector.activeWindows()).isZero();

      // an add after eviction rebuilds cleanly: no orphaned slice, the new add is the whole
      // window.
      assertThat(addCount(detector, "stale-key", 42)).isEqualTo(42);
      assertThat(detector.activeWindows()).isOne();
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  @Test
  void windowSum_shouldReturnZeroForZeroedWindowSlice() {
    SlidingWindowDetector detector = detector(5000, 5, 0);
    addCount(detector, "expired", 100);
    assertThat(addCount(detector, "never-tracked", 0)).isZero();
  }

  @Test
  void sequentialSliceAdvancesKeepOnlyTheCurrentWindow() throws Exception {
    // Each advance writes a new absolute-slice tag; prior rotations remain in the ring but are
    // ignored by the sum.
    WindowGeometry geometry = WindowGeometry.of(10_000, 10);
    SlidingWindowDetector detector = new SlidingWindowDetector(geometry, 0);
    long sliceMs = geometry.sliceMs();

    addCount(detector, "seq", 100);
    for (int step = 1; step <= 6; step++) {
      Thread.sleep(sliceMs + 10);
      long sum = addCount(detector, "seq", 10);
      assertThat(sum).as("sum after step %d", step).isBetween((long) step * 10, 100L + (long) step * 10);
    }
  }

  /**
   * A resident key's slice crossing advances the entry under its monitor instead of re-entering the
   * map's compute: the hot path pays one hash lookup, not two.
   */
  @Test
  void aResidentSliceCrossingDoesNotReenterTheMapCompute() throws Exception {
    SlidingWindowDetector detector = detector(16, 16, 0);
    addCount(detector, "resident", 1);

    Field windowsField = SlidingWindowDetector.class.getDeclaredField("windows");
    windowsField.setAccessible(true);
    @SuppressWarnings({ "unchecked", "rawtypes" })
    ConcurrentHashMap<String, Object> realWindows = (ConcurrentHashMap<String, Object>) windowsField.get(detector);
    AtomicInteger computes = new AtomicInteger();
    @SuppressWarnings({ "unchecked", "rawtypes" })
    ConcurrentHashMap<String, Object> counting = new ConcurrentHashMap<String, Object>(realWindows) {
      @Override
      public Object compute(
        String key,
        java.util.function.BiFunction<? super String, ? super Object, ? extends Object> remappingFunction
      ) {
        computes.incrementAndGet();
        return super.compute(key, remappingFunction);
      }
    };
    windowsField.set(detector, counting);
    try {
      Thread.sleep(10L);
      addCount(detector, "resident", 2);
    } finally {
      windowsField.set(detector, realWindows);
    }

    assertThat(computes).as("a resident slice crossing uses the entry monitor").hasValue(0);
    assertThat(detector.activeWindows()).isOne();
  }

  /**
   * Pins the slice-gated fast path: repeated {@code addCountAtSlice} calls within one slice skip the
   * timestamp update entirely — every add must still land exactly and the returned window sum must
   * include the full history.
   */
  @Test
  void addCount_repeatedAddsInSameSlice_produceExactSums() {
    SlidingWindowDetector detector = detector(5000, 5, 0);
    long sum = 0;
    for (int i = 1; i <= 500; i++) {
      sum = addCount(detector, "burst", 2);
    }
    assertThat(sum).as("500 fast-path adds of 2, all inside the window").isEqualTo(1000);
  }

  /**
   * Pins the fast/slow interleaving: same-slice bursts (fast path) followed
   * by slice crossings (slow path) must keep the window sum exact — a recycled
   * slot's old tag is ignored, and every in-window add remains visible.
   */
  @Test
  void addCount_sameSliceBurstsAcrossSliceCrossings_keepSumExact() throws InterruptedException {
    WindowGeometry geometry = WindowGeometry.of(5_000, 5);
    SlidingWindowDetector detector = new SlidingWindowDetector(geometry, 0);
    long sliceMs = geometry.sliceMs();
    long sum = 0;
    for (int round = 0; round < 3; round++) {
      for (int i = 0; i < 50; i++) {
        sum = addCount(detector, "burst-cross", 1);
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
    SlidingWindowDetector detector = detector(8, 8, 0);
    long deadline = System.nanoTime() + 5_000_000L; // ~5 ms of continuous access
    while (System.nanoTime() < deadline) {
      addCount(detector, "hot-loop", 1);
    }

    detector.evictStale(50);
    assertThat(detector.activeWindows()).as("continuously accessed key survives").isEqualTo(1);

    Thread.sleep(100);
    detector.evictStale(50);
    assertThat(detector.activeWindows()).as("idle key is evicted").isZero();
  }

  /**
   * A key silent for a full window must read zero: the old slots still hold
   * counts, but their absolute-slice tags no longer match any readable slice.
   */
  @Test
  void windowSum_afterAFullSilentWindow_returnsZero() {
    long base = io.github.hyshmily.zeta.util.TimeSource.monotonicMillis();
    try {
      io.github.hyshmily.zeta.util.TimeSource.setTimeOffsetForTest(0, -base);
      SlidingWindowDetector detector = detector(1000, 16, 0);
      assertThat(addCount(detector, "k", 42L)).isEqualTo(42L);

      io.github.hyshmily.zeta.util.TimeSource.setTimeOffsetForTest(0, -base + 5_000);
      assertThat(detector.windowSum("k")).as("idle key must not report ghost traffic").isZero();
    } finally {
      io.github.hyshmily.zeta.util.TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  /**
   * A cap-sized sample reclaims every idle window it visits, not just the oldest: the other
   * report threads cannot take the sample's {@code tryLock}, so the seats released by one walk are
   * what let their admissions succeed instead of being rejected.
   */
  @Test
  void capacityReclaimReleasesEveryIdleWindowInOneSample() {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      SlidingWindowDetector detector = detector(64, 4, 2);
      addCount(detector, "idle-a", 1);
      addCount(detector, "idle-b", 1);

      TimeSource.setTimeOffsetForTest(0, -base + 200);
      assertThat(addCount(detector, "new", 1)).isEqualTo(1);
      assertThat(detector.activeWindows()).as("both idle windows were reclaimed").isOne();
      assertThat(detector.capacityEvictions()).isEqualTo(2);
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  /**
   * A tombstone sampled by a report is a new admission, not a reservation: its seat was returned
   * when it was set, so the replacement must run the same cap sample as a first touch. Directly
   * reserving would bypass the sample and leave reclaimable idle windows tracked.
   */
  @Test
  void aTombstoneReplacementRunsTheCapacitySample() throws Exception {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      SlidingWindowDetector detector = detector(64, 4, 1);

      Field windowsField = SlidingWindowDetector.class.getDeclaredField("windows");
      windowsField.setAccessible(true);
      @SuppressWarnings({ "unchecked", "rawtypes" })
      ConcurrentHashMap<String, Object> realWindows = (ConcurrentHashMap<String, Object>) windowsField.get(detector);

      recordCount(detector, "dead", 1);
      Object deadWindow = realWindows.get("dead");

      // Give the table four live seats at t=200, then tombstone only the t=0 window. The table is
      // above the soft cap (1) but below the hard ledger (9), which is where a direct reservation
      // bypasses the sampling gate.
      TimeSource.setTimeOffsetForTest(0, -base + 200);
      for (int i = 0; i < 4; i++) {
        recordCount(detector, "hot-" + i, 1);
      }
      detector.evictStale(0);
      assertThat(detector.activeWindows()).isEqualTo(4);

      TimeSource.setTimeOffsetForTest(0, -base + 400);
      @SuppressWarnings({ "unchecked", "rawtypes" })
      ConcurrentHashMap<String, Object> tombstoneLookup = new TombstoneLookupMap("ghost", deadWindow);
      tombstoneLookup.putAll(realWindows);

      Field reclaimerField = SlidingWindowDetector.class.getDeclaredField("reclaimer");
      reclaimerField.setAccessible(true);
      Object reclaimer = reclaimerField.get(detector);
      Field reclaimerMap = reclaimer.getClass().getDeclaredField("map");
      reclaimerMap.setAccessible(true);
      reclaimerMap.set(reclaimer, tombstoneLookup);
      windowsField.set(detector, tombstoneLookup);

      assertThat(addCount(detector, "ghost", 1)).isEqualTo(1);
      assertThat(detector.capacityEvictions())
        .as("the four stale windows were reclaimed by the admission's sample")
        .isEqualTo(4);
      assertThat(detector.activeWindows()).as("only the replacement window remains").isOne();
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  /**
   * A weak iterator can still show an entry another path already evicted. That ghost must not be
   * counted as a capacity eviction: this pass did not release its seat. The ghost is injected by a
   * map whose iterator yields the detached, tombstoned window ahead of the two real stale entries.
   */
  @Test
  void aConcurrentlyRemovedGhostIsNotCountedAsACapacityEviction() throws Exception {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      SlidingWindowDetector detector = detector(64, 4, 2);

      Field windowsField = SlidingWindowDetector.class.getDeclaredField("windows");
      windowsField.setAccessible(true);
      @SuppressWarnings({ "unchecked", "rawtypes" })
      ConcurrentHashMap<String, Object> realWindows = (ConcurrentHashMap<String, Object>) windowsField.get(detector);

      addCount(detector, "ghost", 1);
      Object ghostWindow = realWindows.get("ghost");

      // Remove the ghost through the normal evictor, leaving a detached tombstone the weak
      // iterator can still yield. Then seed two fresh seats and make them stale.
      TimeSource.setTimeOffsetForTest(0, -base + 200);
      detector.evictStale(10);
      addCount(detector, "real-a", 1);
      addCount(detector, "real-b", 1);
      TimeSource.setTimeOffsetForTest(0, -base + 400);

      @SuppressWarnings({ "unchecked", "rawtypes" })
      ConcurrentHashMap<String, Object> hooked = new GhostEntryMap("ghost", ghostWindow);
      hooked.putAll(realWindows);

      Field reclaimerField = SlidingWindowDetector.class.getDeclaredField("reclaimer");
      reclaimerField.setAccessible(true);
      Object reclaimer = reclaimerField.get(detector);
      Field reclaimerMap = reclaimer.getClass().getDeclaredField("map");
      reclaimerMap.setAccessible(true);
      reclaimerMap.set(reclaimer, hooked);
      windowsField.set(detector, hooked);

      assertThat(addCount(detector, "new", 1)).isEqualTo(1);
      assertThat(detector.capacityEvictions())
        .as("only the real stale window this pass removed is a capacity eviction")
        .isEqualTo(1);
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  /** A map whose lookup returns a detached tombstone without making it a map entry. */
  private static final class TombstoneLookupMap<K, V> extends ConcurrentHashMap<K, V> {

    private final K tombstoneKey;
    private final V tombstoneValue;

    TombstoneLookupMap(K tombstoneKey, V tombstoneValue) {
      this.tombstoneKey = tombstoneKey;
      this.tombstoneValue = tombstoneValue;
    }

    @Override
    public V get(Object key) {
      return tombstoneKey.equals(key) ? tombstoneValue : super.get(key);
    }
  }

  /** A map whose iterator prepends one detached entry the concurrent removal already tombstoned. */
  private static final class GhostEntryMap<K, V> extends ConcurrentHashMap<K, V> {

    private final K ghostKey;
    private final V ghostValue;

    GhostEntryMap(K ghostKey, V ghostValue) {
      this.ghostKey = ghostKey;
      this.ghostValue = ghostValue;
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
      Set<Map.Entry<K, V>> real = super.entrySet();
      return new AbstractSet<>() {
        @Override
        public Iterator<Map.Entry<K, V>> iterator() {
          Iterator<Map.Entry<K, V>> delegate = real.iterator();
          return new Iterator<>() {
            private boolean ghostPending = true;

            @Override
            public boolean hasNext() {
              return ghostPending || delegate.hasNext();
            }

            @Override
            public Map.Entry<K, V> next() {
              if (ghostPending) {
                ghostPending = false;
                return Map.entry(ghostKey, ghostValue);
              }
              return delegate.next();
            }
          };
        }

        @Override
        public int size() {
          return real.size() + 1;
        }
      };
    }
  }

  /**
   * A rejected admission at the hard ledger must arm the periodic scan's relief signal: the
   * sweep then runs with the window-length threshold instead of the retention, bounding the
   * lockout to one scan interval.
   */
  @Test
  void rejectedAdmissionAtTheHardLedgerArmsReliefOnce() {
    SlidingWindowDetector detector = detector(60_000, 16, 1);
    addCount(detector, "resident", 1);
    assertThat(detector.consumeReliefRequest()).as("an ordinary admission does not arm relief").isFalse();

    // Cap 1 plus the max(8, cap/100) headroom: nine seats. Nine already-hot windows exhaust the
    // ledger without going through the soft-cap sample.
    for (int i = 0; i < 9; i++) {
      recordCount(detector, "hot-" + i, 1);
    }
    assertThat(addCount(detector, "rejected", 1)).isZero();

    assertThat(detector.consumeReliefRequest()).isTrue();
    assertThat(detector.consumeReliefRequest()).as("the signal is consumed exactly once").isFalse();
    assertThat(detector.rejectedNewKeys()).isEqualTo(1);
  }

  /**
   * The hard headroom refuses a new key even when every resident window is idle enough for a sample
   * to reclaim: the sample is what frees seats, and letting admissions race it is the unbounded
   * growth the soft cap alone cannot stop. The stale sweep, not an admission, is what recovers.
   */
  @Test
  void hardHeadroomRejectsANewKeyBeforeTheSampleCanReclaimIdleWindows() {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      SlidingWindowDetector detector = detector(64, 4, 10);
      for (int i = 0; i < 18; i++) {
        recordCount(detector, "hot-" + i, 1);
      }

      TimeSource.setTimeOffsetForTest(0, -base + 200);
      assertThat(addCount(detector, "new", 1)).as("over the hard headroom: no admission, no sample").isZero();
      assertThat(detector.activeWindows()).isEqualTo(18);
      assertThat(detector.capacityEvictions()).isEqualTo(0);

      detector.evictStale(0);
      assertThat(addCount(detector, "new", 1)).as("the sweep freed the table").isEqualTo(1);
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  /**
   * A negative monotonic timestamp is a real window coordinate: a window created at
   * {@code now == -1} remains live and readable instead of being mistaken for the eviction
   * tombstone.
   */
  @Test
  void aNegativeMonotonicTimestampIsNotTheEvictionTombstone() {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base - 1);
      SlidingWindowDetector detector = detector(1000, 16, 0);

      assertThat(addCount(detector, "minus-one", 5)).isEqualTo(5);
      assertThat(detector.windowSum("minus-one")).isEqualTo(5);
      assertThat(detector.activeWindows()).isOne();
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  /** The already-hot report path records the count without paying for the window-sum walk. */
  @Test
  void recordCountFeedsTheWindowWithoutReturningASum() {
    SlidingWindowDetector detector = detector(10_000, 10, 0);
    recordCount(detector, "k", 42);
    assertThat(detector.windowSum("k")).isEqualTo(42);
  }

  /**
   * A key that already holds a registry seat is not a "new key": when its window was reclaimed and
   * the table is at the tracked-key cap, its count is still admitted so a live hot key does not
   * look quiet to a LOW_QPS cool-down.
   */
  @Test
  void recordCountAdmitsAHotKeysWindowAtTheTrackedKeyCap() {
    SlidingWindowDetector detector = detector(10_000, 10, 1);
    addCount(detector, "resident", 1);

    recordCount(detector, "hot", 5);

    assertThat(detector.activeWindows()).isEqualTo(2);
    assertThat(detector.windowSum("hot")).isEqualTo(5);
    assertThat(detector.rejectedNewKeys()).isEqualTo(0);
  }

  /**
   * A hot key's window is admitted even when the seat ledger is exhausted: the bound counts new
   * tracked keys, and losing a live hot key's count would make it look quiet to a LOW_QPS cool-down.
   * The admission is not charged to the new-key rejection counter either.
   */
  @Test
  void recordCountAdmitsAHotKeysWindowAboveTheHardLedger() {
    SlidingWindowDetector detector = detector(10_000, 10, 1);
    for (int i = 0; i < 9; i++) {
      recordCount(detector, "hot-" + i, 1);
    }

    recordCount(detector, "still-hot", 5);

    assertThat(detector.windowSum("still-hot")).isEqualTo(5);
    assertThat(detector.rejectedNewKeys()).isZero();
  }

  /**
   * Every eviction returns exactly one seat, so continuous churn at the tracked-key cap never
   * exhausts the hard ledger. A one-seat leak per round would fill the ten-seat ledger long before
   * the final probe.
   */
  @Test
  void evictionsReturnTheirSeatsSoChurnNeverExhaustsTheHardLedger() {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      // cap 2 plus the max(8, 1%) headroom: ten seats.
      WindowGeometry geometry = WindowGeometry.of(64, 4);
      SlidingWindowDetector detector = new SlidingWindowDetector(geometry, 2);
      long sliceMs = geometry.sliceMs();

      for (int round = 0; round < 32; round++) {
        TimeSource.setTimeOffsetForTest(0, -base + round * sliceMs);
        assertThat(addCount(detector, "a", 1)).as("round %d admits a", round).isEqualTo(1);
        assertThat(addCount(detector, "b", 1)).as("round %d admits b", round).isEqualTo(1);

        // Cross one slice so evictStale(0) reclaims both windows and their seats.
        TimeSource.setTimeOffsetForTest(0, -base + (round + 1) * sliceMs);
        detector.evictStale(0);
        assertThat(detector.activeWindows()).as("round %d reclaimed", round).isZero();
      }

      assertThat(addCount(detector, "probe", 1)).as("the ledger still has room").isEqualTo(1);
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }
}
