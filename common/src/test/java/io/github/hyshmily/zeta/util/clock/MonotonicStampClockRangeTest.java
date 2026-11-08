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
package io.github.hyshmily.zeta.util.clock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Contract tests for the atomic range reservation behind batched version
 * allocation (review 2026-10 P2-3): one CAS reserves a contiguous block no
 * concurrent allocation can enter, so a concurrent single write is ordered
 * against a whole batch at once instead of interleaving between two of its
 * keys.
 */
class MonotonicStampClockRangeTest {

  @Test
  void nextStamps_reservesAContiguousBlockAboveEveryPriorStamp() {
    MonotonicStampClock clock = new MonotonicStampClock(1L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS);
    long first = clock.nextStamp();
    long base = clock.nextStamps(5);
    assertThat(base).isPositive().isGreaterThan(first);
    // The next single allocation must rank above the block's top — the
    // reservation owns the range exclusively.
    assertThat(clock.nextStamp()).isGreaterThan(base + 4);
  }

  @Test
  void nextStamps_repeatedCallsAreStrictlyIncreasingAndDisjoint() {
    MonotonicStampClock clock = new MonotonicStampClock(1L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS);
    long prevTop = 0;
    for (int round = 0; round < 100; round++) {
      long base = clock.nextStamps(7);
      assertThat(base).isGreaterThan(prevTop);
      prevTop = base + 6;
    }
  }

  @Test
  void nextStamps_rejectsCountsOutsideOneSequenceWindow() {
    MonotonicStampClock clock = new MonotonicStampClock(1L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS);
    assertThatThrownBy(() -> clock.nextStamps(0)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> clock.nextStamps(-3)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> clock.nextStamps(StampClock.MAX_ATOMIC_RANGE + 1)).isInstanceOf(
      IllegalArgumentException.class
    );
    // The window edge itself is fine: 4096 stamps fit one sequence window.
    assertThat(clock.nextStamps(StampClock.MAX_ATOMIC_RANGE)).isPositive();
  }

  @Test
  void nextStamps_afterPeerObservation_staysAboveTheMergedFloor() {
    MonotonicStampClock clock = new MonotonicStampClock(1L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS);
    MonotonicStampClock peer = new MonotonicStampClock(1023L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS);
    long peerStamp = peer.nextStamp();
    clock.observe(peerStamp);
    // The floor carries the peer's node bits, which sort above this node's
    // identity in the same window: the range must roll forward, never issue
    // at or below the floor.
    long base = clock.nextStamps(10);
    assertThat(base).isGreaterThan(peerStamp);
    assertThat(clock.nextStamp()).isGreaterThan(base + 9);
  }

  @Test
  void mixedConcurrentAllocation_rangesAreExclusiveAgainstSingleStamps() throws Exception {
    MonotonicStampClock clock = new MonotonicStampClock(4L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS);
    int threads = 4;
    int rounds = 500;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    // Range threads record [base, top] blocks; single threads record their
    // stamps. Every allocation is unique, ranges never overlap, and no single
    // stamp may fall inside a range — the atomicity the batch paths rely on.
    List<long[]> ranges = new ArrayList<>();
    Set<Long> singleStamps = ConcurrentHashMap.newKeySet();
    Set<Long> rangeStamps = ConcurrentHashMap.newKeySet();
    CountDownLatch ready = new CountDownLatch(threads);
    CountDownLatch go = new CountDownLatch(1);
    for (int t = 0; t < threads; t++) {
      final boolean rangeThread = t % 2 == 0;
      pool.submit(() -> {
        ready.countDown();
        try {
          go.await();
          for (int i = 0; i < rounds; i++) {
            if (rangeThread) {
              int n = 1 + (i % 64);
              long base = clock.nextStamps(n);
              ranges.add(new long[] { base, base + n - 1L });
              rangeStamps.add(base);
            } else {
              singleStamps.add(clock.nextStamp());
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
    }
    ready.await();
    go.countDown();
    pool.shutdown();
    assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();

    // 1) ranges are pairwise disjoint (sort by base; next base strictly above
    //    the previous top).
    List<long[]> sorted = new ArrayList<>(ranges);
    sorted.sort((a, b) -> Long.compare(a[0], b[0]));
    for (int i = 1; i < sorted.size(); i++) {
      assertThat(sorted.get(i)[0]).as("ranges must not overlap").isGreaterThan(sorted.get(i - 1)[1]);
    }
    // 2) no single stamp inside any range — binary search over range bases.
    long[] bases = sorted
      .stream()
      .mapToLong(r -> r[0])
      .toArray();
    for (Long s : singleStamps) {
      int idx = floorIndex(bases, s);
      if (idx >= 0) {
        assertThat(s).as("single stamp %s inside reserved range", s).isGreaterThan(sorted.get(idx)[1]);
      }
    }
    // 3) the whole space stayed positive (the stamp contract).
    assertThat(singleStamps).allSatisfy(s -> assertThat(s).isPositive());
    assertThat(rangeStamps).allSatisfy(s -> assertThat(s).isPositive());
  }

  /**
   * A range allocation that lands on this node's own non-zero sequence must
   * continue that sequence, not treat the floor as a higher-ranked peer and
   * roll into the next millisecond. The old range copy compared its rebuilt
   * base (sequence 0) with the raw floor, so alternating single + range
   * allocation burned one full window per pair.
   */
  @Test
  void nextStamps_doesNotBurnAWindowPerCall() {
    MonotonicStampClock clock = new MonotonicStampClock(1L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS);
    for (int i = 0; i < 2_000; i++) {
      clock.nextStamp();
      clock.nextStamps(4);
    }
    // The buggy path pushed the logical clock ~2s ahead of the wall; the fixed
    // one only runs ahead on genuine sequence overflow (a few ms here). A slow
    // machine makes the wall clock advance more, which lowers this value.
    assertThat(clock.skewMillis()).isLessThan(500L);
  }

  /** Index of the greatest range base {@code <= stamp}, or -1. */
  private static int floorIndex(long[] bases, long stamp) {
    int lo = 0;
    int hi = bases.length - 1;
    int ans = -1;
    while (lo <= hi) {
      int mid = (lo + hi) >>> 1;
      if (bases[mid] <= stamp) {
        ans = mid;
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    return ans;
  }
}
