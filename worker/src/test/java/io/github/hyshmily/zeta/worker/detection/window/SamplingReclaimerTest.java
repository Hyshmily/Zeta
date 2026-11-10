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

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.util.TimeSource;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** Admission, reclaim and the saturation verdict are one locked pass. */
class SamplingReclaimerTest {

  private static SamplingReclaimer<Long> reclaimer(ConcurrentHashMap<String, Long> map) {
    return new SamplingReclaimer<>("test", map, 32, 100);
  }

  private static SamplingReclaimer<Long> twoEntryReclaimer(ConcurrentHashMap<String, Long> map) {
    return new SamplingReclaimer<>("test", map, 2, 2);
  }

  @Test
  void belowCapAdmitsWithoutSampling() {
    ConcurrentHashMap<String, Long> map = new ConcurrentHashMap<>();
    map.put("resident", 1L);
    SamplingReclaimer<Long> reclaimer = new SamplingReclaimer<>("test", map, 32, 2);
    AtomicInteger calls = new AtomicInteger();

    assertThat(
      reclaimer.tryAdmit("new", 0L, (entry, slice) -> {
        calls.incrementAndGet();
        return false;
      })
    ).isTrue();
    assertThat(calls).hasValue(0);
  }

  @Test
  void atCapReclaimsEveryIdleEntryInOnePass() {
    ConcurrentHashMap<String, Long> map = new ConcurrentHashMap<>();
    map.put("idle-a", 0L);
    map.put("idle-b", 0L);
    map.put("live", 1L);
    SamplingReclaimer<Long> reclaimer = new SamplingReclaimer<>("test", map, 32, 3);

    assertThat(
      reclaimer.tryAdmit(
        "new",
        0L,
        (entry, slice) -> entry.getValue() == 0L && map.remove(entry.getKey(), entry.getValue())
      )
    ).isTrue();
    assertThat(map).containsOnlyKeys("live");
    assertThat(reclaimer.capacityEvictions()).isEqualTo(2);
  }

  @Test
  void aPassThatFindsNothingIdleOpensTheSaturationWindowButStillAdmitsItsOwnKey() {
    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base);
      ConcurrentHashMap<String, Long> map = new ConcurrentHashMap<>();
      map.put("live", 1L);
      SamplingReclaimer<Long> reclaimer = new SamplingReclaimer<>("test", map, 32, 1);
      AtomicInteger calls = new AtomicInteger();
      SamplingReclaimer.IdleEvictor<Long> predicate = (entry, slice) -> {
        calls.incrementAndGet();
        return false;
      };

      assertThat(reclaimer.tryAdmit("new", 0L, predicate))
        .as("the pass's own admission uses the hard headroom")
        .isTrue();
      map.put("new", 1L);
      assertThat(calls).hasValue(1);
      assertThat(reclaimer.consumeReliefRequest())
        .as("a pass that found nothing idle arms the accelerated sweep")
        .isTrue();
      // Pin the clock so the 50 ms window cannot expire between these two calls on a slow CI.
      TimeSource.setTimeOffsetForTest(0, -base);
      assertThat(reclaimer.tryAdmit("next", 0L, predicate))
        .as("the saturation window rejects the arrival behind the pass without sampling")
        .isFalse();
      assertThat(calls).hasValue(1);
      assertThat(reclaimer.rejectedNewKeys()).isEqualTo(1);

      TimeSource.setTimeOffsetForTest(0, -base + 60);
      assertThat(reclaimer.tryAdmit("later", 0L, predicate)).isTrue();
      assertThat(calls).as("the window expired and the pass ran again").hasValue(3);
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  /**
   * The monotonic clock can sit below zero, so a saturation deadline that lands exactly on zero is
   * a real instant, not the inactive sentinel. The next arrival at the cap must still be suppressed.
   */
  @Test
  void aSaturationDeadlineAtZeroStillSuppressesAdmissions() throws Exception {
    ConcurrentHashMap<String, Long> map = new ConcurrentHashMap<>();
    map.put("live", 1L);
    SamplingReclaimer<Long> reclaimer = new SamplingReclaimer<>("test", map, 2, 1);
    Field until = SamplingReclaimer.class.getDeclaredField("saturatedUntilMs");
    until.setAccessible(true);
    ((AtomicLong) until.get(reclaimer)).set(0L);

    long base = TimeSource.monotonicMillis();
    try {
      TimeSource.setTimeOffsetForTest(0, -base - 50L);
      AtomicInteger calls = new AtomicInteger();
      assertThat(
        reclaimer.tryAdmit("next", 0L, (entry, slice) -> {
          calls.incrementAndGet();
          return false;
        })
      ).isFalse();
      assertThat(calls.get()).as("the zero deadline is a real saturation instant").isZero();
    } finally {
      TimeSource.setTimeOffsetForTest(0, 0);
    }
  }

  @Test
  void hardHeadroomSamplesBeforeRejectingWhenNothingIsIdle() {
    ConcurrentHashMap<String, Long> map = new ConcurrentHashMap<>();
    for (int i = 0; i < 18; i++) {
      map.put("k" + i, 1L);
    }
    SamplingReclaimer<Long> reclaimer = new SamplingReclaimer<>("test", map, 2, 10);
    AtomicInteger calls = new AtomicInteger();

    assertThat(
      reclaimer.tryAdmit("new", 0L, (entry, slice) -> {
        calls.incrementAndGet();
        return false;
      })
    ).isFalse();
    assertThat(calls.get()).as("the hard bound still gets one uncontended pass").isPositive();
    assertThat(reclaimer.consumeReliefRequest()).isTrue();
    assertThat(reclaimer.rejectedNewKeys()).isEqualTo(1);
  }

  /**
   * A table at the hard bound can still hold idle windows. Refusing before the walk left them
   * unreclaimed until the periodic scan, so one uncontended pass must run and admit from the seat
   * it frees.
   */
  @Test
  void hardHeadroomReclaimsAnIdleSeatInsteadOfRejecting() {
    ConcurrentHashMap<String, Long> map = new ConcurrentHashMap<>();
    for (int i = 0; i < 18; i++) {
      map.put("k" + i, 1L);
    }
    SamplingReclaimer<Long> reclaimer = new SamplingReclaimer<>("test", map, 2, 10);
    AtomicBoolean first = new AtomicBoolean(true);

    assertThat(
      reclaimer.tryAdmit("new", 0L, (entry, slice) -> {
        if (!first.compareAndSet(true, false)) {
          return false;
        }
        map.remove(entry.getKey());
        return true;
      })
    ).isTrue();
    assertThat(map).hasSize(17);
    assertThat(reclaimer.usedSeats()).as("the reclaimed seat is consumed by this admission").isEqualTo(18);
  }

  /**
   * The predicate runs under the sample lock, so a holder parked inside it is a deterministic
   * concurrent admission. The second admission must proceed: a missing pass is not evidence that
   * nothing is idle.
   */
  @Test
  void aContendedPassAdmitsInsteadOfRejecting() throws Exception {
    ConcurrentHashMap<String, Long> map = new ConcurrentHashMap<>();
    map.put("resident", 1L);
    SamplingReclaimer<Long> reclaimer = new SamplingReclaimer<>("test", map, 32, 1);

    CountDownLatch insidePass = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Thread holder = new Thread(() ->
      reclaimer.tryAdmit("holder", 0L, (entry, slice) -> {
        insidePass.countDown();
        await(release);
        return false;
      })
    );
    holder.start();
    try {
      assertThat(insidePass.await(3, TimeUnit.SECONDS)).isTrue();
      AtomicInteger calls = new AtomicInteger();
      assertThat(
        reclaimer.tryAdmit("new", 0L, (entry, slice) -> {
          calls.incrementAndGet();
          return false;
        })
      ).isTrue();
      assertThat(calls).hasValue(0);
    } finally {
      release.countDown();
      holder.join(3_000L);
    }
  }

  /**
   * The pass's own admission reserves its hard-headroom seat before the walk. Contenders that lose
   * the lock may fill the rest, but they cannot take the seat the scanning thread is about to use.
   */
  @Test
  void theSamplingAdmissionIsNotRejectedAfterContendersFillTheHeadroom() throws Exception {
    ConcurrentHashMap<String, Long> map = new ConcurrentHashMap<>();
    for (int i = 0; i < 17; i++) {
      map.put("k" + i, 1L);
    }
    // Soft cap 10, hard limit 18: the table starts one seat below the hard bound.
    SamplingReclaimer<Long> reclaimer = new SamplingReclaimer<>("test", map, 32, 10);
    CountDownLatch insidePass = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicBoolean admitted = new AtomicBoolean();
    Thread holder = new Thread(() ->
      admitted.set(
        reclaimer.tryAdmit("holder", 0L, (entry, slice) -> {
          insidePass.countDown();
          await(release);
          return false;
        })
      )
    );
    holder.start();
    try {
      assertThat(insidePass.await(3, TimeUnit.SECONDS)).isTrue();
      // The holder already reserved the last hard-headroom seat; the contenders hit the ledger.
      assertThat(reclaimer.tryAdmit("contender-a", 0L, (entry, slice) -> false)).isFalse();
      assertThat(reclaimer.tryAdmit("contender-b", 0L, (entry, slice) -> false)).isFalse();
    } finally {
      release.countDown();
      holder.join(3_000L);
    }

    assertThat(admitted).as("the thread that paid for the walk keeps its seat").isTrue();
    assertThat(reclaimer.usedSeats()).isEqualTo(18);
  }

  /**
   * A pass that reclaimed a seat can still leave the table at the cap when concurrent admissions fill
   * it during the walk. The relief scan must stay armed.
   */
  @Test
  void aPassThatStillLeavesTheTableAtTheCapArmsRelief() {
    ConcurrentHashMap<String, Long> map = new ConcurrentHashMap<>();
    for (int i = 0; i < 17; i++) {
      map.put("k" + i, 1L);
    }
    SamplingReclaimer<Long> reclaimer = new SamplingReclaimer<>("test", map, 2, 10);
    boolean[] added = { false };

    assertThat(
      reclaimer.tryAdmit("new", 0L, (entry, slice) -> {
        if (!added[0]) {
          added[0] = true;
          map.put("added-a", 1L);
          map.put("added-b", 1L);
          map.put("added-c", 1L);
        }
        return map.remove(entry.getKey(), entry.getValue());
      })
    ).isTrue();

    assertThat(reclaimer.capacityEvictions()).isPositive();
    assertThat(reclaimer.consumeReliefRequest()).as("the hard bound keeps the relief scan armed").isTrue();
  }

  /**
   * The seat a pass frees is returned to the ledger only after the evictor commits, while the
   * pass's own admission is already protected by its pre-walk reservation. A contender that
   * bypasses the lock cannot take the last headroom slot during the walk.
   */
  @Test
  void aReclaimedSeatIsReturnedAfterTheSamplingAdmissionIsProtected() throws Exception {
    ConcurrentHashMap<String, Long> map = new ConcurrentHashMap<>();
    for (int i = 0; i < 8; i++) {
      map.put("k" + i, 1L);
    }
    // Cap 1 plus the max(8, 1%) headroom: nine seats, eight already taken.
    SamplingReclaimer<Long> reclaimer = new SamplingReclaimer<>("test", map, 32, 1);
    boolean[] first = { true };
    AtomicBoolean admitted = new AtomicBoolean();
    CountDownLatch insidePass = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Thread holder = new Thread(() ->
      admitted.set(
        reclaimer.tryAdmit("new", 0L, (entry, slice) -> {
          if (!first[0]) {
            return false;
          }
          first[0] = false;
          map.remove(entry.getKey());
          insidePass.countDown();
          await(release);
          return true;
        })
      )
    );
    holder.start();
    try {
      assertThat(insidePass.await(3, TimeUnit.SECONDS)).isTrue();
      // The reservation already occupies the last headroom slot; the reclaimed seat has not been
      // committed yet, so both contenders hit the hard bound.
      assertThat(reclaimer.tryAdmit("bypass-a", 0L, (entry, slice) -> false)).isFalse();
      assertThat(reclaimer.tryAdmit("bypass-b", 0L, (entry, slice) -> false)).isFalse();
    } finally {
      release.countDown();
      holder.join(3_000L);
    }

    assertThat(admitted).as("the thread that paid for the walk keeps its seat").isTrue();
    assertThat(reclaimer.usedSeats()).as("the reclaimed seat returned after the walk").isEqualTo(8);
    assertThat(reclaimer.tryAdmit("later", 0L, (entry, slice) -> false)).isTrue();
    assertThat(reclaimer.usedSeats()).isEqualTo(9);
  }

  /**
   * The saturation verdict is taken against the ledger as it is after the walk: the periodic sweep does
   * not take the sample lock, so it can release seats while a pass is sampling the survivors. Rejecting
   * then opens a 50 ms window on a table that is no longer at the cap.
   */
  @Test
  void aPassThatFindsNothingIdleDoesNotSaturateAfterAConcurrentSweepFreedSeats() {
    ConcurrentHashMap<String, Long> map = new ConcurrentHashMap<>();
    map.put("a", 1L);
    map.put("b", 1L);
    SamplingReclaimer<Long> reclaimer = new SamplingReclaimer<>("test", map, 32, 2);
    AtomicInteger calls = new AtomicInteger();

    assertThat(
      reclaimer.tryAdmit("new", 0L, (entry, slice) -> {
        calls.incrementAndGet();
        // The periodic sweep released one seat between the cap check and this pass.
        reclaimer.releaseSeat();
        return false;
      })
    ).isTrue();

    assertThat(calls).hasValue(2);
    assertThat(reclaimer.rejectedNewKeys()).isZero();
    assertThat(reclaimer.usedSeats()).isEqualTo(1);

    // No saturation window is open: the next arrival walks the table again instead of being suppressed.
    assertThat(reclaimer.tryAdmit("filler", 0L, (entry, slice) -> false)).isTrue();
    AtomicInteger nextPass = new AtomicInteger();
    assertThat(
      reclaimer.tryAdmit("next", 0L, (entry, slice) -> {
        nextPass.incrementAndGet();
        return false;
      })
    )
      .as("the pass runs and its own admission uses the hard headroom")
      .isTrue();
    assertThat(nextPass).hasValue(2);
  }

  /**
   * The hard bound is a seat ledger, not a size pre-check: many threads racing the same soft-cap
   * observation must still stop at the configured hard limit.
   */
  @Test
  void concurrentAdmissionsNeverExceedTheHardLimit() throws Exception {
    ConcurrentHashMap<String, Long> map = new ConcurrentHashMap<>();
    SamplingReclaimer<Long> reclaimer = new SamplingReclaimer<>("test", map, 8, 10);
    int threads = 64;
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);
    for (int i = 0; i < threads; i++) {
      String key = "k" + i;
      Thread thread = new Thread(() -> {
        await(start);
        if (reclaimer.tryAdmit(key, 0L, (entry, slice) -> false)) {
          map.put(key, 1L);
        }
        done.countDown();
      });
      thread.start();
    }
    start.countDown();
    assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();

    assertThat(reclaimer.usedSeats()).isLessThanOrEqualTo(18);
    assertThat(map).hasSizeLessThanOrEqualTo(18);
  }

  /**
   * The below-cap fast path spends no headroom. A burst that all observe the last below-cap seat
   * must fall through to the sampling path; otherwise the hard ledger can fill without one pass,
   * and the next arrival is rejected from a table that still holds reclaimable idle windows.
   */
  @Test
  void aBurstAtTheCapBoundaryDoesNotSpendTheHeadroomWithoutSampling() throws Exception {
    ConcurrentHashMap<String, Long> map = new ConcurrentHashMap<>();
    for (int i = 0; i < 7; i++) {
      map.put("k" + i, 1L);
    }
    // Soft cap 8: the table starts one seat below it; the hard ledger is 16.
    SamplingReclaimer<Long> reclaimer = new SamplingReclaimer<>("test", map, 32, 8);
    int threads = 32;
    AtomicInteger samples = new AtomicInteger();
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);
    for (int i = 0; i < threads; i++) {
      String key = "n" + i;
      Thread thread = new Thread(() -> {
        await(start);
        if (
          reclaimer.tryAdmit(key, 0L, (entry, slice) -> {
            samples.incrementAndGet();
            return false;
          })
        ) {
          map.put(key, 1L);
        }
        done.countDown();
      });
      thread.start();
    }
    start.countDown();
    assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();

    assertThat(reclaimer.usedSeats()).isLessThanOrEqualTo(16);
    assertThat(reclaimer.rejectedNewKeys()).isPositive();
    assertThat(samples.get()).as("a rejection at the hard bound was preceded by a sampling pass").isPositive();
  }

  /**
   * A pass that consumed the iterator's last entry leaves an exhausted hand. The next pass must
   * rebuild it at the head instead of treating the tail as an empty table.
   */
  @Test
  void aPassThatEndedAtTheTailRestartsAtTheHead() {
    ConcurrentHashMap<String, Long> map = new ConcurrentHashMap<>();
    map.put("a", 1L);
    map.put("b", 2L);
    SamplingReclaimer<Long> reclaimer = twoEntryReclaimer(map);

    List<String> first = new ArrayList<>();
    assertThat(
      reclaimer.tryAdmit("new", 0L, (entry, slice) -> {
        first.add(entry.getKey());
        if (!"a".equals(entry.getKey())) {
          // Reclaiming one entry keeps the pass's seat, so the ledger stays at the cap and the next
          // admission has to sample again — which is the hand rebuild this test is about.
          return false;
        }
        map.remove(entry.getKey());
        return true;
      })
    ).isTrue();
    assertThat(first).as("the pass walked to the tail of the table").containsExactlyInAnyOrder("a", "b");

    map.put("a", 1L);
    List<String> second = new ArrayList<>();
    assertThat(
      reclaimer.tryAdmit("new", 0L, (entry, slice) -> {
        second.add(entry.getKey());
        map.remove(entry.getKey());
        return true;
      })
    ).isTrue();
    assertThat(second).as("the rebuilt hand covers the table again").containsExactlyInAnyOrder("a", "b");
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await(3, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
