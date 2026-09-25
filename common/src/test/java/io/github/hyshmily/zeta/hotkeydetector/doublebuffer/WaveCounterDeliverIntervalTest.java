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
package io.github.hyshmily.zeta.hotkeydetector.doublebuffer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.hotkeydetector.doublebuffer.WaveCounterSchedulingTest.RecordingScheduler;
import java.lang.reflect.Field;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * {@code adjustDeliverIntervalMs} (ADR-0078): the feed-loop interval tuner
 * moves the tide loop's BASE cadence; the existing adaptations keep acting
 * around it — backlog pressure compresses a non-empty tide from the base
 * toward the floor, the empty-tide ladder stretches an idle cycle above it
 * (cap 2x the base), and a sub-floor request is clamped up so the
 * construction-time invariant (the governor's flood-rate normalization
 * divides by the cadence) is never broken by a later adjustment.
 */
class WaveCounterDeliverIntervalTest {

  private RecordingScheduler scheduler;
  private WaveCounter counter;

  @BeforeEach
  void setUp() {
    scheduler = new RecordingScheduler();
    counter = new WaveCounter(ignored -> {}, scheduler);
  }

  @AfterEach
  void tearDown() {
    counter.destroy();
  }

  /** Reads the private {@code deliverIntervalMs} field (test-side mirror). */
  private static long deliverIntervalMs(WaveCounter counter) throws Exception {
    Field f = WaveCounter.class.getDeclaredField("deliverIntervalMs");
    f.setAccessible(true);
    return f.getLong(counter);
  }

  /** Drops the pending-tide reference so the next tide re-arms unabsorbed. */
  private static void dropPendingTide(WaveCounter counter) throws Exception {
    Field f = WaveCounter.class.getDeclaredField("pendingTide");
    f.setAccessible(true);
    f.set(counter, null);
  }

  private void runTide() throws Exception {
    java.lang.reflect.Method tide = WaveCounter.class.getDeclaredMethod("tide");
    tide.setAccessible(true);
    tide.invoke(counter);
  }

  @Test
  void adjust_clampsSubFloorRequestsUpToTheFloor() throws Exception {
    assertThat(deliverIntervalMs(counter)).isEqualTo(500L);
    counter.adjustDeliverIntervalMs(-5L);
    assertThat(deliverIntervalMs(counter)).as("sub-floor clamp").isEqualTo(50L);
    counter.adjustDeliverIntervalMs(0L);
    assertThat(deliverIntervalMs(counter)).as("zero clamp").isEqualTo(50L);
  }

  @Test
  void adjust_takesEffectAtTheNextEmptyTide() throws Exception {
    counter.afterPropertiesSet();
    assertThat(scheduler.delaysMillis).containsExactly(500L);

    counter.adjustDeliverIntervalMs(1000L);
    // The armed tide is intentionally left alone — one fire of lag.
    assertThat(scheduler.delaysMillis).hasSize(1);

    dropPendingTide(counter);
    runTide(); // empty tide: first stretch step = base << 0 = the (new) base
    assertThat(scheduler.delaysMillis).containsExactly(500L, 1000L);
  }

  @Test
  void adjust_movesTheBaseOfTheEmptyTideLadder() throws Exception {
    counter.afterPropertiesSet();
    counter.adjustDeliverIntervalMs(1000L);
    dropPendingTide(counter);
    runTide(); // 1st consecutive empty: base
    dropPendingTide(counter);
    runTide(); // 2nd consecutive empty: 2x the (adjusted) base — ladder follows the base
    assertThat(scheduler.delaysMillis).containsExactly(500L, 1000L, 2000L);
  }

  @Test
  void adjust_movesTheBaseOfThePressureCompression() throws Exception {
    // deliveredKeys = 1 (well under the 20k threshold): the non-empty delay is
    // the base minus integer-truncated pressure — 1 key truncates to 0, so the
    // recorded delay IS the base. Monotonicity across two counters with the
    // same 1-key snapshot pins "pressure compresses FROM the adjusted base".
    RecordingScheduler schedulerA = new RecordingScheduler();
    WaveCounter counterA = new WaveCounter(ignored -> {}, schedulerA);
    try {
      counterA.afterPropertiesSet();
      counterA.count("k", 1);
      dropPendingTide(counterA);
      runTide();
      long baseDelayA = schedulerA.delaysMillis.get(schedulerA.delaysMillis.size() - 1);

      counter.afterPropertiesSet();
      counter.adjustDeliverIntervalMs(1000L);
      counter.count("k", 1);
      dropPendingTide(counter);
      runTide();
      long baseDelayB = scheduler.delaysMillis.get(scheduler.delaysMillis.size() - 1);

      assertThat(baseDelayA).isEqualTo(500L);
      assertThat(baseDelayB).isEqualTo(1000L);
    } finally {
      counterA.destroy();
    }
  }

  @Test
  void adjust_afterDestroy_isNoOp() throws Exception {
    counter.afterPropertiesSet();
    counter.destroy();
    counter.adjustDeliverIntervalMs(999L);
    assertThat(deliverIntervalMs(counter)).as("shutdown short-circuits").isEqualTo(500L);
  }
}
