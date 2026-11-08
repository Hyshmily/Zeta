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
package io.github.hyshmily.zeta.counter;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.counter.WaveCounterSchedulingTest.RecordingScheduler;
import java.lang.reflect.Field;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * {@code adjustDeliverIntervalMs} (design decisions: reporting): the first
 * call transfers cadence ownership from the wave pacer to the feed-loop tuner.
 * After it, a non-empty tide arms the base verbatim (no backlog compression —
 * the controller must measure the input it applied) while the empty-tide
 * ladder still stretches an idle cycle (cap 2x the base) and any non-empty
 * tide breaks the streak. A sub-floor request is clamped up so the
 * construction-time invariant (the governor's flood-rate normalization
 * divides by the cadence) is never broken by a later adjustment.
 */
class WaveCounterDeliverIntervalTest {

  private RecordingScheduler scheduler;
  private WaveCounter counter;

  @BeforeEach
  void setUp() {
    scheduler = new RecordingScheduler();
    counter = new WaveCounter((ignored, windowMs) -> {}, scheduler);
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
    runTide(counter);
  }

  private static void runTide(WaveCounter c) throws Exception {
    java.lang.reflect.Method tide = WaveCounter.class.getDeclaredMethod("tide");
    tide.setAccessible(true);
    tide.invoke(c);
  }

  private static long lastDelay(RecordingScheduler scheduler) {
    return scheduler.delaysMillis.get(scheduler.delaysMillis.size() - 1);
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
  void adjust_transfersCadenceOwnershipFromTheBacklogPacer() throws Exception {
    // Without an external owner the backlog compresses the delay to the floor.
    RecordingScheduler schedulerA = new RecordingScheduler();
    WaveCounter compressionA = new WaveCounter((m, w) -> {}, schedulerA);
    try {
      compressionA.afterPropertiesSet();
      for (int i = 0; i < 20_000; i++) {
        compressionA.count("k" + i, 1);
      }
      dropPendingTide(compressionA);
      runTide(compressionA);
      assertThat(lastDelay(schedulerA)).as("pacer compression reaches the floor").isEqualTo(50L);
    } finally {
      compressionA.destroy();
    }

    // Once the loop owns the cadence, the same backlog uses its base verbatim.
    counter.afterPropertiesSet();
    counter.adjustDeliverIntervalMs(1000L);
    for (int i = 0; i < 20_000; i++) {
      counter.count("k" + i, 1);
    }
    dropPendingTide(counter);
    runTide();
    assertThat(lastDelay(scheduler)).as("external owner's base is the delay").isEqualTo(1000L);
  }

  @Test
  void externalCadence_nonEmptyTideBreaksTheEmptyStreak() throws Exception {
    counter.afterPropertiesSet();
    counter.adjustDeliverIntervalMs(1000L);
    dropPendingTide(counter);
    runTide(); // 1st empty: base
    dropPendingTide(counter);
    runTide(); // 2nd empty: 2x base
    counter.count("k", 1);
    dropPendingTide(counter);
    runTide(); // non-empty: ladder reset, base verbatim
    dropPendingTide(counter);
    runTide(); // empty again: back to 1x base
    assertThat(scheduler.delaysMillis).containsExactly(500L, 1000L, 2000L, 1000L, 1000L);
  }

  @Test
  void adjust_afterDestroy_isNoOp() throws Exception {
    counter.afterPropertiesSet();
    counter.destroy();
    counter.adjustDeliverIntervalMs(999L);
    assertThat(deliverIntervalMs(counter)).as("shutdown short-circuits").isEqualTo(500L);
  }
}
