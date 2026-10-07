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

import io.github.hyshmily.zeta.hotkeydetector.doublebuffer.WaveCounter;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.HeavyKeeper;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Deterministic detection-capability harness for the buffered detection path.
 *
 * <p>Wires the production chain ({@code WaveCounter -> HeavyKeeper.addDirect}) with the
 * production operating point ({@code topK=100 / width=50k / depth=5 / decay=0.92 /
 * minCount=10}, {@code windowCount=3}) and drives tides deterministically via the private
 * {@code tide()} method (same reflection pattern as {@code WaveCounterSimulatorTest}) —
 * never wall-clock, never JMH. Workload is the canonical single-key step on top of
 * high-cardinality cold churn: burst key {@code 2000} counts/tide from tide 5, background
 * {@code 5000} cold samples/tide drawn from a 20k pool with a seed-fixed {@code Random(42)},
 * 20 tides total.
 *
 * <p>SLO under test: the burst key is promoted within 2 burst tides, holds, and ranks
 * first despite 5000:1 cold camouflage. TopK membership itself is sticky by design
 * (recurring cold keys re-raise their sketch estimate every tide, so a 100-slot TopK
 * under churn is always full) — rank, not occupancy, is the false-positive signal.
 */
class HeavyKeeperBatchRecallTest {

  private static final String BURST_KEY = "burst";
  private static final int BURST_PER_TIDE = 2000;
  private static final int SMALL_BURST_PER_TIDE = 200;
  private static final int FLOOR_BURST_PER_TIDE = 15;
  private static final int BURST_START_TIDE = 5;
  private static final int TOTAL_TIDES = 20;
  private static final int COLD_SAMPLES_PER_TIDE = 5000;
  private static final int COLD_POOL = 20_000;

  /**
   * Verifies the canonical burst promotes within 2 burst tides, holds, and outranks
   * the cold background.
   */
  @Test
  void canonicalBurst_promotesWithinTwoTidesAndHolds() {
    Drive drive = driveBurst(BURST_PER_TIDE);

    assertThat(drive.promotedAt())
      .as("burst must promote within 2 burst tides (tides %d-%d)", BURST_START_TIDE, BURST_START_TIDE + 1)
      .isBetween(BURST_START_TIDE, BURST_START_TIDE + 1);
    assertThat(drive.keeper().contains(BURST_KEY)).as("burst holds after 20 tides").isTrue();
    assertThat(drive.keeper().list().get(0).key())
      .as("burst outranks 5000:1 cold camouflage (2000/tide vs ~1/tide per cold key)")
      .isEqualTo(BURST_KEY);
  }

  /**
   * Verifies a 10x smaller burst still promotes (second batch magnitude), with a looser
   * latency bound — it carries less evidence per tide than the canonical burst.
   */
  @Test
  void smallBurst_promotesWithinSixTides() {
    Drive drive = driveBurst(SMALL_BURST_PER_TIDE);

    assertThat(drive.promotedAt())
      .as("small burst (200/tide) must promote within 6 burst tides")
      .isBetween(BURST_START_TIDE, BURST_START_TIDE + 5);
    assertThat(drive.keeper().contains(BURST_KEY)).as("small burst holds after 20 tides").isTrue();
  }

  /**
   * Boundary probe: a burst at 1.5x {@code minCount} sits where collision decay can
   * actually erase the admission margin. Same 2-tide SLO — a miss here locates the
   * floor where batch-aware decay starts to matter, instead of guessing it.
   */
  @Test
  void floorBurst_promotesWithinTwoTides() {
    Drive drive = driveBurst(FLOOR_BURST_PER_TIDE);

    assertThat(drive.promotedAt())
      .as("floor burst (15/tide, 1.5x minCount) must promote within 2 burst tides")
      .isBetween(BURST_START_TIDE, BURST_START_TIDE + 1);
    assertThat(drive.keeper().contains(BURST_KEY)).as("floor burst holds after 20 tides").isTrue();
  }

  /**
   * Verifies cold churn alone promotes nothing burst-like: with no burst key fed, the TopK
   * top counts stay at noise level instead of pinning a fake hot key.
   */
  @Test
  void coldOnly_promotesNothingStable() {
    Drive drive = driveBurst(0); // non-positive delta is a no-op in WaveCounter.count

    List<Item> top = drive.keeper().listTopN(5);
    assertThat(top)
      .as("cold-only top counts stay at noise level (each cold key seen ~5x in 20 tides)")
      .allMatch(item -> item.count() < BURST_PER_TIDE);
  }

  // ── Driver ──

  /** One driven workload: the keeper under test plus the tide the burst promoted on. */
  private record Drive(HeavyKeeper keeper, int promotedAt) {}

  /**
   * Drives the canonical workload with {@code perTide} burst counts from
   * {@link #BURST_START_TIDE} over a cold-churn background. Prints the promotion tide
   * before returning so the evidence survives even when a downstream assertion fails.
   *
   * @param perTide burst counts per tide ({@code 0} disables the burst — cold-only)
   * @return the keeper and the first tide containing the burst key ({@code -1} if never)
   */
  private static Drive driveBurst(int perTide) {
    HeavyKeeper keeper = productionKeeper();
    WaveCounter counter = new WaveCounter(batch -> {
      keeper.addDirect(batch);
    });
    Random random = new Random(42);

    int promotedAt = -1;
    for (int t = 0; t < TOTAL_TIDES; t++) {
      feedCold(counter, random);
      if (t >= BURST_START_TIDE) {
        counter.count(BURST_KEY, perTide);
      }
      invokeTide(counter);
      if (promotedAt < 0 && keeper.contains(BURST_KEY)) {
        promotedAt = t;
      }
    }
    System.out.println("burst " + perTide + "/tide: promotedAt tide " + promotedAt);
    return new Drive(keeper, promotedAt);
  }

  /** Production operating point (see {@code ZetaProperties} defaults). */
  private static HeavyKeeper productionKeeper() {
    return new HeavyKeeper(100, 50_000, 5, 0.92, 10, 10_000, 3, true);
  }

  /** Feeds one tide of cold background traffic sampled from the shared pool. */
  private static void feedCold(WaveCounter counter, Random random) {
    for (int i = 0; i < COLD_SAMPLES_PER_TIDE; i++) {
      counter.count("cold-" + random.nextInt(COLD_POOL), 1);
    }
  }

  /** Drives one deterministic delivery tide (never wall-clock scheduled). */
  private static void invokeTide(WaveCounter counter) {
    try {
      Method tide = WaveCounter.class.getDeclaredMethod("tide");
      tide.setAccessible(true);
      tide.invoke(counter);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}
