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
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
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
 *
 * <p>Extended suite: burst magnitudes (2000/200/15 per tide), capacity overflow (150
 * hot keys for 100 slots), flat-distribution stability (200 equal keys), and drift
 * rotation on a production-aged sketch (90 tides, fading every 40) — the drift test
 * pins a structural blind spot (minority lockout under saturation, documented on the
 * test) rather than a universal SLO.
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
   * Capacity overflow: 150 hot keys at 500/tide contend for 100 TopK slots over cold
   * churn. The hot mass must displace the noise — nearly every slot stays hot and no
   * cold key reaches the top ranks.
   */
  @Test
  void capacityOverflow_hotMassDisplacesCold() {
    Chain chain = newChain();
    Random random = new Random(42);
    int hotKeys = 150;
    for (int t = 0; t < TOTAL_TIDES; t++) {
      feedCold(chain.counter(), random);
      for (int i = 0; i < hotKeys; i++) {
        chain.counter().count("hot-" + i, 500);
      }
      invokeTide(chain.counter());
    }

    List<String> members = chain.keeper().list().stream().map(Item::key).toList();
    long hotMembers = members.stream().filter(k -> k.startsWith("hot-")).count();
    System.out.println("capacityOverflow: hotMembers=" + hotMembers + "/100");
    assertThat(hotMembers).as("150-key hot mass must occupy nearly all TopK slots").isGreaterThanOrEqualTo(95);
    assertThat(members.stream().limit(10).toList()).as("top ranks are never cold").allMatch(k -> k.startsWith("hot-"));
  }

  /**
   * Flat distribution: 200 keys at an identical 50/tide with no churn background. Every
   * key earns equally, so the hot set must stay put instead of rotating a random sample
   * every tide.
   */
  @Test
  void flatDistribution_hotSetStaysStable() {
    Chain chain = newChain();
    int flatKeys = 200;
    Set<String> early = new HashSet<>();
    for (int t = 0; t < TOTAL_TIDES; t++) {
      for (int i = 0; i < flatKeys; i++) {
        chain.counter().count("flat-" + i, 50);
      }
      invokeTide(chain.counter());
      if (t == TOTAL_TIDES / 2 - 1) {
        early.addAll(memberKeys(chain.keeper()));
      }
    }

    Set<String> late = memberKeys(chain.keeper());
    long overlap = late.stream().filter(early::contains).count();
    System.out.println("flatDistribution: overlap=" + overlap + "/100");
    assertThat(overlap).as("flat hot set must stay stable, not rotate").isGreaterThanOrEqualTo(80);
  }

  /**
   * Drift rotation on a production-aged sketch: 10 A-keys burn hot for 41 tides with
   * production-cadence fading (every 40 tides), then go cold while 10 B-keys ignite.
   *
   * <p>Regression gate for the size-aware takeover (ADR-0092): before it, 2 of 10
   * rotation keys never admitted (est=0 across the run, with or without fading) —
   * every row they hashed to stayed occupied with a small sum, and their own
   * per-tide collisions pinned those blockers at an equilibrium far below admission.
   * A strictly bigger batch now takes the slot outright, so all 10 admit promptly
   * and hold. The exact 10/10 pins the fix — any weakening trips the gate.
   */
  @Test
  void driftRotation_newHeatDetectedDespiteIncumbents() {
    Chain chain = newChain();
    Random random = new Random(42);
    int rotationTide = 41;
    int totalTides = 90;
    for (int t = 0; t < rotationTide; t++) {
      feedCold(chain.counter(), random);
      for (int i = 0; i < 10; i++) {
        chain.counter().count("driftA-" + i, 1000);
      }
      invokeTide(chain.counter());
      if ((t + 1) % 40 == 0) {
        chain.keeper().fading();
      }
    }
    assertThat(memberKeys(chain.keeper())).as("phase-1 heat established").contains("driftA-0", "driftA-9");

    int promptCount = -1;
    for (int t = rotationTide; t < totalTides; t++) {
      feedCold(chain.counter(), random);
      for (int i = 0; i < 10; i++) {
        chain.counter().count("driftB-" + i, 1000);
      }
      invokeTide(chain.counter());
      if ((t + 1) % 40 == 0) {
        chain.keeper().fading();
      }
      if (t == rotationTide + 2) {
        promptCount = countPresent(chain.keeper(), "driftB-", 10);
      }
    }
    int finalCount = countPresent(chain.keeper(), "driftB-", 10);

    System.out.println("driftRotation: prompt=" + promptCount + "/10 final=" + finalCount + "/10");
    for (int i = 0; i < 10; i++) {
      String k = "driftB-" + i;
      System.out.println(
        "driftB-" + i + ": present=" + chain.keeper().contains(k) + " est=" + chain.keeper().estimatedCount(k)
      );
    }
    assertThat(promptCount)
      .as("rotated-in heat must evict into the aged TopK within 3 tides")
      .isEqualTo(10);
    assertThat(finalCount).as("admitted rotation heat holds (never evicted by noise)").isEqualTo(10);
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
    Chain chain = newChain();
    HeavyKeeper keeper = chain.keeper();
    WaveCounter counter = chain.counter();
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

  // ── Chain ──

  /** One production-wired detection chain under test. */
  private record Chain(HeavyKeeper keeper, WaveCounter counter) {}

  /** Wires {@code WaveCounter -> HeavyKeeper.addDirect} at the production operating point. */
  private static Chain newChain() {
    HeavyKeeper keeper = productionKeeper();
    return new Chain(keeper, new WaveCounter(batch -> {
      keeper.addDirect(batch);
    }));
  }

  /** Production operating point (see {@code ZetaProperties} defaults). */
  private static HeavyKeeper productionKeeper() {
    return new HeavyKeeper(100, 50_000, 5, 0.92, 10, 10_000, 3, true);
  }

  /** Snapshots current TopK membership as a key set. */
  private static Set<String> memberKeys(HeavyKeeper keeper) {
    Set<String> keys = new HashSet<>();
    for (Item item : keeper.list()) {
      keys.add(item.key());
    }
    return keys;
  }

  /** Counts how many keys of a {@code prefix + i} family the keeper holds. */
  private static int countPresent(HeavyKeeper keeper, String prefix, int count) {
    int present = 0;
    for (int i = 0; i < count; i++) {
      if (keeper.contains(prefix + i)) {
        present++;
      }
    }
    return present;
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
