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
package io.github.hyshmily.zeta.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link FastMath} — the pure-function math families in one class:
 *
 * <ul>
 * <li><b>Kernel decay math</b> — provenance checks of the PELT half-life table
 * against its closed form ({@code 2^32 · y^i}, {@code y^32 = 1/2}; a transcription
 * typo of the generated {@code sched-pelt.h} table cannot ship), and the decay
 * factor against golden half-life values plus a {@code Math.exp}
 * cross-validation with the documented 1.07% quantization bound;</li>
 * <li><b>FastRange</b> — RocksDB {@code util/fastrange.h} property and known-value
 * tests;</li>
 * <li><b>Power-of-two alignment</b> — boundary tests including the {@code v = 1}
 * identity (a power of two maps to itself, never to 0).</li>
 * </ul>
 */
class FastMathTest {

  // ------------------------------------------------------------ PELT table

  @Test
  void table_shouldMatchClosedForm_andMonotonicallyDecrease() {
    assertThat(FastMath.Y_N_INV).hasSize(32);
    for (int i = 0; i < 32; i++) {
      // 2^32 · y^i with y^32 = 1/2; T[0] is clamped to u32 by the kernel (one ulp under 2^32).
      double closedForm = 4294967296.0 * Math.pow(2.0, -i / 32.0);
      assertThat((double) FastMath.Y_N_INV[i])
        .as("Y_N_INV[%d] vs closed form", i)
        .isCloseTo(closedForm, within(2.0));
      if (i > 0) {
        assertThat(FastMath.Y_N_INV[i]).isLessThan(FastMath.Y_N_INV[i - 1]);
      }
    }
    assertThat(FastMath.Y_N_INV[0]).isEqualTo(0xffffffffL);
  }

  // ------------------------------------------------------------- decayFactor

  @Test
  void decayFactor_boundaries() {
    assertThat(FastMath.decayFactor(0)).isEqualTo(1.0);
    assertThat(FastMath.decayFactor(32)).isCloseTo(0.5, within(1e-9));
    assertThat(FastMath.decayFactor(64)).isCloseTo(0.25, within(1e-9));
    assertThat(FastMath.decayFactor(2016)).isCloseTo(Math.pow(0.5, 63), within(1e-24));
    assertThat(FastMath.decayFactor(2017)).isZero();
    assertThatThrownBy(() -> FastMath.decayFactor(-1L)).isInstanceOf(IllegalArgumentException.class);
    double previous = Double.MAX_VALUE;
    for (long n = 1; n <= 2016; n += 7) {
      double factor = FastMath.decayFactor(n);
      assertThat(factor).isStrictlyBetween(0.0, 1.0);
      assertThat(factor).isLessThanOrEqualTo(previous);
      previous = factor;
    }
  }

  // ---------------------------------------------------------- expDecayFactor

  @Test
  void expDecayFactor_timeRollbackIsANoOp() {
    // pelt.c:186-194 port — non-positive deltas are protocol events: factor 1, caller re-anchors.
    assertThat(FastMath.expDecayFactor(0L, 1000L)).isEqualTo(1.0);
    assertThat(FastMath.expDecayFactor(-5L, 1000L)).isEqualTo(1.0);
    assertThat(FastMath.expDecayFactor(100L, 0L)).isEqualTo(1.0);
    assertThat(FastMath.expDecayFactor(100L, -1L)).isEqualTo(1.0);
  }

  @Test
  void expDecayFactor_matchesExp_withinQuantizationBound() {
    long tau = 1000L;
    double[] ratios = {
      0.001,
      0.01,
      0.05,
      0.1,
      0.25,
      0.5,
      0.693147,
      1.0,
      1.5,
      2.0,
      3.7,
      5.0,
      8.0,
      13.0,
      21.0,
      34.0,
      43.0,
    };
    double previous = Double.MAX_VALUE;
    for (double ratio : ratios) {
      long elapsed = Math.round(ratio * tau);
      double factor = FastMath.expDecayFactor(elapsed, tau);
      double expected = Math.exp(-ratio);
      // the quantization error is multiplicative (n rounded within ±0.5 periods,
      // so the factor is off by a factor y^±0.5) — a relative bound applies at
      // every magnitude, not just for large factors
      assertThat(factor).isCloseTo(expected, within(expected * 0.012));
      assertThat(factor).as("monotone at ratio %f", ratio).isLessThanOrEqualTo(previous);
      previous = factor;
    }
  }

  @Test
  void expDecayFactor_halfLifeIsExact() {
    long tau = 1000L;
    long halfLife = Math.round(tau * Math.log(2)); // 693
    assertThat(FastMath.expDecayFactor(halfLife, tau)).isCloseTo(0.5, within(0.006));
    assertThat(FastMath.expDecayFactor(2 * halfLife, tau)).isCloseTo(0.25, within(0.003));
    assertThat(FastMath.expDecayFactor(4 * halfLife, tau)).isCloseTo(0.0625, within(0.001));
  }

  @Test
  void expDecayFactor_hugeDeltaClampsToZero() {
    // beyond the elapsed ceiling: no wrap, exact zero
    assertThat(FastMath.expDecayFactor(280_000_000_001L, 1000L)).isZero();
    // periods beyond the kernel cap (n > 32×63) collapse to zero too
    assertThat(FastMath.expDecayFactor(10_000_000L, 1L)).isZero();
    // absurd time constants are clamped to the supported ceiling instead of wrapping
    assertThatCode(() -> FastMath.expDecayFactor(693_147L, Long.MAX_VALUE / 2)).doesNotThrowAnyException();
    // elapsed 693147ms against the clamped 1e9ms ceiling rounds to n = 0 — no decay
    double clamped = FastMath.expDecayFactor(693_147L, Long.MAX_VALUE / 2);
    assertThat(clamped).isBetween(0.0, 1.0);
  }

  // ------------------------------------------------- fastRange32 (fastrange.h)

  @Test
  void fastRange32_knownValues() {
    assertThat(FastMath.fastRange32(0, 5)).isZero();
    // uint32 max × 5 = 0x4_FFFF_FFFF... high half = 4.
    assertThat(FastMath.fastRange32(-1, 5)).isEqualTo(4);
    // uint32 max maps to range-1 for any range: the product's high half is
    // (range-1) + (2^32-1)/2^32 → truncated to range-1.
    assertThat(FastMath.fastRange32(-1, 1024)).isEqualTo(1023);
  }

  @Test
  void fastRange32_resultAlwaysWithinRange() {
    Random random = new Random(42);
    for (int i = 0; i < 100_000; i++) {
      int hash = random.nextInt();
      int bucket = FastMath.fastRange32(hash, 967);
      assertThat(bucket).as("hash=%s", hash).isBetween(0, 966);
    }
  }

  @Test
  void fastRange32_fullRangeCoverage_forNonPowerOfTwoWidth() {
    Random random = new Random(7);
    int range = 967;
    boolean[] hit = new boolean[range];
    for (int i = 0; i < 100_000; i++) {
      hit[FastMath.fastRange32(random.nextInt(), range)] = true;
    }
    int hits = 0;
    for (boolean b : hit) {
      if (b) {
        hits++;
      }
    }
    assertThat(hits).isEqualTo(range);
  }

  @Test
  void fastRange32_distribution_bucketCountsBalanced_withinFourSigma() {
    Random random = new Random(1234);
    int range = 1023;
    int samples = 1_000_000;
    int[] counts = new int[range];
    for (int i = 0; i < samples; i++) {
      counts[FastMath.fastRange32(random.nextInt(), range)]++;
    }
    long expected = samples / range;
    // Max over `range` buckets of a binomial count deviates ~3.2σ (σ≈31 here);
    // a 4σ bound (expected/8) tolerates that order statistic while still
    // failing a skewed mapping (a modulo of the low bits skews far worse).
    long tolerance = expected / 8;
    for (int count : counts) {
      assertThat(Math.abs(count - expected)).as("bucket deviation").isLessThan(tolerance);
    }
  }

  @Test
  void fastRange32_rangeOne_alwaysZero() {
    for (int i = 0; i < 1_000; i++) {
      assertThat(FastMath.fastRange32(new Random(i).nextInt(), 1)).isZero();
    }
  }

  // ---------------------------------------------------------------- pow2Ceil

  @Test
  void pow2Ceil_knownValues() {
    // powers of two are identities — including 1 (the highestOneBit(0) corner
    // that a naive unconditional form maps to 0)
    assertThat(FastMath.pow2Ceil(1)).isEqualTo(1);
    assertThat(FastMath.pow2Ceil(2)).isEqualTo(2);
    assertThat(FastMath.pow2Ceil(1024)).isEqualTo(1024);
    // non-powers round up to the next power of two
    assertThat(FastMath.pow2Ceil(3)).isEqualTo(4);
    assertThat(FastMath.pow2Ceil(5)).isEqualTo(8);
    assertThat(FastMath.pow2Ceil(1000)).isEqualTo(1024);
    assertThat(FastMath.pow2Ceil(1025)).isEqualTo(2048);
  }
}
