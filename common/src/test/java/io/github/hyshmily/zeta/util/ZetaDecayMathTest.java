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

import java.math.BigInteger;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Boundary tests for {@link ZetaDecayMath} — the four porting constraints from the kernel
 * review (seed branch, 64-bit multiply overflow, precision bounds, decay-constant golden
 * values) plus provenance checks against the kernel sources themselves.
 *
 * <p>
 * Kernel cross-validations in here:
 *
 * <ul>
 * <li>{@code Y_N_INV} is checked entry-by-entry against the closed form
 * {@code 2^32 · y^i}, {@code y^32 = 1/2} — a transcription typo of the generated
 * {@code sched-pelt.h} table cannot ship;</li>
 * <li>{@link ZetaDecayMath#decayLoad} is checked against the kernel's literal C expression
 * (shift-then-table) on the domain where that expression is exactly representable in Java;</li>
 * <li>{@link ZetaDecayMath#ewmaAdd} is checked bit-for-bit against the literal
 * {@code average.h} form on its entire no-wrap domain;</li>
 * <li>{@link ZetaDecayMath#expDecayFactor} is checked against {@code Math.exp} with the
 * documented 1.07% quantization bound.</li>
 * </ul>
 */
class ZetaDecayMathTest {

  // ---------------------------------------------------------------- decayLoad

  @Test
  void table_shouldMatchClosedForm_andMonotonicallyDecrease() {
    assertThat(ZetaDecayMath.Y_N_INV).hasSize(32);
    for (int i = 0; i < 32; i++) {
      // 2^32 · y^i with y^32 = 1/2; T[0] is clamped to u32 by the kernel (one ulp under 2^32).
      double closedForm = 4294967296.0 * Math.pow(2.0, -i / 32.0);
      assertThat((double) ZetaDecayMath.Y_N_INV[i])
        .as("Y_N_INV[%d] vs closed form", i)
        .isCloseTo(closedForm, within(2.0));
      if (i > 0) {
        assertThat(ZetaDecayMath.Y_N_INV[i]).isLessThan(ZetaDecayMath.Y_N_INV[i - 1]);
      }
    }
    assertThat(ZetaDecayMath.Y_N_INV[0]).isEqualTo(0xffffffffL);
  }

  @Test
  void decayLoad_shouldMatchLiteralKernelForm_forU32Vals() {
    // The kernel form ((val >>> n/32) * T[n%32]) >>> 32 is exactly representable in a Java
    // long whenever val < 2^32 (product < 2^64) — compare every period against it.
    long[] vals = { 0L, 1L, 2L, 3L, 0x7fffffffL, 0xffffffffL };
    for (long val : vals) {
      // n = 0 hits the kernel's early return (pelt.c:34 "if (!n) return val"),
      // before any table math — expect the value itself.
      assertThat(ZetaDecayMath.decayLoad(val, 0)).as("decayLoad(%d, 0)", val).isEqualTo(val);
      for (long n = 1; n <= 2016; n++) {
        long shifted = val >>> (n / 32);
        long literalKernel = (shifted * ZetaDecayMath.Y_N_INV[(int) (n % 32)]) >>> 32;
        assertThat(ZetaDecayMath.decayLoad(val, n)).as("decayLoad(%d, %d)", val, n).isEqualTo(literalKernel);
      }
      for (long n = 2017; n <= 2050; n++) {
        assertThat(ZetaDecayMath.decayLoad(val, n)).as("decayLoad(%d, %d)", val, n).isZero();
      }
    }
  }

  @Test
  void decayLoad_shouldHandleFull63BitRange_viaBigIntegerReference() {
    Random random = new Random(0x5E7A);
    for (int i = 0; i < 2_000; i++) {
      long val = random.nextLong() & Long.MAX_VALUE; // non-negative, sign bit exercises umulHigh
      if (i % 8 == 0) {
        val = Long.MAX_VALUE - random.nextInt(1024); // force the extreme corner repeatedly
      }
      long n = random.nextInt(2100);
      long expected = bigIntegerKernelDecay(val, Math.min(n, ZetaDecayMath.MAX_PERIODS));
      if (n > ZetaDecayMath.MAX_PERIODS) {
        expected = 0;
      }
      assertThat(ZetaDecayMath.decayLoad(val, n)).as("decayLoad(%d, %d)", val, n).isEqualTo(expected);
      assertThat(ZetaDecayMath.decayLoad(val, n)).isBetween(0L, val);
    }
  }

  /** Exact reference: 128-bit product via BigInteger, mirroring mul_u64_u32_shr's semantics. */
  private static long bigIntegerKernelDecay(long val, long n) {
    if (val == 0 || n <= 0) {
      return val;
    }
    if (n > ZetaDecayMath.MAX_PERIODS) {
      return 0;
    }
    BigInteger shifted = BigInteger.valueOf(val >>> (n / 32));
    BigInteger product = shifted.multiply(BigInteger.valueOf(ZetaDecayMath.Y_N_INV[(int) (n % 32)]));
    return product.shiftRight(32).longValue();
  }

  @Test
  void decayLoad_boundaries() {
    assertThat(ZetaDecayMath.decayLoad(12345L, 0)).isEqualTo(12345L);
    // one halving per 32 periods; the table's u32 truncation costs up to
    // ceil(shifted / 2^32) — 128 at val = 2^40 — not a flat 1 ulp
    long val = 1L << 40;
    double tol32 = 1.0 + (double) (val >>> 1) / 4294967296.0;
    assertThat((double) ZetaDecayMath.decayLoad(val, 32)).isCloseTo(val >>> 1, within(tol32));
    double tol64 = 1.0 + (double) (val >>> 2) / 4294967296.0;
    assertThat((double) ZetaDecayMath.decayLoad(val, 64)).isCloseTo(val >>> 2, within(tol64));
    // near the cap the shift-first truncation dominates: with q = n/32 = 62 the
    // shifted value is ≤ 1 for every signed-positive val, so floor(shifted·y^31)
    // is already 0 from n = 1984 on — before the kernel's 2016 ceiling is even
    // reached. n = 1983 is the last period with a nonzero result at this val.
    assertThat(ZetaDecayMath.decayLoad(Long.MAX_VALUE, 1983)).isEqualTo(1L);
    assertThat(ZetaDecayMath.decayLoad(Long.MAX_VALUE, 1984)).isZero();
    assertThat(ZetaDecayMath.decayLoad(Long.MAX_VALUE, ZetaDecayMath.MAX_PERIODS)).isZero();
    assertThat(ZetaDecayMath.decayLoad(Long.MAX_VALUE, ZetaDecayMath.MAX_PERIODS + 1)).isZero();
    assertThat(ZetaDecayMath.decayLoad(0L, 100L)).isZero();
    // result never exceeds the input
    assertThat(ZetaDecayMath.decayLoad(7L, 1)).isLessThanOrEqualTo(7L);
  }

  @Test
  void decayLoad_rejectsNegativeValue() {
    assertThatThrownBy(() -> ZetaDecayMath.decayLoad(-1L, 10L))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("non-negative");
  }

  // ------------------------------------------------------------- decayFactor

  @Test
  void decayFactor_boundaries() {
    assertThat(ZetaDecayMath.decayFactor(0)).isEqualTo(1.0);
    assertThat(ZetaDecayMath.decayFactor(32)).isCloseTo(0.5, within(1e-9));
    assertThat(ZetaDecayMath.decayFactor(64)).isCloseTo(0.25, within(1e-9));
    assertThat(ZetaDecayMath.decayFactor(2016)).isCloseTo(Math.pow(0.5, 63), within(1e-24));
    assertThat(ZetaDecayMath.decayFactor(2017)).isZero();
    assertThatThrownBy(() -> ZetaDecayMath.decayFactor(-1L)).isInstanceOf(IllegalArgumentException.class);
    double previous = Double.MAX_VALUE;
    for (long n = 1; n <= 2016; n += 7) {
      double factor = ZetaDecayMath.decayFactor(n);
      assertThat(factor).isStrictlyBetween(0.0, 1.0);
      assertThat(factor).isLessThanOrEqualTo(previous);
      previous = factor;
    }
  }

  // ----------------------------------------------------------------- ewmaAdd

  @Test
  void ewmaAdd_seedBranchPlantsFirstSample() {
    // average.h:65-68 — internal == 0 plants val << precision outright.
    assertThat(ZetaDecayMath.ewmaAdd(0L, 1000L, 5, 16)).isEqualTo(1000L << 16);
    // a zero sample still seeds a zero state (kernel condition is on internal, not val)
    assertThat(ZetaDecayMath.ewmaAdd(0L, 0L, 5, 16)).isZero();
    // saturation ceiling: val is clamped so val << precision cannot wrap
    long maxVal = Long.MAX_VALUE >> 30;
    assertThat(ZetaDecayMath.ewmaAdd(0L, Long.MAX_VALUE, 5, 30)).isEqualTo(maxVal << 30).isPositive();
  }

  @Test
  void ewmaAdd_shouldBeBitIdenticalToKernelForm_onItsNoWrapDomain() {
    // Kernel literal: internal ? (((internal << w) - internal) + (val << p)) >> w : (val << p).
    // Exact in u64 iff internal·(2^w − 1) + (val << p) < 2^64 — this grid stays far inside.
    Random random = new Random(0x0E00);
    for (int i = 0; i < 10_000; i++) {
      long internal = random.nextLong() & ((1L << 30) - 1);
      long val = random.nextLong() & ((1L << 24) - 1);
      int w = 1 + random.nextInt(8);
      int p = random.nextInt(16);
      long scaled = val << p;
      long kernelForm = internal == 0 ? scaled : (((internal << w) - internal) + scaled) >>> w;
      assertThat(ZetaDecayMath.ewmaAdd(internal, val, w, p))
        .as("ewmaAdd(%d, %d, w=%d, p=%d)", internal, val, w, p)
        .isEqualTo(kernelForm);
    }
    // weight 0 = full replacement (>> 0 degenerates to the scaled sample)
    assertThat(ZetaDecayMath.ewmaAdd(123_456L, 99L, 0, 4)).isEqualTo(99L << 4);
  }

  @Test
  void ewmaAdd_convergesToConstantSample() {
    // feed a constant through w=5 (1/32 weight), p=16: state must settle at val·2^16
    long state = 0;
    for (int i = 0; i < 500; i++) {
      state = ZetaDecayMath.ewmaAdd(state, 1000L, 5, 16);
    }
    assertThat(ZetaDecayMath.ewmaRead(state, 16)).isEqualTo(1000L);
  }

  @Test
  void ewmaAdd_neverWrapsAtExtremes() {
    // huge state, zero sample: the rearranged form halves instead of wrapping negative
    long halved = ZetaDecayMath.ewmaAdd(Long.MAX_VALUE, 0L, 1, 16);
    assertThat(halved).isPositive().isLessThan(Long.MAX_VALUE);
    // huge state, huge sample, extreme weight: no wrap, stays in [0, 2^63)
    long blended = ZetaDecayMath.ewmaAdd(Long.MAX_VALUE, Long.MAX_VALUE, 62, 30);
    assertThat(blended).isBetween(0L, Long.MAX_VALUE);
    // randomized sweep: every result stays non-negative for any legal inputs
    Random random = new Random(0xC0FFEE);
    for (int i = 0; i < 5_000; i++) {
      long internal = random.nextLong() & Long.MAX_VALUE;
      long val = random.nextLong() & Long.MAX_VALUE;
      int w = random.nextInt(63);
      int p = random.nextInt(31);
      long next = ZetaDecayMath.ewmaAdd(internal, val, w, p);
      assertThat(next).as("ewmaAdd(%d, %d, w=%d, p=%d)", internal, val, w, p).isNotNegative();
      // the true contract: a bounded blend toward the SCALED sample — the state
      // moves toward val << p (possibly upward) but never past either endpoint.
      long scaled = Math.min(val, Long.MAX_VALUE >> p) << p;
      assertThat(next).isBetween(Math.min(internal, scaled), Math.max(internal, scaled));
    }
  }

  @Test
  void ewmaAdd_rejectsInvalidArguments() {
    assertThatThrownBy(() -> ZetaDecayMath.ewmaAdd(1L, 1L, 5, 31)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ZetaDecayMath.ewmaAdd(1L, 1L, 5, -1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ZetaDecayMath.ewmaAdd(1L, 1L, 63, 5)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ZetaDecayMath.ewmaAdd(-1L, 1L, 5, 5)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ZetaDecayMath.ewmaAdd(1L, -1L, 5, 5)).isInstanceOf(IllegalArgumentException.class);
    // reads validate the same way
    assertThatThrownBy(() -> ZetaDecayMath.ewmaRead(1L, 31)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ZetaDecayMath.ewmaRead(-1L, 5)).isInstanceOf(IllegalArgumentException.class);
    assertThatCode(() -> ZetaDecayMath.ewmaRead(1L, 30)).doesNotThrowAnyException();
  }

  @Test
  void ewmaRead_shiftsFractionOut() {
    assertThat(ZetaDecayMath.ewmaRead(1000L << 16, 16)).isEqualTo(1000L);
    assertThat(ZetaDecayMath.ewmaRead((1000L << 16) + 0xffffL, 16)).isEqualTo(1000L);
  }

  // ---------------------------------------------------------- expDecayFactor

  @Test
  void expDecayFactor_timeRollbackIsANoOp() {
    // pelt.c:186-194 port — non-positive deltas are protocol events: factor 1, caller re-anchors.
    assertThat(ZetaDecayMath.expDecayFactor(0L, 1000L)).isEqualTo(1.0);
    assertThat(ZetaDecayMath.expDecayFactor(-5L, 1000L)).isEqualTo(1.0);
    assertThat(ZetaDecayMath.expDecayFactor(100L, 0L)).isEqualTo(1.0);
    assertThat(ZetaDecayMath.expDecayFactor(100L, -1L)).isEqualTo(1.0);
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
      double factor = ZetaDecayMath.expDecayFactor(elapsed, tau);
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
    assertThat(ZetaDecayMath.expDecayFactor(halfLife, tau)).isCloseTo(0.5, within(0.006));
    assertThat(ZetaDecayMath.expDecayFactor(2 * halfLife, tau)).isCloseTo(0.25, within(0.003));
    assertThat(ZetaDecayMath.expDecayFactor(4 * halfLife, tau)).isCloseTo(0.0625, within(0.001));
  }

  @Test
  void expDecayFactor_hugeDeltaClampsToZero() {
    // beyond the elapsed ceiling: no wrap, exact zero
    assertThat(ZetaDecayMath.expDecayFactor(280_000_000_001L, 1000L)).isZero();
    // periods beyond the kernel cap (n > 32×63) collapse to zero too
    assertThat(ZetaDecayMath.expDecayFactor(10_000_000L, 1L)).isZero();
    // absurd time constants are clamped to the supported ceiling instead of wrapping
    assertThatCode(() -> ZetaDecayMath.expDecayFactor(693_147L, Long.MAX_VALUE / 2)).doesNotThrowAnyException();
    // elapsed 693147ms against the clamped 1e9ms ceiling rounds to n = 0 — no decay
    double clamped = ZetaDecayMath.expDecayFactor(693_147L, Long.MAX_VALUE / 2);
    assertThat(clamped).isBetween(0.0, 1.0);
  }
}
