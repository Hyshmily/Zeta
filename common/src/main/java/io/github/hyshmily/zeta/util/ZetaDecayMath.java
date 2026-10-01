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

import io.github.hyshmily.zeta.Internal;

/**
 * Fixed-point decay and EWMA primitives ported from the Linux kernel — the tool layer
 * behind every "lazy decay" signal (decay on read by an arbitrary elapsed time, no
 * background sweeper, O(1) regardless of the gap).
 *
 * <p>
 * Two kernel mechanisms, one class:
 *
 * <ul>
 * <li><b>PELT table decay</b> ({@code kernel/sched/pelt.c}, local snapshot {@code pelt.c:32-55}):
 * approximate {@code val * y^n} with {@code y^32 = 1/2} in constant time by splitting
 * {@code y^n = 2^-(n/32) × y^(n%32)} — the whole periods are an integer shift, the remainder
 * indexes a Q32 table ({@code runnable_avg_yN_inv}, generated {@code kernel/sched/sched-pelt.h}).
 * {@code n > 32×63} decays to exactly zero ({@code pelt.c:36-37}): {@code y^2016 = 2^-63}
 * exhausts 64-bit resolution anyway. This is the shape every "catch up on the decay I missed
 * since the last touch" path should take — the table lookup replaces both the per-period loop
 * and the floating-point {@code Math.exp}.</li>
 * <li><b>Integer EWMA</b> ({@code include/linux/average.h}, local snapshot {@code average.h:56-68}):
 * {@code new = ((internal << w) - internal + (val << p)) >> w} with the average held in a
 * single long scaled by {@code 2^p} — no floating point, no division, atomicity is a single
 * word. Two details the one-line formula hides:
 * <ul>
 * <li>the <b>seed branch</b> ({@code average.h:65-68}): a zero state plants the first sample
 * directly ({@code val << p}) instead of crawling up from zero for ~0.7·w updates — callers
 * that pre-seed state (like {@code BbrRateLimiterImpl}'s {@code maxPassCache = 1}) are
 * exploiting the same fix from the other side;</li>
 * <li>the <b>wrap hazard</b>: the kernel form needs {@code internal·(2^w − 1) + (val << p)}
 * to stay inside 64 bits. {@link #ewmaAdd} uses the algebraically identical rearrangement
 * {@code internal + ((val << p) - internal) >> w}, which Java's floor-shift evaluates exactly
 * for <em>any</em> non-negative operands — bit-identical to the kernel wherever the kernel
 * form does not wrap, correct beyond it.</li>
 * </ul>
 * </li>
 * </ul>
 *
 * <p>
 * <b>Time-rollback and huge-delta conventions are built in</b> (the kernel's
 * {@code pelt.c:186-194} guard, ported): a non-positive elapsed is treated as a protocol
 * event, not data — the factor is {@code 1.0} (no decay) and the <em>caller</em> re-anchors
 * its timestamp, exactly the kernel's {@code last_update_time = now; return 0}. Zeta callers
 * on {@link TimeSource#monotonicMillis()} never produce negative deltas anyway; the clamp
 * protects wall-clock stragglers. Huge positive deltas clamp to the decay table's own limit
 * ({@link #MAX_PERIODS}) instead of shifting by a garbage count. All periods and factors are
 * pure functions — unit-tested against the kernel table and {@code Math.exp} cross-validation
 * (see {@code ZetaDecayMathTest}).
 */
@Internal
public final class ZetaDecayMath {

  /** One halving every this many periods: {@code y^32 = 1/2} ({@code pelt.c} {@code LOAD_AVG_PERIOD}). */
  private static final int PERIOD_SHIFT = 5;
  private static final int PERIOD_MASK = (1 << PERIOD_SHIFT) - 1;

  /**
   * Kernel cap: periods beyond this return an exact zero ({@code pelt.c:36-37},
   * {@code n > LOAD_AVG_PERIOD * 63}). {@code y^2016 = 2^-63} — below 64-bit resolution, and
   * far below any signal resolution for the double APIs too.
   */
  public static final long MAX_PERIODS = 32L * 63;

  /** ln 2 as fixed point 693147/10^6 (relative error 1.5e-7, absorbed by the period rounding). */
  private static final long LN2_NUM = 693_147L;
  private static final long LN2_DEN = 1_000_000L;

  /**
   * Supported time-constant ceiling, ~11.6 days in ms. Only exists to keep
   * {@code tc * LN2_NUM} and the rounding addition inside 64 bits; real Zeta time constants
   * (window spans, EMA half-lives) are orders of magnitude below it. Larger values are clamped.
   */
  private static final long MAX_TIME_CONSTANT_MS = 1_000_000_000L;

  /**
   * Elapsed-ms ceiling for {@link #expDecayFactor}. Chosen as the largest value whose product
   * with 32·10^6 stays inside {@code Long.MAX_VALUE} (so the period computation below cannot
   * wrap); at this gap the exact factor is {@code e^-280 ≈ 1e-122} for the largest supported
   * time constant — zero for every signal purpose.
   */
  private static final long MAX_ELAPSED_MS = 280_000_000_000L;

  /**
   * {@code runnable_avg_yN_inv} verbatim from the generated kernel header
   * ({@code kernel/sched/sched-pelt.h}, "Generated by Documentation/scheduler/sched-pelt;
   * do not modify"): {@code T[i] = round(2^32 · y^i)} with {@code y^32 = 1/2}, stored as u32 —
   * hence {@code T[0] = 0xffffffff}, one ulp under {@code 2^32}. {@code ZetaDecayMathTest}
   * cross-checks every entry against the closed form, so a transcription typo cannot ship.
   */
  static final long[] Y_N_INV = {
    0xffffffffL,
    0xfa83b2daL,
    0xf5257d14L,
    0xefe4b99aL,
    0xeac0c6e6L,
    0xe5b906e6L,
    0xe0ccdeebL,
    0xdbfbb796L,
    0xd744fcc9L,
    0xd2a81d91L,
    0xce248c14L,
    0xc9b9bd85L,
    0xc5672a10L,
    0xc12c4cc9L,
    0xbd08a39eL,
    0xb8fbaf46L,
    0xb504f333L,
    0xb123f581L,
    0xad583ee9L,
    0xa9a15ab4L,
    0xa5fed6a9L,
    0xa2704302L,
    0x9ef5325fL,
    0x9b8d39b9L,
    0x9837f050L,
    0x94f4efa8L,
    0x91c3d373L,
    0x8ea4398aL,
    0x8b95c1e3L,
    0x88980e80L,
    0x85aac367L,
    0x82cd8698L,
  };

  /**
   * The same 32 factors as doubles, {@code Y_N_INV[i] / 2^32}, precomputed at class init.
   * Dividing an integer-valued double by {@code 2^32} is exact (a pure exponent
   * decrement), so the precomputed entry is bit-identical to computing
   * {@code Y_N_INV[i] / 4294967296.0} per call — this just replaces that per-call
   * long→double conversion plus division with one array load on the decay path.
   */
  private static final double[] Y_N_INV_D = new double[Y_N_INV.length];

  static {
    for (int i = 0; i < Y_N_INV.length; i++) {
      Y_N_INV_D[i] = Y_N_INV[i] / 4294967296.0;
    }
  }

  private ZetaDecayMath() {}

  /**
   * PELT {@code decay_load(val, n)} ({@code pelt.c:32-55}): {@code val · y^n} with
   * {@code y^32 = 1/2}, in O(1) for any {@code n}.
   *
   * <p>
   * Mirror of the kernel's order of operations — whole periods first as an unsigned shift
   * ({@code pelt.c:49-52}), then the remainder through the Q32 table
   * ({@code mul_u64_u32_shr}, {@code pelt.c:54}). The 64×32 product is formed through a
   * 128-bit intermediate (see {@link #umulHigh}), so {@code val} may use the full
   * {@code [0, 2^63)} range — fixed-point state scaled by {@code 2^p} routinely exceeds
   * {@code 2^31} and needs no special-casing.
   *
   * @param val the value to decay; must be non-negative (a negative count is caller corruption
   *            and fails fast — the kernel's u64 simply has no negative domain)
   * @param n   elapsed periods; zero (or a defensively-clamped negative — elapsed values are
   *            guarded non-negative by the monotonic-axis convention, and the kernel early-returns
   *            on zero anyway) returns {@code val} unchanged; beyond {@link #MAX_PERIODS}
   *            returns {@code 0}
   * @return the decayed value, always in {@code [0, val]}
   */
  public static long decayLoad(long val, long n) {
    if (val < 0) {
      throw new IllegalArgumentException("decayLoad value must be non-negative: " + val);
    }
    if (n <= 0) {
      // pelt.c:34 "if (!n) return val" — and a negative period clamps to the same no-op.
      return val;
    }
    if (n > MAX_PERIODS) {
      // pelt.c:36-37: beyond 63 halvings the u64 result is exactly zero.
      return 0;
    }
    long shifted = val >>> (n >> PERIOD_SHIFT); // pelt.c:49-52: y^(n/32) as an integer shift
    long t = Y_N_INV[(int) (n & PERIOD_MASK)]; // pelt.c:54: y^(n%32) as a Q32 table lookup
    long hi = umulHigh(shifted, t);
    long lo = shifted * t; // wrapping low word — the unsigned product's low 64 bits
    // (shifted * t) >>> 32 with the 128-bit product split; hi < 2^32 because the product
    // is < 2^96, so the composition cannot wrap.
    return (hi << 32) | (lo >>> 32);
  }

  /**
   * {@code y^n} as a double factor for floating-point lazy-decay state — the same PELT
   * {@code y^32 = 1/2} table, read side. {@code n = 0} yields exactly {@code 1.0};
   * {@code n >} {@link #MAX_PERIODS} yields exactly {@code 0.0} (the kernel's u64 cap — for
   * doubles {@code y^2017 ≈ 9e-20} would still be representable, but keeping one cap across
   * both APIs makes "decayed to nothing" mean the same thing everywhere).
   *
   * @param n elapsed periods, {@code >= 0}
   * @return the decay factor in {@code [0, 1]}
   */
  public static double decayFactor(long n) {
    if (n < 0) {
      throw new IllegalArgumentException("decayFactor period must be non-negative: " + n);
    }
    if (n == 0) {
      return 1.0;
    }
    if (n > MAX_PERIODS) {
      return 0.0;
    }
    // y^n = 2^-(n/32) · y^(n%32): one table load, one scalb — no exp, no pow, no division.
    return Math.scalb(Y_N_INV_D[(int) (n & PERIOD_MASK)], -(int) (n >> PERIOD_SHIFT));
  }

  /**
   * Decay factor for elapsed wall time against a time constant: ≈ {@code exp(-elapsedMs /
   * timeConstantMs)} — the shape {@code PerKeyEvalState}-style lazy decays need — computed
   * through the PELT table in O(1). The half-life is {@code timeConstantMs · ln 2}; the period
   * count is rounded to the nearest integer, which bounds the factor's quantization error at
   * {@code 1 − y^0.5 ≈ 1.07%} relative (pinned against {@code Math.exp} in
   * {@code ZetaDecayMathTest}).
   *
   * <p>
   * Time-rollback convention built in: {@code elapsedMs <= 0} returns {@code 1.0} — the caller
   * re-anchors its timestamp and this update decays nothing (the kernel drops the update and
   * resets {@code last_update_time}, {@code pelt.c:186-194}). Huge deltas clamp internally:
   * gaps beyond ~8.9 years (or periods beyond {@link #MAX_PERIODS}) return {@code 0.0} instead
   * of shifting by a garbage count.
   *
   * @param elapsedMs      non-negative elapsed milliseconds; non-positive returns {@code 1.0}
   * @param timeConstantMs the e-folding time in ms, in {@code (0, ~11.6 days]} — larger values
   *                       are clamped to the supported ceiling
   * @return the decay factor in {@code [0, 1]}
   */
  public static double expDecayFactor(long elapsedMs, long timeConstantMs) {
    if (elapsedMs <= 0 || timeConstantMs <= 0) {
      // pelt.c:186-194 port: a non-positive delta is a protocol event, not data —
      // no-op decay, and the caller re-anchors.
      return 1.0;
    }
    long tc = Math.min(timeConstantMs, MAX_TIME_CONSTANT_MS);
    if (elapsedMs > MAX_ELAPSED_MS) {
      return 0.0;
    }
    // n = round(elapsed · 32 / (tc · ln2)), with ln2 as fixed point LN2_NUM/LN2_DEN:
    // n = round(elapsed·32·10^6 / (tc·693147)). Both products are pre-bounded
    // (elapsed ≤ 2.8e11 × 3.2e7 = 8.96e18; tc ≤ 1e9 × 693147 = 6.93e14) so the
    // rounding addition cannot wrap. (32·10^6 must stay a literal — the nearest
    // power of two, 2^25 = 33_554_432, would skew every time constant by ~4.9%.)
    long num = elapsedMs * 32_000_000L;
    long den = tc * LN2_NUM;
    long n = (num + (den >> 1)) / den;
    return decayFactor(n);
  }

  /**
   * One integer-EWMA update ({@code average.h:56-68} port): blends {@code val} into the state
   * with new-sample weight {@code 1/2^weightLog2}. The state is the average scaled by
   * {@code 2^precision}; read it back with {@link #ewmaRead}. Single-long state means
   * atomicity is one word — {@link java.util.concurrent.atomic.AtomicLong} or a VarHandle CAS,
   * same as the kernel's struct-of-one.
   *
   * <p>
   * Deviations from the literal kernel form, both deliberate:
   *
   * <ul>
   * <li><b>Seed branch kept</b> ({@code average.h:65-68}): a zero state plants
   * {@code val << precision} directly. The kernel form without it converges from zero like
   * {@code 1 − 2^(-updates/w)} — ~0.7·w updates to reach 50% — which silently distorts every
   * consumer that starts from a fresh state.</li>
   * <li><b>Wrap-proof rearrangement</b>: the kernel evaluates
   * {@code ((internal << w) - internal + (val << p)) >> w} in unsigned arithmetic and needs
   * {@code internal·(2^w − 1) + (val << p) < 2^64} to stay exact. This port evaluates the
   * algebraically identical {@code internal + ((val << p) - internal) >> w}: the difference
   * cannot overflow, the arithmetic shift floors exactly like the unsigned shift of the
   * non-negative kernel result, and the two are bit-identical on the kernel's entire valid
   * domain (asserted by tests) while remaining correct far outside it.</li>
   * </ul>
   *
   * @param internal   current scaled state, {@code >= 0} (zero seeds, see above)
   * @param val        new sample, {@code >= 0}; saturated at {@code (2^63-1) >> precision}
   *                   so the scaled product cannot wrap (the kernel leans on unsigned
   *                   discipline; this port prefers deterministic saturation)
   * @param weightLog2 log2 of the kernel's {@code weight_rcp}: the new sample's weight is
   *                   {@code 1/2^weightLog2}; {@code 0} means full replacement
   * @param precision  fractional bits of the fixed point, {@code [0, 30]} per the kernel's
   *                   {@code BUILD_BUG_ON((_precision) > 30)} ({@code average.h:40})
   * @return the updated scaled state, always in {@code [0, 2^63)}
   */
  public static long ewmaAdd(long internal, long val, int weightLog2, int precision) {
    if (precision < 0 || precision > 30) {
      throw new IllegalArgumentException("precision out of [0, 30]: " + precision);
    }
    if (weightLog2 < 0 || weightLog2 > 62) {
      throw new IllegalArgumentException("weightLog2 out of [0, 62]: " + weightLog2);
    }
    if (internal < 0) {
      throw new IllegalArgumentException("negative EWMA state: " + internal);
    }
    if (val < 0) {
      throw new IllegalArgumentException("negative EWMA sample: " + val);
    }
    long scaled = Math.min(val, Long.MAX_VALUE >> precision) << precision;
    if (internal == 0) {
      // average.h:65-68 seed branch — the first sample plants the state outright.
      return scaled;
    }
    // Wrap-proof rearrangement of ((internal << w) - internal + scaled) >> w; see the
    // Javadoc. floor((internal·2^w - internal + s)/2^w) == internal + floor((s - internal)/2^w)
    // holds for any integers, and Java's >> floors — so this is exact, not approximate.
    return internal + ((scaled - internal) >> weightLog2);
  }

  /**
   * Read the average out of a scaled EWMA state ({@code average.h:51}:
   * {@code internal >> precision}).
   *
   * @param internal  scaled state as produced by {@link #ewmaAdd}
   * @param precision fractional bits, {@code [0, 30]}
   * @return the integer-part average
   */
  public static long ewmaRead(long internal, int precision) {
    if (precision < 0 || precision > 30) {
      throw new IllegalArgumentException("precision out of [0, 30]: " + precision);
    }
    if (internal < 0) {
      throw new IllegalArgumentException("negative EWMA state: " + internal);
    }
    return internal >>> precision;
  }

  /**
   * High 64 bits of the <em>unsigned</em> 64×64 product — {@code Math.unsignedMultiplyHigh}
   * (JDK 18) backported for Java 17, per Hacker's Delight 8-2 / the JDK implementation: the
   * signed high product plus one correction term per operand whose implicit {@code 2^64}
   * offset the signed product ignored. Branch-free via sign-mask selects.
   */
  private static long umulHigh(long x, long y) {
    long hi = Math.multiplyHigh(x, y);
    hi += (y & (x >> 63)); // x's unsigned overflow (sign bit set) adds y to the high word
    hi += (x & (y >> 63)); // y's unsigned overflow adds x
    return hi;
  }
}
