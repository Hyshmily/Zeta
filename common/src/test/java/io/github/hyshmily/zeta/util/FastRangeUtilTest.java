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

import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Property and known-value tests for {@link FastRangeUtil} (RocksDB
 * {@code util/fastrange.h} semantics).
 */
class FastRangeUtilTest {

  @Test
  void knownValues() {
    assertThat(FastRangeUtil.fastRange32(0, 5)).isZero();
    // uint32 max × 5 = 0x4_FFFF_FFFF... high half = 4.
    assertThat(FastRangeUtil.fastRange32(-1, 5)).isEqualTo(4);
    // uint32 max maps to range-1 for any range: the product's high half is
    // (range-1) + (2^32-1)/2^32 → truncated to range-1.
    assertThat(FastRangeUtil.fastRange32(-1, 1024)).isEqualTo(1023);
  }

  @Test
  void resultAlwaysWithinRange() {
    Random random = new Random(42);
    for (int i = 0; i < 100_000; i++) {
      int hash = random.nextInt();
      int bucket = FastRangeUtil.fastRange32(hash, 967);
      assertThat(bucket).as("hash=%s", hash).isBetween(0, 966);
    }
  }

  @Test
  void fullRangeCoverage_forNonPowerOfTwoWidth() {
    Random random = new Random(7);
    int range = 967;
    boolean[] hit = new boolean[range];
    for (int i = 0; i < 100_000; i++) {
      hit[FastRangeUtil.fastRange32(random.nextInt(), range)] = true;
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
  void distribution_bucketCountsBalanced_withinFourSigma() {
    Random random = new Random(1234);
    int range = 1023;
    int samples = 1_000_000;
    int[] counts = new int[range];
    for (int i = 0; i < samples; i++) {
      counts[FastRangeUtil.fastRange32(random.nextInt(), range)]++;
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
  void rangeOne_alwaysZero() {
    for (int i = 0; i < 1_000; i++) {
      assertThat(FastRangeUtil.fastRange32(new Random(i).nextInt(), 1)).isZero();
    }
  }
}
