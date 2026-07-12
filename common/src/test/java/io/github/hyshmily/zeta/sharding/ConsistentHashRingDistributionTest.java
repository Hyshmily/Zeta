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
package io.github.hyshmily.zeta.sharding;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * Quantifies distribution evenness of {@link ConsistentHashRing} across different
 * virtual-node counts using the coefficient of variation (CV = stdDev / mean).
 *
 * <p>Lower CV means more even load distribution.  These tests serve as a regression
 * guard and provide documentary evidence that the default virtual-node count (150)
 * produces acceptable distribution quality.
 */
class ConsistentHashRingDistributionTest {

  private static final int N_KEYS = 10_000;

  /**
   * Route {@code N_KEYS} through a ring with the given nodes and virtual-node count,
   * then return the coefficient of variation of the per-node key counts.
   */
  private static double cv(int virtualNodeCount, Set<String> nodes) {
    return cv(virtualNodeCount, nodes, N_KEYS);
  }

  private static double cv(int virtualNodeCount, Set<String> nodes, int keyCount) {
    ConsistentHashRing ring = new ConsistentHashRing(virtualNodeCount);
    ring.rebuild(nodes);

    Map<String, Integer> counts = new HashMap<>();
    for (int i = 0; i < keyCount; i++) {
      counts.merge(ring.locateNode("key-" + i, s -> true), 1, Integer::sum);
    }

    int n = nodes.size();
    double mean = (double) keyCount / n;
    double sumSq = 0;
    for (int c : counts.values()) {
      double diff = c - mean;
      sumSq += diff * diff;
    }
    double stdDev = Math.sqrt(sumSq / n);
    return stdDev / mean;
  }

  @Test
  void vnode1_shouldHavePoorDistribution() {
    double cv = cv(1, nodes(10));
    assertThat(cv).as("CV with 1 vnode per node should be high").isLessThan(1.5);
  }

  @Test
  void vnode50_shouldHaveReasonableDistribution() {
    double cv = cv(50, nodes(10));
    assertThat(cv).as("CV with 50 vnodes per node should be reasonable").isLessThan(0.30);
  }

  @Test
  void vnode150_shouldHaveGoodDistribution() {
    double cv = cv(150, nodes(10));
    assertThat(cv).as("CV with 150 vnodes (default) should be good").isLessThan(0.15);
  }

  @Test
  void vnode500_shouldHaveExcellentDistribution() {
    double cv = cv(500, nodes(10));
    assertThat(cv).as("CV with 500 vnodes should be excellent").isLessThan(0.10);
  }

  @Test
  void distribution_improvesAsVnodeCountIncreases() {
    double cv1 = cv(1, nodes(10));
    double cv10 = cv(10, nodes(10));
    double cv50 = cv(50, nodes(10));
    double cv150 = cv(150, nodes(10));
    double cv500 = cv(500, nodes(10));

    assertThat(cv1).as("cv1=%.4f should be > cv10=%.4f", cv1, cv10).isGreaterThan(cv10);
    assertThat(cv10).as("cv10=%.4f should be > cv50=%.4f", cv10, cv50).isGreaterThan(cv50);

    assertThat(cv150).as("cv150=%.4f should be < 0.10", cv150).isLessThan(0.10);
    assertThat(cv500).as("cv500=%.4f should be < 0.10", cv500).isLessThan(0.10);

    System.out.printf(
      "CV(vnode=%d)=%.4f | CV(vnode=%d)=%.4f | CV(vnode=%d)=%.4f | CV(vnode=%d)=%.4f | CV(vnode=%d)=%.4f%n",
      1,
      cv1,
      10,
      cv10,
      50,
      cv50,
      150,
      cv150,
      500,
      cv500
    );
  }

  /**
   * For the 100-node cluster we need more keys to keep the CV meaningful
   * (fewer keys per node inflates the CV).  100k keys × 100 nodes = 1000 keys/node.
   */
  @Test
  void largeCluster_shouldMaintainGoodDistribution() {
    double cv = cv(500, nodes(100), 100_000);
    assertThat(cv).as("CV with 100 nodes × 500 vnodes (ADR-0005 spec) should be good").isLessThan(0.15);
  }

  private static Set<String> nodes(int count) {
    return IntStream.range(0, count)
      .mapToObj(i -> "node-" + i)
      .collect(java.util.stream.Collectors.toSet());
  }
}
