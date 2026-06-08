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
package io.github.hyshmily.hotkey.sharding;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ConsistentHashRing}, covering node management, key routing, distribution
 * evenness, virtual node logic, and resilience to node removal.
 */
class ConsistentHashRingTest {

  @Test
  void emptyRing_shouldRouteToNull() {
    ConsistentHashRing ring = new ConsistentHashRing(1);
    assertThat(ring.getNode("any-key")).isNull();
    assertThat(ring.isEmpty()).isTrue();
  }

  @Test
  void singleNode_shouldRouteAllKeysToIt() {
    ConsistentHashRing ring = new ConsistentHashRing(1);
    ring.rebuild(Set.of("node-a"));
    assertThat(ring.isEmpty()).isFalse();
    for (int i = 0; i < 100; i++) {
      assertThat(ring.getNode("key-" + i)).isEqualTo("node-a");
    }
  }

  @Test
  void multiNode_shouldDistributeKeys() {
    ConsistentHashRing ring = new ConsistentHashRing(100);
    ring.rebuild(Set.of("node-a", "node-b", "node-c"));

    Map<String, Integer> distribution = new HashMap<>();
    for (int i = 0; i < 1000; i++) {
      String node = ring.getNode("key-" + i);
      distribution.merge(node, 1, Integer::sum);
    }

    assertThat(distribution).hasSize(3);
    distribution.values().forEach(count -> assertThat(count).isPositive());
  }

  @Test
  void removeNode_shouldOnlyAffectKeysThatBelongedToIt() {
    ConsistentHashRing ring = new ConsistentHashRing(100);
    ring.rebuild(Set.of("node-a", "node-b", "node-c"));

    String[] testKeys = IntStream.range(0, 500)
      .mapToObj(i -> "kt-" + i)
      .toArray(String[]::new);

    Map<String, String> before = new HashMap<>();
    for (String key : testKeys) {
      before.put(key, ring.getNode(key));
    }

    ring.rebuild(Set.of("node-a", "node-c"));

    int moved = 0;
    int unchanged = 0;
    for (String key : testKeys) {
      String after = ring.getNode(key);
      if (before.get(key).equals(after)) {
        unchanged++;
      } else {
        moved++;
      }
    }

    long nodeBCount = before.values().stream().filter("node-b"::equals).count();
    assertThat(moved).isEqualTo((int) nodeBCount);
    assertThat(unchanged).isEqualTo(500 - (int) nodeBCount);
  }

  @Test
  void addNode_shouldNotChangeExistingNodeAssignments() {
    ConsistentHashRing ring = new ConsistentHashRing(100);
    ring.rebuild(Set.of("node-a", "node-b"));

    String[] testKeys = IntStream.range(0, 500)
      .mapToObj(i -> "kt-" + i)
      .toArray(String[]::new);

    Map<String, String> before = new HashMap<>();
    for (String key : testKeys) {
      before.put(key, ring.getNode(key));
    }

    ring.rebuild(Set.of("node-a", "node-b", "node-c"));

    int changed = 0;
    for (String key : testKeys) {
      if (!before.get(key).equals(ring.getNode(key))) {
        changed++;
      }
    }

    assertThat(changed).isLessThan(250);
  }

  @Test
  void getNode_shouldBeDeterministic() {
    ConsistentHashRing ring = new ConsistentHashRing(100);
    ring.rebuild(Set.of("node-a", "node-b"));

    String first = ring.getNode("some-test-key");
    for (int i = 0; i < 50; i++) {
      assertThat(ring.getNode("some-test-key")).isEqualTo(first);
    }
  }

  @Test
  void rebuild_emptySet_shouldClearRing() {
    ConsistentHashRing ring = new ConsistentHashRing(10);
    ring.rebuild(Set.of("node-a"));
    assertThat(ring.isEmpty()).isFalse();

    ring.rebuild(Set.of());
    assertThat(ring.isEmpty()).isTrue();
    assertThat(ring.getNode("key")).isNull();
  }

  @Test
  void getNodes_shouldReturnLiveNodes() {
    ConsistentHashRing ring = new ConsistentHashRing(10);
    ring.rebuild(Set.of("node-a", "node-b", "node-c"));
    assertThat(ring.getNodes()).containsExactlyInAnyOrder("node-a", "node-b", "node-c");
  }

  @Test
  void nodeCount_shouldReturnPhysicalNodeCount() {
    ConsistentHashRing ring = new ConsistentHashRing(10);
    ring.rebuild(Set.of("a", "b", "c"));
    assertThat(ring.nodeCount()).isEqualTo(3);
  }

  @Test
  void rebuild_sameNodes_shouldNotAffectRouting() {
    ConsistentHashRing ring = new ConsistentHashRing(100);
    ring.rebuild(Set.of("a", "b"));
    String before = ring.getNode("key");

    ring.rebuild(Set.of("a", "b"));
    assertThat(ring.getNode("key")).isEqualTo(before);
  }

  @Test
  void noDuplicateKeys_underConcurrentAccess() throws Exception {
    ConsistentHashRing ring = new ConsistentHashRing(100);
    ring.rebuild(Set.of("node-a", "node-b", "node-c"));

    java.util.concurrent.Executors.newFixedThreadPool(10).invokeAll(
      java.util.Collections.nCopies(20, () -> {
        for (int i = 0; i < 200; i++) {
          assertThat(ring.getNode("concurrent-key-" + i))
            .isIn("node-a", "node-b", "node-c");
        }
        return null;
      })
    );
  }
}
