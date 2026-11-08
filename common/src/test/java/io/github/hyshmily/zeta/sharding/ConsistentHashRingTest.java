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
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import io.github.hyshmily.zeta.sync.worker.WorkerHeartbeatMessage;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * Tests for {@link ConsistentHashRing}, covering node management, key routing, distribution
 * evenness, virtual node logic, and resilience to node removal.
 */
class ConsistentHashRingTest {

  /**
   * Verifies that an empty ring routes all keys to null and holds no live nodes.
   */
  @Test
  void emptyRing_shouldRouteToNull() {
    ConsistentHashRing ring = new ConsistentHashRingImpl(1);
    assertThat(ring.locateNode("any-key", s -> true)).isNull();
    assertThat(ring.getNodes()).isEmpty();
  }

  /**
   * Verifies that a ring with a single node routes all keys to that node.
   */
  @Test
  void singleNode_shouldRouteAllKeysToIt() {
    ConsistentHashRing ring = new ConsistentHashRingImpl(1);
    ring.rebuild(Set.of("node-a"));
    assertThat(ring.getNodes()).containsExactly("node-a");
    for (int i = 0; i < 100; i++) {
      assertThat(ring.locateNode("key-" + i, s -> true)).isEqualTo("node-a");
    }
  }

  /**
   * The snapshot-only route is the predicate route with every ring node alive: the reporter
   * relies on the ring's node set already being the last reconciled alive set.
   */
  @Test
  void locateNode_withoutPredicate_shouldMatchTheAllAliveRoute() {
    ConsistentHashRing ring = new ConsistentHashRingImpl(100);
    ring.rebuild(Set.of("node-a", "node-b", "node-c"));

    for (int i = 0; i < 100; i++) {
      String key = "key-" + i;
      assertThat(ring.locateNode(key)).isEqualTo(ring.locateNode(key, s -> true));
    }
  }

  /**
   * Verifies that a multi-node ring distributes keys across all nodes with some keys going to each.
   */
  @Test
  void multiNode_shouldDistributeKeys() {
    ConsistentHashRing ring = new ConsistentHashRingImpl(100);
    ring.rebuild(Set.of("node-a", "node-b", "node-c"));

    Map<String, Integer> distribution = new HashMap<>();
    for (int i = 0; i < 1000; i++) {
      String node = ring.locateNode("key-" + i, s -> true);
      distribution.merge(node, 1, Integer::sum);
    }

    assertThat(distribution).hasSize(3);
    distribution.values().forEach(count -> assertThat(count).isPositive());
  }

  /**
   * Verifies that removing a node only re-routes keys that previously belonged to that node, leaving others unchanged.
   */
  @Test
  void removeNode_shouldOnlyAffectKeysThatBelongedToIt() {
    ConsistentHashRing ring = new ConsistentHashRingImpl(100);
    ring.rebuild(Set.of("node-a", "node-b", "node-c"));

    String[] testKeys = IntStream.range(0, 500)
      .mapToObj(i -> "kt-" + i)
      .toArray(String[]::new);

    Map<String, String> before = new HashMap<>();
    for (String key : testKeys) {
      before.put(key, ring.locateNode(key, s -> true));
    }

    ring.rebuild(Set.of("node-a", "node-c"));

    int moved = 0;
    int unchanged = 0;
    for (String key : testKeys) {
      String after = ring.locateNode(key, s -> true);
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

  /**
   * Verifies that adding a node causes minimal key reassignment, with fewer than half of keys moving.
   */
  @Test
  void addNode_shouldNotChangeExistingNodeAssignments() {
    ConsistentHashRing ring = new ConsistentHashRingImpl(100);
    ring.rebuild(Set.of("node-a", "node-b"));

    String[] testKeys = IntStream.range(0, 500)
      .mapToObj(i -> "kt-" + i)
      .toArray(String[]::new);

    Map<String, String> before = new HashMap<>();
    for (String key : testKeys) {
      before.put(key, ring.locateNode(key, s -> true));
    }

    ring.rebuild(Set.of("node-a", "node-b", "node-c"));

    int changed = 0;
    for (String key : testKeys) {
      if (!before.get(key).equals(ring.locateNode(key, s -> true))) {
        changed++;
      }
    }

    assertThat(changed).isLessThan(250);
  }

  /**
   * Two node IDs that collide in {@link String#hashCode()} must still own independent virtual
   * sets; the ring seeds from a 64-bit hash of the full ID, not the 32-bit polynomial value.
   */
  @Test
  void rebuild_placesBothNodesWhenStringHashCodesCollide() {
    assertThat("Aa".hashCode()).isEqualTo("BB".hashCode());
    ConsistentHashRing ring = new ConsistentHashRingImpl(500);
    ring.rebuild(Set.of("Aa", "BB"));

    Map<String, Integer> distribution = new HashMap<>();
    for (int i = 0; i < 1_000; i++) {
      String node = ring.locateNode("key-" + i, s -> true);
      distribution.merge(node, 1, Integer::sum);
    }

    assertThat(distribution).containsOnlyKeys("Aa", "BB");
    assertThat(distribution.values()).allMatch(count -> count > 0);
  }

  /**
   * Verifies that locateNode returns the same node consistently for the same key.
   */
  @Test
  void locateNode_shouldBeDeterministic() {
    ConsistentHashRing ring = new ConsistentHashRingImpl(100);
    ring.rebuild(Set.of("node-a", "node-b"));

    String first = ring.locateNode("some-test-key", s -> true);
    for (int i = 0; i < 50; i++) {
      assertThat(ring.locateNode("some-test-key", s -> true)).isEqualTo(first);
    }
  }

  /**
   * Verifies that rebuilding with an empty set clears the ring and routes all keys to null.
   */
  @Test
  void rebuild_emptySet_shouldClearRing() {
    ConsistentHashRing ring = new ConsistentHashRingImpl(10);
    ring.rebuild(Set.of("node-a"));
    assertThat(ring.getNodes()).containsExactly("node-a");

    ring.rebuild(Set.of());
    assertThat(ring.getNodes()).isEmpty();
    assertThat(ring.locateNode("key", s -> true)).isNull();
  }

  /**
   * Verifies that getNodes returns all currently registered live nodes.
   */
  @Test
  void getNodes_shouldReturnLiveNodesForSharding() {
    ConsistentHashRing ring = new ConsistentHashRingImpl(10);
    ring.rebuild(Set.of("node-a", "node-b", "node-c"));
    assertThat(ring.getNodes()).containsExactlyInAnyOrder("node-a", "node-b", "node-c");
  }

  /**
   * Verifies that getNodes returns the physical (not virtual) nodes placed by the rebuild.
   */
  @Test
  void getNodes_shouldReturnPhysicalNodeCount() {
    ConsistentHashRing ring = new ConsistentHashRingImpl(10);
    ring.rebuild(Set.of("a", "b", "c"));
    assertThat(ring.getNodes()).hasSize(3);
  }

  /**
   * Verifies that rebuilding with the same nodes produces identical routing results.
   */
  @Test
  void rebuild_sameNodes_shouldNotAffectRouting() {
    ConsistentHashRing ring = new ConsistentHashRingImpl(100);
    ring.rebuild(Set.of("a", "b"));
    String before = ring.locateNode("key", s -> true);

    ring.rebuild(Set.of("a", "b"));
    assertThat(ring.locateNode("key", s -> true)).isEqualTo(before);
  }

  /**
   * Verifies that locateNode remains consistent under concurrent access from multiple threads.
   */
  @Test
  void noDuplicateKeys_underConcurrentAccess() throws Exception {
    ConsistentHashRing ring = new ConsistentHashRingImpl(100);
    ring.rebuild(Set.of("node-a", "node-b", "node-c"));

    java.util.concurrent.Executors.newFixedThreadPool(10).invokeAll(
      java.util.Collections.nCopies(20, () -> {
        for (int i = 0; i < 200; i++) {
          assertThat(ring.locateNode("concurrent-key-" + i, s -> true)).isIn("node-a", "node-b", "node-c");
        }
        return null;
      })
    );
  }

  /**
   * The health-view overload must route exactly like the id-based predicate, and a Worker
   * that dies after the last rebuild must be skipped by the slot probe (the ring still holds
   * it until the next reconcile).
   */
  @Test
  void locateNode_withHealthView_shouldMatchTheIdProbeAndSkipADeadNode() {
    HealthViewImpl view = new HealthViewImpl(30_000);
    view.onHeartbeat(heartbeat("node-a"));
    view.onHeartbeat(heartbeat("node-b"));
    view.onHeartbeat(heartbeat("node-c"));
    ConsistentHashRing ring = new ConsistentHashRingImpl(100);
    assertThat(ring.rebuildIfPossible(view)).containsExactlyInAnyOrder("node-a", "node-b", "node-c");

    for (int i = 0; i < 500; i++) {
      String key = "health-key-" + i;
      assertThat(ring.locateNode(key, view)).isEqualTo(ring.locateNode(key, node -> view.isAlive(node)));
    }

    // node-b dies while the ring still contains it: both liveness sources must skip it.
    view.onHeartbeat(new WorkerHeartbeatMessage(0L, "node-b", 1L, false, null));
    for (int i = 0; i < 500; i++) {
      String key = "health-key-" + i;
      String routed = ring.locateNode(key, view);
      assertThat(routed).isIn("node-a", "node-c");
      assertThat(routed).isEqualTo(ring.locateNode(key, node -> view.isAlive(node)));
    }
  }

  private static WorkerHeartbeatMessage heartbeat(String workerId) {
    return new WorkerHeartbeatMessage(0L, workerId, 1L, true, null);
  }

  @Test
  void allNodesDead_shouldReturnNull() {
    ConsistentHashRing ring = new ConsistentHashRingImpl(10);
    ring.rebuild(Set.of("node-a", "node-b"));
    assertThat(ring.locateNode("any-key", s -> false)).isNull();
  }

  @Test
  void locateNode_withNullKey_shouldThrowNullPointer() {
    ConsistentHashRing ring = new ConsistentHashRingImpl(10);
    ring.rebuild(Set.of("node-a"));
    assertThatNullPointerException().isThrownBy(() -> ring.locateNode(null, s -> true));
  }

  @Test
  void locateNode_withNullPredicate_shouldThrowNullPointer() {
    ConsistentHashRing ring = new ConsistentHashRingImpl(10);
    ring.rebuild(Set.of("node-a"));
    assertThatNullPointerException().isThrownBy(() ->
      ring.locateNode("key", (java.util.function.Predicate<String>) null)
    );
  }

  @Test
  void singleVirtualNode_shouldStillRouteConsistently() {
    ConsistentHashRing ring = new ConsistentHashRingImpl(1);
    ring.rebuild(Set.of("node-a"));
    for (int i = 0; i < 100; i++) {
      assertThat(ring.locateNode("key-" + i, s -> true)).isEqualTo("node-a");
    }
  }

  @Test
  void getNodes_onEmptyRing_shouldBeEmpty() {
    ConsistentHashRing ring = new ConsistentHashRingImpl(10);
    assertThat(ring.getNodes()).isEmpty();
  }

  @Test
  void rebuild_withNull_shouldThrowNullPointer() {
    ConsistentHashRing ring = new ConsistentHashRingImpl(10);
    assertThatNullPointerException().isThrownBy(() -> ring.rebuild(null));
  }

  @Test
  void largeRing_shouldRouteAllKeys() {
    ConsistentHashRing ring = new ConsistentHashRingImpl(50);
    Set<String> nodes = IntStream.range(0, 100)
      .mapToObj(i -> "node-" + i)
      .collect(java.util.stream.Collectors.toSet());
    ring.rebuild(nodes);
    for (int i = 0; i < 1000; i++) {
      assertThat(ring.locateNode("key-" + i, s -> true)).startsWith("node-");
    }
  }

  @Test
  void locateNode_withoutNodes_shouldReturnNull() {
    ConsistentHashRing ring = new ConsistentHashRingImpl(10);
    assertThat(ring.getNodes()).isEmpty();
    assertThat(ring.locateNode("key")).isNull();
  }

  /**
   * Verifies the probe-exhaustion logging contract: {@code locateNode} runs on every
   * routed key (hot path), so exhaustion is reported per key at DEBUG and never at WARN.
   */
  @Test
  void probeExhaustion_logsPerKeyAtDebugOnly() {
    // A large ring makes the exhaustion path reachable for every key: the probe
    // walk completes exactly one full cycle before reporting exhaustion.
    ConsistentHashRing ring = new ConsistentHashRingImpl(1000);
    ring.rebuild(Set.of("node-a"));

    CollectingAppender appender = new CollectingAppender();
    LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
    ch.qos.logback.classic.Logger logbackLogger = context.getLogger(ConsistentHashRingImpl.class);
    // The DEBUG assertion below requires DEBUG to actually be emitted — pin the
    // level for the duration of the test (the effective level may be INFO).
    Level originalLevel = logbackLogger.getLevel();
    logbackLogger.setLevel(Level.DEBUG);
    appender.start();
    logbackLogger.addAppender(appender);
    try {
      for (int i = 0; i < 20; i++) {
        assertThat(ring.locateNode("dead-key-" + i, s -> false)).isNull();
      }

      assertThat(appender.events).filteredOn(e -> e.getLevel() == Level.WARN).isEmpty();

      assertThat(appender.events)
        .filteredOn(e -> e.getLevel() == Level.DEBUG)
        .extracting(ILoggingEvent::getFormattedMessage)
        .anySatisfy(msg -> assertThat(msg).contains("dead-key-0"))
        .anySatisfy(msg -> assertThat(msg).contains("dead-key-19"));
    } finally {
      logbackLogger.detachAppender(appender);
      if (originalLevel != null) {
        logbackLogger.setLevel(originalLevel);
      }
    }
  }

  /** Minimal logback capture appender. */
  private static class CollectingAppender extends AppenderBase<ILoggingEvent> {

    final java.util.List<ILoggingEvent> events = new java.util.ArrayList<>();

    @Override
    protected void append(ILoggingEvent event) {
      events.add(event);
    }
  }
}
