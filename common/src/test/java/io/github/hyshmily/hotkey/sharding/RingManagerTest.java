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

import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RingManager}, covering auto mode rebuild behavior, manual override,
 * reset to auto, and query methods.
 */
class RingManagerTest {

  @Test
  void defaultMode_shouldBeAuto() {
    RingManager manager = new RingManager(10);
    assertThat(manager.isManualMode()).isFalse();
  }

  @Test
  void reconcile_withSameNodes_shouldNotRebuild() {
    RingManager manager = new RingManager(10);
    manager.reconcile(Set.of("a", "b", "c"));
    Set<String> nodes1 = manager.getCurrentNodes();

    manager.reconcile(Set.of("a", "b", "c"));
    Set<String> nodes2 = manager.getCurrentNodes();

    // same reference → no new allocation (Set.copyOf would normally create new set,
    // but the ring's nodes set only changes when rebuild is called)
    assertThat(nodes2).isSameAs(nodes1);
  }

  @Test
  void reconcile_withDifferentNodes_shouldRebuild() {
    RingManager manager = new RingManager(10);
    manager.reconcile(Set.of("a", "b"));
    Set<String> nodes1 = manager.getCurrentNodes();

    manager.reconcile(Set.of("a", "b", "c"));
    Set<String> nodes2 = manager.getCurrentNodes();

    assertThat(nodes2).isNotSameAs(nodes1);
    assertThat(nodes2).containsExactlyInAnyOrder("a", "b", "c");
  }

  @Test
  void addNode_shouldSwitchToManualMode() {
    RingManager manager = new RingManager(10);
    manager.addNode("manual-a");
    manager.addNode("manual-b");

    assertThat(manager.isManualMode()).isTrue();
    assertThat(manager.getCurrentNodes()).containsExactlyInAnyOrder("manual-a", "manual-b");
  }

  @Test
  void addNode_shouldIgnoreSubsequentReconcile() {
    RingManager manager = new RingManager(10);
    manager.addNode("manual-a");

    manager.reconcile(Set.of("auto-a", "auto-b"));
    assertThat(manager.getCurrentNodes()).containsExactly("manual-a");
  }

  @Test
  void addNode_shouldBeIncremental() {
    RingManager manager = new RingManager(10);
    manager.addNode("first");
    assertThat(manager.getCurrentNodes()).containsExactly("first");

    manager.addNode("second");
    assertThat(manager.getCurrentNodes()).containsExactlyInAnyOrder("first", "second");
  }

  @Test
  void removeNode_shouldWorkInManualMode() {
    RingManager manager = new RingManager(10);
    manager.addNode("a");
    manager.addNode("b");
    manager.removeNode("a");
    assertThat(manager.getCurrentNodes()).containsExactly("b");
  }

  @Test
  void removeNode_shouldStartManualModeFromCurrentNodes() {
    // In auto mode, removeNode should snapshot current nodes and then remove
    RingManager manager = new RingManager(10);
    manager.reconcile(Set.of("a", "b", "c"));
    manager.removeNode("c");

    assertThat(manager.isManualMode()).isTrue();
    assertThat(manager.getCurrentNodes()).containsExactlyInAnyOrder("a", "b");
  }

  @Test
  void resetToAuto_shouldReenableAutoMode() {
    RingManager manager = new RingManager(10);
    manager.addNode("manual");
    assertThat(manager.isManualMode()).isTrue();

    manager.resetToAuto();
    assertThat(manager.isManualMode()).isFalse();

    manager.reconcile(Set.of("auto-a"));
    assertThat(manager.getCurrentNodes()).containsExactly("auto-a");
  }

  @Test
  void getNode_shouldRouteToCorrectNode() {
    RingManager manager = new RingManager(10);
    manager.addNode("target-a");
    manager.addNode("target-b");

    String node = manager.getNode("some-key");
    assertThat(node).isIn("target-a", "target-b");
  }

  @Test
  void getNode_withEmptyRing_shouldReturnNull() {
    RingManager manager = new RingManager(10);
    assertThat(manager.getNode("any-key")).isNull();
  }

  @Test
  void getVirtualNodeCount_shouldReturnConfiguredValue() {
    RingManager manager = new RingManager(42);
    assertThat(manager.getVirtualNodeCount()).isEqualTo(42);
  }

  @Test
  void nodeCount_shouldReturnCorrectCount() {
    RingManager manager = new RingManager(10);
    manager.reconcile(Set.of("a", "b", "c"));
    assertThat(manager.nodeCount()).isEqualTo(3);
  }

  @Test
  void reconcile_doesNotAffectManualMode() {
    RingManager manager = new RingManager(10);
    manager.addNode("manual");
    manager.reconcile(Set.of("should-be-ignored"));
    assertThat(manager.getCurrentNodes()).containsExactly("manual");
  }
}
