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

  /**
   * Verifies that a newly created RingManager defaults to auto mode.
   */
  @Test
  void defaultMode_shouldBeAuto() {
    RingManager manager = new RingManager(10);
    assertThat(manager.isManualMode()).isFalse();
  }

  /**
   * Verifies that reconcile does not rebuild the ring when the node set is unchanged.
   */
  @Test
  void reconcile_withSameNodes_shouldNotRebuild() {
    RingManager manager = new RingManager(10);
    manager.onHeartbeat("a", System.currentTimeMillis());
    manager.onHeartbeat("b", System.currentTimeMillis());
    manager.onHeartbeat("c", System.currentTimeMillis());
    manager.reconcile();
    Set<String> nodes1 = manager.getCurrentNodes();

    manager.reconcile();
    Set<String> nodes2 = manager.getCurrentNodes();

    assertThat(nodes2).isSameAs(nodes1);
  }

  /**
   * Verifies that reconcile rebuilds the ring when the node set differs from the current set.
   */
  @Test
  void reconcile_withDifferentNodes_shouldRebuild() {
    RingManager manager = new RingManager(10);
    manager.onHeartbeat("a", System.currentTimeMillis());
    manager.onHeartbeat("b", System.currentTimeMillis());
    manager.reconcile();
    Set<String> nodes1 = manager.getCurrentNodes();

    manager.onHeartbeat("c", System.currentTimeMillis());
    manager.reconcile();
    Set<String> nodes2 = manager.getCurrentNodes();

    assertThat(nodes2).isNotSameAs(nodes1);
    assertThat(nodes2).containsExactlyInAnyOrder("a", "b", "c");
  }

  /**
   * Verifies that calling addNode switches the manager to manual mode with the added nodes.
   */
  @Test
  void addNode_shouldSwitchToManualMode() {
    RingManager manager = new RingManager(10);
    manager.addNode("manual-a");
    manager.addNode("manual-b");

    assertThat(manager.isManualMode()).isTrue();
    assertThat(manager.getCurrentNodes()).containsExactlyInAnyOrder("manual-a", "manual-b");
  }

  /**
   * Verifies that in manual mode, reconcile calls are ignored and the manual node set is preserved.
   */
  @Test
  void addNode_shouldIgnoreSubsequentReconcile() {
    RingManager manager = new RingManager(10);
    manager.addNode("manual-a");

    manager.reconcile();
    assertThat(manager.getCurrentNodes()).containsExactly("manual-a");
  }

  /**
   * Verifies that multiple addNode calls accumulate incrementally.
   */
  @Test
  void addNode_shouldBeIncremental() {
    RingManager manager = new RingManager(10);
    manager.addNode("first");
    assertThat(manager.getCurrentNodes()).containsExactly("first");

    manager.addNode("second");
    assertThat(manager.getCurrentNodes()).containsExactlyInAnyOrder("first", "second");
  }

  /**
   * Verifies that removeNode works correctly in manual mode.
   */
  @Test
  void removeNode_shouldWorkInManualMode() {
    RingManager manager = new RingManager(10);
    manager.addNode("a");
    manager.addNode("b");
    manager.removeNode("a");
    assertThat(manager.getCurrentNodes()).containsExactly("b");
  }

  /**
   * Verifies that removeNode in auto mode snapshots current nodes then switches to manual mode.
   */
  @Test
  void removeNode_shouldStartManualModeFromCurrentNodes() {
    RingManager manager = new RingManager(10);
    manager.onHeartbeat("a", System.currentTimeMillis());
    manager.onHeartbeat("b", System.currentTimeMillis());
    manager.onHeartbeat("c", System.currentTimeMillis());
    manager.reconcile();
    manager.removeNode("c");

    assertThat(manager.isManualMode()).isTrue();
    assertThat(manager.getCurrentNodes()).containsExactlyInAnyOrder("a", "b");
  }

  /**
   * Verifies that resetToAuto re-enables auto mode, allowing reconcile to take effect again.
   */
  @Test
  void resetToAuto_shouldReenableAutoMode() {
    RingManager manager = new RingManager(10);
    manager.addNode("manual");
    assertThat(manager.isManualMode()).isTrue();

    manager.resetToAuto();
    assertThat(manager.isManualMode()).isFalse();

    manager.onHeartbeat("auto-a", System.currentTimeMillis());
    manager.reconcile();
    assertThat(manager.getCurrentNodes()).containsExactly("auto-a");
  }

  /**
   * Verifies that locateNode routes a key to one of the registered nodes.
   */
  @Test
  void routeNode_shouldRouteToCorrectNode() {
    RingManager manager = new RingManager(10);
    manager.addNode("target-a");
    manager.addNode("target-b");

    String node = manager.routeNode("some-key");
    assertThat(node).isIn("target-a", "target-b");
  }

  /**
   * Verifies that locateNode returns null when the ring is empty.
   */
  @Test
  void getNode_ForSharding_withEmptyRing_shouldReturnNull() {
    RingManager manager = new RingManager(10);
    assertThat(manager.routeNode("any-key")).isNull();
  }

  /**
   * Verifies that getVirtualNodeCount returns the value configured at construction time.
   */
  @Test
  void getVirtualNodeCount_shouldReturnConfiguredValue() {
    RingManager manager = new RingManager(42);
    assertThat(manager.getVirtualNodeCount()).isEqualTo(42);
  }

  /**
   * Verifies that nodeCount returns the correct number of registered nodes.
   */
  @Test
  void nodeCount_shouldReturnCorrectCount() {
    RingManager manager = new RingManager(10);
    manager.onHeartbeat("a", System.currentTimeMillis());
    manager.onHeartbeat("b", System.currentTimeMillis());
    manager.onHeartbeat("c", System.currentTimeMillis());
    manager.reconcile();
    assertThat(manager.nodeCount()).isEqualTo(3);
  }

  /**
   * Verifies that reconcile does not affect the node set when in manual mode.
   */
  @Test
  void reconcile_doesNotAffectManualMode() {
    RingManager manager = new RingManager(10);
    manager.addNode("manual");
    manager.reconcile();
    assertThat(manager.getCurrentNodes()).containsExactly("manual");
  }
}
