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

import io.github.hyshmily.hotkey.sync.ClusterHealthView;
import io.github.hyshmily.hotkey.sync.WorkerHeartbeatMessage;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RingManagerTest {

  private static void registerAlive(ClusterHealthView healthView, String nodeId) {
    healthView.onHeartbeat(
        new WorkerHeartbeatMessage(nodeId, 1, System.currentTimeMillis(), 0, 0, true, 0, 0, 0, 0, 0));
  }

  @Test
  void defaultMode_shouldBeAuto() {
    RingManager manager = new RingManager(10);
    assertThat(manager.isManualMode()).isFalse();
  }

  @Test
  void reconcile_withSameNodes_shouldNotRebuild() {
    RingManager manager = new RingManager(10);
    ClusterHealthView healthView = new ClusterHealthView(3, 30000, 3);
    registerAlive(healthView, "a");
    registerAlive(healthView, "b");
    registerAlive(healthView, "c");
    manager.reconcileFromHealthView(healthView);
    Set<String> nodes1 = manager.getCurrentNodes();

    manager.reconcileFromHealthView(healthView);
    Set<String> nodes2 = manager.getCurrentNodes();

    assertThat(nodes2).isSameAs(nodes1);
  }

  @Test
  void reconcile_withDifferentNodes_shouldRebuild() {
    RingManager manager = new RingManager(10);
    ClusterHealthView healthView = new ClusterHealthView(3, 30000, 3);
    registerAlive(healthView, "a");
    registerAlive(healthView, "b");
    manager.reconcileFromHealthView(healthView);
    Set<String> nodes1 = manager.getCurrentNodes();

    registerAlive(healthView, "c");
    manager.reconcileFromHealthView(healthView);
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

    manager.reconcileFromHealthView(new ClusterHealthView(0, 30000, 3));
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
    RingManager manager = new RingManager(10);
    ClusterHealthView healthView = new ClusterHealthView(3, 30000, 3);
    registerAlive(healthView, "a");
    registerAlive(healthView, "b");
    registerAlive(healthView, "c");
    manager.reconcileFromHealthView(healthView);
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

    ClusterHealthView healthView = new ClusterHealthView(1, 30000, 3);
    registerAlive(healthView, "auto-a");
    manager.reconcileFromHealthView(healthView);
    assertThat(manager.getCurrentNodes()).containsExactly("auto-a");
  }

  @Test
  void routeNode_shouldRouteToCorrectNode() {
    RingManager manager = new RingManager(10);
    manager.addNode("target-a");
    manager.addNode("target-b");

    ClusterHealthView healthView = new ClusterHealthView(2, 30000, 3);
    registerAlive(healthView, "target-a");
    registerAlive(healthView, "target-b");
    String node = manager.routeNode("some-key", healthView);
    assertThat(node).isIn("target-a", "target-b");
  }

  @Test
  void getNode_ForSharding_withEmptyRing_shouldReturnNull() {
    RingManager manager = new RingManager(10);
    assertThat(manager.routeNode("any-key", new ClusterHealthView(0, 30000, 3))).isNull();
  }

  @Test
  void getVirtualNodeCount_shouldReturnConfiguredValue() {
    RingManager manager = new RingManager(42);
    assertThat(manager.getVirtualNodeCount()).isEqualTo(42);
  }

  @Test
  void nodeCount_shouldReturnCorrectCount() {
    RingManager manager = new RingManager(10);
    ClusterHealthView healthView = new ClusterHealthView(3, 30000, 3);
    registerAlive(healthView, "a");
    registerAlive(healthView, "b");
    registerAlive(healthView, "c");
    manager.reconcileFromHealthView(healthView);
    assertThat(manager.nodeCount()).isEqualTo(3);
  }

  @Test
  void reconcile_doesNotAffectManualMode() {
    RingManager manager = new RingManager(10);
    manager.addNode("manual");
    manager.reconcileFromHealthView(new ClusterHealthView(0, 30000, 3));
    assertThat(manager.getCurrentNodes()).containsExactly("manual");
  }
}
