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
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.github.hyshmily.hotkey.sync.worker.WorkerHeartbeatMessage;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RingManagerTest {

  private static void registerAlive(ClusterHealthView healthView, String nodeId) {
    healthView.onHeartbeat(new WorkerHeartbeatMessage(nodeId, 1, 0, 0, true, 0, 0, 0, 0));
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
  void routeNode_shouldRouteToCorrectNode() {
    RingManager manager = new RingManager(10);
    ClusterHealthView healthView = new ClusterHealthView(2, 30000, 3);
    registerAlive(healthView, "target-a");
    registerAlive(healthView, "target-b");
    manager.reconcileFromHealthView(healthView);

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
  void routeNode_withNullKey_shouldThrow() {
    RingManager manager = new RingManager(10);
    ClusterHealthView hv = new ClusterHealthView(1, 30000, 3);
    registerAlive(hv, "worker-1");
    manager.reconcileFromHealthView(hv);
    assertThatNullPointerException().isThrownBy(() -> manager.routeNode(null, hv));
  }

  @Test
  void routeNode_withNullHealthView_shouldThrow() {
    RingManager manager = new RingManager(10);
    assertThatNullPointerException().isThrownBy(() -> manager.routeNode("key", (ClusterHealthView) null));
  }

  @Test
  void reconcileFromHealthView_withNull_shouldThrow() {
    RingManager manager = new RingManager(10);
    assertThatNullPointerException().isThrownBy(() -> manager.reconcileFromHealthView(null));
  }

  // ── onRingReconciled callback (P0-1) ──

  @Test
  void reconcileFromHealthView_shouldInvokeOnRingReconciled() {
    RingManager manager = new RingManager(10);
    ClusterHealthView healthView = new ClusterHealthView(3, 30000, 3);

    final int[] capturedCount = { -1 };
    manager.setOnRingReconciled(count -> capturedCount[0] = count);

    registerAlive(healthView, "a");
    registerAlive(healthView, "b");
    registerAlive(healthView, "c");
    manager.reconcileFromHealthView(healthView);

    assertThat(capturedCount[0]).isEqualTo(3);
  }

  @Test
  void reconcileFromHealthView_withSameNodes_shouldNotInvokeOnRingReconciled() {
    RingManager manager = new RingManager(10);
    ClusterHealthView healthView = new ClusterHealthView(1, 30000, 3);
    registerAlive(healthView, "a");

    final int[] invocationCount = { 0 };
    manager.setOnRingReconciled(count -> invocationCount[0]++);

    manager.reconcileFromHealthView(healthView);
    manager.reconcileFromHealthView(healthView);

    assertThat(invocationCount[0]).isEqualTo(1);
  }

  @Test
  void reconcileFromHealthView_withNullOnRingReconciled_shouldNotThrow() {
    RingManager manager = new RingManager(10);
    manager.setOnRingReconciled(null);

    ClusterHealthView healthView = new ClusterHealthView(1, 30000, 3);
    registerAlive(healthView, "a");

    manager.reconcileFromHealthView(healthView);
    assertThat(manager.getCurrentNodes()).containsExactly("a");
  }
}
