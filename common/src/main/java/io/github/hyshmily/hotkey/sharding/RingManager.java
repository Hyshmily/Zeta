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

import io.github.hyshmily.hotkey.Internal;
import java.util.Set;
import java.util.function.IntConsumer;

/**
 * Manages the consistent-hash ring for Worker shard routing.
 */
@Internal
public interface RingManager {

  /**
   * Rebuild the ring from the current cluster health view.
   */
  void reconcileFromHealthView(HealthView healthView);

  /**
   * Return the current set of nodes on the ring.
   */
  Set<String> getCurrentNodes();

  /**
   * Return the number of physical nodes currently on the ring.
   */
  int nodeCount();

  /**
   * Return the virtual node count.
   */
  int getVirtualNodeCount();

  /**
   * Route a key to the responsible Worker node.
   */
  String routeNode(String key, HealthView healthView);

  /**
   * Route a key to its target Worker node, using a pre-snapshotted alive-set.
   */
  String routeNode(String key, Set<String> aliveNodes);

  /**
   * Set the callback invoked after each ring reconciliation.
   */
  void setOnRingReconciled(IntConsumer onRingReconciled);
}
