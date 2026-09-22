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

import io.github.hyshmily.zeta.Internal;
import java.util.Set;
import java.util.function.IntConsumer;
import java.util.function.Predicate;

/**
 * Manages the consistent-hash ring for Worker shard routing.
 */
@Internal
public interface RingManager {
  /**
   * Rebuild the ring from the current cluster health view.
   *
   * @return the alive Worker set this reconciliation is based on — equal to the
   *         ring's node set immediately after the call. Reusing this snapshot as
   *         the routing liveness predicate avoids a second
   *         {@link HealthView#getAliveWorkerIds()} materialisation per flush and
   *         keeps the predicate consistent with the ring that was just rebuilt.
   */
  Set<String> reconcileFromHealthView(HealthView healthView);

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
   * Route a key to its target Worker node using a pre-built liveness predicate.
   * Hot loops should build the predicate once (e.g. {@code aliveNodes::contains})
   * and reuse it across keys, instead of re-deriving it per key from this
   * interface's convenience overloads.
   *
   * @param key     the cache key to route; must not be {@code null}
   * @param isAlive liveness predicate over Worker node IDs; must not be {@code null}
   * @return the target Worker node id, or {@code null} if the ring is empty or no
   *         ring node satisfies the predicate
   */
  String routeNode(String key, Predicate<String> isAlive);

  /**
   * Set the callback invoked after each ring reconciliation.
   */
  void setOnRingReconciled(IntConsumer onRingReconciled);
}
