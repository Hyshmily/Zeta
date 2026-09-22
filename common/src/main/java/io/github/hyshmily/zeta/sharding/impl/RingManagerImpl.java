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
package io.github.hyshmily.zeta.sharding.impl;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.sharding.ConsistentHashRing;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sharding.RingManager;
import java.util.Set;
import java.util.function.IntConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages the consistent-hash ring for Worker shard routing.
 *
 * <p>The ring is rebuilt automatically from the live Worker set reported
 * by {@link HealthView} on each reconciliation cycle.
 */
@Internal
@Slf4j
public class RingManagerImpl implements RingManager {

  @Getter
  private final ConsistentHashRing ring;

  @Getter
  private final int virtualNodeCount;

  @Setter
  private IntConsumer onRingReconciled;

  /**
   * Creates a ring manager with the given virtual-node count.
   *
   * @param virtualNodeCount virtual copies per physical shard on the ring
   */
  public RingManagerImpl(int virtualNodeCount) {
    this.virtualNodeCount = virtualNodeCount;
    this.ring = new ConsistentHashRing(virtualNodeCount);
  }

  /** Maximum Worker IDs shown in the ring-rebuilt INFO log before truncation. */
  private static final int REBUILD_LOG_MAX_NODE_IDS = 20;

  /**
   * Rebuild the ring from the current cluster health view.
   *
   * <p>The node-ID sample in the rebuild log is truncated per the repo logging
   * rule (unbounded {@code joining} must be {@code limit()}-capped) — the Worker
   * counts in the same line already carry the cardinality.
   *
   * @param healthView the current cluster health view; must not be {@code null}
   * @return the alive Worker set this reconciliation is based on — equal to the
   *         ring's node set immediately after the call; callers may reuse it as
   *         the routing liveness predicate without a second
   *         {@link HealthView#getAliveWorkerIds()} materialisation
   * @throws NullPointerException if {@code healthView} is {@code null}
   */
  @Override
  public synchronized Set<String> reconcileFromHealthView(HealthView healthView) {
    Set<String> alive = healthView.getAliveWorkerIds();
    Set<String> prev = ring.getNodes();
    if (!alive.equals(prev)) {
      ring.rebuild(alive);
      log.info(
        "Ring rebuilt: {} -> {} workers [{}]",
        prev.size(),
        alive.size(),
        truncateNodeIds(alive)
      );
      if (onRingReconciled != null) {
        onRingReconciled.accept(alive.size());
      }
    }
    return alive;
  }

  /**
   * First {@code REBUILD_LOG_MAX_NODE_IDS} Worker IDs (sorted, comma-joined),
   * with an ellipsis marker when the set was truncated.
   */
  private static String truncateNodeIds(Set<String> nodes) {
    String joined = nodes.stream().sorted().limit(REBUILD_LOG_MAX_NODE_IDS).collect(Collectors.joining(", "));
    return nodes.size() > REBUILD_LOG_MAX_NODE_IDS ? joined + ", ..." : joined;
  }

  /**
   * Return the current set of nodes on the ring.
   *
   * @return the set of live node identifiers
   */
  @Override
  public Set<String> getCurrentNodes() {
    return ring.getNodes();
  }

  /**
   * Return the number of physical nodes currently on the ring.
   *
   * @return the node count
   */
  @Override
  public int nodeCount() {
    return ring.nodeCount();
  }

  /**
   * Route a key to the responsible Worker node.
   *
   * @param key        the cache key to route; must not be {@code null}
   * @param healthView the current cluster health view; must not be {@code null}
   * @return the node identifier that owns the key, or {@code null} if no node is available
   * @throws NullPointerException if {@code key} or {@code healthView} is {@code null}
   */
  @Override
  public String routeNode(String key, HealthView healthView) {
    Set<String> alive = healthView.getAliveWorkerIds();
    return ring.locateNode(key, alive::contains);
  }

  /**
   * Route a key to its target Worker node, using a pre-snapshotted alive-set
   * supplied by the caller. This avoids re-fetching {@link
   * HealthView#getAliveWorkerIds()} per key inside hot loops.
   *
   * <p>Convenience for {@code routeNode(key, aliveNodes::contains)} — hot loops
   * routing many keys against one snapshot should prefer the {@link #routeNode(
   * String, Predicate)} overload with a predicate built once per batch.
   *
   * @param key        the cache key to route
   * @param aliveNodes the already-snapshotted set of alive Worker ids; must not
   *                   be {@code null} or modified concurrently
   * @return the target Worker node id, or {@code null} if no alive nodes
   */
  @Override
  public String routeNode(String key, Set<String> aliveNodes) {
    if (aliveNodes == null || aliveNodes.isEmpty()) {
      return null;
    }
    return ring.locateNode(key, aliveNodes::contains);
  }

  /**
   * Route a key to its target Worker node using a pre-built liveness predicate.
   * Delegates directly to {@link ConsistentHashRing#locateNode}; hot loops build
   * the predicate once per batch, so no per-key capturing lambda is allocated.
   *
   * @param key     the cache key to route; must not be {@code null}
   * @param isAlive liveness predicate over Worker node IDs; must not be {@code null}
   * @return the target Worker node id, or {@code null} if the ring is empty or no
   *         ring node satisfies the predicate
   */
  @Override
  public String routeNode(String key, Predicate<String> isAlive) {
    return ring.locateNode(key, isAlive);
  }
}
