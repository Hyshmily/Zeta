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

import io.github.hyshmily.zeta.util.Internal;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Consistent hash ring for Worker shard routing, maintained by the App from
 * the cluster-wide {@link HealthView}: the ring rebuilds itself from the
 * alive-Worker set via {@link #rebuildIfPossible(HealthView)} and routes
 * keys with the health view as the liveness source.
 *
 * <p>Each physical node is replicated across the ring with a fixed number of
 * virtual nodes, all mapped to the same physical {@code nodeId}. The entire
 * ring is replaced atomically via {@link #rebuild(Set)} — no partial mutations,
 * so {@link #locateNode} stays lock-free on the hot path
 * (design decisions: topology).
 *
 * <p>Routing follows the classic consistent-hash contract: a key maps to the
 * first live node at or after its hash on the ring (wrapping around), so
 * adding or removing a node only re-routes the keys of the changed span.
 */
@Internal
public interface ConsistentHashRing {
  /**
   * Atomically replace the ring with one built from the given live nodes.
   * Each node is replicated {@code virtualNodeCount} times on the ring.
   *
   * @param liveNodes the set of physical node IDs to place on the ring;
   *                  must not be {@code null}
   */
  void rebuild(Set<String> liveNodes);

  /**
   * Return the physical node responsible for the given business key, skipping nodes
   * that fail the liveness predicate.
   *
   * <p>Performs a binary search to locate the starting virtual node and then walks
   * the sorted array circularly until a live physical node is found.
   *
   * @param key     the business key to route; must not be {@code null}
   * @param isAlive predicate that returns {@code true} if a node is considered alive;
   *                must not be {@code null}
   * @return the physical node ID, or {@code null} if the ring is empty or all nodes are dead
   */
  String locateNode(String key, Predicate<String> isAlive);

  /**
   * Routes {@code key} on the current ring snapshot with <b>no</b> liveness predicate:
   * every physical node on a published ring was alive in the set the last rebuild was
   * built from.
   *
   * <p>This is the reporter's fast path: the caller has just reconciled the ring, so
   * re-probing every node through the alive set would only repeat a tautology. A node
   * that dies after the rebuild still receives the batch, and the consumer discards it
   * through the incarnation check — the same bounded staleness the alive-set predicate
   * already has.
   *
   * @param key the business key to route; must not be {@code null}
   * @return the physical node ID, or {@code null} when the ring is empty
   */
  String locateNode(String key);

  /**
   * {@link #locateNode(String, Predicate)} with the cluster health view as the
   * liveness source: one method reference, no alive-set materialisation.
   *
   * <p>Implementations override this form to sample the monotonic clock once per
   * walk instead of once per probed node.
   *
   * @param key        the business key to route; must not be {@code null}
   * @param healthView the cluster-wide Worker health view; must not be {@code null}
   * @return the physical node ID, or {@code null} if the ring is empty or all nodes are dead
   */
  default String locateNode(String key, HealthView healthView) {
    return locateNode(key, healthView::isAlive);
  }

  /**
   * Rebuild the ring from the alive-Worker set of the given health view when it
   * differs from the current node set; a no-op (and no log) when unchanged.
   *
   * @param healthView the current cluster health view; must not be {@code null}
   * @return the alive Worker set this reconciliation is based on — equal to the
   *         ring's node set immediately after the call; routing reads that same
   *         snapshot through {@link #locateNode(String)}
   */
  Set<String> rebuildIfPossible(HealthView healthView);

  /**
   * Return the set of live physical node IDs.
   *
   * @return an unmodifiable set of node IDs (never {@code null})
   */
  Set<String> getNodes();

  /**
   * Return the number of virtual copies created per physical node on the ring.
   *
   * @return the virtual node count (positive)
   */
  int getVirtualNodeCount();
}
