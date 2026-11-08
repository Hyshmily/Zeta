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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.github.hyshmily.zeta.util.Hashes;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * Consistent hash ring backed by sorted arrays for O(log n + k) lookups
 * (design decisions: topology).
 *
 * <p>Each physical node is replicated across the ring with {@code virtualNodeCount}
 * virtual nodes, all mapped to the same physical {@code nodeId}. A rebuild publishes one
 * immutable snapshot through a single {@link AtomicReference}, so readers never observe a
 * partial ring and {@link #locateNode(String)} stays lock-free.
 */
@Internal
@Slf4j
public class ConsistentHashRingImpl implements ConsistentHashRing {

  /** Published by every empty rebuild, so an empty ring never allocates. */
  private static final RingState EMPTY_STATE = new RingState(new int[0], new int[0], new String[0], Set.of());

  /** Maximum Worker IDs shown in the ring-rebuilt INFO log before truncation. */
  private static final int REBUILD_LOG_MAX_NODE_IDS = 20;

  /**
   * Immutable ring snapshot, held only behind an {@link AtomicReference} and
   * always consumed field-wise.
   *
   * <p>A ring slot's physical node is {@code nodes[indexes[slot]]}: {@code indexes} stores the
   * index rather than the node ID, so each slot costs one {@code int}
   * (design decisions: topology).
   */
  private record RingState(
    /* Sorted hash values of all virtual nodes. */
    int[] hashes,
    /* Index into {@code nodes} for each ring slot, aligned with {@code hashes}. */
    int[] indexes,
    /* Sorted physical node IDs: their index is the identity used by the dead-set. */
    String[] nodes,
    /* Unmodifiable set of live physical node IDs. */
    Set<String> live
  ) {}

  /** Number of virtual copies created per physical node on the ring. */
  private final int virtualNodeCount;

  /** Current immutable ring snapshot, thread-safe via {@link AtomicReference}. */
  private final AtomicReference<RingState> currentState = new AtomicReference<>(EMPTY_STATE);

  /**
   * @param virtualNodeCount replicas per physical node (positive)
   * @throws IllegalArgumentException when {@code virtualNodeCount <= 0}
   */
  public ConsistentHashRingImpl(int virtualNodeCount) {
    if (virtualNodeCount <= 0) {
      throw new IllegalArgumentException("virtualNodeCount (" + virtualNodeCount + ") must be > 0");
    }
    this.virtualNodeCount = virtualNodeCount;
  }

  /**
   * Atomically replace the ring with one built from the given live nodes.
   * Each node is replicated {@code virtualNodeCount} times on the ring.
   *
   * <p>Synchronized like {@link #rebuildIfPossible(HealthView)}: the two entry
   * points both publish a whole snapshot, and without a shared monitor a background
   * reconcile could overwrite a caller's explicit rebuild (or vice versa) with a set the
   * other never saw.
   *
   * @param liveNodes the set of physical node IDs to place on the ring;
   *                  must not be {@code null}
   */
  @Override
  public synchronized void rebuild(Set<String> liveNodes) {
    applyRebuild(liveNodes);
  }

  /** Builds and publishes a ring snapshot. Runs under the instance monitor. */
  @GuardedBy("this")
  private RingState applyRebuild(Set<String> liveNodes) {
    if (liveNodes.isEmpty()) {
      currentState.set(EMPTY_STATE);
      return EMPTY_STATE;
    }

    // Sorted iteration: when two physical nodes hash to the same ring slot, the lower node
    // index owns it. Without a total order two instances holding the same node set could
    // pick different owners for the colliding slot and route the same key to different
    // Workers.
    String[] nodes = liveNodes.stream().sorted().toArray(String[]::new);
    int slots = Math.multiplyExact(nodes.length, virtualNodeCount);

    // 1. Place every virtual node. Each vnode is mixed integer arithmetic — no per-vnode
    // String, no TreeMap entry — and the (hash, nodeIdx) pairs are packed into one long[]
    // that the sort below orders and deduplicates in the same pass.
    long[] packed = new long[slots];
    int placed = 0;
    for (int node = 0; node < nodes.length; node++) {
      long base = Hashes.mix64(nodes[node]);
      for (int i = 0; i < virtualNodeCount; i++) {
        // Stable per-node seed: a node's virtual positions depend only on its own ID and
        // the vnode index, so adding or removing another node never shifts them. The 64-bit
        // base keeps two IDs that collide in String.hashCode from sharing their whole set; the
        // golden-ratio stride separates the virtual positions of one node.
        long mixed = Hashes.mix64(base + i * 0x9E3779B97F4A7C15L);
        int hash = (int) (mixed ^ (mixed >>> 32));
        packed[placed++] = ((long) hash << 32) | (node & 0xFFFFFFFFL);
      }
    }

    // 2. Sort the packed pairs. The high 32 bits are the signed hash, so long ordering
    // matches Arrays.binarySearch(int[]); within one hash, the low 32 bits order node
    // indexes ascending, which is what makes the collision winner deterministic.
    Arrays.sort(packed);

    // 3. Drop duplicate hashes. A 32-bit placement space collides at large cluster sizes;
    // the first entry of each equal-hash run wins, so the lexicographically smaller node
    // keeps the slot and a node's virtual positions depend only on its own ID.
    int unique = 1;
    for (int i = 1; i < packed.length; i++) {
      if ((int) (packed[i] >> 32) != (int) (packed[i - 1] >> 32)) {
        unique++;
      }
    }

    int[] hashes = new int[unique];
    int[] indexes = new int[unique];
    int index = 0;
    for (long pair : packed) {
      int hash = (int) (pair >> 32);
      if (index == 0 || hash != hashes[index - 1]) {
        hashes[index] = hash;
        indexes[index] = (int) pair;
        index++;
      }
    }

    // 4. Publish the snapshot atomically.
    RingState ring = new RingState(hashes, indexes, nodes, Set.copyOf(liveNodes));
    currentState.set(ring);
    return ring;
  }

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
  @Override
  public String locateNode(String key, Predicate<String> isAlive) {
    return locateNode(key, currentState.get(), isAlive, null, 0L);
  }

  /**
   * Fast path over the published snapshot: the first node after the key's hash owns the
   * key, and the ring's node set is the set the last rebuild was built from.
   *
   * @param key the business key to route; must not be {@code null}
   * @return the physical node ID, or {@code null} when the ring is empty
   */
  @Override
  public String locateNode(String key) {
    RingState ring = currentState.get();
    int[] hashes = ring.hashes;
    if (hashes.length == 0) {
      return null;
    }
    return ring.nodes[ring.indexes[slotFor(hashes, hash(key))]];
  }

  /**
   * Health-view variant of {@link #locateNode(String, Predicate)}: samples the monotonic
   * clock once per call and judges every probed node against that instant, so a routing
   * walk pays one clock read rather than one per hop.
   *
   * @param key        the business key to route; must not be {@code null}
   * @param healthView the cluster-wide Worker health view; must not be {@code null}
   * @return the physical node ID, or {@code null} if the ring is empty or all nodes are dead
   */
  @Override
  public String locateNode(String key, HealthView healthView) {
    return locateNode(key, currentState.get(), null, healthView, TimeSource.monotonicMillis());
  }

  /**
   * Shared walk for both liveness sources. Exactly one of {@code isAlive} and
   * {@code healthView} is non-null; the split keeps the per-probe dispatch a predictable
   * branch instead of a callback object allocated per routed key.
   */
  private String locateNode(
    String key,
    RingState ring,
    @Nullable Predicate<String> isAlive,
    @Nullable HealthView healthView,
    long now
  ) {
    int[] hashes = ring.hashes;
    int[] indexes = ring.indexes;
    String[] nodes = ring.nodes;
    int slots = hashes.length;
    if (slots == 0) {
      return null;
    }

    int start = slotFor(hashes, hash(key));
    int first = indexes[start];
    if (aliveAt(ring, first, isAlive, healthView, now)) {
      return nodes[first]; // common fast path: one probe, zero allocations
    }

    // Slow path (the start node is down): probe each DISTINCT physical node at most once.
    // One bitmap indexed by physical node; allocated only when the fast path above missed.
    int nodeCount = nodes.length;
    long[] dead = new long[(nodeCount + 63) >>> 6];
    dead[first >>> 6] |= 1L << (first & 63);
    int deadCount = 1;
    int slot = start;

    while (deadCount < nodeCount) {
      if (++slot >= slots) {
        slot = 0;
      }
      if (slot == start) {
        break; // completed a full ring cycle
      }

      int node = indexes[slot];
      int word = node >>> 6;
      long bit = 1L << (node & 63);
      if ((dead[word] & bit) != 0L) {
        continue;
      }
      if (aliveAt(ring, node, isAlive, healthView, now)) {
        return nodes[node];
      }
      dead[word] |= bit;
      deadCount++;
    }

    // The while loop above terminates once all distinct nodes are condemned or a full cycle
    // completes. Per-key detail stays at DEBUG — locateNode runs on every routed key.
    log.debug(
      "Completed a full ring cycle ({} slots) for key '{}' without finding an alive node, " +
        "all ring nodes appear dead or the ring is stale. Discarding this key.",
      slots,
      key
    );
    return null;
  }

  /** One liveness probe: either the caller's predicate or the health view's id lookup. */
  private static boolean aliveAt(
    RingState ring,
    int node,
    @Nullable Predicate<String> isAlive,
    @Nullable HealthView healthView,
    long now
  ) {
    return healthView != null ? healthView.isAlive(ring.nodes[node], now) : isAlive.test(ring.nodes[node]);
  }

  /**
   * Rebuild the ring from the alive-Worker set of the given health view when it
   * differs from the current node set; a no-op (and no log) when unchanged.
   *
   * <p>The node-ID sample in the rebuild log is taken from the snapshot's already-sorted
   * {@code nodes} and capped at {@code REBUILD_LOG_MAX_NODE_IDS} — the Worker counts in the
   * same line already carry the cardinality.
   *
   * @param healthView the current cluster health view; must not be {@code null}
   * @return the alive Worker set this reconciliation is based on — equal to the
   *         ring's node set immediately after the call; callers may reuse it as
   *         the routing liveness predicate without a second
   *         {@link HealthView#getAliveWorkerIds()} materialisation
   */
  @Override
  public synchronized Set<String> rebuildIfPossible(HealthView healthView) {
    RingState ring = currentState.get();
    Set<String> alive = healthView.getAliveWorkerIds();
    if (alive.equals(ring.live)) {
      return ring.live;
    }

    RingState next = applyRebuild(alive);
    log.info("Ring rebuilt: {} -> {} workers [{}]", ring.live.size(), alive.size(), truncateNodeIds(next.nodes));
    return next.live;
  }

  /**
   * First {@code REBUILD_LOG_MAX_NODE_IDS} entries of the already-sorted node array,
   * with an ellipsis marker when it was truncated.
   */
  private static String truncateNodeIds(String[] nodes) {
    int shown = Math.min(nodes.length, REBUILD_LOG_MAX_NODE_IDS);
    String joined = String.join(", ", Arrays.asList(nodes).subList(0, shown));
    return nodes.length > REBUILD_LOG_MAX_NODE_IDS ? joined + ", ..." : joined;
  }

  /**
   * Return the set of live physical node IDs.
   *
   * @return an unmodifiable set of node IDs (never {@code null})
   */
  @Override
  public Set<String> getNodes() {
    return currentState.get().live;
  }

  /**
   * Return the number of virtual copies created per physical node on the ring.
   *
   * @return the virtual node count (positive)
   */
  @Override
  public int getVirtualNodeCount() {
    return virtualNodeCount;
  }

  /**
   * The ring slot at or after {@code hash}, wrapping to zero when the hash sorts past
   * the last slot.
   */
  private static int slotFor(int[] hashes, int hash) {
    int slot = Arrays.binarySearch(hashes, hash);
    if (slot >= 0) {
      return slot;
    }
    slot = ~slot;
    return slot == hashes.length ? 0 : slot;
  }

  /**
   * Compute a 32-bit avalanche-mixed hash of the given string. The ring hashes
   * every reported key on the reporter's flush path, so {@link String#hashCode()}
   * (cached, allocation-free) is passed through the Murmur3 finalizer instead of
   * a full digest (design decisions: topology).
   *
   * @param key the string to hash; must not be {@code null}
   * @return the hash value (may be negative)
   */
  private static int hash(String key) {
    return Hashes.mix32(key.hashCode());
  }
}
