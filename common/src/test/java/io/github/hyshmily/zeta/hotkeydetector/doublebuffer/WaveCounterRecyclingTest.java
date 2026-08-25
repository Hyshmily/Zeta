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
package io.github.hyshmily.zeta.hotkeydetector.doublebuffer;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.ref.SoftReference;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Correctness assertions for the 2026-08-07 WaveCounter optimizations:
 * the #2a empty-tide early return (no quiescence spin on empty cycles) and
 * the #2b adder-recycling pool (no cross-cycle contamination, instance
 * reuse, pool lifecycle).
 *
 * <p>Deliberately NOT tagged {@code performance} (unlike
 * {@link WaveCounterTest}) so these run in every CI/local {@code mvn test}
 * — they are correctness guards, not benchmarks.
 */
class WaveCounterRecyclingTest {

  private List<Map<String, Long>> batches;
  private Consumer<Map<String, Long>> consumer;
  private WaveCounter counter;

  @BeforeEach
  void setUp() {
    batches = new ArrayList<>();
    consumer = batches::add;
    counter = new WaveCounter(consumer);
  }

  private static void invokeDeliver(WaveCounter c) {
    try {
      Method m = WaveCounter.class.getDeclaredMethod("tide");
      m.setAccessible(true);
      m.invoke(c);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  private static long mergedTotal(List<Map<String, Long>> batches) {
    return batches
      .stream()
      .flatMap(m -> m.values().stream())
      .mapToLong(Long::longValue)
      .sum();
  }

  /**
   * An empty tide delivers nothing and leaves the counter reusable (the
   * 1ms quiescence window is paid on every cycle since 2026-08-07, parked
   * rather than spun — this asserts the semantic, not the timing).
   */
  @Test
  void emptyTide_shouldDeliverNothing() {
    invokeDeliver(counter);
    assertThat(batches).isEmpty();
    counter.count("x", 3);
    invokeDeliver(counter);
    assertThat(mergedTotal(batches)).isEqualTo(3);
  }

  /**
   * #1 approximate-size capacity guard: only NEW keys are dropped at
   * capacity; tracked keys keep counting.  Verifies the O(1) counter
   * drives the same documented semantics as the old size() check.
   */
  @Test
  void capacityGuard_approximateSize_shouldDropNewColdKeys() throws Exception {
    List<Map<String, Long>> captured = new ArrayList<>();
    WaveCounter c = new WaveCounter(captured::add, 3, 50, 0.5, null);
    c.count("a", 1);
    c.count("b", 1);
    c.count("c", 1);
    c.count("d", 1); // 4th distinct key at capacity 3 → dropped
    c.count("a", 10); // tracked key keeps counting
    c.destroy();
    Map<String, Long> merged = new java.util.HashMap<>();
    for (Map<String, Long> b : captured) {
      for (Map.Entry<String, Long> e : b.entrySet()) {
        merged.merge(e.getKey(), e.getValue(), Long::sum);
      }
    }
    assertThat(merged).containsEntry("a", 11L).containsEntry("b", 1L).containsEntry("c", 1L);
    assertThat(merged).doesNotContainKey("d");
  }

  /**
   * #2b adder recycling: a key counted 5 in tide 1, then 3 in tide 2, must
   * deliver exactly [5, 3] — the recycled adder must not carry the
   * previous cycle's 5 into the next (steal-time reset).
   */
  @Test
  void recycledAdder_shouldNotContaminateAcrossTides() {
    counter.count("k", 5);
    invokeDeliver(counter);
    assertThat(mergedTotal(batches)).isEqualTo(5);
    counter.count("k", 3);
    invokeDeliver(counter);
    assertThat(mergedTotal(batches)).isEqualTo(8);
    assertThat(batches.get(batches.size() - 1)).containsEntry("k", 3L);
  }

  /**
   * #2b: the steal must actually reuse the pooled instance — the adder
   * that served tide 1 must be the very object re-inserted for tide 2's
   * first count (identity, not a copy).
   */
  @Test
  void steal_shouldReuseSameAdderInstance() throws Exception {
    counter.count("k", 1);
    invokeDeliver(counter);
    Field poolField = WaveCounter.class.getDeclaredField("ebbReservoir");
    poolField.setAccessible(true);
    @SuppressWarnings("unchecked")
    SoftReference<ConcurrentHashMap<String, LongAdder>> poolRef =
      (SoftReference<ConcurrentHashMap<String, LongAdder>>) poolField.get(counter);
    LongAdder original = poolRef.get().get("k");
    assertThat(original).isNotNull();
    counter.count("k", 1);
    Field resField = WaveCounter.class.getDeclaredField("reservoir");
    resField.setAccessible(true);
    ConcurrentHashMap<String, LongAdder> table = (ConcurrentHashMap<String, LongAdder>) resField.get(counter);
    assertThat(table.get("k")).isSameAs(original);
  }

  /** #2b: clear() and destroy() must drop the adder pool reference. */
  @Test
  void clearAndDestroy_shouldDropPool() throws Exception {
    Field poolField = WaveCounter.class.getDeclaredField("ebbReservoir");
    poolField.setAccessible(true);
    counter.count("k", 1);
    invokeDeliver(counter);
    assertThat(poolField.get(counter)).isNotNull();
    counter.clear();
    assertThat(poolField.get(counter)).isNull();
    counter.count("k", 1);
    invokeDeliver(counter);
    assertThat(poolField.get(counter)).isNotNull();
    counter.destroy();
    assertThat(poolField.get(counter)).isNull();
  }

  /**
   * Counting-beacon drift expiry (2026-08-07, amended by ADR-0049): a
   * promoted key that stops being counted must leave the hot set within a
   * few tides.  With the ADR-0049 every-other-tide halving sweep the memory
   * is 4 tides: the key survives up to three quiet tides (still a member
   * after tide 4) and is gone by the fifth.  The quiet tides count
   * >= MIN_PROMOTION_KEYS distinct keys so the decay actually runs
   * (sub-minimum snapshots freeze evidence by design — see the decay gate).
   */
  @Test
  void drift_promotedKey_shouldExpireWithinFewTides() {
    counter.count("top", 80);
    for (int i = 0; i < 1000; i++) {
      counter.count("k" + i, 5);
    }
    invokeDeliver(counter); // tide 1: promote (decay sweep runs; nothing to decay)
    assertThat(isBeaconMember(counter, "top")).as("promoted at tide 1").isTrue();
    for (int t = 2; t <= 4; t++) {
      for (int i = 0; i < 20; i++) {
        counter.count("k" + i, 1); // >= MIN_PROMOTION_KEYS so the decay runs; "top" stays quiet
      }
      invokeDeliver(counter);
      assertThat(isBeaconMember(counter, "top"))
        .as("4-tide memory: still a member after " + (t - 1) + " quiet tides")
        .isTrue();
    }
    for (int i = 0; i < 20; i++) {
      counter.count("k" + i, 1);
    }
    invokeDeliver(counter); // tide 5: the second decay sweep of the quiet run
    assertThat(isBeaconMember(counter, "top")).as("quiet key must leave the hot set by tide 5").isFalse();
  }

  /**
   * Counting-beacon stability (2026-08-07): a key re-promoted every tide
   * must stay a member — no ping-pong from the decay.
   */
  @Test
  void drift_stableHotKey_shouldNeverLeave() {
    for (int t = 0; t < 5; t++) {
      counter.count("top", 80);
      for (int i = 0; i < 1000; i++) {
        counter.count("k" + i, 5);
      }
      invokeDeliver(counter);
      assertThat(isBeaconMember(counter, "top")).as("stable hot key at tide " + (t + 1)).isTrue();
    }
  }

  /**
   * Counting-beacon re-entry (2026-08-07): after the old key decays away,
   * a NEW relative hot spot must be promotable on any tide — the
   * freeze-at-hotLimit problem of the bit-only design is gone.
   */
  @Test
  void drift_newHotKey_shouldEnterAnyTide() {
    for (int t = 0; t < 2; t++) {
      counter.count("old", 80);
      for (int i = 0; i < 1000; i++) {
        counter.count("k" + i, 5);
      }
      invokeDeliver(counter);
    }
    assertThat(isBeaconMember(counter, "old")).isTrue();
    counter.count("new", 80);
    for (int i = 0; i < 1000; i++) {
      counter.count("k" + i, 5);
    }
    invokeDeliver(counter);
    assertThat(isBeaconMember(counter, "new")).as("new relative hot spot must enter on the next tide").isTrue();
  }

  /**
   * Count of live 4-bit counting rooms (>= 1) — mirrors decayCounts().
   */
  private static int activeBeaconSize(WaveCounter c) throws Exception {
    Field f = WaveCounter.class.getDeclaredField("beacon");
    f.setAccessible(true);
    long[] beacon = (long[]) f.get(c);
    int active = 0;
    for (int i = 0; i < beacon.length; i++) {
      long word = beacon[i];
      if (word == 0) {
        continue;
      }
      for (int r = 0; r < 16; r++) {
        if (((word >>> (r << 2)) & 0x3) != 0) {
          active++;
        }
      }
    }
    return active;
  }

  /**
   * Min-signal guard (2026-08-07, Caffeine discipline): a snapshot below
   * MIN_PROMOTION_KEYS carries no meaningful distribution — promotion
   * must be skipped entirely so startup noise cannot burn beacon slots.
   */
  @Test
  void tinySnapshot_shouldSkipPromotion() throws Exception {
    for (int i = 0; i < 5; i++) {
      counter.count("tiny" + i, 20);
    }
    invokeDeliver(counter);
    assertThat(activeBeaconSize(counter)).as("sub-minimum snapshot must not promote").isZero();
  }

  /**
   * Histogram promotion (2026-08-07): a key carrying the bulk of a
   * LOW-traffic cycle (80 counts vs per-key average ≈ 5) must be promoted
   * — the legacy fixed threshold (100) silently failed below 100
   * counts/cycle.  The promotion is distribution-scale-free.
   */
  @Test
  void lowTraffic_topKey_shouldBePromoted() {
    counter.count("top", 80);
    for (int i = 0; i < 1000; i++) {
      counter.count("k" + i, 5);
    }
    invokeDeliver(counter);
    assertThat(isBeaconMember(counter, "top")).as("relative hot spot must be promoted at low traffic").isTrue();
  }

  /**
   * Histogram promotion: at high traffic the boundary must not drag
   * average keys into the hot set when the universe fits within
   * hotLimit... it does promote every non-zero key when the universe
   * (1001) is far below hotLimit (4096) — free slots make that the
   * optimal allocation — so this asserts only that the top key is
   * promoted and the promotion count is bounded by hotLimit.
   */
  @Test
  void highTraffic_promotion_isBoundedByHotLimit() throws Exception {
    counter.count("top", 1_000_000);
    for (int i = 0; i < 1000; i++) {
      counter.count("k" + i, 20);
    }
    invokeDeliver(counter);
    assertThat(isBeaconMember(counter, "top")).as("top key promoted").isTrue();
    assertThat(activeBeaconSize(counter)).as("promotion bounded by hotLimit").isLessThanOrEqualTo(4096);
  }

  /**
   * Per-field saturation guard (2026-08-08): a trace room saturated by
   * OTHER keys' promotions (evidence 3) must not block a new hot key's
   * count seeding — the former joint guard (c < 3 && t < 3) skipped the
   * promotion entirely and even reported "newly active" while leaving
   * the key cold.
   */
  @Test
  void promote_pollutedTraceRoom_shouldStillActivate() throws Exception {
    int[] rooms = beaconRooms(counter, "hot");
    setRole(counter, rooms[0], 0, 0); // count room empty
    setRole(counter, rooms[1], 2, 3); // trace room saturated by others
    boolean newlyActive = invokePromote(counter, "hot");
    assertThat(newlyActive).as("must report a newly consumed slot").isTrue();
    assertThat(isBeaconMember(counter, "hot")).as("must be promoted despite the polluted trace room").isTrue();
  }

  /**
   * Per-field saturation guard (2026-08-08): a saturated count room must
   * not block the trace seeding of a genuinely hot key — the former
   * joint guard left such a key permanently unpromotable.
   */
  @Test
  void promote_saturatedCountRoom_shouldStillSeedTrace() throws Exception {
    int[] rooms = beaconRooms(counter, "hot");
    setRole(counter, rooms[0], 0, 3); // count room saturated by others
    setRole(counter, rooms[1], 2, 0); // trace room empty
    invokePromote(counter, "hot");
    assertThat(isBeaconMember(counter, "hot"))
      .as("hot key must be promotable despite the saturated count room")
      .isTrue();
  }

  /**
   * Per-field saturation guard (2026-08-08): a stable member whose count
   * room is saturated must still refresh its trace every tide — the
   * former joint guard let the trace decay out within 2 tides (the
   * ping-pong the 4-tide memory was designed to prevent).
   */
  @Test
  void promote_saturatedCountRoom_refreshesMemberTrace() throws Exception {
    int[] rooms = beaconRooms(counter, "hot");
    setRole(counter, rooms[0], 0, 3); // count room saturated by others
    setRole(counter, rooms[1], 2, 1); // member, trace one tide from expiry
    boolean newlyActive = invokePromote(counter, "hot");
    assertThat(newlyActive).as("room was already active — no new slot").isFalse();
    assertThat(getRole(counter, rooms[1], 2)).as("trace must be refreshed 1 → 2").isEqualTo(2);
  }

  /**
   * SWAR decay (2026-08-08): the bit-parallel halving must reproduce the
   * per-lane ladder ({@code 3→1, 2→1, 1→0, 0→0}) for both role fields,
   * and the active count must be POST-decay — a room whose count
   * evidence was 1 is free for the promotion scan this tide (the old
   * pre-decay count would have reported 3 here).
   */
  @Test
  void decay_swar_matchesPerLaneLadder() throws Exception {
    Field f = WaveCounter.class.getDeclaredField("beacon");
    f.setAccessible(true);
    long[] beacon = (long[]) f.get(counter);
    long word = 0;
    word |= 3L << 0; // room 0 count: 3 → 1
    word |= 1L << 2; // room 0 trace: 1 → 0
    word |= 2L << 4; // room 1 count: 2 → 1
    word |= 0L << 6; // room 1 trace: 0
    word |= 1L << 8; // room 2 count: 1 → 0 (frees the slot)
    word |= 2L << 10; // room 2 trace: 2 → 1
    beacon[0] = word;

    Method m = WaveCounter.class.getDeclaredMethod("decayCounts");
    m.setAccessible(true);
    int active = (int) m.invoke(counter);

    // rooms 0 (count 3→1) and 1 (count 2→1) live; room 2 (count 1→0) freed
    assertThat(active).as("post-decay active rooms").isEqualTo(2);
    assertThat(beacon[0])
      .as("decayed word must match the per-lane ladder")
      .isEqualTo((1L << 0) | (1L << 4) | (1L << 10));
  }

  /**
   * Hot-path drain (2026-08-08): the batch drain must recycle the pooled
   * adder across tides, mirroring the cold-path steal test — tide 1
   * allocates (pool empty), tide 2's waveTo steals the very same instance.
   * The stolen adder lands in the pre-swap table, which becomes the NEW
   * pool after tide 2, so the identity is asserted on the pool.
   */
  @Test
  void hotPath_shouldRecycleAdderAcrossTides() throws Exception {
    markHot(counter, "hot");
    counter.count("hot", 1);
    invokeDeliver(counter);
    Field poolField = WaveCounter.class.getDeclaredField("ebbReservoir");
    poolField.setAccessible(true);
    @SuppressWarnings("unchecked")
    SoftReference<ConcurrentHashMap<String, LongAdder>> poolRef =
      (SoftReference<ConcurrentHashMap<String, LongAdder>>) poolField.get(counter);
    LongAdder original = poolRef.get().get("hot");
    assertThat(original).isNotNull();
    counter.count("hot", 1);
    invokeDeliver(counter);
    @SuppressWarnings("unchecked")
    SoftReference<ConcurrentHashMap<String, LongAdder>> poolRef2 =
      (SoftReference<ConcurrentHashMap<String, LongAdder>>) poolField.get(counter);
    assertThat(poolRef2.get().get("hot")).as("tide 2 waveTo must steal the pooled adder").isSameAs(original);
  }

  /**
   * Bitmap sweep (2026-08-08): batch discharges (every opMaxCount) and
   * destroy must deliver hot counts exactly — the claimed-slot sweep must
   * lose nothing.
   */
  @Test
  void bitmapDrain_shouldDeliverExactly() throws Exception {
    markHot(counter, "hot");
    for (int i = 0; i < 1000; i++) {
      counter.count("hot", 1);
    }
    counter.destroy();
    assertThat(mergedTotal(batches)).isEqualTo(1000);
  }

  /**
   * Dead-writer residual (proposal A): a writer thread ends with hot
   * counts still resident in its local map; the next tide must deliver
   * them — the dead thread is drained lock-free, and the registry entry
   * is reaped.
   */
  @Test
  void deadWriter_residual_shouldBeDelivered() throws Exception {
    markHot(counter, "dead-key");
    Thread w = new Thread(() -> {
      for (int i = 0; i < 5; i++) {
        counter.count("dead-key", 1);
      }
    });
    w.start();
    w.join();
    Thread.sleep(10);
    invokeDeliver(counter);
    counter.destroy();

    assertThat(mergedTotal(batches)).isEqualTo(5);
  }

  /**
   * Decay gate (2026-08-12, adversarial audit H2): a sub-minimum snapshot
   * (< {@code MIN_PROMOTION_KEYS} distinct keys) skips the promotion scan,
   * so the halving decay must be skipped with it — otherwise the whole hot
   * set decays to zero within two tides with nothing to re-seed it
   * (evidence 2→1→0), silently routing every key down the cold path for
   * the duration of the small workload.  Evidence stays frozen, exactly
   * like empty tides (ADR-0038).
   */
  @Test
  void smallWorkload_hotSetShouldSurvive() {
    markHot(counter, "hot");
    for (int t = 0; t < 3; t++) {
      counter.count("hot", 80);
      for (int i = 0; i < 4; i++) {
        counter.count("k" + i, 5);
      }
      invokeDeliver(counter);
      assertThat(isBeaconMember(counter, "hot"))
        .as("sub-minimum tides must not strip the hot set (tide " + (t + 1) + ")")
        .isTrue();
    }
  }

  /**
   * Drain signal on the cross-thread recovery sweep (2026-08-12,
   * adversarial audit H1): {@code destroy()} calls
   * {@code Ceils.reconcile} from the deliverer side, and the sweep must
   * flip the drainStamp exactly like {@code tryDrainInto} — otherwise a
   * writer's lock-free fast add races the sweep with no stamp change to
   * trigger the post-check recovery.  Asserts the flip-restore (fresh even
   * epoch) and that the sweep drains the map into the shared table.
   */
  @Test
  void reconcile_shouldFlipAndRestoreDrainStamp() throws Exception {
    markHot(counter, "k");
    Thread w = new Thread(() -> counter.count("k", 1));
    w.start();
    w.join();
    Thread.sleep(10);
    @SuppressWarnings("unchecked")
    ConcurrentHashMap<Thread, Object> registry =
      (ConcurrentHashMap<Thread, Object>) registryField(counter).get(counter);
    Object ceils = registry.get(w);
    Field stampField = ceils.getClass().getDeclaredField("drainStamp");
    stampField.setAccessible(true);
    long before = (long) stampField.get(ceils);

    // The cross-thread recovery path destroy() uses (WaveCounter.reconcile).
    Method m = WaveCounter.class.getDeclaredMethod("reconcile", ceils.getClass());
    m.setAccessible(true);
    m.invoke(counter, ceils);

    long after = (long) stampField.get(ceils);
    assertThat(after).as("stamp restored to a fresh even epoch (s+2)").isEqualTo(before + 2);
    @SuppressWarnings("unchecked")
    ConcurrentHashMap<String, LongAdder> table =
      (ConcurrentHashMap<String, LongAdder>) reservoirField(counter).get(counter);
    assertThat(table.get("k").sum()).as("reconcile merges the residual into the shared table").isEqualTo(1);
    Field sizeField = ceils.getClass().getDeclaredField("size");
    sizeField.setAccessible(true);
    assertThat((int) sizeField.get(ceils)).as("local map drained").isZero();
  }

  /**
   * Periodic-tide drain bookkeeping: {@code Ceils.tryDrainInto} must reset
   * the local-map size after {@code waveTo}, exactly like {@code drainInto},
   * {@code drainDead} and {@code reconcile}.  Without the size reset a
   * live hot writer stays "non-empty" forever, so every later tide pays
   * the lock/drain-stamp/in-flight path for an already-empty map.
   */
  @Test
  void tryDrainInto_shouldResetSize() throws Exception {
    markHot(counter, "k");
    counter.count("k", 3);

    @SuppressWarnings("unchecked")
    ConcurrentHashMap<Thread, Object> registry =
      (ConcurrentHashMap<Thread, Object>) registryField(counter).get(counter);
    Object ceils = registry.get(Thread.currentThread());
    assertThat(ceils).as("current writer must be registered").isNotNull();

    Field sizeField = ceils.getClass().getDeclaredField("size");
    sizeField.setAccessible(true);
    assertThat((int) sizeField.get(ceils)).as("precondition: local map non-empty").isPositive();

    @SuppressWarnings("unchecked")
    ConcurrentHashMap<String, LongAdder> table =
      (ConcurrentHashMap<String, LongAdder>) reservoirField(counter).get(counter);
    Field mergesField = WaveCounter.class.getDeclaredField("mergesInFlight");
    mergesField.setAccessible(true);
    Object merges = mergesField.get(counter);
    Field ebbField = WaveCounter.class.getDeclaredField("ebbReservoir");
    ebbField.setAccessible(true);
    @SuppressWarnings("unchecked")
    SoftReference<ConcurrentHashMap<String, LongAdder>> ebb =
      (SoftReference<ConcurrentHashMap<String, LongAdder>>) ebbField.get(counter);

    Method m = ceils
      .getClass()
      .getDeclaredMethod(
        "tryDrainInto",
        ConcurrentHashMap.class,
        WaveCounter.PaddedMergesInFlight.class,
        SoftReference.class
      );
    m.setAccessible(true);
    assertThat((boolean) m.invoke(ceils, table, merges, ebb)).as("tryLock drain should succeed").isTrue();

    assertThat((int) sizeField.get(ceils)).as("tryDrainInto must reset size").isZero();
    assertThat(table.get("k").sum()).as("drained hot count must reach the shared table").isEqualTo(3);
  }

  /**
   * Ceils pooling: a dead writer's fully-drained local map is returned to
   * the pool at the tide, and the FIRST hot count of a NEW writer claims
   * the very same instance instead of allocating (identity, not a copy) —
   * the per-request-thread allocation-churn fix (see
   * {@code WaveCounter#ceilPool}).
   */
  @Test
  void deadWriter_shouldPoolItsCeils_forReuse() throws Exception {
    markHot(counter, "dead-key");
    Thread w = new Thread(() -> counter.count("dead-key", 1));
    w.start();
    w.join();
    Thread.sleep(10);
    invokeDeliver(counter); // tide 1: reap the dead writer, pool its map

    @SuppressWarnings("unchecked")
    ConcurrentLinkedQueue<Object> pool =
      (ConcurrentLinkedQueue<Object>) poolField(counter).get(counter);
    assertThat(pool).as("dead writer's map enters the pool").hasSize(1);
    Object pooled = pool.peek();

    // A NEW thread's first hot count must claim the pooled instance.
    markHot(counter, "hot-key");
    Thread w2 = new Thread(() -> counter.count("hot-key", 1));
    w2.start();
    w2.join();
    Thread.sleep(10);
    @SuppressWarnings("unchecked")
    ConcurrentHashMap<Thread, Object> registry =
      (ConcurrentHashMap<Thread, Object>) registryField(counter).get(counter);
    assertThat(registry.get(w2)).as("new writer claims the pooled map").isSameAs(pooled);
    assertThat(pool).as("claim empties the pool").isEmpty();
  }

  /**
   * Ceils pooling: repeated dead-writer churn must not grow the pool — the
   * reclaimed map is claimed by the next writer, so the pool oscillates
   * between empty and one, and every cycle's counts are still delivered
   * exactly (the pooled map's data never crosses writers).
   */
  @Test
  void deadWriterChurn_shouldNotGrowThePool() throws Exception {
    markHot(counter, "k");
    for (int cycle = 0; cycle < 3; cycle++) {
      Thread w = new Thread(() -> counter.count("k", 1));
      w.start();
      w.join();
      Thread.sleep(10);
      invokeDeliver(counter);
    }
    @SuppressWarnings("unchecked")
    ConcurrentLinkedQueue<Object> pool =
      (ConcurrentLinkedQueue<Object>) poolField(counter).get(counter);
    assertThat(pool).as("pool holds at most the last dead writer's map").hasSizeLessThanOrEqualTo(1);
    assertThat(mergedTotal(batches)).as("churn loses no hot counts").isEqualTo(3);
  }

  /**
   * Capacity overshoot headroom: new keys are admitted until the table
   * reaches {@code capacity + capacity/10} distinct keys — the drop (whose
   * policy biases against NEW keys) stays off the steady-state edge; the
   * 111th distinct key at capacity 100 is the first one dropped.
   */
  @Test
  void capacityGuard_overshoot_admitsNewKeysWithinHeadroom() throws Exception {
    List<Map<String, Long>> captured = new ArrayList<>();
    WaveCounter c = new WaveCounter(captured::add, 100, 50, 0.5, null);
    for (int i = 0; i < 110; i++) {
      c.count("k" + i, 1);
    }
    c.destroy();
    assertThat(captured.get(captured.size() - 1)).as("headroom (10) admits up to 110 keys").hasSize(110);

    captured.clear();
    WaveCounter c2 = new WaveCounter(captured::add, 100, 50, 0.5, null);
    for (int i = 0; i < 111; i++) {
      c2.count("k" + i, 1);
    }
    c2.destroy();
    assertThat(captured.get(captured.size() - 1)).as("the 111th key is dropped").hasSize(110);
  }

  /**
   * Saturated renewal numerator (2026-08-12, adversarial audit M2): in
   * saturation (remain >= hotLimit, scan skipped) the renewal must count
   * ONLY beacon members that earned the threshold — cold keys waiting for
   * a slot are not "active hot slots".  With 1200 stale members (≈1178
   * active bit1 rooms after collision) and 600 new earners, the old
   * numerator read 600/≈1178 ≈ 0.51 (healthy) and the squatting evidence
   * never fired; the corrected numerator reads ~0 (distress), and the set
   * self-heals by the 4-tide decay (ADR-0049 sweep period).
   */
  @Test
  void saturated_renewal_shouldCountOnlyMembers() throws Exception {
    for (int i = 0; i < 1200; i++) {
      markHot(counter, "m" + i);
    }
    for (int i = 0; i < 600; i++) {
      counter.count("k" + i, 100);
    }
    invokeDeliver(counter);
    Object governor = governorOf(counter);
    Field ratesField = governor.getClass().getDeclaredField("rates");
    ratesField.setAccessible(true);
    Object rates = ratesField.get(governor);
    Field emaField = rates.getClass().getDeclaredField("smoothedRenewal");
    emaField.setAccessible(true);
    double smoothed = (double) emaField.get(rates);
    assertThat(smoothed)
      .as("renewal counts only members, not the 600 new earners")
      .isLessThan(0.1);
  }

  private static Object governorOf(WaveCounter c) throws Exception {
    Field f = WaveCounter.class.getDeclaredField("moonsTidalForce");
    f.setAccessible(true);
    return f.get(c);
  }

  private static java.lang.reflect.Field poolField(WaveCounter c) throws Exception {
    java.lang.reflect.Field f = WaveCounter.class.getDeclaredField("ceilPool");
    f.setAccessible(true);
    return f;
  }

  private static java.lang.reflect.Field registryField(WaveCounter c) throws Exception {
    java.lang.reflect.Field f = WaveCounter.class.getDeclaredField("hotRegistry");
    f.setAccessible(true);
    return f;
  }

  private static java.lang.reflect.Field reservoirField(WaveCounter c) throws Exception {
    java.lang.reflect.Field f = WaveCounter.class.getDeclaredField("reservoir");
    f.setAccessible(true);
    return f;
  }

  /**
   * tryLock-skip delivery guarantee (proposal A): a writer whose local map
   * is locked mid-add at tide time is skipped for that cycle, but the data
   * must still be delivered — by the writer's own flush-clock discharge
   * (50ms) and by destroy()'s blocking drain.  Hot-path counts therefore
   * stay exact even with a 5ms deliver loop racing sustained writers.
   */
  @Test
  void tryLockSkip_shouldNeverLoseHotCounts() throws Exception {
    markHot(counter, "hot-key");
    Thread deliverer = new Thread(() -> {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          Thread.sleep(5);
          invokeDeliver(counter);
        }
      } catch (InterruptedException e) {
        // fall through to the final delivery
      }
      invokeDeliver(counter);
    });
    deliverer.start();

    int threadCount = 8;
    int perThread = 5_000;
    long expected = (long) threadCount * perThread;
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threadCount);
    for (int i = 0; i < threadCount; i++) {
      pool.submit(() -> {
        try {
          start.await();
          for (int j = 0; j < perThread; j++) {
            counter.count("hot-key", 1);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          done.countDown();
        }
      });
    }
    start.countDown();
    assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
    pool.shutdown();
    deliverer.interrupt();
    deliverer.join(10_000);
    counter.destroy();

    assertThat(mergedTotal(batches)).isEqualTo(expected);
  }

  private static boolean isBeaconMember(WaveCounter c, String key) {
    try {
      Field f = WaveCounter.class.getDeclaredField("beacon");
      f.setAccessible(true);
      long[] beacon = (long[]) f.get(c);
      Field m = WaveCounter.class.getDeclaredField("beaconMask");
      m.setAccessible(true);
      int mask = (int) m.get(c);
      int h = mixHash(key.hashCode());
      int bit1 = h & mask;
      int count = (int) ((beacon[bit1 >>> 4] >>> ((bit1 & 15) << 2)) & 0x3);
      if (count == 0) {
        return false;
      }
      int bit2 = rehash(h) & mask;
      return ((beacon[bit2 >>> 4] >>> (((bit2 & 15) << 2) + 2)) & 0x3) >= 1;
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  /** The two beacon rooms ({@code bit1}, {@code bit2}) a key maps to. */
  private static int[] beaconRooms(WaveCounter c, String key) throws Exception {
    Field m = WaveCounter.class.getDeclaredField("beaconMask");
    m.setAccessible(true);
    int mask = (int) m.get(c);
    int h = mixHash(key.hashCode());
    return new int[] { h & mask, rehash(h) & mask };
  }

  /** Write a 2-bit role evidence (0-3) at the given offset of the room. */
  private static void setRole(WaveCounter c, int bit, int offset, int value) throws Exception {
    Field f = WaveCounter.class.getDeclaredField("beacon");
    f.setAccessible(true);
    long[] beacon = (long[]) f.get(c);
    int idx = bit >>> 4;
    int shift = ((bit & 15) << 2) + offset;
    beacon[idx] = (beacon[idx] & ~(0x3L << shift)) | ((long) value << shift);
  }

  /** Read a 2-bit role evidence at the given offset of the room. */
  private static int getRole(WaveCounter c, int bit, int offset) throws Exception {
    Field f = WaveCounter.class.getDeclaredField("beacon");
    f.setAccessible(true);
    long[] beacon = (long[]) f.get(c);
    return (int) ((beacon[bit >>> 4] >>> (((bit & 15) << 2) + offset)) & 0x3);
  }

  private static boolean invokePromote(WaveCounter c, String key) throws Exception {
    Method m = WaveCounter.class.getDeclaredMethod("promoteToBeacon", int.class);
    m.setAccessible(true);
    return (boolean) m.invoke(c, mixHash(key.hashCode()));
  }

  private static void markHot(WaveCounter c, String key) {
    try {
      Field f = WaveCounter.class.getDeclaredField("beacon");
      f.setAccessible(true);
      long[] beacon = (long[]) f.get(c);
      Field m = WaveCounter.class.getDeclaredField("beaconMask");
      m.setAccessible(true);
      int mask = (int) m.get(c);
      int h = mixHash(key.hashCode());
      int bit1 = h & mask;
      beacon[bit1 >>> 4] = beacon[bit1 >>> 4] | (2L << ((bit1 & 15) << 2));
      int bit2 = rehash(h) & mask;
      beacon[bit2 >>> 4] = beacon[bit2 >>> 4] | (2L << (((bit2 & 15) << 2) + 2));
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  private static int mixHash(int h) {
    h ^= h >>> 17;
    h *= 0xed5ad4bb;
    h ^= h >>> 11;
    h *= 0xac4c1b51;
    h ^= h >>> 15;
    return h;
  }

  private static int rehash(int h) {
    h *= 0x31848bab;
    h ^= h >>> 14;
    return h;
  }

  private static void setBit(java.util.concurrent.atomic.AtomicLongArray bits, int mask, int bit) {
    int word = bit & mask;
    bits.getAndSet(word >>> 6, bits.get(word >>> 6) | (1L << (word & 63)));
  }

  private static boolean getBit(java.util.concurrent.atomic.AtomicLongArray bits, int mask, int bit) {
    int word = bit & mask;
    return (bits.get(word >>> 6) & (1L << (word & 63))) != 0;
  }
}
