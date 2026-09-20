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
package io.github.hyshmily.zeta.hotkeydetector.heavykeeper;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Tests for the snapshot-before-sort contract of
 * {@link HeavyKeeper#snapshotMembersSorted(int)}: every member's count is
 * read ONCE into an immutable {@link HeavyKeeper.MemberSnapshot} pair
 * <em>before</em> the sort runs. Member counts are concurrently mutated
 * (lock-free max-raise in {@code admit}, CAS halving in
 * {@code decayMembership}), and a comparator that re-reads a mutable
 * counter mid-sort can feed TimSort an inconsistent total order — it then
 * aborts with "Comparison method violates its general contract!" (reachable
 * once the membership exceeds TimSort's MIN_MERGE of 32, e.g. k=100
 * introspection). The contract also makes the output deterministic under
 * mutation: each returned list is ordered by its OWN reported counts.
 */
class HeavyKeeperSnapshotTest {

  /**
   * Deterministic ordering: count descending, ties broken on key ascending
   * (the original ConcurrentSkipListMap semantics). Seeded via
   * {@link HeavyKeeper#warm} so the member counts are exact — the sketch's
   * probabilistic decay is bypassed.
   */
  @Test
  void snapshotMembersSorted_shouldSortByCountDescThenKeyAsc() {
    HeavyKeeper hk = new HeavyKeeper(8, 1024, 4, 0.9, 1);
    Map<String, Long> seed = new HashMap<>();
    seed.put("alpha", 50L);
    seed.put("gamma", 50L);
    seed.put("beta", 200L);
    seed.put("delta", 1L);
    hk.warm(seed);

    List<HeavyKeeper.MemberSnapshot> snapshot = hk.snapshotMembersSorted(8);

    assertThat(snapshot).extracting(HeavyKeeper.MemberSnapshot::key).containsExactly("beta", "alpha", "gamma", "delta");
    assertThat(snapshot).extracting(HeavyKeeper.MemberSnapshot::count).containsExactly(200L, 50L, 50L, 1L);
  }

  /**
   * The limit truncates the sorted snapshot to the top-n entries (never the
   * other way around).
   */
  @Test
  void snapshotMembersSorted_shouldLimitToRequestedN() {
    HeavyKeeper hk = new HeavyKeeper(8, 1024, 4, 0.9, 1);
    Map<String, Long> seed = new HashMap<>();
    seed.put("alpha", 50L);
    seed.put("gamma", 50L);
    seed.put("beta", 200L);
    hk.warm(seed);

    List<HeavyKeeper.MemberSnapshot> snapshot = hk.snapshotMembersSorted(2);

    assertThat(snapshot).extracting(HeavyKeeper.MemberSnapshot::key).containsExactly("beta", "alpha");
    assertThat(snapshot).extracting(HeavyKeeper.MemberSnapshot::count).containsExactly(200L, 50L);
  }

  /** An empty membership yields an empty snapshot (no sentinel entries). */
  @Test
  void snapshotMembersSorted_shouldReturnEmptyForNoMembers() {
    HeavyKeeper hk = new HeavyKeeper(8, 1024, 4, 0.9, 1);

    assertThat(hk.snapshotMembersSorted(8)).isEmpty();
  }

  /**
   * The public {@code listTopN} output mirrors the snapshot pairs exactly:
   * each {@link Item} carries the count the ordering was computed from.
   */
  @Test
  void listTopN_shouldReportSnapshotCounts() {
    HeavyKeeper hk = new HeavyKeeper(8, 1024, 4, 0.9, 1);
    Map<String, Long> seed = new HashMap<>();
    seed.put("alpha", 50L);
    seed.put("gamma", 50L);
    seed.put("beta", 200L);
    seed.put("delta", 1L);
    hk.warm(seed);

    List<HeavyKeeper.MemberSnapshot> snapshot = hk.snapshotMembersSorted(8);
    List<Item> items = hk.listTopN(8);

    assertThat(items).hasSameSizeAs(snapshot);
    for (int i = 0; i < items.size(); i++) {
      assertThat(items.get(i).key()).isEqualTo(snapshot.get(i).key());
      assertThat(items.get(i).count()).isEqualTo(snapshot.get(i).count());
    }
  }

  /**
   * Best-effort concurrency loop: {@code list()}/{@code listTopN()} must
   * survive many iterations while other threads mutate member counts — two
   * writers through the lock-free max-raise fast path ({@code addDirect})
   * and a fader through the CAS-halving decay ({@code fading}), the two
   * mutation sites named by the snapshot-before-sort contract. The output
   * invariant is asserted on every read: sorted by the items' own reported
   * counts (descending, ties by key ascending), no duplicates, size <= k.
   */
  @Test
  void list_shouldSurviveConcurrentCountMutation() throws Exception {
    final int k = 100; // > TimSort MIN_MERGE (32): the merge passes run
    HeavyKeeper hk = new HeavyKeeper(k, 1024, 4, 0.9, 1, 8192, 3);
    Map<String, Long> seed = new HashMap<>();
    for (int i = 0; i < k; i++) {
      seed.put("key-" + i, 10L + i);
    }
    hk.warm(seed);

    AtomicBoolean stop = new AtomicBoolean(false);
    AtomicReference<Throwable> mutatorFailure = new AtomicReference<>();
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(3);
    ExecutorService pool = Executors.newFixedThreadPool(3);

    // Two count mutators: hammer existing members through the lock-free
    // accumulateAndGet fast path (and re-admit decay-dropped ones).
    for (int t = 0; t < 2; t++) {
      final int offset = t;
      pool.submit(() -> {
        try {
          start.await();
          long i = offset;
          while (!stop.get()) {
            hk.addDirect("key-" + ((i + offset * 37) % k), 1 + (i % 7));
            i++;
          }
        } catch (Throwable e) {
          mutatorFailure.compareAndSet(null, e);
        } finally {
          done.countDown();
        }
      });
    }
    // One fader: full membership halving while the readers sort — the
    // largest count jumps a live-comparator sort could observe.
    pool.submit(() -> {
      try {
        start.await();
        while (!stop.get()) {
          hk.fading();
          Thread.sleep(1);
        }
      } catch (Throwable e) {
        mutatorFailure.compareAndSet(null, e);
      } finally {
        done.countDown();
      }
    });
    start.countDown();

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    long reads = 0;
    try {
      while (System.nanoTime() < deadline) {
        reads++;
        List<Item> items = (reads % 2 == 0) ? hk.list() : hk.listTopN(k);
        // Keep the expelled queue drained so the fader's decay drops do
        // not fill it and spam the rate-limited WARN (that accounting has
        // its own test).
        hk.expelled().clear();

        String problem = orderingProblem(items, k);
        assertThat(problem).as("read #%d", reads).isNull();
      }
    } finally {
      stop.set(true);
      pool.shutdown();
      assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }

    assertThat(mutatorFailure.get()).isNull();
    assertThat(reads).as("reader iterations completed").isGreaterThan(100);
  }

  /**
   * Plain-Java invariant check (AssertJ per element would dominate the
   * read loop's runtime): returns {@code null} when the list is sorted by
   * its own reported counts (descending, ties by key ascending), has no
   * duplicate keys, non-negative counts, and at most {@code k} entries.
   */
  private static String orderingProblem(List<Item> items, int k) {
    if (items.size() > k) {
      return "size " + items.size() + " exceeds k=" + k;
    }
    Set<String> seen = new HashSet<>();
    for (int i = 0; i < items.size(); i++) {
      Item item = items.get(i);
      if (item.count() < 0) {
        return "negative count at index " + i;
      }
      if (!seen.add(item.key())) {
        return "duplicate key '" + item.key() + "' at index " + i;
      }
      if (i > 0) {
        Item prev = items.get(i - 1);
        boolean ordered =
          prev.count() > item.count() || (prev.count() == item.count() && prev.key().compareTo(item.key()) < 0);
        if (!ordered) {
          return "misordered at index " + i + ": " + describe(items);
        }
      }
    }
    return null;
  }

  /** Compact one-line dump of the offending list for the failure message. */
  private static String describe(List<Item> items) {
    List<String> parts = new ArrayList<>(items.size());
    for (Item item : items) {
      parts.add(item.key() + "=" + item.count());
    }
    return String.join(", ", parts);
  }
}
