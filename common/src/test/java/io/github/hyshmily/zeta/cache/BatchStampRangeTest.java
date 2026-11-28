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
package io.github.hyshmily.zeta.cache;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.InvalidatePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.StalePolicy;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.util.clock.MonotonicStampClock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Batched version allocation against the real stamp clock (review 2026-10
 * P2-3): a batch's versions are one contiguous block, a single write ordered
 * against the batch is applied or superseded all-or-nothing, and a batch load
 * stamps each key when its own load runs.
 */
class BatchStampRangeTest {

  @Test
  void batchInvalidate_allocatesOneContiguousVersionBlock() {
    try (
      StampTestSupport support = new StampTestSupport(
        new MonotonicStampClock(1L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS)
      )
    ) {
      // Seed one single allocation so the block does not start at the clock origin.
      support.cache.get("seed", () -> "v", GetPolicy.defaults());
      long seedStamp = support.cache.table().getIfPresent("seed").getDataVersion();

      List<String> keys = List.of("k1", "k2", "k3", "k4");
      support.cache.invalidate(keys, new InvalidatePolicy(InvalidatePolicy.Scope.BOTH, false));

      // The block went out on the wire as one INVALIDATE_BATCH: contiguous (one
      // clock reservation) and entirely above the seed allocation; exact
      // adjacency to the seed is not asserted — the wall clock may open a fresh
      // window between the two calls.
      assertThat(support.batchedInvalidations).hasSize(keys.size());
      List<Long> sorted = support.batchedInvalidations.stream().map(SyncMessage.Entry::version).sorted().toList();
      for (int i = 1; i < sorted.size(); i++) {
        assertThat(sorted.get(i)).as("block stamps are contiguous").isEqualTo(sorted.get(i - 1) + 1);
      }
      assertThat(sorted.get(0)).isGreaterThan(seedStamp);
      // A subsequent single write ranks above the whole block: the reservation
      // owned the range exclusively.
      support.cache.putThrough("after", "v", 0L, 0L);
      assertThat(support.cache.table().getIfPresent("after").getDataVersion()).isGreaterThan(
        sorted.get(sorted.size() - 1)
      );
    }
  }

  @Test
  void writeAfterBatchInvalidate_winsAndSurvives() {
    try (
      StampTestSupport support = new StampTestSupport(
        new MonotonicStampClock(1L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS)
      )
    ) {
      support.cache.get("k", () -> "old", GetPolicy.defaults());
      support.cache.invalidate(List.of("k"), new InvalidatePolicy(InvalidatePolicy.Scope.BOTH, false));
      // The write's stamp is newer than every batch version: the batch must
      // not suppress it.
      support.cache.putThrough("k", "fresh", 0, 0);
      assertThat(support.cache.<String>peek("k")).isEqualTo("fresh");
    }
  }

  @Test
  void writeBeforeBatchInvalidate_isSuperseded() {
    try (
      StampTestSupport support = new StampTestSupport(
        new MonotonicStampClock(1L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS)
      )
    ) {
      support.cache.putThrough("k", "fresh", 0, 0);
      // The batch's block is newer than the write's stamp: every entry applies.
      support.cache.invalidate(List.of("k"), new InvalidatePolicy(InvalidatePolicy.Scope.BOTH, false));
      assertThat(support.cache.<String>peek("k")).isNull();
    }
  }

  @Test
  void aBatchOfPureHits_shouldNotEvaluateTheTtlSuppliers() {
    try (
      StampTestSupport support = new StampTestSupport(
        new MonotonicStampClock(1L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS)
      )
    ) {
      support.cache.get("k", () -> "v", GetPolicy.defaults());
      AtomicInteger evaluations = new AtomicInteger();
      GetPolicy policy = new GetPolicy(
        () -> {
          evaluations.incrementAndGet();
          return 60_000L;
        },
        () -> {
          evaluations.incrementAndGet();
          return 0L;
        },
        StalePolicy.SOFT_REFRESH,
        false,
        false
      );

      Map<String, Object> result = support.cache.get(List.of("k"), k -> "ignored", policy);

      assertThat(result).containsEntry("k", "v");
      assertThat(evaluations).as("a pure hit resolves no TTL").hasValue(0);
    }
  }

  @Test
  void batchLoad_stampsEachKeyWhenItsOwnLoadRuns() {
    try (
      StampTestSupport support = new StampTestSupport(
        new MonotonicStampClock(1L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS)
      )
    ) {
      support.cache.get("seed", () -> "v", GetPolicy.defaults());
      long seedStamp = support.cache.table().getIfPresent("seed").getDataVersion();

      Map<String, Object> result = support.cache.get(List.of("a", "b", "c"), k -> "v-" + k, GetPolicy.defaults());
      assertThat(result).containsEntry("a", "v-a").containsEntry("b", "v-b").containsEntry("c", "v-c");

      long a = support.cache.table().getIfPresent("a").getDataVersion();
      long b = support.cache.table().getIfPresent("b").getDataVersion();
      long c = support.cache.table().getIfPresent("c").getDataVersion();
      // Each stamp is allocated by the load it belongs to, so a key that never loads spends none
      // and no key can carry a stamp minted before a concurrent invalidation.
      assertThat(a).isGreaterThan(seedStamp);
      assertThat(b).isGreaterThan(a);
      assertThat(c).isGreaterThan(b);
    }
  }
}
