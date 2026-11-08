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
package io.github.hyshmily.zeta.sync;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link VersionGuard}: single-space sync comparison and the Worker decision domain.
 */
class VersionGuardTest {

  private Cache<String, CacheEntry> cache;

  @BeforeEach
  void setUp() {
    cache = Caffeine.newBuilder().maximumSize(100).build();
  }

  // ── shouldSkipForSync / shouldSkipForRefresh: single stamp space (design decisions: version stamps) ──

  /** Row 1: a newer incoming stamp applies over a real resident stamp. */
  @Test
  void shouldSkipForSync_newerIncoming_shouldNotSkip() {
    cache.put("key", entry(5, 0));
    assertThat(VersionGuard.shouldSkipForSync(cache.getIfPresent("key"), 6)).isFalse();
  }

  /** Row 3: an older incoming stamp never rolls a real resident stamp back. */
  @Test
  void shouldSkipForSync_olderIncoming_shouldSkip() {
    cache.put("key", entry(5, 0));
    assertThat(VersionGuard.shouldSkipForSync(cache.getIfPresent("key"), 4)).isTrue();
  }

  /** Row 4: an unknown resident stamp (0) accepts any real incoming stamp. */
  @Test
  void shouldSkipForSync_unknownResident_shouldAcceptAnyRealStamp() {
    cache.put("key", entry(0, 0));
    assertThat(VersionGuard.shouldSkipForSync(cache.getIfPresent("key"), 4)).isFalse();
  }

  /** The unknown resident stamp stays permissive even against a very large one. */
  @Test
  void shouldSkipForSync_unknownResident_acceptsLargeStamp() {
    cache.put("key", entry(0, 0));
    assertThat(VersionGuard.shouldSkipForSync(cache.getIfPresent("key"), Long.MAX_VALUE)).isFalse();
  }

  /** The plain sync guard skips an equal stamp (the writer's own path uses refresh). */
  @Test
  void shouldSkipForSync_equalStamp_shouldSkip() {
    cache.put("key", entry(5, 0));
    assertThat(VersionGuard.shouldSkipForSync(cache.getIfPresent("key"), 5)).isTrue();
  }

  /** The REFRESH receiver lets an equal stamp apply (the heal path). */
  @Test
  void shouldSkipForRefresh_equalStamp_shouldNotSkip() {
    cache.put("key", entry(5, 0));
    assertThat(VersionGuard.shouldSkipForRefresh(cache, "key", 5)).isFalse();
  }

  /** The REFRESH receiver still skips a strictly older incoming stamp. */
  @Test
  void shouldSkipForRefresh_olderIncoming_shouldSkip() {
    cache.put("key", entry(5, 0));
    assertThat(VersionGuard.shouldSkipForRefresh(cache, "key", 4)).isTrue();
  }

  /** Absence never skips: there is nothing to protect. */
  @Test
  void shouldSkipForSync_noExistingEntry_shouldNotSkip() {
    assertThat(VersionGuard.shouldSkipForSync(cache.getIfPresent("missing"), 1)).isFalse();
  }

  /** Entry-level overload: null existing entry returns false. */
  @Test
  void shouldSkipForSync_cacheEntryOverload_withNull_shouldNotSkip() {
    assertThat(VersionGuard.shouldSkipForSync((CacheEntry) null, 1)).isFalse();
  }

  // ── shouldSkipForWorker with nodeId/epoch (P0-2) ──

  /**
   * Verifies that null existing entry is accepted (not skipped).
   */
  @Test
  void shouldSkipForWorker_withEpoch_nullExisting_shouldNotSkip() {
    assertThat(VersionGuard.shouldSkipForWorker((CacheEntry) null, stamp(100, "W1", 1))).isFalse();
  }

  /**
   * Verifies that a existing entry with a higher decisionVersion
   * correctly rejects a stale incoming decision — the data stamp does
   * not bypass decisionVersion ordering (orthogonal version spaces).
   */
  @Test
  void shouldSkipForWorker_withEpoch_existingEntry_higherExistingDv_shouldSkip() {
    CacheEntry existing = EntryDraft.of(entry(5, 50)).decision(DecisionStamp.hot(50, "W1", 1)).build();
    assertThat(VersionGuard.shouldSkipForWorker(existing, stamp(1, "W1", 1))).isTrue();
  }

  /**
   * Verifies that a existing entry with a LOWER decisionVersion
   * accepts a legitimate higher incoming decision (same epoch, same nodeId).
   */
  @Test
  void shouldSkipForWorker_withEpoch_existingEntry_lowerExistingDv_shouldNotSkip() {
    CacheEntry existing = EntryDraft.of(entry(5, 3)).decision(DecisionStamp.hot(3, "W1", 1)).build();
    assertThat(VersionGuard.shouldSkipForWorker(existing, stamp(5, "W1", 1))).isFalse();
  }

  /**
   * Verifies that a existing entry accepts a higher-epoch message (Worker restart).
   * The data stamp does not block epoch-based restart detection.
   */
  @Test
  void shouldSkipForWorker_withEpoch_existingEntry_higherEpoch_shouldNotSkip() {
    CacheEntry existing = EntryDraft.of(entry(-5, 100)).decision(DecisionStamp.hot(100, "W1", 1)).build();
    assertThat(VersionGuard.shouldSkipForWorker(existing, stamp(1, "W1", 2))).isFalse();
  }

  /**
   * Verifies that a existing entry skips a lower-epoch message (stale incarnation).
   * The data stamp does not bypass epoch ordering.
   */
  @Test
  void shouldSkipForWorker_withEpoch_existingEntry_lowerEpoch_shouldSkip() {
    CacheEntry existing = EntryDraft.of(entry(-5, 100)).decision(DecisionStamp.hot(100, "W1", 2)).build();
    assertThat(VersionGuard.shouldSkipForWorker(existing, stamp(1, "W1", 1))).isTrue();
  }

  /**
   * Verifies that a same-nodeId epoch-rollback skip is counted. A persistently
   * growing counter is the observable signature of the lost-epoch-file +
   * clock-regression scenario: the new incarnation is muted until the resident
   * entries expire, and the counter makes that visible.
   */
  @Test
  void shouldSkipForWorker_epochRollback_shouldCountSkip() {
    long before = VersionGuard.epochRegressSkips();
    CacheEntry existing = EntryDraft.of(entry(-5, 100)).decision(DecisionStamp.hot(100, "W1", 2)).build();
    assertThat(VersionGuard.shouldSkipForWorker(existing, stamp(1, "W1", 1))).isTrue();
    assertThat(VersionGuard.epochRegressSkips()).isEqualTo(before + 1);
  }

  /**
   * Verifies that a existing entry accepts a cross-Worker handoff (different nodeId).
   * The data stamp does not block last-writer-wins convergence.
   */
  @Test
  void shouldSkipForWorker_withEpoch_existingEntry_differentNodeId_shouldNotSkip() {
    CacheEntry existing = EntryDraft.of(entry(-5, 100)).decision(DecisionStamp.hot(100, "W1", 1)).build();
    assertThat(VersionGuard.shouldSkipForWorker(existing, stamp(1, "W2", 1))).isFalse();
  }

  /**
   * Verifies that a existing entry with no Worker history (epoch=0, nodeId=null, dv=0)
   * accepts the first Worker message — the different-nodeId branch handles this.
   */
  @Test
  void shouldSkipForWorker_existingEntry_noWorkerHistory_firstContact_shouldNotSkip() {
    CacheEntry existing = entry(-5, 0);
    assertThat(VersionGuard.shouldSkipForWorker(existing, stamp(1, "W1", 0))).isFalse();
  }

  /**
   * Verifies that incoming epoch higher than existing epoch unconditionally accepts
   * (Worker restart detected — design decisions: heartbeat and health).
   */
  @Test
  void shouldSkipForWorker_withEpoch_higherEpoch_shouldNotSkip() {
    CacheEntry existing = EntryDraft.of(entry(5, 100)).decision(DecisionStamp.hot(100, "W1", 1)).build();
    assertThat(VersionGuard.shouldSkipForWorker(existing, stamp(10, "W1", 2))).isFalse();
  }

  /**
   * Verifies that same nodeId and existing dv >= incoming dv skips the decision.
   */
  @Test
  void shouldSkipForWorker_withEpoch_existingDvHigher_shouldSkip() {
    CacheEntry existing = EntryDraft.of(entry(5, 100)).decision(DecisionStamp.hot(100, "W1", 1)).build();
    assertThat(VersionGuard.shouldSkipForWorker(existing, stamp(99, "W1", 1))).isTrue();
  }

  /**
   * Verifies that same nodeId and existing dv == incoming dv skips the decision.
   */
  @Test
  void shouldSkipForWorker_withEpoch_sameNodeId_existingDvEqual_shouldSkip() {
    CacheEntry existing = EntryDraft.of(entry(5, 100)).decision(DecisionStamp.hot(100, "W1", 1)).build();
    assertThat(VersionGuard.shouldSkipForWorker(existing, stamp(100, "W1", 1))).isTrue();
  }

  /**
   * Verifies that same nodeId and incoming dv > existing dv does NOT skip.
   */
  @Test
  void shouldSkipForWorker_withEpoch_incomingDvHigher_shouldNotSkip() {
    CacheEntry existing = EntryDraft.of(entry(5, 100)).decision(DecisionStamp.hot(100, "W1", 1)).build();
    assertThat(VersionGuard.shouldSkipForWorker(existing, stamp(101, "W1", 1))).isFalse();
  }

  /**
   * Verifies that same epoch but different nodeId with lower incoming dv is accepted
   * (different nodeId → accept unconditionally, last-writer-wins).
   */
  @Test
  void shouldSkipForWorker_withEpoch_differentNodeId_lowerDv_shouldNotSkip() {
    CacheEntry existing = EntryDraft.of(entry(5, 100)).decision(DecisionStamp.hot(100, "W1", 1)).build();
    // Same epoch, different nodeId → accept unconditionally
    assertThat(VersionGuard.shouldSkipForWorker(existing, stamp(50, "W2", 1))).isFalse();
  }

  /**
   * Verifies that same epoch, different nodeId with equal dv is accepted
   * (different nodeId → accept unconditionally, last-writer-wins).
   */
  @Test
  void shouldSkipForWorker_withEpoch_differentNodeId_equalDv_shouldNotSkip() {
    CacheEntry existing = EntryDraft.of(entry(5, 100)).decision(DecisionStamp.hot(100, "W1", 1)).build();
    // Same epoch, different nodeId → accept unconditionally (last-writer-wins)
    assertThat(VersionGuard.shouldSkipForWorker(existing, stamp(100, "W2", 1))).isFalse();
  }

  /**
   * Verifies that same epoch, different nodeId with higher incoming dv is accepted
   * (legitimate new-owner assertion with a newer decision).
   */
  @Test
  void shouldSkipForWorker_withEpoch_differentNodeId_higherDv_shouldNotSkip() {
    CacheEntry existing = EntryDraft.of(entry(5, 100)).decision(DecisionStamp.hot(100, "W1", 1)).build();
    // Same epoch, different nodeId → accept unconditionally
    assertThat(VersionGuard.shouldSkipForWorker(existing, stamp(101, "W2", 1))).isFalse();
  }

  // ── shouldSkipForWorker via the resident entry (P0-2) ──

  /**
   * Verifies that the entry-level guard with nodeId/epoch works correctly on
   * an entry fetched from the cache.
   */
  @Test
  void shouldSkipForWorker_residentEntry_withNodeIdEpoch_shouldDelegate() {
    CacheEntry existing = EntryDraft.of(entry(5, 100)).decision(DecisionStamp.hot(100, "W1", 1)).build();
    cache.put("key", existing);
    CacheEntry resident = cache.getIfPresent("key");

    // Same nodeId, existing dv >= incoming → skip
    assertThat(VersionGuard.shouldSkipForWorker(resident, stamp(99, "W1", 1))).isTrue();

    // Same nodeId, incoming dv > existing → accept
    assertThat(VersionGuard.shouldSkipForWorker(resident, stamp(101, "W1", 1))).isFalse();

    // Different nodeId → accept unconditionally
    assertThat(VersionGuard.shouldSkipForWorker(resident, stamp(50, "W2", 1))).isFalse();

    // Different nodeId → accept unconditionally
    assertThat(VersionGuard.shouldSkipForWorker(resident, stamp(101, "W2", 1))).isFalse();

    // Higher epoch (regardless of nodeId) → accept
    assertThat(VersionGuard.shouldSkipForWorker(resident, stamp(10, "W1", 2))).isFalse();
  }

  /**
   * Verifies that incoming epoch lower than existing epoch skips (stale incarnation).
   */
  @Test
  void shouldSkipForWorker_withEpoch_lowerEpoch_shouldSkip() {
    CacheEntry existing = EntryDraft.of(entry(5, 100)).decision(DecisionStamp.hot(100, "W1", 2)).build();
    assertThat(VersionGuard.shouldSkipForWorker(existing, stamp(1, "W1", 1))).isTrue();
  }

  // ── shouldSkipForHot: an equal decision re-applies only to a NORMAL resident ──

  /**
   * A resident demoted to NORMAL keeps the decision stamp; replaying the same
   * HOT decision must re-promote it instead of being skipped as a duplicate.
   */
  @Test
  void shouldSkipForHot_equalDecisionOnNormalResident_shouldNotSkip() {
    CacheEntry existing = entryWithDecision(KeyState.NORMAL);
    assertThat(VersionGuard.shouldSkipForHot(existing, stamp(100, "W1", 2))).isFalse();
  }

  /**
   * The same decision on a resident that is still HOT is a duplicate — the
   * re-promotion exception is scoped to NORMAL.
   */
  @Test
  void shouldSkipForHot_equalDecisionOnHotResident_shouldSkip() {
    CacheEntry existing = entryWithDecision(KeyState.HOT);
    assertThat(VersionGuard.shouldSkipForHot(existing, stamp(100, "W1", 2))).isTrue();
  }

  /** COOL is an in-force decision too: an equal duplicate must not re-promote it. */
  @Test
  void shouldSkipForHot_equalDecisionOnCoolResident_shouldSkip() {
    CacheEntry existing = entryWithDecision(KeyState.COOL);
    assertThat(VersionGuard.shouldSkipForHot(existing, stamp(100, "W1", 2))).isTrue();
  }

  /** The NORMAL exception only covers equality: a strictly newer resident still wins. */
  @Test
  void shouldSkipForHot_olderDecisionOnNormalResident_shouldSkip() {
    CacheEntry existing = entryWithDecision(KeyState.NORMAL);
    assertThat(VersionGuard.shouldSkipForHot(existing, stamp(99, "W1", 2))).isTrue();
  }

  private static DecisionStamp stamp(long version, String nodeId, long epoch) {
    return DecisionStamp.hot(version, nodeId, epoch);
  }

  private static CacheEntry entryWithDecision(KeyState keyState) {
    return EntryDraft.of(entry(5, 100)).decision(DecisionStamp.hot(100, "W1", 2)).keyState(keyState).build();
  }

  private static CacheEntry entry(long dataVersion, long decisionVersion) {
    return EntryDraft.blank(null)
      .value("v")
      .version(dataVersion)
      .decision(DecisionStamp.hot(decisionVersion, null, 0L))
      .ttl(300_000L, 30_000L, 300_000L, 30_000L)
      .expiryAt(Long.MAX_VALUE, 30_000L)
      .keyState(KeyState.NORMAL)
      .build();
  }
}
