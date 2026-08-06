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
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link VersionGuard}, covering degraded/normal version comparison in sync and worker paths.
 */
class VersionGuardTest {

  private Cache<String, Object> cache;

  @BeforeEach
  void setUp() {
    cache = Caffeine.newBuilder().maximumSize(100).build();
  }

  /**
   * Verifies that for two normal (non-degraded) entries, the sync is skipped when the existing version is higher.
   */
  @Test
  void shouldSkipForSync_bothNormal_shouldSkipWhenExistingVersionHigher() {
    cache.put("key", entry(5, false, 0));
    assertThat(VersionGuard.shouldSkipForSync(cache, "key", 4, false)).isTrue();
  }

  /**
   * Verifies that for two normal entries, the sync is not skipped when the incoming version is higher.
   */
  @Test
  void shouldSkipForSync_bothNormal_shouldNotSkipWhenIncomingVersionHigher() {
    cache.put("key", entry(5, false, 0));
    assertThat(VersionGuard.shouldSkipForSync(cache, "key", 6, false)).isFalse();
  }

  /**
   * Verifies that a degraded incoming message is always skipped when the existing entry is normal.
   */
  @Test
  void shouldSkipForSync_existingNormalIncomingDegraded_shouldAlwaysSkip() {
    cache.put("key", entry(5, false, 0));
    assertThat(VersionGuard.shouldSkipForSync(cache, "key", 10, true)).isTrue();
    assertThat(VersionGuard.shouldSkipForSync(cache, "key", 0, true)).isTrue();
  }

  /**
   * Verifies that for two degraded entries, the sync is skipped when the existing degraded version is higher.
   */
  @Test
  void shouldSkipForSync_bothDegraded_shouldSkipWhenExistingVersionHigher() {
    cache.put("key", entry(-10, true, 0));
    assertThat(VersionGuard.shouldSkipForSync(cache, "key", -12, true)).isTrue();
  }

  /**
   * Verifies that a normal (non-degraded) incoming message is always accepted even when the existing entry is degraded with a higher version.
   */
  @Test
  void shouldSkipForSync_existingDegradedIncomingNormal_shouldNotSkip() {
    cache.put("key", entry(-5, true, 0));
    assertThat(VersionGuard.shouldSkipForSync(cache, "key", 1, false)).isFalse();
  }

  /**
   * Verifies that a sync is never skipped when there is no existing entry in the cache.
   */
  @Test
  void shouldSkipForSync_noExistingEntry_shouldNotSkip() {
    assertThat(VersionGuard.shouldSkipForSync(cache, "missing", 1, false)).isFalse();
  }

  /**
   * Verifies that a worker decision is not skipped when the existing entry's
   * decisionVersion is lower than the incoming, regardless of degraded flag.
   * The degraded flag (dataVersion domain) is irrelevant to decision ordering.
   */
  @Test
  void shouldSkipForWorker_degradedEntry_lowerDecisionVersion_shouldNotSkip() {
    cache.put("key", entry(-5, true, 0));
    assertThat(VersionGuard.shouldSkipForWorker(cache, "key", 10, null, 0)).isFalse();
  }

  /**
   * Verifies that a worker decision is not skipped when the incoming decision version is greater than the existing one.
   */
  @Test
  void shouldSkipForWorker_existingNormalLowerVersion_shouldNotSkip() {
    cache.put("key", entry(5, false, 3));
    assertThat(VersionGuard.shouldSkipForWorker(cache, "key", 5, null, 0)).isFalse();
  }

  /**
   * Verifies that a worker decision is skipped when the incoming decision version is less than or equal to the existing one.
   */
  @Test
  void shouldSkipForWorker_existingNormalEqualOrHigherVersion_shouldSkip() {
    cache.put("key", entry(5, false, 5));
    assertThat(VersionGuard.shouldSkipForWorker(cache, "key", 4, null, 0)).isTrue();
    assertThat(VersionGuard.shouldSkipForWorker(cache, "key", 5, null, 0)).isTrue();
  }

  /**
   * Verifies that a worker decision is not skipped when there is no existing entry in the cache.
   */
  @Test
  void shouldSkipForWorker_noExistingEntry_shouldNotSkip() {
    assertThat(VersionGuard.shouldSkipForWorker(cache, "missing", 1, null, 0)).isFalse();
  }

  /**
   * Verifies that for two normal entries with equal version, the sync is skipped.
   */
  @Test
  void shouldSkipForSync_bothNormalEqualVersion_shouldSkip() {
    cache.put("key", entry(5, false, 0));
    assertThat(VersionGuard.shouldSkipForSync(cache, "key", 5, false)).isTrue();
  }

  /**
   * Verifies that a degraded incoming message to a non-existent entry should not skip.
   */
  @Test
  void shouldSkipForSync_noExistingEntryDegradedIncoming_shouldNotSkip() {
    assertThat(VersionGuard.shouldSkipForSync(cache, "missing", 1, true)).isFalse();
  }

  /**
   * Verifies the CacheEntry-overload shouldSkipForSync with null existing entry returns false.
   */
  @Test
  void shouldSkipForSync_cacheEntryOverload_withNull_shouldNotSkip() {
    assertThat(VersionGuard.shouldSkipForSync((CacheEntry) null, 1, false)).isFalse();
  }

  /**
   * Verifies the 4-argument shouldSkipForWorker with null existing entry returns false.
   */
  @Test
  void shouldSkipForWorker_cacheEntryOverload_withNull_shouldNotSkip() {
    assertThat(VersionGuard.shouldSkipForWorker(null, 1, null, 0)).isFalse();
  }

  /**
   * Verifies that a worker decision with equal decision version to existing normal entry is skipped.
   */
  @Test
  void shouldSkipForWorker_existingNormalEqualVersion_shouldSkip() {
    cache.put("key", entry(5, false, 10));
    assertThat(VersionGuard.shouldSkipForWorker(cache, "key", 10, null, 0)).isTrue();
  }

  /**
   * Verifies that both-degraded scenario is skipped when existing degraded version equals incoming.
   */
  @Test
  void shouldSkipForSync_bothDegradedEqualVersion_shouldSkip() {
    cache.put("key", entry(-10, true, 0));
    assertThat(VersionGuard.shouldSkipForSync(cache, "key", -10, true)).isTrue();
  }

  /**
   * Verifies that when the cache contains a non-{@link CacheEntry} object, the worker guard
   * returns false (do not skip).
   */
  @Test
  void shouldSkipForWorker_withNonCacheEntryInCache_shouldNotSkip() {
    cache.put("key", "not-a-cache-entry");
    assertThat(VersionGuard.shouldSkipForWorker(cache, "key", 1, null, 0)).isFalse();
  }

  /**
   * Verifies that when the cache contains a non-{@link CacheEntry} object, the sync guard
   * returns false (do not skip).
   */
  @Test
  void shouldSkipForSync_withNonCacheEntryInCache_shouldNotSkip() {
    cache.put("key", "not-a-cache-entry");
    assertThat(VersionGuard.shouldSkipForSync(cache, "key", 1, false)).isFalse();
  }

  // ── shouldSkipForWorker with nodeId/epoch (P0-2) ──

  /**
   * Verifies that null existing entry is accepted (not skipped).
   */
  @Test
  void shouldSkipForWorker_withEpoch_nullExisting_shouldNotSkip() {
    assertThat(VersionGuard.shouldSkipForWorker(null, 100, "W1", 1)).isFalse();
  }

  /**
   * Verifies that a degraded existing entry with a higher decisionVersion
   * correctly rejects a stale incoming decision — the degraded flag does
   * not bypass decisionVersion ordering (orthogonal version spaces).
   */
  @Test
  void shouldSkipForWorker_withEpoch_degradedEntry_higherExistingDv_shouldSkip() {
    CacheEntry degraded = entry(-5, true, 50).toBuilder().decisionNodeId("W1").decisionEpoch(1).build();
    assertThat(VersionGuard.shouldSkipForWorker(degraded, 1, "W1", 1)).isTrue();
  }

  /**
   * Verifies that a degraded existing entry with a LOWER decisionVersion
   * accepts a legitimate higher incoming decision (same epoch, same nodeId).
   */
  @Test
  void shouldSkipForWorker_withEpoch_degradedEntry_lowerExistingDv_shouldNotSkip() {
    CacheEntry degraded = entry(-5, true, 3).toBuilder().decisionNodeId("W1").decisionEpoch(1).build();
    assertThat(VersionGuard.shouldSkipForWorker(degraded, 5, "W1", 1)).isFalse();
  }

  /**
   * Verifies that a degraded entry accepts a higher-epoch message (Worker restart).
   * The degraded flag does not block epoch-based restart detection.
   */
  @Test
  void shouldSkipForWorker_withEpoch_degradedEntry_higherEpoch_shouldNotSkip() {
    CacheEntry existing = entry(-5, true, 100).toBuilder().decisionNodeId("W1").decisionEpoch(1).build();
    assertThat(VersionGuard.shouldSkipForWorker(existing, 1, "W1", 2)).isFalse();
  }

  /**
   * Verifies that a degraded entry skips a lower-epoch message (stale incarnation).
   * The degraded flag does not bypass epoch ordering.
   */
  @Test
  void shouldSkipForWorker_withEpoch_degradedEntry_lowerEpoch_shouldSkip() {
    CacheEntry existing = entry(-5, true, 100).toBuilder().decisionNodeId("W1").decisionEpoch(2).build();
    assertThat(VersionGuard.shouldSkipForWorker(existing, 1, "W1", 1)).isTrue();
  }

  /**
   * Verifies that a degraded entry accepts a cross-Worker handoff (different nodeId).
   * The degraded flag does not block last-writer-wins convergence.
   */
  @Test
  void shouldSkipForWorker_withEpoch_degradedEntry_differentNodeId_shouldNotSkip() {
    CacheEntry existing = entry(-5, true, 100).toBuilder().decisionNodeId("W1").decisionEpoch(1).build();
    assertThat(VersionGuard.shouldSkipForWorker(existing, 1, "W2", 1)).isFalse();
  }

  /**
   * Verifies that a degraded entry with no Worker history (epoch=0, nodeId=null, dv=0)
   * accepts the first Worker message — the different-nodeId branch handles this.
   */
  @Test
  void shouldSkipForWorker_degradedEntry_noWorkerHistory_firstContact_shouldNotSkip() {
    CacheEntry existing = entry(-5, true, 0);
    assertThat(VersionGuard.shouldSkipForWorker(existing, 1, "W1", 0)).isFalse();
  }

  /**
   * Verifies that incoming epoch higher than existing epoch unconditionally accepts
   * (Worker restart detected — ADR-0010).
   */
  @Test
  void shouldSkipForWorker_withEpoch_higherEpoch_shouldNotSkip() {
    CacheEntry existing = entry(5, false, 100).toBuilder().decisionNodeId("W1").decisionEpoch(1).build();
    assertThat(VersionGuard.shouldSkipForWorker(existing, 10, "W1", 2)).isFalse();
  }

  /**
   * Verifies that same nodeId and existing dv >= incoming dv skips the decision.
   */
  @Test
  void shouldSkipForWorker_withEpoch_existingDvHigher_shouldSkip() {
    CacheEntry existing = entry(5, false, 100).toBuilder().decisionNodeId("W1").decisionEpoch(1).build();
    assertThat(VersionGuard.shouldSkipForWorker(existing, 99, "W1", 1)).isTrue();
  }

  /**
   * Verifies that same nodeId and existing dv == incoming dv skips the decision.
   */
  @Test
  void shouldSkipForWorker_withEpoch_sameNodeId_existingDvEqual_shouldSkip() {
    CacheEntry existing = entry(5, false, 100).toBuilder().decisionNodeId("W1").decisionEpoch(1).build();
    assertThat(VersionGuard.shouldSkipForWorker(existing, 100, "W1", 1)).isTrue();
  }

  /**
   * Verifies that same nodeId and incoming dv > existing dv does NOT skip.
   */
  @Test
  void shouldSkipForWorker_withEpoch_incomingDvHigher_shouldNotSkip() {
    CacheEntry existing = entry(5, false, 100).toBuilder().decisionNodeId("W1").decisionEpoch(1).build();
    assertThat(VersionGuard.shouldSkipForWorker(existing, 101, "W1", 1)).isFalse();
  }

  /**
   * Verifies that same epoch but different nodeId with lower incoming dv is accepted
   * (different nodeId → accept unconditionally, last-writer-wins).
   */
  @Test
  void shouldSkipForWorker_withEpoch_differentNodeId_lowerDv_shouldNotSkip() {
    CacheEntry existing = entry(5, false, 100).toBuilder().decisionNodeId("W1").decisionEpoch(1).build();
    // Same epoch, different nodeId → accept unconditionally
    assertThat(VersionGuard.shouldSkipForWorker(existing, 50, "W2", 1)).isFalse();
  }

  /**
   * Verifies that same epoch, different nodeId with equal dv is skipped.
   */
  @Test
  void shouldSkipForWorker_withEpoch_differentNodeId_equalDv_shouldNotSkip() {
    CacheEntry existing = entry(5, false, 100).toBuilder().decisionNodeId("W1").decisionEpoch(1).build();
    // Same epoch, different nodeId → accept unconditionally (last-writer-wins)
    assertThat(VersionGuard.shouldSkipForWorker(existing, 100, "W2", 1)).isFalse();
  }

  /**
   * Verifies that same epoch, different nodeId with higher incoming dv is accepted
   * (legitimate new-owner assertion with a newer decision).
   */
  @Test
  void shouldSkipForWorker_withEpoch_differentNodeId_higherDv_shouldNotSkip() {
    CacheEntry existing = entry(5, false, 100).toBuilder().decisionNodeId("W1").decisionEpoch(1).build();
    // Same epoch, different nodeId → accept unconditionally
    assertThat(VersionGuard.shouldSkipForWorker(existing, 101, "W2", 1)).isFalse();
  }

  // ── Cache-level shouldSkipForWorker with epoch (P0-2) ──

  /**
   * Verifies that the cache-level overload with nodeId/epoch works correctly.
   */
  @Test
  void shouldSkipForWorker_cacheLevel_withNodeIdEpoch_shouldDelegate() {
    CacheEntry existing = entry(5, false, 100).toBuilder().decisionNodeId("W1").decisionEpoch(1).build();
    cache.put("key", existing);

    // Same nodeId, existing dv >= incoming → skip
    assertThat(VersionGuard.shouldSkipForWorker(cache, "key", 99, "W1", 1)).isTrue();

    // Same nodeId, incoming dv > existing → accept
    assertThat(VersionGuard.shouldSkipForWorker(cache, "key", 101, "W1", 1)).isFalse();

    // Different nodeId → accept unconditionally
    assertThat(VersionGuard.shouldSkipForWorker(cache, "key", 50, "W2", 1)).isFalse();

    // Different nodeId → accept unconditionally
    assertThat(VersionGuard.shouldSkipForWorker(cache, "key", 101, "W2", 1)).isFalse();

    // Higher epoch (regardless of nodeId) → accept
    assertThat(VersionGuard.shouldSkipForWorker(cache, "key", 10, "W1", 2)).isFalse();
  }

  /**
   * Verifies that the cache-level overload with nodeId/epoch returns false for missing keys.
   */
  @Test
  void shouldSkipForWorker_cacheLevel_withNodeIdEpoch_missingKey_shouldNotSkip() {
    assertThat(VersionGuard.shouldSkipForWorker(cache, "missing", 1, "W1", 1)).isFalse();
  }

  /**
   * Verifies that incoming epoch lower than existing epoch skips (stale incarnation).
   */
  @Test
  void shouldSkipForWorker_withEpoch_lowerEpoch_shouldSkip() {
    CacheEntry existing = entry(5, false, 100).toBuilder().decisionNodeId("W1").decisionEpoch(2).build();
    assertThat(VersionGuard.shouldSkipForWorker(existing, 1, "W1", 1)).isTrue();
  }

  /**
   * Verifies that both-degraded sync is NOT skipped when incoming version is higher.
   */
  @Test
  void shouldSkipForSync_bothDegradedHigherIncoming_shouldNotSkip() {
    cache.put("key", entry(-10, true, 0));
    assertThat(VersionGuard.shouldSkipForSync(cache, "key", -1, true)).isFalse();
  }

  private static CacheEntry entry(long dataVersion, boolean degraded, long decisionVersion) {
    return CacheEntry.builder()
      .value("v")
      .dataVersion(dataVersion)
      .isVersionDegraded(degraded)
      .decisionVersion(decisionVersion)
      .hardTtlMs(300_000L)
      .hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(30_000L)
      .softExpireAtMs(30_000L)
      .keyState(KeyState.NORMAL)
      .normalHardTtlMs(300_000L)
      .normalSoftTtlMs(30_000L)
      .build();
  }
}
