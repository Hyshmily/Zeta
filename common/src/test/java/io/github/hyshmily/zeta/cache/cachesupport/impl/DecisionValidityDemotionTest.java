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
package io.github.hyshmily.zeta.cache.cachesupport.impl;

import static io.github.hyshmily.zeta.constants.ZetaConstants.Version.VERSION_DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.codec.CacheCompressor;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.sharding.HealthView;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the Decision-Validity demotion (ADR-0035): a Worker-sourced HOT
 * entry whose issuing Worker incarnation is dead or restarted is reverted
 * in place to the NORMAL lifecycle on the read path.
 */
class DecisionValidityDemotionTest {

  private Cache<String, Object> caffeineCache;
  private ZetaProperties ttlConfig;
  private HealthView healthView;

  @BeforeEach
  void setUp() {
    caffeineCache = Caffeine.newBuilder().maximumSize(100).build();
    ttlConfig = new ZetaProperties();
    healthView = mock(HealthView.class);
  }

  private ExpireManagerImpl expireManager() {
    return new ExpireManagerImpl(caffeineCache, Runnable::run, ttlConfig, 10, CacheCompressor.NONE, healthView);
  }

  private ExpireManagerImpl expireManagerWithoutHealthView() {
    return new ExpireManagerImpl(caffeineCache, Runnable::run, ttlConfig, 10, CacheCompressor.NONE, null);
  }

  private CacheEntry hotEntry(
    String decisionNodeId,
    long decisionEpoch,
    long normalHardTtlMs,
    long normalSoftTtlMs
  ) {
    long now = System.currentTimeMillis();
    return CacheEntry.builder()
      .value("v")
      .dataVersion(1)
      .isVersionDegraded(false)
      .decisionVersion(42)
      .decisionNodeId(decisionNodeId)
      .decisionEpoch(decisionEpoch)
      .hardTtlMs(3_600_000)
      .hardExpireAtMs(now + 3_600_000)
      .softTtlMs(300_000)
      .softExpireAtMs(now + 300_000)
      .keyState(KeyState.HOT)
      .normalHardTtlMs(normalHardTtlMs)
      .normalSoftTtlMs(normalSoftTtlMs)
      .build();
  }

  private void mockWorker(String nodeId, boolean alive, long epoch) {
    when(healthView.isAlive(nodeId)).thenReturn(alive);
    when(healthView.epochOf(nodeId)).thenReturn(epoch);
  }

  @Test
  void deadWorker_demotesEntryInPlace() {
    mockWorker("w1", false, 5L);
    caffeineCache.put("k", hotEntry("w1", 5L, 60_000, 15_000));

    ExpireManagerImpl em = expireManager();
    Object raw = caffeineCache.getIfPresent("k");
    boolean demoted = em.demoteIfDecisionInvalid("k", raw);

    assertThat(demoted).isTrue();
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(entry.getKeyState()).isEqualTo(KeyState.NORMAL);
    assertThat(entry.getDecisionNodeId()).isNull();
    assertThat(entry.getDecisionVersion()).isEqualTo(VERSION_DEFAULT);
    assertThat(entry.getDecisionEpoch()).isZero();
    assertThat(entry.getHardTtlMs()).isEqualTo(60_000);
    assertThat(entry.getSoftTtlMs()).isEqualTo(15_000);
    assertThat(entry.getHardExpireAtMs())
      .isGreaterThan(System.currentTimeMillis() - 10_000)
      .isLessThanOrEqualTo(System.currentTimeMillis() + 120_000);
    assertThat(entry.getValue()).isEqualTo("v");
  }

  @Test
  void aliveWorkerSameEpoch_noDemotion() {
    mockWorker("w1", true, 5L);
    caffeineCache.put("k", hotEntry("w1", 5L, 60_000, 15_000));

    ExpireManagerImpl em = expireManager();
    boolean demoted = em.demoteIfDecisionInvalid("k", caffeineCache.getIfPresent("k"));

    assertThat(demoted).isFalse();
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(entry.getDecisionNodeId()).isEqualTo("w1");
  }

  @Test
  void restartedWorker_epochMismatch_demotesEvenIfAlive() {
    mockWorker("w1", true, 6L);
    caffeineCache.put("k", hotEntry("w1", 5L, 60_000, 15_000));

    ExpireManagerImpl em = expireManager();
    boolean demoted = em.demoteIfDecisionInvalid("k", caffeineCache.getIfPresent("k"));

    assertThat(demoted).isTrue();
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(entry.getKeyState()).isEqualTo(KeyState.NORMAL);
    assertThat(entry.getDecisionNodeId()).isNull();
  }

  @Test
  void unknownWorker_noHealthRecord_demotes() {
    mockWorker("w1", false, HealthView.UNKNOWN_EPOCH);
    caffeineCache.put("k", hotEntry("w1", 5L, 60_000, 15_000));

    ExpireManagerImpl em = expireManager();
    boolean demoted = em.demoteIfDecisionInvalid("k", caffeineCache.getIfPresent("k"));

    assertThat(demoted).isTrue();
    assertThat(((CacheEntry) caffeineCache.getIfPresent("k")).getKeyState()).isEqualTo(KeyState.NORMAL);
  }

  @Test
  void localEntry_withoutDecisionStamp_neverDemoted() {
    caffeineCache.put(
      "k",
      hotEntry(null, 0L, 60_000, 15_000)
    );

    ExpireManagerImpl em = expireManager();
    boolean demoted = em.demoteIfDecisionInvalid("k", caffeineCache.getIfPresent("k"));

    assertThat(demoted).isFalse();
    assertThat(((CacheEntry) caffeineCache.getIfPresent("k")).getKeyState()).isEqualTo(KeyState.HOT);
  }

  @Test
  void coolEntry_neverDemoted() {
    mockWorker("w1", false, 5L);
    CacheEntry cool = hotEntry("w1", 5L, 60_000, 15_000).withKeyState(KeyState.COOL);
    caffeineCache.put("k", cool);

    ExpireManagerImpl em = expireManager();
    boolean demoted = em.demoteIfDecisionInvalid("k", caffeineCache.getIfPresent("k"));

    assertThat(demoted).isFalse();
    assertThat(((CacheEntry) caffeineCache.getIfPresent("k")).getKeyState()).isEqualTo(KeyState.COOL);
  }

  @Test
  void bareValue_neverDemoted() {
    mockWorker("w1", false, 5L);
    caffeineCache.put("k", "bare");

    ExpireManagerImpl em = expireManager();
    boolean demoted = em.demoteIfDecisionInvalid("k", caffeineCache.getIfPresent("k"));

    assertThat(demoted).isFalse();
    assertThat(caffeineCache.getIfPresent("k")).isEqualTo("bare");
  }

  @Test
  void noHealthView_disablesDemotion() {
    caffeineCache.put("k", hotEntry("w1", 5L, 60_000, 15_000));

    ExpireManagerImpl em = expireManagerWithoutHealthView();
    boolean demoted = em.demoteIfDecisionInvalid("k", caffeineCache.getIfPresent("k"));

    assertThat(demoted).isFalse();
    assertThat(((CacheEntry) caffeineCache.getIfPresent("k")).getKeyState()).isEqualTo(KeyState.HOT);
  }

  @Test
  void entryRestampedByAnotherWorker_betweenCheckAndCompute_notClobbered() {
    // The stale raw carries a dead worker's stamp, but the CURRENT entry in
    // the cache was re-stamped by a live worker: the compute-time re-verification
    // must leave the fresh entry untouched.
    mockWorker("w1", false, 5L);
    mockWorker("w2", true, 9L);
    caffeineCache.put("k", hotEntry("w2", 9L, 60_000, 15_000));

    CacheEntry staleRaw = hotEntry("w1", 5L, 60_000, 15_000);
    ExpireManagerImpl em = expireManager();
    boolean demoted = em.demoteIfDecisionInvalid("k", staleRaw);

    assertThat(demoted).isFalse();
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(entry.getDecisionNodeId()).isEqualTo("w2");
  }

  @Test
  void demotedEntry_isStableOnSecondCheck() {
    mockWorker("w1", false, 5L);
    caffeineCache.put("k", hotEntry("w1", 5L, 60_000, 15_000));

    ExpireManagerImpl em = expireManager();
    em.demoteIfDecisionInvalid("k", caffeineCache.getIfPresent("k"));
    boolean second = em.demoteIfDecisionInvalid("k", caffeineCache.getIfPresent("k"));

    assertThat(second).isFalse();
    assertThat(((CacheEntry) caffeineCache.getIfPresent("k")).getKeyState()).isEqualTo(KeyState.NORMAL);
  }

  @Test
  void zeroNormalTtl_fallsBackToConfiguredDefaults() {
    mockWorker("w1", false, 5L);
    caffeineCache.put("k", hotEntry("w1", 5L, 0, 0));

    ExpireManagerImpl em = expireManager();
    boolean demoted = em.demoteIfDecisionInvalid("k", caffeineCache.getIfPresent("k"));

    assertThat(demoted).isTrue();
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(entry.getHardTtlMs()).isEqualTo(ttlConfig.effectiveHardTtlMs());
    assertThat(entry.getSoftTtlMs()).isEqualTo(ttlConfig.effectiveSoftTtlMs());
  }
}
