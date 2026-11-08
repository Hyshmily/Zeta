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
package io.github.hyshmily.zeta.cache.cachesupport.lifecycle;

import static io.github.hyshmily.zeta.constants.ZetaConstants.Version.VERSION_DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryMutations;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.sharding.HealthView;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the Decision-Validity demotion (design decisions: hot state): a Worker-sourced HOT
 * entry whose issuing Worker incarnation is dead or restarted is reverted
 * in place to the NORMAL lifecycle on the read path.
 */
class DecisionValidityDemotionTest {

  private Cache<String, CacheEntry> caffeineCache;
  private ZetaProperties ttlConfig;
  private HealthView healthView;

  @BeforeEach
  void setUp() {
    caffeineCache = Caffeine.newBuilder().maximumSize(100).build();
    ttlConfig = new ZetaProperties();
    healthView = mock(HealthView.class);
  }

  private EntryLifecycleImpl entryLifecycle() {
    return new EntryLifecycleImpl(caffeineCache, ttlConfig, healthView);
  }

  private EntryLifecycleImpl entryLifecycleWithoutHealthView() {
    return new EntryLifecycleImpl(caffeineCache, ttlConfig, null);
  }

  private CacheEntry normalEntry() {
    long now = System.currentTimeMillis();
    return EntryDraft.blank(null)
      .value("v")
      .version(1)
      .ttl(300_000, 0, 300_000, 0)
      .expiryAt(now + 300_000, 0)
      .keyState(KeyState.NORMAL)
      .build();
  }

  private CacheEntry hotEntry(String decisionNodeId, long decisionEpoch, long normalHardTtlMs, long normalSoftTtlMs) {
    long now = System.currentTimeMillis();
    return EntryDraft.blank(null)
      .value("v")
      .version(1)
      .decision(DecisionStamp.hot(42, decisionNodeId, decisionEpoch))
      .ttl(3_600_000, 300_000, normalHardTtlMs, normalSoftTtlMs)
      .expiryAt(now + 3_600_000, now + 300_000)
      .keyState(KeyState.HOT)
      .build();
  }

  private void mockWorker(String nodeId, boolean alive, long epoch) {
    when(healthView.isAlive(nodeId)).thenReturn(alive);
    when(healthView.epochOf(nodeId)).thenReturn(epoch);
    when(healthView.isCurrentIncarnation(eq(nodeId), anyLong())).thenAnswer(
      inv -> alive && ((long) inv.getArgument(1)) == epoch
    );
  }

  @Test
  void deadWorker_demotesEntryInPlace() {
    mockWorker("w1", false, 5L);
    caffeineCache.put("k", hotEntry("w1", 5L, 60_000, 15_000));

    EntryLifecycleImpl em = entryLifecycle();
    CacheEntry raw = caffeineCache.getIfPresent("k");
    EntryMutations.Applied demoted = em.demoteIfRemoteNodeIdInvalid("k", raw);

    assertThat(demoted).isNotNull();
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

    EntryLifecycleImpl em = entryLifecycle();
    EntryMutations.Applied demoted = em.demoteIfRemoteNodeIdInvalid("k", caffeineCache.getIfPresent("k"));

    assertThat(demoted).isNull();
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(entry.getDecisionNodeId()).isEqualTo("w1");
  }

  @Test
  void restartedWorker_epochMismatch_demotesEvenIfAlive() {
    mockWorker("w1", true, 6L);
    caffeineCache.put("k", hotEntry("w1", 5L, 60_000, 15_000));

    EntryLifecycleImpl em = entryLifecycle();
    EntryMutations.Applied demoted = em.demoteIfRemoteNodeIdInvalid("k", caffeineCache.getIfPresent("k"));

    assertThat(demoted).isNotNull();
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(entry.getKeyState()).isEqualTo(KeyState.NORMAL);
    assertThat(entry.getDecisionNodeId()).isNull();
  }

  @Test
  void unknownWorker_noHealthRecord_demotes() {
    mockWorker("w1", false, HealthView.UNKNOWN_EPOCH);
    caffeineCache.put("k", hotEntry("w1", 5L, 60_000, 15_000));

    EntryLifecycleImpl em = entryLifecycle();
    EntryMutations.Applied demoted = em.demoteIfRemoteNodeIdInvalid("k", caffeineCache.getIfPresent("k"));

    assertThat(demoted).isNotNull();
    assertThat(((CacheEntry) caffeineCache.getIfPresent("k")).getKeyState()).isEqualTo(KeyState.NORMAL);
  }

  @Test
  void localEntry_withoutDecisionStamp_neverDemoted() {
    caffeineCache.put("k", hotEntry(null, 0L, 60_000, 15_000));

    EntryLifecycleImpl em = entryLifecycle();
    EntryMutations.Applied demoted = em.demoteIfRemoteNodeIdInvalid("k", caffeineCache.getIfPresent("k"));

    assertThat(demoted).isNull();
    assertThat(((CacheEntry) caffeineCache.getIfPresent("k")).getKeyState()).isEqualTo(KeyState.HOT);
  }

  @Test
  void coolEntry_neverDemoted() {
    mockWorker("w1", false, 5L);
    CacheEntry cool = EntryDraft.of(hotEntry("w1", 5L, 60_000, 15_000)).keyState(KeyState.COOL).build();
    caffeineCache.put("k", cool);

    EntryLifecycleImpl em = entryLifecycle();
    EntryMutations.Applied demoted = em.demoteIfRemoteNodeIdInvalid("k", caffeineCache.getIfPresent("k"));

    assertThat(demoted).isNull();
    assertThat(((CacheEntry) caffeineCache.getIfPresent("k")).getKeyState()).isEqualTo(KeyState.COOL);
  }

  @Test
  void localNormalEntry_neverDemoted() {
    mockWorker("w1", false, 5L);
    caffeineCache.put("k", normalEntry());

    EntryLifecycleImpl em = entryLifecycle();
    EntryMutations.Applied demoted = em.demoteIfRemoteNodeIdInvalid("k", caffeineCache.getIfPresent("k"));

    assertThat(demoted).isNull();
    assertThat(caffeineCache.getIfPresent("k").getKeyState()).isEqualTo(KeyState.NORMAL);
  }

  @Test
  void noHealthView_disablesDemotion() {
    caffeineCache.put("k", hotEntry("w1", 5L, 60_000, 15_000));

    EntryLifecycleImpl em = entryLifecycleWithoutHealthView();
    EntryMutations.Applied demoted = em.demoteIfRemoteNodeIdInvalid("k", caffeineCache.getIfPresent("k"));

    assertThat(demoted).isNull();
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
    EntryLifecycleImpl em = entryLifecycle();
    EntryMutations.Applied demoted = em.demoteIfRemoteNodeIdInvalid("k", staleRaw);

    assertThat(demoted).isNull();
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
    assertThat(entry.getDecisionNodeId()).isEqualTo("w2");
  }

  @Test
  void demotedEntry_isStableOnSecondCheck() {
    mockWorker("w1", false, 5L);
    caffeineCache.put("k", hotEntry("w1", 5L, 60_000, 15_000));

    EntryLifecycleImpl em = entryLifecycle();
    em.demoteIfRemoteNodeIdInvalid("k", caffeineCache.getIfPresent("k"));
    EntryMutations.Applied second = em.demoteIfRemoteNodeIdInvalid("k", caffeineCache.getIfPresent("k"));

    assertThat(second).isNull();
    assertThat(((CacheEntry) caffeineCache.getIfPresent("k")).getKeyState()).isEqualTo(KeyState.NORMAL);
  }

  @Test
  void zeroNormalTtl_fallsBackToConfiguredDefaults() {
    mockWorker("w1", false, 5L);
    caffeineCache.put("k", hotEntry("w1", 5L, 0, 0));

    EntryLifecycleImpl em = entryLifecycle();
    EntryMutations.Applied demoted = em.demoteIfRemoteNodeIdInvalid("k", caffeineCache.getIfPresent("k"));

    assertThat(demoted).isNotNull();
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(entry.getHardTtlMs()).isEqualTo(ttlConfig.getHardTtlMs());
    assertThat(entry.getSoftTtlMs()).isEqualTo(ttlConfig.getSoftTtlMs());
  }

  /**
   * Demotion caps the remaining life at the normal TTL instead of restarting it: a value
   * keeps its age, so the rewrite may shorten the deadline the dead Worker's decision
   * granted, never extend the entry's own.
   */
  @Test
  void demotion_shortensTheHotDeadlineToTheNormalTtl() {
    mockWorker("w1", false, 5L);
    CacheEntry hot = hotEntry("w1", 5L, 60_000, 15_000);
    caffeineCache.put("k", hot);

    assertThat(entryLifecycle().demoteIfRemoteNodeIdInvalid("k", caffeineCache.getIfPresent("k"))).isNotNull();

    CacheEntry demoted = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(demoted.getHardExpireAtMs()).isLessThan(hot.getHardExpireAtMs());
    assertThat(demoted.getHardExpireAtMs()).isLessThanOrEqualTo(System.currentTimeMillis() + 63_000);
  }

  /**
   * The cap never extends an entry that already sits inside its normal window: an entry
   * five seconds from its hard deadline keeps that deadline, instead of being re-armed
   * with a fresh full normal TTL by the rewrite.
   */
  @Test
  void demotion_neverExtendsAnEntryInsideTheNormalWindow() {
    mockWorker("w1", false, 5L);
    long now = System.currentTimeMillis();
    CacheEntry hot = EntryDraft.blank(null)
      .value("v")
      .version(1)
      .decision(DecisionStamp.hot(42, "w1", 5L))
      .ttl(3_600_000, 300_000, 60_000, 15_000)
      .expiryAt(now + 5_000, now + 4_000)
      .keyState(KeyState.HOT)
      .build();
    caffeineCache.put("k", hot);

    assertThat(entryLifecycle().demoteIfRemoteNodeIdInvalid("k", caffeineCache.getIfPresent("k"))).isNotNull();

    CacheEntry demoted = (CacheEntry) caffeineCache.getIfPresent("k");
    assertThat(demoted.getKeyState()).isEqualTo(KeyState.NORMAL);
    assertThat(demoted.getHardExpireAtMs()).isEqualTo(hot.getHardExpireAtMs());
    assertThat(demoted.getSoftExpireAtMs()).isEqualTo(hot.getSoftExpireAtMs());
  }
}
