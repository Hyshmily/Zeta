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
package io.github.hyshmily.zeta.heat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.TtlPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.policy.HeatTrigger;
import io.github.hyshmily.zeta.policy.NamespacePolicyRegistry;
import io.github.hyshmily.zeta.scheduler.WarmScheduler;
import io.github.hyshmily.zeta.scheduler.WarmSignal;
import io.github.hyshmily.zeta.sync.worker.WorkerDecision;
import io.github.hyshmily.zeta.util.TimeSource;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Pins the bridge from a cluster HOT on a resident key to the local renewal scheduler. */
class HotKeyContextTest {

  private static final String NAMESPACE = "users\u0000";
  private static final String KEY = NAMESPACE + "u-1";

  private Cache<String, CacheEntry> table;
  private HotKeyCache cache;
  private NamespacePolicyRegistry policies;
  private ZetaProperties properties;
  private HotKeyDetector detector;
  private HeatView heatView;
  private WorkerDecision decision;

  @BeforeEach
  void setUp() {
    TimeSource.start();
    properties = new ZetaProperties();
    table = Caffeine.newBuilder().build();
    cache = mock(HotKeyCache.class);
    when(cache.table()).thenReturn(table);
    policies = new NamespacePolicyRegistry(properties, null);
    detector = mock(HotKeyDetector.class);
    heatView = new HeatView(null, detector);
    decision = new WorkerDecision(1L, KEY, DecisionStamp.hot(1L, "worker-1", 1L));
  }

  @Test
  void armKeepWarm_withResident_shouldArmARenewalThroughTheScheduler() {
    CacheEntry resident = EntryDraft.blank(new TtlPolicy(properties)).value("v").version(1L).ttl(60_000L, 0L).build();
    table.put(KEY, resident);
    policies.registerProgrammaticNamespace(NAMESPACE, null, HeatTrigger.CLUSTER, key -> "v");
    HeatDirectory cluster = new HeatDirectory(1_000, 60_000L, Runnable::run, null, null);
    cluster.grant(new WorkerDecision(1L, KEY, DecisionStamp.hot(1L, "worker-1", 1L)));
    heatView = new HeatView(cluster, detector);
    WarmScheduler scheduler = mock(WarmScheduler.class);
    when(scheduler.scheduleIfEligible(KEY, resident, WarmSignal.HEAT)).thenReturn(true);

    assertThat(context(scheduler).armKeepWarm()).isTrue();

    verify(scheduler).scheduleIfEligible(KEY, resident, WarmSignal.HEAT);
  }

  @Test
  void armKeepWarm_withoutAResident_shouldNotOfferAnythingToTheScheduler() {
    policies.registerProgrammaticNamespace(NAMESPACE, null, HeatTrigger.CLUSTER, key -> "v");
    WarmScheduler scheduler = mock(WarmScheduler.class);

    assertThat(context(scheduler).armKeepWarm()).isFalse();

    verify(scheduler, never()).scheduleIfEligible(anyString(), any(CacheEntry.class), any(WarmSignal.class));
  }

  @Test
  void armKeepWarm_withoutAScheduler_shouldAnswerFalse() {
    CacheEntry resident = EntryDraft.blank(new TtlPolicy(properties)).value("v").version(1L).ttl(60_000L, 0L).build();
    table.put(KEY, resident);

    assertThat(context(null).armKeepWarm()).isFalse();
  }

  @Test
  void prewarmIfAbsent_withAResident_shouldNotArmBehindTheCallersBack() {
    CacheEntry resident = EntryDraft.blank(new TtlPolicy(properties)).value("v").version(1L).ttl(60_000L, 0L).build();
    table.put(KEY, resident);
    policies.registerProgrammaticNamespace(NAMESPACE, HeatTrigger.LOCAL, HeatTrigger.CLUSTER, key -> "v");
    when(detector.contains(KEY)).thenReturn(true);
    WarmScheduler scheduler = mock(WarmScheduler.class);

    assertThat(context(scheduler).prewarmIfAbsent().join()).isFalse();

    verify(scheduler, never()).scheduleIfEligible(anyString(), any(CacheEntry.class), any(WarmSignal.class));
    verify(cache, never()).warmAsync(anyString(), any(CacheLoader.class), anyLong(), anyLong());
  }

  @Test
  void prewarmIfAbsent_withNoWarmingTrigger_shouldSkipTheResidentProbe() {
    policies.registerProgrammaticNamespace(NAMESPACE, null, null, key -> "v");
    WarmScheduler scheduler = mock(WarmScheduler.class);
    HotKeyContext context = context(scheduler);

    assertThat(context.prewarmIfAbsent().join()).isFalse();

    verify(cache, never()).table();
    verify(scheduler, never()).scheduleIfEligible(anyString(), any(CacheEntry.class), any(WarmSignal.class));
  }

  @Test
  void prewarmIfAbsent_withoutAResident_shouldLoadThroughTheNamespaceLoader() {
    when(detector.contains(KEY)).thenReturn(true);
    when(cache.warmAsync(anyString(), any(CacheLoader.class), anyLong(), anyLong())).thenReturn(
      CompletableFuture.completedFuture(true)
    );
    policies.registerProgrammaticNamespace(NAMESPACE, HeatTrigger.LOCAL, null, key -> "v");
    HotKeyContext context = context(mock(WarmScheduler.class));

    assertThat(context.prewarmIfAbsent().join()).isTrue();
  }

  @Test
  void hasLiveEntry_shouldRejectAnEntryPastItsHardDeadline() {
    TtlPolicy ttlPolicy = new TtlPolicy(properties);
    table.put(KEY, EntryDraft.blank(ttlPolicy).value("v").version(1L).ttl(60_000L, 0L).build());
    HotKeyContext context = context(mock(WarmScheduler.class));

    assertThat(context.hasLiveEntry()).isTrue();

    // A hard-expired entry is still returned as a snapshot but is no longer servable.
    table.put(
      KEY,
      EntryDraft.blank(ttlPolicy)
        .value("v")
        .version(2L)
        .ttl(60_000L, 0L)
        .hardExpiryAt(TimeSource.monotonicMillis() - 1_000L)
        .build()
    );

    assertThat(context.currentEntry()).isNotNull();
    assertThat(context.hasLiveEntry()).isFalse();
  }

  @Test
  void namespace_shouldAnswerTheSameBareNameForBothEncodings() {
    HotKeyContext typed = context(mock(WarmScheduler.class));
    HotKeyContext plain = HotKeyContextFactory.of(cache, heatView, policies, null, new TtlPolicy(properties)).create(
      new WorkerDecision(1L, "users:u-1", DecisionStamp.hot(1L, "worker-1", 1L)),
      true
    );

    assertThat(typed.namespace()).isEqualTo("users");
    assertThat(plain.namespace()).isEqualTo("users");
  }

  @Test
  void namespace_shouldBeNullForAKeyWithoutOne() {
    HotKeyContext context = HotKeyContextFactory.of(cache, heatView, policies, null, new TtlPolicy(properties)).create(
      new WorkerDecision(1L, "plain-key", DecisionStamp.hot(1L, "worker-1", 1L)),
      true
    );

    assertThat(context.namespace()).isNull();
  }

  @Test
  void inNamespace_shouldBeNullSafeAndNameBased() {
    HotKeyContext context = context(mock(WarmScheduler.class));
    HotKeyContext plain = HotKeyContextFactory.of(cache, heatView, policies, null, new TtlPolicy(properties)).create(
      new WorkerDecision(1L, "plain-key", DecisionStamp.hot(1L, "worker-1", 1L)),
      true
    );

    assertThat(context.inNamespace("users")).isTrue();
    assertThat(context.inNamespace("orders")).isFalse();
    // A key with no namespace answers false instead of throwing on the nullable namespace().
    assertThat(plain.inNamespace("users")).isFalse();
  }

  @Test
  void isHot_shouldCombineBothSources() {
    when(detector.contains(KEY)).thenReturn(true);
    assertThat(context(mock(WarmScheduler.class)).isHot()).isTrue();

    when(detector.contains(KEY)).thenReturn(false);
    HeatDirectory cluster = new HeatDirectory(1_000, 60_000L, Runnable::run, null, null);
    cluster.grant(new WorkerDecision(1L, KEY, DecisionStamp.hot(1L, "worker-1", 1L)));
    heatView = new HeatView(cluster, detector);
    assertThat(context(mock(WarmScheduler.class)).isHot()).isTrue();

    heatView = new HeatView(null, detector);
    assertThat(context(mock(WarmScheduler.class)).isHot()).isFalse();
  }

  @Test
  void isClusterHot_shouldReadTheLeaseTable() {
    HeatDirectory cluster = new HeatDirectory(1_000, 60_000L, Runnable::run, null, null);
    heatView = new HeatView(cluster, detector);
    HotKeyContext context = context(mock(WarmScheduler.class));

    assertThat(context.isClusterHot()).isFalse();

    cluster.grant(new WorkerDecision(1L, KEY, DecisionStamp.hot(1L, "worker-1", 1L)));

    assertThat(context.isClusterHot()).isTrue();
    assertThat(context.isLocalHot()).isFalse();
  }

  @Test
  void cancelKeepWarmIfCold_shouldDelegateTheJudgmentToTheScheduler() {
    WarmScheduler scheduler = mock(WarmScheduler.class);

    context(scheduler).cancelKeepWarmIfCold();

    verify(scheduler).cancelIfCold(KEY);
  }

  @Test
  void cancelKeepWarmIfCold_withoutAScheduler_shouldBeANoOp() {
    assertThatCode(() -> context(null).cancelKeepWarmIfCold()).doesNotThrowAnyException();
  }

  @Test
  void isNewlyHot_shouldReportTheGrantTransition() {
    assertThat(context(true).isNewlyHot()).isTrue();
    assertThat(context(false).isNewlyHot()).isFalse();
  }

  @Test
  void key_shouldBeTheAppliedDecisionsKey() {
    HotKeyContext context = context(mock(WarmScheduler.class));

    assertThat(context.decision()).isSameAs(decision);
    assertThat(context.key()).isEqualTo(decision.cacheKey());
  }

  private HotKeyContext context(WarmScheduler scheduler) {
    return context(true, scheduler);
  }

  private HotKeyContext context(boolean newlyHot) {
    return context(newlyHot, mock(WarmScheduler.class));
  }

  private HotKeyContext context(boolean newlyHot, WarmScheduler scheduler) {
    return HotKeyContextFactory.of(cache, heatView, policies, scheduler, new TtlPolicy(properties)).create(
      decision,
      newlyHot
    );
  }
}
