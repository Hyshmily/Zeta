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
package io.github.hyshmily.zeta.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.TtlPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.cache.cachesupport.raw.VersionedValue;
import io.github.hyshmily.zeta.heat.HeatDirectory;
import io.github.hyshmily.zeta.heat.HeatView;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.policy.HeatTrigger;
import io.github.hyshmily.zeta.policy.NamespacePolicyRegistry;
import io.github.hyshmily.zeta.sync.worker.WorkerDecision;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.clock.MonotonicStampClock;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pins the keep-warm local-read gate: a cluster lease may schedule a renewal, but only a read
 * observed since that warm cycle began may spend it; local TopK membership is its own evidence.
 */
class KeepWarmTest {

  private static final String KEY = "users\u0000u-1";
  private static final String NODE = "worker-1";

  private Cache<String, CacheEntry> cache;
  private HotKeyDetector detector;
  private HeatDirectory cluster;
  private CountingRefresher refresher;
  private ScheduledExecutorService scheduler;
  private AtomicReference<Runnable> scheduled;
  private AtomicLong scheduledDelay;
  private KeepWarm keepWarm;
  private CacheEntry entry;
  private ZetaProperties props;

  @BeforeEach
  void setUp() {
    TimeSource.start();
    props = new ZetaProperties();
    props.getWarm().getKeepWarm().setMinWindowMs(1L);
    props.getWarm().getKeepWarm().setLeadMaxMs(1_000L);

    cache = Caffeine.newBuilder().build();
    detector = mock(HotKeyDetector.class);
    cluster = new HeatDirectory(1_000, 60_000L, Runnable::run, null, null);
    NamespacePolicyRegistry policies = new NamespacePolicyRegistry(props, null);
    policies.registerAnnotatedNamespace("users\u0000", 0L, 30_000L, false, HeatTrigger.OFF, HeatTrigger.CLUSTER);
    policies.registerProgrammaticNamespace("users\u0000", null, null, key -> "renewed");

    refresher = new CountingRefresher(cache);
    scheduler = mock(ScheduledExecutorService.class);
    scheduled = new AtomicReference<>();
    scheduledDelay = new AtomicLong();
    when(scheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
      scheduled.set(invocation.getArgument(0));
      scheduledDelay.set(invocation.getArgument(1, Long.class));
      return mock(ScheduledFuture.class);
    });

    keepWarm = new KeepWarm(
      cache,
      new HeatView(cluster, detector),
      policies,
      refresher,
      scheduler,
      new MonotonicStampClock(1L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS),
      new TtlPolicy(props),
      props
    );
    entry = EntryDraft.blank(new TtlPolicy(props)).value("v").version(1L).ttl(60_000L, 30_000L).build();
  }

  private void grantClusterLease() {
    cluster.grant(new WorkerDecision(1L, KEY, DecisionStamp.hot(1L, NODE, 1L)));
  }

  @Test
  void aSoftWindowBelowTheMinimum_shouldRenewAgainstTheHardDeadline() {
    ZetaProperties configured = new ZetaProperties();
    configured.getWarm().getKeepWarm().setMinWindowMs(2_000L);
    configured.getWarm().getKeepWarm().setLeadMaxMs(1_000L);
    NamespacePolicyRegistry policies = new NamespacePolicyRegistry(configured, null);
    policies.registerAnnotatedNamespace("users\u0000", 0L, 30_000L, false, HeatTrigger.OFF, HeatTrigger.CLUSTER);
    policies.registerProgrammaticNamespace("users\u0000", null, null, key -> "renewed");
    TtlPolicy ttlPolicy = new TtlPolicy(configured);
    AtomicReference<Runnable> fire = new AtomicReference<>();
    scheduler = mock(ScheduledExecutorService.class);
    when(scheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
      fire.set(invocation.getArgument(0));
      return mock(ScheduledFuture.class);
    });
    KeepWarm shortSoft = new KeepWarm(
      cache,
      new HeatView(cluster, detector),
      policies,
      refresher,
      scheduler,
      new MonotonicStampClock(1L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS),
      ttlPolicy,
      configured
    );
    // A one-second soft window against a two-second floor: the soft deadline cannot hold a
    // renewal, so the long hard TTL is the deadline that must be defended.
    CacheEntry shortWindow = EntryDraft.blank(ttlPolicy).value("v").version(1L).ttl(60_000L, 1_000L).build();
    grantClusterLease();
    cache.put(KEY, shortWindow);

    shortSoft.scheduleIfEligible(KEY, shortWindow, WarmSignal.HEAT);
    assertThat(fire.get()).isNotNull();
    shortSoft.onReads(Set.of(KEY), TimeSource.monotonicMillis() + 1L);
    fire.get().run();

    assertThat(refresher.renewals).isEqualTo(1);
  }

  @Test
  void aDisabledScheduler_shouldNotProbeHeatOnTheTide() {
    props.getWarm().setEnabled(false);

    keepWarm.onReads(Set.of(KEY), TimeSource.monotonicMillis() + 1L);

    verifyNoInteractions(detector);
  }

  @Test
  void clusterLeaseWithoutARead_shouldNotSpendTheRenewal() {
    grantClusterLease();
    cache.put(KEY, entry);

    keepWarm.scheduleIfEligible(KEY, entry, WarmSignal.HEAT);
    assertThat(scheduled.get()).isNotNull();
    scheduled.get().run();

    assertThat(refresher.renewals).isZero();
  }

  @Test
  void clusterLeaseAfterARead_shouldRenewOncePerWarmCycle() {
    grantClusterLease();
    cache.put(KEY, entry);
    keepWarm.scheduleIfEligible(KEY, entry, WarmSignal.HEAT);
    keepWarm.onReads(Set.of(KEY), TimeSource.monotonicMillis() + 1L);

    scheduled.get().run();
    assertThat(refresher.renewals).isEqualTo(1);

    // The renewal re-armed the next cycle without new read evidence: it must not fire.
    scheduled.get().run();
    assertThat(refresher.renewals).isEqualTo(1);
  }

  @Test
  void localHeatIsItsOwnEvidence() {
    useLocalKeepWarmTrigger();
    cache.put(KEY, entry);

    keepWarm.scheduleIfEligible(KEY, entry, WarmSignal.WRITE);
    scheduled.get().run();

    assertThat(refresher.renewals).isEqualTo(1);
  }

  @Test
  void cancelIfCold_shouldKeepTheRenewalOfALocallyHotKey() {
    useLocalKeepWarmTrigger();
    cache.put(KEY, entry);
    keepWarm.scheduleIfEligible(KEY, entry, WarmSignal.WRITE);

    keepWarm.cancelIfCold(KEY);
    scheduled.get().run();

    assertThat(refresher.renewals).isEqualTo(1);
  }

  @Test
  void cancelIfCold_shouldDropTheRenewalWhenTheTriggerNoLongerAcceptsTheKey() {
    grantClusterLease();
    cache.put(KEY, entry);
    keepWarm.scheduleIfEligible(KEY, entry, WarmSignal.HEAT);
    keepWarm.onReads(Set.of(KEY), TimeSource.monotonicMillis() + 1L);
    cluster.revoke(new WorkerDecision(2L, KEY, DecisionStamp.cool(2L, NODE, 1L)));

    keepWarm.cancelIfCold(KEY);
    // Heat returns, but a task the cold judgment dropped must not spend the renewal.
    cluster.grant(new WorkerDecision(3L, KEY, DecisionStamp.hot(3L, NODE, 1L)));
    scheduled.get().run();

    assertThat(refresher.renewals).isZero();
  }

  /** Rebuilds the scheduler over a namespace whose keep-warm trigger accepts local heat only. */
  private void useLocalKeepWarmTrigger() {
    ZetaProperties localProps = new ZetaProperties();
    localProps.getWarm().getKeepWarm().setTrigger(HeatTrigger.LOCAL);
    NamespacePolicyRegistry policies = new NamespacePolicyRegistry(localProps, null);
    policies.registerAnnotatedNamespace("users\u0000", 0L, 0L, false, HeatTrigger.OFF, HeatTrigger.LOCAL);
    policies.registerProgrammaticNamespace("users\u0000", null, null, key -> "renewed");
    when(detector.contains(KEY)).thenReturn(true);
    scheduler = mock(ScheduledExecutorService.class);
    when(scheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
      scheduled.set(invocation.getArgument(0));
      return mock(ScheduledFuture.class);
    });
    keepWarm = new KeepWarm(
      cache,
      new HeatView(cluster, detector),
      policies,
      refresher,
      scheduler,
      new MonotonicStampClock(1L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS),
      new TtlPolicy(localProps),
      localProps
    );
  }

  @Test
  void shortWindow_shouldKeepTheLeadBelowAQuarterOfTheWindow() {
    grantClusterLease();
    props.getWarm().getKeepWarm().setLeadMaxMs(100L);
    CacheEntry shortEntry = EntryDraft.blank(new TtlPolicy(props)).value("v").version(1L).ttl(1_000L, 0L).build();
    cache.put(KEY, shortEntry);

    keepWarm.scheduleIfEligible(KEY, shortEntry, WarmSignal.HEAT);

    assertThat(scheduled.get()).isNotNull();
    // A 1 s window with a 100 ms lead cap must keep the task in the second half instead of firing
    // every MIN_INTERVAL_MS; a lead capped at window/4 leaves window - lead - jitter above 500 ms.
    assertThat(scheduledDelay.get()).isGreaterThan(500L);
  }

  @Test
  void writeOnANewVersion_shouldKeepTheObservedReadEvidence() {
    grantClusterLease();
    cache.put(KEY, entry);
    keepWarm.scheduleIfEligible(KEY, entry, WarmSignal.HEAT);
    keepWarm.onReads(Set.of(KEY), TimeSource.monotonicMillis());

    CacheEntry version2 = EntryDraft.blank(new TtlPolicy(props)).value("v2").version(2L).ttl(60_000L, 30_000L).build();
    cache.put(KEY, version2);
    keepWarm.scheduleIfEligible(KEY, version2, WarmSignal.WRITE);
    scheduled.get().run();

    assertThat(refresher.renewals).isEqualTo(1);
  }

  /**
   * A read tide samples one instant for the whole batch. A task replaced while that walk is still
   * in flight already holds a newer read, and the batch's older sample must not push the evidence
   * back before the new cycle started — the next fire would then discard a key just served.
   */
  @Test
  void anOlderTideSample_shouldNotOverwriteANewerRead() {
    grantClusterLease();
    cache.put(KEY, entry);
    long tideSample = TimeSource.monotonicMillis() - 1_000L;
    keepWarm.scheduleIfEligible(KEY, entry, WarmSignal.HEAT);

    // A write re-arms the cycle and a real read lands on it while the tide is still walking.
    CacheEntry version2 = EntryDraft.blank(new TtlPolicy(props)).value("v2").version(2L).ttl(60_000L, 30_000L).build();
    cache.put(KEY, version2);
    keepWarm.scheduleIfEligible(KEY, version2, WarmSignal.WRITE);
    keepWarm.scheduleIfEligible(KEY, version2, WarmSignal.READ);

    keepWarm.onReads(Set.of(KEY), tideSample);
    scheduled.get().run();

    assertThat(refresher.renewals).isEqualTo(1);
  }

  @Test
  void onReads_shouldRearmATaskBoundToAnOlderVersion() {
    grantClusterLease();
    cache.put(KEY, entry);
    keepWarm.scheduleIfEligible(KEY, entry, WarmSignal.HEAT);

    CacheEntry version2 = EntryDraft.blank(new TtlPolicy(props)).value("v2").version(2L).ttl(60_000L, 30_000L).build();
    cache.put(KEY, version2);
    keepWarm.onReads(Set.of(KEY), TimeSource.monotonicMillis());
    scheduled.get().run();

    assertThat(refresher.renewals).isEqualTo(1);
  }

  @Test
  void aCancelledTask_shouldNotSpendTheRenewal() {
    grantClusterLease();
    cache.put(KEY, entry);
    keepWarm.scheduleIfEligible(KEY, entry, WarmSignal.HEAT);
    keepWarm.onReads(Set.of(KEY), TimeSource.monotonicMillis() + 1L);
    Runnable fire = scheduled.get();

    keepWarm.cancel(KEY);
    fire.run();

    assertThat(refresher.renewals).isZero();
  }

  @Test
  void scheduleIfEligible_shouldReportWhetherATaskIsArmed() {
    grantClusterLease();
    cache.put(KEY, entry);

    assertThat(keepWarm.scheduleIfEligible(KEY, entry, WarmSignal.HEAT)).isTrue();
    // A namespace that declares no loader can never spend a renewal, so nothing is armed.
    assertThat(keepWarm.scheduleIfEligible("other\u0000k", entry, WarmSignal.HEAT)).isFalse();
  }

  @Test
  void aVersionThatCannotBeRenewed_shouldDropTheOldTask() {
    grantClusterLease();
    cache.put(KEY, entry);
    keepWarm.scheduleIfEligible(KEY, entry, WarmSignal.HEAT);
    keepWarm.onReads(Set.of(KEY), TimeSource.monotonicMillis() + 1L);
    Runnable oldFire = scheduled.get();

    CacheEntry permanent = EntryDraft.blank(new TtlPolicy(props))
      .value("p")
      .version(2L)
      .ttl(Long.MAX_VALUE, 0L)
      .build();
    cache.put(KEY, permanent);
    keepWarm.scheduleIfEligible(KEY, permanent, WarmSignal.WRITE);

    verify(scheduler, times(1)).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    oldFire.run();
    assertThat(refresher.renewals).isZero();
  }

  @Test
  void aRenewalThatSettlesAfterACancel_shouldNotRearm() {
    grantClusterLease();
    cache.put(KEY, entry);
    keepWarm.scheduleIfEligible(KEY, entry, WarmSignal.HEAT);
    keepWarm.onReads(Set.of(KEY), TimeSource.monotonicMillis() + 1L);
    refresher.manual = true;

    scheduled.get().run();
    assertThat(refresher.renewals).isEqualTo(1);

    keepWarm.cancel(KEY);
    refresher.pending.complete(new BackgroundRefresher.RenewOutcome.Applied(cache.getIfPresent(KEY)));

    verify(scheduler, times(1)).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    assertThat(refresher.renewals).isEqualTo(1);
  }

  @Test
  void aWriteAfterTheReadWindow_shouldNotCarryTheReadForward() throws Exception {
    grantClusterLease();
    CacheEntry shortSoft = EntryDraft.blank(new TtlPolicy(props)).value("v").version(1L).ttl(60_000L, 300L).build();
    cache.put(KEY, shortSoft);
    keepWarm.scheduleIfEligible(KEY, shortSoft, WarmSignal.HEAT);
    keepWarm.onReads(Set.of(KEY), TimeSource.monotonicMillis());
    Thread.sleep(400L);

    CacheEntry version2 = EntryDraft.blank(new TtlPolicy(props)).value("v2").version(2L).ttl(60_000L, 300L).build();
    cache.put(KEY, version2);
    keepWarm.scheduleIfEligible(KEY, version2, WarmSignal.WRITE);
    scheduled.get().run();

    // The only read predates the new cycle by more than its renewal window: a write stream must
    // not keep it alive as if this instance were still serving the key.
    assertThat(refresher.renewals).isZero();
  }

  /** Counts renewals and resolves with the resident entry so the next cycle can be re-armed. */
  private static final class CountingRefresher implements BackgroundRefresher {

    private final Cache<String, CacheEntry> cache;
    private final Semaphore limiter = new Semaphore(1);
    private int renewals;
    private boolean manual;
    private CompletableFuture<RenewOutcome> pending;

    private CountingRefresher(Cache<String, CacheEntry> cache) {
      this.cache = cache;
    }

    @Override
    public void triggerBackgroundRefresh(String cacheKey, Supplier<VersionedValue> reader, long softTtlMs) {}

    @Override
    public CompletableFuture<RenewOutcome> renewHard(String cacheKey, Supplier<VersionedValue> reader) {
      renewals++;
      if (manual) {
        pending = new CompletableFuture<>();
        return pending;
      }
      CacheEntry resident = cache.getIfPresent(cacheKey);
      return CompletableFuture.completedFuture(
        resident == null ? RenewOutcome.Skipped.ABSENT_OR_EXPIRED : new RenewOutcome.Applied(resident)
      );
    }

    @Override
    public Semaphore getRefreshLimiter() {
      return limiter;
    }
  }
}
