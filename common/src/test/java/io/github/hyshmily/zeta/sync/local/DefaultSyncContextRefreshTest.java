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
package io.github.hyshmily.zeta.sync.local;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.EntryView;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycleImpl;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.TtlPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.heat.HeatView;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.policy.HeatTrigger;
import io.github.hyshmily.zeta.policy.NamespacePolicyRegistry;
import io.github.hyshmily.zeta.scheduler.WarmScheduler;
import io.github.hyshmily.zeta.util.TimeSource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Pins the peer-REFRESH isolation, cold-key gate, reload choice, dynamic-TTL refusal and
 * the admission actions that must not leave a stale entry behind. */
class DefaultSyncContextRefreshTest {

  private static final String NAMESPACE = "users\u0000";
  private static final String KEY = NAMESPACE + "u-1";

  private Cache<String, CacheEntry> table;
  private EntryLifecycle lifecycle;
  private NamespacePolicyRegistry policies;
  private ZetaProperties properties;
  private HeatView heatView;
  private HotKeyDetector detector;
  private List<SyncSkipReason> skips;

  @BeforeEach
  void setUp() {
    TimeSource.start();
    properties = new ZetaProperties();
    table = Caffeine.newBuilder().build();
    lifecycle = new EntryLifecycleImpl(table, properties);
    policies = new NamespacePolicyRegistry(properties, null);
    detector = mock(HotKeyDetector.class);
    heatView = new HeatView(null, detector);
    skips = new ArrayList<>();
  }

  @Test
  void aColdRefreshOutsideThePrewarmTrigger_shouldNotTouchTheLoader() {
    CacheLoader<String, Object> loader = mock(CacheLoader.class);
    policies.registerProgrammaticNamespace(NAMESPACE, null, null, loader);
    DefaultSyncContext context = context(Runnable::run);

    context.apply(refresh(1L), new SyncPolicy() {});

    assertThat(table.getIfPresent(KEY)).isNull();
    verifyNoInteractions(loader);
    assertThat(skips).containsExactly(SyncSkipReason.ABSENT_NOT_PREWARMABLE);
  }

  @Test
  void aColdRefreshInsideThePrewarmTrigger_shouldLoadOnce() {
    when(detector.contains(KEY)).thenReturn(true);
    CountingLoader loader = new CountingLoader("fresh");
    policies.registerProgrammaticNamespace(NAMESPACE, HeatTrigger.LOCAL, null, loader);
    DefaultSyncContext context = context(Runnable::run);

    context.apply(refresh(1L), new SyncPolicy() {});

    assertThat(loader.loads.get()).isEqualTo(1);
    assertThat(loader.reloads.get()).isZero();
    assertThat(table.getIfPresent(KEY).getValue()).isEqualTo("fresh");
  }

  @Test
  void aRefreshOfAResident_shouldUseReload() {
    resident("old");
    CountingLoader loader = new CountingLoader("new");
    policies.registerProgrammaticNamespace(NAMESPACE, null, null, loader);
    DefaultSyncContext context = context(Runnable::run);

    context.apply(refresh(2L), new SyncPolicy() {});

    assertThat(loader.loads.get()).isZero();
    assertThat(loader.reloads.get()).isEqualTo(1);
    assertThat(table.getIfPresent(KEY).getValue()).isEqualTo("new");
  }

  @Test
  void aFailingRefreshLoader_shouldReportRefreshFailed() {
    resident("old");
    CacheLoader<String, Object> loader = key -> {
      throw new IllegalStateException("source down");
    };
    policies.registerProgrammaticNamespace(NAMESPACE, null, null, loader);
    DefaultSyncContext context = context(Runnable::run);

    context.apply(refresh(2L), new SyncPolicy() {});

    assertThat(table.getIfPresent(KEY)).isNull();
    assertThat(skips).containsExactly(SyncSkipReason.REFRESH_FAILED);
  }

  @Test
  void aRefreshLoaderMiss_shouldReportValueNotFound() {
    resident("old");
    policies.registerProgrammaticNamespace(NAMESPACE, null, null, key -> null);
    DefaultSyncContext context = context(Runnable::run);

    context.apply(refresh(2L), new SyncPolicy() {});

    assertThat(table.getIfPresent(KEY)).isNull();
    assertThat(skips).containsExactly(SyncSkipReason.VALUE_NOT_FOUND);
  }

  @Test
  void aRefreshIsCacheableFailure_shouldReportValueDeclinedByLoader() {
    resident("old");
    CacheLoader<String, Object> loader = new CacheLoader<>() {
      @Override
      public Object load(String key) {
        return "fresh";
      }

      @Override
      public boolean isCacheable(Object value) {
        throw new IllegalStateException("predicate down");
      }
    };
    policies.registerProgrammaticNamespace(NAMESPACE, null, null, loader);
    DefaultSyncContext context = context(Runnable::run);

    context.apply(refresh(2L), new SyncPolicy() {});

    assertThat(table.getIfPresent(KEY)).isNull();
    assertThat(skips).containsExactly(SyncSkipReason.VALUE_DECLINED_BY_LOADER);
  }

  @Test
  void anInvalidateAdmittedAsInvalidateLocal_shouldStillEvict() {
    resident("old");
    DefaultSyncContext context = context(Runnable::run);

    context.apply(
      invalidate(2L),
      new SyncPolicy() {
        @Override
        public SyncAdmission admit(SyncEvent event, EntryView current) {
          return SyncAdmission.INVALIDATE_LOCAL;
        }
      }
    );

    assertThat(table.getIfPresent(KEY)).isNull();
    assertThat(skips).isEmpty();
  }

  @Test
  void aRejectedRefreshExecutor_shouldDegradeToVersionedInvalidation() {
    when(detector.contains(KEY)).thenReturn(true);
    CountingLoader loader = new CountingLoader("fresh");
    policies.registerProgrammaticNamespace(NAMESPACE, HeatTrigger.LOCAL, null, loader);
    Executor rejecting = command -> {
      throw new RejectedExecutionException("saturated");
    };
    DefaultSyncContext context = context(rejecting);

    context.apply(refresh(1L), new SyncPolicy() {});

    assertThat(loader.loads.get()).isZero();
    assertThat(table.getIfPresent(KEY)).isNull();
    assertThat(skips).containsExactly(SyncSkipReason.REFRESH_REJECTED);
  }

  @Test
  void aWarmSchedulerFailureAfterAPeerWrite_shouldNotDegradeTheAppliedValue() {
    resident("old");
    CountingLoader loader = new CountingLoader("new");
    policies.registerProgrammaticNamespace(NAMESPACE, null, null, loader);
    WarmScheduler scheduler = mock(WarmScheduler.class);
    doThrow(new IllegalStateException("scheduler down"))
      .when(scheduler)
      .scheduleIfEligible(anyString(), any(CacheEntry.class), any());

    context(Runnable::run, scheduler).apply(refresh(2L), new SyncPolicy() {});

    assertThat(table.getIfPresent(KEY).getValue()).isEqualTo("new");
    assertThat(skips).isEmpty();
  }

  @Test
  void aColdPutIntoADynamicTtlNamespace_shouldRefuseToInventTheDefaultTtl() {
    policies.registerAnnotatedNamespace(NAMESPACE, 0L, 0L, true, null, null);
    DefaultSyncContext context = context(Runnable::run);
    SyncMessage message = new SyncMessage(0L, KEY, SyncType.PUT, 1L, new byte[] { 1 });

    context.apply(new Batch.Value(message, List.of(), null), new SyncPolicy() {});

    assertThat(table.getIfPresent(KEY)).isNull();
    assertThat(skips).containsExactly(SyncSkipReason.TTL_NOT_PREWARMABLE);
  }

  @Test
  void aRefreshQueuedWhileResident_shouldRecheckColdAdmissionWhenTheEntryIsGone() {
    resident("old");
    CountingLoader loader = new CountingLoader("new");
    policies.registerProgrammaticNamespace(NAMESPACE, null, null, loader);
    List<Runnable> queued = new ArrayList<>();
    DefaultSyncContext context = context(queued::add);

    context.apply(refresh(2L), new SyncPolicy() {});
    assertThat(queued).hasSize(1);

    // The entry is evicted while the task waits in the refresh queue.
    table.invalidate(KEY);
    queued.forEach(Runnable::run);

    assertThat(loader.loads.get()).isZero();
    assertThat(loader.reloads.get()).isZero();
    assertThat(skips).containsExactly(SyncSkipReason.ABSENT_NOT_PREWARMABLE);
  }

  private void resident(Object value) {
    table.put(KEY, EntryDraft.blank(new TtlPolicy(properties)).value(value).version(1L).ttl(60_000L, 0L).build());
  }

  private DefaultSyncContext context(Executor refreshExecutor) {
    return context(refreshExecutor, null);
  }

  private DefaultSyncContext context(Executor refreshExecutor, WarmScheduler scheduler) {
    SyncHook hook = new SyncHook() {
      @Override
      public void afterValueApplied(SyncEvent event, EntryView entry) {}

      @Override
      public void afterValueDegraded(SyncEvent event, SyncSkipReason reason) {
        skips.add(reason);
      }

      @Override
      public void onSkipped(SyncEvent event, SyncSkipReason reason) {
        skips.add(reason);
      }
    };
    return new DefaultSyncContext(
      table,
      null,
      lifecycle,
      List.of(hook),
      null,
      null,
      Runnable::run,
      refreshExecutor,
      null,
      heatView,
      scheduler,
      policies
    );
  }

  private static Batch.Value refresh(long version) {
    SyncMessage message = new SyncMessage(0L, KEY, SyncType.REFRESH, version);
    return new Batch.Value(message, List.of(), null);
  }

  private static Batch.Value invalidate(long version) {
    SyncMessage message = new SyncMessage(0L, KEY, SyncType.INVALIDATE, version);
    return new Batch.Value(message, List.of(), null);
  }

  private static final class CountingLoader implements CacheLoader<String, Object> {

    private final AtomicInteger loads = new AtomicInteger();
    private final AtomicInteger reloads = new AtomicInteger();
    private final Object value;

    private CountingLoader(Object value) {
      this.value = value;
    }

    @Override
    public Object load(String key) {
      loads.incrementAndGet();
      return value;
    }

    @Override
    public Object reload(String key) {
      reloads.incrementAndGet();
      return value;
    }
  }
}
