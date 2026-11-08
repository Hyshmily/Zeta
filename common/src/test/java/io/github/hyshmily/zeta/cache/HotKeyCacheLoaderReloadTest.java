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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.circuitbreaker.CircuitBreaker;
import io.github.hyshmily.zeta.cache.cachesupport.circuitbreaker.CircuitBreakerRegistry;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycleImpl;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlightImpl;
import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.StalePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.scheduler.DefaultBackgroundRefresher;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.clock.MonotonicStampClock;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests for the {@link CacheLoader#reload} seam at the
 * {@link HotKeyCache} level: a foreground miss calls {@code load}, while an
 * engine background refresh calls {@code reload} — even when the refresh
 * executor runs the task inline on the caller's own thread, the shape a
 * {@code CallerRunsPolicy} or a synchronous executor produces. A loader that
 * carries caller-bound state (a one-shot invocation chain) therefore never has
 * that state resumed from a refresh.
 */
class HotKeyCacheLoaderReloadTest {

  private HotKeyDetector hotKeyDetector;
  private Cache<String, CacheEntry> caffeineCache;
  private EntryLifecycle entryLifecycle;
  private HotKeyCache hotKeyCache;
  private ScheduledExecutorService scheduler;

  @BeforeEach
  void setUp() {
    hotKeyDetector = mock(HotKeyDetector.class);
    when(hotKeyDetector.contains(anyString())).thenReturn(false);
    caffeineCache = Caffeine.newBuilder().maximumSize(100).build();
    // Direct executor: a background refresh runs inline on the calling thread —
    // the CallerRunsPolicy shape the load/reload split must survive.
    Executor executor = Runnable::run;
    ZetaProperties ttlConfig = new ZetaProperties();
    HealthView healthView = mock(HealthView.class);
    entryLifecycle = new EntryLifecycleImpl(caffeineCache, ttlConfig, healthView);
    DefaultBackgroundRefresher backgroundRefresher = new DefaultBackgroundRefresher(
      caffeineCache,
      executor,
      entryLifecycle,
      10
    );
    CircuitBreaker circuitBreaker = mock(CircuitBreaker.class);
    when(circuitBreaker.tryAcquireStamp()).thenReturn(CircuitBreaker.STAMP_CLOSED);
    when(circuitBreaker.isOpen()).thenReturn(false);
    CircuitBreakerRegistry breakerRegistry = mock(CircuitBreakerRegistry.class);
    when(breakerRegistry.breakerFor(anyString())).thenReturn(circuitBreaker);
    scheduler = Executors.newSingleThreadScheduledExecutor();
    hotKeyCache = new HotKeyCache(
      hotKeyDetector,
      caffeineCache,
      new SingleFlightImpl(10_000, 5, 5, executor, breakerRegistry),
      entryLifecycle,
      backgroundRefresher,
      new CentralDispatcher(
        mock(AccessCounter.class),
        null,
        new BroadcastBuffer(scheduler, null, 500, 2_000, null),
        null
      ),
      new MonotonicStampClock(1L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS),
      healthView
    );
  }

  @AfterEach
  void tearDown() {
    scheduler.shutdownNow();
  }

  @Test
  @DisplayName("foreground miss calls load, never reload")
  void foregroundMiss_callsLoadOnly() {
    RecordingLoader loader = new RecordingLoader();

    Object served = hotKeyCache.get("miss", loader, GetPolicy.defaults());

    assertThat(served).isEqualTo("loaded");
    assertThat(loader.loads.get()).isEqualTo(1);
    assertThat(loader.reloads.get()).isZero();
    assertThat((Object) ((CacheEntry) caffeineCache.getIfPresent("miss")).getValue()).isEqualTo("loaded");
  }

  @Test
  @DisplayName("SOFT_REFRESH soft-expired hit serves stale and refreshes through reload, never load")
  void softExpiredHit_backgroundRefresh_callsReloadOnly() {
    caffeineCache.put("stale", softExpiredNormalEntry("stale-value"));
    RecordingLoader loader = new RecordingLoader();

    Object served = hotKeyCache.get("stale", loader, GetPolicy.of(0L, 30_000L, true, StalePolicy.SOFT_REFRESH));

    // The caller is served the stale value; the inline refresh executor has
    // already applied the reload result when the call returns.
    assertThat(served).isEqualTo("stale-value");
    assertThat(loader.reloads.get()).isEqualTo(1);
    assertThat(loader.loads.get()).isZero();
    assertThat((Object) ((CacheEntry) caffeineCache.getIfPresent("stale")).getValue()).isEqualTo("reloaded");
  }

  @Test
  @DisplayName("default reload delegates to load, so plain lambdas keep working")
  void defaultReload_delegatesToLoad() {
    AtomicInteger loads = new AtomicInteger();
    CacheLoader<String, String> loadOnly = new CacheLoader<>() {
      @Override
      public String load(String key) {
        loads.incrementAndGet();
        return "value:" + key;
      }
    };

    assertThat(loadOnly.reload("k")).isEqualTo("value:k");
    assertThat(loads.get()).isEqualTo(1);

    // A plain lambda still compiles as a CacheLoader only because reload has a
    // default implementation; it inherits the load delegation.
    CacheLoader<String, String> lambda = key -> "lambda:" + key;
    assertThat(lambda.reload("k")).isEqualTo("lambda:k");
  }

  /** Counts which loader entry point the engine invoked. */
  private static final class RecordingLoader implements CacheLoader<String, String> {

    private final AtomicInteger loads = new AtomicInteger();
    private final AtomicInteger reloads = new AtomicInteger();

    @Override
    public String load(String key) {
      loads.incrementAndGet();
      return "loaded";
    }

    @Override
    public String reload(String key) {
      reloads.incrementAndGet();
      return "reloaded";
    }
  }

  /**
   * Build a soft-expired NORMAL entry: the soft window lapsed while the hard TTL
   * is still valid.
   */
  private static CacheEntry softExpiredNormalEntry(Object value) {
    long now = TimeSource.currentTimeMillis();
    return EntryDraft.blank(null)
      .value(value)
      .version(5)
      .decision(DecisionStamp.hot(0, null, 0L))
      .ttl(300_000, 30_000, 300_000, 30_000)
      .expiryAt(now + 300_000, now - 10_000)
      .keyState(KeyState.NORMAL)
      .build();
  }
}
