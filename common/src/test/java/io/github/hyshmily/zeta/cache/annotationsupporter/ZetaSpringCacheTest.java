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
package io.github.hyshmily.zeta.cache.annotationsupporter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.github.hyshmily.zeta.DefaultZeta;
import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.annotation.annotationsupporter.NullValue;
import io.github.hyshmily.zeta.annotation.annotationsupporter.ZetaCacheContext;
import io.github.hyshmily.zeta.model.InvalidatePolicy;
import io.github.hyshmily.zeta.model.ReadPolicy;
import io.github.hyshmily.zeta.model.WritePolicy;
import io.github.hyshmily.zeta.annotation.annotationsupporter.ZetaSpringCache;
import io.github.hyshmily.zeta.annotation.annotationsupporter.SpringCacheSettings;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.CentralDispatcher;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.cache.cachesupport.BroadcastBuffer;
import io.github.hyshmily.zeta.cache.cachesupport.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.impl.EntryLifecycleImpl;
import io.github.hyshmily.zeta.scheduler.BackgroundRefresher;
import io.github.hyshmily.zeta.scheduler.DefaultBackgroundRefresher;
import io.github.hyshmily.zeta.cache.codec.CacheCompressor;
import io.github.hyshmily.zeta.exception.ZetaBlockedException;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.rule.impl.RuleMatcherImpl;
import io.github.hyshmily.zeta.rule.RuleMatcher;
import io.github.hyshmily.zeta.rule.RuleService;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.util.id.SnowflakeIdGenerator;
import io.github.hyshmily.zeta.util.version.impl.VersionControllerImpl;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.cache.Cache.ValueRetrievalException;

@DisplayName("ZetaSpringCache tests")
class ZetaSpringCacheTest {

  private Zeta zeta;
  private SpringCacheSettings settings;
  private ZetaSpringCache cache;

  @BeforeEach
  @SuppressWarnings("all")
  void setUp() {
    zeta = mock(Zeta.class);
    settings = mock(SpringCacheSettings.class);
    when(settings.keySeparator()).thenReturn("::");
    cache = new ZetaSpringCache("test", zeta, settings, true);
  }

  @AfterEach
  void tearDown() {
    ZetaCacheContext.get().restore(null);
  }

  @Test
  @DisplayName("getName returns the cache name")
  void getName_returnsCacheName() {
    assertThat(cache.getName()).isEqualTo("test");
  }

  @Test
  @DisplayName("getNativeCache returns the HotKey facade")
  void getNativeCache_returnsHotKey() {
    assertThat(cache.getNativeCache()).isSameAs(zeta);
  }

  @Test
  @DisplayName("lookup returns value from hotKey.peekAndTag")
  void lookup_returnsValueFromPeek() {
    when(zeta.peekAndTag("test::myKey")).thenReturn("stored-value");
    Object result = cache.lookup("myKey");
    assertThat(result).isEqualTo("stored-value");
  }

  @Test
  @DisplayName("lookup returns null when hotKey.peekAndTag finds nothing")
  void lookup_returnsNullWhenPeekEmpty() {
    when(zeta.peekAndTag("test::myKey")).thenReturn(null);
    Object result = cache.lookup("myKey");
    assertThat(result).isNull();
  }

  @Test
  @DisplayName("lookup returns null when key is null-cached")
  void lookup_returnsNullWhenNullCached() {
    ZetaCacheContext.get().push(ReadPolicy.of(0, 0));
    when(zeta.computeIfAbsentWithSoftExpireOptional(anyString(), any(ReadPolicy.class))).thenReturn(Optional.empty());
    cache.get("myKey", (Callable<String>) () -> null);
    ZetaCacheContext.get().restore(null);

    assertThat(cache.lookup("myKey")).isNull();
  }

  @Test
  @DisplayName("get without TTL override calls computeIfAbsentWithSoftExpire with default policy")
  void get_withoutTtl_callsHotKeyComputeIfAbsent() {
    when(zeta.computeIfAbsentWithSoftExpireOptional(
        eq("test::myKey"), any(ReadPolicy.class)
    )).thenReturn(Optional.of("value"));
    String result = cache.get("myKey", (Callable<String>) () -> "loaded");
    assertThat(result).isEqualTo("value");
    verify(zeta).computeIfAbsentWithSoftExpireOptional(
        eq("test::myKey"), any(ReadPolicy.class));
  }

  @Test
  @DisplayName("get with TTL override calls computeIfAbsentWithSoftExpire with policy")
  void get_withTtlOverride_callsComputeIfAbsentSoft() {
    ZetaCacheContext.get().push(ReadPolicy.of(5000L, 1000L).withNullCaching(false));
    when(zeta.computeIfAbsentWithSoftExpireOptional(
        eq("test::myKey"), any(ReadPolicy.class)
    )).thenReturn(Optional.of("value"));
    String result = cache.get("myKey", (Callable<String>) () -> "loaded");
    assertThat(result).isEqualTo("value");
    verify(zeta).computeIfAbsentWithSoftExpireOptional(
        eq("test::myKey"), argThat((ReadPolicy p) -> p.hardTtlMs().getAsLong() == 5000L));
  }

  @Test
  @DisplayName("get with only softTtl override calls computeIfAbsentWithSoftExpire")
  void get_withSoftTtlOverride_only() {
    ZetaCacheContext.get().push(ReadPolicy.of(0L, 500L).withNullCaching(false));
    when(zeta.computeIfAbsentWithSoftExpireOptional(
        eq("test::myKey"), any(ReadPolicy.class)
    )).thenReturn(Optional.of("value"));
    cache.get("myKey", (Callable<String>) () -> "loaded");
    verify(zeta).computeIfAbsentWithSoftExpireOptional(
        eq("test::myKey"), argThat((ReadPolicy p) -> p.softTtlMs().getAsLong() == 500L));
  }

  @Test
  @DisplayName("get returns fromStoreValue of the result")
  void get_whenResultPresent_returnsFromStoreValue() {
    when(zeta.computeIfAbsentWithSoftExpireOptional(
        anyString(), any(ReadPolicy.class)
    )).thenReturn(Optional.of("computed-value"));
    String result = cache.get("myKey", (Callable<String>) () -> "loaded");
    assertThat(result).isEqualTo("computed-value");
  }

  @Test
  @DisplayName("get returns null when result is null and allowNull is false")
  void get_whenNullAndNotAllowNull_returnsNull() {
    when(zeta.computeIfAbsentWithSoftExpireOptional(
        anyString(), any(ReadPolicy.class)
    )).thenReturn(Optional.empty());
    String result = cache.get("myKey", (Callable<String>) () -> null);
    assertThat(result).isNull();
    verify(zeta, never()).putThrough(anyString(), any(), any());
  }

  @Test
  @DisplayName("get returns null when result is null and allowNull is true")
  void get_whenNullAndAllowNull_returnsNull() {
    ZetaCacheContext.get().push(ReadPolicy.of(0, 0));
    when(zeta.computeIfAbsentWithSoftExpireOptional(
        anyString(), any(ReadPolicy.class)
    )).thenReturn(Optional.empty());
    String result = cache.get("myKey", (Callable<String>) () -> null);
    assertThat(result).isNull();
    verify(zeta, never()).putThrough(anyString(), any(), any());
    verify(zeta, never()).putLocal(anyString(), any());
  }

  @Test
  @DisplayName("get with allowNull and skipBroadcast does not call putLocal")
  void get_whenNullAllowNullAndSkipBroadcast_doesNotCallPutLocal() {
    ZetaCacheContext.get().push(ReadPolicy.of(0, 0), true);
    when(zeta.computeIfAbsentWithSoftExpireOptional(anyString(), any(ReadPolicy.class))).thenReturn(Optional.empty());
    String result = cache.get("myKey", (Callable<String>) () -> null);
    assertThat(result).isNull();
    verify(zeta, never()).putLocal(anyString(), any());
    verify(zeta, never()).putThrough(anyString(), any(), any());
  }

  @Test
  @DisplayName("get with allowNull=true adds key to nullCachedKeys")
  void get_whenAllowNull_addsToNullCachedKeys() {
    ZetaCacheContext.get().push(ReadPolicy.of(0, 0));
    when(zeta.computeIfAbsentWithSoftExpireOptional(anyString(), any(ReadPolicy.class))).thenReturn(Optional.empty());
    cache.get("myKey", (Callable<String>) () -> null);
    assertThat(cache.lookup("myKey")).isNull();
  }

  @Test
  @DisplayName("put stores value via hotKey.putThrough (default TTLs when no override)")
  void put_storesValue() {
    cache.put("myKey", "myValue");
    verify(zeta).putThrough(eq("test::myKey"), eq("myValue"), any(), eq(WritePolicy.of(0L, 0L)));
  }

  @Test
  @DisplayName("put with skipBroadcast uses putLocal")
  void put_withSkipBroadcast_usesPutLocal() {
    ZetaCacheContext.get().push(ReadPolicy.of(0, 0).withNullCaching(false), true);
    cache.put("myKey", "myValue");
    verify(zeta).putLocal("test::myKey", "myValue", WritePolicy.of(0L, 0L));
    verify(zeta, never()).putThrough(anyString(), any(), any());
  }

  @Test
  @DisplayName("put resolves the @CacheTTL override into the TTL-carrying putThrough")
  void put_withTtlOverride_passesTtlsToFacade() {
    ZetaCacheContext.get().push(ReadPolicy.of(5000L, 1000L).withNullCaching(false));
    cache.put("myKey", "myValue");
    ArgumentCaptor<WritePolicy> policy = ArgumentCaptor.forClass(WritePolicy.class);
    verify(zeta).putThrough(eq("test::myKey"), eq("myValue"), any(), policy.capture());
    assertThat(policy.getValue().hardTtlMs().getAsLong()).isEqualTo(5000L);
    assertThat(policy.getValue().softTtlMs().getAsLong()).isEqualTo(1000L);
  }

  @Test
  @DisplayName("put(null) with @NullCaching(false) writes nothing (next call re-invokes the method)")
  void put_null_withNullCachingDisabled_writesNothing() {
    ZetaCacheContext.get().push(ReadPolicy.of(0, 0).withNullCaching(false));

    cache.put("myKey", null);

    verify(zeta, never()).putThrough(anyString(), any(), any(), any(WritePolicy.class));
    verify(zeta, never()).putLocal(anyString(), any(), any(WritePolicy.class));
    verify(zeta, never()).putThrough(anyString(), any(), any());
    verify(zeta, never()).putLocal(anyString(), any());
  }

  @Test
  @DisplayName("evict with skipBroadcast calls invalidateLocal")
  void evict_withSkipBroadcast_callsInvalidateLocal() {
    ZetaCacheContext.get().push(ReadPolicy.of(0, 0).withNullCaching(false), true);
    cache.evict("myKey");
    verify(zeta).invalidate(eq("test::myKey"), argThat(InvalidatePolicy::skipBroadcast));
    verify(zeta, never()).invalidate(anyString());
  }

  @Test
  @DisplayName("put removes key from nullCachedKeys")
  void put_removesFromNullCachedKeys() {
    ZetaCacheContext.get().push(ReadPolicy.of(0, 0));
    when(zeta.computeIfAbsentWithSoftExpireOptional(anyString(), any(ReadPolicy.class))).thenReturn(Optional.empty());
    cache.get("myKey", (Callable<String>) () -> null);
    assertThat(cache.lookup("myKey")).isNull();
    ZetaCacheContext.get().restore(null);

    when(zeta.peekAndTag("test::myKey")).thenReturn("real-value");
    cache.put("myKey", "real-value");
    assertThat(cache.lookup("myKey")).isEqualTo("real-value");
  }

  @Test
  @DisplayName("evict removes from nullCachedKeys and calls invalidate")
  void evict_removesFromNullCachedKeysAndInvalidates() {
    ZetaCacheContext.get().push(ReadPolicy.of(0, 0));
    when(zeta.computeIfAbsentWithSoftExpireOptional(anyString(), any(ReadPolicy.class))).thenReturn(Optional.empty());
    cache.get("myKey", (Callable<String>) () -> null);
    ZetaCacheContext.get().restore(null);

    cache.evict("myKey");
    verify(zeta).invalidate("test::myKey");
    when(zeta.peekAndTag("test::myKey")).thenReturn(null);
    assertThat(cache.lookup("myKey")).isNull();
  }

  @Test
  @DisplayName("clear without app mode is a no-op")
  void clear_withoutLocalCache_isNoOp() {
    Zeta.StatsAdmin statsView = mock(Zeta.StatsAdmin.class);
    when(zeta.stats()).thenReturn(statsView);
    when(zeta.isApp()).thenReturn(false);
    cache.clear();
    verify(zeta, never()).invalidateAllLocal();
    verify(zeta, never()).invalidate(anyCollection(), any(InvalidatePolicy.class));
  }

  @Test
  @DisplayName("clear invalidates only this cache's keys, never the whole L1")
  void clear_invalidatesOnlyThisCachesKeys() {
    Zeta.StatsAdmin statsView = mock(Zeta.StatsAdmin.class);
    when(zeta.stats()).thenReturn(statsView);
    when(zeta.isApp()).thenReturn(true);
    when(statsView.localKeysWithPrefix("test::")).thenReturn(java.util.List.of("test::a", "test::b"));

    cache.clear();

    verify(zeta, never()).invalidateAllLocal();
    org.mockito.ArgumentCaptor<java.util.Collection<String>> captor =
      org.mockito.ArgumentCaptor.forClass(java.util.Collection.class);
    ArgumentCaptor<InvalidatePolicy> policyCaptor = ArgumentCaptor.forClass(InvalidatePolicy.class);
    verify(zeta).invalidate(captor.capture(), policyCaptor.capture());
    assertThat(captor.getValue()).containsExactlyInAnyOrder("test::a", "test::b");
    assertThat(policyCaptor.getValue().skipBroadcast()).isFalse();
  }

  @Test
  @DisplayName("lookup hit delegates to a single peekAndTag (one rule evaluation, one lookup behind the facade)")
  void lookup_hit_delegatesToPeekAndTag() {
    Zeta.RuleAdmin ruleAdmin = mock(Zeta.RuleAdmin.class);
    when(zeta.rules()).thenReturn(ruleAdmin);
    when(zeta.peekAndTag("test::myKey")).thenReturn("stored-value");

    assertThat(cache.lookup("myKey")).isEqualTo("stored-value");

    verify(zeta).peekAndTag("test::myKey");
    verify(zeta, never()).peek(anyString());
    verify(ruleAdmin, never()).evaluateRule(anyString());
    verify(zeta, never()).tag(anyString(), any(Zeta.TagMode.class));
  }

  @Test
  @DisplayName("lookup miss returns null without touching the facade beyond peekAndTag")
  void lookup_miss_returnsNull() {
    when(zeta.peekAndTag("test::myKey")).thenReturn(null);

    assertThat(cache.lookup("myKey")).isNull();
  }

  @Test
  @DisplayName("lookup with nullCachedKeys removed returns peek value")
  void lookup_afterNullCachedKeysRemoved_returnsPeekValue() {
    ZetaCacheContext.get().push(ReadPolicy.of(0, 0));
    when(zeta.computeIfAbsentWithSoftExpireOptional(anyString(), any(ReadPolicy.class))).thenReturn(Optional.empty());
    cache.get("myKey", (Callable<String>) () -> null);
    ZetaCacheContext.get().restore(null);

    cache.evict("myKey");
    when(zeta.peekAndTag("test::myKey")).thenReturn("new-value");
    assertThat(cache.lookup("myKey")).isEqualTo("new-value");
  }

  @Test
  @DisplayName("prefixedKey formats correctly")
  void prefixedKey_formatsCorrectly() {
    when(zeta.peekAndTag("test::myCustomKey")).thenReturn("v");
    assertThat(cache.lookup("myCustomKey")).isEqualTo("v");
  }

  @Test
  @DisplayName("prefixedKey uses custom separator when configured")
  void prefixedKey_usesCustomSeparator() {
    // The separator is bound at startup and captured in the per-cache key
    // prefix — configure it BEFORE the cache is constructed, then verify the
    // prefix shape.
    SpringCacheSettings customSettings = mock(SpringCacheSettings.class);
    when(customSettings.keySeparator()).thenReturn("-->");
    ZetaSpringCache custom = new ZetaSpringCache("test", zeta, customSettings, true);
    when(zeta.peekAndTag("test-->otherKey")).thenReturn("v2");
    assertThat(custom.lookup("otherKey")).isEqualTo("v2");
  }

  @Test
  @DisplayName("get wraps loader exception in ValueRetrievalException")
  void get_whenLoaderThrows_wrapsInValueRetrievalException() {
    when(zeta.computeIfAbsentWithSoftExpireOptional(anyString(), any(ReadPolicy.class))).thenAnswer(invocation -> {
      ReadPolicy p = invocation.getArgument(1);
      @SuppressWarnings("all")
      Supplier<Object> supplier = (Supplier<Object>) p.reader();
      supplier.get();
      return null;
    });
    assertThatThrownBy(() ->
      cache.get(
        "myKey",
        (Callable<String>) () -> {
          throw new RuntimeException("db error");
        }
      )
    ).isInstanceOf(ValueRetrievalException.class);
  }

  @Test
  @DisplayName("get invokes valueLoader successfully")
  void get_whenLoaderSucceeds_invokesLoader() {
    when(zeta.computeIfAbsentWithSoftExpireOptional(anyString(), any(ReadPolicy.class))).thenAnswer(invocation -> {
      ReadPolicy p = invocation.getArgument(1);
      @SuppressWarnings("all")
      Supplier<Object> supplier = (Supplier<Object>) p.reader();
      return Optional.ofNullable(supplier.get());
    });
    String result = cache.get("myKey", (Callable<String>) () -> "loaded");
    assertThat(result).isEqualTo("loaded");
  }

  @Test
  @DisplayName("put with skipBroadcast=false calls putThrough")
  void put_withSkipBroadcastFalse_callsPutThrough() {
    ZetaCacheContext.get().push(ReadPolicy.of(0, 0).withNullCaching(false));
    cache.put("myKey", "myValue");
    verify(zeta).putThrough(eq("test::myKey"), eq("myValue"), any(), eq(WritePolicy.of(0L, 0L)));
  }

  @Test
  @DisplayName("get with allowNull and skipBroadcast=false does not call putThrough")
  void get_whenNullAllowNullAndNoSkipBroadcast_doesNotCallPutThrough() {
    ZetaCacheContext.get().push(ReadPolicy.of(0, 0));
    when(zeta.computeIfAbsentWithSoftExpireOptional(anyString(), any(ReadPolicy.class))).thenReturn(Optional.empty());
    String result = cache.get("myKey", (Callable<String>) () -> null);
    assertThat(result).isNull();
    verify(zeta, never()).putThrough(anyString(), any(), any());
    verify(zeta, never()).putLocal(anyString(), any());
  }

  @Test
  @DisplayName("put(null) stores Zeta's sentinel with the short null TTL via putThrough")
  void put_null_storesZetaSentinelWithShortTtl() {
    when(settings.effectiveNullTtlMs()).thenReturn(10_000L);

    cache.put("myKey", null);

    ArgumentCaptor<WritePolicy> policy = ArgumentCaptor.forClass(WritePolicy.class);
    verify(zeta).putThrough(eq("test::myKey"), eq(NullValue.INSTANCE), any(), policy.capture());
    assertThat(policy.getValue().hardTtlMs().getAsLong()).isEqualTo(10_000L);
    assertThat(policy.getValue().softTtlMs().getAsLong()).isEqualTo(10_000L);
    assertThat(policy.getValue().skipBroadcast()).isFalse();
    verify(zeta, never()).putLocal(anyString(), any(), any(WritePolicy.class));
  }

  @Test
  @DisplayName("put(null) with skipBroadcast stores the sentinel locally with the short null TTL")
  void put_null_skipBroadcast_storesSentinelLocally() {
    when(settings.effectiveNullTtlMs()).thenReturn(10_000L);
    ZetaCacheContext.get().push(ReadPolicy.of(0, 0), true);

    cache.put("myKey", null);

    ArgumentCaptor<WritePolicy> policy = ArgumentCaptor.forClass(WritePolicy.class);
    verify(zeta).putLocal(eq("test::myKey"), eq(NullValue.INSTANCE), policy.capture());
    assertThat(policy.getValue().hardTtlMs().getAsLong()).isEqualTo(10_000L);
    assertThat(policy.getValue().softTtlMs().getAsLong()).isEqualTo(10_000L);
    verify(zeta, never()).putThrough(anyString(), any(), any(), any(WritePolicy.class));
  }

  @Test
  @DisplayName("put of an explicit Spring NullValue is translated to Zeta's sentinel")
  void put_springNullValue_translatedToZetaSentinel() {
    when(settings.effectiveNullTtlMs()).thenReturn(10_000L);

    cache.put("myKey", org.springframework.cache.support.NullValue.INSTANCE);

    ArgumentCaptor<WritePolicy> policy = ArgumentCaptor.forClass(WritePolicy.class);
    verify(zeta).putThrough(eq("test::myKey"), eq(NullValue.INSTANCE), any(), policy.capture());
    assertThat(policy.getValue().hardTtlMs().getAsLong()).isEqualTo(10_000L);
    assertThat(policy.getValue().softTtlMs().getAsLong()).isEqualTo(10_000L);
  }

  /**
   * Integration tests over a real {@link HotKeyCache} (no mocks on the cache
   * path) proving the cross-cache clear isolation, null-sentinel TTL, and
   * detection-on-lookup contracts end to end.
   */
  @Nested
  @DisplayName("integration over a real HotKeyCache")
  class Integration {

    private Zeta realZeta;
    private HotKeyDetector hotKeyDetector;
    private com.github.benmanes.caffeine.cache.Cache<String, CacheEntry> caffeineCache;
    private ZetaSpringCache alpha;
    private ZetaSpringCache beta;

    @BeforeEach
    @SuppressWarnings("all")
    void setUp() {
      hotKeyDetector = mock(HotKeyDetector.class);
      when(hotKeyDetector.contains(anyString())).thenReturn(false);
      caffeineCache =
        com.github.benmanes.caffeine.cache.Caffeine.newBuilder().maximumSize(100).build();
      com.github.benmanes.caffeine.cache.Cache<String, CacheEntry> caffeine = caffeineCache;
      ZetaProperties props = new ZetaProperties();
      ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
      EntryLifecycleImpl entryLifecycle = new EntryLifecycleImpl(caffeine, props, CacheCompressor.NONE, mock(HealthView.class));
      BackgroundRefresher backgroundRefresher =
        new DefaultBackgroundRefresher(caffeine, Runnable::run, entryLifecycle.ttlPolicy(), CacheCompressor.NONE, 10);
      RuleMatcher ruleMatcher = new RuleMatcherImpl(Optional.empty(), Optional.empty());
      HotKeyCache hotKeyCache = new HotKeyCache(
        hotKeyDetector,
        caffeine,
        mock(SingleFlight.class),
        entryLifecycle,
        backgroundRefresher,
        Runnable::run,
        new CentralDispatcher(
          Optional.empty(),
          Optional.empty(),
          new BroadcastBuffer(scheduler, Optional.empty(), 500, 2_000, null),
          hotKeyDetector
        ),
        ruleMatcher,
        new VersionControllerImpl(Optional.empty(), 60, new SnowflakeIdGenerator(0, 1, 5L, false)),
        props,
        mock(HealthView.class),
        CacheCompressor.NONE,
        null
      );
      RuleService ruleService = new RuleService(ruleMatcher, props);
      realZeta = new DefaultZeta(hotKeyCache, hotKeyDetector, null, null, ruleService);
      alpha = new ZetaSpringCache("alpha", realZeta, props, true);
      beta = new ZetaSpringCache("beta", realZeta, props, true);
    }

    @AfterEach
    void tearDown() {
      ZetaCacheContext.get().restore(null);
    }

    @Test
    @DisplayName("clearing one cache leaves the other cache's keys alive")
    void clear_isolatesByCacheName() {
      alpha.put("k1", "a1");
      beta.put("k2", "b2");

      alpha.clear();

      assertThat(realZeta.peek("alpha::k1")).isEmpty();
      assertThat(realZeta.peek("beta::k2")).contains("b2");

      beta.clear();
      assertThat(realZeta.peek("beta::k2")).isEmpty();
    }

    @Test
    @DisplayName("lookup hit feeds the local detector (annotation path detects)")
    void lookup_hit_feedsDetector() {
      alpha.put("k1", "a1");

      assertThat(alpha.lookup("k1")).isEqualTo("a1");

      verify(hotKeyDetector, atLeastOnce()).add("alpha::k1");
    }

    @Test
    @DisplayName("put(null) stores Zeta's sentinel with the short null TTL; lookup returns Spring's marker")
    void put_null_storesShortTtlSentinel() {
      alpha.put("nk", null);

      Object raw = caffeineCache.getIfPresent("alpha::nk");
      assertThat(raw).isInstanceOf(CacheEntry.class);
      CacheEntry entry = (CacheEntry) raw;
      assertThat(entry.getValue()).isEqualTo(NullValue.INSTANCE);
      // Short null TTL (default 10s): hard expiry within [now+9s, now+11s]
      // even under the ±5% TTL jitter — NOT the 300s default TTL.
      long now = System.currentTimeMillis();
      assertThat(entry.getHardExpireAtMs()).isBetween(now + 9_000L, now + 11_000L);

      // Lookup surfaces Spring's marker so Spring skips the method invocation.
      assertThat(alpha.lookup("nk")).isEqualTo(org.springframework.cache.support.NullValue.INSTANCE);
      // And the sentinel hit is still counted for detection.
      verify(hotKeyDetector, atLeastOnce()).add("alpha::nk");
    }

    @Test
    @DisplayName("put resolves the @CacheTTL override into the stored entry (not the global default)")
    void put_withTtlOverride_storesOverriddenTtl() {
      ZetaCacheContext.get().push(ReadPolicy.of(5_000L, 0L).withNullCaching(false));
      alpha.put("tlock", "v");

      CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("alpha::tlock");
      assertThat(entry).isNotNull();
      // 5s override (±5% jitter) — NOT the 300s default.
      long now = System.currentTimeMillis();
      assertThat(entry.getHardExpireAtMs()).isBetween(now + 4_700L, now + 5_300L);
    }

    @Test
    @DisplayName("put(null) with @NullCaching(false) leaves no entry; the next lookup re-invokes")
    void put_null_withNullCachingDisabled_leavesNoEntry() {
      ZetaCacheContext.get().push(ReadPolicy.of(0, 0).withNullCaching(false));
      alpha.put("nk2", null);

      assertThat(caffeineCache.getIfPresent("alpha::nk2")).isNull();
      assertThat(alpha.lookup("nk2")).isNull();
    }

    @Test
    @DisplayName("lookup hit on a whitelisted key still detects (annotation path)")
    void lookup_whitelistedKey_stillDetects() {
      realZeta.rules().addWhitelist("alpha::wlk");
      alpha.put("wlk", "wl-value");

      assertThat(alpha.lookup("wlk")).isEqualTo("wl-value");
      verify(hotKeyDetector, atLeastOnce()).add("alpha::wlk");
    }

    @Test
    @DisplayName("lookup of a blocked key throws ZetaBlockedException")
    void lookup_blockedKey_throws() {
      realZeta.rules().addBlacklist("alpha::blk");
      caffeineCache.put(
        "alpha::blk",
        CacheEntry.builder().value("cached-value").dataVersion(0).hardExpireAtMs(Long.MAX_VALUE).build()
      );

      assertThatThrownBy(() -> alpha.lookup("blk")).isInstanceOf(ZetaBlockedException.class);
      verify(hotKeyDetector, never()).add("alpha::blk");
    }
  }
}
