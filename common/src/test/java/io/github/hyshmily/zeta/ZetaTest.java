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
package io.github.hyshmily.zeta;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.cache.loader.ZetaLoaderRegistry;
import io.github.hyshmily.zeta.cache.loader.ZetaLoadingSpec;
import io.github.hyshmily.zeta.exception.ZetaBlockedException;
import io.github.hyshmily.zeta.exception.ZetaModeException;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.zeta.model.CachePolicy;
import io.github.hyshmily.zeta.model.StalePolicy;
import io.github.hyshmily.zeta.model.ZetaCacheStats;
import io.github.hyshmily.zeta.rule.Rule;
import io.github.hyshmily.zeta.rule.Rule.RuleAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class ZetaTest {

  private HotKeyCache hotKeyCache;
  private HotKeyDetector appDetector;
  private Zeta zeta;

  @BeforeEach
  @SuppressWarnings("all")
  void setUp() {
    hotKeyCache = mock(HotKeyCache.class);
    appDetector = mock(HotKeyDetector.class);
    // Mirror the real TtlPolicy behaviour (softTtlMs > 0 is used as-is). Without this stub the
    // mock default 0 would collapse every refresh interval to 1ms, turning destroy_shouldNotThrow
    // into a race between the scheduled refresh and the cancellation.
    when(hotKeyCache.resolveEffectiveSoftTtl(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));
    zeta = new Zeta(hotKeyCache, appDetector);
  }

  @Test
  void isLocalZeta_shouldDelegateToCache() {
    when(hotKeyCache.isHot("key1")).thenReturn(true);
    assertThat(zeta.isLocalHotKey("key1")).isTrue();
    verify(hotKeyCache).isHot("key1");
  }

  @Test
  void peek_shouldDelegateToCache() {
    when(hotKeyCache.peek("key1")).thenReturn(Optional.of("value"));
    assertThat(zeta.peek("key1")).contains("value");
    verify(hotKeyCache).peek("key1");
  }

  @Test
  void get_shouldDelegateToCache() {
    when(hotKeyCache.get(anyString(), any(CachePolicy.class))).thenReturn(Optional.of("value"));
    assertThat(zeta.get("key1", () -> "loaded")).contains("value");
    verify(hotKeyCache).get(anyString(), any(CachePolicy.class));
  }

  @Test
  void get_withTtl_shouldDelegateToCache() {
    when(hotKeyCache.get(anyString(), any(CachePolicy.class))).thenReturn(Optional.of("v"));
    assertThat(zeta.get("key1", CachePolicy.of(() -> "loaded").withHardTtl(1000L).withSoftTtl(100L))).contains("v");
    verify(hotKeyCache).get(anyString(), any(CachePolicy.class));
  }

  @Test
  void getWithSoftExpire_shouldDelegateToCache() {
    when(hotKeyCache.getWithSoftExpire(anyString(), any(CachePolicy.class))).thenReturn(
      Optional.of("v")
    );
    assertThat(zeta.getWithSoftExpire("key1", () -> "v")).contains("v");
    verify(hotKeyCache).getWithSoftExpire(anyString(), any(CachePolicy.class));
  }

  // ── No-reader get via ZetaLoaderRegistry (ADR-0070) ──

  @Test
  @SuppressWarnings("all")
  void getNoReader_shouldLoadThroughRegisteredSpec() {
    ZetaLoaderRegistry registry = new ZetaLoaderRegistry();
    registry.register("user:", ZetaLoadingSpec.<String>builder()
      .loader(key -> "loaded:" + key)
      .hardTtl(5_000)
      .softTtl(500)
      .build());
    Zeta withRegistry = new Zeta(hotKeyCache, appDetector, null, registry);
    when(hotKeyCache.get(anyString(), any(CachePolicy.class))).thenReturn(Optional.of("loaded:user:42"));

    assertThat(withRegistry.get("user:42")).contains("loaded:user:42");
    verify(hotKeyCache)
      .get(eq("user:42"), argThat(p ->
        p.hardTtlMs().getAsLong() == 5_000
          && p.softTtlMs().getAsLong() == 500
          && "loaded:user:42".equals(p.reader().get())
      ));
  }

  @Test
  @SuppressWarnings("all")
  void getWithSoftExpireNoReader_shouldForceSoftRefresh() {
    ZetaLoaderRegistry registry = new ZetaLoaderRegistry();
    registry.register(
        "user:", ZetaLoadingSpec.<String>builder().loader(key -> "v").stalePolicy(StalePolicy.RETURN).build());
    Zeta withRegistry = new Zeta(hotKeyCache, appDetector, null, registry);
    when(hotKeyCache.getWithSoftExpire(anyString(), any(CachePolicy.class))).thenReturn(Optional.of("v"));

    assertThat(withRegistry.getWithSoftExpire("user:42")).contains("v");
    verify(hotKeyCache).getWithSoftExpire(eq("user:42"), argThat(p -> p.stalePolicy() == StalePolicy.SOFT_REFRESH));
  }

  @Test
  void getNoReader_withoutRegistry_shouldFailFast() {
    assertThatThrownBy(() -> zeta.get("user:42")).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void getNoReader_withoutMatchingPrefix_shouldFailFast() {
    ZetaLoaderRegistry registry = new ZetaLoaderRegistry();
    registry.register("user:", ZetaLoadingSpec.of(key -> "v"));
    Zeta withRegistry = new Zeta(hotKeyCache, appDetector, null, registry);
    assertThatThrownBy(() -> withRegistry.get("vendor:42")).isInstanceOf(IllegalStateException.class);
  }

  @Test
  @SuppressWarnings("all")
  void getNoReaderBatch_shouldGroupBySpecAndMerge() {
    ZetaLoaderRegistry registry = new ZetaLoaderRegistry();
    registry.register(
      "user:",
      ZetaLoadingSpec.<String>builder().loader(key -> "loaded:" + key).hardTtl(5_000).softTtl(500).build()
    );
    registry.register("order:", ZetaLoadingSpec.of(key -> "O:" + key));
    Zeta withRegistry = new Zeta(hotKeyCache, appDetector, null, registry);

    when(hotKeyCache.get(any(Iterable.class), any(Function.class), anyLong(), anyLong(), anyBoolean(), anyBoolean()))
      .thenAnswer(inv -> {
        Iterable<String> keys = (Iterable<String>) inv.getArgument(0);
        Function<String, String> reader = (Function<String, String>) inv.getArgument(1);
        Map<String, Optional<String>> out = new LinkedHashMap<>();
        for (String k : keys) {
          out.put(k, Optional.ofNullable(reader.apply(k)));
        }
        return out;
      });

    Map<String, Optional<String>> result = withRegistry.getAll(List.of("user:42", "order:7", "user:43"));

    assertThat(result)
      .containsEntry("user:42", Optional.of("loaded:user:42"))
      .containsEntry("order:7", Optional.of("O:order:7"))
      .containsEntry("user:43", Optional.of("loaded:user:43"))
      .hasSize(3);

    // Two spec groups: user: keys share the user spec (hard 5000/soft 500), order: keys its own (0/0).
    ArgumentCaptor<Iterable> keysCaptor = ArgumentCaptor.forClass(Iterable.class);
    ArgumentCaptor<Long> hardCaptor = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Long> softCaptor = ArgumentCaptor.forClass(Long.class);
    verify(hotKeyCache, times(2))
      .get(
        keysCaptor.capture(),
        any(Function.class),
        hardCaptor.capture(),
        softCaptor.capture(),
        anyBoolean(),
        anyBoolean());
    assertThat(hardCaptor.getAllValues()).containsExactly(5_000L, 0L);
    assertThat(softCaptor.getAllValues()).containsExactly(500L, 0L);
    assertThat((List<String>) keysCaptor.getAllValues().get(0)).containsExactly("user:42", "user:43");
    assertThat((List<String>) keysCaptor.getAllValues().get(1)).containsExactly("order:7");
  }

  @Test
  @SuppressWarnings("all")
  void getNoReaderBatch_unmatchedKeyFailsFast() {
    ZetaLoaderRegistry registry = new ZetaLoaderRegistry();
    registry.register("user:", ZetaLoadingSpec.of(key -> "U"));
    Zeta withRegistry = new Zeta(hotKeyCache, appDetector, null, registry);

    assertThatThrownBy(() -> withRegistry.getAll(List.of("user:42", "vendor:9")))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("No CacheLoader registered for key 'vendor:9'");
    verify(hotKeyCache, never())
      .get(any(Iterable.class), any(Function.class), anyLong(), anyLong(), anyBoolean(), anyBoolean());
  }

  @Test
  @SuppressWarnings("all")
  void getWithSoftExpireNoReaderBatch_shouldRoutePerSpec() {
    ZetaLoaderRegistry registry = new ZetaLoaderRegistry();
    registry.register("user:", ZetaLoadingSpec.<String>builder().loader(key -> "U:" + key).hardTtl(5_000).build());
    Zeta withRegistry = new Zeta(hotKeyCache, appDetector, null, registry);

    when(hotKeyCache
        .getWithSoftExpire(any(Iterable.class), any(Function.class), anyLong(), anyLong(), anyBoolean(), anyBoolean()))
      .thenAnswer(inv -> {
        Iterable<String> keys = (Iterable<String>) inv.getArgument(0);
        Function<String, String> reader = (Function<String, String>) inv.getArgument(1);
        Map<String, Optional<String>> out = new LinkedHashMap<>();
        for (String k : keys) {
          out.put(k, Optional.ofNullable(reader.apply(k)));
        }
        return out;
      });

    Map<String, Optional<String>> result = withRegistry.getAllWithSoftExpire(List.of("user:1", "user:2"));

    assertThat(result)
      .containsEntry("user:1", Optional.of("U:user:1"))
      .containsEntry("user:2", Optional.of("U:user:2"));
    ArgumentCaptor<Long> hardCaptor = ArgumentCaptor.forClass(Long.class);
    verify(hotKeyCache)
      .getWithSoftExpire(
          any(Iterable.class), any(Function.class), hardCaptor.capture(), anyLong(), anyBoolean(), anyBoolean());
    assertThat(hardCaptor.getValue()).isEqualTo(5_000L);
  }

  @Test
  @SuppressWarnings("all")
  void computeIfAbsentNoReader_shouldCarrySpecPolicy() {
    ZetaLoaderRegistry registry = new ZetaLoaderRegistry();
    registry.register(
      "user:",
      ZetaLoadingSpec.<String>builder().loader(key -> "loaded:" + key).hardTtl(5_000).softTtl(500).build()
    );
    Zeta withRegistry = new Zeta(hotKeyCache, appDetector, null, registry);

    when(hotKeyCache.computeIfAbsent(anyString(), any(CachePolicy.class))).thenAnswer(inv -> {
      CachePolicy policy = inv.getArgument(1);
      return Optional.ofNullable(policy.reader().get());
    });

    String value = withRegistry.computeIfAbsent("user:42");
    assertThat(value).isEqualTo("loaded:user:42");

    ArgumentCaptor<CachePolicy> policyCaptor = ArgumentCaptor.forClass(CachePolicy.class);
    verify(hotKeyCache).computeIfAbsent(eq("user:42"), policyCaptor.capture());
    CachePolicy policy = policyCaptor.getValue();
    assertThat(policy.hardTtlMs().getAsLong()).isEqualTo(5_000);
    assertThat(policy.softTtlMs().getAsLong()).isEqualTo(500);
  }

  @Test
  @SuppressWarnings("all")
  void computeIfAbsentWithSoftExpireNoReader_shouldForceSoftRefresh() {
    ZetaLoaderRegistry registry = new ZetaLoaderRegistry();
    registry.register("user:", ZetaLoadingSpec.<String>builder().loader(key -> "U:" + key).softTtl(500).build());
    Zeta withRegistry = new Zeta(hotKeyCache, appDetector, null, registry);

    when(hotKeyCache.computeIfAbsentWithSoftExpire(anyString(), any(CachePolicy.class))).thenAnswer(inv -> {
      CachePolicy policy = inv.getArgument(1);
      return Optional.ofNullable(policy.reader().get());
    });

    String value = withRegistry.computeIfAbsentWithSoftExpire("user:42");
    assertThat(value).isEqualTo("U:user:42");

    ArgumentCaptor<CachePolicy> policyCaptor = ArgumentCaptor.forClass(CachePolicy.class);
    verify(hotKeyCache).computeIfAbsentWithSoftExpire(eq("user:42"), policyCaptor.capture());
    assertThat(policyCaptor.getValue().stalePolicy()).isEqualTo(StalePolicy.SOFT_REFRESH);
    assertThat(policyCaptor.getValue().softTtlMs().getAsLong()).isEqualTo(500);
  }

  @Test
  void invalidate_shouldDelegateToCache() {
    zeta.invalidate("key1");
    verify(hotKeyCache).invalidate("key1", true);
  }

  @Test
  void invalidate_varargs_shouldDelegateToCache() {
    zeta.invalidate(List.of("k1", "k2"));
    verify(hotKeyCache).invalidate(List.of("k1", "k2"), true);
  }

  @Test
  void putThrough_shouldDelegateToCache() {
    zeta.putThrough("key1", "value", () -> {});
    verify(hotKeyCache).putThrough(anyString(), any(), any(), anyLong(), anyLong(), anyBoolean());
  }

  @Test
  void putThrough_withTtl_shouldDelegateToCache() {
    zeta.putThrough("key1", "value", () -> {}, CachePolicy.of(2000L, 200L));
    verify(hotKeyCache).putThrough(anyString(), any(), any(), anyLong(), anyLong(), anyBoolean());
  }

  @Test
  void invalidateAfterPut_single_shouldDelegateToCache() {
    zeta.invalidateAfterPut("key1", () -> {});
    verify(hotKeyCache).invalidateAfterPut(anyString(), any(), anyBoolean());
  }

  @Test
  void returnHotKeys_shouldReturnLocalFromTopK() {
    when(appDetector.list()).thenReturn(List.of(new Item("k1", 10)));
    assertThat(zeta.returnLocalHotKeys()).hasSize(1);
  }

  @Test
  void returnHotKeys_shouldReturnLocalEmptyWhenTopKNull() {
    Zeta hk = new Zeta(hotKeyCache, null);
    assertThat(hk.returnLocalHotKeys()).isEmpty();
  }

  @Test
  void returnTotalDataStreams_shouldReturnLocalFromTopK() {
    when(appDetector.total()).thenReturn(100L);
    assertThat(zeta.returnLocalTotalDataStreams()).isEqualTo(100L);
  }

  @Test
  void returnTotalDataStreams_shouldReturnLocalZeroWhenTopKNull() {
    assertThat(new Zeta(hotKeyCache, null).returnLocalTotalDataStreams()).isZero();
  }

  @Test
  void returnExpelledHotKeys_shouldReturnLocalFromTopK() {
    LinkedBlockingQueue<Item> queue = new LinkedBlockingQueue<>();
    queue.add(new Item("k1", 5));
    when(appDetector.expelled()).thenReturn(queue);
    assertThat(zeta.returnLocalExpelledHotKeys()).hasSize(1);
  }

  @Test
  void cacheMethods_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null);
    assertThatThrownBy(() -> workerOnly.get("k", () -> "v")).isInstanceOf(ZetaModeException.class);
    assertThatThrownBy(() -> workerOnly.isLocalHotKey("k")).isInstanceOf(ZetaModeException.class);
    assertThatThrownBy(() -> workerOnly.peek("k")).isInstanceOf(ZetaModeException.class);
    assertThatThrownBy(() -> workerOnly.invalidate("k")).isInstanceOf(ZetaModeException.class);
    assertThatThrownBy(() -> workerOnly.putThrough("k", "v", () -> {})).isInstanceOf(ZetaModeException.class);
    assertThatThrownBy(() -> workerOnly.invalidateAfterPut("k", () -> {})).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void get_shouldPropagateZetaBlockedException() {
    when(hotKeyCache.get(anyString(), any(CachePolicy.class))).thenThrow(
      new ZetaBlockedException("HotKeyCache", "secret")
    );
    assertThatThrownBy(() -> zeta.get("secret", () -> "v")).isInstanceOf(ZetaBlockedException.class);
  }

  // ── Additional getWithSoftExpire overloads ──

  @Test
  void getWithSoftExpire_withSoftTtl_shouldDelegateToCache() {
    when(hotKeyCache.getWithSoftExpire(anyString(), any(CachePolicy.class))).thenReturn(
      Optional.of("v")
    );
    assertThat(zeta.getWithSoftExpire("key1", CachePolicy.of(() -> "v").withSoftTtl(200L))).contains("v");
    verify(hotKeyCache).getWithSoftExpire(anyString(), any(CachePolicy.class));
  }

  // ── returnLocalExpelledHotKeys null guard ──

  @Test
  void returnExpelledHotKeys_shouldReturnEmptyQueueWhenTopKNull() {
    Zeta hk = new Zeta(hotKeyCache, null);
    assertThat(hk.returnLocalExpelledHotKeys()).isEmpty();
  }

  // ── returnLocalTotalDataStreams null guard (2-arg ctor) ──

  @Test
  void returnTotalDataStreams_shouldReturnLocalZeroWhenTopKNullTwoArg() {
    Zeta hk = new Zeta(hotKeyCache, null);
    assertThat(hk.returnLocalTotalDataStreams()).isZero();
  }

  // ── getLocalCache ──

  @SuppressWarnings("all")
  @Test
  void getLocalCache_shouldDelegateToCache() {
    Cache<String, Object> caffeine = mock(Cache.class);
    when(hotKeyCache.getLocalCache()).thenReturn(caffeine);
    assertThat(zeta.getLocalCache()).isSameAs(caffeine);
    verify(hotKeyCache).getLocalCache();
  }

  @Test
  void getLocalCache_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null);
    assertThatThrownBy(workerOnly::getLocalCache).isInstanceOf(ZetaModeException.class);
  }

  // ── addBlacklist / removeBlacklist / addWhitelist / removeWhitelist ──

  @Test
  void addBlacklist_shouldDelegateToCache() {
    zeta.addBlacklist("secret-*");
    verify(hotKeyCache).addBlacklist("secret-*");
  }

  @Test
  void addBlacklist_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null);
    assertThatThrownBy(() -> workerOnly.addBlacklist("x")).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void removeBlacklist_shouldDelegateToCache() {
    zeta.removeBlacklist("old-rule");
    verify(hotKeyCache).unBlacklist("old-rule");
  }

  @Test
  void removeBlacklist_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null);
    assertThatThrownBy(() -> workerOnly.removeBlacklist("x")).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void addWhitelist_shouldDelegateToCache() {
    zeta.addWhitelist("health-*");
    verify(hotKeyCache).addWhitelist("health-*");
  }

  @Test
  void addWhitelist_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null);
    assertThatThrownBy(() -> workerOnly.addWhitelist("x")).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void removeWhitelist_shouldDelegateToCache() {
    zeta.removeWhitelist("health-*");
    verify(hotKeyCache).unWhitelist("health-*");
  }

  @Test
  void removeWhitelist_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null);
    assertThatThrownBy(() -> workerOnly.removeWhitelist("x")).isInstanceOf(ZetaModeException.class);
  }

  // ── getAllRules ──

  @Test
  void getAllRules_shouldDelegateToCache() {
    Rule rule = new Rule(Rule.RuleType.EXACT, "blocked", RuleAction.BLOCK);
    when(hotKeyCache.getAllRules()).thenReturn(List.of(rule));
    assertThat(zeta.getAllRules()).containsExactly(rule);
    verify(hotKeyCache).getAllRules();
  }

  @Test
  void getAllRules_shouldReturnEmptyWhenCacheNull() {
    Zeta workerOnly = new Zeta(null, null);
    assertThat(workerOnly.getAllRules()).isEmpty();
  }

  // ── evaluateRule ──

  @Test
  void evaluateRule_shouldDelegateToCache() {
    when(hotKeyCache.evaluateRule("secret")).thenReturn(RuleAction.BLOCK);
    assertThat(zeta.evaluateRule("secret")).isEqualTo(RuleAction.BLOCK);
    verify(hotKeyCache).evaluateRule("secret");
  }

  @Test
  void evaluateRule_shouldReturnAllowWhenCacheNull() {
    Zeta workerOnly = new Zeta(null, null);
    assertThat(workerOnly.evaluateRule("any")).isEqualTo(RuleAction.ALLOW);
  }

  // ── clearAllRules ──

  @Test
  void clearAllRules_shouldDelegateToCache() {
    zeta.clearAllRules();
    verify(hotKeyCache).clearAllRules();
  }

  @Test
  void clearAllRules_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null);
    assertThatThrownBy(workerOnly::clearAllRules).isInstanceOf(ZetaModeException.class);
  }

  // ── broadcastAllLocalRulesManually ──

  @Test
  void broadcastAllLocalRulesManually_shouldDelegateToCache() {
    zeta.broadcastAllLocalRulesManually();
    verify(hotKeyCache).broadcastAllLocalRulesManually();
  }

  @Test
  void broadcastAllLocalRulesManually_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null);
    assertThatThrownBy(workerOnly::broadcastAllLocalRulesManually).isInstanceOf(ZetaModeException.class);
  }

  // ── notifyLocalDetector ──

  @Test
  void notifyLocalDetector_shouldDelegateToTopK() {
    zeta.notifyLocalDetector("my-key");
    verify(appDetector).add("my-key");
  }

  @Test
  void notifyLocalDetector_shouldIgnoreNullKey() {
    zeta.notifyLocalDetector((String) null);
    verify(appDetector, never()).add(anyString());
  }

  // ── read / write factories ──

  @Test
  void read_shouldReturnZetaReadQuery() {
    when(hotKeyCache.evaluateRule("k")).thenReturn(RuleAction.ALLOW);
    when(hotKeyCache.get(anyString(), any(CachePolicy.class))).thenReturn(Optional.of("v"));
    assertThat(
      zeta
        .read("k")
        .withPrimary(() -> "db")
        .execute()
    ).contains("v");
  }

  @Test
  void write_shouldReturnZetaWriteCommand() {
    zeta.write("k").invalidate();
    verify(hotKeyCache).invalidate("k", true);
  }

  // ── computeIfAbsent single-key ──

  @Test
  void computeIfAbsent_shouldReturnValue() {
    when(hotKeyCache.computeIfAbsent(anyString(), any(CachePolicy.class))).thenReturn(
      Optional.of("v")
    );
    assertThat(zeta.computeIfAbsent("k", () -> "loaded")).isEqualTo("v");
    verify(hotKeyCache).computeIfAbsent(eq("k"), any(CachePolicy.class));
  }

  @Test
  void computeIfAbsent_shouldReturnNullWhenLoaderNull() {
    when(hotKeyCache.computeIfAbsent(anyString(), any(CachePolicy.class))).thenReturn(
      Optional.empty()
    );
    assertThat((Object) zeta.computeIfAbsent("k", () -> null)).isNull();
  }

  @Test
  void computeIfAbsent_withHardTtl_shouldDelegate() {
    when(hotKeyCache.computeIfAbsent(anyString(), any(CachePolicy.class))).thenReturn(
      Optional.of("v")
    );
    assertThat(zeta.computeIfAbsent("k", CachePolicy.of(() -> "db").withHardTtl(5000L))).contains("v");
    verify(hotKeyCache).computeIfAbsent(eq("k"), any(CachePolicy.class));
  }

  @Test
  void computeIfAbsent_withHardSoftTtl_shouldDelegate() {
    when(hotKeyCache.computeIfAbsent(anyString(), any(CachePolicy.class))).thenReturn(
      Optional.of("v")
    );
    assertThat(
        zeta.computeIfAbsent("k", CachePolicy.of(() -> "db", 5000L, 500L, true, true, StalePolicy.SOFT_REFRESH)))
      .contains("v");
    verify(hotKeyCache).computeIfAbsent(eq("k"), any(CachePolicy.class));
  }

  @Test
  void computeIfAbsent_withNonDefaultReport_shouldDelegate() {
    when(hotKeyCache.computeIfAbsent(anyString(), any(CachePolicy.class))).thenReturn(
      Optional.of("v")
    );
    assertThat(
        zeta.computeIfAbsent("k", CachePolicy.of(() -> "db", 0L, 0L, true, false, StalePolicy.SOFT_REFRESH)))
      .contains("v");
    verify(hotKeyCache).computeIfAbsent(eq("k"), any(CachePolicy.class));
  }

  @Test
  void computeIfAbsentWithSoftExpire_SoftTtl_shouldDelegate() {
    when(hotKeyCache.computeIfAbsentWithSoftExpire(anyString(), any(CachePolicy.class))).thenReturn(
      Optional.of("v")
    );
    assertThat(zeta.computeIfAbsentWithSoftExpire("k", CachePolicy.of(() -> "db").withSoftTtl(500L))).contains("v");
    verify(hotKeyCache).computeIfAbsentWithSoftExpire(eq("k"), any(CachePolicy.class));
  }

  @Test
  void computeIfAbsentWithSoftExpire_collectionSoftTtl_shouldDelegate() {
    when(hotKeyCache.computeIfAbsentWithSoftExpire(anyString(), any(CachePolicy.class))).thenReturn(
      Optional.of("v")
    );
    assertThat(zeta.computeIfAbsentWithSoftExpire("k", CachePolicy.of(() -> "db").withSoftTtl(500L))).contains("v");
    verify(hotKeyCache).computeIfAbsentWithSoftExpire(eq("k"), any(CachePolicy.class));
  }

  @Test
  void computeIfAbsentWithSoftExpire_BothTtls_shouldDelegate() {
    when(hotKeyCache.computeIfAbsentWithSoftExpire(anyString(), any(CachePolicy.class))).thenReturn(
      Optional.of("v")
    );
    assertThat(
        zeta.computeIfAbsentWithSoftExpire("k", CachePolicy.of(() -> "db").withHardTtl(5000L).withSoftTtl(500L)))
      .contains("v");
    verify(hotKeyCache).computeIfAbsentWithSoftExpire(eq("k"), any(CachePolicy.class));
  }

  @Test
  void computeIfAbsentWithSoftExpire_BothTtlsAndReport_shouldDelegate() {
    when(hotKeyCache.computeIfAbsentWithSoftExpire(anyString(), any(CachePolicy.class))).thenReturn(
      Optional.of("v")
    );
    assertThat(zeta.computeIfAbsentWithSoftExpire(
        "k", CachePolicy.of(() -> "db", 5000L, 500L, true, true, StalePolicy.SOFT_REFRESH)))
      .contains("v");
    verify(hotKeyCache).computeIfAbsentWithSoftExpire(eq("k"), any(CachePolicy.class));
  }

  // ── invalidateAllLocal no-arg ──

  @Test
  void invalidateAllLocal_shouldDelegateToCache() {
    zeta.invalidateAllLocal();
    verify(hotKeyCache).invalidateAllLocal();
  }

  @Test
  void invalidateAllLocal_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null);
    assertThatThrownBy(workerOnly::invalidateAllLocal).isInstanceOf(ZetaModeException.class);
  }

  // ── invalidate Collection ──

  @Test
  void invalidate_collection_shouldDelegateToCache() {
    zeta.invalidate(List.of("k1", "k2"));
    verify(hotKeyCache).invalidate(List.of("k1", "k2"), true);
  }

  // ── putLocal ──

  @Test
  void putLocal_shouldDelegateToCache() {
    zeta.putLocal("k", "v");
    verify(hotKeyCache).putLocal("k", "v", 0L, 0L);
  }

  @Test
  void putLocal_withTtl_shouldDelegateToCache() {
    zeta.putLocal("k", "v", CachePolicy.of(5000L, 500L));
    verify(hotKeyCache).putLocal("k", "v", 5000L, 500L);
  }

  // ── compareAndSet / compareAndInvalidate ──

  @Test
  void compareAndSet_shouldDelegateToCache() {
    when(hotKeyCache.compareAndSet(eq("k"), eq("old"), eq("new"))).thenReturn(true);
    assertThat(zeta.compareAndSet("k", "old", "new")).isTrue();
    verify(hotKeyCache).compareAndSet("k", "old", "new");
  }

  @Test
  void compareAndSet_shouldReturnFalseOnMismatch() {
    when(hotKeyCache.compareAndSet(eq("k"), eq("wrong"), eq("new"))).thenReturn(false);
    assertThat(zeta.compareAndSet("k", "wrong", "new")).isFalse();
    verify(hotKeyCache).compareAndSet("k", "wrong", "new");
  }

  @Test
  void compareAndSet_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null);
    assertThatThrownBy(() -> workerOnly.compareAndSet("k", "old", "new")).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void compareAndInvalidate_shouldDelegateToCache() {
    when(hotKeyCache.compareAndInvalidate(eq("k"), eq("old"))).thenReturn(true);
    assertThat(zeta.compareAndInvalidate("k", "old")).isTrue();
    verify(hotKeyCache).compareAndInvalidate("k", "old");
  }

  @Test
  void compareAndInvalidate_shouldReturnFalseOnMismatch() {
    when(hotKeyCache.compareAndInvalidate(eq("k"), eq("wrong"))).thenReturn(false);
    assertThat(zeta.compareAndInvalidate("k", "wrong")).isFalse();
    verify(hotKeyCache).compareAndInvalidate("k", "wrong");
  }

  @Test
  void compareAndInvalidate_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null);
    assertThatThrownBy(() -> workerOnly.compareAndInvalidate("k", "old")).isInstanceOf(ZetaModeException.class);
  }

  // ── getAndSet / putIfAbsent ──

  @Test
  void getAndSet_shouldDelegateToCache() {
    @SuppressWarnings("all")
    Optional<Object> old = (Optional) Optional.of("old");
    when(hotKeyCache.getAndSet(eq("k"), eq("new"), eq(0L), eq(0L))).thenReturn(old);
    assertThat(zeta.getAndSet("k", "new", CachePolicy.defaults())).isSameAs(old);
    verify(hotKeyCache).getAndSet("k", "new", 0L, 0L);
  }

  @Test
  void getAndSet_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null);
    assertThatThrownBy(() -> workerOnly.getAndSet("k", "v", CachePolicy.defaults()))
      .isInstanceOf(ZetaModeException.class);
  }

  @Test
  void putIfAbsent_shouldDelegateToCache() {
    when(hotKeyCache.putIfAbsent(eq("k"), eq("v"), eq(0L), eq(0L))).thenReturn(true);
    assertThat(zeta.putIfAbsent("k", "v", CachePolicy.defaults())).isTrue();
    verify(hotKeyCache).putIfAbsent("k", "v", 0L, 0L);
  }

  @Test
  void putIfAbsent_shouldReturnFalseWhenPresent() {
    when(hotKeyCache.putIfAbsent(eq("k"), eq("v"), eq(0L), eq(0L))).thenReturn(false);
    assertThat(zeta.putIfAbsent("k", "v", CachePolicy.defaults())).isFalse();
    verify(hotKeyCache).putIfAbsent("k", "v", 0L, 0L);
  }

  @Test
  void putIfAbsent_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null);
    assertThatThrownBy(() -> workerOnly.putIfAbsent("k", "v", CachePolicy.defaults()))
      .isInstanceOf(ZetaModeException.class);
  }

  // ── estimatedSizeOfKeysCount ──

  @Test
  void estimatedSize_shouldDelegateToCache() {
    when(hotKeyCache.estimatedSize()).thenReturn(42L);
    assertThat(zeta.estimatedSize()).isEqualTo(42L);
    verify(hotKeyCache).estimatedSize();
  }

  @Test
  void estimatedSize_shouldReturnZeroInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null);
    assertThat(workerOnly.estimatedSize()).isZero();
  }

  // ── stats ──

  @Test
  void stats_shouldDelegateToCache() {
    ZetaCacheStats stats = mock(ZetaCacheStats.class);
    when(hotKeyCache.stats()).thenReturn(stats);
    assertThat(zeta.stats()).isSameAs(stats);
    verify(hotKeyCache).stats();
  }

  @Test
  void stats_shouldReturnNullInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null);
    assertThat(workerOnly.stats()).isNull();
  }

  // ── notifyLocalDetectorDirect ──

  @Test
  void notifyLocalDetectorDirect_shouldDelegateToAppDetector() {
    zeta.notifyLocalDetectorDirect("k", 5);
    verify(appDetector).addDirect("k", 5);
  }

  @Test
  void notifyLocalDetectorDirect_map_shouldDelegateToAppDetector() {
    Map<String, Long> map = Map.of("k", 3L);
    zeta.notifyLocalDetectorDirect(map);
    verify(appDetector).addDirect(map);
  }

  // ── notifyLocalDetector(String, long) and Map ──

  @Test
  void notifyLocalDetector_withDelta_shouldDelegateToAppDetector() {
    zeta.notifyLocalDetector("k", 7);
    verify(appDetector).add("k", 7);
  }

  @Test
  void notifyLocalDetector_map_shouldDelegateToAppDetector() {
    Map<String, Long> map = Map.of("k", 3L);
    zeta.notifyLocalDetector(map);
    verify(appDetector).add(map);
  }

  // ── returnLocalTopNHotKeys ──

  @Test
  void returnLocalTopNHotKeys_shouldDelegateToAppDetector() {
    List<Item> items = List.of(new Item("k", 10));
    when(appDetector.listTopN(3)).thenReturn(items);
    assertThat(zeta.returnLocalTopNHotKeys(3)).isSameAs(items);
    verify(appDetector).listTopN(3);
  }

  @Test
  void returnLocalTopNHotKeys_shouldReturnEmptyWhenDetectorNull() {
    Zeta hk = new Zeta(hotKeyCache, null);
    assertThat(hk.returnLocalTopNHotKeys(5)).isEmpty();
  }

  // ── isBlacklisted ──

  @Test
  void isBlacklisted_shouldDelegateToCache() {
    when(hotKeyCache.isBlacklisted("secret")).thenReturn(true);
    assertThat(zeta.isBlacklisted("secret")).isTrue();
    verify(hotKeyCache).isBlacklisted("secret");
  }

  @Test
  void isBlacklisted_shouldReturnFalseWhenCacheNull() {
    Zeta workerOnly = new Zeta(null, null);
    assertThat(workerOnly.isBlacklisted("x")).isFalse();
  }

  // ── isWhitelisted ──

  @Test
  void isWhitelisted_shouldDelegateToCache() {
    when(hotKeyCache.isWhitelisted("health")).thenReturn(true);
    assertThat(zeta.isWhitelisted("health")).isTrue();
    verify(hotKeyCache).isWhitelisted("health");
  }

  @Test
  void isWhitelisted_shouldReturnFalseWhenCacheNull() {
    Zeta workerOnly = new Zeta(null, null);
    assertThat(workerOnly.isWhitelisted("x")).isFalse();
  }

  // ── peek ──

  @Test
  void peek_shouldReturnMapOfPresentValues() {
    when(hotKeyCache.peek("k1")).thenReturn(Optional.of("v1"));
    when(hotKeyCache.peek("k2")).thenReturn(Optional.empty());
    assertThat(zeta.peekAll(List.of("k1", "k2"))).containsEntry("k1", "v1").doesNotContainKey("k2");
  }

  @Test
  void peek_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null);
    assertThatThrownBy(() -> workerOnly.peekAll(List.of("k"))).isInstanceOf(ZetaModeException.class);
  }

  // ── invalidateLocal ──

  @Test
  void invalidateLocal_shouldDelegateToCache() {
    zeta.invalidate("key1", CachePolicy.defaults().withSkipBroadcast(true));
    verify(hotKeyCache).invalidate("key1", false);
  }

  @Test
  void invalidateLocal_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null);
    assertThatThrownBy(() -> workerOnly.invalidate("k", CachePolicy.defaults().withSkipBroadcast(true)))
      .isInstanceOf(ZetaModeException.class);
  }

  // ── areLocalHotKeys ──

  @Test
  void areLocalHotKeys_shouldDelegateToCache() {
    when(hotKeyCache.isHot("k1")).thenReturn(true);
    when(hotKeyCache.isHot("k2")).thenReturn(false);
    assertThat(zeta.areLocalHotKeys(List.of("k1", "k2"))).containsEntry("k1", true).containsEntry("k2", false);
  }

  @Test
  void areLocalHotKeys_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null);
    assertThatThrownBy(() -> workerOnly.areLocalHotKeys(List.of("k"))).isInstanceOf(ZetaModeException.class);
  }

  // ── refresh ──

  @Test
  void refresh_shouldEvictAndPutThrough() {
    zeta.refresh("k1", () -> "v");
    verify(hotKeyCache).invalidate("k1", false);
    verify(hotKeyCache).putThrough(eq("k1"), eq("v"), any(), anyLong(), anyLong(), anyBoolean());
  }

  @Test
  void refresh_withTtl_shouldEvictAndPutThroughWithTtl() {
    zeta.refresh("k1", () -> "v", CachePolicy.of(5000L, 500L));
    verify(hotKeyCache).invalidate("k1", false);
    verify(hotKeyCache).putThrough(eq("k1"), eq("v"), any(), eq(5000L), eq(500L), anyBoolean());
  }

  @Test
  void refresh_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null);
    assertThatThrownBy(() -> workerOnly.refresh("k", () -> "v")).isInstanceOf(ZetaModeException.class);
  }

  // ── refreshAll ──

  @Test
  void refreshAll_shouldRefreshEachKey() {
    zeta.refreshAll(Map.of("k1", (Supplier<?>) () -> "v1", "k2", (Supplier<?>) () -> "v2"));
    verify(hotKeyCache, times(2)).invalidate(anyString(), eq(false));
    verify(hotKeyCache, times(2)).putThrough(anyString(), any(), any(), anyLong(), anyLong(), anyBoolean());
  }

  // ── invalidateAfterPut ──

  @Test
  void invalidateAfterPut_shouldDelegateToCache() {
    zeta.invalidateAfterPut(Map.of("k1", () -> {}, "k2", () -> {}));
    verify(hotKeyCache).invalidateAfterPut(anyMap(), anyBoolean());
  }

  @Test
  void invalidateAfterPut_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null);
    assertThatThrownBy(() -> workerOnly.invalidateAfterPut(Map.of("k", () -> {}))).isInstanceOf(
      ZetaModeException.class
    );
  }

  // ── addBlacklist(Collection) / removeBlacklist(Collection) ──

  @Test
  void addBlacklist_collection_shouldDelegateToCache() {
    zeta.addBlacklist(List.of("a", "b"));
    verify(hotKeyCache).addBlacklist("a");
    verify(hotKeyCache).addBlacklist("b");
  }

  @Test
  void addBlacklist_collection_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null);
    assertThatThrownBy(() -> workerOnly.addBlacklist(List.of("x"))).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void removeBlacklist_collection_shouldDelegateToCache() {
    zeta.removeBlacklist(List.of("a", "b"));
    verify(hotKeyCache).unBlacklist("a");
    verify(hotKeyCache).unBlacklist("b");
  }

  @Test
  void removeBlacklist_collection_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null);
    assertThatThrownBy(() -> workerOnly.removeBlacklist(List.of("x"))).isInstanceOf(ZetaModeException.class);
  }

  // ── addWhitelist(Collection) / removeWhitelist(Collection) ──

  @Test
  void addWhitelist_collection_shouldDelegateToCache() {
    zeta.addWhitelist(List.of("a", "b"));
    verify(hotKeyCache).addWhitelist("a");
    verify(hotKeyCache).addWhitelist("b");
  }

  @Test
  void addWhitelist_collection_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null);
    assertThatThrownBy(() -> workerOnly.addWhitelist(List.of("x"))).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void removeWhitelist_collection_shouldDelegateToCache() {
    zeta.removeWhitelist(List.of("a", "b"));
    verify(hotKeyCache).unWhitelist("a");
    verify(hotKeyCache).unWhitelist("b");
  }

  @Test
  void removeWhitelist_collection_shouldThrowInWorkerMode() {
    Zeta workerOnly = new Zeta(null, null);
    assertThatThrownBy(() -> workerOnly.removeWhitelist(List.of("x"))).isInstanceOf(ZetaModeException.class);
  }

  // ── evaluateRules(Collection) ──

  @Test
  void evaluateRules_shouldReturnMap() {
    when(hotKeyCache.evaluateRule("k1")).thenReturn(RuleAction.BLOCK);
    when(hotKeyCache.evaluateRule("k2")).thenReturn(RuleAction.ALLOW);
    assertThat(zeta.evaluateRules(List.of("k1", "k2")))
      .containsEntry("k1", RuleAction.BLOCK)
      .containsEntry("k2", RuleAction.ALLOW);
  }

  @Test
  void evaluateRules_whenCacheNull_shouldReturnAllAllow() {
    Zeta workerOnly = new Zeta(null, null);
    assertThat(workerOnly.evaluateRules(List.of("k1", "k2")))
      .containsEntry("k1", RuleAction.ALLOW)
      .containsEntry("k2", RuleAction.ALLOW);
  }

  // ── isBlacklisted(Collection) / isWhitelisted(Collection) ──

  @Test
  void isBlacklisted_collection_shouldReturnMap() {
    when(hotKeyCache.isBlacklisted("k1")).thenReturn(true);
    when(hotKeyCache.isBlacklisted("k2")).thenReturn(false);
    assertThat(zeta.isBlacklisted(List.of("k1", "k2"))).containsEntry("k1", true).containsEntry("k2", false);
  }

  @Test
  void isBlacklisted_collection_whenCacheNull_shouldReturnAllFalse() {
    Zeta workerOnly = new Zeta(null, null);
    assertThat(workerOnly.isBlacklisted(List.of("k1"))).containsEntry("k1", false);
  }

  @Test
  void isWhitelisted_collection_shouldReturnMap() {
    when(hotKeyCache.isWhitelisted("k1")).thenReturn(true);
    when(hotKeyCache.isWhitelisted("k2")).thenReturn(false);
    assertThat(zeta.isWhitelisted(List.of("k1", "k2"))).containsEntry("k1", true).containsEntry("k2", false);
  }

  @Test
  void isWhitelisted_collection_whenCacheNull_shouldReturnAllFalse() {
    Zeta workerOnly = new Zeta(null, null);
    assertThat(workerOnly.isWhitelisted(List.of("k1"))).containsEntry("k1", false);
  }

  // ── registerRefresh / unregisterRefresh ────────────────────────

  @Test
  void unregisterRefresh_shouldNotThrow() throws InterruptedException {
    CountDownLatch firstCallLatch = new CountDownLatch(1);
    when(hotKeyCache.getWithSoftExpire(eq("cancel-key"), any(CachePolicy.class))).thenAnswer(
      invocation -> {
        firstCallLatch.countDown();
        return Optional.of("v");
      }
    );

    zeta.registerRefresh("cancel-key", () -> "v", CachePolicy.of(300_000L, 10L));
    assertThat(firstCallLatch.await(5, TimeUnit.SECONDS)).as("first scheduled refresh occurred").isTrue();

    zeta.unregisterRefresh("cancel-key");

    verify(hotKeyCache, atLeastOnce()).getWithSoftExpire(eq("cancel-key"), any(CachePolicy.class));
  }

  @Test
  void registerRefresh_replacesExistingRegistration() throws InterruptedException {
    AtomicInteger callCount = new AtomicInteger(0);
    when(hotKeyCache.getWithSoftExpire(anyString(), any(CachePolicy.class))).thenAnswer(
      invocation -> {
        callCount.incrementAndGet();
        return Optional.of("v");
      }
    );

    zeta.registerRefresh("dup-key", () -> "v1", CachePolicy.of(300_000L, 5L));
    Thread.sleep(20);
    zeta.registerRefresh("dup-key", () -> "v2", CachePolicy.of(300_000L, 5L));

    int countBefore = callCount.get();
    Thread.sleep(30);
    int countAfter = callCount.get();

    assertThat(countAfter).isGreaterThan(countBefore);
  }

  @Test
  void registerRefresh_invokesGetWithSoftExpire() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    when(hotKeyCache.getWithSoftExpire(eq("k1"), any(CachePolicy.class))).thenAnswer(invocation -> {
      latch.countDown();
      return Optional.of("v");
    });

    zeta.registerRefresh("k1", () -> "v", CachePolicy.of(300_000L, 5L));
    assertThat(latch.await(5, TimeUnit.SECONDS)).as("getWithSoftExpire was invoked by scheduled refresh").isTrue();

    zeta.unregisterRefresh("k1");
  }

  /**
   * The replace-and-cancel race: two concurrent registrations for the same key
   * must leave exactly one ALIVE future registered. With the former
   * put-then-cancel sequence, T1 and T2 can each cancel the OTHER's new future
   * (T1's prev read is F2, T2's is F1), silently killing the timed refresh.
   */
  @Test
  void registerRefresh_concurrentRegistrations_leaveExactlyOneAliveFuture() throws Exception {
    int threads = 8;
    int iterations = 25;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<?>> jobs = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      jobs.add(pool.submit(() -> {
        start.await();
        for (int i = 0; i < iterations; i++) {
          zeta.registerRefresh("race-key", () -> "v", CachePolicy.of(300_000L, 5_000L));
        }
        return null;
      }));
    }
    start.countDown();
    for (Future<?> job : jobs) {
      job.get(30, TimeUnit.SECONDS);
    }
    pool.shutdown();

    java.lang.reflect.Field field = Zeta.class.getDeclaredField("refreshFutures");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.ScheduledFuture<?>> futures =
      (java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.ScheduledFuture<?>>)
        field.get(zeta);

    Object surviving = futures.get("race-key");
    assertThat(surviving).as("a registration must survive the race").isNotNull();
    assertThat(futures.get("race-key").isCancelled())
      .as("the surviving future must not be cancelled")
      .isFalse();

    zeta.unregisterRefresh("race-key");
  }

  @Test
  void destroy_shouldNotThrow() {
    zeta.registerRefresh("k1", () -> "v", CachePolicy.of(300_000L, 10_000L));
    zeta.destroy();
    verify(hotKeyCache, never()).getWithSoftExpire(eq("k1"), any(CachePolicy.class));
  }

  // ── Parameter validation ──

  @Test
  void read_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.read(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void read_shouldRejectEmptyKey() {
    assertThatThrownBy(() -> zeta.read("")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void write_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.write(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void getWithSoftExpire_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.getWithSoftExpire(null, () -> "v")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void batchGet_shouldRejectNullKeys() {
    assertThatThrownBy(() -> zeta.getAll((Iterable<String>) null, k -> "v")).isInstanceOf(NullPointerException.class);
  }

  @Test
  void putThrough_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.putThrough(null, "v", () -> {})).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void putThrough_shouldRejectNullValue() {
    assertThatThrownBy(() -> zeta.putThrough("k", null, () -> {})).isInstanceOf(NullPointerException.class);
  }

  @Test
  void putThrough_shouldRejectNullWriter() {
    assertThatThrownBy(() -> zeta.putThrough("k", "v", null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void putLocal_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.putLocal(null, "v")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void putLocal_shouldRejectNullValue() {
    assertThatThrownBy(() -> zeta.putLocal("k", null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void invalidate_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.invalidate((String) null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void invalidate_shouldRejectEmptyKey() {
    assertThatThrownBy(() -> zeta.invalidate("")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void batchInvalidate_shouldRejectNullKeys() {
    assertThatThrownBy(() -> zeta.invalidate((Collection<String>) null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void invalidateAfterPut_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.invalidateAfterPut(null, () -> {})).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void invalidateAfterPut_shouldRejectNullMutation() {
    assertThatThrownBy(() -> zeta.invalidateAfterPut("k", null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void peek_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.peek(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void peekAll_shouldRejectNullKeys() {
    assertThatThrownBy(() -> zeta.peekAll(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void compareAndSet_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.compareAndSet(null, "old", "new")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void compareAndInvalidate_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.compareAndInvalidate(null, "old")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void tryLock_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.tryLock(null, 10, TimeUnit.SECONDS)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void tryLock_shouldRejectNullUnit() {
    assertThatThrownBy(() -> zeta.tryLock("k", 10, null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void tryLock_shouldRejectNegativeExpire() {
    assertThatThrownBy(() -> zeta.tryLock("k", -1, TimeUnit.SECONDS)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void isLocalHotKey_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.isLocalHotKey(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void registerRefresh_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.registerRefresh(null, () -> "v", CachePolicy.of(1000L, 100L))).isInstanceOf(
      IllegalArgumentException.class
    );
  }

  @Test
  void registerRefresh_shouldRejectNullSupplier() {
    assertThatThrownBy(() -> zeta.registerRefresh("k", null, CachePolicy.of(1000L, 100L)))
      .isInstanceOf(NullPointerException.class);
  }

  @Test
  void registerRefresh_intervalIsSoftTtlTimesOnePointOne() {
    // The documented cadence contract: interval = resolved soft TTL × 1.1 so
    // the entry is stale at every tick even under the ±5% TTL jitter.
    assertThat(Zeta.refreshIntervalMs(100L)).isEqualTo(110L);
    assertThat(Zeta.refreshIntervalMs(1_000L)).isEqualTo(1_100L);
    assertThat(Zeta.refreshIntervalMs(5_000L)).isEqualTo(5_500L);
    // Rounding: 7ms × 1.1 = 7.7 → ceil to 8 so the interval never undershoots.
    assertThat(Zeta.refreshIntervalMs(7L)).isEqualTo(8L);
    // Clamped to at least 1ms for degenerate TTLs.
    assertThat(Zeta.refreshIntervalMs(0L)).isEqualTo(1L);
    assertThat(Zeta.refreshIntervalMs(1L)).isEqualTo(2L);
  }

  @Test
  void unregisterRefresh_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.unregisterRefresh(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void refresh_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.refresh(null, () -> "v")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void refresh_shouldRejectNullLoader() {
    assertThatThrownBy(() -> zeta.refresh("k", null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void addBlacklist_shouldRejectNullPattern() {
    assertThatThrownBy(() -> zeta.addBlacklist((String) null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void addBlacklist_shouldRejectEmptyPattern() {
    assertThatThrownBy(() -> zeta.addBlacklist("")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void evaluateRule_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.evaluateRule(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void isBlacklisted_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.isBlacklisted((String) null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void isWhitelisted_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.isWhitelisted((String) null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void areLocalHotKeys_shouldRejectNullKeys() {
    assertThatThrownBy(() -> zeta.areLocalHotKeys(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void workerMode_shouldStillRejectNullKey() {
    Zeta workerOnly = new Zeta(null, null);
    assertThatThrownBy(() -> workerOnly.get(null, () -> "v")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void putLocal_shouldRejectNegativeHardTtl() {
    assertThatThrownBy(() -> zeta.putLocal("k", "v", CachePolicy.of(-1L, 0L)))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void putThrough_shouldRejectNegativeHardTtl() {
    assertThatThrownBy(() -> zeta.putThrough("k", "v", () -> {}, CachePolicy.of(-1L, 0L))).isInstanceOf(
      IllegalArgumentException.class
    );
  }
}
