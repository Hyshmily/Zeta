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
import io.github.hyshmily.zeta.scheduler.TimedRefreshCoordinator;
import io.github.hyshmily.zeta.cache.loader.ZetaLoaderRegistry;
import io.github.hyshmily.zeta.cache.loader.ZetaLoadingSpec;
import io.github.hyshmily.zeta.exception.ZetaBlockedException;
import io.github.hyshmily.zeta.exception.ZetaModeException;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.hotkeydetector.heavykeeper.Item;
import io.github.hyshmily.zeta.model.InvalidatePolicy;
import io.github.hyshmily.zeta.model.ReadPolicy;
import io.github.hyshmily.zeta.model.WritePolicy;
import io.github.hyshmily.zeta.model.HotKey;
import io.github.hyshmily.zeta.model.StalePolicy;
import io.github.hyshmily.zeta.model.ZetaCacheStats;
import io.github.hyshmily.zeta.rule.Rule;
import io.github.hyshmily.zeta.rule.RuleService;
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
  private RuleService ruleService;
  private Zeta zeta;

  @BeforeEach
  @SuppressWarnings("all")
  void setUp() {
    hotKeyCache = mock(HotKeyCache.class);
    appDetector = mock(HotKeyDetector.class);
    ruleService = mock(RuleService.class);
    // Mirror the real TtlPolicy behaviour (softTtlMs > 0 is used as-is). Without this stub the
    // mock default 0 would collapse every refresh interval to 1ms, turning destroy_shouldNotThrow
    // into a race between the scheduled refresh and the cancellation.
    when(hotKeyCache.resolveEffectiveSoftTtl(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));
    zeta = new DefaultZeta(hotKeyCache, appDetector, null, null, ruleService);
  }

  @Test
  void isLocalZeta_shouldDelegateToCache() {
    when(hotKeyCache.isHot("key1")).thenReturn(true);
    assertThat(zeta.detector().isLocalHotKey("key1")).isTrue();
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
    when(hotKeyCache.get(anyString(), any(ReadPolicy.class))).thenReturn(Optional.of("value"));
    assertThat(zeta.get("key1", () -> "loaded")).contains("value");
    verify(hotKeyCache).get(anyString(), any(ReadPolicy.class));
  }

  @Test
  void get_withTtl_shouldDelegateToCache() {
    when(hotKeyCache.get(anyString(), any(ReadPolicy.class))).thenReturn(Optional.of("v"));
    assertThat(zeta.get("key1", ReadPolicy.of(() -> "loaded").withHardTtl(1000L).withSoftTtl(100L))).contains("v");
    verify(hotKeyCache).get(anyString(), any(ReadPolicy.class));
  }

  @Test
  void getWithSoftExpire_shouldDelegateToCache() {
    when(hotKeyCache.getWithSoftExpire(anyString(), any(ReadPolicy.class))).thenReturn(
      Optional.of("v")
    );
    assertThat(zeta.getWithSoftExpire("key1", () -> "v")).contains("v");
    verify(hotKeyCache).getWithSoftExpire(anyString(), any(ReadPolicy.class));
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
    Zeta withRegistry = new DefaultZeta(hotKeyCache, appDetector, null, registry, ruleService);
    when(hotKeyCache.get(anyString(), any(ReadPolicy.class))).thenReturn(Optional.of("loaded:user:42"));

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
    Zeta withRegistry = new DefaultZeta(hotKeyCache, appDetector, null, registry, ruleService);
    when(hotKeyCache.getWithSoftExpire(anyString(), any(ReadPolicy.class))).thenReturn(Optional.of("v"));

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
    Zeta withRegistry = new DefaultZeta(hotKeyCache, appDetector, null, registry, ruleService);
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
    Zeta withRegistry = new DefaultZeta(hotKeyCache, appDetector, null, registry, ruleService);

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

    Map<String, String> result = withRegistry.getAll(List.of("user:42", "order:7", "user:43"));

    assertThat(result)
      .containsEntry("user:42", "loaded:user:42")
      .containsEntry("order:7", "O:order:7")
      .containsEntry("user:43", "loaded:user:43")
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
    Zeta withRegistry = new DefaultZeta(hotKeyCache, appDetector, null, registry, ruleService);

    assertThatThrownBy(() -> withRegistry.getAll(List.of("user:42", "vendor:9")))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("No CacheLoader registered for key 'vendor:9'");
    verify(hotKeyCache, never())
      .get(any(Iterable.class), any(Function.class), anyLong(), anyLong(), anyBoolean(), anyBoolean());
  }

  @Test
  @SuppressWarnings("all")
  void getBatch_nullAndMissOmittedFromMap() {
    when(hotKeyCache.get(any(Iterable.class), any(Function.class), anyLong(), anyLong(), anyBoolean(), anyBoolean()))
      .thenAnswer(inv -> {
        Map<String, Optional<String>> out = new LinkedHashMap<>();
        out.put("a", Optional.of("va"));
        out.put("b", Optional.empty());
        return out;
      });

    Map<String, String> result = zeta.getAll(List.of("a", "b"), k -> "v");

    // Absent semantics: a miss (or cached null) omits the key instead of
    // surfacing an empty Optional value.
    assertThat(result).containsExactly(Map.entry("a", "va"));
  }

  @Test
  @SuppressWarnings("all")
  void getWithSoftExpireNoReaderBatch_shouldRoutePerSpec() {
    ZetaLoaderRegistry registry = new ZetaLoaderRegistry();
    registry.register("user:", ZetaLoadingSpec.<String>builder().loader(key -> "U:" + key).hardTtl(5_000).build());
    Zeta withRegistry = new DefaultZeta(hotKeyCache, appDetector, null, registry, ruleService);

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

    Map<String, String> result = withRegistry.getAllWithSoftExpire(List.of("user:1", "user:2"));

    assertThat(result)
      .containsEntry("user:1", "U:user:1")
      .containsEntry("user:2", "U:user:2");
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
    Zeta withRegistry = new DefaultZeta(hotKeyCache, appDetector, null, registry, ruleService);

    when(hotKeyCache.computeIfAbsent(anyString(), any(ReadPolicy.class))).thenAnswer(inv -> {
      ReadPolicy policy = inv.getArgument(1);
      return Optional.ofNullable(policy.reader().get());
    });

    String value = withRegistry.computeIfAbsent("user:42");
    assertThat(value).isEqualTo("loaded:user:42");

    ArgumentCaptor<ReadPolicy> policyCaptor = ArgumentCaptor.forClass(ReadPolicy.class);
    verify(hotKeyCache).computeIfAbsent(eq("user:42"), policyCaptor.capture());
    ReadPolicy policy = policyCaptor.getValue();
    assertThat(policy.hardTtlMs().getAsLong()).isEqualTo(5_000);
    assertThat(policy.softTtlMs().getAsLong()).isEqualTo(500);
  }

  @Test
  @SuppressWarnings("all")
  void computeIfAbsentWithSoftExpireNoReader_shouldForceSoftRefresh() {
    ZetaLoaderRegistry registry = new ZetaLoaderRegistry();
    registry.register("user:", ZetaLoadingSpec.<String>builder().loader(key -> "U:" + key).softTtl(500).build());
    Zeta withRegistry = new DefaultZeta(hotKeyCache, appDetector, null, registry, ruleService);

    when(hotKeyCache.computeIfAbsentWithSoftExpire(anyString(), any(ReadPolicy.class))).thenAnswer(inv -> {
      ReadPolicy policy = inv.getArgument(1);
      return Optional.ofNullable(policy.reader().get());
    });

    String value = withRegistry.computeIfAbsentWithSoftExpire("user:42");
    assertThat(value).isEqualTo("U:user:42");

    ArgumentCaptor<ReadPolicy> policyCaptor = ArgumentCaptor.forClass(ReadPolicy.class);
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
    zeta.putThrough("key1", "value", () -> {}, WritePolicy.of(2000L, 200L));
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
    assertThat(zeta.detector().localTopKeys()).containsExactly(new HotKey("k1", 10));
  }

  @Test
  void returnHotKeys_shouldReturnLocalEmptyWhenTopKNull() {
    Zeta hk = new DefaultZeta(hotKeyCache, null, null, null, null);
    assertThat(hk.detector().localTopKeys()).isEmpty();
  }

  @Test
  void returnTotalDataStreams_shouldReturnLocalFromTopK() {
    when(appDetector.total()).thenReturn(100L);
    assertThat(zeta.detector().returnLocalTotalDataStreams()).isEqualTo(100L);
  }

  @Test
  void returnTotalDataStreams_shouldReturnLocalZeroWhenTopKNull() {
    assertThat(new DefaultZeta(hotKeyCache, null, null, null, null).detector().returnLocalTotalDataStreams()).isZero();
  }

  @Test
  void returnExpelledHotKeys_shouldReturnLocalFromTopK() {
    LinkedBlockingQueue<Item> queue = new LinkedBlockingQueue<>();
    queue.add(new Item("k1", 5));
    when(appDetector.expelled()).thenReturn(queue);
    assertThat(zeta.detector().returnLocalExpelledHotKeys()).hasSize(1);
  }

  @Test
  void cacheMethods_shouldThrowInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThatThrownBy(() -> workerOnly.get("k", () -> "v")).isInstanceOf(ZetaModeException.class);
    assertThatThrownBy(() -> workerOnly.detector().isLocalHotKey("k")).isInstanceOf(ZetaModeException.class);
    assertThatThrownBy(() -> workerOnly.peek("k")).isInstanceOf(ZetaModeException.class);
    assertThatThrownBy(() -> workerOnly.invalidate("k")).isInstanceOf(ZetaModeException.class);
    assertThatThrownBy(() -> workerOnly.putThrough("k", "v", () -> {})).isInstanceOf(ZetaModeException.class);
    assertThatThrownBy(() -> workerOnly.invalidateAfterPut("k", () -> {})).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void get_shouldPropagateZetaBlockedException() {
    when(hotKeyCache.get(anyString(), any(ReadPolicy.class))).thenThrow(
      new ZetaBlockedException("HotKeyCache", "secret")
    );
    assertThatThrownBy(() -> zeta.get("secret", () -> "v")).isInstanceOf(ZetaBlockedException.class);
  }

  // ── Additional getWithSoftExpire overloads ──

  @Test
  void getWithSoftExpire_withSoftTtl_shouldDelegateToCache() {
    when(hotKeyCache.getWithSoftExpire(anyString(), any(ReadPolicy.class))).thenReturn(
      Optional.of("v")
    );
    assertThat(zeta.getWithSoftExpire("key1", ReadPolicy.of(() -> "v").withSoftTtl(200L))).contains("v");
    verify(hotKeyCache).getWithSoftExpire(anyString(), any(ReadPolicy.class));
  }

  // ── returnLocalExpelledHotKeys null guard ──

  @Test
  void returnExpelledHotKeys_shouldReturnEmptyQueueWhenTopKNull() {
    Zeta hk = new DefaultZeta(hotKeyCache, null, null, null, null);
    assertThat(hk.detector().returnLocalExpelledHotKeys()).isEmpty();
  }

  // ── returnLocalTotalDataStreams null guard (2-arg ctor) ──

  @Test
  void returnTotalDataStreams_shouldReturnLocalZeroWhenTopKNullTwoArg() {
    Zeta hk = new DefaultZeta(hotKeyCache, null, null, null, null);
    assertThat(hk.detector().returnLocalTotalDataStreams()).isZero();
  }

  // ── snapshotValues / localKeysWithPrefix ──

  @Test
  void snapshotValues_shouldDelegateToCache() {
    when(hotKeyCache.snapshotValues(100)).thenReturn(Map.of("k", (Object) "v"));
    assertThat(zeta.stats().snapshotValues(100)).isEqualTo(Map.of("k", "v"));
    verify(hotKeyCache).snapshotValues(100);
  }

  @Test
  void localKeysWithPrefix_shouldDelegateToCache() {
    when(hotKeyCache.localKeysWithPrefix("a::")).thenReturn(List.of("a::1"));
    assertThat(zeta.stats().localKeysWithPrefix("a::")).containsExactly("a::1");
    verify(hotKeyCache).localKeysWithPrefix("a::");
  }

  @Test
  void statsViews_shouldAnswerEmptyInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThat(workerOnly.stats().snapshotValues(100)).isEmpty();
    assertThat(workerOnly.stats().localKeysWithPrefix("a::")).isEmpty();
  }

  // ── addBlacklist / removeBlacklist / addWhitelist / removeWhitelist ──

  @Test
  void addBlacklist_shouldDelegateToRuleService() {
    zeta.rules().addBlacklist("secret-*");
    verify(ruleService).addBlacklist("secret-*");
  }

  @Test
  void addBlacklist_shouldThrowInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThatThrownBy(() -> workerOnly.rules().addBlacklist("x")).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void removeBlacklist_shouldDelegateToCache() {
    zeta.rules().removeBlacklist("old-rule");
    verify(ruleService).removeBlacklist("old-rule");
  }

  @Test
  void removeBlacklist_shouldThrowInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThatThrownBy(() -> workerOnly.rules().removeBlacklist("x")).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void addWhitelist_shouldDelegateToRuleService() {
    zeta.rules().addWhitelist("health-*");
    verify(ruleService).addWhitelist("health-*");
  }

  @Test
  void addWhitelist_shouldThrowInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThatThrownBy(() -> workerOnly.rules().addWhitelist("x")).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void removeWhitelist_shouldDelegateToCache() {
    zeta.rules().removeWhitelist("health-*");
    verify(ruleService).removeWhitelist("health-*");
  }

  @Test
  void removeWhitelist_shouldThrowInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThatThrownBy(() -> workerOnly.rules().removeWhitelist("x")).isInstanceOf(ZetaModeException.class);
  }

  // ── getAllRules ──

  @Test
  void getAllRules_shouldDelegateToRuleService() {
    Rule rule = new Rule(Rule.RuleType.EXACT, "blocked", RuleAction.BLOCK);
    when(ruleService.getAllRules()).thenReturn(List.of(rule));
    assertThat(zeta.rules().getAllRules()).containsExactly(rule);
    verify(ruleService).getAllRules();
  }

  @Test
  void getAllRules_shouldReturnEmptyWhenCacheNull() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThat(workerOnly.rules().getAllRules()).isEmpty();
  }

  // ── evaluateRule ──

  @Test
  void evaluateRule_shouldDelegateToRuleService() {
    when(ruleService.evaluateRule("secret")).thenReturn(RuleAction.BLOCK);
    assertThat(zeta.rules().evaluateRule("secret")).isEqualTo(RuleAction.BLOCK);
    verify(ruleService).evaluateRule("secret");
  }

  @Test
  void evaluateRule_shouldReturnAllowWhenCacheNull() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThat(workerOnly.rules().evaluateRule("any")).isEqualTo(RuleAction.ALLOW);
  }

  // ── clearAllRules ──

  @Test
  void clearAllRules_shouldDelegateToRuleService() {
    zeta.rules().clearAllRules();
    verify(ruleService).clearAllRules();
  }

  @Test
  void clearAllRules_shouldThrowInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThatThrownBy(() -> workerOnly.rules().clearAllRules()).isInstanceOf(ZetaModeException.class);
  }

  // ── broadcastAllLocalRulesManually ──

  @Test
  void broadcastAllLocalRulesManually_shouldDelegateToRuleService() {
    zeta.rules().broadcastAllLocalRulesManually();
    verify(ruleService).broadcastAllLocalRulesManually();
  }

  @Test
  void broadcastAllLocalRulesManually_shouldThrowInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThatThrownBy(() -> workerOnly.rules().broadcastAllLocalRulesManually()).isInstanceOf(ZetaModeException.class);
  }

  // ── notifyLocalDetector ──

  @Test
  void notifyLocalDetector_shouldDelegateToTopK() {
    zeta.detector().notifyLocalDetector("my-key");
    verify(appDetector).add("my-key");
  }

  @Test
  void notifyLocalDetector_shouldIgnoreNullKey() {
    zeta.detector().notifyLocalDetector((String) null);
    verify(appDetector, never()).add(anyString());
  }

  // ── read / write factories ──

  @Test
  void read_shouldReturnZetaReadQuery() {
    when(ruleService.evaluateRule("k")).thenReturn(RuleAction.ALLOW);
    when(hotKeyCache.get(anyString(), any(ReadPolicy.class))).thenReturn(Optional.of("v"));
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
    when(hotKeyCache.computeIfAbsent(anyString(), any(ReadPolicy.class))).thenReturn(
      Optional.of("v")
    );
    assertThat(zeta.computeIfAbsent("k", () -> "loaded")).isEqualTo("v");
    verify(hotKeyCache).computeIfAbsent(eq("k"), any(ReadPolicy.class));
  }

  @Test
  void computeIfAbsent_shouldReturnNullWhenLoaderNull() {
    when(hotKeyCache.computeIfAbsent(anyString(), any(ReadPolicy.class))).thenReturn(
      Optional.empty()
    );
    assertThat((Object) zeta.computeIfAbsent("k", () -> null)).isNull();
  }

  @Test
  void computeIfAbsent_withHardTtl_shouldDelegate() {
    when(hotKeyCache.computeIfAbsent(anyString(), any(ReadPolicy.class))).thenReturn(
      Optional.of("v")
    );
    assertThat(zeta.computeIfAbsentOptional("k", ReadPolicy.of(() -> "db").withHardTtl(5000L))).contains("v");
    verify(hotKeyCache).computeIfAbsent(eq("k"), any(ReadPolicy.class));
  }

  @Test
  void computeIfAbsent_withHardSoftTtl_shouldDelegate() {
    when(hotKeyCache.computeIfAbsent(anyString(), any(ReadPolicy.class))).thenReturn(
      Optional.of("v")
    );
    assertThat(
        zeta.computeIfAbsentOptional("k", ReadPolicy.of(() -> "db", 5000L, 500L, true, true, StalePolicy.SOFT_REFRESH)))
      .contains("v");
    verify(hotKeyCache).computeIfAbsent(eq("k"), any(ReadPolicy.class));
  }

  @Test
  void computeIfAbsent_withNonDefaultReport_shouldDelegate() {
    when(hotKeyCache.computeIfAbsent(anyString(), any(ReadPolicy.class))).thenReturn(
      Optional.of("v")
    );
    assertThat(
        zeta.computeIfAbsentOptional("k", ReadPolicy.of(() -> "db", 0L, 0L, true, false, StalePolicy.SOFT_REFRESH)))
      .contains("v");
    verify(hotKeyCache).computeIfAbsent(eq("k"), any(ReadPolicy.class));
  }

  @Test
  void computeIfAbsentWithSoftExpire_SoftTtl_shouldDelegate() {
    when(hotKeyCache.computeIfAbsentWithSoftExpire(anyString(), any(ReadPolicy.class))).thenReturn(
      Optional.of("v")
    );
    assertThat(zeta.computeIfAbsentWithSoftExpireOptional("k", ReadPolicy.of(() -> "db").withSoftTtl(500L))).contains("v");
    verify(hotKeyCache).computeIfAbsentWithSoftExpire(eq("k"), any(ReadPolicy.class));
  }

  @Test
  void computeIfAbsentWithSoftExpire_collectionSoftTtl_shouldDelegate() {
    when(hotKeyCache.computeIfAbsentWithSoftExpire(anyString(), any(ReadPolicy.class))).thenReturn(
      Optional.of("v")
    );
    assertThat(zeta.computeIfAbsentWithSoftExpireOptional("k", ReadPolicy.of(() -> "db").withSoftTtl(500L))).contains("v");
    verify(hotKeyCache).computeIfAbsentWithSoftExpire(eq("k"), any(ReadPolicy.class));
  }

  @Test
  void computeIfAbsentWithSoftExpire_BothTtls_shouldDelegate() {
    when(hotKeyCache.computeIfAbsentWithSoftExpire(anyString(), any(ReadPolicy.class))).thenReturn(
      Optional.of("v")
    );
    assertThat(
        zeta.computeIfAbsentWithSoftExpireOptional("k", ReadPolicy.of(() -> "db").withHardTtl(5000L).withSoftTtl(500L)))
      .contains("v");
    verify(hotKeyCache).computeIfAbsentWithSoftExpire(eq("k"), any(ReadPolicy.class));
  }

  @Test
  void computeIfAbsentWithSoftExpire_BothTtlsAndReport_shouldDelegate() {
    when(hotKeyCache.computeIfAbsentWithSoftExpire(anyString(), any(ReadPolicy.class))).thenReturn(
      Optional.of("v")
    );
    assertThat(zeta.computeIfAbsentWithSoftExpireOptional(
        "k", ReadPolicy.of(() -> "db", 5000L, 500L, true, true, StalePolicy.SOFT_REFRESH)))
      .contains("v");
    verify(hotKeyCache).computeIfAbsentWithSoftExpire(eq("k"), any(ReadPolicy.class));
  }

  // ── invalidateAllLocal no-arg ──

  @Test
  void invalidateAllLocal_shouldDelegateToCache() {
    zeta.invalidateAllLocal();
    verify(hotKeyCache).invalidateAllLocal();
  }

  @Test
  void invalidateAllLocal_shouldThrowInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
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
    zeta.putLocal("k", "v", WritePolicy.of(5000L, 500L));
    verify(hotKeyCache).putLocal("k", "v", 5000L, 500L);
  }

  // ── estimatedSizeOfKeysCount ──

  @Test
  void estimatedSize_shouldDelegateToCache() {
    when(hotKeyCache.estimatedSize()).thenReturn(42L);
    assertThat(zeta.stats().estimatedSize()).isEqualTo(42L);
    verify(hotKeyCache).estimatedSize();
  }

  @Test
  void estimatedSize_shouldReturnZeroInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThat(workerOnly.stats().estimatedSize()).isZero();
  }

  // ── stats ──

  @Test
  void stats_shouldDelegateToCache() {
    ZetaCacheStats stats = mock(ZetaCacheStats.class);
    when(hotKeyCache.stats()).thenReturn(stats);
    assertThat(zeta.stats().snapshot()).isSameAs(stats);
    verify(hotKeyCache).stats();
  }

  @Test
  void stats_shouldReturnNullInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThat(workerOnly.stats().snapshot()).isNull();
  }

  // ── notifyLocalDetectorDirect ──

  @Test
  void notifyLocalDetectorDirect_shouldDelegateToAppDetector() {
    zeta.detector().notifyLocalDetectorDirect("k", 5);
    verify(appDetector).addDirect("k", 5);
  }

  @Test
  void notifyLocalDetectorDirect_map_shouldDelegateToAppDetector() {
    Map<String, Long> map = Map.of("k", 3L);
    zeta.detector().notifyLocalDetectorDirect(map);
    verify(appDetector).addDirect(map);
  }

  // ── notifyLocalDetector(String, long) and Map ──

  @Test
  void notifyLocalDetector_withDelta_shouldDelegateToAppDetector() {
    zeta.detector().notifyLocalDetector("k", 7);
    verify(appDetector).add("k", 7);
  }

  @Test
  void notifyLocalDetector_map_shouldDelegateToAppDetector() {
    Map<String, Long> map = Map.of("k", 3L);
    zeta.detector().notifyLocalDetector(map);
    verify(appDetector).add(map);
  }

  // ── localTopKeys(int) via the detector view ──

  @Test
  void returnLocalTopNHotKeys_shouldDelegateToAppDetector() {
    List<Item> items = List.of(new Item("k", 10));
    when(appDetector.listTopN(3)).thenReturn(items);
    assertThat(zeta.detector().localTopKeys(3)).containsExactly(new HotKey("k", 10));
    verify(appDetector).listTopN(3);
  }

  @Test
  void returnLocalTopNHotKeys_shouldReturnEmptyWhenDetectorNull() {
    Zeta hk = new DefaultZeta(hotKeyCache, null, null, null, null);
    assertThat(hk.detector().localTopKeys(5)).isEmpty();
  }

  // ── isBlacklisted ──

  @Test
  void isBlacklisted_shouldDelegateToRuleService() {
    when(ruleService.isBlacklisted("secret")).thenReturn(true);
    assertThat(zeta.rules().isBlacklisted("secret")).isTrue();
    verify(ruleService).isBlacklisted("secret");
  }

  @Test
  void isBlacklisted_shouldReturnFalseWhenCacheNull() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThat(workerOnly.rules().isBlacklisted("x")).isFalse();
  }

  // ── isWhitelisted ──

  @Test
  void isWhitelisted_shouldDelegateToRuleService() {
    when(ruleService.isWhitelisted("health")).thenReturn(true);
    assertThat(zeta.rules().isWhitelisted("health")).isTrue();
    verify(ruleService).isWhitelisted("health");
  }

  @Test
  void isWhitelisted_shouldReturnFalseWhenCacheNull() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThat(workerOnly.rules().isWhitelisted("x")).isFalse();
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
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThatThrownBy(() -> workerOnly.peekAll(List.of("k"))).isInstanceOf(ZetaModeException.class);
  }

  // ── invalidateLocal ──

  @Test
  void invalidateLocal_shouldDelegateToCache() {
    zeta.invalidate("key1", InvalidatePolicy.of(true));
    verify(hotKeyCache).invalidate("key1", false);
  }

  @Test
  void invalidateLocal_shouldThrowInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThatThrownBy(() -> workerOnly.invalidate("k", InvalidatePolicy.of(true)))
      .isInstanceOf(ZetaModeException.class);
  }

  // ── areLocalHotKeys ──

  @Test
  void areLocalHotKeys_shouldDelegateToCache() {
    when(hotKeyCache.isHot("k1")).thenReturn(true);
    when(hotKeyCache.isHot("k2")).thenReturn(false);
    assertThat(zeta.detector().areLocalHotKeys(List.of("k1", "k2"))).containsEntry("k1", true).containsEntry("k2", false);
  }

  @Test
  void areLocalHotKeys_shouldThrowInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThatThrownBy(() -> workerOnly.detector().areLocalHotKeys(List.of("k"))).isInstanceOf(ZetaModeException.class);
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
    zeta.refresh("k1", () -> "v", WritePolicy.of(5000L, 500L));
    verify(hotKeyCache).invalidate("k1", false);
    verify(hotKeyCache).putThrough(eq("k1"), eq("v"), any(), eq(5000L), eq(500L), anyBoolean());
  }

  @Test
  void refresh_shouldThrowInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
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
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThatThrownBy(() -> workerOnly.invalidateAfterPut(Map.of("k", () -> {}))).isInstanceOf(
      ZetaModeException.class
    );
  }

  // ── addBlacklist(Collection) / removeBlacklist(Collection) ──

  @Test
  void addBlacklist_collection_shouldDelegateToRuleService() {
    zeta.rules().addBlacklist(List.of("a", "b"));
    verify(ruleService).addBlacklist("a");
    verify(ruleService).addBlacklist("b");
  }

  @Test
  void addBlacklist_collection_shouldThrowInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThatThrownBy(() -> workerOnly.rules().addBlacklist(List.of("x"))).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void removeBlacklist_collection_shouldDelegateToCache() {
    zeta.rules().removeBlacklist(List.of("a", "b"));
    verify(ruleService).removeBlacklist("a");
    verify(ruleService).removeBlacklist("b");
  }

  @Test
  void removeBlacklist_collection_shouldThrowInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThatThrownBy(() -> workerOnly.rules().removeBlacklist(List.of("x"))).isInstanceOf(ZetaModeException.class);
  }

  // ── addWhitelist(Collection) / removeWhitelist(Collection) ──

  @Test
  void addWhitelist_collection_shouldDelegateToRuleService() {
    zeta.rules().addWhitelist(List.of("a", "b"));
    verify(ruleService).addWhitelist("a");
    verify(ruleService).addWhitelist("b");
  }

  @Test
  void addWhitelist_collection_shouldThrowInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThatThrownBy(() -> workerOnly.rules().addWhitelist(List.of("x"))).isInstanceOf(ZetaModeException.class);
  }

  @Test
  void removeWhitelist_collection_shouldDelegateToCache() {
    zeta.rules().removeWhitelist(List.of("a", "b"));
    verify(ruleService).removeWhitelist("a");
    verify(ruleService).removeWhitelist("b");
  }

  @Test
  void removeWhitelist_collection_shouldThrowInWorkerMode() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThatThrownBy(() -> workerOnly.rules().removeWhitelist(List.of("x"))).isInstanceOf(ZetaModeException.class);
  }

  // ── evaluateRules(Collection) ──

  @Test
  void evaluateRules_shouldReturnMap() {
    when(ruleService.evaluateRule("k1")).thenReturn(RuleAction.BLOCK);
    when(ruleService.evaluateRule("k2")).thenReturn(RuleAction.ALLOW);
    assertThat(zeta.rules().evaluateRules(List.of("k1", "k2")))
      .containsEntry("k1", RuleAction.BLOCK)
      .containsEntry("k2", RuleAction.ALLOW);
  }

  @Test
  void evaluateRules_whenCacheNull_shouldReturnAllAllow() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThat(workerOnly.rules().evaluateRules(List.of("k1", "k2")))
      .containsEntry("k1", RuleAction.ALLOW)
      .containsEntry("k2", RuleAction.ALLOW);
  }

  // ── isBlacklisted(Collection) / isWhitelisted(Collection) ──

  @Test
  void isBlacklisted_collection_shouldReturnMap() {
    when(ruleService.isBlacklisted("k1")).thenReturn(true);
    when(ruleService.isBlacklisted("k2")).thenReturn(false);
    assertThat(zeta.rules().isBlacklisted(List.of("k1", "k2"))).containsEntry("k1", true).containsEntry("k2", false);
  }

  @Test
  void isBlacklisted_collection_whenCacheNull_shouldReturnAllFalse() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThat(workerOnly.rules().isBlacklisted(List.of("k1"))).containsEntry("k1", false);
  }

  @Test
  void isWhitelisted_collection_shouldReturnMap() {
    when(ruleService.isWhitelisted("k1")).thenReturn(true);
    when(ruleService.isWhitelisted("k2")).thenReturn(false);
    assertThat(zeta.rules().isWhitelisted(List.of("k1", "k2"))).containsEntry("k1", true).containsEntry("k2", false);
  }

  @Test
  void isWhitelisted_collection_whenCacheNull_shouldReturnAllFalse() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThat(workerOnly.rules().isWhitelisted(List.of("k1"))).containsEntry("k1", false);
  }

  // ── registerRefresh / unregisterRefresh ────────────────────────

  @Test
  void unregisterRefresh_shouldNotThrow() throws InterruptedException {
    CountDownLatch firstCallLatch = new CountDownLatch(1);
    when(hotKeyCache.getWithSoftExpire(eq("cancel-key"), any(ReadPolicy.class))).thenAnswer(
      invocation -> {
        firstCallLatch.countDown();
        return Optional.of("v");
      }
    );

    zeta.refresh().registerRefresh("cancel-key", () -> "v", ReadPolicy.of(300_000L, 10L));
    assertThat(firstCallLatch.await(5, TimeUnit.SECONDS)).as("first scheduled refresh occurred").isTrue();

    zeta.refresh().unregisterRefresh("cancel-key");

    verify(hotKeyCache, atLeastOnce()).getWithSoftExpire(eq("cancel-key"), any(ReadPolicy.class));
  }

  @Test
  void registerRefresh_replacesExistingRegistration() throws InterruptedException {
    AtomicInteger callCount = new AtomicInteger(0);
    when(hotKeyCache.getWithSoftExpire(anyString(), any(ReadPolicy.class))).thenAnswer(
      invocation -> {
        callCount.incrementAndGet();
        return Optional.of("v");
      }
    );

    zeta.refresh().registerRefresh("dup-key", () -> "v1", ReadPolicy.of(300_000L, 5L));
    Thread.sleep(20);
    zeta.refresh().registerRefresh("dup-key", () -> "v2", ReadPolicy.of(300_000L, 5L));

    int countBefore = callCount.get();
    Thread.sleep(30);
    int countAfter = callCount.get();

    assertThat(countAfter).isGreaterThan(countBefore);
  }

  @Test
  void registerRefresh_invokesGetWithSoftExpire() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    when(hotKeyCache.getWithSoftExpire(eq("k1"), any(ReadPolicy.class))).thenAnswer(invocation -> {
      latch.countDown();
      return Optional.of("v");
    });

    zeta.refresh().registerRefresh("k1", () -> "v", ReadPolicy.of(300_000L, 5L));
    assertThat(latch.await(5, TimeUnit.SECONDS)).as("getWithSoftExpire was invoked by scheduled refresh").isTrue();

    zeta.refresh().unregisterRefresh("k1");
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
          zeta.refresh().registerRefresh("race-key", () -> "v", ReadPolicy.of(300_000L, 5_000L));
        }
        return null;
      }));
    }
    start.countDown();
    for (Future<?> job : jobs) {
      job.get(30, TimeUnit.SECONDS);
    }
    pool.shutdown();

    // The per-key future map moved into the TimedRefreshCoordinator when the
    // scheduling infrastructure was extracted from the facade; reach it through
    // the facade's coordinator field.
    java.lang.reflect.Field coordinatorField = DefaultZeta.class.getDeclaredField("refreshCoordinator");
    coordinatorField.setAccessible(true);
    Object coordinator = coordinatorField.get(zeta);
    java.lang.reflect.Field field = coordinator.getClass().getDeclaredField("refreshFutures");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.ScheduledFuture<?>> futures =
      (java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.ScheduledFuture<?>>)
        field.get(coordinator);

    Object surviving = futures.get("race-key");
    assertThat(surviving).as("a registration must survive the race").isNotNull();
    assertThat(futures.get("race-key").isCancelled())
      .as("the surviving future must not be cancelled")
      .isFalse();

    zeta.refresh().unregisterRefresh("race-key");
  }

  @Test
  void destroy_shouldNotThrow() {
    zeta.refresh().registerRefresh("k1", () -> "v", ReadPolicy.of(300_000L, 10_000L));
    ((DefaultZeta) zeta).destroy();
    verify(hotKeyCache, never()).getWithSoftExpire(eq("k1"), any(ReadPolicy.class));
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
    assertThatThrownBy(() -> zeta.detector().isLocalHotKey(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void registerRefresh_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.refresh().registerRefresh(null, () -> "v", ReadPolicy.of(1000L, 100L))).isInstanceOf(
      IllegalArgumentException.class
    );
  }

  @Test
  void registerRefresh_shouldRejectNullSupplier() {
    assertThatThrownBy(() -> zeta.refresh().registerRefresh("k", null, ReadPolicy.of(1000L, 100L)))
      .isInstanceOf(NullPointerException.class);
  }

  @Test
  void registerRefresh_intervalIsSoftTtlTimesOnePointOne() {
    // The documented cadence contract: interval = resolved soft TTL × 1.1 so
    // the entry is stale at every tick even under the ±5% TTL jitter.
    assertThat(TimedRefreshCoordinator.intervalFor(100L)).isEqualTo(110L);
    assertThat(TimedRefreshCoordinator.intervalFor(1_000L)).isEqualTo(1_100L);
    assertThat(TimedRefreshCoordinator.intervalFor(5_000L)).isEqualTo(5_500L);
    // Rounding: 7ms × 1.1 = 7.7 → ceil to 8 so the interval never undershoots.
    assertThat(TimedRefreshCoordinator.intervalFor(7L)).isEqualTo(8L);
    // Clamped to at least 1ms for degenerate TTLs.
    assertThat(TimedRefreshCoordinator.intervalFor(0L)).isEqualTo(1L);
    assertThat(TimedRefreshCoordinator.intervalFor(1L)).isEqualTo(2L);
  }

  @Test
  void unregisterRefresh_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.refresh().unregisterRefresh(null)).isInstanceOf(IllegalArgumentException.class);
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
    assertThatThrownBy(() -> zeta.rules().addBlacklist((String) null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void addBlacklist_shouldRejectEmptyPattern() {
    assertThatThrownBy(() -> zeta.rules().addBlacklist("")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void evaluateRule_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.rules().evaluateRule(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void isBlacklisted_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.rules().isBlacklisted((String) null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void isWhitelisted_shouldRejectNullKey() {
    assertThatThrownBy(() -> zeta.rules().isWhitelisted((String) null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void areLocalHotKeys_shouldRejectNullKeys() {
    assertThatThrownBy(() -> zeta.detector().areLocalHotKeys(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void workerMode_shouldStillRejectNullKey() {
    Zeta workerOnly = new DefaultZeta(null, null, null, null, null);
    assertThatThrownBy(() -> workerOnly.get(null, () -> "v")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void putLocal_shouldRejectNegativeHardTtl() {
    assertThatThrownBy(() -> zeta.putLocal("k", "v", WritePolicy.of(-1L, 0L)))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void putThrough_shouldRejectNegativeHardTtl() {
    assertThatThrownBy(() -> zeta.putThrough("k", "v", () -> {}, WritePolicy.of(-1L, 0L))).isInstanceOf(
      IllegalArgumentException.class
    );
  }
}
