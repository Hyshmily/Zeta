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

import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.impl.ExpireManagerImpl;
import io.github.hyshmily.zeta.cache.loader.CacheLoader;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.rule.RuleMatcher;
import io.github.hyshmily.zeta.sync.local.DefaultSyncDecisionHandler;
import io.github.hyshmily.zeta.sync.local.SyncDecisionHandler;
import io.github.hyshmily.zeta.sync.local.SyncHook;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

class DefaultSyncDecisionHandlerTest {

  private Cache<String, Object> cache;
  private DefaultSyncDecisionHandler handler;
  private CacheLoader loader;
  private ExpireManagerImpl expireManager;
  private RuleMatcher ruleMatcher;

  @BeforeEach
  void setUp() {
    cache = Caffeine.newBuilder().maximumSize(100).build();
    ZetaProperties ttlConfig = new ZetaProperties();
    expireManager = new ExpireManagerImpl(cache, Runnable::run, ttlConfig, 10);
    loader = k -> "refreshed";
    ruleMatcher = mock(RuleMatcher.class);
    handler = new DefaultSyncDecisionHandler(cache, loader, expireManager, ruleMatcher, Collections.emptyList());
  }

  private static SyncMessage syncMessage(String key, String type, long version, boolean degraded) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, type);
    props.setHeader(HEADER_VERSION, version);
    props.setHeader(HEADER_IS_VERSION_DEGRADED, degraded);
    props.setHeader(HEADER_MESSAGE_ID, 1L);
    return SyncMessage.from(new Message(key.getBytes(StandardCharsets.UTF_8), props));
  }

  private static CacheEntry entry(long dv, boolean degraded, KeyState state) {
    return CacheEntry.builder()
      .value("v")
      .dataVersion(dv)
      .isVersionDegraded(degraded)
      .decisionVersion(0)
      .hardTtlMs(300_000)
      .hardExpireAtMs(Long.MAX_VALUE)
      .softTtlMs(30_000)
      .softExpireAtMs(30_000)
      .keyState(state)
      .normalHardTtlMs(300_000)
      .normalSoftTtlMs(30_000)
      .build();
  }

  @Test
  void handleRefresh_shouldInvokeAfterRefreshHook() {
    cache.put("key1", entry(1, false, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    handler = new DefaultSyncDecisionHandler(cache, loader, expireManager, ruleMatcher, List.of(hook));

    handler.handleRefresh(syncMessage("key1", SyncMessage.TYPE_REFRESH, 2L, false));

    verify(hook).afterRefresh(eq("key1"), any(), any());
  }

  @Test
  void handleRefresh_staleVersion_shouldInvokeOnRefreshSkipped() {
    cache.put("key1", entry(5, false, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    handler = new DefaultSyncDecisionHandler(cache, loader, expireManager, ruleMatcher, List.of(hook));

    handler.handleRefresh(syncMessage("key1", SyncMessage.TYPE_REFRESH, 3L, false));

    verify(hook).onRefreshSkipped(eq("key1"), any());
  }

  @Test
  void handleRefresh_nullValue_shouldInvokeOnRefreshSkipped() {
    cache.put("key1", entry(1, false, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    handler = new DefaultSyncDecisionHandler(cache, k -> null, expireManager, ruleMatcher, List.of(hook));

    handler.handleRefresh(syncMessage("key1", SyncMessage.TYPE_REFRESH, 2L, false));

    verify(hook).onRefreshSkipped(eq("key1"), any());
  }

  @Test
  void handleLocalInvalidate_shouldInvokeAfterInvalidateHook() {
    cache.put("key1", entry(1, false, KeyState.NORMAL));
    SyncHook hook = mock(SyncHook.class);
    handler = new DefaultSyncDecisionHandler(cache, loader, expireManager, ruleMatcher, List.of(hook));

    handler.handleLocalInvalidate(syncMessage("key1", SyncMessage.TYPE_INVALIDATE, 2L, false));

    verify(hook).afterInvalidate(eq("key1"), any());
  }

  @Test
  void handleRulesSync_shouldDelegateToRuleMatcher() {
    handler.handleRulesSync(syncMessage("rules-payload", SyncMessage.TYPE_RULES_SYNC, 0L, false));

    verify(ruleMatcher).syncRules("rules-payload", 0L);
  }

  @Test
  void multipleHooks_shouldAllBeInvoked() {
    cache.put("key1", entry(1, false, KeyState.NORMAL));
    SyncHook h1 = mock(SyncHook.class);
    SyncHook h2 = mock(SyncHook.class);
    handler = new DefaultSyncDecisionHandler(cache, loader, expireManager, ruleMatcher, List.of(h1, h2));

    handler.handleRefresh(syncMessage("key1", SyncMessage.TYPE_REFRESH, 2L, false));

    verify(h1).afterRefresh(eq("key1"), any(), any());
    verify(h2).afterRefresh(eq("key1"), any(), any());
  }

  @Test
  void hookException_shouldNotBreakOtherHooks() {
    cache.put("key1", entry(1, false, KeyState.NORMAL));
    AtomicInteger count = new AtomicInteger(0);
    SyncHook failingHook = new SyncHook() {
      @Override public void afterRefresh(String k, SyncMessage sm, CacheEntry e) { throw new RuntimeException("fail"); }
    };
    SyncHook countingHook = new SyncHook() {
      @Override public void afterRefresh(String k, SyncMessage sm, CacheEntry e) { count.incrementAndGet(); }
    };
    handler = new DefaultSyncDecisionHandler(cache, loader, expireManager, ruleMatcher, List.of(failingHook, countingHook));

    handler.handleRefresh(syncMessage("key1", SyncMessage.TYPE_REFRESH, 2L, false));

    assertThat(count.get()).isEqualTo(1);
  }

  @Test
  void customHandler_shouldReplaceDefaultBehavior() {
    SyncDecisionHandler custom = new SyncDecisionHandler() {
      @Override public void handleRefresh(SyncMessage sm) {
        cache.put(sm.cacheKey(), "custom-refresh");
      }
      @Override public void handleLocalInvalidate(SyncMessage sm) {
        cache.put(sm.cacheKey(), "custom-invalidate");
      }
      @Override public void handleLocalInvalidateAll(SyncMessage sm) {
        cache.put("batch", "custom-batch");
      }
      @Override public void handleRulesSync(SyncMessage sm) {
        cache.put("rules", "custom-rules");
      }
    };

    custom.handleRefresh(syncMessage("k", SyncMessage.TYPE_REFRESH, 1L, false));
    assertThat(cache.getIfPresent("k")).isEqualTo("custom-refresh");

    custom.handleLocalInvalidate(syncMessage("k", SyncMessage.TYPE_INVALIDATE, 1L, false));
    assertThat(cache.getIfPresent("k")).isEqualTo("custom-invalidate");

    custom.handleLocalInvalidateAll(syncMessage("[]", SyncMessage.TYPE_INVALIDATE_ALL, 0L, false));
    assertThat(cache.getIfPresent("batch")).isEqualTo("custom-batch");

    custom.handleRulesSync(syncMessage("rules", SyncMessage.TYPE_RULES_SYNC, 0L, false));
    assertThat(cache.getIfPresent("rules")).isEqualTo("custom-rules");
  }
}
