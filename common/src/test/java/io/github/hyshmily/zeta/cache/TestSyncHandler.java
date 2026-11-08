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

import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.sync.local.DefaultSyncDecisionHandler;
import io.github.hyshmily.zeta.sync.local.SyncHook;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.sync.local.SyncType;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * Test facade over {@link DefaultSyncDecisionHandler} that keeps the
 * per-message call shape the semantic tests read best with. Every method wraps
 * its message in a one-element batch; the handler's own contract is unchanged.
 */
final class TestSyncHandler {

  private final DefaultSyncDecisionHandler delegate;

  TestSyncHandler(
    com.github.benmanes.caffeine.cache.Cache<String, CacheEntry> cache,
    @Nullable CacheLoader<String, Object> loader,
    EntryLifecycle lifecycle,
    List<SyncHook> hooks
  ) {
    this.delegate = new DefaultSyncDecisionHandler(cache, loader, lifecycle, hooks, null, null);
  }

  TestSyncHandler(
    com.github.benmanes.caffeine.cache.Cache<String, CacheEntry> cache,
    @Nullable CacheLoader<String, Object> loader,
    EntryLifecycle lifecycle,
    List<SyncHook> hooks,
    io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight singleFlight
  ) {
    this.delegate = new DefaultSyncDecisionHandler(cache, loader, lifecycle, hooks, singleFlight, null);
  }

  void handleRefresh(SyncMessage message) {
    delegate.handleBatch(List.of(message));
  }

  void handlePut(SyncMessage message) {
    delegate.handleBatch(List.of(message));
  }

  void handleLocalInvalidate(SyncMessage message) {
    delegate.handleBatch(List.of(message));
  }

  void handleClearAll(long stamp) {
    delegate.handleBatch(List.of(new SyncMessage(0L, "clear-all", SyncType.INVALIDATE_ALL, stamp)));
  }

  void handleClearPrefix(String prefix, long stamp) {
    delegate.handleBatch(List.of(new SyncMessage(0L, prefix, SyncType.INVALIDATE_PREFIX, stamp)));
  }
}
