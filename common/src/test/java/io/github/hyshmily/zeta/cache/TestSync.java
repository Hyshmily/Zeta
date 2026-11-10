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

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.sync.local.Batch;
import io.github.hyshmily.zeta.sync.local.DefaultSyncContext;
import io.github.hyshmily.zeta.sync.local.SyncHook;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.sync.local.SyncPolicy;
import io.github.hyshmily.zeta.sync.local.SyncType;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * Test facade over {@link DefaultSyncContext} that keeps the per-message call shape the
 * semantic tests read best with. Every method wraps its message in the matching one-element
 * batch and calls the framework's default {@link SyncPolicy}.
 */
final class TestSync {

  private final DefaultSyncContext context;

  private final SyncPolicy policy = new SyncPolicy() {};

  TestSync(
    Cache<String, CacheEntry> cache,
    @Nullable CacheLoader<String, Object> loader,
    EntryLifecycle lifecycle,
    List<SyncHook> hooks
  ) {
    this.context = new DefaultSyncContext(cache, loader, lifecycle, hooks, null, null);
  }

  TestSync(
    Cache<String, CacheEntry> cache,
    @Nullable CacheLoader<String, Object> loader,
    EntryLifecycle lifecycle,
    List<SyncHook> hooks,
    SingleFlight singleFlight
  ) {
    this.context = new DefaultSyncContext(cache, loader, lifecycle, hooks, singleFlight, null);
  }

  void handleRefresh(SyncMessage message) {
    policy.value(value(message), context);
  }

  void handlePut(SyncMessage message) {
    policy.value(value(message), context);
  }

  void handleLocalInvalidate(SyncMessage message) {
    policy.invalidate(value(message), context);
  }

  void handleClearAll(long stamp) {
    policy.clear(new Batch.Clear(List.of(new SyncMessage(0L, "clear-all", SyncType.INVALIDATE_ALL, stamp))), context);
  }

  void handleClearPrefix(String prefix, long stamp) {
    policy.clear(new Batch.Clear(List.of(new SyncMessage(0L, prefix, SyncType.INVALIDATE_PREFIX, stamp))), context);
  }

  private static Batch.Value value(SyncMessage message) {
    return new Batch.Value(List.of(message), message, List.of(), null);
  }
}
