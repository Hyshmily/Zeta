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

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycleImpl;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.policy.InvalidatePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.scheduler.BackgroundRefresher;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.util.clock.MonotonicStampClock;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/** Remote-only invalidation and clear follow the same after-commit contract as the local paths. */
class RemoteInvalidationTransactionTest {

  private BroadcastBuffer broadcastBuffer;
  private HotKeyCache cache;

  @BeforeEach
  void setUp() {
    Cache<String, CacheEntry> table = Caffeine.newBuilder().maximumSize(10).build();
    ZetaProperties properties = new ZetaProperties();
    HealthView healthView = mock(HealthView.class);
    EntryLifecycle lifecycle = new EntryLifecycleImpl(table, properties, healthView);
    broadcastBuffer = mock(BroadcastBuffer.class);
    cache = new HotKeyCache(
      mock(HotKeyDetector.class),
      table,
      mock(SingleFlight.class),
      lifecycle,
      mock(BackgroundRefresher.class),
      mock(AccessCounter.class),
      broadcastBuffer,
      new MonotonicStampClock(1L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS),
      healthView
    );
  }

  @Test
  @DisplayName("a remote invalidation is not broadcast before the transaction commits")
  void remoteInvalidationWaitsForCommit() {
    TransactionSynchronizationManager.initSynchronization();
    try {
      cache.invalidate("k", new InvalidatePolicy(InvalidatePolicy.Scope.REMOTE, false));
      verifyNoInteractions(broadcastBuffer);

      commit();

      verify(broadcastBuffer).invalidate(eq("k"), anyLong());
    } finally {
      TransactionSynchronizationManager.clear();
    }
  }

  @Test
  @DisplayName("immediate remote invalidation broadcasts on the calling thread")
  void immediateRemoteInvalidationBroadcastsNow() {
    TransactionSynchronizationManager.initSynchronization();
    try {
      cache.invalidate("k", new InvalidatePolicy(InvalidatePolicy.Scope.REMOTE, true));
      verify(broadcastBuffer).invalidate(eq("k"), anyLong());
    } finally {
      TransactionSynchronizationManager.clear();
    }
  }

  @Test
  @DisplayName("remote batch and clear wait for commit")
  void remoteBatchAndClearWaitForCommit() {
    TransactionSynchronizationManager.initSynchronization();
    try {
      InvalidatePolicy remote = new InvalidatePolicy(InvalidatePolicy.Scope.REMOTE, false);
      cache.invalidate(List.of("k1", "k2"), remote);
      cache.clear(remote);
      cache.clearPrefix("ns" + CacheKeysPolicy.NAMESPACE_DELIMITER, remote);
      verifyNoInteractions(broadcastBuffer);

      commit();

      verify(broadcastBuffer).invalidateBatch(org.mockito.ArgumentMatchers.anyList());
      verify(broadcastBuffer).invalidateAll(anyLong());
      verify(broadcastBuffer).invalidatePrefix(org.mockito.ArgumentMatchers.anyString(), anyLong());
    } finally {
      TransactionSynchronizationManager.clear();
    }
  }

  private static void commit() {
    TransactionSynchronizationManager.getSynchronizations().forEach(TransactionSynchronization::afterCommit);
  }
}
