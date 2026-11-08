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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycleImpl;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.TtlPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.cache.cachesupport.raw.VersionedValue;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.util.TimeSource;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the background soft-expire refresh executor: dedup, limiter,
 * timeout, fault modes, version-guarded merge, and lease-on-failure.
 *
 * <p>The stateless TTL arithmetic tests live in {@code TtlPolicyTest}; the
 * entry-factory draft tests live in {@code EntryLifecycleImplTest}.
 */
class DefaultBackgroundRefresherTest {

  private BackgroundRefresher refresher;
  private Cache<String, CacheEntry> caffeineCache;
  private ZetaProperties ttlConfig;
  private TtlPolicy ttlPolicy;
  private EntryLifecycle lifecycle;

  @BeforeEach
  void setUp() {
    caffeineCache = Caffeine.newBuilder().maximumSize(100).build();
    ttlConfig = new ZetaProperties();
    Executor executor = Runnable::run;
    ttlPolicy = new TtlPolicy(ttlConfig);
    lifecycle = new EntryLifecycleImpl(caffeineCache, ttlConfig, null);
    refresher = new DefaultBackgroundRefresher(caffeineCache, executor, lifecycle, 10);
  }

  @AfterEach
  void resetClock() {
    TimeSource.setTimeOffsetForTest(0, 0);
  }

  /**
   * Verifies that triggerBackgroundRefresh executes the supplier and preserves the cache entry.
   */
  @Test
  void triggerBackgroundRefresh_shouldAcquireAndReleasePermit() {
    caffeineCache.put(
      "key",
      EntryDraft.blank(null)
        .value("old")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 30_000)
        .keyState(KeyState.HOT)
        .build()
    );

    refresher.triggerBackgroundRefresh("key", () -> new VersionedValue("newValue", 0L), 30_000);

    assertThat(caffeineCache.getIfPresent("key")).isNotNull();
  }

  /**
   * Entry-absent rebuild carries the evicted entry's OWN hard TTL: when the
   * entry is evicted while the refresh is in flight, the rebuilt entry keeps
   * the per-call hard TTL override instead of silently reverting to the
   * configured default (300_000 in this suite).
   */
  @Test
  void triggerBackgroundRefresh_entryEvictedDuringRefresh_shouldRebuildWithSnapshotHardTtl() {
    caffeineCache.put(
      "key",
      EntryDraft.blank(null)
        .value("old")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(123_456, 30_000, 123_456, 30_000)
        .expiryAt(System.currentTimeMillis() + 123_456, System.currentTimeMillis() - 1_000)
        .keyState(KeyState.NORMAL)
        .build()
    );

    refresher.triggerBackgroundRefresh(
      "key",
      () -> {
        // The direct test executor runs the reader right after the snapshot
        // was taken: evicting here reproduces "entry absent at apply time".
        caffeineCache.invalidate("key");
        return new VersionedValue("fresh", 7L);
      },
      30_000
    );

    CacheEntry rebuilt = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(rebuilt).isNotNull();
    assertThat((Object) rebuilt.getValue()).isEqualTo("fresh");
    assertThat(rebuilt.getDataVersion()).isEqualTo(7L);
    assertThat(rebuilt.getHardTtlMs()).isEqualTo(123_456L);
  }

  /**
   * A refresh task that completes before the {@code refreshes} compute
   * stores it (fast reader, synchronous executor) must not leave a done marker
   * behind: the map is unbounded, so a cold key would otherwise retain the
   * completed marker until its next trigger.
   */
  @Test
  void triggerBackgroundRefresh_fastCompletedTask_shouldNotLeaveDoneMarker() throws Exception {
    refresher.triggerBackgroundRefresh("key", () -> new VersionedValue("v", 0L), 30_000);

    java.lang.reflect.Field pending = DefaultBackgroundRefresher.class.getDeclaredField("refreshes");
    pending.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<String, ?> map = (java.util.Map<String, ?>) pending.get(refresher);

    assertThat(map).isEmpty();
  }

  /**
   * A completed refresh frees its key immediately: the synchronous executor
   * settles the marker before {@code triggerBackgroundRefresh} returns, and the
   * next call must arm a new task rather than being mistaken for a duplicate.
   */
  @Test
  void triggerBackgroundRefresh_afterSynchronousCompletion_shouldArmAgain() throws Exception {
    AtomicInteger counter = new AtomicInteger();
    for (int i = 0; i < 2; i++) {
      refresher.triggerBackgroundRefresh(
        "key",
        () -> {
          counter.incrementAndGet();
          return new VersionedValue("v", 0L);
        },
        30_000
      );
    }

    assertThat(counter.get()).isEqualTo(2);
    java.lang.reflect.Field pending = DefaultBackgroundRefresher.class.getDeclaredField("refreshes");
    pending.setAccessible(true);
    assertThat((java.util.Map<?, ?>) pending.get(refresher)).isEmpty();
  }

  /**
   * Verifies that triggerBackgroundRefresh discards the result when the entry's data version has changed since the refresh started.
   */
  @Test
  void triggerBackgroundRefresh_withStaleVersion_shouldDiscardResult() {
    caffeineCache.put(
      "key",
      EntryDraft.blank(null)
        .value("original")
        .version(5)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 300_000)
        .keyState(KeyState.HOT)
        .build()
    );

    refresher.triggerBackgroundRefresh(
      "key",
      () -> {
        caffeineCache
          .asMap()
          .computeIfPresent("key", (k, existing) -> {
            if (existing != null) {
              return EntryDraft.of(existing).version(10).build();
            }
            return null;
          });
        // Unknown-stamp carrier: the snapshot guard still discards it.
        return new VersionedValue("stale-value", 0L);
      },
      30_000
    );

    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(entry).isNotNull();
    assertThat(entry.getDataVersion()).isEqualTo(10);
    assertThat((Object) entry.getValue()).isEqualTo("original");
  }

  /**
   * The C1 regression: a stamped refresh with NO concurrent write must apply —
   * entry == probe, so a probe-version guard with equal-skip discarded every
   * ordinary refresh and SWR never extended TTLs. The snapshot guard applies
   * it (same version as the snapshot, no boundary crossed).
   */
  @Test
  void triggerBackgroundRefresh_stampedNoConcurrentWrite_shouldApply() {
    caffeineCache.put(
      "key",
      EntryDraft.blank(null)
        .value("original")
        .version(5)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(System.currentTimeMillis() + 300_000, System.currentTimeMillis() - 1_000)
        .keyState(KeyState.NORMAL)
        .build()
    );

    refresher.triggerBackgroundRefresh("key", () -> new VersionedValue("fresh", 5L), 30_000);

    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(entry).isNotNull();
    assertThat((Object) entry.getValue()).isEqualTo("fresh");
    assertThat(entry.getDataVersion()).isEqualTo(5);
  }

  /**
   * A stamped refresh must NOT overwrite a concurrent writer's fresh entry,
   * even when the probe was taken after the write (probe == entry): the
   * refresh value is stale, the writer's entry wins. The snapshot guard
   * discards it (entry strictly above the snapshot).
   */
  @Test
  void triggerBackgroundRefresh_stampedConcurrentWrite_shouldDiscard() {
    caffeineCache.put(
      "key",
      EntryDraft.blank(null)
        .value("original")
        .version(5)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(System.currentTimeMillis() + 300_000, System.currentTimeMillis() - 1_000)
        .keyState(KeyState.NORMAL)
        .build()
    );

    refresher.triggerBackgroundRefresh(
      "key",
      () -> {
        caffeineCache
          .asMap()
          .computeIfPresent("key", (k, existing) -> {
            if (existing != null) {
              return EntryDraft.of(existing).value("writer-value").version(6).build();
            }
            return null;
          });
        // Stale value, probe taken after the concurrent write.
        return new VersionedValue("stale-value", 6L);
      },
      30_000
    );

    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(entry).isNotNull();
    assertThat(entry.getDataVersion()).isEqualTo(6);
    assertThat((Object) entry.getValue()).isEqualTo("writer-value");
  }

  /**
   * A failed refresh must not reopen a retry window that a peer invalidation already closed: the
   * snapshot identity is unchanged by the invalidation, so only the tombstone guard distinguishes
   * "same entry" from "same entry the cluster has since condemned".
   */
  @Test
  void triggerBackgroundRefresh_failureDoesNotReopenAWindowClosedByInvalidation() {
    long now = System.currentTimeMillis();
    long hardExpireAt = now + 300_000L;
    long softExpireAt = now - 1_000L;
    caffeineCache.put(
      "key",
      EntryDraft.blank(null)
        .value("old")
        .version(1)
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(hardExpireAt, softExpireAt)
        .keyState(KeyState.NORMAL)
        .build()
    );

    refresher.triggerBackgroundRefresh(
      "key",
      () -> {
        lifecycle.recordInvalidation("key", 2L);
        throw new IllegalStateException("source down");
      },
      30_000
    );

    CacheEntry after = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(after).isNotNull();
    assertThat(after.getSoftExpireAtMs()).isEqualTo(softExpireAt);
    assertThat(after.getHardExpireAtMs()).isEqualTo(hardExpireAt);
  }

  /**
   * Verifies that triggerBackgroundRefresh skips the cache update when the supplier
   * returns null (fault mode: null supplier result).
   */
  @Test
  void triggerBackgroundRefresh_withNullResult_shouldNotUpdateCache() throws InterruptedException {
    Executor asyncExec = Executors.newCachedThreadPool();
    BackgroundRefresher asyncExpire = new DefaultBackgroundRefresher(caffeineCache, asyncExec, lifecycle, 10);

    caffeineCache.put(
      "key",
      EntryDraft.blank(null)
        .value("original")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 30_000)
        .keyState(KeyState.HOT)
        .build()
    );

    asyncExpire.triggerBackgroundRefresh("key", () -> null, 30_000);
    Thread.sleep(200);

    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(entry).isNotNull();
    assertThat((Object) entry.getValue()).isEqualTo("original");
    assertThat(entry.getSoftExpireAtMs())
      .as("a null refresh closes the retry window instead of re-arming every read")
      .isEqualTo(entry.getHardExpireAtMs());
    assertThat(lifecycle.ttlPolicy().isSoftExpired(entry)).isFalse();
  }

  /**
   * Verifies that triggerBackgroundRefresh skips when the refresh limiter semaphore
   * is exhausted (fault mode: limiter backpressure).
   */
  @Test
  void triggerBackgroundRefresh_withExhaustedLimiter_shouldSkip() throws InterruptedException {
    Executor asyncExec = Executors.newCachedThreadPool();
    BackgroundRefresher limited = new DefaultBackgroundRefresher(caffeineCache, asyncExec, lifecycle, 1);

    caffeineCache.put(
      "key1",
      EntryDraft.blank(null)
        .value("original1")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 30_000)
        .keyState(KeyState.HOT)
        .build()
    );
    caffeineCache.put(
      "key2",
      EntryDraft.blank(null)
        .value("original2")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 30_000)
        .keyState(KeyState.HOT)
        .build()
    );

    CountDownLatch blockLatch = new CountDownLatch(1);

    // First call blocks the supplier, holding the only permit
    limited.triggerBackgroundRefresh(
      "key1",
      () -> {
        try {
          blockLatch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return "first-value";
      },
      30_000
    );

    // Give first call time to acquire the permit
    Thread.sleep(50);

    // Second refresh for DIFFERENT key should fail tryAcquire and return immediately
    limited.triggerBackgroundRefresh("key2", () -> "second-value", 30_000);

    // key1 should still be "original1" (first hasn't completed yet)
    CacheEntry entry1 = (CacheEntry) caffeineCache.getIfPresent("key1");
    assertThat(entry1).isNotNull();
    assertThat((Object) entry1.getValue()).isEqualTo("original1");

    // key2 should still be "original2" (limiter exhausted, refresh skipped)
    CacheEntry entry2 = (CacheEntry) caffeineCache.getIfPresent("key2");
    assertThat(entry2).isNotNull();
    assertThat((Object) entry2.getValue()).isEqualTo("original2");

    blockLatch.countDown();
  }

  /**
   * Verifies that triggerBackgroundRefresh deduplicates concurrent calls for the
   * same key — only the first caller executes the supplier (per-key dedup).
   */
  @Test
  void triggerBackgroundRefresh_withSameKey_shouldDeduplicate() throws InterruptedException {
    Executor asyncExec = Executors.newCachedThreadPool();
    BackgroundRefresher asyncExpire = new DefaultBackgroundRefresher(caffeineCache, asyncExec, lifecycle, 10);

    caffeineCache.put(
      "key",
      EntryDraft.blank(null)
        .value("original")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 30_000)
        .keyState(KeyState.HOT)
        .build()
    );

    AtomicInteger counter = new AtomicInteger(0);
    CountDownLatch blockLatch = new CountDownLatch(1);

    // First call blocks the supplier
    asyncExpire.triggerBackgroundRefresh(
      "key",
      () -> {
        counter.incrementAndGet();
        try {
          blockLatch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return "first";
      },
      30_000
    );

    // Give first call time to register in refreshes
    Thread.sleep(50);

    // Second call for same key should be deduped (supplier not invoked)
    asyncExpire.triggerBackgroundRefresh(
      "key",
      () -> {
        counter.incrementAndGet();
        return "second";
      },
      30_000
    );

    blockLatch.countDown();
    Thread.sleep(300);

    assertThat(counter.get()).isEqualTo(1);
  }

  /**
   * Verifies that triggerBackgroundRefresh with a supplier error logs the failure
   * and leaves the existing entry intact (fault mode: supplier exception).
   * A permanent entry ({@code hardExpireAtMs == Long.MAX_VALUE}) is never
   * leased — there is nothing to extend (design decisions: expiry and refresh).
   */
  @Test
  void triggerBackgroundRefresh_withSupplierError_shouldPreserveExistingEntry() throws InterruptedException {
    Executor asyncExec = Executors.newCachedThreadPool();
    BackgroundRefresher asyncExpire = new DefaultBackgroundRefresher(caffeineCache, asyncExec, lifecycle, 10);

    caffeineCache.put(
      "key",
      EntryDraft.blank(null)
        .value("original")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 30_000)
        .keyState(KeyState.HOT)
        .build()
    );

    asyncExpire.triggerBackgroundRefresh(
      "key",
      () -> {
        throw new RuntimeException("refresh-failed");
      },
      30_000
    );

    Thread.sleep(200);

    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(entry).isNotNull();
    assertThat((Object) entry.getValue()).isEqualTo("original");
    assertThat(entry.getHardExpireAtMs()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Verifies that a key that is removed from the cache during an in-flight background
   * refresh does not cause errors (fault mode: key evicted mid-refresh).
   */
  @Test
  void triggerBackgroundRefresh_withEvictedKeyDuringRefresh_shouldNotError() throws InterruptedException {
    Executor asyncExec = Executors.newCachedThreadPool();
    BackgroundRefresher asyncExpire = new DefaultBackgroundRefresher(caffeineCache, asyncExec, lifecycle, 10);

    caffeineCache.put(
      "key",
      EntryDraft.blank(null)
        .value("original")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 30_000)
        .keyState(KeyState.HOT)
        .build()
    );

    asyncExpire.triggerBackgroundRefresh(
      "key",
      () -> {
        caffeineCache.invalidate("key");
        return new VersionedValue("fresh-value", 0L);
      },
      30_000
    );

    Thread.sleep(200);

    // Key was evicted in the supplier; the refresh compute should create a new entry
    // because Optional.ofNullable(existing) will be empty, triggering orElseGet branch
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(entry).isNotNull();
    assertThat((Object) entry.getValue()).isEqualTo("fresh-value");
  }

  /**
   * Verifies that triggerBackgroundRefresh catches RejectedExecutionException from
   * CompletableFuture.supplyAsync, releases the limiter permit and pending-refresh
   * marker, and preserves the existing cache entry.
   */
  @Test
  void triggerBackgroundRefresh_withRejectedExecution_shouldReleaseResources() {
    Executor rejectingExecutor = task -> {
      throw new RejectedExecutionException("rejected");
    };
    BackgroundRefresher rejectingMgr = new DefaultBackgroundRefresher(caffeineCache, rejectingExecutor, lifecycle, 10);

    caffeineCache.put(
      "key",
      EntryDraft.blank(null)
        .value("original")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 30_000)
        .keyState(KeyState.HOT)
        .build()
    );

    rejectingMgr.triggerBackgroundRefresh("key", () -> "newValue", 30_000);

    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(entry).isNotNull();
    assertThat((Object) entry.getValue()).isEqualTo("original");
  }

  // ── Lease-on-Failure (design decisions: expiry and refresh) ───────────────────────────────────

  /**
   * Verifies that a failed background refresh leases the existing entry:
   * value and metadata preserved, duration fields untouched (Option B). The
   * lease never shortens a healthy budget — with ~300s remaining the hard
   * timestamp is preserved (not halved), and soft lands at the lease midpoint
   * (the retry window).
   */
  @Test
  void triggerBackgroundRefresh_withSupplierError_shouldLeaseExistingEntry() throws InterruptedException {
    ExecutorService asyncExec = Executors.newCachedThreadPool();
    BackgroundRefresher asyncExpire = new DefaultBackgroundRefresher(caffeineCache, asyncExec, lifecycle, 10);
    try {
      long originalExpire = System.currentTimeMillis() + 300_000;
      caffeineCache.put("key", hotEntry("original", 1, originalExpire));

      asyncExpire.triggerBackgroundRefresh(
        "key",
        () -> {
          throw new RuntimeException("refresh-failed");
        },
        30_000
      );
      Thread.sleep(200);

      long now = System.currentTimeMillis();
      CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
      assertThat(entry).isNotNull();
      assertThat((Object) entry.getValue()).isEqualTo("original");
      assertThat(entry.getDataVersion()).isEqualTo(1);
      assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
      // Duration fields stay authoritative — only timestamps move (design decisions: expiry and refresh).
      assertThat(entry.getHardTtlMs()).isEqualTo(300_000);
      assertThat(entry.getSoftTtlMs()).isEqualTo(30_000);
      // Never-shorten: remaining (~300s) exceeds the floor, so hard is
      // preserved (within elapsed tolerance), soft at the lease midpoint.
      assertThat(entry.getHardExpireAtMs()).isBetween(now + 290_000, originalExpire + 1_000);
      assertThat(entry.getSoftExpireAtMs()).isBetween(now + 140_000, now + 160_000);
      assertThat(entry.getSoftExpireAtMs()).isLessThan(entry.getHardExpireAtMs());
      // Observability: one granted lease, nothing refused.
      assertThat(asyncExpire.getRefreshFailureLeased()).isEqualTo(1);
      assertThat(asyncExpire.leasedStaleness(10)).containsKey("key");
    } finally {
      asyncExec.shutdownNow();
    }
  }

  /**
   * Verifies that repeated failures on a long-lived entry preserve the
   * remaining budget instead of halving it: a failure must not punish a
   * healthy TTL. Decay toward the floor happens through wall-time passage,
   * not through halving.
   */
  @Test
  void triggerBackgroundRefresh_repeatedFailures_shouldPreserveRemainingBudget() {
    long originalExpire = System.currentTimeMillis() + 600_000;
    caffeineCache.put("key", hotEntry("original", 1, originalExpire));
    BackgroundRefresher syncExpire = new DefaultBackgroundRefresher(caffeineCache, Runnable::run, lifecycle, 10);

    syncExpire.triggerBackgroundRefresh("key", failingReader(), 30_000);
    long now = System.currentTimeMillis();
    CacheEntry first = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(first.getHardExpireAtMs()).isBetween(now + 590_000, originalExpire + 1_000);

    syncExpire.triggerBackgroundRefresh("key", failingReader(), 30_000);
    now = System.currentTimeMillis();
    CacheEntry second = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(second.getHardExpireAtMs()).isBetween(now + 580_000, originalExpire + 1_000);

    syncExpire.triggerBackgroundRefresh("key", failingReader(), 30_000);
    now = System.currentTimeMillis();
    CacheEntry third = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(third.getHardExpireAtMs()).isBetween(now + 570_000, originalExpire + 1_000);
    // Soft at the lease midpoint — the entry stays retry-eligible.
    assertThat(third.getSoftExpireAtMs()).isBetween(now + 280_000, now + 320_000);
    assertThat(third.getSoftExpireAtMs()).isLessThan(third.getHardExpireAtMs());
    assertThat((Object) third.getValue()).isEqualTo("original");
    assertThat(syncExpire.getRefreshFailureLeased()).isEqualTo(3);
  }

  /**
   * Verifies the floor follows the entry's own scale and extends short
   * budgets: with ~100s remaining (below the 120s floor, hard TTL 300s) the
   * hard timestamp extends to now + 120s instead of tracking the remainder.
   */
  @Test
  void triggerBackgroundRefresh_shortRemaining_shouldExtendToFloor() {
    caffeineCache.put("key", hotEntry("original", 1, System.currentTimeMillis() + 100_000));
    BackgroundRefresher syncExpire = new DefaultBackgroundRefresher(caffeineCache, Runnable::run, lifecycle, 10);

    syncExpire.triggerBackgroundRefresh("key", failingReader(), 30_000);
    long now = System.currentTimeMillis();
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(entry).isNotNull();
    assertThat(entry.getHardExpireAtMs()).isBetween(now + 110_000, now + 130_000);
    assertThat(entry.getSoftExpireAtMs()).isBetween(now + 50_000, now + 70_000);
    assertThat((Object) entry.getValue()).isEqualTo("original");
  }

  /**
   * Verifies that an entry whose declared hard TTL cannot fit a retry window
   * below the refresh timeout (hard TTL &lt; 2 × 30s) is refused a lease and
   * left to hard-expire.
   */
  @Test
  void triggerBackgroundRefresh_tinyHardTtl_shouldAbandonLease() {
    caffeineCache.put(
      "key",
      EntryDraft.blank(null)
        .value("original")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(30_000, 10_000, 30_000, 10_000)
        .expiryAt(System.currentTimeMillis() + 30_000, System.currentTimeMillis() - 1_000)
        .keyState(KeyState.NORMAL)
        .build()
    );
    long before = System.currentTimeMillis();
    BackgroundRefresher syncExpire = new DefaultBackgroundRefresher(caffeineCache, Runnable::run, lifecycle, 10);

    syncExpire.triggerBackgroundRefresh("key", failingReader(), 10_000);

    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(entry).isNotNull();
    // Untouched: no extension granted, and the retry window is closed at the
    // existing hard deadline instead of re-triggering the failing reader on every read.
    assertThat(entry.getHardExpireAtMs()).isLessThanOrEqualTo(before + 30_000);
    assertThat(entry.getSoftExpireAtMs()).isEqualTo(entry.getHardExpireAtMs());
    assertThat(syncExpire.getRefreshFailureLeased()).isZero();
  }

  /**
   * Verifies the failure classifier: a JVM-level {@code Error} never leases
   * (fatal, not a reason to serve older data longer) and is counted as
   * suppressed.
   */
  @Test
  void triggerBackgroundRefresh_withError_shouldRefuseLeaseByClassifier() {
    caffeineCache.put("key", hotEntry("original", 1, System.currentTimeMillis() + 300_000));
    BackgroundRefresher syncExpire = new DefaultBackgroundRefresher(caffeineCache, Runnable::run, lifecycle, 10);

    syncExpire.triggerBackgroundRefresh(
      "key",
      () -> {
        throw new AssertionError("fatal");
      },
      30_000
    );

    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(entry).isNotNull();
    assertThat((Object) entry.getValue()).isEqualTo("original");
    assertThat(entry.getHardExpireAtMs())
      .as("a classifier refusal hard-expires instead of serving known-stale data")
      .isLessThanOrEqualTo(System.currentTimeMillis());
    assertThat(syncExpire.getRefreshFailureLeased()).isZero();
  }

  /**
   * Verifies the staleness cap: an entry born over 30 minutes ago is refused
   * a lease (counted as capped) and left to hard-expire.
   */
  @Test
  void triggerBackgroundRefresh_overAgeEntry_shouldRefuseLeaseByStalenessCap() {
    // The staleness cap is measured on the monotonic axis, whose origin is JVM
    // boot: a fresh test JVM cannot otherwise hold an entry older than 30 min.
    // Advance the axis so the birth stamp stays positive and genuinely old.
    TimeSource.setTimeOffsetForTest(0, 3_701_000L);
    int oldBirth = TimeSource.monotonicSecondsSinceBoot() - 3700;
    caffeineCache.put(
      "key",
      EntryDraft.blank(null)
        .value("original")
        .version(1)
        .decision(DecisionStamp.hot(0, null, 0L))
        .ttl(300_000, 30_000, 300_000, 30_000)
        .expiryAt(System.currentTimeMillis() + 300_000, System.currentTimeMillis() - 1_000)
        .keyState(KeyState.HOT)
        .birthStampSec(oldBirth)
        .build()
    );
    BackgroundRefresher syncExpire = new DefaultBackgroundRefresher(caffeineCache, Runnable::run, lifecycle, 10);

    syncExpire.triggerBackgroundRefresh("key", failingReader(), 30_000);

    assertThat(syncExpire.getRefreshFailureLeased()).isZero();
    assertThat(syncExpire.leasedStaleness(10)).doesNotContainKey("key");
    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(entry).isNotNull();
    assertThat(entry.getSoftExpireAtMs())
      .as("a capped lease closes the retry window at the hard deadline")
      .isEqualTo(entry.getHardExpireAtMs());
  }

  /**
   * The stale-debt view must keep the OLDEST episodes when more than 1024 keys are
   * leased: evicting the eldest would hide exactly the debt the view exists to expose.
   */
  @Test
  void leasedEpisodes_atCap_shouldKeepTheOldestAndRefuseNewKeys() {
    Cache<String, CacheEntry> largeCache = Caffeine.newBuilder().maximumSize(2048).build();
    EntryLifecycle largeLifecycle = new EntryLifecycleImpl(largeCache, ttlConfig, null);
    BackgroundRefresher largeRefresher = new DefaultBackgroundRefresher(largeCache, Runnable::run, largeLifecycle, 10);
    long expireAt = System.currentTimeMillis() + 300_000;
    for (int i = 0; i < 1025; i++) {
      String key = "k" + i;
      largeCache.put(key, hotEntry("v", 1, expireAt));
      largeRefresher.triggerBackgroundRefresh(key, failingReader(), 30_000);
    }

    assertThat(largeRefresher.leasedKeyCount()).isEqualTo(1024);
    assertThat(largeRefresher.leasedStaleness(2000)).containsKey("k0");
    assertThat(largeRefresher.leasedStaleness(2000)).doesNotContainKey("k1024");
  }

  /**
   * Verifies that a version advance landing during the in-flight refresh
   * (a write) suppresses the lease — the guard treats it as a different
   * logical entry (design decisions: expiry and refresh identity guard).
   */
  @Test
  void triggerBackgroundRefresh_withSupplierError_shouldNotLeaseWhenVersionAdvanced() throws InterruptedException {
    ExecutorService asyncExec = Executors.newCachedThreadPool();
    BackgroundRefresher asyncExpire = new DefaultBackgroundRefresher(caffeineCache, asyncExec, lifecycle, 10);
    try {
      long originalExpire = System.currentTimeMillis() + 300_000;
      caffeineCache.put("key", hotEntry("original", 5, originalExpire));

      asyncExpire.triggerBackgroundRefresh(
        "key",
        () -> {
          caffeineCache
            .asMap()
            .computeIfPresent("key", (k, existing) -> {
              if (existing != null) {
                return EntryDraft.of(existing).version(10).build();
              }
              return null;
            });
          throw new RuntimeException("refresh-failed");
        },
        30_000
      );
      Thread.sleep(200);

      CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
      assertThat(entry).isNotNull();
      assertThat(entry.getDataVersion()).isEqualTo(10);
      assertThat(entry.getHardExpireAtMs()).isEqualTo(originalExpire);
    } finally {
      asyncExec.shutdownNow();
    }
  }

  /**
   * Verifies that an in-flight entry rewrite with fresh TTLs (simulating a
   * Worker rebroadcast, design decisions: state machine) suppresses the lease — the new hard expiry
   * is never shortened (design decisions: expiry and refresh identity guard on hardExpireAtMs).
   */
  @Test
  void triggerBackgroundRefresh_withSupplierError_shouldNotLeaseWhenEntryRewritten() throws InterruptedException {
    ExecutorService asyncExec = Executors.newCachedThreadPool();
    BackgroundRefresher asyncExpire = new DefaultBackgroundRefresher(caffeineCache, asyncExec, lifecycle, 10);
    try {
      long originalExpire = System.currentTimeMillis() + 300_000;
      caffeineCache.put("key", hotEntry("original", 5, originalExpire));
      long rewrittenExpire = System.currentTimeMillis() + 60_000;

      asyncExpire.triggerBackgroundRefresh(
        "key",
        () -> {
          caffeineCache
            .asMap()
            .computeIfPresent("key", (k, existing) -> {
              if (existing != null) {
                return EntryDraft.of(existing, ttlPolicy)
                  .ttl(60_000, existing.getSoftTtlMs())
                  .hardExpiryAt(rewrittenExpire)
                  .softExpiryAt(existing.getSoftExpireAtMs())
                  .build();
              }
              return null;
            });
          throw new RuntimeException("refresh-failed");
        },
        30_000
      );
      Thread.sleep(200);

      CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
      assertThat(entry).isNotNull();
      assertThat(entry.getHardExpireAtMs()).isEqualTo(rewrittenExpire);
    } finally {
      asyncExec.shutdownNow();
    }
  }

  /**
   * Verifies that an entry evicted during the in-flight refresh is never
   * recreated on the failure path — the lease has no new value to write.
   */
  @Test
  void triggerBackgroundRefresh_withSupplierError_shouldNotRecreateEvictedEntry() throws InterruptedException {
    Executor asyncExec = Executors.newCachedThreadPool();
    BackgroundRefresher asyncExpire = new DefaultBackgroundRefresher(caffeineCache, asyncExec, lifecycle, 10);
    try {
      caffeineCache.put("key", hotEntry("original", 1, System.currentTimeMillis() + 300_000));

      asyncExpire.triggerBackgroundRefresh(
        "key",
        () -> {
          caffeineCache.invalidate("key");
          throw new RuntimeException("refresh-failed");
        },
        30_000
      );
      Thread.sleep(200);

      assertThat(caffeineCache.getIfPresent("key")).isNull();
    } finally {
      ((ExecutorService) asyncExec).shutdownNow();
    }
  }

  /**
   * Verifies that a failed refresh with a real async executor and the
   * new refresh-timeout wrapper works normally — the value is updated on success.
   */
  @Test
  void triggerBackgroundRefresh_async_withRefreshTimeoutEnabled_shouldWorkNormally() throws InterruptedException {
    ExecutorService asyncExec = Executors.newCachedThreadPool();
    try {
      BackgroundRefresher asyncExpire = new DefaultBackgroundRefresher(caffeineCache, asyncExec, lifecycle, 10);

      caffeineCache.put(
        "key",
        EntryDraft.blank(null)
          .value("original")
          .version(1)
          .decision(DecisionStamp.hot(0, null, 0L))
          .ttl(300_000, 30_000, 300_000, 30_000)
          .expiryAt(Long.MAX_VALUE, System.currentTimeMillis() + 30_000)
          .keyState(KeyState.HOT)
          .build()
      );

      asyncExpire.triggerBackgroundRefresh("key", () -> new VersionedValue("updated", 0L), 30_000);
      Thread.sleep(200);

      CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
      assertThat(entry).isNotNull();
      assertThat((Object) entry.getValue()).isEqualTo("updated");
    } finally {
      asyncExec.shutdownNow();
    }
  }

  private static Supplier<?> failingReader() {
    return () -> {
      throw new RuntimeException("refresh-failed");
    };
  }

  /**
   * The refresh's own snapshot lookup is internal bookkeeping, not a caller
   * read: it must not add a cache hit, or every soft-expired read would be
   * counted twice and the reported hit rate would drift upward with refresh
   * volume.
   */
  @Test
  void triggerBackgroundRefresh_shouldNotRecordCacheHits() {
    Cache<String, CacheEntry> statsCache = Caffeine.newBuilder().maximumSize(10).recordStats().build();
    EntryLifecycle statsLifecycle = new EntryLifecycleImpl(statsCache, ttlConfig, null);
    BackgroundRefresher statsRefresher = new DefaultBackgroundRefresher(statsCache, Runnable::run, statsLifecycle, 10);
    statsCache.put("key", hotEntry("old", 1, System.currentTimeMillis() + 300_000));

    long hitsBefore = statsCache.stats().hitCount();
    statsRefresher.triggerBackgroundRefresh("key", () -> new VersionedValue("fresh", 1L), 30_000);

    assertThat(statsCache.stats().hitCount()).isEqualTo(hitsBefore);
  }

  @Test
  void applyRefresh_discardsRebuildWhenInvalidatedDuringRefresh() {
    String key = "refresh-invalidated";
    refresher.triggerBackgroundRefresh(
      key,
      () -> {
        // An INVALIDATE / clearAll lands after the value was read but before
        // the apply. The absent-entry branch is then indistinguishable from a
        // capacity eviction by structure alone — only the tombstone tells them
        // apart, and it must win.
        lifecycle.recordInvalidation(key, 2L);
        return new VersionedValue("stale", 1L);
      },
      30_000L
    );

    assertThat(caffeineCache.getIfPresent(key)).isNull();
  }

  /** Build a Worker-managed HOT entry with the given value, version and hard expiry. */
  private static CacheEntry hotEntry(Object value, long dataVersion, long hardExpireAtMs) {
    return EntryDraft.blank(null)
      .value(value)
      .version(dataVersion)
      .decision(DecisionStamp.hot(1, null, 0L))
      .ttl(300_000, 30_000, 300_000, 30_000)
      .expiryAt(hardExpireAtMs, hardExpireAtMs == Long.MAX_VALUE ? System.currentTimeMillis() + 30_000 : hardExpireAtMs)
      .keyState(KeyState.HOT)
      .build();
  }
}
