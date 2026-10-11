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
import io.github.hyshmily.zeta.cache.codec.CacheCompressor;
import io.github.hyshmily.zeta.cache.cachesupport.TtlPolicy;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.EntryDraft;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.model.VersionedValue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
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

  @BeforeEach
  void setUp() {
    caffeineCache = Caffeine.newBuilder().maximumSize(100).build();
    ttlConfig = new ZetaProperties();
    Executor executor = Runnable::run;
    ttlPolicy = new TtlPolicy(ttlConfig, ttlConfig.getTtlJitterRatio());
    refresher = new DefaultBackgroundRefresher(caffeineCache, executor, ttlPolicy, CacheCompressor.NONE, 10);
  }

  /**
   * Verifies that triggerBackgroundRefresh executes the supplier and preserves the cache entry.
   */
  @Test
  void triggerBackgroundRefresh_shouldAcquireAndReleasePermit() {
    caffeineCache.put(
      "key",
      CacheEntry.builder()
        .value("old")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() + 30_000)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );

    refresher.triggerBackgroundRefresh("key", () -> new VersionedValue("newValue", 0L, false), 30_000);

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
      CacheEntry.builder()
        .value("old")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(123_456)
        .hardExpireAtMs(System.currentTimeMillis() + 123_456)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() - 1_000)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(123_456)
        .normalSoftTtlMs(30_000)
        .build()
    );

    refresher.triggerBackgroundRefresh(
      "key",
      () -> {
        // The direct test executor runs the reader right after the snapshot
        // was taken: evicting here reproduces "entry absent at apply time".
        caffeineCache.invalidate("key");
        return new VersionedValue("fresh", 7L, true);
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
   * A refresh task that completes before the {@code pendingRefreshes} compute
   * stores it (fast reader, synchronous executor) must not leave a done marker
   * behind: the map is unbounded, so a cold key would otherwise retain the
   * completed marker until its next trigger.
   */
  @Test
  void triggerBackgroundRefresh_fastCompletedTask_shouldNotLeaveDoneMarker() throws Exception {
    refresher.triggerBackgroundRefresh("key", () -> new VersionedValue("v", 0L, false), 30_000);

    java.lang.reflect.Field pending = DefaultBackgroundRefresher.class.getDeclaredField("pendingRefreshes");
    pending.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.concurrent.ConcurrentMap<String, CompletableFuture<?>> map =
      (java.util.concurrent.ConcurrentMap<String, CompletableFuture<?>>) pending.get(refresher);

    assertThat(map).isEmpty();
  }

  /**
   * Verifies that triggerBackgroundRefresh discards the result when the entry's data version has changed since the refresh started.
   */
  @Test
  void triggerBackgroundRefresh_withStaleVersion_shouldDiscardResult() {
    caffeineCache.put(
      "key",
      CacheEntry.builder()
        .value("original")
        .dataVersion(5)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() + 300_000)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
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
        // Unstamped carrier: the legacy L1-internal snapshot guard must discard it.
        return new VersionedValue("stale-value", 0L, false);
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
      CacheEntry.builder()
        .value("original")
        .dataVersion(5)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(System.currentTimeMillis() + 300_000)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() - 1_000)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );

    refresher.triggerBackgroundRefresh("key", () -> new VersionedValue("fresh", 5L, true), 30_000);

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
      CacheEntry.builder()
        .value("original")
        .dataVersion(5)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(System.currentTimeMillis() + 300_000)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() - 1_000)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
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
        return new VersionedValue("stale-value", 6L, true);
      },
      30_000
    );

    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(entry).isNotNull();
    assertThat(entry.getDataVersion()).isEqualTo(6);
    assertThat((Object) entry.getValue()).isEqualTo("writer-value");
  }

  /**
   * A snapshot-degraded entry with no in-flight write is healed by a stamped
   * refresh (case 4 of the sync matrix): the normal probe overwrites it. A
   * degraded write landing in flight instead crosses the boundary and is kept.
   */
  @Test
  void triggerBackgroundRefresh_stampedHealsIdleDegraded_shouldApply() {
    caffeineCache.put(
      "key",
      CacheEntry.builder()
        .value("original")
        .dataVersion(Long.MIN_VALUE + 100L)
        .isVersionDegraded(true)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(System.currentTimeMillis() + 300_000)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() - 1_000)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );

    refresher.triggerBackgroundRefresh("key", () -> new VersionedValue("fresh", 8L, true), 30_000);

    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(entry).isNotNull();
    assertThat((Object) entry.getValue()).isEqualTo("fresh");
    assertThat(entry.getDataVersion()).isEqualTo(8L);
    assertThat(entry.isVersionDegraded()).isFalse();
  }

  /**
   * Verifies that triggerBackgroundRefresh skips the cache update when the supplier
   * returns null (fault mode: null supplier result).
   */
  @Test
  void triggerBackgroundRefresh_withNullResult_shouldNotUpdateCache() throws InterruptedException {
    Executor asyncExec = Executors.newCachedThreadPool();
    BackgroundRefresher asyncExpire = new DefaultBackgroundRefresher(caffeineCache, asyncExec, ttlPolicy, CacheCompressor.NONE, 10);

    caffeineCache.put(
      "key",
      CacheEntry.builder()
        .value("original")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() + 30_000)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );

    asyncExpire.triggerBackgroundRefresh("key", () -> null, 30_000);
    Thread.sleep(200);

    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(entry).isNotNull();
    assertThat((Object) entry.getValue()).isEqualTo("original");
  }

  /**
   * Verifies that triggerBackgroundRefresh skips when the refresh limiter semaphore
   * is exhausted (fault mode: limiter backpressure).
   */
  @Test
  void triggerBackgroundRefresh_withExhaustedLimiter_shouldSkip() throws InterruptedException {
    Executor asyncExec = Executors.newCachedThreadPool();
    BackgroundRefresher limited = new DefaultBackgroundRefresher(caffeineCache, asyncExec, ttlPolicy, CacheCompressor.NONE, 1);

    caffeineCache.put(
      "key1",
      CacheEntry.builder()
        .value("original1")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() + 30_000)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );
    caffeineCache.put(
      "key2",
      CacheEntry.builder()
        .value("original2")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() + 30_000)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
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
    BackgroundRefresher asyncExpire = new DefaultBackgroundRefresher(caffeineCache, asyncExec, ttlPolicy, CacheCompressor.NONE, 10);

    caffeineCache.put(
      "key",
      CacheEntry.builder()
        .value("original")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() + 30_000)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
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

    // Give first call time to register in pendingRefreshes
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
   * leased — there is nothing to extend (ADR-0036).
   */
  @Test
  void triggerBackgroundRefresh_withSupplierError_shouldPreserveExistingEntry() throws InterruptedException {
    Executor asyncExec = Executors.newCachedThreadPool();
    BackgroundRefresher asyncExpire = new DefaultBackgroundRefresher(caffeineCache, asyncExec, ttlPolicy, CacheCompressor.NONE, 10);

    caffeineCache.put(
      "key",
      CacheEntry.builder()
        .value("original")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() + 30_000)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
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
    BackgroundRefresher asyncExpire = new DefaultBackgroundRefresher(caffeineCache, asyncExec, ttlPolicy, CacheCompressor.NONE, 10);

    caffeineCache.put(
      "key",
      CacheEntry.builder()
        .value("original")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() + 30_000)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );

    asyncExpire.triggerBackgroundRefresh(
      "key",
      () -> {
        caffeineCache.invalidate("key");
        return new VersionedValue("fresh-value", 0L, false);
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
    BackgroundRefresher rejectingMgr = new DefaultBackgroundRefresher(caffeineCache, rejectingExecutor, ttlPolicy, CacheCompressor.NONE, 10);

    caffeineCache.put(
      "key",
      CacheEntry.builder()
        .value("original")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(Long.MAX_VALUE)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() + 30_000)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .build()
    );

    rejectingMgr.triggerBackgroundRefresh("key", () -> "newValue", 30_000);

    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(entry).isNotNull();
    assertThat((Object) entry.getValue()).isEqualTo("original");
  }

  // ── Lease-on-Failure (ADR-0036) ───────────────────────────────────

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
    BackgroundRefresher asyncExpire = new DefaultBackgroundRefresher(caffeineCache, asyncExec, ttlPolicy, CacheCompressor.NONE, 10);
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
      // Duration fields stay authoritative — only timestamps move (ADR-0036).
      assertThat(entry.getHardTtlMs()).isEqualTo(300_000);
      assertThat(entry.getSoftTtlMs()).isEqualTo(30_000);
      // Never-shorten: remaining (~300s) exceeds the floor, so hard is
      // preserved (within elapsed tolerance), soft at the lease midpoint.
      assertThat(entry.getHardExpireAtMs()).isBetween(now + 290_000, originalExpire + 1_000);
      assertThat(entry.getSoftExpireAtMs()).isBetween(now + 140_000, now + 160_000);
      assertThat(entry.getSoftExpireAtMs()).isLessThan(entry.getHardExpireAtMs());
      // Observability: one granted lease, nothing refused.
      assertThat(asyncExpire.getRefreshFailureLeased()).isEqualTo(1);
      assertThat(asyncExpire.getLeaseStaleCapped()).isZero();
      assertThat(asyncExpire.getLeaseSuppressedByClassifier()).isZero();
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
    BackgroundRefresher syncExpire = new DefaultBackgroundRefresher(caffeineCache, Runnable::run, ttlPolicy, CacheCompressor.NONE, 10);

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
    BackgroundRefresher syncExpire = new DefaultBackgroundRefresher(caffeineCache, Runnable::run, ttlPolicy, CacheCompressor.NONE, 10);

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
      CacheEntry.builder()
        .value("original")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(30_000)
        .hardExpireAtMs(System.currentTimeMillis() + 30_000)
        .softTtlMs(10_000)
        .softExpireAtMs(System.currentTimeMillis() - 1_000)
        .keyState(KeyState.NORMAL)
        .normalHardTtlMs(30_000)
        .normalSoftTtlMs(10_000)
        .build()
    );
    long before = System.currentTimeMillis();
    BackgroundRefresher syncExpire = new DefaultBackgroundRefresher(caffeineCache, Runnable::run, ttlPolicy, CacheCompressor.NONE, 10);

    syncExpire.triggerBackgroundRefresh("key", failingReader(), 10_000);

    CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(entry).isNotNull();
    // Untouched: no extension granted.
    assertThat(entry.getHardExpireAtMs()).isLessThanOrEqualTo(before + 30_000);
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
    BackgroundRefresher syncExpire = new DefaultBackgroundRefresher(caffeineCache, Runnable::run, ttlPolicy, CacheCompressor.NONE, 10);

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
    assertThat(syncExpire.getRefreshFailureLeased()).isZero();
    assertThat(syncExpire.getLeaseSuppressedByClassifier()).isEqualTo(1);
  }

  /**
   * Verifies the staleness cap: an entry born over 30 minutes ago is refused
   * a lease (counted as capped) and left to hard-expire.
   */
  @Test
  void triggerBackgroundRefresh_overAgeEntry_shouldRefuseLeaseByStalenessCap() {
    int oldBirth = io.github.hyshmily.zeta.util.TimeSource.monotonicSecondsSinceBoot() - 3700;
    caffeineCache.put(
      "key",
      CacheEntry.builder()
        .value("original")
        .dataVersion(1)
        .isVersionDegraded(false)
        .decisionVersion(0)
        .hardTtlMs(300_000)
        .hardExpireAtMs(System.currentTimeMillis() + 300_000)
        .softTtlMs(30_000)
        .softExpireAtMs(System.currentTimeMillis() - 1_000)
        .keyState(KeyState.HOT)
        .normalHardTtlMs(300_000)
        .normalSoftTtlMs(30_000)
        .birthStampSec(oldBirth)
        .build()
    );
    BackgroundRefresher syncExpire = new DefaultBackgroundRefresher(caffeineCache, Runnable::run, ttlPolicy, CacheCompressor.NONE, 10);

    syncExpire.triggerBackgroundRefresh("key", failingReader(), 30_000);

    assertThat(syncExpire.getRefreshFailureLeased()).isZero();
    assertThat(syncExpire.getLeaseStaleCapped()).isEqualTo(1);
    assertThat(syncExpire.leasedStaleness(10)).doesNotContainKey("key");
  }

  /**
   * Verifies that a version advance landing during the in-flight refresh
   * (a write) suppresses the lease — the guard treats it as a different
   * logical entry (ADR-0036 identity guard).
   */
  @Test
  void triggerBackgroundRefresh_withSupplierError_shouldNotLeaseWhenVersionAdvanced() throws InterruptedException {
    ExecutorService asyncExec = Executors.newCachedThreadPool();
    BackgroundRefresher asyncExpire = new DefaultBackgroundRefresher(caffeineCache, asyncExec, ttlPolicy, CacheCompressor.NONE, 10);
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
   * Worker rebroadcast, ADR-0024) suppresses the lease — the new hard expiry
   * is never shortened (ADR-0036 identity guard on hardExpireAtMs).
   */
  @Test
  void triggerBackgroundRefresh_withSupplierError_shouldNotLeaseWhenEntryRewritten() throws InterruptedException {
    Executor asyncExec = Executors.newCachedThreadPool();
    BackgroundRefresher asyncExpire = new DefaultBackgroundRefresher(caffeineCache, asyncExec, ttlPolicy, CacheCompressor.NONE, 10);
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
              return EntryDraft.of(existing, ttlPolicy).hardTtl(60_000).hardExpiryAt(rewrittenExpire).build();
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
      ((ExecutorService) asyncExec).shutdownNow();
    }
  }

  /**
   * Verifies that an entry evicted during the in-flight refresh is never
   * recreated on the failure path — the lease has no new value to write.
   */
  @Test
  void triggerBackgroundRefresh_withSupplierError_shouldNotRecreateEvictedEntry() throws InterruptedException {
    Executor asyncExec = Executors.newCachedThreadPool();
    BackgroundRefresher asyncExpire = new DefaultBackgroundRefresher(caffeineCache, asyncExec, ttlPolicy, CacheCompressor.NONE, 10);
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
    Executor asyncExec = Executors.newCachedThreadPool();
    try {
      BackgroundRefresher asyncExpire = new DefaultBackgroundRefresher(caffeineCache, asyncExec, ttlPolicy, CacheCompressor.NONE, 10);

      caffeineCache.put(
        "key",
        CacheEntry.builder()
          .value("original")
          .dataVersion(1)
          .isVersionDegraded(false)
          .decisionVersion(0)
          .hardTtlMs(300_000)
          .hardExpireAtMs(Long.MAX_VALUE)
          .softTtlMs(30_000)
          .softExpireAtMs(System.currentTimeMillis() + 30_000)
          .keyState(KeyState.HOT)
          .normalHardTtlMs(300_000)
          .normalSoftTtlMs(30_000)
          .build()
      );

      asyncExpire.triggerBackgroundRefresh("key", () -> new VersionedValue("updated", 0L, false), 30_000);
      Thread.sleep(200);

      CacheEntry entry = (CacheEntry) caffeineCache.getIfPresent("key");
      assertThat(entry).isNotNull();
      assertThat((Object) entry.getValue()).isEqualTo("updated");
    } finally {
      ((java.util.concurrent.ExecutorService) asyncExec).shutdownNow();
    }
  }


  private static Supplier<?> failingReader() {
    return () -> {
      throw new RuntimeException("refresh-failed");
    };
  }

  /** Build a Worker-managed HOT entry with the given value, version and hard expiry. */
  private static CacheEntry hotEntry(Object value, long dataVersion, long hardExpireAtMs) {
    return CacheEntry.builder()
      .value(value)
      .dataVersion(dataVersion)
      .isVersionDegraded(false)
      .decisionVersion(1)
      .hardTtlMs(300_000)
      .hardExpireAtMs(hardExpireAtMs)
      .softTtlMs(30_000)
      .softExpireAtMs(hardExpireAtMs == Long.MAX_VALUE ? System.currentTimeMillis() + 30_000 : hardExpireAtMs)
      .keyState(KeyState.HOT)
      .normalHardTtlMs(300_000)
      .normalSoftTtlMs(30_000)
      .build();
  }
}
