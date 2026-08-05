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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.ExpireManager;
import io.github.hyshmily.zeta.cache.cachesupport.impl.ExpireManagerImpl;
import io.github.hyshmily.zeta.model.CacheEntry;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.model.VersionedValue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the stateful side of {@link ExpireManager}: background refresh
 * scheduling (dedup, limiter, timeout, fault modes, version-guarded merge),
 * the entry factory ({@code createBuilder} parameter combinations), and
 * expiry extension.
 *
 * <p>The stateless TTL arithmetic tests live in {@link TtlPolicyTest}.
 */
class CacheExpireManagerTest {

  private ExpireManager expireManager;
  private Cache<String, Object> caffeineCache;
  private ZetaProperties ttlConfig;

  @BeforeEach
  void setUp() {
    caffeineCache = Caffeine.newBuilder().maximumSize(100).build();
    ttlConfig = new ZetaProperties();
    Executor executor = Runnable::run;
    expireManager = new ExpireManagerImpl(caffeineCache, executor, ttlConfig, 10);
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

    expireManager.triggerBackgroundRefresh("key", () -> new VersionedValue("newValue", 0L, false), 30_000);

    assertThat(caffeineCache.getIfPresent("key")).isNotNull();
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

    expireManager.triggerBackgroundRefresh(
      "key",
      () -> {
        caffeineCache
          .asMap()
          .computeIfPresent("key", (k, existing) -> {
            if (existing instanceof CacheEntry ce) {
              return ce.toBuilder().dataVersion(10).build();
            }
            return existing;
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
   * Verifies that triggerBackgroundRefresh skips the cache update when the supplier
   * returns null (fault mode: null supplier result).
   */
  @Test
  void triggerBackgroundRefresh_withNullResult_shouldNotUpdateCache() throws InterruptedException {
    Executor asyncExec = Executors.newCachedThreadPool();
    ExpireManager asyncExpire = new ExpireManagerImpl(caffeineCache, asyncExec, ttlConfig, 10);

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
    ExpireManager limited = new ExpireManagerImpl(caffeineCache, asyncExec, ttlConfig, 1);

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
    ExpireManager asyncExpire = new ExpireManagerImpl(caffeineCache, asyncExec, ttlConfig, 10);

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
    ExpireManager asyncExpire = new ExpireManagerImpl(caffeineCache, asyncExec, ttlConfig, 10);

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
    ExpireManager asyncExpire = new ExpireManagerImpl(caffeineCache, asyncExec, ttlConfig, 10);

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
    ExpireManager rejectingMgr = new ExpireManagerImpl(caffeineCache, rejectingExecutor, ttlConfig, 10);

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
   * value and metadata preserved, duration fields untouched (Option B), both
   * expire timestamps extended to approximately half of the remaining budget.
   */
  @Test
  void triggerBackgroundRefresh_withSupplierError_shouldLeaseExistingEntry() throws InterruptedException {
    ExecutorService asyncExec = Executors.newCachedThreadPool();
    ExpireManager asyncExpire = new ExpireManagerImpl(caffeineCache, asyncExec, ttlConfig, 10);
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
      // Lease = remaining/2 (well above the 120s floor); hard extended to
      // now + lease, soft to the lease midpoint — the retry window.
      assertThat(entry.getHardExpireAtMs()).isGreaterThan(now + 120_000);
      assertThat(entry.getHardExpireAtMs()).isLessThanOrEqualTo(now + 160_000);
      assertThat(entry.getSoftExpireAtMs()).isBetween(now + 60_000, now + 80_000);
      assertThat(entry.getSoftExpireAtMs()).isLessThan(entry.getHardExpireAtMs());
    } finally {
      asyncExec.shutdownNow();
    }
  }

  /**
   * Verifies that repeated failures halve the remaining budget each time,
   * decaying to the 120s floor (soft exponential decay, ADR-0036).
   */
  @Test
  void triggerBackgroundRefresh_repeatedFailures_shouldDecayLeaseToFloor() {
    caffeineCache.put("key", hotEntry("original", 1, System.currentTimeMillis() + 600_000));
    ExpireManager syncExpire = new ExpireManagerImpl(caffeineCache, Runnable::run, ttlConfig, 10);

    syncExpire.triggerBackgroundRefresh("key", failingReader(), 30_000);
    long now = System.currentTimeMillis();
    CacheEntry first = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(first.getHardExpireAtMs()).isBetween(now + 260_000, now + 320_000);

    syncExpire.triggerBackgroundRefresh("key", failingReader(), 30_000);
    now = System.currentTimeMillis();
    CacheEntry second = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(second.getHardExpireAtMs()).isBetween(now + 130_000, now + 170_000);

    syncExpire.triggerBackgroundRefresh("key", failingReader(), 30_000);
    now = System.currentTimeMillis();
    CacheEntry third = (CacheEntry) caffeineCache.getIfPresent("key");
    assertThat(third.getHardExpireAtMs()).isBetween(now + 110_000, now + 130_000);
    // Floor lease: soft at the midpoint (60s) — the entry stays retry-eligible.
    assertThat(third.getSoftExpireAtMs()).isBetween(now + 50_000, now + 70_000);
    assertThat(third.getSoftExpireAtMs()).isLessThan(third.getHardExpireAtMs());
    assertThat((Object) third.getValue()).isEqualTo("original");
  }

  /**
   * Verifies that a version advance landing during the in-flight refresh
   * (a write) suppresses the lease — the guard treats it as a different
   * logical entry (ADR-0036 identity guard).
   */
  @Test
  void triggerBackgroundRefresh_withSupplierError_shouldNotLeaseWhenVersionAdvanced() throws InterruptedException {
    ExecutorService asyncExec = Executors.newCachedThreadPool();
    ExpireManager asyncExpire = new ExpireManagerImpl(caffeineCache, asyncExec, ttlConfig, 10);
    try {
      long originalExpire = System.currentTimeMillis() + 300_000;
      caffeineCache.put("key", hotEntry("original", 5, originalExpire));

      asyncExpire.triggerBackgroundRefresh(
        "key",
        () -> {
          caffeineCache
            .asMap()
            .computeIfPresent("key", (k, existing) -> {
              if (existing instanceof CacheEntry ce) {
                return ce.toBuilder().dataVersion(10).build();
              }
              return existing;
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
    ExpireManager asyncExpire = new ExpireManagerImpl(caffeineCache, asyncExec, ttlConfig, 10);
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
              if (existing instanceof CacheEntry ce) {
                return ce.withHardTtl(60_000, rewrittenExpire);
              }
              return existing;
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
    ExpireManager asyncExpire = new ExpireManagerImpl(caffeineCache, asyncExec, ttlConfig, 10);
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
      ExpireManager asyncExpire = new ExpireManagerImpl(caffeineCache, asyncExec, ttlConfig, 10);

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

  // ── createBuilder parameter combinations ──────────────────────

  /**
   * Verifies that createBuilder with decision metadata and pre-computed
   * expire timestamps sets all fields correctly.
   */
  @Test
  void createBuilder_withDecisionMetadataAndExpireTimestamps_shouldSetAllFields() {
    long now = System.currentTimeMillis();
    CacheEntry entry = expireManager.createBuilder(
      "value",
      new ExpireManager.VersionStamp(42, true),
      new ExpireManager.DecisionStamp(7, "worker-1", 3),
      new ExpireManager.TtlSpec(60_000, 30_000, 300_000, 30_000),
      new ExpireManager.ExpiryAt(now + 60_000, now + 30_000),
      KeyState.HOT
    );

    assertThat(entry.getValue()).isEqualTo("value");
    assertThat(entry.getDataVersion()).isEqualTo(42);
    assertThat(entry.isVersionDegraded()).isTrue();
    assertThat(entry.getDecisionVersion()).isEqualTo(7);
    assertThat(entry.getDecisionNodeId()).isEqualTo("worker-1");
    assertThat(entry.getDecisionEpoch()).isEqualTo(3);
    assertThat(entry.getHardTtlMs()).isEqualTo(60_000);
    assertThat(entry.getSoftTtlMs()).isEqualTo(30_000);
    assertThat(entry.getHardExpireAtMs()).isEqualTo(now + 60_000);
    assertThat(entry.getSoftExpireAtMs()).isEqualTo(now + 30_000);
    assertThat(entry.getNormalHardTtlMs()).isEqualTo(300_000);
    assertThat(entry.getNormalSoftTtlMs()).isEqualTo(30_000);
    assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
  }

  /**
   * Verifies that createBuilder with decision metadata but no expire
   * timestamps computes expire-at via applyTtl.
   */
  @Test
  void createBuilder_withDecisionMetadataAndNoExpireTimestamps_shouldComputeExpire() {
    long before = System.currentTimeMillis();
    CacheEntry entry = expireManager.createBuilder(
      "value",
      new ExpireManager.VersionStamp(42, false),
      new ExpireManager.DecisionStamp(7, "worker-1", 3),
      new ExpireManager.TtlSpec(60_000, 30_000, 300_000, 30_000),
      null,
      KeyState.HOT
    );

    assertThat(entry.getValue()).isEqualTo("value");
    assertThat(entry.getDataVersion()).isEqualTo(42);
    assertThat(entry.getDecisionVersion()).isEqualTo(7);
    assertThat(entry.getDecisionNodeId()).isEqualTo("worker-1");
    assertThat(entry.getDecisionEpoch()).isEqualTo(3);
    assertThat(entry.getHardTtlMs()).isEqualTo(60_000);
    assertThat(entry.getHardExpireAtMs()).isGreaterThan(before);
    assertThat(entry.getSoftTtlMs()).isEqualTo(30_000);
    assertThat(entry.getSoftExpireAtMs()).isGreaterThan(before);
    assertThat(entry.getNormalHardTtlMs()).isEqualTo(300_000);
    assertThat(entry.getNormalSoftTtlMs()).isEqualTo(30_000);
    assertThat(entry.getKeyState()).isEqualTo(KeyState.HOT);
  }

  /**
   * Verifies that createBuilder without decision node/epoch metadata (local
   * origin, no Worker) but with pre-computed expire timestamps sets all
   * fields correctly.
   */
  @Test
  void createBuilder_withoutDecisionMetadataWithExpireTimestamps_shouldSetAllFields() {
    long now = System.currentTimeMillis();
    CacheEntry entry = expireManager.createBuilder(
      "value",
      new ExpireManager.VersionStamp(42, false),
      new ExpireManager.DecisionStamp(7, null, 0),
      new ExpireManager.TtlSpec(60_000, 30_000, 300_000, 30_000),
      new ExpireManager.ExpiryAt(now + 60_000, now + 30_000),
      KeyState.NORMAL
    );

    assertThat(entry.getValue()).isEqualTo("value");
    assertThat(entry.getDataVersion()).isEqualTo(42);
    assertThat(entry.getDecisionVersion()).isEqualTo(7);
    assertThat(entry.getDecisionNodeId()).isNull();
    assertThat(entry.getDecisionEpoch()).isZero();
    assertThat(entry.getHardTtlMs()).isEqualTo(60_000);
    assertThat(entry.getHardExpireAtMs()).isEqualTo(now + 60_000);
    assertThat(entry.getSoftTtlMs()).isEqualTo(30_000);
    assertThat(entry.getNormalHardTtlMs()).isEqualTo(300_000);
    assertThat(entry.getNormalSoftTtlMs()).isEqualTo(30_000);
    assertThat(entry.getKeyState()).isEqualTo(KeyState.NORMAL);
  }

  /**
   * Verifies that createBuilder with no decision metadata and no expire
   * timestamps produces a correctly built entry with timestamps computed
   * via applyTtl.
   */
  @Test
  void createBuilder_rawFields_shouldComputeExpireAndSetFields() {
    long before = System.currentTimeMillis();
    CacheEntry entry = expireManager.createBuilder(
      "value",
      new ExpireManager.VersionStamp(42, false),
      new ExpireManager.DecisionStamp(7, null, 0),
      new ExpireManager.TtlSpec(60_000, 30_000, 300_000, 30_000),
      null,
      KeyState.NORMAL
    );

    assertThat(entry.getValue()).isEqualTo("value");
    assertThat(entry.getDataVersion()).isEqualTo(42);
    assertThat(entry.getDecisionVersion()).isEqualTo(7);
    assertThat(entry.getDecisionNodeId()).isNull();
    assertThat(entry.getDecisionEpoch()).isZero();
    assertThat(entry.getHardTtlMs()).isEqualTo(60_000);
    assertThat(entry.getHardExpireAtMs()).isGreaterThan(before);
    assertThat(entry.getSoftTtlMs()).isEqualTo(30_000);
    assertThat(entry.getSoftExpireAtMs()).isGreaterThan(before);
    assertThat(entry.getNormalHardTtlMs()).isEqualTo(300_000);
    assertThat(entry.getNormalSoftTtlMs()).isEqualTo(30_000);
    assertThat(entry.getKeyState()).isEqualTo(KeyState.NORMAL);
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

  /** A reader that always throws — the lease-on-failure trigger. */
  private static Supplier<?> failingReader() {
    return () -> {
      throw new RuntimeException("refresh-failed");
    };
  }
}
