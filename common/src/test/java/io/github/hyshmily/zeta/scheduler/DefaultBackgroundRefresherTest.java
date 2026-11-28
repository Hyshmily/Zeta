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
import io.github.hyshmily.zeta.util.TimeSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pins the soft-to-hard hand-off: a renewal reuses an in-flight soft load, and a soft settlement
 * that already claimed the ticket completes the renewal's future only after its write-back, so the
 * caller reschedules from the settled entry.
 */
class DefaultBackgroundRefresherTest {

  private static final String KEY = "users\u0000[J:42]";

  private Cache<String, CacheEntry> cache;
  private Queue<Runnable> submitted;
  private TtlPolicy ttlPolicy;
  private DefaultBackgroundRefresher refresher;

  @BeforeEach
  void setUp() {
    TimeSource.start();
    cache = Caffeine.newBuilder().build();
    ZetaProperties properties = new ZetaProperties();
    ttlPolicy = new TtlPolicy(properties);
    submitted = new ConcurrentLinkedQueue<>();
    Executor executor = submitted::add;
    refresher = new DefaultBackgroundRefresher(
      cache,
      executor,
      new EntryLifecycleImpl(cache, properties),
      1,
      properties.getRefreshTimeoutMs()
    );
    cache.put(KEY, EntryDraft.blank(ttlPolicy).value("old").version(1L).ttl(60_000L, 30_000L).build());
  }

  @Test
  void renewHard_shouldUpgradeAnInFlightSoftRefresh() {
    refresher.triggerBackgroundRefresh(KEY, () -> new VersionedValue("fresh", 2L), 30_000L);

    var renewal = refresher.renewHard(KEY, () -> new VersionedValue("hard", 3L));
    runSubmitted();

    assertThat(renewal.join()).isInstanceOf(BackgroundRefresher.RenewOutcome.Applied.class);
    assertThat(cache.getIfPresent(KEY).getDataVersion()).isEqualTo(2L);
  }

  @Test
  void renewHard_shouldWaitForATicketAlreadySettlingSoft() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicBoolean blockOnce = new AtomicBoolean(true);
    EntryLifecycle gate = blockingLifecycle(entered, release, blockOnce);
    DefaultBackgroundRefresher gated = new DefaultBackgroundRefresher(cache, submitted::add, gate, 1, 30_000L);
    long hardExpireAtMs = cache.getIfPresent(KEY).getHardExpireAtMs();

    gated.triggerBackgroundRefresh(KEY, () -> new VersionedValue("fresh", 2L), 30_000L);
    Runnable task = submitted.poll();
    Thread soft = new Thread(task);
    soft.start();
    assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

    var renewal = gated.renewHard(KEY, () -> new VersionedValue("hard", 3L));

    // The soft settlement owns the ticket; the renewal's future must not complete before that
    // write-back lands, or its caller would reschedule from the pre-refresh entry.
    assertThat(renewal.isDone()).isFalse();
    release.countDown();
    soft.join();

    assertThat(renewal.join()).isSameAs(BackgroundRefresher.RenewOutcome.Skipped.SOFT_REFRESH);
    assertThat(cache.getIfPresent(KEY).getDataVersion()).isEqualTo(2L);
    assertThat(cache.getIfPresent(KEY).getHardExpireAtMs()).isEqualTo(hardExpireAtMs);
    assertThat(gated.getRefreshLimiter().availablePermits()).isEqualTo(1);
  }

  @Test
  void renewHard_shouldApplyWhenTheResidentExpiresDuringTheLoad() throws Exception {
    long deadline = TimeSource.monotonicMillis() + 20L;
    cache.put(
      KEY,
      EntryDraft.blank(ttlPolicy).value("old").version(1L).ttl(60_000L, 30_000L).hardExpiryAt(deadline).build()
    );

    var renewal = refresher.renewHard(KEY, () -> new VersionedValue("fresh", 1L));
    Runnable task = submitted.poll();
    assertThat(task).isNotNull();
    // The source outlives the deadline, exactly when the renewal is the last line of defence.
    sleepPast(deadline);
    task.run();

    assertThat(renewal.join()).isInstanceOf(BackgroundRefresher.RenewOutcome.Applied.class);
    CacheEntry resident = cache.getIfPresent(KEY);
    assertThat(resident.getValue()).isEqualTo("fresh");
    assertThat(resident.getHardExpireAtMs()).isGreaterThan(deadline);
  }

  @Test
  void softRefresh_shouldNotWritePastTheHardDeadline() throws Exception {
    long deadline = TimeSource.monotonicMillis() + 20L;
    cache.put(
      KEY,
      EntryDraft.blank(ttlPolicy).value("old").version(1L).ttl(60_000L, 30_000L).hardExpiryAt(deadline).build()
    );

    refresher.triggerBackgroundRefresh(KEY, () -> new VersionedValue("fresh", 1L), 30_000L);
    Runnable task = submitted.poll();
    assertThat(task).isNotNull();
    sleepPast(deadline);
    task.run();

    CacheEntry resident = cache.getIfPresent(KEY);
    assertThat(resident.getValue()).isEqualTo("old");
    assertThat(resident.getHardExpireAtMs()).isEqualTo(deadline);
  }

  @Test
  void softRefresh_shouldNotRebuildAnEvictedResident() {
    refresher.triggerBackgroundRefresh(KEY, () -> new VersionedValue("fresh", 2L), 30_000L);
    Runnable task = submitted.poll();
    assertThat(task).isNotNull();

    cache.invalidate(KEY);
    task.run();

    assertThat(cache.getIfPresent(KEY)).isNull();
  }

  @Test
  void renewHard_shouldNotSpendAPermitOnAnAbsentResident() {
    cache.invalidate(KEY);

    var renewal = refresher.renewHard(KEY, () -> new VersionedValue("fresh", 1L));

    assertThat(renewal.join()).isSameAs(BackgroundRefresher.RenewOutcome.Skipped.ABSENT_OR_EXPIRED);
    assertThat(refresher.getRefreshLimiter().availablePermits()).isEqualTo(1);
    assertThat(submitted).isEmpty();
  }

  @Test
  void renewHard_shouldReportTheLoaderFailureUnwrapped() {
    var renewal = refresher.renewHard(KEY, () -> {
      throw new IllegalStateException("boom");
    });
    Runnable task = submitted.poll();
    assertThat(task).isNotNull();
    task.run();

    BackgroundRefresher.RenewOutcome outcome = renewal.join();
    assertThat(outcome).isInstanceOf(BackgroundRefresher.RenewOutcome.Failed.class);
    assertThat(((BackgroundRefresher.RenewOutcome.Failed) outcome).cause())
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("boom");
    assertThat(refresher.getRefreshLimiter().availablePermits()).isEqualTo(1);
  }

  /** Sleeps until the monotonic deadline has passed, plus a small scheduling margin. */
  private static void sleepPast(long deadlineMonoMs) throws InterruptedException {
    long waitMs = deadlineMonoMs - TimeSource.monotonicMillis() + 5L;
    if (waitMs > 0L) {
      Thread.sleep(waitMs);
    }
  }

  private EntryLifecycle blockingLifecycle(CountDownLatch entered, CountDownLatch release, AtomicBoolean blockOnce) {
    EntryLifecycle delegate = new EntryLifecycleImpl(cache, new ZetaProperties());
    return (EntryLifecycle) Proxy.newProxyInstance(
      getClass().getClassLoader(),
      new Class<?>[] { EntryLifecycle.class },
      (proxy, method, args) -> {
        if (method.getName().equals("staleAgainstInvalidation") && blockOnce.compareAndSet(true, false)) {
          entered.countDown();
          release.await(5, TimeUnit.SECONDS);
        }
        try {
          return method.invoke(delegate, args);
        } catch (InvocationTargetException e) {
          throw e.getCause();
        }
      }
    );
  }

  private void runSubmitted() {
    Runnable task = submitted.poll();
    assertThat(task).isNotNull();
    task.run();
  }
}
