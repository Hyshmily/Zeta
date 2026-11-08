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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.circuitbreaker.CircuitBreakerRegistry;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycleImpl;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlightImpl;
import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.StalePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.scheduler.DefaultBackgroundRefresher;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.util.clock.MonotonicStampClock;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Converged SingleFlight lifecycle: the read path pins its dedup future and
 * drops it conditionally immediately after the L1 commit (L1 first, then dedup
 * removal), so a declined admission is never replayed as a phantom cache.
 */
class HotKeyCacheSingleFlightLifecycleTest {

  private HotKeyCache hotKeyCache;
  private Cache<String, CacheEntry> l1;
  private TestSingleFlight singleFlight;
  private ExecutorService flightExecutor;
  private ScheduledExecutorService scheduler;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    hotKeyCache = buildCache(new ZetaProperties());
  }

  /**
   * Assembles a real {@link HotKeyCache} around the given TTL configuration —
   * the shared fixture for every lifecycle test here, so a case can pin its
   * own expiry scale (the hard-expiry forget test uses a 150 ms hard TTL).
   */
  @SuppressWarnings("unchecked")
  private HotKeyCache buildCache(ZetaProperties props) {
    HotKeyDetector detector = mock(HotKeyDetector.class);
    when(detector.contains(anyString())).thenReturn(false);
    l1 = Caffeine.newBuilder().maximumSize(100).build();
    flightExecutor = Executors.newCachedThreadPool();
    singleFlight = new TestSingleFlight(
      1000,
      5,
      5,
      flightExecutor,
      // Production wiring shape: a registry whose default (disabled) config keeps every shard closed.
      new CircuitBreakerRegistry(new ZetaProperties.CircuitBreaker(), 64)
    );
    HealthView healthView = mock(HealthView.class);
    scheduler = Executors.newSingleThreadScheduledExecutor();
    return new HotKeyCache(
      detector,
      l1,
      singleFlight,
      new EntryLifecycleImpl(l1, props, healthView),
      new DefaultBackgroundRefresher(l1, Runnable::run, new EntryLifecycleImpl(l1, props, healthView), 10),
      new CentralDispatcher(
        new AccessCounter(detector, null, scheduler, 50L, AccessCounter.DEFAULT_DETECTOR_INTERVAL_MS),
        null,
        new BroadcastBuffer(scheduler, null, 500, 2_000, null),
        null
      ),
      new MonotonicStampClock(1L, MonotonicStampClock.DEFAULT_MAX_FORWARD_DRIFT_MS),
      healthView
    );
  }

  @AfterEach
  void tearDown() {
    flightExecutor.shutdownNow();
    scheduler.shutdownNow();
  }

  @Test
  @DisplayName("isCacheable=false leaves no L1 entry and no phantom future: the second get re-invokes the loader")
  void isCacheableFalse_secondGetReinvokesLoader() {
    AtomicInteger calls = new AtomicInteger();
    CacheLoader<String, String> loader = new CacheLoader<>() {
      @Override
      public String load(String key) {
        return "v" + calls.incrementAndGet();
      }

      @Override
      public boolean isCacheable(String value) {
        return false;
      }
    };

    String first = hotKeyCache.get("k", loader, GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH));
    String second = hotKeyCache.get("k", loader, GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH));

    assertThat(first).isEqualTo("v1");
    assertThat(second).isEqualTo("v2");
    assertThat(calls.get()).as("declined admission must not be replayed from dedup").isEqualTo(2);
    assertThat(l1.getIfPresent("k")).as("declined admission writes no L1 entry").isNull();
    assertThat(singleFlight.estimatedInFlightSize()).as("no phantom in-flight after settled loads").isZero();
  }

  /**
   * design decisions: dedup lifecycle residual, closed: the read path's hard-expiry removal now
   * forgets the dedup entry (singleflight Forget semantics), so a read that
   * arrives after the hard expiry — but well inside the 5 s dedup window —
   * must reload through the source instead of replaying the dead entry's
   * completed future.
   */
  @Test
  @DisplayName("hard expiry forgets the dedup entry: a post-expiry read reloads, never replays the dead future")
  void hardExpiry_forgetsDedupEntry() throws Exception {
    ZetaProperties props = new ZetaProperties();
    props.setHardTtlMs(150);
    props.setSoftTtlMs(0);
    HotKeyCache shortLived = buildCache(props);

    AtomicInteger calls = new AtomicInteger();
    String first = shortLived.get(
      "k",
      key -> "v" + calls.incrementAndGet(),
      GetPolicy.of(0L, 0L, true, StalePolicy.RETURN)
    );
    assertThat(first).isEqualTo("v1");

    // Outlive the 150 ms hard TTL (±5 % jitter) while staying far inside the
    // 5 s dedup retention: exactly the window the old replay served stale from.
    Thread.sleep(400);

    String second = shortLived.get(
      "k",
      key -> "v" + calls.incrementAndGet(),
      GetPolicy.of(0L, 0L, true, StalePolicy.RETURN)
    );
    assertThat(second).as("the expired entry's completed future must not be replayed").isEqualTo("v2");
    assertThat(calls.get()).isEqualTo(2);
  }

  /**
   * design decisions: dedup lifecycle — the raw-mutation half. A raw removal skips every
   * guarantee the verbs carry; the one the caller cannot uphold alone is the
   * dedup forget. The read path pins its future and drops it after the L1
   * commit, so a <em>retained</em> future only exists while a load is in
   * flight (or was left by a producer that does not unpin) —
   * {@code forgetDedup} hands the raw mutator the engine's own tool, and the
   * drop also arms the {@code !isCurrent} guard: the in-flight load serves
   * its waiters but no longer re-populates the mutated key.
   */
  @Test
  @DisplayName("a raw removal + forgetDedup: the in-flight load serves but never re-populates, the next get reloads")
  void rawRemoval_withForgetDedup_armsTheInFlightGuard() throws Exception {
    CountDownLatch loadEntered = new CountDownLatch(1);
    CountDownLatch releaseLoad = new CountDownLatch(1);
    AtomicInteger calls = new AtomicInteger();

    // The raw mutator's own step: the entry disappears outside the verbs.
    l1.asMap().remove("guarded");

    ExecutorService callers = Executors.newFixedThreadPool(1);
    try {
      Future<String> inFlight = callers.submit(() ->
        hotKeyCache.get(
          "guarded",
          () -> {
            loadEntered.countDown();
            try {
              releaseLoad.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new RuntimeException(e);
            }
            return "v" + calls.incrementAndGet();
          },
          GetPolicy.of(0L, 0L, true, StalePolicy.RETURN)
        )
      );
      assertThat(loadEntered.await(5, TimeUnit.SECONDS))
        .as("the reader runs on the flight executor — the dedup future is live")
        .isTrue();

      // The half of the contract the raw mutator cannot uphold alone.
      hotKeyCache.forgetDedup("guarded");

      releaseLoad.countDown();
      String served = inFlight.get(5, TimeUnit.SECONDS);
      assertThat(served).as("the in-flight load still serves its waiter").isEqualTo("v1");

      assertThat(l1.getIfPresent("guarded"))
        .as("the !isCurrent guard must refuse the in-flight load's re-populate after the forget")
        .isNull();

      // The next read is a clean miss: fresh load, no replay of v1.
      String next = hotKeyCache.get(
        "guarded",
        () -> "w" + calls.incrementAndGet(),
        GetPolicy.of(0L, 0L, true, StalePolicy.RETURN)
      );
      assertThat(next).isEqualTo("w2");
      assertThat(calls.get()).isEqualTo(2);
    } finally {
      callers.shutdownNow();
    }
  }

  /**
   * A retained completed future (a producer that does not unpin — e.g. a
   * direct SingleFlight user) is dropped by {@code forgetDedup}: the next
   * load re-executes the reader instead of replaying the outcome.
   */
  @Test
  @DisplayName("forgetDedup drops a retained completed dedup entry")
  void forgetDedup_dropsRetainedCompletedEntry() {
    AtomicInteger calls = new AtomicInteger();
    java.util.function.Supplier<Object> first = () -> "x" + calls.incrementAndGet();
    singleFlight.loadOutcome("retained", first);
    assertThat(calls.get()).isOne();

    hotKeyCache.forgetDedup("retained");

    java.util.function.Supplier<Object> second = () -> "y" + calls.incrementAndGet();
    singleFlight.loadOutcome("retained", second);
    assertThat(calls.get()).as("the retained completed future must not be replayed after forgetDedup").isEqualTo(2);
  }

  @Test
  @DisplayName("concurrent T1/T2 merge onto one loader execution; T3 after the commit hits L1")
  void concurrentLoaders_mergeOnceThenHitL1() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch entered2 = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);

    ExecutorService callers = Executors.newFixedThreadPool(2);
    try {
      Future<String> t1 = callers.submit(() ->
        hotKeyCache.get(
          "k",
          () -> {
            calls.incrementAndGet();
            entered.countDown();
            try {
              release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            return "v";
          },
          GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH)
        )
      );
      assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
      Future<String> t2 = callers.submit(() ->
        hotKeyCache.get(
          "k",
          () -> {
            calls.incrementAndGet();
            entered2.countDown();
            return "v-duplicate";
          },
          GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH)
        )
      );
      // Deterministic merge precondition: T2's get() must reach the dedup
      // election while T1's future is still pinned. The converged lifecycle
      // (L1 commit, then dedup removal) narrows but cannot close the classic
      // miss/elect interleave — a T2 electing after T1's commit+removal
      // re-runs the loader by design (the residual stampede window every
      // singleflight has). Give T2 a generous scheduling margin and assert
      // its loader never fired inside the window: if it did, the merge
      // premise is broken and the test fails here with a clear signal.
      assertThat(entered2.await(200, TimeUnit.MILLISECONDS))
        .as("T2 must merge onto T1's in-flight load, not run its own loader")
        .isFalse();
      release.countDown();

      assertThat(t1.get(15, TimeUnit.SECONDS)).isEqualTo("v");
      assertThat(t2.get(15, TimeUnit.SECONDS)).isEqualTo("v");
      assertThat(calls.get()).as("T1/T2 must merge onto one loader execution").isEqualTo(1);

      String third = hotKeyCache.get(
        "k",
        () -> {
          calls.incrementAndGet();
          return "v-reload";
        },
        GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH)
      );
      assertThat(third).isEqualTo("v");
      assertThat(calls.get()).as("T3 must hit L1, not re-invoke the loader").isEqualTo(1);
    } finally {
      callers.shutdownNow();
    }
  }
}
