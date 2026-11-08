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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycle;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycleImpl;
import io.github.hyshmily.zeta.cache.cachesupport.loader.LoaderOutcome;
import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.scheduler.DefaultBackgroundRefresher;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.util.clock.StampClock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Minimal {@link HotKeyCache} harness for the semantics tests (design decisions: version stamps).
 *
 * <p>The clock is a Mockito stub over a monotone counter, so every allocated
 * stamp is observable and strictly increasing. SingleFlight runs the supplier
 * inline; the refresher uses the direct executor. Everything is torn down by
 * {@link #close()}.
 */
final class StampTestSupport implements AutoCloseable {

  final AtomicLong stamps = new AtomicLong();
  final StampClock clock;
  final Cache<String, CacheEntry> l1;
  final SingleFlight singleFlight;
  final EntryLifecycle lifecycle;
  final ScheduledExecutorService scheduler;
  final HotKeyCache cache;
  /** Every INVALIDATE_BATCH wire entry the cache broadcast (with a null publisher the plain dispatcher drops them). */
  final List<SyncMessage.Entry> batchedInvalidations = new ArrayList<>();

  StampTestSupport() {
    this(null);
  }

  /**
   * @param providedClock the clock to use, or {@code null} for a Mockito stub
   *                      over an incrementing counter
   */
  StampTestSupport(StampClock providedClock) {
    l1 = Caffeine.newBuilder().maximumSize(100).build();
    clock = providedClock != null ? providedClock : mock(StampClock.class);
    if (providedClock == null) {
      when(clock.nextStamp()).thenAnswer(inv -> stamps.incrementAndGet());
      // Range reservation shares the same counter: contiguous block, no overlap
      // with single allocations (mirrors the real clock's contract).
      when(clock.nextStamps(org.mockito.ArgumentMatchers.anyInt())).thenAnswer(inv -> {
        int n = inv.getArgument(0);
        return stamps.addAndGet(n) - n + 1;
      });
    }
    singleFlight = mock(SingleFlight.class);
    when(singleFlight.load(anyString(), any(), anyBoolean(), any())).thenAnswer(inv -> {
      @SuppressWarnings("unchecked")
      Supplier<Object> supplier = inv.getArgument(1);
      @SuppressWarnings("unchecked")
      Consumer<Object> commit = inv.getArgument(3);
      Object value = supplier.get();
      if (value != null) {
        commit.accept(value);
        return LoaderOutcome.loaded(value);
      }
      return LoaderOutcome.sourceEmpty();
    });
    when(singleFlight.loadIsolated(anyString(), any(), any())).thenAnswer(inv -> {
      @SuppressWarnings("unchecked")
      Supplier<Object> supplier = inv.getArgument(1);
      @SuppressWarnings("unchecked")
      Consumer<Object> commit = inv.getArgument(2);
      Object value = supplier.get();
      if (value != null) {
        commit.accept(value);
        return LoaderOutcome.loaded(value);
      }
      return LoaderOutcome.sourceEmpty();
    });
    when(singleFlight.load(anyIterable(), any(), any())).thenAnswer(inv -> {
      Iterable<String> keys = inv.getArgument(0);
      @SuppressWarnings("unchecked")
      Function<String, Object> fn = inv.getArgument(1);
      @SuppressWarnings("unchecked")
      BiConsumer<String, Object> commit = inv.getArgument(2);
      Map<String, LoaderOutcome<Object>> out = new LinkedHashMap<>();
      for (String key : keys) {
        Object value = fn.apply(key);
        if (value != null) {
          commit.accept(key, value);
        }
        out.put(key, value == null ? LoaderOutcome.sourceEmpty() : LoaderOutcome.loaded(value));
      }
      return out;
    });
    scheduler = Executors.newSingleThreadScheduledExecutor();
    HotKeyDetector detector = mock(HotKeyDetector.class);
    when(detector.contains(anyString())).thenReturn(false);
    lifecycle = new EntryLifecycleImpl(l1, new ZetaProperties(), null);
    // The shared access counter owns the read path's counting (design decisions: access counting);
    // a null reporter keeps the harness detection-only.
    AccessCounter accessCounter = new AccessCounter(
      detector,
      null,
      scheduler,
      50,
      AccessCounter.DEFAULT_DETECTOR_INTERVAL_MS
    );
    cache = new HotKeyCache(
      detector,
      l1,
      singleFlight,
      lifecycle,
      new DefaultBackgroundRefresher(l1, Runnable::run, lifecycle, 10),
      new RecordingDispatcher(accessCounter),
      clock,
      mock(HealthView.class)
    );
  }

  /** Delegates everything to the null-publisher dispatcher, keeping only the batched-invalidation wire entries. */
  private final class RecordingDispatcher extends CentralDispatcher {

    private RecordingDispatcher(AccessCounter accessCounter) {
      super(accessCounter, null, new BroadcastBuffer(scheduler, null, 500, 2_000, null), null);
    }

    @Override
    public void broadcastInvalidateBatch(List<SyncMessage.Entry> entries) {
      batchedInvalidations.addAll(entries);
    }
  }

  /**
   * A NORMAL entry with the given value and stamp.
   *
   * @param value       the stored value
   * @param dataVersion the stamp ({@code 0} = unknown)
   * @return the entry
   */
  static CacheEntry entry(String value, long dataVersion) {
    return EntryDraft.blank(null)
      .value(value)
      .version(dataVersion)
      .decision(DecisionStamp.hot(0, null, 0L))
      .ttl(300_000, 30_000, 300_000, 30_000)
      .expiryAt(Long.MAX_VALUE, Long.MAX_VALUE)
      .keyState(KeyState.NORMAL)
      .build();
  }

  @Override
  public void close() {
    scheduler.shutdownNow();
  }
}
