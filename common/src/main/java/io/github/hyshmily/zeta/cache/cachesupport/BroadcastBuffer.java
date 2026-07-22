/*
 * Copyright 2026 Hyshmily. All Rights Reserved.
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
package io.github.hyshmily.zeta.cache.cachesupport;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * Lossy deferred send buffer for cache-sync messages.
 *
 * <p>Records the latest version for each key; on {@link #flush()} sends the
 * most recent entry per key via the {@link CacheSyncPublisher}. Only the
 * last-writer-wins version is retained per key, so intermediate versions
 * may be lost — this is acceptable because the next flush will send
 * the latest known state.
 *
 * <p>Uses a lazy delayed flush strategy: the first {@link #record} after a
 * quiet period schedules a one-shot flush after a configurable delay
 * (default 500ms). Subsequent records within that window reset the timer,
 * combining multiple writes into a single send cycle.
 *
 * <p>The internal {@code pending} map is lazily initialized on first
 * {@code reportToWorker()} call to avoid allocating maps that are never used.
 */
@Slf4j
@Internal
public class BroadcastBuffer {

  private static final long DEFAULT_FLUSH_DELAY_MS = 500;
  private static final long DEFAULT_MAX_DEFER_MS = 2_000;
  private final long maxDeferMs;
  private long firstRecordAtMs = 0L;

  @SuppressWarnings("java:S3077")
  private volatile ConcurrentHashMap<String, VersionInfo> pending = new ConcurrentHashMap<>();

  private final ScheduledExecutorService scheduler;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private final Optional<CacheSyncPublisher> publisher;

  private final long flushDelayMs;
  private ScheduledFuture<?> scheduledFlush;
  private final Object scheduleLock = new Object();

  /**
   * Creates a BroadcastBuffer with the default flush delay of 500ms.
   *
   * @param scheduler the shared scheduler ({@code hotKeyScheduler})
   * @param publisher the optional sync publisher
   */
  public BroadcastBuffer(
    ScheduledExecutorService scheduler,
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<CacheSyncPublisher> publisher
  ) {
    this(scheduler, publisher, DEFAULT_FLUSH_DELAY_MS, DEFAULT_MAX_DEFER_MS);
  }

  public BroadcastBuffer(
    ScheduledExecutorService scheduler,
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<CacheSyncPublisher> publisher,
    long flushDelayMs
  ) {
    this(scheduler, publisher, flushDelayMs, Math.max(flushDelayMs, DEFAULT_MAX_DEFER_MS));
  }

  public BroadcastBuffer(
    ScheduledExecutorService scheduler,
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<CacheSyncPublisher> publisher,
    long flushDelayMs,
    long maxDeferMs
  ) {
    this.scheduler = scheduler;
    this.publisher = publisher;
    this.flushDelayMs = flushDelayMs;
    this.maxDeferMs = maxDeferMs;
  }

  /**
   * Record a version update for the given key.  If the key already has a
   * pending entry, it is unconditionally replaced (last-writer-wins).
   * Resets the deferred flush timer on each call.
   *
   * @param key      the cache key
   * @param version  the data version to send
   * @param degraded whether the version is degraded
   */
  @SuppressWarnings("java:S6213")
  public void record(String key, long version, boolean degraded) {
    pending.merge(key, new VersionInfo(version, degraded), (old, cur) -> cur);
    rescheduleFlush();
  }

  /**
   * Immediately flush all pending entries to the sync publisher.
   * Safe to call concurrently — swaps the internal map before iterating,
   * so concurrent {@link #record} calls see a fresh map.
   */
  public void flush() {
    ConcurrentHashMap<String, VersionInfo> toFlush;
    synchronized (scheduleLock) {
      ConcurrentHashMap<String, VersionInfo> current = pending;
      if (current.isEmpty()) {
        cancelScheduledFlush();
        return;
      }
      pending = new ConcurrentHashMap<>();
      toFlush = current;
      cancelScheduledFlush();
    }
    publisher.ifPresent(pub ->
      toFlush.forEach((key, vi) -> {
        try {
          pub.broadcastRefresh(key, vi.version, vi.degraded);
        } catch (Exception e) {
          log.warn("Failed to send refresh for key {}", key, e);
        }
      })
    );
  }

  /**
   * Reschedules the flush operation based on the current state.
   */
  private void rescheduleFlush() {
    synchronized (scheduleLock) {
      long now = System.currentTimeMillis();
      if (firstRecordAtMs == 0L) {
        firstRecordAtMs = now;
      }
      boolean exceedMax = now - firstRecordAtMs >= maxDeferMs;

      if (scheduledFlush == null || scheduledFlush.isDone()) {
        scheduledFlush = scheduler.schedule(this::flushAndReset, exceedMax ? 0 : flushDelayMs, TimeUnit.MILLISECONDS);
        return;
      }
      if (!exceedMax) {
        cancelScheduledFlush();
        scheduledFlush = scheduler.schedule(this::flushAndReset, flushDelayMs, TimeUnit.MILLISECONDS);
      }
    }
  }

  /**
   * Flushes the pending entries and resets the firstRecordAtMs timestamp.
   * This method is called by the scheduled flush task.
   */
  private void flushAndReset() {
    synchronized (scheduleLock) {
      firstRecordAtMs = 0L;
    }
    flush();
  }

  private void cancelScheduledFlush() {
    if (scheduledFlush != null && !scheduledFlush.isDone()) {
      scheduledFlush.cancel(false);
      scheduledFlush = null;
    }
  }

  /** A simple record to hold version information. */
  record VersionInfo(long version, boolean degraded) {}
}
