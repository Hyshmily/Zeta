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
import io.github.hyshmily.zeta.util.LogThrottle;
import io.github.hyshmily.zeta.util.TimeSource;
import jakarta.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

/**
 * Lossy deferred send buffer for cache-sync messages.
 *
 * <p>Records the newest version for each key; on {@link #flush()} sends the
 * most recent entry per key via the {@link CacheSyncPublisher}. Concurrent
 * records for the same key are merged: within the same space (normal vs
 * degraded) the strictly newer allocation wins, across the degraded boundary
 * the newest record wins — an out-of-order record can never regress the
 * pending version within a space, so only intermediate versions may be lost,
 * which is acceptable because the next flush sends the latest known state.
 *
 * <p>Uses a lazy delayed flush strategy: the first {@link #record} after a
 * quiet period schedules a one-shot flush after a configurable delay
 * (default 500ms). Subsequent records within that window leave the scheduled
 * flush alone — the flush swaps the whole pending map at fire time, so the
 * latest writes are always included in that single send cycle (worst-case
 * flush latency for a record is {@code maxDeferMs}, and typically
 * {@code flushDelayMs}).
 *
 * <p>To bound memory under write bursts, the pending map is capped at
 * {@value #MAX_PENDING_ENTRIES} entries; crossing the cap triggers an
 * immediate flush (off-loaded to the scheduler thread) rather than deferring further,
 * bounding memory under write bursts.
 *
 * <p>The internal {@code pending} map is allocated eagerly at construction;
 * entries only appear once the first {@link #record} arrives.
 *
 * <p>Send-failure logging is aggregated and rate-limited to one WARN per 10s
 * window (ADR-0037). The send loop can additionally be handed off to a
 * dedicated executor (ADR-0037 Part B) so a broker outage never stalls the
 * shared scheduler — production wiring must pass the executor for that
 * guarantee to hold; without one, sends run synchronously (legacy path).
 */
@Slf4j
@Internal
public class BroadcastBuffer {

  private static final long DEFAULT_FLUSH_DELAY_MS = 500;
  private static final long DEFAULT_MAX_DEFER_MS = 2_000;

  /**
   * Maximum number of pending entries before a forced flush is triggered (soft cap: the flush is
   * off-loaded to the scheduler thread, so the map may briefly overshoot under a write burst).
   * Bounds memory of the defer window (≈12MB at ~120B per entry).
   */
  static final int MAX_PENDING_ENTRIES = 100_000;

  /**
   * Retry bound for {@link #record}'s swap re-check: consecutive flush swaps are
   * separated by at least one scheduler round trip, so a record losing the race
   * twice in a row requires a flush firing on every retry — bounded retries keep
   * the pathological case from spinning while covering every realistic schedule.
   */
  private static final int RECORD_SWAP_RETRIES = 4;

  private final long maxDeferMs;
  /**
   * Wall-clock (monotonic) timestamp of the first record of the current deferral
   * window, for the max-deferral bound. Volatile so {@link #rescheduleFlush}'s
   * lock-free fast path can read it without {@code scheduleLock}; mutated only
   * under the lock.
   */
  private volatile long firstRecordAtMs = 0L;

  @SuppressWarnings("java:S3077")
  private volatile ConcurrentHashMap<String, VersionInfo> pending = new ConcurrentHashMap<>();

  private final ScheduledExecutorService scheduler;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private final Optional<CacheSyncPublisher> publisher;

  /**
   * Optional dedicated send executor (ADR-0037). When present, {@link #flush()} hands the swapped
   * map off to it and the calling thread only does bookkeeping; a saturated executor drops the
   * batch (reported via the rate-limited WARN). When {@code null}, sends run synchronously on the
   * calling thread (legacy path).
   */
  @Nullable
  private final Executor sendExecutor;

  /**
   * Rate-limits the aggregated flush-failure WARN to one per
   * {@value LogThrottle#DEFAULT_WINDOW_MS}ms window (ADR-0037). Admission is strict —
   * {@link LogThrottle} claims the window with a compare-and-set, so exactly one
   * caller per window logs. The atomicity and the monotonic clock are provided by
   * {@link LogThrottle}.
   */
  private final LogThrottle flushErrorLogThrottle = LogThrottle.perDefaultWindow();

  /**
   * Cumulative refresh broadcasts that failed to send (broker errors in
   * {@link #doSend}) — the attribution counter behind
   * {@code zeta.stall.broadcast_storm.stopped.total} (the WARN is
   * rate-limited, this counter keeps the real total, ADR-0037 convention).
   */
  private final AtomicLong sendFailureCounter = new AtomicLong();

  /**
   * Cumulative refresh broadcasts dropped because the send executor was
   * saturated or shutting down — the second attribution counter behind
   * {@code zeta.stall.broadcast_storm.stopped.total}.
   */
  private final AtomicLong saturationDropCounter = new AtomicLong();

  private final long flushDelayMs;
  /**
   * The pending scheduled flush, or {@code null}. Volatile so
   * {@link #rescheduleFlush}'s lock-free fast path can check it without
   * {@code scheduleLock}; mutated only under the lock.
   */
  private volatile ScheduledFuture<?> scheduledFlush;
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
    this(scheduler, publisher, flushDelayMs, maxDeferMs, null);
  }

  /**
   * Creates a BroadcastBuffer with explicit flush delay, max deferral and an optional dedicated
   * send executor (ADR-0037).
   *
   * @param scheduler     the shared scheduler ({@code hotKeyScheduler})
   * @param publisher     the optional sync publisher
   * @param flushDelayMs  the deferral delay before a scheduled flush fires
   * @param maxDeferMs    the maximum deferral before a flush is forced
   * @param sendExecutor  optional executor for the send loop; {@code null} keeps the legacy
   *                      synchronous send on the calling thread
   */
  public BroadcastBuffer(
    ScheduledExecutorService scheduler,
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<CacheSyncPublisher> publisher,
    long flushDelayMs,
    long maxDeferMs,
    @Nullable Executor sendExecutor
  ) {
    this.scheduler = scheduler;
    this.publisher = publisher;
    this.flushDelayMs = flushDelayMs;
    this.maxDeferMs = maxDeferMs;
    this.sendExecutor = sendExecutor;
  }

  /**
   * Record a version update for the given key.  The pending entry is merged with
   * the sync version ordering (see {@link #mergeVersion}), so the buffer always
   * holds the newest known version per key even when concurrent writers record
   * out of INCR order.
   * Schedules the deferred flush if none is pending; a flush that is already
   * scheduled is left alone (it picks up the latest state at fire time), and
   * is only forced when the max-deferral bound is crossed. If the pending map
   * exceeds {@link #MAX_PENDING_ENTRIES}, a flush is triggered immediately
   * (off-loaded to the scheduler thread) instead of deferring further,
   * bounding memory under write bursts.
   *
   * <p>If the scheduler rejects the flush scheduling (saturated or shutting
   * down), the buffer degrades to a synchronous flush on the calling thread
   * so pending REFRESH messages are never stranded until the next write.
   *
   * @param key      the cache key
   * @param version  the data version to send
   * @param degraded whether the version was obtained in degraded mode
   */
  @SuppressWarnings("java:S6213")
  public void record(String key, long version, boolean degraded) {
    // Compute into the map the field currently points to, then verify the map is
    // still the live one. A record whose field-read preceded a concurrent flush's
    // swap but whose compute landed after the flush's snapshot could otherwise
    // land in the swapped-out map — absent from both the snapshot and the fresh
    // map, with no successor flush scheduled (its lock-free fast path may still
    // see the cancelled future). The version-ordering merge makes the redo
    // idempotent, so re-computing into the live map is always safe.
    ConcurrentHashMap<String, VersionInfo> map = pending;
    map.compute(key, (k, old) -> mergeVersion(old, version, degraded));
    int retries = 0;
    while (map != pending && retries++ < RECORD_SWAP_RETRIES) {
      map = pending;
      map.compute(key, (k, old) -> mergeVersion(old, version, degraded));
    }
    try {
      if (pending.size() > MAX_PENDING_ENTRIES) {
        scheduler.execute(this::flushAndReset);
      } else {
        rescheduleFlush();
      }
    } catch (RejectedExecutionException e) {
      // Scheduler saturated or shutting down — the pending entry would never
      // be flushed by a scheduled task, so degrade to a synchronous send on
      // the calling thread. Without this, a lost REFRESH leaves peers on the
      // stale value until the next write for the same key.
      log.warn("Scheduler rejected flush scheduling, flushing synchronously (key={})", key, e);
      flush();
    }
  }

  /**
   * Merge a recorded {@code (version, degraded)} pair into the pending entry:
   * <ul>
   *   <li><b>Same space</b> (both normal or both degraded): the strictly newer
   *       allocation wins. Versions are issued by Redis INCR, so a lower number is
   *       an out-of-order record of an older write, never a regression of the
   *       newest one — the defect this merge fixes (a regressed pending version let
   *       a flush send a REFRESH every receiver would skip, pinning peers on stale
   *       data until the next write or TTL).</li>
   *   <li><b>Across the degraded boundary</b>: the newest RECORD wins
   *       (last-writer-wins, the legacy semantics). The record order reflects real
   *       write order here, and the receiver-side 4-case matrix decides what
   *       applies — a newer degraded write must still reach peers without entries,
   *       while peers holding normal entries skip it on their own
   *       ({@code VersionGuard.shouldSkipForSync}).</li>
   * </ul>
   *
   * @param old      the currently pending entry, or {@code null}
   * @param version  the recorded version
   * @param degraded whether the recorded version was obtained in degraded mode
   * @return the entry to keep pending
   */
  private static VersionInfo mergeVersion(VersionInfo old, long version, boolean degraded) {
    if (old == null || old.degraded() != degraded) {
      return new VersionInfo(version, degraded);
    }
    return version > old.version() ? new VersionInfo(version, degraded) : old;
  }

  /**
   * Immediately flush all pending entries to the sync publisher.
   * Safe to call concurrently — swaps the internal map before sending, so
   * concurrent {@link #record} calls see a fresh map. With a {@link #sendExecutor}
   * the send loop is handed off asynchronously and the calling thread never blocks
   * on AMQP (ADR-0037); without one, sends run synchronously as before.
   */
  public void flush() {
    ConcurrentHashMap<String, VersionInfo> toFlush;
    synchronized (scheduleLock) {
      // Cancel BEFORE the swap (program order: cancel → swap). A record whose
      // entry lands in the fresh map must observe scheduledFlush == null: its
      // pending-map read synchronizes with the swap's volatile write, so it
      // sees every write that preceded the swap — including the cancel. It
      // then takes rescheduleFlush's slow path and schedules a successor
      // flush. Cancelling after the swap left a window where such a record saw
      // the running task as "still pending" and trusted a flush that had
      // already picked its snapshot — the record sat unsent until the next
      // record arrived (potentially forever after a quiet tail).
      cancelScheduledFlush();
      ConcurrentHashMap<String, VersionInfo> current = pending;
      if (current.isEmpty()) {
        return;
      }
      pending = new ConcurrentHashMap<>();
      toFlush = current;
    }

    if (sendExecutor != null) {
      try {
        sendExecutor.execute(() -> doSend(toFlush));
      } catch (RejectedExecutionException e) {
        // Executor saturated or shutting down — drop the batch (ADR-0007/0013:
        // a lost REFRESH is re-sent by the next flush after recovery).
        logSaturationDrop(toFlush.size());
      }
    } else {
      doSend(toFlush);
    }
  }

  /**
   * Sends a flushed snapshot to the sync publisher, one attempt per key, never
   * aborting on a partial failure. Failures are aggregated into a single
   * rate-limited WARN (one per 10s window, first exception kept) instead of a
   * per-key WARN with a full stack trace (ADR-0037).
   *
   * @param toFlush the flushed snapshot
   */
  private void doSend(Map<String, VersionInfo> toFlush) {
    if (publisher.isEmpty()) {
      return;
    }
    CacheSyncPublisher pub = publisher.get();
    long failed = 0L;
    Exception firstError = null;
    for (Map.Entry<String, VersionInfo> entry : toFlush.entrySet()) {
      VersionInfo vi = entry.getValue();

      try {
        pub.broadcastRefresh(entry.getKey(), vi.version, vi.degraded);
      } catch (Exception e) {
        failed++;
        if (firstError == null) {
          firstError = e;
        }
      }
    }
    if (failed > 0) {
      sendFailureCounter.addAndGet(failed);
      if (flushErrorLogThrottle.tryAcquire()) {
        log.warn(
          "Failed to send {} of {} refresh broadcasts (previous errors suppressed for 10s): {}",
          failed,
          toFlush.size(),
          firstError.toString()
        );
      }
    }
  }

  /**
   * Cumulative count of refresh broadcasts that failed to send (broker
   * errors) since startup. Monotonic; safe to read from any thread.
   *
   * @return the total send-failure count
   */
  public long sendFailures() {
    return sendFailureCounter.get();
  }

  /**
   * Cumulative count of refresh broadcasts dropped because the send executor
   * was saturated or shutting down since startup. Monotonic; safe to read
   * from any thread.
   *
   * @return the total saturation-drop count
   */
  public long saturationDrops() {
    return saturationDropCounter.get();
  }

  /**
   * Reports a saturation drop of the send executor, subject to the same 10s
   * rate limit as send failures (ADR-0037).
   *
   * @param dropped the number of dropped refresh broadcasts
   */
  private void logSaturationDrop(int dropped) {
    saturationDropCounter.addAndGet(dropped);
    if (flushErrorLogThrottle.tryAcquire()) {
      log.warn("Send executor saturated, dropping {} refresh broadcasts (previous drops suppressed for 10s)", dropped);
    }
  }

  /**
   * Reschedules the flush operation based on the current state.
   *
   * <p>Records never cancel a pending flush: the flush swaps the whole pending
   * map at fire time, so the latest records are always included and re-arming
   * the task per record would pay a {@code scheduleLock} round trip plus a
   * cancel + new {@link ScheduledFuture} per write for no behavioral gain.
   * The only re-scheduling cases are: no flush pending (first record after a
   * quiet period, or the previous task already fired) and the max-deferral
   * bound being crossed (the pending task is forced to fire now). Every
   * record is therefore flushed within {@code flushDelayMs} of itself, and
   * never later than {@code maxDeferMs} — the documented worst case.
   */
  private void rescheduleFlush() {
    long now = TimeSource.monotonicMillis();
    // Lock-free fast path (the hot case: a flush is already scheduled inside
    // the deferral window — leave it alone). Skipping the lock is safe because
    // both races resolve conservatively: a record that landed in the fresh map
    // observes scheduledFlush == null (cancel precedes the swap — see flush())
    // and falls through to the slow path, and a record whose compute landed in
    // a swapped-out map was already re-recorded into the live map by record()'s
    // re-check, so it cannot be stranded here.
    ScheduledFuture<?> scheduled = scheduledFlush;
    long first = firstRecordAtMs;
    if (scheduled != null && !scheduled.isDone() && first != 0L && now - first < maxDeferMs) {
      return;
    }

    synchronized (scheduleLock) {
      if (firstRecordAtMs == 0L) {
        firstRecordAtMs = now;
      }
      boolean exceedMax = now - firstRecordAtMs >= maxDeferMs;

      if (scheduledFlush == null || scheduledFlush.isDone()) {
        scheduledFlush = scheduler.schedule(this::flushAndReset, exceedMax ? 0 : flushDelayMs, TimeUnit.MILLISECONDS);
        return;
      }
      if (exceedMax) {
        // A flush is already scheduled, but the max-deferral bound has been
        // crossed: force it now instead of letting the pending task ride up
        // to flushDelayMs past the documented bound.
        cancelScheduledFlush();
        scheduledFlush = scheduler.schedule(this::flushAndReset, 0, TimeUnit.MILLISECONDS);
      }
      // else: a flush is already scheduled within the deferral window — leave
      // it alone (see the method Javadoc).
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
