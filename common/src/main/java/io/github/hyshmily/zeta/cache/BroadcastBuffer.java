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

import static io.github.hyshmily.zeta.constants.ZetaConstants.NO_SYNC_PUBLISHER_MESSAGE;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.sync.local.SyncType;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;
import java.util.function.LongPredicate;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.DisposableBean;

/**
 * Single send outlet of the sync plane: a lossy deferred buffer for PUT/REFRESH broadcasts and an
 * immediate dispatch path for the INVALIDATE family.
 *
 * <p><b>Causal cancellation.</b> Every INVALIDATE operation cancels buffered PUT/REFRESH records
 * for affected keys at or below the invalidation version before publishing, ensuring superseded
 * updates never ship after an invalidation and re-warm deleted data on peers.
 *
 * <p><b>Version-ordered merging and deferral.</b> Concurrent records for the same key merge so the
 * strictly newest version (and its optional immutable serialized payload) always wins. The first
 * record after an idle period schedules a one-shot flush after {@code flushDelayMs}, capped by
 * {@code maxDeferMs}, {@value #MAX_PENDING_ENTRIES} entries, and {@code maxPendingPayloadBytes}.
 *
 * <p><b>Isolation and shutdown.</b> When a dedicated {@code sendExecutor} is wired, flushed chunks
 * and invalidations run off the shared scheduler thread; without one, sends run inline. On
 * {@link #destroy()}, the final window drains synchronously on the closing thread before Spring
 * shuts down the send executor.
 */
@Slf4j
@Internal
public class BroadcastBuffer implements DisposableBean {

  /** Default cap (64 MiB) on total pending serialized value payload bytes. */
  public static final long DEFAULT_MAX_PENDING_PAYLOAD_BYTES = 64L * 1024 * 1024;

  /** Soft cap on pending entries before forcing an immediate scheduled flush. */
  static final int MAX_PENDING_ENTRIES = 100_000;

  /** Upper bound of detached entries submitted to the send executor per chunk. */
  private static final int SEND_CHUNK_SIZE = 1024;

  private final @Nullable CacheSyncPublisher publisher;
  private final ScheduledExecutorService scheduler;
  private final long maxPendingPayloadBytes;
  private final @Nullable Executor sendExecutor;
  private final long flushDelayMs;
  private final long maxDeferMs;

  /**
   * Guards the {@link #pending} map reference swap: records and cancellations hold the read lock
   * concurrently, while {@link #drain} acquires the write lock solely to swap in a fresh map and
   * publish {@link #closed}.
   */
  private final ReentrantReadWriteLock swapLock = new ReentrantReadWriteLock();

  /** Serializes flush scheduling, cancellation, and deferral-window anchoring. */
  private final Object scheduleLock = new Object();

  /** Approximate total serialized payload bytes currently buffered in {@link #pending}. */
  private final AtomicLong pendingPayloadBytesApprox = new AtomicLong();

  /** Approximate entry count of {@link #pending} to avoid {@code ConcurrentHashMap.size()} scans. */
  private final AtomicLong pendingSizeApprox = new AtomicLong();

  /** Cumulative put/refresh broadcasts dropped due to send-executor rejection. */
  private final AtomicLong saturationDropCounter = new AtomicLong();

  /** Cumulative INVALIDATE-family messages dropped due to send-executor rejection. */
  private final AtomicLong invalidatesDropped = new AtomicLong();

  /** Cumulative put/refresh broadcasts that failed at the publisher layer. */
  private final AtomicLong sendFailureCounter = new AtomicLong();

  /** Published under {@link #swapLock}'s write lock during {@link #destroy()} to halt new records. */
  private volatile boolean closed;

  @GuardedBy("swapLock")
  private ConcurrentHashMap<String, VersionInfo> pending = new ConcurrentHashMap<>();

  /** Armed deferred flush future, or {@code null} when idle; mutated only under {@link #scheduleLock}. */
  private volatile @Nullable ScheduledFuture<?> scheduledFlush;

  /** Monotonic timestamp of the first record in the active deferral window; {@code 0} when idle. */
  private volatile long firstRecordAtMs = 0L;

  /**
   * Creates a {@link BroadcastBuffer} with the default pending payload byte cap
   * ({@value #DEFAULT_MAX_PENDING_PAYLOAD_BYTES}).
   *
   * @param scheduler    the shared scheduler driving deferred flushes; must not be {@code null}
   * @param publisher    the optional sync publisher, or {@code null} in local-only mode
   * @param flushDelayMs deferral delay in milliseconds before a scheduled flush fires
   * @param maxDeferMs   maximum deferral duration in milliseconds before forcing a flush
   * @param sendExecutor optional dedicated executor for broker sends, or {@code null} for inline sends
   */
  public BroadcastBuffer(
    ScheduledExecutorService scheduler,
    @Nullable CacheSyncPublisher publisher,
    long flushDelayMs,
    long maxDeferMs,
    @Nullable Executor sendExecutor
  ) {
    this(scheduler, publisher, flushDelayMs, maxDeferMs, sendExecutor, DEFAULT_MAX_PENDING_PAYLOAD_BYTES);
  }

  /**
   * Creates a {@link BroadcastBuffer} with an explicit pending payload byte cap.
   *
   * @param scheduler              the shared scheduler driving deferred flushes; must not be {@code null}
   * @param publisher              the optional sync publisher, or {@code null} in local-only mode
   * @param flushDelayMs           deferral delay in milliseconds before a scheduled flush fires
   * @param maxDeferMs             maximum deferral duration in milliseconds before forcing a flush
   * @param sendExecutor           optional dedicated executor for broker sends, or {@code null} for inline sends
   * @param maxPendingPayloadBytes soft cap on total pending serialized payload bytes
   */
  public BroadcastBuffer(
    ScheduledExecutorService scheduler,
    @Nullable CacheSyncPublisher publisher,
    long flushDelayMs,
    long maxDeferMs,
    @Nullable Executor sendExecutor,
    long maxPendingPayloadBytes
  ) {
    this.scheduler = Objects.requireNonNull(scheduler, "scheduler must not be null");
    this.publisher = publisher;
    this.flushDelayMs = flushDelayMs;
    this.maxDeferMs = maxDeferMs;
    this.sendExecutor = sendExecutor;
    this.maxPendingPayloadBytes = maxPendingPayloadBytes;
    if (publisher != null && sendExecutor == null) {
      log.warn(
        "BroadcastBuffer created without a send executor while a sync publisher is present: " +
          "put/refresh sends run synchronously on the calling thread (design decisions: buffering and ordering). " +
          "Supply an executor to isolate AMQP sends from the shared scheduler."
      );
    }
  }

  /**
   * Buffers a PUT or REFRESH record, merging by strictly newest version per key and scheduling (or
   * forcing) a deferred flush.
   *
   * <p>When {@code stillCurrent} is provided, it is checked under the swap read lock both before
   * and after merging to close the serialize-then-record race against concurrent invalidations.
   *
   * @param key          the physical cache key
   * @param version      the data version stamp to broadcast
   * @param payload      immutable serialized value bytes for PUT, or {@code null} for REFRESH
   * @param stillCurrent predicate verifying {@code version} has not been superseded, or {@code null}
   */
  public void record(String key, long version, byte@Nullable [] payload, @Nullable LongPredicate stillCurrent) {
    if (publisher == null || closed) {
      return;
    }

    long sizeAfter;
    long bytesAfter;
    swapLock.readLock().lock();
    try {
      if (closed) {
        return;
      }
      if (stillCurrent != null && !stillCurrent.test(version)) {
        return;
      }
      mergeInto(key, version, payload);
      // Re-check after publishing into pending to pair with invalidation's tombstone-then-cancel order.
      if (stillCurrent != null && !stillCurrent.test(version)) {
        cancelInBin(key, version);
        return;
      }
      sizeAfter = pendingSizeApprox.get();
      bytesAfter = pendingPayloadBytesApprox.get();
    } finally {
      swapLock.readLock().unlock();
    }

    try {
      ensureFlushScheduled(sizeAfter > MAX_PENDING_ENTRIES || bytesAfter > maxPendingPayloadBytes);
    } catch (RejectedExecutionException ignored) {
      log.debug("Scheduler rejected flush scheduling, flushing synchronously (key={})", key);
      flush();
    }
  }

  /**
   * Serializes {@code value} on the calling thread and buffers a value-carrying PUT broadcast
   * (degrading to a version-only {@link SyncType#REFRESH} when serialization is skipped or exceeds
   * the payload limit).
   *
   * @param key          the physical cache key
   * @param value        the non-null value written locally
   * @param version      the data version allocated for the write
   * @param stillCurrent predicate verifying {@code version} is still current, or {@code null}
   */
  public void put(String key, Object value, long version, @Nullable LongPredicate stillCurrent) {
    if (closed) {
      return;
    }
    CacheSyncPublisher pub = publisherFor("PUT");
    if (pub == null || (stillCurrent != null && !stillCurrent.test(version))) {
      return;
    }
    record(key, version, pub.serializePutPayload(value), stillCurrent);
  }

  /**
   * Cancels any buffered PUT/REFRESH for {@code key} at or below {@code version} and broadcasts a
   * single-key INVALIDATE message immediately.
   *
   * @param key     the affected physical cache key
   * @param version the invalidation's data version stamp
   */
  public void invalidate(String key, long version) {
    CacheSyncPublisher pub = publisherFor("INVALIDATE");
    if (pub == null) {
      return;
    }
    cancel(key, version);
    sendInvalidate("INVALIDATE", () -> pub.broadcastInvalidate(key, version));
  }

  /**
   * Cancels any buffered PUT/REFRESH records superseded by {@code entries} and broadcasts one
   * batched invalidation message.
   *
   * @param entries non-null key/version pairs to invalidate
   */
  public void invalidateBatch(List<SyncMessage.Entry> entries) {
    Objects.requireNonNull(entries, "entries must not be null");
    CacheSyncPublisher pub = publisherFor("INVALIDATE_BATCH");
    if (pub == null) {
      return;
    }
    cancelBatch(entries);
    sendInvalidate("INVALIDATE_BATCH", () -> pub.broadcastInvalidateBatch(entries));
  }

  /**
   * Cancels buffered PUT/REFRESH records under {@code prefix} at or below {@code clearStamp} and
   * broadcasts a prefix-scoped invalidation message.
   *
   * @param prefix     the physical key prefix (ignored when {@code null} or empty)
   * @param clearStamp the clear's data version stamp
   */
  public void invalidatePrefix(@Nullable String prefix, long clearStamp) {
    if (prefix == null || prefix.isEmpty()) {
      return;
    }
    CacheSyncPublisher pub = publisherFor("INVALIDATE_PREFIX");
    if (pub == null) {
      return;
    }
    cancelMatching(prefix, clearStamp);
    sendInvalidate("INVALIDATE_PREFIX", () -> pub.broadcastInvalidatePrefix(prefix, clearStamp));
  }

  /**
   * Cancels all buffered PUT/REFRESH records at or below {@code clearStamp} and broadcasts a
   * table-wide invalidation message.
   *
   * @param clearStamp the clear's data version stamp
   */
  public void invalidateAll(long clearStamp) {
    CacheSyncPublisher pub = publisherFor("INVALIDATE_ALL");
    if (pub == null) {
      return;
    }
    cancelMatching(null, clearStamp);
    sendInvalidate("INVALIDATE_ALL", () -> pub.broadcastInvalidateAll(clearStamp));
  }

  /**
   * Swaps out and flushes all pending PUT/REFRESH records, handing chunks off to
   * {@link #sendExecutor} when configured or sending inline otherwise.
   */
  public void flush() {
    drain(true, false);
  }

  /**
   * Marks the buffer closed atomically with the final map swap and drains any remaining pending
   * records synchronously on the closing thread.
   */
  @Override
  public void destroy() {
    drain(false, true);
  }

  /**
   * Returns the cumulative count of put/refresh broadcasts that failed at the publisher since
   * startup.
   *
   * @return total publisher send failures
   */
  public long sendFailures() {
    return sendFailureCounter.get();
  }

  /**
   * Returns the cumulative count of put/refresh broadcasts dropped because {@link #sendExecutor}
   * refused a chunk since startup.
   *
   * @return total put/refresh executor drops
   */
  public long saturationDrops() {
    return saturationDropCounter.get();
  }

  /**
   * Returns the cumulative count of INVALIDATE-family messages dropped because
   * {@link #sendExecutor} refused execution since startup.
   *
   * @return total invalidation messages dropped
   */
  public long invalidatesDropped() {
    return invalidatesDropped.get();
  }

  /**
   * Returns the wired {@link CacheSyncPublisher} or logs a uniform debug message and returns
   * {@code null} when running without a sync publisher.
   *
   * @param op the operation name for debug logging
   * @return the publisher, or {@code null} when disabled
   */
  private @Nullable CacheSyncPublisher publisherFor(String op) {
    CacheSyncPublisher current = publisher;
    if (current != null) {
      return current;
    }
    if (log.isDebugEnabled()) {
      log.debug("send {}: {}", op, NO_SYNC_PUBLISHER_MESSAGE);
    }
    return null;
  }

  /**
   * Dispatches one immediate INVALIDATE-family publish via {@link #sendExecutor} (or inline if no
   * executor is configured), aggregating executor rejections in {@link #invalidatesDropped}.
   *
   * @param op   the message type name for diagnostics
   * @param send the publisher invocation returning {@code true} on success
   */
  private void sendInvalidate(String op, BooleanSupplier send) {
    Runnable publish = () -> {
      try {
        if (!send.getAsBoolean()) {
          log.warn("Failed to send {} sync message", op);
        }
      } catch (RuntimeException e) {
        log.warn("Failed to send {} sync message: {}", op, e.toString());
      }
    };

    Executor executor = sendExecutor;
    if (executor == null) {
      publish.run();
      return;
    }

    try {
      executor.execute(publish);
    } catch (RuntimeException ignored) {
      long total = invalidatesDropped.incrementAndGet();
      log.debug(
        "INVALIDATE send dropped: send executor saturated (cumulativeDrops={}; peers converge via hard TTL)",
        total
      );
    }
  }

  /**
   * Cancels any pending PUT/REFRESH for {@code key} whose version is at or below {@code version}.
   *
   * @param key     the physical cache key
   * @param version the invalidation version stamp
   */
  private void cancel(String key, long version) {
    if (version <= 0L) {
      return;
    }
    withReadLock(() -> cancelEntry(key, version));
  }

  /**
   * Cancels all pending PUT/REFRESH entries superseded by a batch of invalidation entries under a
   * single swap read-lock acquisition.
   *
   * @param entries the invalidation entries
   */
  private void cancelBatch(List<SyncMessage.Entry> entries) {
    if (entries.isEmpty()) {
      return;
    }
    withReadLock(() -> {
      for (SyncMessage.Entry entry : entries) {
        if (entry.version() > 0L) {
          cancelEntry(entry.key(), entry.version());
        }
      }
    });
  }

  /**
   * Cancels all pending PUT/REFRESH entries matching {@code prefix} (or all entries when
   * {@code prefix} is {@code null}) at or below {@code clearStamp}.
   *
   * @param prefix     the physical key prefix, or {@code null} for the entire buffer
   * @param clearStamp the clear's version stamp
   */
  private void cancelMatching(@Nullable String prefix, long clearStamp) {
    if (clearStamp <= 0L) {
      return;
    }
    withReadLock(() -> cancelWhere(prefix, clearStamp));
  }

  /**
   * Executes a cancellation action while holding {@link #swapLock}'s read lock so counter updates
   * cannot interleave with a flush swap reset.
   *
   * @param action the cancellation routine
   */
  private void withReadLock(Runnable action) {
    swapLock.readLock().lock();
    try {
      action.run();
    } finally {
      swapLock.readLock().unlock();
    }
  }

  /**
   * Performs a lock-free version pre-check before taking the map bin lock to cancel {@code key}.
   *
   * @param key     the physical cache key
   * @param version the invalidation version stamp
   */
  private void cancelEntry(String key, long version) {
    VersionInfo current = pending.get(key);
    if (current == null || current.version() > version) {
      return;
    }
    cancelInBin(key, version);
  }

  /**
   * Removes {@code key} from {@link #pending} inside the bin lock if its live version is at or
   * below {@code version}, updating the approximate entry and byte counters atomically with removal.
   *
   * @param key     the physical cache key
   * @param version the invalidation version stamp
   */
  private void cancelInBin(String key, long version) {
    pending.computeIfPresent(key, (k, old) -> {
      if (old.version() > version) {
        return old;
      }
      pendingSizeApprox.decrementAndGet();
      int bytes = payloadBytes(old);
      if (bytes != 0) {
        pendingPayloadBytesApprox.addAndGet(-bytes);
      }
      return null;
    });
  }

  /**
   * Scans {@link #pending} and cancels every entry whose version is at or below {@code version}
   * and whose key matches {@code prefix} (if non-null).
   *
   * @param prefix  the physical key prefix, or {@code null} for all keys
   * @param version the clear's version stamp
   */
  private void cancelWhere(@Nullable String prefix, long version) {
    for (Map.Entry<String, VersionInfo> entry : pending.entrySet()) {
      if (entry.getValue().version() <= version && (prefix == null || entry.getKey().startsWith(prefix))) {
        cancelInBin(entry.getKey(), version);
      }
    }
  }

  /**
   * Merges a single {@code (version, payload)} record into {@link #pending} under the bin lock and
   * updates the approximate entry-count and payload-byte counters.
   *
   * @param key     the physical cache key
   * @param version the data version stamp
   * @param payload the serialized value payload, or {@code null} for REFRESH
   */
  private void mergeInto(String key, long version, byte@Nullable [] payload) {
    pending.compute(key, (k, old) -> {
      if (old == null) {
        pendingSizeApprox.incrementAndGet();
      }
      int oldBytes = payloadBytes(old);
      VersionInfo merged = mergeVersion(old, version, payload);
      int mergedBytes = payloadBytes(merged);
      if (mergedBytes != oldBytes) {
        pendingPayloadBytesApprox.addAndGet(mergedBytes - oldBytes);
      }
      return merged;
    });
  }

  /**
   * Merges a candidate {@code (version, payload)} against an existing pending record so only a
   * strictly newer version replaces the entry and its payload.
   *
   * @param old     the existing pending record, or {@code null}
   * @param version the incoming version stamp
   * @param payload the incoming serialized payload, or {@code null}
   * @return the winning {@link VersionInfo}
   */
  private static VersionInfo mergeVersion(@Nullable VersionInfo old, long version, byte@Nullable [] payload) {
    return (old == null || version > old.version()) ? new VersionInfo(version, payload) : old;
  }

  /**
   * Returns the serialized payload length in bytes of {@code info}, or {@code 0} when absent.
   *
   * @param info the pending record, or {@code null}
   * @return payload byte length, or {@code 0}
   */
  private static int payloadBytes(@Nullable VersionInfo info) {
    if (info == null) {
      return 0;
    }
    byte[] payload = info.payload();
    return payload == null ? 0 : payload.length;
  }

  /**
   * Ensures a flush task is armed on {@link #scheduler}, re-using an existing pending task within
   * the deferral window and expediting to zero delay when {@code immediate} is {@code true} or
   * {@link #maxDeferMs} has elapsed.
   *
   * @param immediate {@code true} when a pending entry or payload cap was crossed
   * @throws RejectedExecutionException if {@link #scheduler} rejects the flush task
   */
  private void ensureFlushScheduled(boolean immediate) {
    long now = TimeSource.monotonicMillis();
    long firstAt = firstRecordAtMs;
    boolean fireNow = immediate || (firstAt != 0L && now - firstAt >= maxDeferMs);
    ScheduledFuture<?> scheduled = scheduledFlush;
    if (scheduled != null && !scheduled.isDone()) {
      if ((!fireNow && firstAt != 0L) || scheduled.getDelay(TimeUnit.MILLISECONDS) <= 0L) {
        return;
      }
    }

    synchronized (scheduleLock) {
      long anchorNow = TimeSource.monotonicMillis();
      if (pendingSizeApprox.get() == 0L) {
        return;
      }
      if (firstRecordAtMs == 0L) {
        firstRecordAtMs = anchorNow;
      }
      boolean urgent = immediate || anchorNow - firstRecordAtMs >= maxDeferMs;
      ScheduledFuture<?> current = scheduledFlush;
      if (current != null && !current.isDone()) {
        if (!urgent || current.getDelay(TimeUnit.MILLISECONDS) <= 0L) {
          return;
        }
        cancelScheduledFlush();
      }
      scheduledFlush = scheduler.schedule(this::flush, urgent ? 0L : flushDelayMs, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Cancels and clears any armed deferred flush future. The caller must hold {@link #scheduleLock}:
   * cancellation and the {@link #scheduledFlush} write are one critical section, so a concurrent
   * {@code ensureFlushScheduled} cannot re-arm a task that this method just retired. Non-blocking
   * cancel leaves an already-running flush to finish against its swapped snapshot.
   */
  private void cancelScheduledFlush() {
    ScheduledFuture<?> current = scheduledFlush;
    if (current != null) {
      if (!current.isDone()) {
        current.cancel(false);
      }
      scheduledFlush = null;
    }
  }

  /**
   * Atomically resets the deferral window, optionally marks the buffer closed, swaps out
   * {@link #pending}, and dispatches the swapped snapshot asynchronously or synchronously.
   *
   * @param async {@code true} to offload chunked sends to {@link #sendExecutor} when available;
   *              {@code false} to send inline on the calling thread
   * @param close {@code true} to set {@link #closed} under {@link #swapLock}'s write lock
   */
  private void drain(boolean async, boolean close) {
    ConcurrentHashMap<String, VersionInfo> toFlush;
    synchronized (scheduleLock) {
      // Cancel before swapping so any record landing in the fresh map arms a successor flush.
      cancelScheduledFlush();
      firstRecordAtMs = 0L;

      swapLock.writeLock().lock();
      try {
        if (close) {
          closed = true;
        }
        pendingSizeApprox.set(0L);
        pendingPayloadBytesApprox.set(0L);
        if (pending.isEmpty()) {
          return;
        }
        toFlush = pending;
        pending = new ConcurrentHashMap<>();
      } finally {
        swapLock.writeLock().unlock();
      }
    }

    Executor executor = sendExecutor;
    if (async && executor != null) {
      submitSendChunks(toFlush, executor);
    } else {
      send(detach(toFlush));
    }
  }

  /**
   * Detaches all entries from a swapped snapshot into a flat list for synchronous sending.
   *
   * @param snapshot the quiescent swapped map
   * @return detached list of {@link SendItem}s
   */
  private static List<SendItem> detach(Map<String, VersionInfo> snapshot) {
    List<SendItem> items = new ArrayList<>(snapshot.size());
    snapshot.forEach((key, info) -> items.add(new SendItem(key, info)));
    return items;
  }

  /**
   * Splits a swapped snapshot into detached chunks of at most {@value #SEND_CHUNK_SIZE} entries
   * and submits each chunk independently to {@code executor} so no chunk pins the swapped map and
   * partial rejections affect only the rejected chunk.
   *
   * @param toFlush  the quiescent swapped map
   * @param executor the dedicated send executor
   */
  @SuppressWarnings("all")
  private void submitSendChunks(Map<String, VersionInfo> toFlush, Executor executor) {
    int remaining = toFlush.size();
    List<SendItem> chunk = new ArrayList<>(Math.min(SEND_CHUNK_SIZE, remaining));
    for (Map.Entry<String, VersionInfo> entry : toFlush.entrySet()) {
      chunk.add(new SendItem(entry.getKey(), entry.getValue()));
      remaining--;
      if (chunk.size() == SEND_CHUNK_SIZE) {
        submitSendChunk(chunk, executor);
        chunk = remaining > 0 ? new ArrayList<>(Math.min(SEND_CHUNK_SIZE, remaining)) : List.of();
      }
    }
    if (!chunk.isEmpty()) {
      submitSendChunk(chunk, executor);
    }
  }

  /**
   * Submits a single detached chunk to {@code executor}, accounting for any submission failure in
   * {@link #saturationDropCounter} instead of propagating into caller write paths.
   *
   * @param chunk    the detached items to broadcast
   * @param executor the dedicated send executor
   */
  private void submitSendChunk(Collection<SendItem> chunk, Executor executor) {
    try {
      executor.execute(() -> send(chunk));
    } catch (RuntimeException e) {
      logExecutorDrop(chunk.size(), e);
    }
  }

  /**
   * Sends every item in {@code toFlush} to {@link #publisher}, continuing past individual errors
   * and aggregating failures into one warning per chunk.
   *
   * @param toFlush the detached items to broadcast
   */
  private void send(Collection<SendItem> toFlush) {
    CacheSyncPublisher pub = publisher;
    if (pub == null) {
      return;
    }

    long failed = 0L;
    RuntimeException firstError = null;
    for (SendItem item : toFlush) {
      VersionInfo info = item.info();
      byte[] payload = info.payload();
      try {
        boolean sent =
          payload != null
            ? pub.broadcastPut(item.key(), info.version(), payload)
            : pub.broadcastRefresh(item.key(), info.version());
        if (!sent) {
          failed++;
        }
      } catch (RuntimeException e) {
        failed++;
        if (firstError == null) {
          firstError = e;
        }
      }
    }

    if (failed > 0L) {
      sendFailureCounter.addAndGet(failed);
      log.warn(
        "Failed to send {} of {} put/refresh broadcasts: {}",
        failed,
        toFlush.size(),
        firstError != null ? firstError.toString() : "broker send failed"
      );
    }
  }

  /**
   * Records and logs a chunk dropped by {@link #sendExecutor}.
   *
   * @param dropped the number of dropped put/refresh items
   * @param cause   the exception thrown by the send executor
   */
  private void logExecutorDrop(int dropped, RuntimeException cause) {
    saturationDropCounter.addAndGet(dropped);
    log.debug("Send executor refused the batch, dropping {} put/refresh broadcasts", dropped, cause);
  }

  /**
   * Detached broadcast item carrying a key and its newest {@link VersionInfo} without retaining
   * a reference to the swapped {@link ConcurrentHashMap}.
   *
   * @param key  the physical cache key
   * @param info the newest version and optional payload snapshot
   */
  private record SendItem(String key, VersionInfo info) {}

  /**
   * Pending sync record storing the newest version for a key and an optional immutable serialized
   * value payload snapshot ({@code null} for version-only REFRESH broadcasts).
   *
   * @param version the data version stamp
   * @param payload the serialized value bytes for PUT, or {@code null} for REFRESH
   */
  record VersionInfo(long version, byte@Nullable [] payload) {}
}
