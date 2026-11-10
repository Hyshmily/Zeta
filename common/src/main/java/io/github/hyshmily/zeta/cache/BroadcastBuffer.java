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
package io.github.hyshmily.zeta.cache;

import static io.github.hyshmily.zeta.constants.ZetaConstants.NO_SYNC_PUBLISHER_MESSAGE;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.sync.local.SyncType;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.TimeSource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongPredicate;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * The sync plane's single send outlet: a lossy deferred buffer for PUT/REFRESH
 * records and the immediate INVALIDATE family.
 *
 * <p>All broadcasts to peers leave through this class, so the publisher and the
 * dedicated send executor are owned in one place and a deployment without a
 * publisher explains its silence identically at every site.
 *
 * <p><b>Causal cancellation.</b> An INVALIDATE at version {@code v} cancels every
 * buffered PUT/REFRESH for the same key at or below {@code v} before the
 * invalidation leaves: those records are dead the moment the invalidation exists,
 * and shipping them afterwards would re-warm what the peers just removed. The
 * cancellation is part of the invalidation send, not a caller obligation, so a
 * new invalidation shape cannot forget it.
 *
 * <p>Records the newest version for each key; on {@link #flush()} sends the
 * most recent entry per key via the {@link CacheSyncPublisher}. Concurrent
 * records for the same key are merged: the strictly newer allocation wins
 * (one stamp space, design decisions: version stamps) — an out-of-order record can never regress the
 * pending version, so only intermediate versions may be lost, which is
 * acceptable because the next flush sends the latest known state.
 *
 * <p>Records may carry a serialized value payload ({@link #record(String, long, byte[], LongPredicate)},
 * the value-carrying PUT broadcast): the payload is an immutable byte snapshot
 * taken on the writer thread, merged together with its version — the newest
 * version's payload wins, a REFRESH record (no payload) never displaces a
 * newer PUT and vice versa.
 *
 * <p>Uses a lazy delayed flush strategy: the first {@link #record} after a
 * quiet period schedules a one-shot flush after a configurable delay
 * (default 50ms — the window IS the sync plane's propagation latency).
 * Subsequent records within that window leave the scheduled flush alone — the
 * flush swaps the whole pending map at fire time, so the latest writes are
 * always included in that single send cycle (worst-case flush latency for a
 * record is {@code maxDeferMs}, and typically {@code flushDelayMs}).
 *
 * <p>To bound memory under write bursts, the pending map is capped at
 * {@value #MAX_PENDING_ENTRIES} entries AND at {@code maxPendingPayloadBytes}
 * total serialized payload bytes; crossing either cap triggers an immediate
 * flush (off-loaded to the scheduler thread) rather than deferring further.
 *
 * <p>The internal {@code pending} map is allocated eagerly at construction;
 * entries only appear once the first {@link #record} arrives.
 *
 * <p>Send-failure logging is aggregated to one WARN per flush (design decisions: buffering and ordering). The send loop can additionally be handed off to a
 * dedicated executor (design decisions: buffering and ordering) so a broker outage never stalls the
 * shared scheduler — production wiring must pass the executor for that
 * guarantee to hold; without one, sends run synchronously on the calling thread.
 */
@Slf4j
@Internal
public class BroadcastBuffer {

  /** Default cap (64MB) on total pending serialized value payload bytes. */
  public static final long DEFAULT_MAX_PENDING_PAYLOAD_BYTES = 64L * 1024 * 1024;

  /** Upper bound of entries handed to the send executor per submission. */
  private static final int SEND_CHUNK_SIZE = 1024;

  /**
   * Maximum number of pending entries before a forced flush is triggered (soft cap: the flush is
   * off-loaded to the scheduler thread, so the map may briefly overshoot under a write burst).
   * Bounds the entry count of the defer window (≈12MB at ~120B per version-only entry; value
   * payloads are bounded separately by {@code maxPendingPayloadBytes}).
   */
  static final int MAX_PENDING_ENTRIES = 100_000;

  private final @Nullable CacheSyncPublisher publisher;

  private final ScheduledExecutorService scheduler;

  /** Cap on total pending payload bytes; crossing it forces an immediate flush. */
  private final long maxPendingPayloadBytes;

  /**
   * Optional dedicated send executor (design decisions: buffering and ordering). When present, {@link #flush()} hands the swapped
   * map off to it and the calling thread only does bookkeeping; an executor that refuses the
   * batch (saturation, shutdown, or a thrown submission failure) drops it, reported at DEBUG. When
   * {@code null}, sends run synchronously on the calling thread.
   */
  @Nullable
  private final Executor sendExecutor;

  private final long flushDelayMs;

  private final long maxDeferMs;

  /**
   * Guards the pending-map reference and its swap. Records and evictions take
   * the read side, so they stay concurrent with each other; {@link #flush()}
   * takes the write side only for the reference swap, which makes every
   * swapped-out map quiescent before it is sent.
   */
  private final ReentrantReadWriteLock swapLock = new ReentrantReadWriteLock();

  /**
   * Approximate total bytes of serialized value payloads held pending. Updated
   * inside the record computes (delta between old and merged payload) and reset
   * on every flush swap. Approximate under races is fine: it only gates when a
   * flush is forced, never correctness — the flush drains whatever the map holds.
   */
  private final AtomicLong pendingPayloadBytesApprox = new AtomicLong();

  /**
   * Cumulative put/refresh broadcasts dropped because the send executor
   * refused the batch (saturated, shutting down, or a custom executor threw a
   * submission failure) — the second attribution counter behind
   * {@code zeta.stall.broadcast_storm.stopped.total}.
   */
  private final AtomicLong saturationDropCounter = new AtomicLong();

  /**
   * Cumulative INVALIDATE-family messages dropped because the send executor
   * refused them. Kept separate from {@link #saturationDropCounter}: that meter
   * is documented as put/refresh loss, and one batch invalidation is one message
   * regardless of how many keys it carries. A dropped invalidation converges
   * through the peer's hard TTL (at-most-once sync contract, design decisions: sync messages).
   */
  private final AtomicLong invalidatesDropped = new AtomicLong();

  /**
   * Cumulative put/refresh broadcasts that failed to send (broker errors in
   * {@link #doSend}) — the attribution counter behind
   * {@code zeta.stall.broadcast_storm.stopped.total}.
   */
  private final AtomicLong sendFailureCounter = new AtomicLong();

  /**
   * Approximate size of the pending map, maintained alongside the records.
   *
   * <p>{@code ConcurrentHashMap.size()} is {@code sumCount()} — a walk over the
   * map's {@code counterCells}, i.e. O(cores) volatile reads, and it allocates
   * {@code counterCells} under write contention, so the cost grows exactly when
   * the buffer is busiest. This counter is read on every record instead, so the
   * flush decision pays one plain read; an approximate value is sufficient
   * because it only gates <em>when</em> a flush is triggered, never correctness:
   * the flush itself drains whatever the map actually holds.
   */
  private final AtomicLong pendingSizeApprox = new AtomicLong();

  private final Object scheduleLock = new Object();

  @GuardedBy("swapLock")
  private ConcurrentHashMap<String, VersionInfo> pending = new ConcurrentHashMap<>();

  /**
   * The pending scheduled flush, or {@code null}. Volatile so
   * {@link #ensureFlushScheduled}'s lock-free fast path can check it without
   * {@code scheduleLock}; mutated only under the lock.
   */
  private volatile ScheduledFuture<?> scheduledFlush;

  /**
   * Wall-clock (monotonic) timestamp of the first record of the current deferral
   * window, for the max-deferral bound. Volatile so {@link #ensureFlushScheduled}'s
   * lock-free fast path can read it without {@code scheduleLock}; mutated only under the
   * lock.
   */
  private volatile long firstRecordAtMs = 0L;

  /**
   * Creates a BroadcastBuffer with the default pending payload byte cap
   * ({@value #DEFAULT_MAX_PENDING_PAYLOAD_BYTES}).
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
   * Creates a BroadcastBuffer with explicit flush delay, max deferral, an optional dedicated
   * send executor (design decisions: buffering and ordering) and the pending payload byte cap.
   *
   * @param scheduler               the shared scheduler ({@code hotKeyScheduler})
   * @param publisher               the optional sync publisher
   * @param flushDelayMs            the deferral delay before a scheduled flush fires
   * @param maxDeferMs              the maximum deferral before a flush is forced
   * @param sendExecutor            optional executor for the send loop; {@code null} keeps the
   *                                synchronous send on the calling thread
   * @param maxPendingPayloadBytes  cap on total pending serialized value payload bytes
   */
  public BroadcastBuffer(
    ScheduledExecutorService scheduler,
    @Nullable CacheSyncPublisher publisher,
    long flushDelayMs,
    long maxDeferMs,
    @Nullable Executor sendExecutor,
    long maxPendingPayloadBytes
  ) {
    this.scheduler = scheduler;
    this.publisher = publisher;
    this.maxDeferMs = maxDeferMs;
    this.flushDelayMs = flushDelayMs;
    this.sendExecutor = sendExecutor;
    this.maxPendingPayloadBytes = maxPendingPayloadBytes;
    if (publisher != null && sendExecutor == null) {
      // One-time wiring warning: without the dedicated send
      // executor every deferred REFRESH/PUT runs synchronously on the calling
      // thread (the shared hotKeyScheduler on flush, or the putThrough
      // submitter on scheduler saturation) — a slow broker then stalls
      // tide/park work. Production wiring (ZetaAutoConfiguration) always
      // supplies zetaSendExecutor; this path only triggers for manual
      // construction or a custom BroadcastBuffer bean (design decisions: buffering and ordering).
      log.warn(
        "BroadcastBuffer created without a send executor while a sync publisher is present: " +
          "put/refresh sends run synchronously on the calling thread (design decisions: buffering and ordering). " +
          "Supply an executor to isolate AMQP sends from the shared scheduler."
      );
    }
  }

  /**
   * Records a pending PUT/REFRESH. The pending entry is merged with the sync version
   * ordering (see {@link #mergeVersion}), so the buffer always holds the newest known
   * version — and its payload — per key even when concurrent writers record out of INCR
   * order. A value-carrying record keeps its immutable byte snapshot (already serialized
   * on the writer thread); a payload-less record is a version-only REFRESH.
   *
   * <p>When {@code stillCurrent} is supplied, it runs once under the swap read side
   * before the merge and once after it: a tombstone already recorded refuses the
   * record, and one recorded while the payload was being serialized is re-checked
   * after the merge so the record removes itself. Paired with the invalidation's
   * {@link #cancel} running before the send, the two sides close the
   * serialize-then-record window without a second lock.
   *
   * <p>Schedules the deferred flush if none is pending; a flush that is already scheduled
   * is left alone (it picks up the latest state at fire time), and is only forced when the
   * max-deferral bound is crossed. If the pending map exceeds {@link #MAX_PENDING_ENTRIES}
   * entries or the pending payload exceeds the byte cap, a flush is triggered immediately
   * (off-loaded to the scheduler thread) instead of deferring further, bounding memory under
   * write bursts.
   *
   * <p>If the scheduler rejects the flush scheduling (saturated or shutting down), the buffer
   * degrades to a synchronous flush on the calling thread so pending REFRESH/PUT messages are
   * never stranded until the next write.
   *
   * @param key           the cache key
   * @param version       the data version to send
   * @param payload       the serialized value payload, or {@code null} for a
   *                      version-only (REFRESH) record
   * @param stillCurrent  the currentness predicate for {@code version}, or {@code null}
   *                      to admit every record
   */
  public void record(String key, long version, byte@Nullable [] payload, @Nullable LongPredicate stillCurrent) {
    long sizeAfter;
    long bytesAfter;
    // Holding the read side until the merge finishes keeps the record in the
    // live map: flush cannot swap it out mid-compute, and an invalidation that
    // records its tombstone before this predicate sees it here.
    swapLock.readLock().lock();
    try {
      if (stillCurrent != null && !stillCurrent.test(version)) {
        return;
      }
      mergeInto(key, version, payload);
      // Publishing into pending before re-reading the invalidation tombstone pairs
      // with the invalidation path's tombstone-then-evict order: whichever store
      // lands first, the other thread's subsequent read sees this key and removes
      // or refuses it. The read side is shared, so this second check — not the
      // lock — closes the serialize-then-record window.
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
      // Over a cap: the buffer is bounded, so flush now — re-using the pending task so a
      // burst across the cap costs at most one extra schedule rather than one per record.
      ensureFlushScheduled(sizeAfter > MAX_PENDING_ENTRIES || bytesAfter > maxPendingPayloadBytes);
    } catch (RejectedExecutionException e) {
      // Scheduler saturated or shutting down — the pending entry would never be
      // flushed by a scheduled task, so flush on the calling thread. Without
      // this, a lost REFRESH leaves peers on the stale value until the next
      // write for the same key. Bounded rather than a network call on the
      // request thread: flush() only swaps the map and hands the batch to the
      // dedicated send executor (design decisions: buffering and ordering);
      // without a send executor the batch is sent inline.
      log.debug("Scheduler rejected flush scheduling, flushing synchronously (key={})", key);
      flush();
    }
  }

  /**
   * Broadcast a value-carrying PUT for a freshly written key (design decisions: sync messages).
   *
   * <p>The value is serialized into an immutable byte payload on the calling
   * thread so the buffer holds a frozen snapshot; the payload flows through the
   * same deferred path as REFRESH (same-key writes merge to the newest
   * version+value). Degrades to a value-less {@link SyncType#REFRESH} when the
   * value cannot be carried (not JSON-serializable or over
   * {@code zeta.sync.put-max-bytes}) — peers reload through their own loader.
   *
   * @param key          the affected cache key
   * @param value        the value just written (never {@code null} — the facade
   *                     rejects null puts)
   * @param version      the data version allocated for the write
   * @param stillCurrent the currentness predicate, or {@code null} to admit
   *                     every record
   */
  public void put(String key, Object value, long version, @Nullable LongPredicate stillCurrent) {
    CacheSyncPublisher pub = publisherFor("PUT");
    if (pub == null) {
      return;
    }
    record(key, version, pub.serializePutPayload(value), stillCurrent);
  }

  /**
   * Broadcast a single-key INVALIDATE on the send executor, cancelling any
   * buffered PUT/REFRESH for the same key at or below {@code version} first.
   *
   * @param key     the affected cache key
   * @param version the data version the invalidation was allocated at
   */
  public void invalidate(String key, long version) {
    CacheSyncPublisher pub = publisherFor("INVALIDATE");
    if (pub == null) {
      return;
    }
    cancel(key, version);
    sendInvalidate(() -> pub.broadcastInvalidate(key, version));
  }

  /**
   * Broadcast one batched invalidation message carrying up to
   * {@value SyncMessage#MAX_BATCH_ENTRIES} key/version pairs (design decisions: invalidation).
   *
   * @param entries the key/version pairs (never {@code null}/empty — the engine
   *                chunks its key set into non-empty blocks)
   */
  public void invalidateBatch(List<SyncMessage.Entry> entries) {
    CacheSyncPublisher pub = publisherFor("INVALIDATE_BATCH");
    if (pub == null) {
      return;
    }
    cancelBatch(entries);
    sendInvalidate(() -> pub.broadcastInvalidateBatch(entries));
  }

  /**
   * Broadcast a namespace clear: peers remove every entry whose key starts with
   * {@code prefix} and whose data version is at or below {@code clearStamp}.
   *
   * @param prefix     the physical key prefix (never null or empty)
   * @param clearStamp the data stamp the clear was allocated at
   */
  public void invalidatePrefix(String prefix, long clearStamp) {
    CacheSyncPublisher pub = publisherFor("INVALIDATE_PREFIX");
    if (pub == null) {
      return;
    }
    if (prefix != null && !prefix.isEmpty()) {
      cancelMatching(prefix, clearStamp);
    }
    sendInvalidate(() -> pub.broadcastInvalidatePrefix(prefix, clearStamp));
  }

  /**
   * Broadcast a full clear: peers remove every entry whose data version is at
   * or below {@code clearStamp}. One message covers the whole cache regardless
   * of how many keys the sender holds locally.
   *
   * @param clearStamp the data stamp the clear was allocated at
   */
  public void invalidateAll(long clearStamp) {
    CacheSyncPublisher pub = publisherFor("INVALIDATE_ALL");
    if (pub == null) {
      return;
    }
    cancelMatching(null, clearStamp);
    sendInvalidate(() -> pub.broadcastInvalidateAll(clearStamp));
  }

  /**
   * The no-publisher guard shared by every immediate broadcast: returns the
   * wired publisher, or {@code null} after logging the plane's debug line so a
   * local-only deployment explains its silence identically at every site.
   *
   * <p>Returning the reference rather than a boolean keeps the non-null
   * guarantee in the caller's local scope: the send lambdas capture it directly
   * instead of re-testing the field the guard just checked.
   *
   * @param op the operation that could not be sent (named in the debug line)
   * @return the publisher, or {@code null} when the caller must return without broadcasting
   */
  @Nullable
  private CacheSyncPublisher publisherFor(String op) {
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
   * Run one INVALIDATE-family publish, off the calling thread when a send
   * executor is wired (design decisions: invalidation). Executor saturation or shutdown drops the
   * message with a counted DEBUG line — at-most-once sync means the peer
   * converges via the next write or its hard TTL.
   *
   * @param send the publish action (never {@code null})
   */
  private void sendInvalidate(Runnable send) {
    Executor executor = sendExecutor;
    if (executor == null) {
      // Manual construction / tests: no executor, so send synchronously.
      send.run();
      return;
    }

    try {
      executor.execute(() -> {
        try {
          send.run();
        } catch (RuntimeException e) {
          // The publisher logs and counts its own broker failures; this guard keeps an
          // executor-side failure from killing a pooled thread.
          log.debug("INVALIDATE send failed on send executor: {}", e.toString());
        }
      });
    } catch (RuntimeException rejected) {
      long total = invalidatesDropped.incrementAndGet();
      log.debug(
        "INVALIDATE send dropped: send executor saturated (cumulativeDrops={}; peers converge via hard TTL)",
        total
      );
    }
  }

  /**
   * Cancels pending PUT/REFRESH records for {@code key} whose version is at or
   * below {@code version} — the process-local causal cancellation for the
   * cross-channel race: a PUT/REFRESH buffered here is flushed up to
   * {@code flushDelayMs} later, while a same-key INVALIDATE (or a batch, a
   * namespace clear, a full clear) leaves through the immediate send channel.
   * Without the cancellation the older buffered record ships after the
   * invalidation that superseded it, wasting a broadcast and — if the
   * INVALIDATE was dropped or its peer tombstone expired — re-warming deleted
   * data on the peers.
   *
   * <p>The approximate size/payload counters move inside the same bin compute as
   * the removal. Every cancellation holds the swap read side and every flush reset
   * holds the write side, so the decrement cannot race the reset, and a record that
   * re-creates the key afterwards is counted from zero instead of inheriting a
   * phantom capacity.
   *
   * @param key     the key whose pending records to cancel
   * @param version the invalidation's data stamp; records at or below it die
   */
  private void cancel(String key, long version) {
    if (version <= 0L) {
      return;
    }

    swapLock.readLock().lock();
    try {
      cancelEntry(key, version);
    } finally {
      swapLock.readLock().unlock();
    }
  }

  /**
   * Batch form of {@link #cancel(String, long)}: one read-lock acquisition for
   * the whole key/version list instead of one per entry.
   *
   * @param entries the key/version pairs to cancel; never {@code null}
   */
  private void cancelBatch(List<SyncMessage.Entry> entries) {
    if (entries.isEmpty()) {
      return;
    }
    swapLock.readLock().lock();
    try {
      for (SyncMessage.Entry entry : entries) {
        if (entry.version() > 0L) {
          cancelEntry(entry.key(), entry.version());
        }
      }
    } finally {
      swapLock.readLock().unlock();
    }
  }

  /**
   * Cancels pending records for every key under {@code prefix} whose version
   * is at or below {@code clearStamp}; a {@code null} prefix is the table-wide
   * form.
   *
   * @param prefix     the physical key prefix, or {@code null} for the whole table
   * @param clearStamp the clear's data stamp
   */
  private void cancelMatching(@Nullable String prefix, long clearStamp) {
    if (clearStamp <= 0L) {
      return;
    }

    swapLock.readLock().lock();
    try {
      cancelWhere(prefix, clearStamp);
    } finally {
      swapLock.readLock().unlock();
    }
  }

  private void cancelEntry(String key, long version) {
    // Lock-free pre-check: an absent key, or one already newer than the
    // invalidation, must not take the bin lock that computeIfPresent would.
    VersionInfo current = pending.get(key);
    if (current == null || current.version() > version) {
      return;
    }
    cancelInBin(key, version);
  }

  /**
   * The version-guarded removal itself. The counters move inside the compute, in the
   * same critical section as the removal: a record that re-creates the key afterwards
   * increments the new entry, and the eviction's decrement cannot land on that new
   * entry's count. The caller holds the swap read side, so this cannot interleave a
   * flush reset.
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

  private void cancelWhere(@Nullable String prefix, long version) {
    for (Map.Entry<String, VersionInfo> entry : pending.entrySet()) {
      // Integer compare first: startsWith is the expensive half and most
      // entries in a table-wide sweep fail this cheaper test.
      if (entry.getValue().version() <= version && (prefix == null || entry.getKey().startsWith(prefix))) {
        // Version-guarded removal, not remove(key, value): a record merged after
        // this iteration entry was read must still be cancelled when its live
        // version is at or below the invalidation, whatever its object identity.
        cancelInBin(entry.getKey(), version);
      }
    }
  }

  /**
   * Merge one record into the live pending map and maintain the approximate
   * size/payload counters. Caller holds {@link #swapLock}'s read side, so the
   * merge cannot race a flush swap; both counters are reset under the write
   * side on every flush.
   */
  private void mergeInto(String key, long version, byte@Nullable [] payload) {
    pending.compute(key, (k, old) -> {
      if (old == null) {
        pendingSizeApprox.incrementAndGet();
      }

      int oldBytes = payloadBytes(old);
      VersionInfo merged = mergeVersion(old, version, payload);
      int mergedBytes = payloadBytes(merged);
      // A version-only record, an out-of-order older record and a same-size PUT
      // replacement leave the total unchanged; skipping the add keeps them off
      // the shared counter's cache line.
      if (mergedBytes != oldBytes) {
        pendingPayloadBytesApprox.addAndGet(mergedBytes - oldBytes);
      }
      return merged;
    });
  }

  /** Serialized payload byte length of a pending entry, or 0. */
  private static int payloadBytes(@Nullable VersionInfo info) {
    return info == null || info.payload() == null ? 0 : info.payload().length;
  }

  /**
   * Arm the flush task, re-using an already-armed one, and force it to fire now when a cap
   * was crossed or the deferral window is over.
   *
   * <p>Records never cancel a pending flush: the flush swaps the whole pending map at fire
   * time, so the latest records are always included. Re-arming per record would pay a
   * {@code scheduleLock} round trip plus a cancel + new future per write for no behavioral
   * gain. The two re-scheduling cases are "nothing armed" (first record after a quiet
   * period) and "must fire now" (a cap crossed, or {@code maxDeferMs} elapsed).
   *
   * @param immediate {@code true} when a pending flush must fire now (a cap was crossed)
   * @throws RejectedExecutionException if the scheduler refuses the task
   */
  private void ensureFlushScheduled(boolean immediate) {
    long now = TimeSource.monotonicMillis();
    long firstAt = firstRecordAtMs;
    boolean fireNow = immediate || (firstAt != 0L && now - firstAt >= maxDeferMs);
    ScheduledFuture<?> scheduled = scheduledFlush;
    if (scheduled != null && !scheduled.isDone()) {
      // A due task is left in place instead of cancel/re-armed per over-cap or
      // over-age record; a record inside the deferral window also leaves the
      // armed task alone because the flush picks up the latest state at fire time.
      if ((!fireNow && firstAt != 0L) || scheduled.getDelay(TimeUnit.MILLISECONDS) <= 0L) {
        return;
      }
    }

    synchronized (scheduleLock) {
      // Re-read inside the lock: a flush that completed between this call's first sample and
      // the lock reset the window, and anchoring the new window to the stale sample would make
      // it look older than it is (an early urgent flush).
      long anchorNow = TimeSource.monotonicMillis();
      // The record whose entry this call observed was swapped out by a flush that
      // already cleared the window: arming a task for the empty map would anchor
      // the next record's deferral window to a cycle that carries nothing.
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
   * Merge a recorded {@code (version, payload)} pair into the pending entry:
   * the strictly newer allocation wins — together with its payload (a PUT's
   * value belongs to its version; a payload-less REFRESH merges to null).
   * Stamps are allocated by the node-local clock and merged per node, so a
   * lower number is an out-of-order record of an older write, never a
   * regression of the newest one; a regressed pending version would let a
   * flush send a REFRESH every receiver skips, pinning peers on stale data
   * until the next write or TTL.
   *
   * @param old      the currently pending entry, or {@code null}
   * @param version  the recorded version
   * @param payload  the recorded serialized value payload, or {@code null}
   * @return the entry to keep pending
   */
  private static VersionInfo mergeVersion(VersionInfo old, long version, byte@Nullable [] payload) {
    return (old == null || version > old.version()) ? new VersionInfo(version, payload) : old;
  }

  /**
   * Immediately flush all pending entries to the sync publisher.
   * Safe to call concurrently — swaps the internal map before sending, so
   * concurrent {@link #record} calls see a fresh map. With a {@link #sendExecutor}
   * the send loop is handed off asynchronously and the calling thread never blocks
   * on AMQP (design decisions: buffering and ordering); without one, sends run synchronously on the
   * calling thread.
   *
   * <p>The deferral-window timestamp resets atomically with the swap under the same
   * {@code scheduleLock}: a record landing between the two would otherwise stamp a fresh
   * window that the swap discards, and the next write after a quiet period would fire
   * immediately instead of batching.
   */
  public void flush() {
    ConcurrentHashMap<String, VersionInfo> toFlush;
    synchronized (scheduleLock) {
      // Cancel BEFORE the swap (program order: cancel → swap). A record that
      // lands in the fresh map takes ensureFlushScheduled's slow path and arms a
      // successor flush; cancelling after the swap would let it trust a flush
      // that already picked its snapshot.
      cancelScheduledFlush();
      firstRecordAtMs = 0L;

      swapLock.writeLock().lock();
      try {
        // The swap is exclusive with every merge, so no in-flight record can add to the
        // counters after this reset; reset before the emptiness test so both paths leave
        // the gauges consistent.
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

    if (sendExecutor != null) {
      submitSendChunks(toFlush);
    } else {
      doSend(toFlush.entrySet());
    }
  }

  /**
   * Hands a flushed snapshot to the send executor in bounded chunks. One
   * executor rejection drops only its own ~{@value #SEND_CHUNK_SIZE} entries
   * instead of the whole snapshot, so a single saturated window strands at most
   * one chunk of PUT/REFRESH broadcasts (each peer then converges only via its
   * own hard TTL).
   * Chunks are submitted in iteration order, so under partial rejection the
   * earliest entries are the survivors; per-key convergence is version-guarded
   * and order-independent either way (design decisions: sync messages).
   *
   * @param toFlush the swapped-out map; the write-side swap guarantees no
   *                record is still merging into it, so the send sees a
   *                quiescent snapshot
   */
  private void submitSendChunks(Map<String, VersionInfo> toFlush) {
    // A list of entries, not a new LinkedHashMap per chunk: the send loop is a
    // pure traversal, so rehashing every key into a second map only allocates
    // nodes and recomputes hashes. Each chunk is an independent list
    // (design decisions: buffering and ordering, chunk lifetime).
    int remaining = toFlush.size();
    List<Map.Entry<String, VersionInfo>> chunk = new ArrayList<>(Math.min(SEND_CHUNK_SIZE, remaining));
    for (Map.Entry<String, VersionInfo> entry : toFlush.entrySet()) {
      chunk.add(entry);
      remaining--;
      if (chunk.size() == SEND_CHUNK_SIZE) {
        submitSendChunk(chunk);
        chunk = new ArrayList<>(Math.min(SEND_CHUNK_SIZE, remaining));
      }
    }
    if (!chunk.isEmpty()) {
      submitSendChunk(chunk);
    }
  }

  /**
   * Submits one chunk to the send executor. Any submission failure — not only
   * a RejectedExecutionException: the executor is a caller-supplied SPI, so a
   * shutting-down custom pool can throw another unchecked exception — drops
   * that chunk and reports it at DEBUG instead of letting the failure escape
   * into {@link #flush()}'s callers: from record()'s scheduler-rejection
   * fallback it would abort the caller's write path after the L1/version
   * mutation.
   *
   * @param chunk the entries to send; never modified
   */
  private void submitSendChunk(Collection<Map.Entry<String, VersionInfo>> chunk) {
    try {
      // submitSendChunks is only called on the send-executor branch of flush().
      sendExecutor.execute(() -> doSend(chunk));
    } catch (RuntimeException e) {
      logExecutorDrop(chunk.size(), e);
    }
  }

  /**
   * Sends a flushed snapshot to the sync publisher, one attempt per key, never
   * aborting on a partial failure. Failures are aggregated into a single WARN
   * per flush (first exception kept) instead of a per-key WARN with a full
   * stack trace.
   *
   * @param toFlush the swapped-out entries (see {@link #submitSendChunks});
   *                never mutated by this method
   */
  private void doSend(Collection<Map.Entry<String, VersionInfo>> toFlush) {
    if (publisher == null) {
      return;
    }

    CacheSyncPublisher pub = publisher;
    long failed = 0L;
    Exception firstError = null;
    for (Map.Entry<String, VersionInfo> entry : toFlush) {
      VersionInfo info = entry.getValue();

      try {
        if (info.payload() != null) {
          // Value-carrying PUT: peers apply the carried value version-guarded,
          // no per-instance reload. A null payload falls through to the
          // version-only REFRESH (peer reloads through the cluster loader).
          pub.broadcastPut(entry.getKey(), info.version(), info.payload());
        } else {
          pub.broadcastRefresh(entry.getKey(), info.version());
        }
      } catch (Exception e) {
        failed++;
        if (firstError == null) {
          firstError = e;
        }
      }
    }

    if (failed > 0) {
      sendFailureCounter.addAndGet(failed);
      log.warn("Failed to send {} of {} put/refresh broadcasts: {}", failed, toFlush.size(), firstError.toString());
    }
  }

  /**
   * Cumulative add of put/refresh broadcasts that failed to send (broker
   * errors) since startup. Monotonic; safe to read from any thread.
   *
   * @return the total send-failure add
   */
  public long sendFailures() {
    return sendFailureCounter.get();
  }

  /**
   * Cumulative add of put/refresh broadcasts dropped because the send
   * executor refused the batch (saturated, shutting down, or a custom executor
   * threw a submission failure) since startup. Monotonic; safe to read from any
   * thread.
   *
   * @return the total executor-drop add
   */
  public long saturationDrops() {
    return saturationDropCounter.get();
  }

  /**
   * Cumulative INVALIDATE-family messages dropped because the send executor
   * refused them since startup. Monotonic; safe to read from any thread.
   * Counted per message, not per carried key, and kept out of
   * {@code zeta.stall.broadcast_storm.stopped.total} because that meter is
   * documented as put/refresh loss.
   *
   * @return the total invalidation-drop count
   */
  public long invalidatesDropped() {
    return invalidatesDropped.get();
  }

  /**
   * Reports a batch dropped because the send executor refused it.
   *
   * @param dropped the number of dropped put/refresh broadcasts
   */
  private void logExecutorDrop(int dropped, RuntimeException cause) {
    saturationDropCounter.addAndGet(dropped);
    log.debug("Send executor refused the batch, dropping {} put/refresh broadcasts", dropped, cause);
  }

  private void cancelScheduledFlush() {
    if (scheduledFlush != null && !scheduledFlush.isDone()) {
      scheduledFlush.cancel(false);
      scheduledFlush = null;
    }
  }

  /**
   * A pending sync record: the newest version for its key plus, for value-carrying
   * PUT records, the serialized value payload snapshot (immutable bytes taken on
   * the writer thread; {@code null} for a version-only REFRESH record).
   */
  record VersionInfo(long version, byte@Nullable [] payload) {}
}
