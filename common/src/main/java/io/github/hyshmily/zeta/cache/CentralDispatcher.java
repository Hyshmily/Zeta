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

import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.sync.local.SyncType;
import io.github.hyshmily.zeta.util.Internal;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongPredicate;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * Central dispatch for all external communication from the cache layer.
 * <ul>
 * <li>Counting key accesses once into the shared {@link AccessCounter}
 * (detection + Worker reporting, design decisions: access counting)</li>
 * <li>Broadcasting INVALIDATE/REFRESH/PUT operations to peer instances
 * via {@link CacheSyncPublisher}</li>
 * <li>Deferred buffering of REFRESH and value-carrying PUT messages via
 * {@link BroadcastBuffer}</li>
 * </ul>
 * <p>
 * <b>Send isolation (design decisions: buffering and ordering, invalidation):</b> every
 * INVALIDATE-family publish runs on the supplied send executor, so a slow broker never stalls the
 * calling (business) thread; PUT and REFRESH go through {@link BroadcastBuffer} and its own send
 * executor. When no executor is wired (manual construction, tests), sends run synchronously on the
 * calling thread.
 */
@Slf4j
@Internal
public class CentralDispatcher {

  private final @Nullable CacheSyncPublisher cacheSyncPublisher;

  private final BroadcastBuffer broadcastBuffer;

  /**
   * Executor for the INVALIDATE-family sends (single-key, batch, namespace).
   * {@code null} sends synchronously on the calling thread.
   */
  private final @Nullable Executor sendExecutor;

  private final AccessCounter accessCounter;

  /**
   * Cumulative INVALIDATE broadcasts dropped because the send executor refused
   * them. Monotonic; safe to read from any thread. A dropped invalidation
   * converges via the peer's hard TTL (at-most-once sync contract, design decisions: sync messages).
   */
  private final AtomicLong invalidateSendDrops = new AtomicLong();

  /**
   * Create the dispatcher. Collaborators that are absent in a deployment
   * (no sync publisher) are passed as {@code null} — plain nullable
   * references, not {@code Optional} fields.
   *
   * @param accessCounter      the shared read-path counter (detection + reporting;
   *                           never {@code null})
   * @param cacheSyncPublisher the sync publisher, or {@code null} when sync is disabled
   * @param broadcastBuffer    the deferred PUT/REFRESH send buffer (never {@code null})
   * @param sendExecutor       executor for INVALIDATE-family sends, or {@code null}
   *                           for the synchronous send
   */
  public CentralDispatcher(
    AccessCounter accessCounter,
    @Nullable CacheSyncPublisher cacheSyncPublisher,
    BroadcastBuffer broadcastBuffer,
    @Nullable Executor sendExecutor
  ) {
    this.sendExecutor = sendExecutor;
    this.cacheSyncPublisher = cacheSyncPublisher;
    this.accessCounter = Objects.requireNonNull(accessCounter, "accessCounter must not be null");
    this.broadcastBuffer = Objects.requireNonNull(broadcastBuffer, "broadcastBuffer must not be null");
  }

  /**
   * Record a cache access from the engine: the merged path when reporting is allowed,
   * detection-only when the caller suppressed Worker reporting.
   *
   * @param cacheKey the key to record
   * @param report   whether this access feeds the Worker reporting plane
   */
  public void recordAccess(String cacheKey, boolean report) {
    accessCounter.record(cacheKey, report);
  }

  /**
   * Record a cache access with an explicit recording mode ({@code DETECT_ONLY}
   * counts locally without reporting, {@code REPORT_ONLY} reports without local
   * counting, {@code SILENT} does neither).
   *
   * @param cacheKey the key to record
   * @param mode     how much of the pipeline to run (never {@code null})
   */
  public void recordAccess(String cacheKey, Zeta.TagMode mode) {
    // No null check: the public boundaries that accept a mode (DefaultZeta.tag and the annotation
    // aspect) own the contract; this is the internal forwarder.
    accessCounter.record(cacheKey, mode);
  }

  /**
   * Record {@code add} accesses on the merged path (detection and Worker
   * reporting) — the bulk form of {@link #recordAccess(String)}.
   *
   * @param cacheKey the accessed key
   * @param count    the number of accesses to add (must be positive)
   */
  public void recordAccess(String cacheKey, long count) {
    accessCounter.add(cacheKey, count);
  }

  /**
   * Broadcast a single-key INVALIDATE on the send executor, cancelling any buffered
   * PUT/REFRESH for the same key at or below {@code version} first: those records are
   * dead the moment the invalidation exists, and shipping them afterwards would
   * re-warm what the peers just removed.
   *
   * @param cacheKey the affected cache key
   * @param version  the data version the invalidation was allocated at
   */
  public void broadcastInvalidate(String cacheKey, long version) {
    CacheSyncPublisher pub = publisher("INVALIDATE");
    if (pub == null) {
      return;
    }
    broadcastBuffer.evictIfAtOrBelow(cacheKey, version);
    sendInvalidation(() -> pub.broadcastLocalInvalidate(cacheKey, version));
  }

  /**
   * The no-publisher guard shared by every broadcast shape: returns the wired
   * {@link CacheSyncPublisher}, or {@code null} after logging the plane's debug
   * line so a local-only deployment explains its silence identically at every
   * site (the message text is part of the support contract — the envelope around
   * the shared constant should be too).
   *
   * <p>Returning the reference rather than a boolean keeps the non-null
   * guarantee in the caller's local scope: the send lambdas capture it directly
   * instead of re-testing the field the guard just checked.
   *
   * @param op the operation that could not be sent (named in the debug line)
   * @return the publisher, or {@code null} when the caller must return without broadcasting
   */
  @Nullable
  private CacheSyncPublisher publisher(String op) {
    CacheSyncPublisher current = cacheSyncPublisher;
    if (current != null) {
      return current;
    }

    if (log.isDebugEnabled()) {
      log.debug("send {}: {}", op, NO_SYNC_PUBLISHER_MESSAGE);
    }
    return null;
  }

  /**
   * Broadcast a full clear: peers remove every entry whose data version is at
   * or below {@code clearStamp} (design decisions: invalidation, design decisions: cache table). One message covers the
   * whole cache regardless of how many keys the sender holds locally.
   *
   * @param clearStamp the data stamp the clear was allocated at
   */
  public void broadcastInvalidateAll(long clearStamp) {
    CacheSyncPublisher pub = publisher("INVALIDATE_ALL");
    if (pub == null) {
      return;
    }

    broadcastBuffer.evictAllIfAtOrBelow(clearStamp);
    sendInvalidation(() -> pub.broadcastInvalidateAll(clearStamp));
  }

  /**
   * Broadcast a namespace clear: peers remove every entry whose key starts with
   * {@code prefix} and whose data version is at or below {@code clearStamp}.
   * The scoped counterpart of {@link #broadcastInvalidateAll(long)} — one
   * message converges one namespace without flushing the table the other
   * namespaces live in.
   *
   * @param prefix     the physical key prefix (never null or empty)
   * @param clearStamp the data stamp the clear was allocated at
   */
  public void broadcastInvalidatePrefix(String prefix, long clearStamp) {
    CacheSyncPublisher pub = publisher("INVALIDATE_PREFIX");
    if (pub == null) {
      return;
    }

    broadcastBuffer.evictPrefixIfAtOrBelow(prefix, clearStamp);
    sendInvalidation(() -> pub.broadcastInvalidatePrefix(prefix, clearStamp));
  }

  /**
   * Broadcast one batched invalidation message carrying up to
   * {@value SyncMessage#MAX_BATCH_ENTRIES} key/version pairs (design decisions: invalidation): the wire cost of an
   * explicit multi-key invalidate drops from one message per key to one per
   * chunk. Receivers apply each entry under the same per-key version guard.
   *
   * @param entries the key/version pairs (never {@code null}/empty — the engine
   *                chunks its key set into non-empty blocks)
   */
  public void broadcastInvalidateBatch(List<SyncMessage.Entry> entries) {
    CacheSyncPublisher pub = publisher("INVALIDATE_BATCH");
    if (pub == null) {
      return;
    }

    broadcastBuffer.evictBatchIfAtOrBelow(entries);
    sendInvalidation(() -> pub.broadcastInvalidateBatch(entries));
  }

  /**
   * Run one INVALIDATE-family publish, off the calling thread when a send
   * executor is wired (design decisions: invalidation). Executor saturation or shutdown drops the
   * message with a counted DEBUG line — at-most-once sync means the
   * peer converges via the next write or its hard TTL.
   *
   * @param send the publish action (never {@code null})
   */
  private void sendInvalidation(Runnable send) {
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
    } catch (RejectedExecutionException rejected) {
      long total = invalidateSendDrops.incrementAndGet();
      log.debug(
        "INVALIDATE send dropped: send executor saturated (cumulativeDrops={}; peers converge via hard TTL)",
        total
      );
    }
  }

  /**
   * Broadcast a value-carrying PUT for a freshly written key (design decisions: sync messages).
   * <p>
   * The value is serialized into an immutable byte payload on the calling
   * thread so the buffer holds a frozen snapshot; the payload flows through
   * the same deferred {@link BroadcastBuffer} as REFRESH (same-key writes
   * merge to the newest version+value). Degrades to a value-less
   * {@link SyncType#REFRESH} when the value cannot be carried
   * (not JSON-serializable or over {@code zeta.sync.put-max-bytes}) — peers
   * reload through their own loader.
   *
   * @param cacheKey the affected cache key
   * @param value    the value just written (never {@code null} — the facade rejects null puts)
   * @param version  the data version allocated for the write
   */
  public void broadcastPut(String cacheKey, Object value, long version) {
    broadcastPut(cacheKey, value, version, null);
  }

  /**
   * Broadcast a value-carrying PUT admitted only while {@code stillCurrent} accepts its
   * version — the serialize-then-enqueue guard against a concurrent invalidation. The
   * predicate runs under the buffer's swap read side, so an invalidation that recorded its
   * tombstone first refuses the record and one that lands later evicts it.
   *
   * @param cacheKey     the affected cache key
   * @param value        the value just written (never {@code null})
   * @param version      the data version allocated for the write
   * @param stillCurrent the currentness predicate, or {@code null} to admit every record
   */
  public void broadcastPut(String cacheKey, Object value, long version, @Nullable LongPredicate stillCurrent) {
    CacheSyncPublisher pub = publisher("PUT");
    if (pub == null) {
      return;
    }
    // Serialization may answer null for a value the wire cannot carry (not
    // JSON-serializable or over the size bound): peers reload through their
    // own loader instead of receiving the value.
    broadcastBuffer.record(cacheKey, version, pub.serializePutPayload(value), stillCurrent);
  }
}
