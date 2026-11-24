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
package io.github.hyshmily.zeta.sync.local;

import static io.github.hyshmily.zeta.cache.CacheKeysPolicy.invalidCacheKey;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.*;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.hyshmily.zeta.sync.worker.WorkerListener;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.NodeIdentity;
import io.github.hyshmily.zeta.util.TimeSource;
import io.github.hyshmily.zeta.util.clock.StampClock;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.jspecify.annotations.Nullable;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * Outbound publisher of peer cache synchronization messages (INVALIDATE, REFRESH, PUT, and clear
 * operations) over the {@code zeta.sync.exchange} fanout exchange.
 *
 * <p>Handles application-level data mutations only; Worker-driven HOT/COOL lifecycle broadcasts
 * are delivered separately to {@link WorkerListener}. Single-key INVALIDATE, REFRESH, and PUT
 * sends share one per-key deduplication cache over the monotonic {@code dataVersion} space, where
 * strictly newer versions win and equal-version priority follows
 * {@code INVALIDATE > PUT > REFRESH}.
 *
 * @see CacheSyncListener
 * @see SyncMessage
 */
@Internal
public class CacheSyncPublisher {

  /** Pre-encoded UTF-8 body for {@link SyncType#INVALIDATE_ALL} broadcasts. */
  private static final byte[] CLEAR_ALL_BYTES = SyncMessage.CLEAR_ALL_BODY.getBytes(StandardCharsets.UTF_8);

  private final RabbitTemplate rabbitTemplate;
  private final CacheSyncProperties properties;
  private final PutPayloadCodec putPayloadCodec;
  private final NodeIdentity identity;

  /** Normalized application name stamped for shared-broker isolation, or {@code null} if omitted. */
  private final @Nullable String appName;

  /** Per-key deduplication window tracking the highest claimed {@link BroadcastClaim}. */
  private final Cache<String, BroadcastClaim> recentBroadcasts;

  private final LongAdder sendFailureCounter = new LongAdder();

  /**
   * Creates a fully initialized {@link CacheSyncPublisher}.
   *
   * @param rabbitTemplate  the RabbitMQ template publishing to the sync fanout exchange
   * @param properties      sync configuration supplying exchange name and dedup bounds
   * @param putPayloadCodec shared codec serializing value-carrying PUT payloads
   * @param appName         this application's name for shared-broker isolation, or {@code null}/blank
   * @param identity        this instance's identity for origin instance and node-id headers
   * @throws NullPointerException if any required collaborator is {@code null}
   */
  public CacheSyncPublisher(
    RabbitTemplate rabbitTemplate,
    CacheSyncProperties properties,
    PutPayloadCodec putPayloadCodec,
    @Nullable String appName,
    NodeIdentity identity
  ) {
    this.rabbitTemplate = Objects.requireNonNull(rabbitTemplate, "rabbitTemplate must not be null");
    this.properties = Objects.requireNonNull(properties, "properties must not be null");
    this.putPayloadCodec = Objects.requireNonNull(putPayloadCodec, "putPayloadCodec must not be null");
    this.identity = Objects.requireNonNull(identity, "identity must not be null");
    // Normalize blank to null so rolling-upgrade receivers keep processing unlabelled messages.
    this.appName = (appName != null && !appName.isBlank()) ? appName : null;
    this.recentBroadcasts = Caffeine.newBuilder()
      .expireAfterWrite(properties.getDedupWindowSeconds(), TimeUnit.SECONDS)
      .maximumSize(properties.getDedupMaxSize())
      .build();
  }

  /**
   * Serializes a freshly written value into an immutable byte payload on the writer thread so
   * subsequent caller mutations cannot alter what peers receive.
   *
   * @param value the written value, or {@code null}
   * @return the serialized bytes, or {@code null} to signal degrading to a value-less REFRESH
   */
  public byte@Nullable [] serializePutPayload(@Nullable Object value) {
    return putPayloadCodec.encode(value);
  }

  /**
   * Broadcasts a value-carrying PUT sync message with the cache key in {@code HEADER_CACHE_KEY}
   * and the serialized value in the body, subject to per-key version deduplication.
   *
   * @param cacheKey the affected physical cache key
   * @param version  the positive {@code dataVersion} of the write
   * @param payload  the serialized value payload from {@link #serializePutPayload}
   * @return {@code true} if published or intentionally skipped, {@code false} on broker failure
   */
  public boolean broadcastPut(String cacheKey, long version, byte@Nullable [] payload) {
    if (payload == null || payload.length == 0 || invalidCacheKey(cacheKey) || version <= 0L) {
      return true;
    }
    return publishDeduped(cacheKey, SyncType.PUT, version, payload);
  }

  /**
   * Broadcasts a versioned REFRESH sync message triggering peers to reload {@code cacheKey},
   * skipping sends outranked within the deduplication window.
   *
   * @param cacheKey the affected physical cache key
   * @param version  the positive {@code dataVersion} from {@link StampClock#nextStamp()}
   * @return {@code true} if published or intentionally skipped, {@code false} on broker failure
   */
  public boolean broadcastRefresh(String cacheKey, long version) {
    return publishDeduped(cacheKey, SyncType.REFRESH, version, null);
  }

  /**
   * Broadcasts a versioned INVALIDATE sync message requesting peers to remove {@code cacheKey}.
   * An equal-version INVALIDATE replaces any prior PUT or REFRESH deduplication claim.
   *
   * @param cacheKey the affected physical cache key
   * @param version  the positive {@code dataVersion} of the invalidation
   * @return {@code true} if published or intentionally skipped, {@code false} on broker failure
   */
  public boolean broadcastInvalidate(String cacheKey, long version) {
    return publishDeduped(cacheKey, SyncType.INVALIDATE, version, null);
  }

  /**
   * Broadcasts multi-key invalidations in chunks of at most {@link SyncMessage#MAX_BATCH_ENTRIES}
   * entries without deduplication, falling back to per-key INVALIDATE messages if a chunk cannot
   * be frame-encoded.
   *
   * @param entries key/version pairs to invalidate; {@code null}/empty is a no-op and invalid
   *                entries are skipped
   * @return {@code true} if every accepted entry published or was skipped, {@code false} if any
   *         send failed
   */
  public boolean broadcastInvalidateBatch(@Nullable List<SyncMessage.Entry> entries) {
    if (entries == null || entries.isEmpty()) {
      return true;
    }
    boolean published = true;
    List<SyncMessage.Entry> chunk = new ArrayList<>(Math.min(entries.size(), SyncMessage.MAX_BATCH_ENTRIES));
    for (SyncMessage.Entry entry : entries) {
      if (entry == null || invalidCacheKey(entry.key()) || entry.version() <= 0L) {
        continue;
      }
      chunk.add(entry);
      if (chunk.size() == SyncMessage.MAX_BATCH_ENTRIES) {
        published &= publishBatchChunk(chunk);
        // Safe to reuse: publishBatchChunk encodes and sends synchronously without retaining chunk.
        chunk.clear();
      }
    }
    if (!chunk.isEmpty()) {
      published &= publishBatchChunk(chunk);
    }
    return published;
  }

  /**
   * Broadcasts a prefix-scoped clear removing peer entries under {@code prefix} at or below
   * {@code clearStamp} and recording a scoped watermark. Not deduplicated.
   *
   * @param prefix     the non-blank physical key prefix
   * @param clearStamp the positive data stamp allocated for the clear
   * @return {@code true} if published or intentionally skipped, {@code false} on broker failure
   */
  public boolean broadcastInvalidatePrefix(String prefix, long clearStamp) {
    if (invalidCacheKey(prefix) || clearStamp <= 0L) {
      return true;
    }
    return publishSync(SyncType.INVALIDATE_PREFIX, prefix.getBytes(StandardCharsets.UTF_8), clearStamp, null);
  }

  /**
   * Broadcasts a table-wide clear removing peer entries at or below {@code clearStamp}. Not
   * deduplicated.
   *
   * @param clearStamp the positive data stamp allocated for the clear
   * @return {@code true} if published or intentionally skipped, {@code false} on broker failure
   */
  public boolean broadcastInvalidateAll(long clearStamp) {
    if (clearStamp <= 0L) {
      return true;
    }
    return publishSync(SyncType.INVALIDATE_ALL, CLEAR_ALL_BYTES, clearStamp, null);
  }

  /** Returns the cumulative count of sync messages that failed to publish since startup. */
  public long sendFailures() {
    return sendFailureCounter.sum();
  }

  /**
   * Encodes and publishes one bounded batch chunk, degrading to individual INVALIDATE broadcasts
   * if {@link SyncMessage#encodeBatchBody} cannot encode the frame.
   *
   * @param chunk non-empty chunk of at most {@link SyncMessage#MAX_BATCH_ENTRIES} valid entries
   * @return {@code true} if all entries in the chunk published cleanly
   */
  private boolean publishBatchChunk(List<SyncMessage.Entry> chunk) {
    byte[] body = SyncMessage.encodeBatchBody(chunk);
    if (body != null) {
      return publishSync(SyncType.INVALIDATE_BATCH, body, 0L, null);
    }
    // Frame encoding failed; fall back to un-deduplicated per-key INVALIDATEs so no entry is lost
    // and the failed chunk leaves no claims behind in the dedup window.
    boolean published = true;
    for (SyncMessage.Entry entry : chunk) {
      published &= publishSync(
        SyncType.INVALIDATE,
        entry.key().getBytes(StandardCharsets.UTF_8),
        entry.version(),
        null
      );
    }
    return published;
  }

  /**
   * Claims the per-key deduplication slot before publishing and releases the claim on broker
   * failure so a subsequent retry within the window is not suppressed. An invalid key or
   * non-positive version is a successful no-op: the caller's write already committed, and a
   * malformed broadcast is worse than a dropped one. The claim is taken before the key is encoded,
   * so a suppressed send allocates nothing.
   *
   * @param cacheKey the affected physical key
   * @param type     the sync operation type
   * @param version  the positive data stamp
   * @param payload  the PUT payload bytes, or {@code null} when the key itself is the body
   * @return {@code true} if published or suppressed by an equal/newer claim, {@code false} on failure
   */
  private boolean publishDeduped(String cacheKey, SyncType type, long version, byte@Nullable [] payload) {
    if (invalidCacheKey(cacheKey) || version <= 0L) {
      return true;
    }
    BroadcastClaim claim = claim(cacheKey, version, type);
    if (claim == null) {
      return true;
    }
    // PUT carries the value in the body and passes the key via HEADER_CACHE_KEY; every other type
    // carries its key as the body.
    byte[] body = payload != null ? payload : cacheKey.getBytes(StandardCharsets.UTF_8);
    if (publishSync(type, body, version, type == SyncType.PUT ? cacheKey : null)) {
      return true;
    }
    // Release only our exact claim so a newer concurrent broadcast's claim stays intact.
    recentBroadcasts.asMap().remove(cacheKey, claim);
    return false;
  }

  /**
   * Installs a new {@link BroadcastClaim} unless an existing claim outranks it. The fast read
   * rejects a duplicate without touching the map; the CAS loop installs the winner without the
   * expire-after-write re-arm that {@code Map.compute} performs even when it returns the resident
   * instance. Only the winning caller may publish, and only it may release the claim on a broker
   * failure. A losing candidate leaves the stronger claim in place.
   *
   * @return the newly installed claim, or {@code null} when an equal-or-newer claim suppressed it
   */
  private @Nullable BroadcastClaim claim(String cacheKey, long version, SyncType type) {
    BroadcastClaim candidate = new BroadcastClaim(version, type);
    BroadcastClaim current = recentBroadcasts.getIfPresent(cacheKey);
    if (current != null && current.outranks(version, type)) {
      return null;
    }
    ConcurrentMap<String, BroadcastClaim> claims = recentBroadcasts.asMap();
    for (;;) {
      BroadcastClaim existing = claims.putIfAbsent(cacheKey, candidate);
      if (existing == null) {
        return candidate;
      }
      if (existing.outranks(version, type)) {
        return null;
      }
      // A same-or-newer claim loses to this send; replace it without re-arming the entry's
      // expire-after-write window (a losing compute would have).
      if (claims.replace(cacheKey, existing, candidate)) {
        return candidate;
      }
    }
  }

  /**
   * Sends one sync message on the fanout exchange and increments {@link #sendFailureCounter} on
   * any unchecked broker error without logging (callers aggregate batch-level warnings).
   *
   * @param type     the sync message type
   * @param body     the serialized message body
   * @param version  the data version header, or {@code 0} when omitted
   * @param cacheKey the {@code HEADER_CACHE_KEY} value for PUT messages, or {@code null}
   * @return {@code true} on success, {@code false} on failure
   */
  private boolean publishSync(SyncType type, byte[] body, long version, @Nullable String cacheKey) {
    try {
      MessageProperties props = new MessageProperties();
      props.setHeader(HEADER_TYPE, type.code());
      if (appName != null) {
        props.setHeader(HEADER_APP_NAME, appName);
      }
      props.setHeader(HEADER_MESSAGE_ID, TimeSource.currentTimeMillis());
      props.setHeader(HEADER_ORIGIN_INSTANCE, identity.instanceId());
      props.setHeader(HEADER_ORIGIN_NODE_ID, identity.nodeId());
      if (version > 0L) {
        props.setHeader(HEADER_VERSION, version);
      }
      if (cacheKey != null) {
        props.setHeader(HEADER_CACHE_KEY, cacheKey);
      }
      rabbitTemplate.send(properties.getExchangeName(), "", new Message(body, props));
      return true;
    } catch (RuntimeException e) {
      sendFailureCounter.increment();
      return false;
    }
  }

  /**
   * Per-key deduplication record tracking the last claimed version and sync type.
   *
   * @param version the data version of the claimed send
   * @param type    the sync type of the claimed send
   */
  private record BroadcastClaim(long version, SyncType type) {
    /**
     * Whether this claim suppresses an incoming {@code (version, type)} send. A strictly newer
     * version wins outright; on an equal version the higher-priority type wins, so a control
     * message is not dropped by a value carrying the same stamp.
     */
    private boolean outranks(long incomingVersion, SyncType incomingType) {
      return version != incomingVersion ? version > incomingVersion : type.priority() >= incomingType.priority();
    }
  }
}
