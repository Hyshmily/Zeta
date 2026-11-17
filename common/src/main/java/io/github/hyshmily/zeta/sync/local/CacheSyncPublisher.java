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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * Publishes cache synchronization messages (INVALIDATE / REFRESH / PUT and the
 * clear family) to all peer application instances via the
 * {@code zeta.sync.exchange} FanoutExchange.
 *
 * <p>
 * This is the outbound half of the instance-to-instance cache coherence
 * protocol. The inbound half is {@link CacheSyncListener}. Together they ensure
 * that a data mutation on one instance (write, evict) is propagated to all
 * other
 * instances within the same deployment.
 *
 * <p>
 * <b>Responsibility boundary:</b> This publisher handles
 * <em>application-level</em> data changes only (a
 * {@code put}/{@code invalidate}
 * through the facade or an annotation). HOT/COOL lifecycle decisions from the
 * Worker cluster are handled separately by the Worker's
 * {@code WorkerBroadcaster} and received by {@link WorkerListener}.
 *
 * <p>
 * <b>Deduplication:</b> a {@link Caffeine} cache holds the last claimed version
 * and type per {@code cacheKey}. INVALIDATE, REFRESH and PUT share one slot because
 * they draw from the same monotonic {@code dataVersion} space; an equal-version
 * INVALIDATE still replaces a REFRESH or PUT claim.
 *
 * @see CacheSyncListener
 * @see SyncMessage
 */
@Slf4j
@Internal
public class CacheSyncPublisher {

  /** The fixed body of an INVALIDATE_ALL message, encoded once. */
  private static final byte[] CLEAR_ALL_BYTES = SyncMessage.CLEAR_ALL_BODY.getBytes(StandardCharsets.UTF_8);

  private final LongAdder sendFailureCounter = new LongAdder();

  /**
   * Dedup cache keyed by {@code cacheKey} → the last claimed version and type. All
   * sync value/removal messages for one key share the monotonic {@code dataVersion} space;
   * an equal-version INVALIDATE replaces a REFRESH or PUT claim (a removal is the safer final
   * state), while an equal-version REFRESH stays suppressed. Built once by the constructor.
   */
  private final Cache<String, BroadcastClaim> recentBroadcasts;

  private final CacheSyncProperties properties;
  private final RabbitTemplate rabbitTemplate;

  /** This instance's identity: message origin and node-id headers. */
  private final NodeIdentity identity;

  /** Serializes the value-carrying {@code PUT} body; the receive side decodes with the same codec. */
  private final PutPayloadCodec putPayloadCodec;

  /**
   * This application's {@code zeta.app-name}, stamped onto every sync
   * message so a receiver sharing the broker can drop messages that belong to a
   * <em>different</em> application (design decisions: sync messages).
   *
   * <p>
   * {@code null} means "no identity to declare": the header is omitted
   * and receivers keep processing the message, which is what rolling-upgrade
   * wiring needs. Blank is normalized to {@code null} in the constructor.
   */
  private final String appName;

  /**
   * The dedup record for one key: the version and the type that holds the claim.
   *
   * @param version the data version of the last published send
   * @param type    the type of that send
   */
  private record BroadcastClaim(long version, SyncType type) {
    /**
     * Whether this claim blocks an incoming send: a strictly newer claim always
     * wins, and an equal-version claim wins unless the incoming message is an
     * INVALIDATE upgrading a REFRESH or PUT claim.
     *
     * @param incomingVersion the incoming version
     * @param incomingType    the incoming type
     * @return {@code true} when the incoming send must be skipped
     */
    boolean outranks(long incomingVersion, SyncType incomingType) {
      return (
        version > incomingVersion ||
        (version == incomingVersion && (incomingType != SyncType.INVALIDATE || type == SyncType.INVALIDATE))
      );
    }
  }

  /**
   * Creates a fully initialized publisher, so a directly constructed instance
   * (tests, non-Spring embedding) is usable immediately. Initialization is
   * constructor-only: a container lifecycle callback would run a second time under
   * Spring and replace the dedup cache and the PUT mapper built here.
   *
   * @param rabbitTemplate the RabbitMQ template for the sync fanout exchange
   * @param properties     sync configuration (dedup window/size)
   * @param putPayloadCodec the PUT payload serializer shared with the receive side
   * @param appName        this application's name, or {@code null}/blank for none
   * @param identity       this instance's identity; never {@code null}
   */
  public CacheSyncPublisher(
    RabbitTemplate rabbitTemplate,
    CacheSyncProperties properties,
    PutPayloadCodec putPayloadCodec,
    String appName,
    NodeIdentity identity
  ) {
    this.properties = properties;
    this.putPayloadCodec = putPayloadCodec;
    this.rabbitTemplate = rabbitTemplate;
    this.identity = identity;
    this.appName = (appName != null && !appName.isBlank()) ? appName : null;
    this.recentBroadcasts = Caffeine.newBuilder()
      .expireAfterWrite(properties.getDedupWindowSeconds(), TimeUnit.SECONDS)
      .maximumSize(properties.getDedupMaxSize())
      .build();
  }

  /**
   * Cumulative count of sync messages that failed to publish since startup.
   *
   * @return the total send-failure count
   */
  public long sendFailures() {
    return sendFailureCounter.sum();
  }

  /**
   * Builds the {@link MessageProperties} common to every sync-plane message: the type header,
   * the application-name stamp (design decisions: sync messages), the time-sortable message
   * ID, the origin-instance stamp (design decisions: sync messages) and the origin node id
   * (design decisions: version stamps). Callers add their type-specific headers afterwards.
   *
   * <p>The app-name header lets a receiver on a shared broker tell applications apart: the
   * sync exchange is a fanout whose name is a global constant.
   *
   * @param type the sync message type
   * @return the prepared message properties
   */
  private MessageProperties newSyncMessageProperties(SyncType type) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, type.code());
    if (appName != null) {
      props.setHeader(HEADER_APP_NAME, appName);
    }

    props.setHeader(HEADER_MESSAGE_ID, TimeSource.currentTimeMillis());
    props.setHeader(HEADER_ORIGIN_INSTANCE, identity.instanceId());
    props.setHeader(HEADER_ORIGIN_NODE_ID, identity.nodeId());
    return props;
  }

  /**
   * Serializes a value for the value-carrying {@code PUT} broadcast into an
   * immutable byte payload. Called on the <em>writer</em> thread (the
   * transaction-commit thread for a transactional put) so the buffer holds a
   * frozen snapshot: later mutation of the caller's object by business code
   * cannot change what peers receive.
   *
   * <p>The payload is the typed value alone: the cache key travels in the
   * {@code HEADER_CACHE_KEY} header. A value that cannot be carried degrades to
   * a value-less REFRESH; {@link PutPayloadCodec#encode(Object)} owns that rule.
   *
   * @param value the freshly written value; {@code null} yields {@code null}
   * @return the serialized payload bytes, or {@code null} to signal "degrade to
   *         REFRESH"
   */
  public byte@Nullable [] serializePutPayload(@Nullable Object value) {
    return putPayloadCodec.encode(value);
  }

  /**
   * Broadcasts a full clear (design decisions: invalidation, design decisions:
   * cache table): the body is the fixed
   * {@link SyncMessage#CLEAR_ALL_BODY} marker, the version header carries the
   * {@code clearStamp}. Receivers remove every entry whose data version is at
   * or below the stamp — one message converges the whole cache regardless of
   * how many keys the sender held locally.
   *
   * <p>
   * Not deduplicated (a clear is a one-shot administrative operation with a
   * single stamp); a publish failure is counted and logged through the plane's
   * send-failure log.
   *
   * @param clearStamp the data stamp the clear was allocated at (positive)
   */
  public void broadcastInvalidateAll(long clearStamp) {
    if (clearStamp <= 0L) {
      return;
    }
    publishSync(SyncType.INVALIDATE_ALL, CLEAR_ALL_BYTES, clearStamp);
  }

  /**
   * Broadcasts a scoped clear: the body is the physical key prefix, the version
   * header carries the {@code clearStamp}. Receivers remove every entry under
   * the prefix whose data version is at or below the stamp and record the
   * scoped watermark, so pre-clear PUT/REFRESH messages cannot re-warm the
   * namespace the sender just emptied — without touching the other namespaces
   * that share the same L1 table.
   *
   * <p>
   * Not deduplicated (a clear is a one-shot administrative operation with a
   * single stamp); a publish failure is counted and logged through the plane's
   * send-failure log.
   *
   * @param prefix     the physical key prefix (non-empty)
   * @param clearStamp the data stamp the clear was allocated at (positive)
   */
  public void broadcastInvalidatePrefix(String prefix, long clearStamp) {
    if (invalidCacheKey(prefix) || clearStamp <= 0L) {
      return;
    }
    publishSync(SyncType.INVALIDATE_PREFIX, prefix.getBytes(StandardCharsets.UTF_8), clearStamp);
  }

  /**
   * Broadcasts batched INVALIDATE messages (design decisions: invalidation): the body is a
   * JSON array of {@code [key, version]} pairs (see {@link SyncMessage#encodeBatchBody}).
   * Receivers apply each entry under the same per-key version guard as a single-key
   * INVALIDATE — the wire cost of an explicit multi-key invalidate drops from one message
   * per key to one per {@link SyncMessage#MAX_BATCH_ENTRIES}-entry chunk.
   *
   * <p>Entries are filtered at this wire boundary and split into chunks; when one chunk
   * cannot be encoded it degrades to per-key INVALIDATE sends, so no accepted entry is
   * silently lost. Not deduplicated (each entry carries its own stamp and the per-key
   * receiver guard subsumes dedup); a publish failure is counted and logged through the
   * plane's send-failure log.
   *
   * @param entries the key/version pairs; {@code null}/empty is a no-op, invalid entries
   *                (null, blank key, non-positive version) are dropped
   */
  public void broadcastInvalidateBatch(List<SyncMessage.Entry> entries) {
    if (entries == null || entries.isEmpty()) {
      return;
    }
    List<SyncMessage.Entry> chunk = new ArrayList<>(Math.min(entries.size(), SyncMessage.MAX_BATCH_ENTRIES));
    for (SyncMessage.Entry entry : entries) {
      if (entry == null || invalidCacheKey(entry.key()) || entry.version() <= 0L) {
        continue;
      }
      chunk.add(entry);
      if (chunk.size() == SyncMessage.MAX_BATCH_ENTRIES) {
        publishBatchChunk(chunk);
        // publishBatchChunk encodes and publishes synchronously and keeps no reference.
        chunk.clear();
      }
    }
    if (!chunk.isEmpty()) {
      publishBatchChunk(chunk);
    }
  }

  /**
   * Publishes one bounded batch chunk, degrading to per-key sends when the frame cannot be
   * encoded. The fallback preserves the {@link SyncMessage#encodeBatchBody} contract.
   *
   * @param chunk the non-empty chunk, at most {@link SyncMessage#MAX_BATCH_ENTRIES} entries
   */
  private void publishBatchChunk(List<SyncMessage.Entry> chunk) {
    byte[] body = SyncMessage.encodeBatchBody(chunk);
    if (body != null) {
      publishSync(SyncType.INVALIDATE_BATCH, body, 0L);
      return;
    }
    for (SyncMessage.Entry entry : chunk) {
      broadcastInvalidate(entry.key(), entry.version());
    }
  }

  /**
   * Broadcasts a value-carrying PUT sync message: the body is the serialized value payload, the
   * real cache key rides in the {@code cacheKey} header so receivers keep per-key ordering on
   * the shared ordered dispatcher and filter their own PUTs like self-REFRESH.
   *
   * <p>The PUT claims its key at {@code version}, so a same- or lower-version REFRESH is
   * suppressed (the value is already on the wire) while an equal-version INVALIDATE still
   * replaces the claim. A publish failure releases the claim, so a later REFRESH within the
   * window is not lost; it is counted and logged and does not propagate — the same self-healing
   * contract as a lost REFRESH.
   *
   * @param cacheKey the affected cache key; must not be null or empty
   * @param version  the positive {@code dataVersion} at which the value was written
   * @param payload  the serialized value payload from {@link #serializePutPayload}
   */
  public void broadcastPut(String cacheKey, long version, byte[] payload) {
    if (payload == null || payload.length == 0) {
      return;
    }
    publishDeduped(cacheKey, SyncType.PUT, version, payload, cacheKey);
  }

  /**
   * Broadcasts a REFRESH sync message to all peer instances, triggering them
   * to reload the value for the given key from the data source.
   *
   * <p>
   * Deduplicated: if a sync message for the same key with a higher version — or an
   * equal version it cannot outrank — was recorded within the dedup window, this call
   * is silently skipped.
   *
   * @param cacheKey the affected cache key; must not be null or empty
   * @param version  the {@code dataVersion} at which the operation occurred;
   *                 see {@link StampClock#nextStamp()}
   */
  public void broadcastRefresh(String cacheKey, long version) {
    sendDeduped(cacheKey, SyncType.REFRESH, version);
  }

  /**
   * Broadcasts an INVALIDATE sync message to all peer instances, requesting them
   * to remove the specified key from their local cache.
   *
   * <p>
   * Deduplicated: if a sync message for the same key with a higher version — or an
   * equal-version INVALIDATE — was recorded within the dedup window, this call is
   * silently skipped. An equal-version INVALIDATE replaces a REFRESH or PUT claim.
   *
   * @param cacheKey the affected cache key; must not be null or empty
   * @param version  the {@code dataVersion} at which the invalidation occurred
   */
  public void broadcastInvalidate(String cacheKey, long version) {
    sendDeduped(cacheKey, SyncType.INVALIDATE, version);
  }

  /**
   * Unified sync-plane publish contract shared by every send site (the deduped
   * single-key INVALIDATE/REFRESH send, the value-carrying PUT and the clear family):
   * build the common {@link #newSyncMessageProperties(SyncType) sync properties},
   * stamp the version and — for PUT — the cache-key header, publish on the fanout
   * exchange, and swallow any publish failure into one WARN — the
   * documented contract is "failures are logged and do not propagate to the caller".
   *
   * <p>{@code RuntimeException} covers {@code AmqpException} and any unchecked
   * failure from a closed/initializing template.
   *
   * @param type     the sync message type header value
   * @param body     the pre-serialized message body (UTF-8 bytes)
   * @param version  the data version header, or {@code 0} for a message that carries none
   * @param cacheKey the per-key PUT header, or {@code null} for none
   * @return {@code true} if the message was published, {@code false} on failure
   */
  private boolean publishSync(SyncType type, byte[] body, long version, @Nullable String cacheKey) {
    try {
      MessageProperties props = newSyncMessageProperties(type);
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
      log.warn("Failed to send {} sync message", type, e);
      return false;
    }
  }

  /**
   * A message without a cache-key header (REFRESH, INVALIDATE, the clear family).
   *
   * @param type    the sync message type header value
   * @param body    the pre-serialized message body
   * @param version the data version header, or {@code 0} for none
   * @return {@code true} if the message was published, {@code false} on failure
   */
  private boolean publishSync(SyncType type, byte[] body, long version) {
    return publishSync(type, body, version, null);
  }

  /**
   * The single-key body/header wrapper: a null/blank key cannot be applied by any peer, so it
   * is dropped before the UTF-8 body is built; a valid key delegates to {@link #publishDeduped}.
   *
   * @param cacheKey the affected cache key
   * @param type     the sync message type
   * @param version  the {@code dataVersion} of the operation
   */
  private void sendDeduped(String cacheKey, SyncType type, long version) {
    if (invalidCacheKey(cacheKey)) {
      log.debug("Invalid cache key, skip sync: cacheKey={}, type={}", cacheKey, type);
      return;
    }
    publishDeduped(cacheKey, type, version, cacheKey.getBytes(StandardCharsets.UTF_8), null);
  }

  /**
   * The shared deduplicated send: only publishes if no recent send for the key with a greater
   * version — or an equal version that cannot be outranked — exists in the dedup window.
   * An equal-version INVALIDATE replaces a REFRESH/PUT claim; a failed publish releases the
   * claim so a retry inside the window is still possible.
   *
   * <p>Concurrent callers may briefly race past the check and duplicate one broadcast; the
   * plane is at-most-once and self-heals. The claim expires after
   * {@link CacheSyncProperties#getDedupWindowSeconds} seconds.
   *
   * @param cacheKey       the affected key
   * @param type           the sync operation
   * @param version        the positive data stamp
   * @param body           the pre-serialized body
   * @param headerCacheKey the PUT cache-key header, or {@code null}
   */
  private void publishDeduped(
    String cacheKey,
    SyncType type,
    long version,
    byte[] body,
    @Nullable String headerCacheKey
  ) {
    if (invalidCacheKey(cacheKey) || version <= 0L) {
      return;
    }
    BroadcastClaim claim = claim(cacheKey, version, type);
    if (claim == null) {
      log.debug(
        "Skip {} due to recent send with same or newer version: cacheKey={}, version={}",
        type,
        cacheKey,
        version
      );
      return;
    }
    if (publishSync(type, body, version, headerCacheKey)) {
      log.debug("Sync sent: type={}, cacheKey={}, version={}", type, cacheKey, version);
    } else {
      release(cacheKey, claim);
    }
  }

  /**
   * Atomically checks and stores one key's claim. A strictly newer claim, or an equal
   * version the incoming type cannot outrank, wins and returns {@code null}.
   *
   * @param cacheKey the affected cache key
   * @param version  the incoming data version
   * @param type     the incoming sync type
   * @return the stored claim, or {@code null} when a recent claim outranks this send
   */
  private @Nullable BroadcastClaim claim(String cacheKey, long version, SyncType type) {
    BroadcastClaim candidate = new BroadcastClaim(version, type);
    BroadcastClaim claimed = recentBroadcasts
      .asMap()
      .compute(cacheKey, (k, current) -> current != null && current.outranks(version, type) ? current : candidate);
    return claimed == candidate ? candidate : null;
  }

  /**
   * Releases a claim only if it is still the current one, so a newer claim from a
   * concurrent sender is never removed by an older failed publish.
   *
   * @param cacheKey the affected cache key
   * @param claim    the claim to release
   */
  private void release(String cacheKey, BroadcastClaim claim) {
    recentBroadcasts.asMap().remove(cacheKey, claim);
  }
}
