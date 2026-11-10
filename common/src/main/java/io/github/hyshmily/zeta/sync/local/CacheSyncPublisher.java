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
 * and type per {@code cacheKey}. INVALIDATE and REFRESH share one slot because
 * they draw from the same monotonic {@code dataVersion} space; an equal-version
 * INVALIDATE still replaces a REFRESH claim.
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
   * sync value/removal messages for one key share the monotonic {@code dataVersion}
   * space, so one slot suppresses a stale REFRESH that a concurrent INVALIDATE at a
   * higher version already outranks. Keeping the type lets an equal-version INVALIDATE
   * replace a REFRESH claim (a removal is the safer final state) while an equal-version
   * REFRESH stays suppressed. Built once by the constructor.
   */
  private final Cache<String, BroadcastClaim> recentBroadcasts;

  private final CacheSyncProperties properties;
  private final RabbitTemplate rabbitTemplate;

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
     * INVALIDATE upgrading a REFRESH.
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
   */
  public CacheSyncPublisher(
    RabbitTemplate rabbitTemplate,
    CacheSyncProperties properties,
    PutPayloadCodec putPayloadCodec,
    String appName
  ) {
    this.properties = properties;
    this.putPayloadCodec = putPayloadCodec;
    this.rabbitTemplate = rabbitTemplate;
    this.appName = (appName != null && !appName.isBlank()) ? appName : null;
    this.recentBroadcasts = Caffeine.newBuilder()
      .expireAfterWrite(properties.getDedupWindowSeconds(), TimeUnit.SECONDS)
      .maximumSize(properties.getDedupMaxSize())
      .build();
  }

  /**
   * Cumulative add of sync messages that failed to publish since startup.
   *
   * @return the total send-failure add
   */
  public long sendFailures() {
    return sendFailureCounter.sum();
  }

  /**
   * Builds the {@link MessageProperties} assignment common to every sync-plane
   * message: the type header, the application-name stamp (design decisions: sync messages),
   * the time-sortable message ID, the origin-instance stamp
   * (design decisions: dedup lifecycle) and the origin node id used for collision
   * detection
   * (design decisions: version stamps). Callers add their type-specific headers
   * (version, cache key, ...)
   * afterwards.
   *
   * <p>The application name is what lets a receiver identify the sender: the sync exchange is a
   * {@code FanoutExchange} whose name is a global constant, so RabbitMQ fans every
   * INVALIDATE / REFRESH / PUT out to <em>every</em> instance on the broker — including instances
   * of other applications (see the receiver-side filter in {@link CacheSyncListener}).
   *
   * @param type the sync message type
   * @return the prepared message properties
   */
  private MessageProperties newSyncMessageProperties(SyncType type) {
    MessageProperties props = new MessageProperties();
    props.setHeader(HEADER_TYPE, type.code());
    // No-op when this instance declares no application name, so the header is absent and
    // receivers process the message (rolling-upgrade compatibility).
    if (appName != null) {
      props.setHeader(HEADER_APP_NAME, appName);
    }
    // Trace-only ID (no plane compares it for ordering), so it must not contend
    // with the data-version clock: a stamp-clock allocation would pay one CAS
    // and one sequence number of the shared 4096/ms window. The cached
    // TimeSource read keeps the ID time-sortable at zero contention.
    props.setHeader(HEADER_MESSAGE_ID, TimeSource.currentTimeMillis());
    props.setHeader(HEADER_ORIGIN_INSTANCE, NodeIdentity.getInstanceId());
    props.setHeader(HEADER_ORIGIN_NODE_ID, NodeIdentity.getBoxedNodeId());
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
    if (prefix == null || prefix.isEmpty() || clearStamp <= 0L) {
      return;
    }
    publishSync(SyncType.INVALIDATE_PREFIX, prefix.getBytes(StandardCharsets.UTF_8), clearStamp);
  }

  /**
   * Broadcasts one batched INVALIDATE message (design decisions: invalidation):
   * the body is a JSON
   * array of {@code [key, version]} pairs (see
   * {@link SyncMessage#encodeBatchBody}). Receivers apply each entry under the
   * same per-key version guard as a single-key INVALIDATE — the wire cost of
   * an explicit multi-key invalidate drops from one message per key to one per
   * {@link SyncMessage#MAX_BATCH_ENTRIES}-entry chunk.
   *
   * <p>
   * Not deduplicated (each entry carries its own stamp and the per-key
   * receiver guard subsumes dedup); a publish failure is counted and logged
   * through the plane's send-failure log.
   *
   * @param entries the key/version pairs; must not be null or empty, at most
   *                {@link SyncMessage#MAX_BATCH_ENTRIES} entries
   */
  public void broadcastInvalidateBatch(List<SyncMessage.Entry> entries) {
    if (entries == null || entries.isEmpty() || entries.size() > SyncMessage.MAX_BATCH_ENTRIES) {
      return;
    }
    byte[] body = SyncMessage.encodeBatchBody(entries);
    if (body != null) {
      publishSync(SyncType.INVALIDATE_BATCH, body, 0L);
    }
  }

  /**
   * Broadcasts a value-carrying PUT sync message to all peer instances: the
   * body is the serialized value payload, the real cache key rides in the
   * {@code cacheKey} header so receivers keep per-key ordering on the shared
   * ordered dispatcher and filter their own PUTs like self-REFRESH.
   *
   * <p>
   * Not deduplicated via the version-claim window: a PUT is sent once per
   * write by the {@code BroadcastBuffer} flush (which merges same-key writes
   * to the newest version+value), so there is no duplicate source for the
   * claim to suppress. A publish failure is counted, logged through the plane's
   * send-failure log and does not propagate — the receiving peers
   * keep their current value until the next write, the same self-healing
   * contract as a lost REFRESH.
   *
   * @param cacheKey the affected cache key; must not be null or empty
   * @param version  the {@code dataVersion} at which the value was written
   * @param payload  the serialized value payload from
   *                 {@link #serializePutPayload}
   */
  public void broadcastPut(String cacheKey, long version, byte[] payload) {
    if (invalidCacheKey(cacheKey) || payload == null || payload.length == 0) {
      return;
    }
    publishSync(SyncType.PUT, payload, version, cacheKey);
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
   * silently skipped. An equal-version INVALIDATE replaces a REFRESH claim.
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
   * Core deduplicated send implementation: only publishes if no recent send for
   * the key with a greater version — or an equal version that cannot be
   * outranked by this message — exists in the dedup window.
   *
   * <p>
   * <b>Dedup algorithm:</b>
   * <ol>
   * <li>Atomically claim the key's slot: a strictly newer claim skips this send;
   * an equal-version claim skips a REFRESH but is replaced by an INVALIDATE.</li>
   * <li>Otherwise the message is published <em>outside</em> the cache lock and,
   * on success, the claimed version stays recorded. A publish failure removes the
   * claim, so a retry within the same window is still possible.</li>
   * </ol>
   *
   * <p>
   * Concurrent callers with the same key may briefly race past the check and
   * produce duplicate broadcasts. This is acceptable: the system uses
   * at-most-once delivery and self-healing via periodic cycles.
   *
   * <p>
   * The dedup cache entry expires after
   * {@link CacheSyncProperties#getDedupWindowSeconds}
   * seconds, after which a subsequent call with any version will be accepted.
   *
   * @param cacheKey the affected cache key; must not be null or empty
   * @param type     the sync message type (e.g. {@link SyncType#INVALIDATE},
   *                 {@link SyncType#REFRESH})
   * @param version  the {@code dataVersion} of the operation
   */
  private void sendDeduped(String cacheKey, SyncType type, long version) {
    // The sync plane is the wire boundary: a null/blank key cannot be applied by any
    // peer, so it is dropped here even though the internal callers already validated it.
    if (invalidCacheKey(cacheKey)) {
      log.debug("Invalid cache key, skip sync: cacheKey={}, type={}", cacheKey, type);
      return;
    }

    // Atomic claim: check-and-store in one compute so two concurrent senders
    // with versions 5 and 3 cannot both pass the check and let the stale 3
    // land last (which would have allowed a later duplicate send of 4). Returning
    // the exact candidate identifies the winner without a side-channel flag.
    BroadcastClaim candidate = new BroadcastClaim(version, type);
    BroadcastClaim claimed = recentBroadcasts
      .asMap()
      .compute(cacheKey, (k, current) -> current != null && current.outranks(version, type) ? current : candidate);

    if (claimed != candidate) {
      log.debug(
        "Skip sync due to recent send with same or newer version: cacheKey={}, newVersion={}",
        cacheKey,
        version
      );
      return;
    }

    if (publishSync(type, cacheKey.getBytes(StandardCharsets.UTF_8), version)) {
      // Success — the compute already stored the claim.
      log.debug("Sync sent: cacheKey={}, version={}", cacheKey, version);
    } else {
      // Publish failed — drop this claim outright. Rolling it back to the
      // displaced version was wrong: that version's own send may equally have
      // failed (or still be in flight), and restoring it left an unsent version
      // recorded as sent for the whole dedup window — a retry of it was then
      // silently skipped. Removing the claim at worst re-sends one idempotent
      // broadcast (at-most-once sync contract); keeping a phantom claim can
      // lose the mutation for the window.
      recentBroadcasts.asMap().remove(cacheKey, candidate);
    }
  }
}
