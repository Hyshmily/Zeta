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
package io.github.hyshmily.hotkey.sync.local;

import static io.github.hyshmily.hotkey.cache.cachesupport.CacheKeysPolicy.invalidCacheKey;
import static io.github.hyshmily.hotkey.constants.HotKeyConstants.*;

import io.github.hyshmily.hotkey.Internal;
import io.github.hyshmily.hotkey.constants.HotKeyConstants;
import io.github.hyshmily.hotkey.sync.worker.WorkerMessage;
import io.github.hyshmily.hotkey.util.version.VersionController;
import io.github.hyshmily.hotkey.util.version.VersionGuard;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.springframework.amqp.core.Message;

/**
 * Message between application instances for cache synchronization, carrying
 * invalidation or refresh operations for a single cache key (or a batch of keys).
 * Travels via the {@code hotkey.sync.exchange} FanoutExchange.
 *
 * <p>This is the data-plane counterpart to {@link WorkerMessage}: while
 * {@code WorkerMessage} carries hot/cool lifecycle decisions from the Worker cluster,
 * {@code SyncMessage} carries application-level data mutation events (writes, evictions)
 * between peer instances to keep L1 caches coherent.
 *
 * <p><b>Message types:</b>
 * <ul>
 *   <li>{@link #TYPE_INVALIDATE} — Remove a single key from the local cache</li>
 *   <li>{@link #TYPE_REFRESH} — Reload a single key from Redis into the local cache</li>
 *   <li>{@link #TYPE_INVALIDATE_ALL} — Batch-remove multiple keys (body is JSON array)</li>
 *   <li>{@link #TYPE_RULES_SYNC} — Synchronize the full rule set (body is rules JSON)</li>
 * </ul>
 *
 * <p>Each message carries a {@code dataVersion} and {@code isVersionDegraded} flag
 * to enable the 4-case degraded comparison in {@link VersionGuard#shouldSkipForSync}.
 *
 * @param cacheKey          the affected cache key (for single-key operations) or
 *                          the serialized payload (for batch / rules-sync types)
 * @param type              the operation type: {@link #TYPE_INVALIDATE},
 *                          {@link #TYPE_REFRESH}, {@link #TYPE_INVALIDATE_ALL},
 *                          or {@link #TYPE_RULES_SYNC}
 * @param version           the {@code dataVersion} at which the operation occurred;
 *                          see {@link VersionController#nextVersion}
 * @param isVersionDegraded whether the {@code dataVersion} was obtained in degraded mode
 *                          (node-local counter fallback, indicating Redis was unavailable)
 * @param rulesVersion      the rules version for {@code TYPE_RULES_SYNC} messages;
 *                          {@link HotKeyConstants#VERSION_DEFAULT} (0) for other types
 */
@Internal
public record SyncMessage(String cacheKey, String type, long version, boolean isVersionDegraded, long rulesVersion) {
  /** Invalidates a single cache key across all peer instances. */
  public static final String TYPE_INVALIDATE = "INVALIDATE";

  /** Refreshes a cache key from Redis across all peer instances. */
  public static final String TYPE_REFRESH = "REFRESH";

  /** Batch-invalidates multiple keys encoded as a JSON array in the message body. */
  public static final String TYPE_INVALIDATE_ALL = "INVALIDATE_ALL";

  /** Synchronizes the full rule set — receivers replace their local rules entirely. */
  public static final String TYPE_RULES_SYNC = "RULES_SYNC";

  /**
   * Message types whose body is NOT a single cache key but a payload
   * (JSON array of keys or full ruleset). The {@link #from(Message)}
   * deserializer skips the cache-key validity check for these types.
   */
  private static final List<String> BATCH_TYPES = List.of(TYPE_INVALIDATE_ALL, TYPE_RULES_SYNC);

  /**
   * Deserializes a {@code SyncMessage} from an AMQP message body and headers.
   *
   * <p>Deserialization rules:
   * <ul>
   *   <li>The cache key / payload is read from the message body (UTF-8 decoded).</li>
   *   <li>The type is read from the {@link HotKeyConstants#AMQP_HEADER_TYPE} header.</li>
   *   <li>The {@code dataVersion} is read from the {@code AMQP_HEADER_VERSION} header;
   *       defaults to {@link HotKeyConstants#VERSION_DEFAULT} (0) if missing or non-numeric.</li>
   *   <li>The degraded flag is read from the {@code AMQP_HEADER_IS_VERSION_DEGRADED} header;
   *       defaults to {@code false} if missing.</li>
   *   <li>For batch types ({@link #TYPE_INVALIDATE_ALL}, {@link #TYPE_RULES_SYNC}),
   *       the key validity check is skipped — the body carries a JSON payload rather
   *       than a single cache key.</li>
   * </ul>
   *
   * @param msg the incoming AMQP message; must not be null
   * @return a parsed {@link SyncMessage}, or {@code null} if the body is empty
   *         or the cache key is invalid for non-batch types
   */

  public static SyncMessage from(Message msg) {
    byte[] body = msg.getBody();
    if (body == null || body.length == 0) {
      return null;
    }

    String type = msg.getMessageProperties().getHeader(AMQP_HEADER_TYPE);
    String cacheKey = new String(body, StandardCharsets.UTF_8);
    if ((type == null || !BATCH_TYPES.contains(type)) && invalidCacheKey(cacheKey)) {
      return null;
    }

    long version =
      msg.getMessageProperties().getHeader(AMQP_HEADER_VERSION) instanceof Number n ? n.longValue() : VERSION_DEFAULT;
    boolean isVersionDegraded =
      msg.getMessageProperties().getHeader(AMQP_HEADER_IS_VERSION_DEGRADED) instanceof Boolean b ? b : false;
    long rulesVersion =
      msg.getMessageProperties().getHeader(AMQP_HEADER_RULES_VERSION) instanceof Number n2
        ? n2.longValue()
        : VERSION_DEFAULT;

    return new SyncMessage(cacheKey, type, version, isVersionDegraded, rulesVersion);
  }
}
