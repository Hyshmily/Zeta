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
package io.github.hyshmily.hotkey.sync;

import org.springframework.amqp.core.Message;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static io.github.hyshmily.hotkey.cache.CacheKeysPolicy.invalidCacheKey;
import static io.github.hyshmily.hotkey.constants.HotKeyConstants.*;

/**
 * Message between app instances for cache synchronization (INVALIDATE / REFRESH).
 * Travels via {@code hotkey.sync.exchange} (FanoutExchange).
 *
 * @param cacheKey         the affected cache key
 * @param type             the operation type ({@link #TYPE_INVALIDATE} or {@link #TYPE_REFRESH})
 * @param version          the {@code dataVersion} at which the operation occurred
 * @param isVersionDegraded whether the dataVersion was obtained in degraded mode (node-local counter fallback)
 */
public record SyncMessage(String cacheKey, String type, long version, boolean isVersionDegraded) {
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
   * Deserialize a {@code SyncMessage} from an AMQP message body and headers.
   * <p>
   * The cache key is read from the message body (UTF-8 decoded). The type,
   * version, and degraded flag are read from message headers. For batch types
   * ({@link #TYPE_INVALIDATE_ALL}, {@link #TYPE_RULES_SYNC}), the key validity
   * check is skipped — those types carry a payload rather than a single key.
   * <p>
   * Returns {@code null} when the body is empty or the cache key is invalid
   * (for non-batch types).
   *
   * @param msg the incoming AMQP message; must not be null
   * @return a parsed {@link SyncMessage}, or {@code null} if the body is empty
   *         or the key is invalid
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

    return new SyncMessage(cacheKey, type, version, isVersionDegraded);
  }
}
