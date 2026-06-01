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
package io.github.hyshmily.hotkey.broadcast;

import static io.github.hyshmily.hotkey.constant.HotKeyConstants.*;
import static io.github.hyshmily.hotkey.hotkeycache.CacheKeysPolicy.invalidCacheKey;

import java.nio.charset.StandardCharsets;
import org.springframework.amqp.core.Message;

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
  public static final String TYPE_INVALIDATE = "INVALIDATE";
  public static final String TYPE_REFRESH = "REFRESH";

  /**
   * Deserialize a {@code SyncMessage} from an AMQP message body and headers.
   * Returns {@code null} when the body is empty or the cache key is invalid.
   *
   * @param msg the incoming AMQP message
   * @return a parsed {@link SyncMessage}, or {@code null}
   */
  public static SyncMessage from(Message msg) {
    byte[] body = msg.getBody();
    if (body == null || body.length == 0) {
      return null;
    }

    String cacheKey = new String(body, StandardCharsets.UTF_8);
    if (invalidCacheKey(cacheKey)) {
      return null;
    }

    String type = msg.getMessageProperties().getHeader(AMQP_HEADER_TYPE);
    long version =
      msg.getMessageProperties().getHeader(AMQP_HEADER_VERSION) instanceof Number n ? n.longValue() : VERSION_DEFAULT;
    boolean isVersionDegraded =
      msg.getMessageProperties().getHeader(AMQP_HEADER_IS_VERSION_DEGRADED) instanceof Boolean b ? b : true;

    return new SyncMessage(cacheKey, type, version, isVersionDegraded);
  }
}
