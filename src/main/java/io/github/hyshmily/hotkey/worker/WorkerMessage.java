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
package io.github.hyshmily.hotkey.worker;

import org.springframework.amqp.core.Message;

import java.nio.charset.StandardCharsets;

import static io.github.hyshmily.hotkey.constant.HotKeyConstants.*;

/**
 * Message from Worker to app instances carrying hot/cool decisions.
 * Travels via {@code hotkey.worker.exchange} (FanoutExchange).
 *
 * <p>No {@code isVersionDegraded} — Worker skips broadcast if Redis GET fails.
 *
 * @param cacheKey the affected cache key
 * @param type     the decision ({@link #TYPE_HOT} or {@link #TYPE_COOL})
 * @param version  the version number at which the decision was made
 */
public record WorkerMessage(String cacheKey, String type, long version) {

  public static final String TYPE_HOT = "HOT";
  public static final String TYPE_COOL = "COOL";

  /**
   * Deserialize a {@code WorkerMessage} from an AMQP message body and headers.
   * Returns {@code null} when the body is empty.
   *
   * @param msg the incoming AMQP message
   * @return a parsed {@link WorkerMessage}, or {@code null}
   */
  public static WorkerMessage from(Message msg) {
    byte[] body = msg.getBody();
    if (body == null || body.length == 0) {
      return null;
    }

    String cacheKey = new String(body, StandardCharsets.UTF_8);
    String type = msg.getMessageProperties().getHeader(AMQP_HEADER_TYPE);

    long version =
      msg.getMessageProperties().getHeader(AMQP_HEADER_VERSION) instanceof Number n ? n.longValue() : VERSION_DEFAULT;
    return new WorkerMessage(cacheKey, type, version);
  }
}
