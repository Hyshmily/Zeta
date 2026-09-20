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
package io.github.hyshmily.zeta.util;

import io.github.hyshmily.zeta.Internal;
import jakarta.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;

/**
 * Shared helpers for reading AMQP messages in the sync layer — numeric header
 * extraction with a one-shot type-mismatch warning, and bounded-size body
 * summaries for error logs.
 *
 * <p>Extracted from the byte-identical copies that used to live in
 * {@code SyncMessage} and {@code WorkerMessage}; the contract is unchanged.
 */
@Slf4j
@Internal
public final class AmqpMessageReader {

  private AmqpMessageReader() {}

  /**
   * Reads a numeric AMQP message header, returning its {@code long} value.
   *
   * <p>If the header value is not a {@link Number} (e.g. a String was sent by
   * a different version of the sender), the method logs a one-time warning
   * via the {@code warnFlag} and returns the {@code defaultValue}. The
   * one-time flag prevents log flooding when every message has the wrong type.
   *
   * <p>When {@code warnFlag} is {@code null}, the warning is silently skipped
   * (used for best-effort headers like {@code timestamp}).
   *
   * @param msg          the AMQP message containing the header
   * @param header       the header name to read
   * @param defaultValue the fallback value when the header is missing or non-numeric
   * @param warnFlag     one-shot warning gate; null to suppress warnings
   * @return the header's numeric value, or {@code defaultValue}
   */
  public static long readLongHeader(Message msg, String header, long defaultValue, @Nullable AtomicBoolean warnFlag) {
    Object value = msg.getMessageProperties().getHeader(header);
    if (value instanceof Number n) {
      return n.longValue();
    }
    if (value != null && warnFlag != null && warnFlag.compareAndSet(false, true)) {
      log.warn(
        "Non-numeric header '{}' (type {}), defaulting to {}. Value: {}",
        header,
        value.getClass().getName(),
        defaultValue,
        value
      );
    }
    return defaultValue;
  }

  /**
   * Decodes an AMQP body as UTF-8 and bounds it for error-log output. Batch
   * payloads (INVALIDATE_ALL key lists, RULES_SYNC rule sets) can be tens of
   * KB; a parse-failure storm must not echo them into the log at message rate.
   *
   * @param body     the raw message body; may be null
   * @param maxChars hard cap on the returned string length (excluding the suffix)
   * @return the decoded body, truncated with a total-length suffix when longer than
   *         {@code maxChars}; {@code "<null>"} for a null body
   */
  public static String abbreviateBody(byte[] body, int maxChars) {
    if (body == null) {
      return "<null>";
    }
    String text = new String(body, StandardCharsets.UTF_8);
    if (text.length() <= maxChars) {
      return text;
    }
    return text.substring(0, maxChars) + "...(total " + text.length() + " chars)";
  }
}
