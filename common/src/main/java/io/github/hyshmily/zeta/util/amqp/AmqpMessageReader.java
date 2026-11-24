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
package io.github.hyshmily.zeta.util.amqp;

import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.HEADER_APP_NAME;

import io.github.hyshmily.zeta.util.Internal;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * Shared AMQP message-reading utilities for the sync and decision planes: typed header extraction,
 * bounded-allocation body abbreviation for diagnostics, and shared-broker tenant isolation via
 * {@link AppIsolationFilter}.
 */
@Slf4j
@Internal
public final class AmqpMessageReader {

  /** Maximum body characters decoded when logging a dropped unknown-type message. */
  private static final int UNKNOWN_TYPE_BODY_LIMIT = 128;

  /** Worst-case UTF-8 byte length of one Unicode scalar value. */
  private static final long MAX_UTF8_BYTES_PER_CHAR = 4L;

  /** Prevents instantiation of this utility class. */
  private AmqpMessageReader() {
    throw new AssertionError("No AmqpMessageReader instances");
  }

  /**
   * Reads a numeric AMQP header as a {@code long}, falling back to {@code defaultValue} when
   * missing or non-numeric (logging a DEBUG line on type mismatch).
   *
   * @param props        message properties carrying the header; {@code null} returns {@code defaultValue}
   * @param header       header name to inspect
   * @param defaultValue fallback value when absent or non-numeric
   * @return the header's {@code long} value, or {@code defaultValue}
   */
  public static long readLongHeader(@Nullable MessageProperties props, String header, long defaultValue) {
    if (props == null) {
      return defaultValue;
    }
    Object value = props.getHeader(header);
    if (value instanceof Number number) {
      return number.longValue();
    }
    if (value != null) {
      log.debug(
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
   * Reads a text AMQP header, converting non-{@link String} values (such as a {@link CharSequence}
   * from another sender) via {@link Object#toString()}.
   *
   * @param props  message properties carrying the header; {@code null} returns {@code null}
   * @param header header name to inspect
   * @return the header text, or {@code null} when absent
   */
  public static @Nullable String readStringHeader(@Nullable MessageProperties props, String header) {
    Object value = props == null ? null : props.getHeader(header);
    // String.toString() returns itself, covering both exact String and CharSequence headers.
    return value == null ? null : value.toString();
  }

  /**
   * Logs a dropped unknown- or missing-type message at DEBUG, decoding the body prefix lazily only
   * when DEBUG logging is enabled.
   *
   * @param logger     the listener's logger
   * @param planeLabel descriptive plane label (e.g., {@code "sync message"}, {@code "worker decision"})
   * @param typeHeader the unrecognized or {@code null} type header value
   * @param msg        the dropped AMQP message
   */
  public static void logUnknownType(Logger logger, String planeLabel, @Nullable Object typeHeader, Message msg) {
    if (!logger.isDebugEnabled()) {
      return;
    }
    logger.debug(
      "Dropping {} with unknown or missing type header: {}, body: {}",
      planeLabel,
      typeHeader,
      abbreviateBody(msg.getBody(), UNKNOWN_TYPE_BODY_LIMIT)
    );
  }

  /**
   * Decodes a bounded UTF-8 prefix of an AMQP body for diagnostic logging so large PUT or batch
   * payloads never allocate full strings under parse-failure bursts.
   *
   * @param body     raw message body bytes, or {@code null}
   * @param maxChars maximum decoded characters before appending a total-byte suffix
   * @return decoded text (truncated with a byte-size suffix when exceeding {@code maxChars}), or
   *         {@code "<null>"} when {@code body} is {@code null}
   */
  public static String abbreviateBody(byte@Nullable [] body, int maxChars) {
    if (body == null) {
      return "<null>";
    }
    if (maxChars <= 0) {
      return "...(total " + body.length + " bytes)";
    }
    // Decode at most maxChars * 4 bytes so tens-of-KB payloads never materialize in full.
    int decodeLen = (int) Math.min(body.length, (long) maxChars * MAX_UTF8_BYTES_PER_CHAR);
    String text = new String(body, 0, decodeLen, StandardCharsets.UTF_8);
    if (decodeLen == body.length && text.length() <= maxChars) {
      return text;
    }
    if (text.length() > maxChars) {
      // Never split a UTF-16 surrogate pair: an orphaned high surrogate breaks JSON log encoders.
      int cut = Character.isHighSurrogate(text.charAt(maxChars - 1)) ? maxChars - 1 : maxChars;
      text = text.substring(0, cut);
    }
    return text + "...(total " + body.length + " bytes)";
  }

  /**
   * Shared-broker {@code appName} isolation filter used by both the sync and decision listeners.
   *
   * <p>A message is treated as foreign if and only if this instance declares a non-blank
   * {@code appName} and the sender declared a different non-blank {@code HEADER_APP_NAME}. Messages
   * without an app header (rolling upgrades) or listeners with no configured {@code appName} pass
   * through unfiltered.
   *
   * <p>Thread-safe.
   */
  @Internal
  public static final class AppIsolationFilter {

    /** Cumulative foreign-application messages dropped since startup. */
    private final AtomicLong foreignDropCounter = new AtomicLong();

    /** Normalized local {@code appName}, or {@code null} when isolation is disabled. */
    private final @Nullable String appName;

    /** Most recently observed foreign sender {@code appName}, paired with {@link #foreignDropCounter}. */
    private volatile @Nullable String lastForeignApp;

    /**
     * Creates an isolation filter, normalizing a blank {@code appName} to {@code null} so disabled
     * instances short-circuit on a single reference check.
     *
     * @param appName this application's name, or {@code null}/blank to disable filtering
     */
    public AppIsolationFilter(@Nullable String appName) {
      this.appName = (appName != null && !appName.isBlank()) ? appName : null;
    }

    /**
     * Checks whether {@code msg} belongs to another application on a shared broker; if foreign,
     * records the drop, updates {@link #lastForeignApp()}, and logs at DEBUG.
     *
     * @param msg        the inbound AMQP message
     * @param logger     the calling listener's logger
     * @param planeLabel descriptive plane label (e.g., {@code "sync message"}, {@code "worker decision"})
     * @return {@code true} if the message is foreign and should be dropped
     */
    public boolean dropIfForeign(Message msg, Logger logger, String planeLabel) {
      if (appName == null) {
        return false;
      }
      MessageProperties props = msg.getMessageProperties();
      if (props == null) {
        return false;
      }
      String senderApp = readStringHeader(props, HEADER_APP_NAME);
      if (!isForeign(senderApp)) {
        return false;
      }

      lastForeignApp = senderApp;
      foreignDropCounter.incrementAndGet();
      logger.debug(
        "Dropped {} from another application: senderApp={}, localApp={} (check zeta.app-name)",
        planeLabel,
        senderApp,
        appName
      );
      return true;
    }

    /** Returns the cumulative count of foreign-application messages dropped since startup. */
    public long drops() {
      return foreignDropCounter.get();
    }

    /** Returns the most recently observed foreign sender {@code appName}, or {@code null}. */
    public @Nullable String lastForeignApp() {
      return lastForeignApp;
    }

    /** Returns {@code true} if {@code senderApp} names a different application than {@link #appName}. */
    private boolean isForeign(@Nullable String senderApp) {
      // Blank headers declare no tenant and must not be rejected as foreign during rolling upgrades.
      return senderApp != null && !senderApp.isBlank() && !senderApp.equals(appName);
    }
  }
}
