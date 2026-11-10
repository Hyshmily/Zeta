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
 * Shared helpers for reading AMQP messages across the sync and decision
 * planes — numeric header extraction with a DEBUG type-mismatch line,
 * bounded-size body summaries for error logs, and the per-listener
 * shared-broker appName isolation filter.
 *
 * <p>{@link AppIsolationFilter} is the single foreign-app predicate shared by
 * the sync and decision planes (design decisions: class layout).
 */
@Slf4j
@Internal
public final class AmqpMessageReader {

  private AmqpMessageReader() {}

  /**
   * Reads a numeric AMQP message header, returning its {@code long} value.
   *
   * <p>If the header value is not a {@link Number} (e.g. a String was sent by
   * a different version of the sender), the method logs at DEBUG and returns
   * the {@code defaultValue}. Missing headers are silent.
   *
   * @param props        the message properties containing the header
   * @param header       the header name to read
   * @param defaultValue the fallback value when the header is missing or non-numeric
   * @return the header's numeric value, or {@code defaultValue}
   */
  public static long readLongHeader(MessageProperties props, String header, long defaultValue) {
    Object value = props.getHeader(header);
    if (value instanceof Number n) {
      return n.longValue();
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
   * Reads a string AMQP message header. A non-String header is converted with
   * {@code toString()} because the sync wire treats the header as text (an older
   * sender may write a {@link CharSequence}); a missing header is {@code null}.
   *
   * @param props  the message properties containing the header
   * @param header the header name to read
   * @return the header text, or {@code null} when absent
   */
  public static @Nullable String readStringHeader(MessageProperties props, String header) {
    Object value = props.getHeader(header);
    if (value == null) {
      return null;
    }
    return value instanceof String s ? s : value.toString();
  }

  /**
   * Reports a message whose type header is unknown or missing, at DEBUG: the
   * message is dropped, and its body is decoded only when DEBUG is on.
   *
   * @param log        the listener's logger
   * @param planeLabel what is being dropped ("sync message", "worker decision")
   * @param typeHeader the raw header value
   * @param msg        the message carrying it
   */
  public static void logUnknownType(Logger log, String planeLabel, @Nullable Object typeHeader, Message msg) {
    if (!log.isDebugEnabled()) {
      return;
    }
    log.debug(
      "Dropping {} with unknown or missing type header: {}, body: {}",
      planeLabel,
      typeHeader,
      abbreviateBody(msg.getBody(), 128)
    );
  }

  /**
   * Decodes an AMQP body as UTF-8 and bounds it for error-log output. Batch and
   * PUT value payloads can be tens of KB; a parse-failure storm must not echo
   * them into the log at message rate.
   *
   * <p>Only the prefix needed for {@code maxChars} characters is decoded (a
   * UTF-8 code point is at most 4 bytes), so a tens-of-KB body never
   * materializes as a full String just to be truncated — the allocation is
   * bounded by the log line even under a malformed-message flood.
   *
   * @param body     the raw message body; may be null
   * @param maxChars hard cap on the returned string length (excluding the suffix)
   * @return the decoded body, truncated with a total-size suffix when longer than
   *         {@code maxChars}; {@code "<null>"} for a null body
   */
  public static String abbreviateBody(byte[] body, int maxChars) {
    if (body == null) {
      return "<null>";
    }
    if (maxChars <= 0) {
      return "...(total " + body.length + " bytes)";
    }
    int decodeLen = (int) Math.min(body.length, (long) maxChars * 4L);
    String text = new String(body, 0, decodeLen, StandardCharsets.UTF_8);
    if (decodeLen == body.length && text.length() <= maxChars) {
      return text;
    }
    if (text.length() > maxChars) {
      // Never cut between the two chars of a surrogate pair: the orphaned high surrogate is not
      // valid UTF-16 and breaks downstream JSON log encoders.
      int cut = Character.isHighSurrogate(text.charAt(maxChars - 1)) ? maxChars - 1 : maxChars;
      text = text.substring(0, cut);
    }
    return text + "...(total " + body.length + " bytes)";
  }

  /**
   * Shared-broker appName isolation (design decisions: sync messages): the sync plane is a
   * fanout whose exchange name is a global constant, so on a shared broker every bound queue
   * receives every application's sync messages. The decision plane routes by
   * {@code send.<appName>} and does not need this filter for delivery — it applies the same
   * check as its own boundary. The predicate itself lives in {@link #isForeign}.
   *
   * <p>The filter owns the drop counter, the last-observed foreign sender, the
   * DEBUG log line, and the drop-and-log tail
   * ({@link #dropIfForeign}) — the two planes pass their own label, so a dropped
   * decision and a dropped sync message stay distinguishable without forking the
   * wording. Each listener keeps its own delegating
   * {@code foreignAppDrops()} / {@code lastForeignApp()} accessors so the
   * Actuator surface is unchanged.
   *
   * <p>Thread safety: all state is an immutable field, an {@link AtomicLong} or
   * a volatile reference — safe for concurrent AMQP consumer threads with no
   * locking.
   */
  @Internal
  public static final class AppIsolationFilter {

    /** Cumulative foreign-app drops (a non-zero value with a stable sender almost always means a misconfigured app-name). */
    private final AtomicLong foreignDropCounter = new AtomicLong();

    /** This instance's appName; {@code null} disables the filter (blank is normalized to null). */
    private final String appName;

    /** The most recent foreign sender appName observed (for pairing with the counter). */
    private volatile String lastForeignApp;

    /**
     * Creates the filter. {@code appName} may be {@code null}/blank to disable
     * foreign-app filtering (the unconfigured-filter behavior, which rolling
     * upgrades still need).
     *
     * <p>Blank is normalized to {@code null} once, here, so the per-message check in
     * {@link #dropIfForeign} is one reference comparison.
     *
     * @param appName this application's appName
     */
    public AppIsolationFilter(String appName) {
      this.appName = (appName != null && !appName.isBlank()) ? appName : null;
    }

    /**
     * Drops an inbound message that belongs to a different application on the
     * same broker.
     *
     * <p>A message is foreign when — and only when — the sender declared an
     * {@code appName} header that differs from this instance's appName:
     * <ul>
     *   <li>sender declared an appName that differs from ours → foreign (drop)</li>
     *   <li>sender declared no appName (older sender) → not foreign (process)</li>
     *   <li>this instance declares no appName → not foreign (process everything)</li>
     * </ul>
     *
     * <p>The header is read and converted once; the drop counter, the
     * last-observed sender and the DEBUG line are settled in one call.
     *
     * @param msg        the inbound AMQP message
     * @param log        the plane's logger
     * @param planeLabel what is being dropped ("sync message", "worker decision")
     * @return {@code true} when the message was foreign and has been counted and logged
     */
    public boolean dropIfForeign(Message msg, Logger log, String planeLabel) {
      String senderApp = readStringHeader(msg.getMessageProperties(), HEADER_APP_NAME);
      if (!isForeign(senderApp)) {
        return false;
      }

      lastForeignApp = senderApp;
      foreignDropCounter.incrementAndGet();
      log.debug(
        "Dropped {} from another application: senderApp={}, localApp={} (check zeta.app-name)",
        planeLabel,
        senderApp,
        appName
      );
      return true;
    }

    /** Whether the sender declared an appName that differs from this instance's. */
    private boolean isForeign(@Nullable String senderApp) {
      return appName != null && senderApp != null && !appName.equals(senderApp);
    }

    /** Total foreign-app drops since startup. */
    public long drops() {
      return foreignDropCounter.get();
    }

    /** The most recently observed foreign sender appName, or {@code null}. */
    public String lastForeignApp() {
      return lastForeignApp;
    }
  }
}
