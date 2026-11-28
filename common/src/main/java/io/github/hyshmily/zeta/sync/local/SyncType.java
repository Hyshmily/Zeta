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

import org.jspecify.annotations.Nullable;

/**
 * The wire operation of a {@link SyncMessage}, parsed once at the AMQP boundary.
 *
 * <p>Every downstream decision reads a predicate instead of re-comparing the header string:
 * <ul>
 *   <li>{@link #isClear()} — an O(n) table/namespace sweep, the control-plane family that shares
 *       one dispatcher channel and bypasses the global payload budget.</li>
 *   <li>{@link #isControl()} — an O(1) removal or clear that must survive a value-payload storm;
 *       it bypasses the global pending budget without leaving the per-key ordered channel.</li>
 *   <li>{@link #isValue()} — the operation carries a value path (PUT or REFRESH), the only two
 *       operations that decode a payload or run a loader read.</li>
 *   <li>{@link #needsJitter()} — the operation runs a cluster-loader read and must be spread
 *       across peers.</li>
 * </ul>
 *
 * <p>The enum name is the wire value; {@link #parse(Object)} is the one place the header value
 * becomes an operation.
 */
public enum SyncType {
  INVALIDATE,
  REFRESH,
  PUT,
  INVALIDATE_ALL,
  INVALIDATE_PREFIX,
  INVALIDATE_BATCH;

  /** Whether applying the operation sweeps the cache table or a key namespace. */
  public boolean isClear() {
    return this == INVALIDATE_ALL || this == INVALIDATE_PREFIX;
  }

  /** Whether the operation is an O(1) control message exempt from the global pending budget. */
  public boolean isControl() {
    return this == INVALIDATE || isClear();
  }

  /** Whether the operation carries a value path: a PUT payload or a peer REFRESH reload. */
  public boolean isValue() {
    return this == PUT || this == REFRESH;
  }

  /** Whether applying the operation may run a cluster-loader read. */
  public boolean needsJitter() {
    return this == REFRESH;
  }

  /**
   * The receive-side tie-break rank at one data version: INVALIDATE outranks PUT, and PUT outranks
   * REFRESH. The publisher claim and the batch reducer share this order.
   *
   * @return the rank; a non-key type ranks below every key operation
   */
  public int priority() {
    return switch (this) {
      case INVALIDATE -> 2;
      case PUT -> 1;
      default -> 0;
    };
  }

  /** The wire value written to the AMQP type header. */
  public String code() {
    return name();
  }

  /**
   * Parses the AMQP type header.
   *
   * @param header the header value; may be {@code null} or any non-text type
   * @return the matching operation, or {@code null} for an unknown value
   */
  public static @Nullable SyncType parse(@Nullable Object header) {
    return header instanceof CharSequence value
      ? switch (value.toString()) {
          case "INVALIDATE" -> INVALIDATE;
          case "REFRESH" -> REFRESH;
          case "PUT" -> PUT;
          case "INVALIDATE_ALL" -> INVALIDATE_ALL;
          case "INVALIDATE_PREFIX" -> INVALIDATE_PREFIX;
          case "INVALIDATE_BATCH" -> INVALIDATE_BATCH;
          default -> null;
        }
      : null;
  }
}
