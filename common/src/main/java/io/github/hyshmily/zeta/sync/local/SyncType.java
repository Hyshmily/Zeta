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
 * <p>Every downstream decision reads a flag instead of re-comparing the header string:
 * <ul>
 *   <li>{@link #ignoresSelf()} — the fanout copy of this instance's own broadcast carries
 *       no information (the sender already applied it); only INVALIDATE is re-applied.</li>
 *   <li>{@link #isClear()} — an O(n) table/namespace sweep, the control-plane family that
 *       shares one dispatcher channel and bypasses the global payload budget.</li>
 *   <li>{@link #needsJitter()} — the operation runs a cluster-loader read and must be
 *       spread across peers.</li>
 * </ul>
 *
 * <p>The enum name is the wire value; {@link #parse(String)} is the one place the header
 * string becomes an operation.
 */
public enum SyncType {
  INVALIDATE(0),
  REFRESH(SyncType.IGNORES_SELF | SyncType.NEEDS_JITTER),
  PUT(SyncType.IGNORES_SELF),
  INVALIDATE_ALL(SyncType.IGNORES_SELF | SyncType.CLEAR),
  INVALIDATE_PREFIX(SyncType.IGNORES_SELF | SyncType.CLEAR),
  INVALIDATE_BATCH(SyncType.IGNORES_SELF);

  // Compile-time constants: the enum-constant arguments above are evaluated before these
  // fields are initialized, so they must stay constant expressions.
  private static final byte IGNORES_SELF = 1;
  private static final byte CLEAR = 2;
  private static final byte NEEDS_JITTER = 4;

  private final byte flags;

  SyncType(int flags) {
    this.flags = (byte) flags;
  }

  /** Whether the fanout copy of this instance's own broadcast is discarded unread. */
  public boolean ignoresSelf() {
    return (flags & IGNORES_SELF) != 0;
  }

  /** Whether applying the operation sweeps the cache table or a key namespace. */
  public boolean isClear() {
    return (flags & CLEAR) != 0;
  }

  /** Whether applying the operation may run a cluster-loader read. */
  public boolean needsJitter() {
    return (flags & NEEDS_JITTER) != 0;
  }

  /** The wire value written to the AMQP type header. */
  public String code() {
    return name();
  }

  /**
   * Parses the AMQP type header.
   *
   * @param header the header value; may be {@code null} or a non-String type
   * @return the matching operation, or {@code null} for an unknown value
   */
  public static @Nullable SyncType parse(@Nullable Object header) {
    return header instanceof String value
      ? switch (value) {
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
