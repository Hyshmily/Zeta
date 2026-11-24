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
package io.github.hyshmily.zeta.model;

import org.jspecify.annotations.Nullable;

/**
 * The operation carried by a Worker decision and its lease stamp.
 *
 * <p>The kind is part of the lease, not of the cached data: a {@code COOL}
 * stamp can remain on an entry whose effective state is {@code NORMAL} (the
 * cool-down completed) or {@code HOT} (a local promotion overrode it while
 * the cluster was degraded). Keeping the kind with the stamp lets the read
 * path tell those cases apart without a second state field.
 *
 * <p>Every downstream decision asks the constant itself instead of re-comparing the header
 * string: {@link #isControl()} marks the O(1) COOL control signal that bypasses the global
 * payload budget, and {@link #needsJitter()} marks the HOT path that may run a cluster-loader
 * read and must be spread across instances. The enum name is the wire value;
 * {@link #parse(Object)} is the one place the header string becomes an operation.
 */
public enum DecisionKind {
  HOT,
  COOL;

  /** Whether this decision is a control signal exempt from the global payload budget. */
  public boolean isControl() {
    return this == COOL;
  }

  /** Whether applying this decision may run the cluster loader and needs a start-jitter park. */
  public boolean needsJitter() {
    return this == HOT;
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
  public static @Nullable DecisionKind parse(@Nullable Object header) {
    return header instanceof CharSequence value
      ? switch (value.toString()) {
          case "HOT" -> HOT;
          case "COOL" -> COOL;
          default -> null;
        }
      : null;
  }
}
