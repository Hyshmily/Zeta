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
package io.github.hyshmily.zeta.sync.worker;

import java.util.Objects;

/**
 * A value resolved for a HOT promotion plus the data version it carries.
 *
 * <p>{@code loadedByThisCall} distinguishes a value loaded during this promotion from a value
 * reused out of L1 as the source-outage fallback. A loaded value may create a new entry; a
 * fallback may only promote the resident it came from, otherwise it could resurrect a value
 * that a concurrent invalidation removed while the load was in flight.
 *
 * @param value             the value to promote; never {@code null}
 * @param dataVersion       the data stamp the value carries ({@code 0} = unknown)
 * @param loadedByThisCall  whether this call loaded the value (as opposed to reusing an L1
 *                          fallback)
 */
public record HotValue(Object value, long dataVersion, boolean loadedByThisCall) {
  public HotValue {
    Objects.requireNonNull(value, "value must not be null");
  }

  /**
   * A value loaded during this promotion. Its data version is the pre-read stamp, so a write
   * that lands during the load carries a strictly greater stamp.
   *
   * @param value       the loaded value
   * @param dataVersion the pre-read data stamp
   * @return the loaded value
   */
  public static HotValue loaded(Object value, long dataVersion) {
    return new HotValue(value, dataVersion, true);
  }

  /**
   * A resident L1 value reused as the source-outage fallback. It keeps its own data version:
   * it is an existing value and must not be re-signed with a fresher stamp.
   *
   * @param value       the resident value
   * @param dataVersion the resident's data stamp
   * @return the resident value
   */
  public static HotValue resident(Object value, long dataVersion) {
    return new HotValue(value, dataVersion, false);
  }
}
