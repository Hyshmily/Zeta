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

import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;

/**
 * The one merge rule for declarations that arrive from several configuration sources: an absent
 * value is inherited, an equal value is idempotent, and two different explicit values are an
 * ambiguity no runtime choice can resolve.
 */
@Internal
public final class Merge {

  /**
   * Merge one attribute declared twice. {@code null} means "not declared here".
   *
   * @param current         the value already accepted, or {@code null}
   * @param candidate       the next declaration, or {@code null}
   * @param conflictMessage builds the failure message when both are non-null and differ
   * @param <T>             the value type
   * @return the accepted value, or {@code null} when neither side declared one
   * @throws IllegalStateException when both sides declare different values
   */
  public static <T> @Nullable T mergeOrThrow(
    @Nullable T current,
    @Nullable T candidate,
    Supplier<String> conflictMessage
  ) {
    if (current == null || candidate == null || current.equals(candidate)) {
      return current != null ? current : candidate;
    }
    throw new IllegalStateException(conflictMessage.get());
  }

  private Merge() {}
}
