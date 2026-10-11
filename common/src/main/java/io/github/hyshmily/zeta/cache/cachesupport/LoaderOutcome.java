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
package io.github.hyshmily.zeta.cache.cachesupport;

import java.util.Optional;
import org.jspecify.annotations.Nullable;

/**
 * Explicit result of a {@link SingleFlight} load.
 *
 * <p>One allocation per load — the same cost shape as the {@code Optional} it
 * replaces. Suppressed outcomes carry no stack trace (they are expected
 * degradation, never faults); only {@link LoadStatus#FAILED} carries a cause.
 *
 * @param value  the loaded value; non-null iff {@code status == LOADED}
 * @param status the outcome classification (never {@code null})
 * @param cause  the reader failure; non-null iff {@code status == FAILED}
 */
public record LoaderOutcome<T>(@Nullable T value, LoadStatus status, @Nullable Throwable cause) {
  /**
   * Compact constructor: enforces the value/cause invariants per status.
   */
  public LoaderOutcome {
    if (status == null) {
      throw new NullPointerException("status must not be null");
    }
    if (status == LoadStatus.LOADED && value == null) {
      throw new IllegalArgumentException("LOADED must carry a non-null value");
    }
    if ((status == LoadStatus.SOURCE_EMPTY
        || status == LoadStatus.SUPPRESSED_BREAKER
        || status == LoadStatus.SUPPRESSED_TIMEOUT
        || status == LoadStatus.SUPPRESSED_EXECUTOR)
      && value != null) {
      throw new IllegalArgumentException(status + " must carry a null value");
    }
    if (status == LoadStatus.FAILED && cause == null) {
      throw new IllegalArgumentException("FAILED must carry a cause");
    }
    if (status != LoadStatus.FAILED && cause != null) {
      throw new IllegalArgumentException("only FAILED may carry a cause");
    }
  }

  /**
   * A successful load of a non-null value.
   *
   * @param value the loaded value (never {@code null})
   * @return the outcome
   */
  public static <T> LoaderOutcome<T> loaded(T value) {
    return new LoaderOutcome<>(value, LoadStatus.LOADED, null);
  }

  /**
   * The reader completed with {@code null} — the source has no data.
   *
   * @return the outcome
   */
  public static <T> LoaderOutcome<T> sourceEmpty() {
    return new LoaderOutcome<>(null, LoadStatus.SOURCE_EMPTY, null);
  }

  /**
   * A suppressed load (breaker / timeout / executor saturation).
   *
   * @param status the suppression reason (a SUPPRESSED_* status)
   * @return the outcome
   * @throws IllegalArgumentException for any non-suppression status
   */
  public static <T> LoaderOutcome<T> suppressed(LoadStatus status) {
    if (status != LoadStatus.SUPPRESSED_BREAKER
      && status != LoadStatus.SUPPRESSED_TIMEOUT
      && status != LoadStatus.SUPPRESSED_EXECUTOR) {
      throw new IllegalArgumentException("suppressed() requires a SUPPRESSED_* status, got " + status);
    }
    return new LoaderOutcome<>(null, status, null);
  }

  /**
   * The reader threw.
   *
   * @param cause the reader failure (never {@code null})
   * @return the outcome
   */
  public static <T> LoaderOutcome<T> failed(Throwable cause) {
    return new LoaderOutcome<>(null, LoadStatus.FAILED, cause);
  }

  /**
   * Whether this outcome was suppressed by protection machinery (breaker,
   * timeout, or executor saturation) — transient degradation, not a failure.
   *
   * @return {@code true} for the three SUPPRESSED_* statuses
   */
  public boolean isSuppressed() {
    return status == LoadStatus.SUPPRESSED_BREAKER
      || status == LoadStatus.SUPPRESSED_TIMEOUT
      || status == LoadStatus.SUPPRESSED_EXECUTOR;
  }

  /**
   * Legacy view: the loaded value, or empty for every other status.
   *
   * @return the value iff {@code status == LOADED}
   */
  public Optional<T> toOptional() {
    return status == LoadStatus.LOADED ? Optional.of(value) : Optional.empty();
  }
}
