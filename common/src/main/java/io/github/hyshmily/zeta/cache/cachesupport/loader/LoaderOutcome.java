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
package io.github.hyshmily.zeta.cache.cachesupport.loader;

import io.github.hyshmily.zeta.cache.cachesupport.loader.singleflight.SingleFlight;
import org.jspecify.annotations.Nullable;

/**
 * Explicit result of a {@link SingleFlight} load.
 *
 * <p>One allocation per load. Suppressed outcomes carry no stack trace (they
 * are expected degradation, never faults); only {@link LoadStatus#FAILED}
 * carries a cause.
 *
 * @param value  the loaded value; non-null iff {@code status == LOADED}
 * @param status the outcome classification (never {@code null})
 * @param cause  the reader failure; non-null iff {@code status == FAILED}
 */
public record LoaderOutcome<T>(@Nullable T value, LoadStatus status, @Nullable Throwable cause) {
  /** The shared no-data outcome: no value, no cause, one instance per JVM. */
  private static final LoaderOutcome<?> SOURCE_EMPTY = new LoaderOutcome<>(null, LoadStatus.SOURCE_EMPTY, null);

  /** The shared breaker-suppressed outcome. */
  private static final LoaderOutcome<?> SUPPRESSED_BREAKER = new LoaderOutcome<>(
    null,
    LoadStatus.SUPPRESSED_BREAKER,
    null
  );

  /** The shared timeout-suppressed outcome. */
  private static final LoaderOutcome<?> SUPPRESSED_TIMEOUT = new LoaderOutcome<>(
    null,
    LoadStatus.SUPPRESSED_TIMEOUT,
    null
  );

  /** The shared executor-suppressed outcome. */
  private static final LoaderOutcome<?> SUPPRESSED_EXECUTOR = new LoaderOutcome<>(
    null,
    LoadStatus.SUPPRESSED_EXECUTOR,
    null
  );

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
    if (status != LoadStatus.LOADED && value != null) {
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
  @SuppressWarnings("unchecked")
  public static <T> LoaderOutcome<T> sourceEmpty() {
    return (LoaderOutcome<T>) SOURCE_EMPTY;
  }

  /**
   * A successful load of a nullable value: the value when present, the empty outcome otherwise.
   * The one place the reader's null is classified, so no call site repeats the ternary.
   *
   * @param value the loaded value, or {@code null}
   * @param <T>   the value type
   * @return the loaded outcome, or the empty outcome for a null value
   */
  public static <T> LoaderOutcome<T> ofNullable(@Nullable T value) {
    return value == null ? sourceEmpty() : loaded(value);
  }

  /**
   * A suppressed load (breaker / timeout / executor saturation).
   *
   * <p>The three suppressions carry no per-call state, so a suppression storm allocates nothing.
   *
   * @param status the suppression reason (a SUPPRESSED_* status)
   * @return the outcome
   * @throws IllegalArgumentException for any non-suppression status
   */
  @SuppressWarnings("unchecked")
  public static <T> LoaderOutcome<T> suppressed(LoadStatus status) {
    if (status == null) {
      throw new IllegalArgumentException("suppressed() requires a SUPPRESSED_* status, got null");
    }
    return (LoaderOutcome<T>) switch (status) {
      case SUPPRESSED_BREAKER -> SUPPRESSED_BREAKER;
      case SUPPRESSED_TIMEOUT -> SUPPRESSED_TIMEOUT;
      case SUPPRESSED_EXECUTOR -> SUPPRESSED_EXECUTOR;
      default -> throw new IllegalArgumentException("suppressed() requires a SUPPRESSED_* status, got " + status);
    };
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
}
