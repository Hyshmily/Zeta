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
package io.github.hyshmily.zeta.exception;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import lombok.Getter;

/**
 * Base exception carrying the source class name and the instant at which this
 * exception was created. All HotKey-specific exceptions should extend this
 * class to provide consistent context for diagnostics and monitoring.
 *
 * <p>The log-formatted message ({@link #getLogMessage()}) is built lazily on
 * first access, so exceptions that are constructed per operation and caught
 * without being stringified (control-flow exceptions such as
 * {@link ZetaBlockedException}) pay no timestamp-formatting cost.
 */
public class ZetaContextException extends RuntimeException {

  private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(
    ZoneOffset.UTC
  );

  /** Simple name of the source class that threw this exception. */
  @Getter
  private final String sourceClass;

  /** Timestamp (UTC) when this exception was created. */
  @Getter
  private final Instant timestamp;

  /**
   * Lazily formatted log message. Volatile with a benign race: every input is
   * final, so a concurrent first access may format twice and one identical
   * write wins.
   */
  private volatile String logMessage;

  /**
   * Creates a new context exception.
   *
   * @param sourceClass the simple name of the throwing class
   * @param message     the detail message
   */
  public ZetaContextException(String sourceClass, String message) {
    this(sourceClass, message, null);
  }

  /**
   * Creates a new context exception with a cause.
   *
   * @param sourceClass the simple name of the throwing class
   * @param message     the detail message
   * @param cause       the cause (maybe {@code null})
   */
  public ZetaContextException(String sourceClass, String message, Throwable cause) {
    this(sourceClass, message, cause, true, true);
  }

  /**
   * Creates a new context exception with full {@link Throwable} control. Subclasses
   * thrown per operation as control flow (e.g. {@link ZetaBlockedException}) pass
   * {@code writableStackTrace=false} to skip stack-trace filling — their throw
   * sites are fixed and their fields already carry the diagnostics (ADR-0063).
   *
   * @param sourceClass        the simple name of the throwing class
   * @param message            the detail message
   * @param cause              the cause (maybe {@code null})
   * @param enableSuppression  whether suppressed exceptions are enabled
   * @param writableStackTrace whether the stack trace is writable (and filled)
   */
  protected ZetaContextException(
    String sourceClass,
    String message,
    Throwable cause,
    boolean enableSuppression,
    boolean writableStackTrace
  ) {
    super(message, cause, enableSuppression, writableStackTrace);
    this.sourceClass = sourceClass;
    this.timestamp = Instant.now();
  }

  /**
   * Returns the log-formatted message
   * ({@code "yyyy-MM-dd HH:mm:ss.SSS [sourceClass] message"}), built lazily on
   * first access and memoized.
   *
   * @return the formatted log message
   */
  public String getLogMessage() {
    String lm = logMessage;
    if (lm == null) {
      lm = FMT.format(timestamp) + " [" + sourceClass + "] " + super.getMessage();
      logMessage = lm;
    }
    return lm;
  }

  @Override
  public String getMessage() {
    return getLogMessage();
  }
}
