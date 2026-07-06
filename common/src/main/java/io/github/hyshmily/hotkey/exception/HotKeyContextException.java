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
package io.github.hyshmily.hotkey.exception;

import lombok.Getter;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Base exception carrying the source class name and the instant at which this
 * exception was created. All HotKey-specific exceptions should extend this
 * class to provide consistent context for diagnostics and monitoring.
 */
@Getter
public class HotKeyContextException extends RuntimeException {

  private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(
    ZoneOffset.UTC
  );

  /** Simple name of the source class that threw this exception. */
  private final String sourceClass;

  /** Timestamp (UTC) when this exception was created. */
  private final Instant timestamp;

  private final String logMessage;

  /**
   * Creates a new context exception.
   *
   * @param sourceClass the simple name of the throwing class
   * @param message     the detail message
   */
  public HotKeyContextException(String sourceClass, String message) {
    this(sourceClass, message, null);
  }

  /**
   * Creates a new context exception with a cause.
   *
   * @param sourceClass the simple name of the throwing class
   * @param message     the detail message
   * @param cause       the cause (maybe {@code null})
   */
  public HotKeyContextException(String sourceClass, String message, Throwable cause) {
    super(message, cause);
    this.sourceClass = sourceClass;
    this.timestamp = Instant.now();
    this.logMessage = FMT.format(timestamp) + " [" + sourceClass + "] " + message;
  }

  @Override
  public String getMessage() {
    return logMessage;
  }
}
