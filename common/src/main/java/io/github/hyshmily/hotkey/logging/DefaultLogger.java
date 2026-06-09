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
package io.github.hyshmily.hotkey.logging;

import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default {@link HotKeyLogger} implementation that delegates to SLF4J.
 *
 * <p>The SLF4J {@link Logger} is resolved once at construction time and
 * cached for the lifetime of this instance. Each source class creates its
 * own {@code static final} instance bound to its own class, so per-call
 * overhead is identical to Lombok's {@code @Slf4j}.
 */
public class DefaultLogger implements HotKeyLogger {

  private final Logger logger;

  /**
   * Create a {@code DefaultLogger} that delegates to an SLF4J {@link Logger}
   * resolved for the given class.
   *
   * @param clazz the class to bind the logger to
   */
  public DefaultLogger(Class<?> clazz) {
    this.logger = LoggerFactory.getLogger(clazz);
  }

  /** Delegates to SLF4J {@code debug}. */
  @Override
  public void debug(String msg) {
    logger.debug(msg);
  }

  /** Delegates to SLF4J {@code debug} with format arguments. */
  @Override
  public void debug(String format, Object... args) {
    logger.debug(format, args);
  }

 /** Delegates to SLF4J {@code debug} with lazy supplier evaluation. */
  @Override
  public void debug(Supplier<String> supplier) {
    if (logger.isDebugEnabled()) {
      logger.debug(supplier.get());
    }
  }

  /** Delegates to SLF4J {@code info}. */
  @Override
  public void info(String msg) {
    logger.info(msg);
  }

  /** Delegates to SLF4J {@code info} with format arguments. */
  @Override
  public void info(String format, Object... args) {
    logger.info(format, args);
  }

  /** Delegates to SLF4J {@code info} with lazy supplier evaluation. */
  @Override
  public void info(Supplier<String> supplier) {
    if (logger.isInfoEnabled()) {
      logger.info(supplier.get());
    }
  }

  /** Delegates to SLF4J {@code warn}. */
  @Override
  public void warn(String msg) {
    logger.warn(msg);
  }

  /** Delegates to SLF4J {@code warn} with format arguments. */
  @Override
  public void warn(String format, Object... args) {
    logger.warn(format, args);
  }

  /** Delegates to SLF4J {@code warn} with lazy supplier evaluation. */
  @Override
  public void warn(Supplier<String> supplier) {
    if (logger.isWarnEnabled()) {
      logger.warn(supplier.get());
    }
  }

  /** Delegates to SLF4J {@code error}. */
  @Override
  public void error(String msg) {
    logger.error(msg);
  }

  /** Delegates to SLF4J {@code error} with format arguments. */
  @Override
  public void error(String format, Object... args) {
    logger.error(format, args);
  }

  /** Delegates to SLF4J {@code error} with a throwable. */
  @Override
  public void error(String msg, Throwable t) {
    logger.error(msg, t);
  }

  /** Delegates to SLF4J {@code error} with lazy supplier evaluation. */
  @Override
  public void error(Supplier<String> supplier) {
    if (logger.isErrorEnabled()) {
      logger.error(supplier.get());
    }
  }
}
