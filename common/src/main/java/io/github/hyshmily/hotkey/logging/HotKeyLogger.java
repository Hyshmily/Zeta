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

/**
 * HotKey's logging facade. Method signatures are compatible with SLF4J's
 * {@code Logger} so existing {@code log.xxx()} calls compile without change
 * when swapping from Lombok's {@code @Slf4j} to a {@code HotKeyLogger} field.
 *
 * <p>Adds {@link Supplier}-based lazy evaluation on top of SLF4J's built-in
 * parameterized message support — the supplier is only called when the
 * corresponding log level is enabled.
 */
public interface HotKeyLogger {

  /** Log a message at DEBUG level. */
  void debug(String msg);

  /** Log a parameterized message at DEBUG level. */
  void debug(String format, Object... args);

  /** Log a lazily-supplied message at DEBUG level. */
  void debug(Supplier<String> supplier);

  /** Log a message at INFO level. */
  void info(String msg);

  /** Log a parameterized message at INFO level. */
  void info(String format, Object... args);

  /** Log a lazily-supplied message at INFO level. */
  void info(Supplier<String> supplier);

  /** Log a message at WARN level. */
  void warn(String msg);

  /** Log a parameterized message at WARN level. */
  void warn(String format, Object... args);

  /** Log a lazily-supplied message at WARN level. */
  void warn(Supplier<String> supplier);

  /** Log a message at ERROR level. */
  void error(String msg);

  /** Log a parameterized message at ERROR level. */
  void error(String format, Object... args);

  /** Log a message at ERROR level with an associated throwable. */
  void error(String msg, Throwable t);

  /** Log a lazily-supplied message at ERROR level. */
  void error(Supplier<String> supplier);
}
