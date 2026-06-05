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
package io.github.hyshmily.hotkey.log;

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

  void debug(String msg);

  void debug(String format, Object... args);

  void debug(Supplier<String> supplier);

  void info(String msg);

  void info(String format, Object... args);

  void info(Supplier<String> supplier);

  void warn(String msg);

  void warn(String format, Object... args);

  void warn(Supplier<String> supplier);

  void error(String msg);

  void error(String format, Object... args);

  void error(String msg, Throwable t);

  void error(Supplier<String> supplier);
}
