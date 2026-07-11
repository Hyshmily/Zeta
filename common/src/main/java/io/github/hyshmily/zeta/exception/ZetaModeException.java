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

import io.github.hyshmily.zeta.Zeta;
import lombok.Getter;

/**
 * Thrown when a {@link Zeta} API is called in a
 * deployment mode that does not support it.
 *
 * <p>For example, calling {@code hotKey.get(key, reader)} in Worker-only mode
 * throws this exception because there is no app-side cache. Callers can probe
 * the current mode before calling cache-dependent APIs:
 * <pre>{@code
 * if (hotKey.isApp()) {
 *     Optional<User> user = hotKey.get("user:42", repo::findById);
 * }
 * }</pre>
 *
 * <p>The exception carries three fields to help diagnose the mismatch:
 * <ul>
 *   <li>{@code getOperation()} — the API name that was called</li>
 *   <li>{@code getCurrentMode()} — the human-readable label of the current
 *       deployment mode (e.g. {@code "Worker-only mode"})</li>
 *   <li>{@code getRequiredMode()} — the human-readable label of the mode
 *       required to perform the operation (e.g. {@code "App-mode cache"})</li>
 * </ul>
 *
 * @see Zeta#isApp()
 * @see Zeta#isWorker()
 */
@Getter
public class ZetaModeException extends ZetaContextException {

  private final String operation;

  private final String currentMode;

  private final String requiredMode;

  /**
   * Creates a new {@code ZetaModeException}.
   *
   * @param operation     the API operation name that was called (e.g. {@code "get"})
   * @param currentMode   human-readable label of the current deployment mode
   *                      (e.g. {@code "Worker-only mode"})
   * @param requiredMode  human-readable label of the mode required to perform
   *                      the operation (e.g. {@code "App-mode cache"})
   */
  public ZetaModeException(String operation, String currentMode, String requiredMode) {
    super(
      "ZetaModeException",
      "HotKey '" + operation + "' requires " + requiredMode + ", but instance is in " + currentMode
    );
    this.operation = operation;
    this.currentMode = currentMode;
    this.requiredMode = requiredMode;
  }
}
