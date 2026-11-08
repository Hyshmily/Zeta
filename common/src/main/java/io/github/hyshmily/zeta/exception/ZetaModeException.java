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
import java.io.Serial;
import lombok.Getter;

/**
 * Thrown when a {@link Zeta} API is called in a deployment mode that does not support it.
 *
 * <p>For example, calling {@code zeta.get(key, reader, options)} in Worker-only mode throws this
 * exception because there is no app-side cache. Callers can probe the current mode before calling
 * cache-dependent APIs:
 * <pre>{@code
 * if (zeta.isCacheAvailable()) {
 *     User user = zeta.get("user:42", repo::findById, GetPolicy.defaults());
 * }
 * }</pre>
 *
 * @see Zeta#isCacheAvailable()
 */
@Getter
public class ZetaModeException extends IllegalStateException {

  @Serial
  private static final long serialVersionUID = 1L;

  /** Human-readable label of the mode the operation requires. */
  private final String requiredMode;

  /** Human-readable label of the current deployment mode. */
  private final String currentMode;

  /** The API operation that was called (e.g. {@code "get"}). */
  private final String operation;

  /**
   * Creates a mode mismatch.
   *
   * @param operation    the API operation name that was called (e.g. {@code "get"})
   * @param currentMode  human-readable label of the current deployment mode
   *                     (e.g. {@code "Worker-only mode"})
   * @param requiredMode human-readable label of the mode required to perform the operation
   *                     (e.g. {@code "App-mode cache"})
   */
  public ZetaModeException(String operation, String currentMode, String requiredMode) {
    super("Zeta '" + operation + "' requires " + requiredMode + ", but instance is in " + currentMode);
    this.operation = operation;
    this.currentMode = currentMode;
    this.requiredMode = requiredMode;
  }
}
