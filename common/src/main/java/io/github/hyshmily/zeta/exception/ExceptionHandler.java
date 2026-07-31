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

import io.github.hyshmily.zeta.Internal;

/**
 * Callback invoked by {@link ZetaExceptionHandler#handleException(String, Throwable)} when a
 * task running on a Zeta executor throws an unexpected {@link Throwable}.
 *
 * <p>Zeta's executors ({@code SafeScheduledExecutorService}, {@code StandardThreadExecutor} and
 * {@code PerKeyOrderedDispatcher}) never let a task exception kill the worker thread, but they
 * need a well-defined place to report it. Instead of hard-coding a log statement at every call
 * site, they funnel the throwable through {@link ZetaExceptionHandler}, which resolves a handler
 * with the following precedence:
 * <ol>
 *   <li>a handler bound to the current thread via
 *       {@link ZetaExceptionHandler#setThreadExceptionHandler(ExceptionHandler)}</li>
 *   <li>a handler inherited by threads spawned from the binding thread via
 *       {@link ZetaExceptionHandler#setInheritableExceptionHandler(ExceptionHandler)}</li>
 *   <li>the JVM-wide default via
 *       {@link ZetaExceptionHandler#setDefaultExceptionHandler(ExceptionHandler)}</li>
 *   <li>a fallback WARN log (the behaviour that predates this chain)</li>
 * </ol>
 *
 * <p>Typical uses: increment a Micrometer failure counter, feed an alerting pipeline, or attach
 * contextual tags that are not available at the call site. A handler must never throw; the
 * {@link ZetaExceptionHandler} guards against misbehaving handlers itself.
 *
 * <p>This mirrors the handler chain of Threadly's {@code ExceptionUtils} (thread-local,
 * inheritable, default, then the thread's {@link java.lang.Thread.UncaughtExceptionHandler}),
 * adapted to Zeta's logging-based fallback.
 *
 * @see ZetaExceptionHandler
 */
@FunctionalInterface
@Internal
public interface ExceptionHandler {

  /**
   * Handles a throwable produced by a Zeta-managed task.
   *
   * <p>Implementations should be idempotent and fast: this method may be invoked on the
   * executor hot path. It must not throw — {@link ZetaExceptionHandler} catches any throwable
   * escaping this method and logs it at ERROR level.
   *
   * @param t the throwable to handle; never {@code null}
   */
  void handleException(Throwable t);
}
