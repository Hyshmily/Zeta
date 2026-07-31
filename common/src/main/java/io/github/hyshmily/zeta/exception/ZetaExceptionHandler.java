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
import lombok.extern.slf4j.Slf4j;

/**
 * Central exception-handling chain for tasks executed by Zeta's own executors.
 *
 * <p>Zeta's executor framework is designed so that <b>a task exception never kills a worker
 * thread</b> (see {@code SafeScheduledExecutorService} and {@code PerKeyOrderedDispatcher}).
 * Instead of each call site deciding how to report failures, they all funnel the throwable
 * through {@link #handleException(String, Throwable)}, which resolves the target handler with
 * this precedence:
 * <ol>
 *   <li><b>Thread-local handler</b> — bound to the <em>current</em> thread via
 *       {@link #setThreadExceptionHandler(ExceptionHandler)}. Only visible to that one thread.</li>
 *   <li><b>Inheritable handler</b> — bound via {@link #setInheritableExceptionHandler(ExceptionHandler)}.
 *       Propagates to every thread created <em>after</em> the binding, which makes it the
 *       natural way to install a handler for a lazily-started executor pool (set it before the
 *       first task is submitted and every pool thread inherits it).</li>
 *   <li><b>Default handler</b> — JVM-wide, set via {@link #setDefaultExceptionHandler(ExceptionHandler)}.
 *       Good for global concerns such as Micrometer failure counters or alerting.</li>
 *   <li><b>Fallback WARN log</b> — if no handler is configured anywhere, the throwable is logged
 *       at WARN level with the provided context message. This preserves Zeta's historical
 *       observable behaviour (exceptions are visible in logs, never silently swallowed).</li>
 * </ol>
 *
 * <p>{@link #handleException(String, Throwable)} is guaranteed to never throw: a throwing
 * handler is caught and reported at ERROR level, and {@code null} inputs are no-ops. This makes
 * it safe to call on the executor hot path.
 *
 * <p>Design note: this is adapted from Threadly's {@code ExceptionUtils} resolution chain
 * (thread-local → inheritable → default → thread's {@link java.lang.Thread.UncaughtExceptionHandler}).
 * Two deliberate simplifications for Zeta:
 * <ul>
 *   <li>The final fallback is a WARN log instead of {@code UncaughtExceptionHandler} — Zeta's
 *       threads are daemons with no per-thread handler installed, and a log preserves the
 *       pre-existing behaviour.</li>
 *   <li>Threadly's async {@code StackOverflowError} re-reporting is omitted — a stack-overflowed
 *       handler attempt degrades to the ERROR log inside {@link #handleException(String, Throwable)}
 *       itself, which is good enough for a best-effort report.</li>
 * </ul>
 *
 * @see ExceptionHandler
 */
@Slf4j
@Internal
public final class ZetaExceptionHandler {

  private static final ThreadLocal<ExceptionHandler> THREAD_LOCAL_HANDLER = new ThreadLocal<>();

  private static final InheritableThreadLocal<ExceptionHandler> INHERITED_HANDLER =
    new InheritableThreadLocal<>();

  private static volatile ExceptionHandler defaultHandler = null;

  private ZetaExceptionHandler() {
    // utility class, not meant to be instantiated
  }

  /**
   * Binds a handler to the <em>current</em> thread only. Subsequent
   * {@link #handleException(String, Throwable)} calls on this thread resolve to this handler
   * (highest precedence), regardless of any inheritable or default handler.
   *
   * @param exceptionHandler the handler to use on this thread, or {@code null} to clear it
   */
  public static void setThreadExceptionHandler(ExceptionHandler exceptionHandler) {
    if (exceptionHandler == null) {
      // remove() instead of set(null): clears the ThreadLocalMap entry entirely, so a cleared
      // handler cannot be resurrected by an inherited value and the entry is not retained.
      THREAD_LOCAL_HANDLER.remove();
    } else {
      THREAD_LOCAL_HANDLER.set(exceptionHandler);
    }
  }

  /**
   * Binds a handler to the current thread <em>and</em> every thread created from it afterwards.
   *
   * <p>Because {@link InheritableThreadLocal} values are copied to child threads at creation
   * time, this is the recommended way to instrument an executor pool: call this once before the
   * pool starts its (lazily created) worker threads, and every worker inherits the handler.
   *
   * @param exceptionHandler the handler to inherit, or {@code null} to clear it
   */
  public static void setInheritableExceptionHandler(ExceptionHandler exceptionHandler) {
    if (exceptionHandler == null) {
      // remove() instead of set(null): clears the ThreadLocalMap entry entirely, so a cleared
      // handler cannot be resurrected by an inherited value and the entry is not retained.
      INHERITED_HANDLER.remove();
    } else {
      INHERITED_HANDLER.set(exceptionHandler);
    }
  }

  /**
   * Sets the JVM-wide default handler, used when neither a thread-local nor an inheritable
   * handler is bound.
   *
   * @param exceptionHandler the default handler, or {@code null} to clear it (falls back to the
   *                         WARN log)
   */
  public static void setDefaultExceptionHandler(ExceptionHandler exceptionHandler) {
    defaultHandler = exceptionHandler;
  }

  /**
   * Resolves the handler currently in effect for this thread, applying the documented precedence
   * (thread-local → inheritable → default). Returns {@code null} when no handler is configured,
   * in which case {@link #handleException(String, Throwable)} falls back to the WARN log.
   *
   * @return the resolved handler, or {@code null} if none is configured
   */
  public static ExceptionHandler getExceptionHandler() {
    ExceptionHandler handler = THREAD_LOCAL_HANDLER.get();
    if (handler != null) {
      return handler;
    }
    handler = INHERITED_HANDLER.get();
    if (handler != null) {
      return handler;
    }
    return defaultHandler;
  }

  /**
   * Routes a throwable produced by a Zeta-managed task to the resolved handler, or logs it at
   * WARN level (with the given context) when no handler is configured.
   *
   * <p>Guarantees:
   * <ul>
   *   <li>Never throws — a throwing handler is caught and reported at ERROR level, so a broken
   *       handler cannot break the executor path that reported the original failure.</li>
   *   <li>{@code null} input is a no-op.</li>
   *   <li>The context message preserves the informative wording that used to live at each call
   *       site (e.g. "the next run is still scheduled"), so log-based debugging keeps its
   *       quality when no handler is installed.</li>
   * </ul>
   *
   * @param context human-readable description of where the failure occurred; included in the
   *                fallback WARN log
   * @param t       the throwable to handle; may be {@code null} (no-op)
   */
  public static void handleException(String context, Throwable t) {
    if (t == null) {
      return;
    }
    try {
      ExceptionHandler handler = getExceptionHandler();
      if (handler != null) {
        handler.handleException(t);
      } else {
        log.warn("{} — uncaught exception", context, t);
      }
    } catch (Throwable handlerError) {
      // A misbehaving handler must never mask the original failure nor crash the executor
      // thread. Report the handler's own failure at ERROR level and move on.
      log.error("Exception handler threw while handling '{}'", context, handlerError);
    }
  }
}
