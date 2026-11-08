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

import lombok.extern.slf4j.Slf4j;

/**
 * Failures raised by tasks running on a Zeta executor are reported here, never rethrown on the
 * executor thread.
 *
 * <p>The handlers resolve in this order:
 * <ol>
 *   <li>the handler bound to the current thread ({@link #setThreadExceptionHandler(Handler)});</li>
 *   <li>the handler inherited at thread creation ({@link #setInheritableExceptionHandler(Handler)});</li>
 *   <li>the JVM-wide default ({@link #setDefaultExceptionHandler(Handler)});</li>
 *   <li>a WARN log.</li>
 * </ol>
 *
 * <p>{@link #handleException(String, Throwable)} never throws: a handler that throws is logged at
 * ERROR with the original failure attached as suppressed, so neither a broken handler nor a
 * failing task can break the executor that reported it.
 *
 * @see Handler
 */
@Slf4j
public final class ZetaExceptionHandler {

  private static final InheritableThreadLocal<Handler> INHERITED_HANDLER = new InheritableThreadLocal<>();

  private static final ThreadLocal<Handler> THREAD_LOCAL_HANDLER = new ThreadLocal<>();

  @SuppressWarnings("java:S3077") // a single reference; volatile is enough, no AtomicReference needed
  private static volatile Handler defaultHandler;

  private ZetaExceptionHandler() {
    // utility class, not meant to be instantiated
  }

  /**
   * Binds a handler to the <em>current</em> thread only (highest precedence).
   *
   * @param exceptionHandler the handler to use on this thread, or {@code null} to clear it
   */
  public static void setThreadExceptionHandler(Handler exceptionHandler) {
    if (exceptionHandler == null) {
      THREAD_LOCAL_HANDLER.remove();
    } else {
      THREAD_LOCAL_HANDLER.set(exceptionHandler);
    }
  }

  /**
   * Binds a handler to the current thread and to every thread created from it afterwards.
   *
   * <p>The value is copied when a thread is created, so it does not reach a pool whose threads
   * already exist. Use {@link #setDefaultExceptionHandler(Handler)} for a process-wide handler.
   *
   * @param exceptionHandler the handler to inherit, or {@code null} to clear it
   */
  public static void setInheritableExceptionHandler(Handler exceptionHandler) {
    if (exceptionHandler == null) {
      INHERITED_HANDLER.remove();
    } else {
      INHERITED_HANDLER.set(exceptionHandler);
    }
  }

  /**
   * Sets the JVM-wide default handler, used when neither a thread-local nor an inheritable handler
   * is bound.
   *
   * @param exceptionHandler the default handler, or {@code null} to fall back to the WARN log
   */
  public static void setDefaultExceptionHandler(Handler exceptionHandler) {
    defaultHandler = exceptionHandler;
  }

  /**
   * Routes a task failure to the resolved handler, or logs it at WARN when none is configured.
   *
   * @param context human-readable description of where the failure occurred; included in the
   *                fallback log
   * @param t       the throwable to handle; {@code null} is a no-op
   */
  public static void handleException(String context, Throwable t) {
    if (t == null) {
      return;
    }
    try {
      Handler handler = currentHandler();
      if (handler != null) {
        handler.handleException(context, t);
      } else {
        log.warn("{} — uncaught exception", context, t);
      }
    } catch (Throwable handlerError) {
      // A handler failure must neither mask the original nor kill the executor thread.
      if (handlerError != t) {
        handlerError.addSuppressed(t);
      }
      log.error("Exception handler threw while handling '{}'", context, handlerError);
    }
  }

  /**
   * Resolves the handler in effect for the current thread.
   *
   * @return the resolved handler, or {@code null} when none is configured
   */
  private static Handler currentHandler() {
    Handler handler = THREAD_LOCAL_HANDLER.get();
    if (handler != null) {
      return handler;
    }
    handler = INHERITED_HANDLER.get();
    if (handler != null) {
      return handler;
    }
    return defaultHandler;
  }

  /** Callback invoked when a task running on a Zeta executor throws. */
  @FunctionalInterface
  public interface Handler {
    /**
     * Handles a throwable produced by a Zeta-managed task.
     *
     * @param t the throwable to handle; never {@code null}
     */
    void handleException(Throwable t);

    /**
     * Handles a throwable together with the call site's context.
     *
     * <p>The default drops the context; override it when a metric tag or alert needs to
     * distinguish, for example, the scheduler from the ordered dispatcher.
     *
     * @param context human-readable description of where the failure occurred; never {@code null}
     * @param t       the throwable to handle; never {@code null}
     */
    default void handleException(String context, Throwable t) {
      handleException(t);
    }
  }
}
