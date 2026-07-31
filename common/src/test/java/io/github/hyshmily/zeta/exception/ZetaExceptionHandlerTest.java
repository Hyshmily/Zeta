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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ZetaExceptionHandler}.
 *
 * <p>Verifies the resolution chain (thread-local → inheritable → default → WARN-log fallback),
 * the never-throw guarantee, and the end-to-end propagation into executor pool threads via
 * {@link InheritableThreadLocal}.
 */
class ZetaExceptionHandlerTest {

  @AfterEach
  void clearHandlers() {
    // The handler chain is JVM-global static state; always reset it so tests never leak into
    // each other (or into other test classes running in the same JVM).
    ZetaExceptionHandler.setThreadExceptionHandler(null);
    ZetaExceptionHandler.setInheritableExceptionHandler(null);
    ZetaExceptionHandler.setDefaultExceptionHandler(null);
  }

  @Test
  void noHandler_shouldFallbackToWarnLog_withoutThrowing() {
    // No handler configured anywhere: handleException must degrade to the WARN-log fallback
    // and never throw on the caller's thread.
    assertThatCode(() -> ZetaExceptionHandler.handleException("ctx", new IllegalStateException("boom")))
      .doesNotThrowAnyException();
  }

  @Test
  void nullThrowable_shouldBeNoOp() {
    assertThatCode(() -> ZetaExceptionHandler.handleException("ctx", null)).doesNotThrowAnyException();
  }

  @Test
  void threadHandler_shouldBeInvoked() {
    AtomicInteger calls = new AtomicInteger(0);
    ZetaExceptionHandler.setThreadExceptionHandler(t -> calls.incrementAndGet());

    ZetaExceptionHandler.handleException("ctx", new IllegalStateException("boom"));

    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  void threadHandler_shouldNotLeakToOtherThreads() throws Exception {
    AtomicInteger calls = new AtomicInteger(0);
    // Binding on the test thread must not affect a different (already created) thread.
    ZetaExceptionHandler.setThreadExceptionHandler(t -> calls.incrementAndGet());

    CountDownLatch done = new CountDownLatch(1);
    Thread other = new Thread(() -> {
      ZetaExceptionHandler.handleException("ctx", new IllegalStateException("boom"));
      done.countDown();
    });
    other.start();
    assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(calls.get()).isZero();
  }

  @Test
  void inheritableHandler_shouldPropagateToChildThreads() throws Exception {
    AtomicInteger calls = new AtomicInteger(0);
    // InheritableThreadLocal values are copied at thread creation: a thread created AFTER this
    // binding resolves the handler, which is how a lazily-started executor pool gets instrumented.
    ZetaExceptionHandler.setInheritableExceptionHandler(t -> calls.incrementAndGet());

    CountDownLatch done = new CountDownLatch(1);
    Thread child = new Thread(() -> {
      ZetaExceptionHandler.handleException("ctx", new IllegalStateException("boom"));
      done.countDown();
    });
    child.start();
    assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  void precedence_threadOverInheritableOverDefault() {
    AtomicInteger winner = new AtomicInteger(-1);

    ZetaExceptionHandler.setDefaultExceptionHandler(t -> winner.set(0));
    ZetaExceptionHandler.handleException("ctx", new IllegalStateException());
    assertThat(winner.get()).isZero();

    // Inheritable beats default.
    ZetaExceptionHandler.setInheritableExceptionHandler(t -> winner.set(1));
    ZetaExceptionHandler.handleException("ctx", new IllegalStateException());
    assertThat(winner.get()).isEqualTo(1);

    // Thread-local beats inheritable.
    ZetaExceptionHandler.setThreadExceptionHandler(t -> winner.set(2));
    ZetaExceptionHandler.handleException("ctx", new IllegalStateException());
    assertThat(winner.get()).isEqualTo(2);

    // Unbinding the thread-local falls back to the inheritable handler again.
    ZetaExceptionHandler.setThreadExceptionHandler(null);
    ZetaExceptionHandler.handleException("ctx", new IllegalStateException());
    assertThat(winner.get()).isEqualTo(1);
  }

  @Test
  void throwingHandler_shouldNotPropagate() {
    ZetaExceptionHandler.setDefaultExceptionHandler(
      t -> {
        throw new IllegalStateException("handler is broken");
      }
    );

    // A misbehaving handler must never escape: the original failure is logged at ERROR level
    // and the caller's thread continues.
    assertThatCode(() -> ZetaExceptionHandler.handleException("ctx", new IllegalStateException("boom")))
      .doesNotThrowAnyException();
  }
}
