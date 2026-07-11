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
package io.github.hyshmily.hotkey.util;

import io.github.hyshmily.hotkey.Internal;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

/**
 * Performance-optimized clock source that caches {@link System#currentTimeMillis()}
 * on a low-frequency background thread (every 5ms) to avoid the cost of a
 * {@code native} call on every read via {@link #currentTimeMillis()}.
 * <p>
 * The background daemon thread is started once via {@link #start()} and runs
 * for the lifetime of the JVM.  If the thread is interrupted it silently falls
 * back to calling {@link System#currentTimeMillis()} directly.
 * <p>
 * All time-sensitive components in the hot-path (expiry, rate limiting, circuit
 * breaker) use this source instead of {@code System.currentTimeMillis()}.
 */
@Internal
@Slf4j
public final class TimeSource {

  private static volatile long currentMillis = System.currentTimeMillis();
  private static final AtomicBoolean threadRunning = new AtomicBoolean(false);
  private static final AtomicInteger threadTryCount = new AtomicInteger(0);
  private static final int THREAD_TRY_MAX = 3;

  /**
   * Start the background clock-cache thread.  Idempotent after the thread is
   * running.  If the thread dies unexpectedly it will be restarted up to
   * {@link #THREAD_TRY_MAX} times with a 1-second delay between attempts.
   * Called automatically during {@code HotKeyFacadeAutoConfiguration}
   * initialisation.
   */
  @SuppressWarnings("BusyWait")
  public static void start() {
    if (threadTryCount.incrementAndGet() <= THREAD_TRY_MAX && threadRunning.compareAndSet(false, true)) {
      Thread t = new HotKeyThreadFactory("TimeSource").newThread(() -> {
        while (threadRunning.get()) {
          currentMillis = System.currentTimeMillis();
          try {
            Thread.sleep(5);
          } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
            threadRunning.set(false);
          }
        }
      });
      t.setUncaughtExceptionHandler((th, ex) -> {
        log.error("TimeSource thread terminated unexpectedly, will restart after 1s.", ex);
        threadRunning.set(false);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ignored) {
          Thread.currentThread().interrupt();
        }
        start();
      });
      t.start();
    }
  }

  /**
   * Returns the current time in milliseconds since the epoch.
   * <p>
   * When the background thread is running, this returns the cached value
   * updated every 5ms — avoiding a {@code native} JNI call on every read.
   * Falls back to {@link System#currentTimeMillis()} if the thread has not
   * been started or was interrupted.
   *
   * @return current time in milliseconds (epoch-based)
   */
  public static long currentTimeMillis() {
    return threadRunning.get() ? currentMillis : System.currentTimeMillis();
  }

  private TimeSource() {}
}
