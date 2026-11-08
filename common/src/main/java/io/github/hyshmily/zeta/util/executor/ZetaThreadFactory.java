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
package io.github.hyshmily.zeta.util.executor;

import io.github.hyshmily.zeta.util.Internal;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.NonNull;
import org.springframework.util.Assert;

/**
 * A named, daemon-mode {@link ThreadFactory} used consistently across all Zeta
 * thread pools. Every created thread is a daemon thread with
 * {@link Thread#NORM_PRIORITY} and a human-readable name prefix for diagnostics.
 * <p>
 * Thread names follow the pattern {@code <namePrefix><N>} where {@code N} is an
 * auto-incrementing counter starting at 1 (e.g. {@code zeta-scheduler-1}). A
 * trailing {@code -} is appended to the prefix when absent, so
 * {@code "zeta-scheduler"} and {@code "zeta-scheduler-"} name threads identically.
 */
@Internal
public class ZetaThreadFactory implements ThreadFactory {

  private final AtomicInteger threadNumber = new AtomicInteger(1);
  private final String namePrefix;

  /**
   * Creates a factory whose threads will be named {@code <namePrefix><N>}.
   *
   * @param namePrefix the prefix for all thread names created by this factory
   *                   (e.g. {@code "zeta-scheduler-"}); a trailing {@code -} is
   *                   added when absent
   */
  public ZetaThreadFactory(String namePrefix) {
    Assert.hasText(namePrefix, "namePrefix must not be blank");
    this.namePrefix = namePrefix.endsWith("-") ? namePrefix : namePrefix + "-";
  }

  @Override
  public Thread newThread(@NonNull Runnable task) {
    // New threads inherit the creating thread's daemon flag and priority — both
    // are forced back to the documented daemon/NORM defaults regardless of caller.
    Thread thread = new Thread(task, namePrefix + threadNumber.getAndIncrement());
    thread.setDaemon(true);
    if (thread.getPriority() != Thread.NORM_PRIORITY) {
      thread.setPriority(Thread.NORM_PRIORITY);
    }
    return thread;
  }
}
