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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.NonNull;

/**
 * A named, daemon-mode {@link ThreadFactory} used consistently across all HotKey
 * thread pools. Every created thread is a daemon thread with
 * {@link Thread#NORM_PRIORITY} and a human-readable name prefix for diagnostics.
 * <p>
 * Thread names follow the pattern {@code <namePrefix><N>} where {@code N} is an
 * auto-incrementing counter starting at 1 (e.g. {@code hotkey-scheduler-1}).
 */
@Internal
public class HotKeyThreadFactory implements ThreadFactory {

  private final AtomicInteger threadNumber = new AtomicInteger(1);
  private final String namePrefix;

  /**
   * Creates a factory whose threads will be named {@code <namePrefix><N>}.
   *
   * @param namePrefix the prefix for all thread names created by this factory
   *                   (e.g. {@code "hotkey-scheduler-"})
   */
  public HotKeyThreadFactory(String namePrefix) {
    this.namePrefix = namePrefix;
  }

  @Override
  public Thread newThread(@NonNull Runnable r) {
    Thread t = new Thread(r, namePrefix + threadNumber.getAndIncrement());
    t.setDaemon(true);
    if (t.getPriority() != Thread.NORM_PRIORITY) {
      t.setPriority(Thread.NORM_PRIORITY);
    }
    return t;
  }
}
