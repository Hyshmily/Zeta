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
package io.github.hyshmily.zeta.sync;

import io.github.hyshmily.zeta.util.Internal;
import java.util.List;
import java.util.function.Consumer;
import org.slf4j.Logger;

/**
 * The hook-dispatch tail shared by the sync and Worker decision planes.
 *
 * <p>Both planes run the same "one terminal callback per message, and a broken callback never
 * breaks the decision" rule; keeping it here stops the two contexts from drifting apart. Entry
 * snapshots and loader reads are plane-specific and stay with their callers.
 */
@Internal
public final class DecisionPlaneSupport {

  private DecisionPlaneSupport() {}

  /**
   * Runs one hook callback per registered hook; a broken hook never breaks the decision
   * apply. The caller checks for an empty list first, so no capturing callback is
   * allocated in the default no-hook deployment.
   *
   * @param hooks    the registered hooks
   * @param hookType the hook interface name used in the WARN line
   * @param hookName the callback name used in the WARN line
   * @param key      the key the decision touched
   * @param callback the callback to invoke for each hook
   * @param log      the plane's logger
   * @param <H>      the hook type
   */
  public static <H> void fireHooks(
    List<H> hooks,
    String hookType,
    String hookName,
    String key,
    Consumer<H> callback,
    Logger log
  ) {
    if (hooks.isEmpty()) {
      return;
    }
    for (H hook : hooks) {
      try {
        callback.accept(hook);
      } catch (Exception e) {
        log.warn("{}.{} failed for key={}", hookType, hookName, key, e);
      }
    }
  }
}
