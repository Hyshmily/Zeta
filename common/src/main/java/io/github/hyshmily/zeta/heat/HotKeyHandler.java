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
package io.github.hyshmily.zeta.heat;

/**
 * The single replaceable strategy for what a heat lease change does to this instance's data plane.
 *
 * <p>The framework has already applied the decision to its lease table when a callback runs, and
 * the callbacks are purely observational: a handler cannot change whether a lease exists, and it
 * cannot write an entry or a deadline. What it may do is spend a background load
 * ({@link HotKeyContext#prewarmIfAbsent()}, {@link HotKeyContext#prewarmWith}), arm a local renewal
 * ({@link HotKeyContext#armKeepWarm()}) or drop one the namespace no longer authorizes
 * ({@link HotKeyContext#cancelKeepWarmIfCold()}).
 *
 * <p>Both callbacks receive the applied decision's context, which carries the event facts
 * ({@link HotKeyContext#decision()}, {@link HotKeyContext#isNewlyHot()}) and the guarded
 * operations. A new per-decision fact is a context getter, never another callback parameter.
 *
 * <p>A handler runs on the ordered per-key decision dispatcher: calls for one key are serialized,
 * different keys run concurrently, and a blocking call stalls every key behind it. Keep the body
 * short and non-blocking; the loads themselves are asynchronous and return a future.
 *
 * <p>Expose an implementation as a Spring {@code @Bean} to replace the default; autoconfiguration
 * backs off with {@code @ConditionalOnMissingBean}, so the first bean wins.
 */
public interface HotKeyHandler {
  /**
   * Called after a valid HOT lease is granted, on the ordered decision dispatcher thread.
   *
   * <p>The default separates the two situations the decision can describe: an absent key is
   * prewarmed through the namespace policy, while a live resident has a renewal armed for it. A
   * repeated HOT over the same lease is not a new heat event and does nothing at all, so a handler
   * that spends a load must gate on {@link HotKeyContext#isNewlyHot()} as well.
   *
   * @param context the applied decision's read-only facts and guarded data-plane operations
   */
  default void onHot(HotKeyContext context) {
    if (!context.isNewlyHot()) {
      // A repeat broadcast over a live lease is not a new heat event: it must not arm a second
      // renewal and must not cold-load a key that expired under the lease.
      return;
    }
    if (context.hasLiveEntry()) {
      context.armKeepWarm();
    } else {
      context.prewarmIfAbsent();
    }
  }

  /**
   * Called after a COOL decision is applied, on the ordered decision dispatcher thread. The
   * decision is applied even when this node never held a HOT lease (it writes the version
   * tombstone), so this hook reports "the cluster no longer considers the key hot" rather than
   * "a live lease just ended"; {@link HotKeyListener#onCool} is the lease-end observer. A handler
   * that cleans up state it created in {@code onHot} must track whether it created one.
   *
   * <p>The default cancels a renewal task only when the key's keep-warm trigger no longer accepts
   * any live heat signal, so a locally hot key with {@code keepWarm = LOCAL}/{@code ANY} keeps its
   * task. A COOL never evicts the resident and never shortens a deadline.
   *
   * @param context the applied decision's read-only facts and guarded data-plane operations
   */
  default void onCool(HotKeyContext context) {
    context.cancelKeepWarmIfCold();
  }
}
