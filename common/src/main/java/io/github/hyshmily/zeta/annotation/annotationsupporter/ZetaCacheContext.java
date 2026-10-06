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
package io.github.hyshmily.zeta.annotation.annotationsupporter;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.model.ReadPolicy;
import jakarta.annotation.Nullable;

/**
 * Thread-bound transport for the per-invocation read policy plus the
 * broadcast flag, carrying storage-side decisions from
 * {@code CacheExtensionAspect} into {@link ZetaSpringCache}.
 *
 * <p>This class is a <b>dumb transport</b>: it holds exactly one immutable
 * snapshot per thread and knows nothing about annotations, SpEL, or caching
 * semantics. The aspect (the <em>whether</em> layer) builds and pushes the
 * snapshot; {@link ZetaSpringCache} (the <em>how</em> layer) reads it.
 *
 * <p>Usage pattern (in aspect):
 *
 * <pre>{@code
 * ZetaCacheContext.Snapshot prev = ZetaCacheContext.get().snapshot();
 * try {
 *   ZetaCacheContext.get().push(policy, skipBroadcast);
 *   // proceed to Spring's CacheInterceptor
 * } finally {
 *   ZetaCacheContext.get().restore(prev);
 * }
 * }</pre>
 *
 * <p>The snapshot/restore pair makes nested {@code @Cacheable} invocations on
 * the same thread safe: an inner method always sees its own snapshot (never
 * the outer one), and the outer snapshot is restored afterwards.
 *
 * <p>The holder is deliberately a <b>plain</b> {@link ThreadLocal}, not an
 * inheritable one. Making it inheritable was tried and reverted: a policy
 * pushed on a caller thread would be silently copied into every thread born
 * from it, and under pooled executors (the common case for async loads) a
 * pooled thread inherits the policy of whichever task happened to create it
 * first and then keeps it across unrelated tasks — cross-invocation leakage,
 * which is strictly worse than the visible fallback-to-defaults it was meant
 * to fix. {@code ZetaCacheContextTest#threadIsolation} pins this contract.
 * A loader dispatched to another thread therefore runs with
 * {@link ReadPolicy#defaults()}; if a caller needs its policy carried across
 * an async boundary it must pass a {@link ReadPolicy} explicitly.
 *
 * @see ZetaSpringCache
 * @see ReadPolicy
 * @see NullValue
 */
@Internal
public final class ZetaCacheContext {

  /**
   * One thread's pushed state: the read policy plus the broadcast flag. A
   * record (not two thread-locals) so snapshot/restore stay atomic.
   *
   * @param readPolicy    the read policy for this invocation
   * @param skipBroadcast whether cross-instance sync is suppressed
   */
  public record Snapshot(ReadPolicy readPolicy, boolean skipBroadcast) {}

  private static final ThreadLocal<Snapshot> HOLDER = new ThreadLocal<>();
  private static final ZetaCacheContext INSTANCE = new ZetaCacheContext();

  private ZetaCacheContext() {}

  /**
   * Returns the thread-bound singleton instance of the cache context.
   *
   * @return the singleton {@link ZetaCacheContext} instance
   */
  public static ZetaCacheContext get() {
    return INSTANCE;
  }

  /**
   * Pushes the given read policy with broadcast enabled for the current
   * thread's cache operation.
   *
   * @param policy the read policy to install (may be {@code null} to clear)
   */
  public void push(@Nullable ReadPolicy policy) {
    push(policy, false);
  }

  /**
   * Pushes the given read policy and broadcast flag for the current thread's
   * cache operation. A {@code null} policy clears the thread-local slot.
   *
   * @param policy        the read policy to install (may be {@code null} to clear)
   * @param skipBroadcast whether cross-instance sync is suppressed
   */
  public void push(@Nullable ReadPolicy policy, boolean skipBroadcast) {
    if (policy == null) {
      HOLDER.remove();
    } else {
      HOLDER.set(new Snapshot(policy, skipBroadcast));
    }
  }

  /**
   * Returns the current thread's read policy, or the shared
   * {@link ReadPolicy#defaults() defaults} when none is active.
   * Never returns {@code null}.
   *
   * @return the active read policy, or defaults
   */
  public ReadPolicy current() {
    Snapshot snapshot = HOLDER.get();
    return snapshot != null ? snapshot.readPolicy() : ReadPolicy.defaults();
  }

  /**
   * Returns whether the current thread's invocation suppresses
   * cross-instance sync ({@code false} when no snapshot is active).
   *
   * @return the active skip-broadcast flag
   */
  public boolean skipBroadcast() {
    Snapshot snapshot = HOLDER.get();
    return snapshot != null && snapshot.skipBroadcast();
  }

  /**
   * Captures the current thread's snapshot for later restoration.
   *
   * @return the active snapshot, or {@code null} when none is active
   */
  @Nullable
  public Snapshot snapshot() {
    return HOLDER.get();
  }

  /**
   * Restores a previously captured snapshot. A {@code null} snapshot clears
   * the thread-local slot.
   *
   * @param snapshot the snapshot to restore (may be {@code null})
   */
  public void restore(@Nullable Snapshot snapshot) {
    if (snapshot == null) {
      HOLDER.remove();
    } else {
      HOLDER.set(snapshot);
    }
  }
}
