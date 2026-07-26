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
import io.github.hyshmily.zeta.model.CachePolicy;
import jakarta.annotation.Nullable;

/**
 * Thread-bound transport for the per-invocation {@link CachePolicy}, carrying
 * storage-side decisions from {@code CacheExtensionAspect} into
 * {@link ZetaSpringCache}.
 *
 * <p>This class is a <b>dumb transport</b>: it holds exactly one immutable
 * policy per thread and knows nothing about annotations, SpEL, or caching
 * semantics. The aspect (the <em>whether</em> layer) builds and pushes the
 * policy; {@link ZetaSpringCache} (the <em>how</em> layer) reads it.
 *
 * <p>Usage pattern (in aspect):
 *
 * <pre>{@code
 * CachePolicy prev = ZetaCacheContext.get().snapshot();
 * try {
 *   ZetaCacheContext.get().push(policy);
 *   // proceed to Spring's CacheInterceptor
 * } finally {
 *   ZetaCacheContext.get().restore(prev);
 * }
 * }</pre>
 *
 * <p>The snapshot/restore pair makes nested {@code @Cacheable} invocations on
 * the same thread safe: an inner method always sees its own policy (never the
 * outer one), and the outer policy is restored afterwards.
 *
 * @see ZetaSpringCache
 * @see CachePolicy
 * @see NullValue
 */
@Internal
public final class ZetaCacheContext {

  private static final ThreadLocal<CachePolicy> HOLDER = new ThreadLocal<>();
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
   * Pushes the given policy for the current thread's cache operation.
   * A {@code null} policy clears the thread-local slot.
   *
   * @param policy the policy to install (may be {@code null} to clear)
   */
  public void push(@Nullable CachePolicy policy) {
    if (policy == null) {
      HOLDER.remove();
    } else {
      HOLDER.set(policy);
    }
  }

  /**
   * Returns the current thread's policy, or the shared
   * {@link CachePolicy#defaults() defaults} when none is active.
   * Never returns {@code null}.
   *
   * @return the active policy, or defaults
   */
  public CachePolicy current() {
    CachePolicy policy = HOLDER.get();
    return policy != null ? policy : CachePolicy.defaults();
  }

  /**
   * Captures the current thread's policy for later restoration.
   *
   * @return the active policy, or {@code null} when none is active
   */
  @Nullable
  public CachePolicy snapshot() {
    return HOLDER.get();
  }

  /**
   * Restores a previously captured snapshot. A {@code null} snapshot clears
   * the thread-local slot.
   *
   * @param snapshot the snapshot to restore (may be {@code null})
   */
  public void restore(@Nullable CachePolicy snapshot) {
    push(snapshot);
  }
}
