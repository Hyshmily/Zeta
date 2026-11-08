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
package io.github.hyshmily.zeta.cache.cachesupport.policy;

import java.util.Objects;

/**
 * Immutable invalidation options: <b>where</b> the removal applies and
 * <b>when</b> it runs. The annotation layer prebuilds one instance per plan and
 * reuses it for every invocation; the record is immutable, so sharing is safe.
 *
 * <p>One of three closed option records, matching the read side
 * ({@link GetPolicy}) and the write side ({@link PutPolicy}).
 *
 * <h2>Scope</h2>
 * {@link Scope#LOCAL} removes this instance's entry only; {@link Scope#REMOTE}
 * broadcasts the invalidation to peers without touching the local entry (the
 * caller already refreshed it by another route — binlog replay, bulk import,
 * an operator script); {@link Scope#BOTH} does both and is the default.
 * Broadcasts are fire-and-forget fan-out tolerated by TTL convergence, never a
 * blocking wait for peer acknowledgement (design decisions: sync messages).
 *
 * <h2>Transactional timing</h2>
 * By default an invalidation inside an active transaction defers to
 * {@code afterCommit} (registration order becomes version order, a rollback
 * leaks no removal — design decisions: invalidation). {@code immediate = true} bypasses that
 * deferral. This exists for the
 * {@code @ZetaInvalidate(beforeInvocation = true)} semantics.
 *
 * @param scope     where the invalidation applies (never {@code null})
 * @param immediate whether the removal bypasses the active-transaction
 *                  after-commit deferral and executes on the calling thread
 */
public record InvalidatePolicy(Scope scope, boolean immediate) {
  /**
   * Compact constructor: {@code scope} is the record's only non-primitive
   * component and every consumer dereferences it without a null check, so a
   * {@code null} scope is rejected at construction instead of surfacing as an
   * NPE several frames deeper in the call.
   *
   * @throws NullPointerException when {@code scope} is {@code null}
   */
  public InvalidatePolicy {
    Objects.requireNonNull(scope, "scope must not be null");
  }

  /**
   * Where an invalidation applies. The three values are closed: a broadcast and
   * a local removal are independent, but "neither" is not a meaningful request.
   */
  public enum Scope {
    /** Remove the local entry only; send nothing. */
    LOCAL,

    /** Send the invalidation to peers only; leave the local entry untouched. */
    REMOTE,

    /** Remove locally and notify peers (the default). */
    BOTH;

    /** Whether peers are notified. */
    public boolean remote() {
      return this != LOCAL;
    }
  }

  /** Singleton carrying local-only semantics. */
  private static final InvalidatePolicy LOCAL_ONLY = new InvalidatePolicy(Scope.LOCAL, false);
  /** Singleton carrying all-default semantics (both sides, transactional deferral). */
  private static final InvalidatePolicy DEFAULTS = new InvalidatePolicy(Scope.BOTH, false);

  /**
   * Returns the shared all-defaults options: removed locally and synchronized
   * to peer instances, transaction-aware.
   *
   * @return the default options singleton
   */
  public static InvalidatePolicy defaults() {
    return DEFAULTS;
  }

  /**
   * Returns the shared local-only options: no sync message is sent, only the
   * local entry is removed.
   *
   * @return the local-only options singleton
   */
  public static InvalidatePolicy localOnly() {
    return LOCAL_ONLY;
  }

  /**
   * Returns options with {@code immediate} semantics, mapping {@code sync} to
   * {@link Scope#BOTH} or {@link Scope#LOCAL}.
   *
   * @param sync whether cross-instance sync messages are sent
   * @return immediate invalidation options
   */
  public static InvalidatePolicy immediate(boolean sync) {
    return new InvalidatePolicy(sync ? Scope.BOTH : Scope.LOCAL, true);
  }

  /**
   * Whether peers are notified. Alias for {@code scope.remote()}, named for
   * callers that read the flag as "sync on/off".
   *
   * @return {@code true} for {@link Scope#BOTH} and {@link Scope#REMOTE}
   */
  public boolean sync() {
    return scope.remote();
  }
}
