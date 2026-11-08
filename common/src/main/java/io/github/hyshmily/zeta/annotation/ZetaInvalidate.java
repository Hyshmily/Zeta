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
package io.github.hyshmily.zeta.annotation;

import io.github.hyshmily.zeta.cache.cachesupport.policy.InvalidatePolicy;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares "this call makes that key wrong" — the declarative form of
 * {@link io.github.hyshmily.zeta.Zeta#invalidate(String, InvalidatePolicy)}.
 *
 * <p>
 * Nothing is stored and nothing is loaded: the entry is removed locally
 * and/or broadcast to peers according to {@link #scope()} — both by default.
 * Peer convergence is best-effort and bounded by TTL, not by latency.
 *
 * <p>
 * <b>Ordering.</b> By default the removal runs after the body returns, so a
 * body that throws leaves the cache untouched; with the default advice ordering
 * the transaction has already committed by then, and a custom order that places
 * the transaction outside makes the facade defer the removal to after-commit.
 * {@link #beforeInvocation()} flips that for the wedge case — a body that must
 * observe an empty cache — and then the removal also bypasses the transaction's
 * after-commit deferral, because "before the body" and "after commit" cannot
 * both hold.
 *
 * <p>
 * <b>{@code #result}.</b> The default (after-the-body) form resolves its key
 * after the body returned, so the key expression may read {@code #result} — an
 * invalidation keyed by an id the body generated. A key that does not read
 * {@code #result} is resolved before the body, so a null argument or evaluation
 * failure surfaces before the business side effect.
 * {@code beforeInvocation = true}
 * resolves the key before the body runs, where {@code #result} does not exist.
 *
 * <p>
 * <b>A null result is evaluated by SpEL, not special-cased.</b> A key that
 * dereferences a null result — {@code #result.id} on a body that returned
 * {@code null} — fails at evaluation, after the body ran. A key that must
 * tolerate a nullable return writes {@code #result?.id}: safe navigation
 * yields {@code null}, and a null key is a no-op removal. An expression that
 * handles null itself ({@code #result != null ? #result.id : #fallbackId})
 * runs unchanged and removes its fallback key. A key that does not read
 * {@code #result} is resolved before the body and treats a null resolution as
 * a configuration error.
 *
 * <p>
 * <b>One verb per method</b> — see {@link ZetaCached}.
 *
 * <pre>{@code
 * @ZetaInvalidate(cache = "users", key = "#id")
 * void rename(String id, String name) {
 *   repository.rename(id, name);
 * }
 * }</pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ZetaInvalidate {
  /**
   * The cache namespace, required (same encoding as {@link ZetaCached#cache()}).
   *
   * @return the namespace
   */
  String cache();

  /**
   * SpEL expression resolving the logical key to remove. The arguments are
   * bound as {@code #p0}/{@code #a0} and by parameter name; {@code #root.*} is
   * always bound, and {@code #result} is bound for the default
   * after-the-body form (never for {@link #beforeInvocation()}). A key that
   * does not read {@code #result} is resolved before the body even in the
   * after-the-body form, so a null argument fails before the side effect.
   * Empty means the default key — see {@link ZetaCached#key()}.
   *
   * @return the key expression, or empty for the default key
   */
  String key() default "";

  /**
   * Whether the removal runs before the body instead of after it. A
   * pre-invocation removal resolves its key before the body runs, so the key
   * expression must not depend on {@code #result}; declaring it is rejected at
   * resolution.
   *
   * @return {@code true} to invalidate before invoking the method
   */
  boolean beforeInvocation() default false;

  /**
   * Where the invalidation applies: {@link InvalidatePolicy.Scope#BOTH}
   * (default) removes locally and notifies peers, {@code LOCAL} keeps the
   * message channel quiet, {@code REMOTE} notifies peers and keeps this
   * instance's entry.
   *
   * @return the invalidation scope
   */
  InvalidatePolicy.Scope scope() default InvalidatePolicy.Scope.BOTH;
}
