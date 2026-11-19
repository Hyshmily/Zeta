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
import java.lang.annotation.Repeatable;
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
 * {@code #result} is resolved before the body when no condition is present, so a
 * null argument or evaluation failure surfaces before the business side effect;
 * a present condition gates key resolution and, in the after-the-body form, is
 * itself evaluated after the body.
 * {@code beforeInvocation = true}
 * resolves the key before the body runs, where {@code #result} does not exist.
 *
 * <p>
 * <b>A null result is evaluated by SpEL, not special-cased.</b> A key that
 * dereferences a null result — {@code #result.id} on a body that returned
 * {@code null} — fails at evaluation, after the body ran. A key that must
 * tolerate a nullable return writes {@code #result?.id}: safe navigation
 * yields {@code null}, and a null key is a no-op removal. A non-null result
 * whose selected property is null also yields a null key and is skipped. An
 * expression that handles null itself
 * ({@code #result != null ? #result.id : #fallbackId})
 * runs unchanged and removes its fallback key. A key that a pre-body phase
 * resolves treats a null resolution as a configuration error; a post-body key
 * (result-dependent, or resolved after a condition) treats a null as a no-op.
 *
 * <p>
 * <b>Repeatable.</b> A method may declare several invalidations, each with its
 * own namespace, key and policy — a write that must drop both an id key and a
 * secondary index key. Removals are independent, so their execution order is
 * not a contract; two unrelated views must still declare the same list in the
 * same order to be recognized as one declaration. A read or write verb may not
 * share the method with an invalidation.
 *
 * <p>
 * <b>{@code eachKey} and {@code allEntries}.</b> {@code eachKey = true} expands
 * an array or {@link Iterable} key into one physical key per non-null element,
 * so a batch delete invalidates every id. {@code allEntries = true} clears the
 * whole namespace instead of one key; the two are mutually exclusive with each
 * other, and {@code allEntries} forbids a {@code key}.
 *
 * <p>
 * <b>One read or write verb per method</b> — see {@link ZetaCached}.
 *
 * <pre>{@code
 * @ZetaInvalidate(cache = "users", key = "#id")
 * void rename(String id, String name) {
 *   repository.rename(id, name);
 * }
 *
 * @ZetaInvalidate(cache = "users", key = "#ids", eachKey = true)
 * @ZetaInvalidate(cache = "user_email", key = "#result?.emails", eachKey = true)
 * void deleteAll(List<Long> ids) {
 *   repository.deleteAll(ids);
 * }
 * }</pre>
 */
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Repeatable(ZetaInvalidates.class)
public @interface ZetaInvalidate {
  /**
   * The cache namespace, or empty to inherit {@link ZetaConfig#cache()} from the bean's user class
   * (or the business interfaces of a JDK proxy), falling back to the method's declaring class
   * (same encoding as {@link ZetaCached#cache()}).
   *
   * @return the namespace, or empty to inherit the class configuration
   */
  String cache() default "";

  /**
   * SpEL expression resolving the logical key to remove. The arguments are
   * bound as {@code #p0}/{@code #a0} and by parameter name; {@code #root.*} is
   * always bound, and {@code #result} is bound for the default
   * after-the-body form (never for {@link #beforeInvocation()}). A key that
   * does not read {@code #result} is resolved before the body when no
   * {@link #condition()} is present, so a null argument fails before the side
   * effect; a present condition gates key resolution and, in the after-the-body
   * form, is itself evaluated after the body. Once a condition defers resolution
   * past the body, a null key is a no-op removal rather than a pre-body failure.
   * Empty means the default key — see {@link ZetaCached#key()}.
   *
   * @return the key expression, or empty for the default key
   */
  String key() default "";

  /**
   * SpEL expression deciding whether this removal runs, evaluated in the same
   * phase as the removal it guards: before the body when
   * {@link #beforeInvocation()} is {@code true}, after it otherwise. A
   * {@code false} result skips the removal; the body still runs. With the
   * default after-the-body form, {@code #result} is available and refers to the
   * unwrapped returned value; a pre-invocation condition must not read it.
   *
   * <p>
   * A malformed expression fails startup; a constant expression is evaluated once at resolution
   * and must yield a {@code Boolean} or {@code null}. An expression that throws, returns
   * {@code null} or returns a non-Boolean at evaluation time is treated as {@code false} and
   * logged at DEBUG. A broken guard must not remove a key the caller meant to keep.
   *
   * @return the removal condition, or empty for "always remove"
   */
  String condition() default "";

  /**
   * Whether the removal runs before the body instead of after it. A
   * pre-invocation removal resolves its key and condition before the body runs,
   * so neither may depend on {@code #result}; declaring it is rejected at
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

  /**
   * Whether the key expression selects many keys. When {@code true}, the
   * expression must resolve to an array or an {@link Iterable}; every non-null
   * element is encoded as one default-key value and removed in one engine batch
   * call. An empty collection is a no-op; more than 1024 elements fails the
   * call instead of building an unbounded key list. A top-level {@code null} is
   * a configuration error when resolved before the body and a no-op when
   * resolved after it.
   *
   * <p>
   * Requires an explicit {@link #key()}; the default argument-list key is one
   * key, not an element list. Mutually exclusive with {@link #allEntries()}.
   *
   * @return {@code true} to invalidate one key per resolved element
   */
  boolean eachKey() default false;

  /**
   * Whether every entry of the namespace is removed instead of one key. The
   * removal is scoped to {@code cache}: it never flushes unrelated namespaces.
   * Mutually exclusive with {@link #key()} and {@link #eachKey()}.
   *
   * @return {@code true} to clear the namespace
   */
  boolean allEntries() default false;
}
