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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares a read-through cache on the annotated method — the declarative form
 * of
 * {@link io.github.hyshmily.zeta.Zeta#get(String, io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader,
 * io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy)}.
 *
 * <p>
 * <b>The method body is the loader.</b> A hit returns the cached value and
 * never runs the body; a miss runs the body once and stores the result in L1
 * under {@code cache + '\u0000' + key}. Because the body is handed to the
 * engine as a real loader, a soft-expired entry is served stale while one
 * background refresh runs behind the caller (design decisions: annotation
 * reads) and concurrent same-key misses merge through SingleFlight.
 *
 * <p>
 * <b>Contract.</b>
 * <ul>
 * <li>{@code null} is never stored: a {@code null} result writes no entry and
 * the next read re-invokes the body.</li>
 * <li>A body exception propagates to the caller unchanged and stores nothing.
 * Zeta does not convert a business failure into a value.</li>
 * <li>A background refresh cannot resume the interceptor chain
 * ({@code MethodInvocation.proceed()} is single-shot), so it invokes the
 * target method reflectively — <b>without</b> the transaction interceptor
 * and without method security. That is true even when the refresh executor
 * runs the task on the calling thread. A method whose body depends on
 * request-scoped state must therefore not be given a soft TTL. A refresh
 * failure reaches the engine's failure path (WARN plus lease-on-failure),
 * so a failing source keeps serving the stale value instead of re-running
 * the body on every stale read.</li>
 * <li>Configuration is validated when the bean is proxied: a blank
 * {@link #cache()}, a conflicting verb, two views declaring the same verb
 * with different attributes, a {@code void}/{@code Void} return, an
 * unparsable expression, an unknown SpEL variable (when the method's
 * parameter names are discoverable), an out-of-range {@code #pN}/{@code #aN},
 * a digits-only TTL literal outside the {@code long} range or a pre-body
 * {@code #result} fails startup, not the first call. Every candidate method of
 * the bean is checked on the first pointcut question, including through a JDK
 * proxy.</li>
 * <li>{@code root}, {@code this}, {@code result} and canonical
 * {@code pN}/{@code aN} are reserved SpEL bindings. A method parameter
 * with one of those names is rejected when an expression on the method is
 * compiled, so a parameter can never shadow a binding.</li>
 * </ul>
 *
 * <p>
 * <b>One verb per method.</b> A method may carry exactly one of
 * {@code @ZetaCached}, {@link ZetaPut} and {@link ZetaInvalidate}; a second one
 * is rejected at method resolution instead of being silently ignored.
 *
 * <pre>{@code
 * @ZetaCached(cache = "users", key = "#id")
 * User findUser(String id) {
 *   return repository.load(id);
 * }
 *
 * @ZetaCached(cache = "quotes", key = "#symbol", softTtl = "1000", hardTtl = "5000")
 * Quote quote(String symbol) {
 *   return feed.snapshot(symbol);
 * }
 * }</pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ZetaCached {
  /**
   * The cache namespace, required. Every physical L1 key of this method is
   * {@code cache + '\u0000' + key}, so two namespaces cannot collide on the
   * same logical key. Must not contain {@code U+0000}. Supports {@code ${...}}
   * placeholders, resolved once when the method's plan is created.
   *
   * @return the namespace
   */
  String cache();

  /**
   * SpEL expression resolving the logical key, evaluated once per call with the
   * method arguments bound as {@code #p0}/{@code #a0} and by parameter name
   * ({@code #id}) — discovered from the most-specific implementation method, so
   * an interface method without parameter metadata still binds by name.
   * {@code #root.methodName}, {@code #root.method}, {@code #root.target},
   * {@code #root.targetClass}, {@code #root.args} and
   * {@code @beanName.method(...)}
   * are available. {@code #result} is not: the key is resolved before the body
   * runs.
   *
   * <p>
   * Empty means the default key: a canonical encoding of the arguments (see
   * {@link io.github.hyshmily.zeta.annotation.supporter.DefaultKey}) — text
   * values length-prefixed, numeric/Boolean and primitive-array elements
   * tagged, not the bare {@code toString()} of a list. A {@code null} argument
   * or a resolved {@code null} key is a configuration bug and fails the call
   * rather than addressing the default key. Two values of different
   * reference-array component types whose element encodings coincide, or
   * non-array objects with the same {@code toString()}, can still share an
   * encoding; declare an explicit key when that matters.
   *
   * <p>
   * An explicit expression that resolves to an array — {@code #p0} on a
   * {@code byte[]}/{@code String[]}, {@code #root.args}, a varargs tail, or an
   * array {@code #result} on a write — is encoded by content with the same
   * canonical encoder. Its identity hash is never used, so a freshly built
   * array still hits the same entry.
   *
   * @return the key expression, or empty for the default key
   */
  String key() default "";

  /**
   * SpEL expression deciding whether this result is stored, evaluated at most
   * once per load with {@code #result} bound. {@code true} serves the value but
   * writes no entry — the {@code unless} contract of Spring's
   * {@code @Cacheable}, without requiring Spring Cache.
   *
   * <p>
   * An expression that throws at evaluation time fails open (the value is
   * stored) and logs at DEBUG, because a broken veto must not turn every read into a
   * miss and must not become a per-call WARN storm; a malformed expression fails
   * startup instead.
   *
   * @return the store veto expression, or empty for "always store"
   */
  String unless() default "";

  /**
   * Hard TTL override: the absolute lifetime of the entry this method creates.
   * Either a literal millisecond count ({@code "60000"}) or a SpEL expression
   * ({@code "#vip ? 600000 : 60000"}). Empty or {@code "0"} means the
   * configured default.
   *
   * <p>
   * A literal — optionally signed — is folded to a constant at method
   * resolution; surrounding whitespace is ignored, a negative literal clamps to
   * {@code 0} (the configured default), and a digits-only literal outside the
   * {@code long} range fails startup. Supports {@code ${...}} placeholders. An
   * expression is compiled once and evaluated lazily — only when the engine
   * actually needs it (entry creation, promotion to HOT, renewal, or a
   * soft-expire refresh), never on a plain hit. A malformed expression or an
   * unknown SpEL variable fails startup; at evaluation time a negative result
   * is clamped to {@code 0} (use the default), a huge {@code BigInteger} or
   * {@code BigDecimal} is clamped to {@link Long#MAX_VALUE} instead of wrapping
   * negative, and a failure degrades to the default. {@code #result} is not
   * available here (the same reason as {@link #key()}).
   *
   * @return the hard TTL literal or expression
   */
  String hardTtl() default "";

  /**
   * Soft TTL override — how long the entry is served without revalidation
   * before a background refresh is armed. Same literal-or-SpEL rule as
   * {@link #hardTtl()}, evaluated independently. Empty or {@code "0"} means the
   * configured default.
   *
   * @return the soft TTL literal or expression
   */
  String softTtl() default "";
}
