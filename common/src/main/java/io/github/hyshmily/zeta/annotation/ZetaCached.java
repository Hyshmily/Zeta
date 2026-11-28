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

import io.github.hyshmily.zeta.policy.HeatTrigger;
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
 * <li>{@code null} is never stored: a {@code null} result or an empty
 * {@code Optional} writes no entry and the next read re-invokes the body.</li>
 * <li>A body exception propagates to the caller unchanged and stores nothing.
 * Zeta does not convert a business failure into a value.</li>
 * <li>A background refresh cannot resume the interceptor chain
 * ({@code MethodInvocation.proceed()} is single-shot), so it invokes the
 * target method reflectively — normally <b>without</b> the transaction
 * interceptor and without method security. That is true even when the refresh
 * executor runs the task on the calling thread. When only a proxy can invoke
 * the annotated view (a repository or scoped-proxy fallback), the reflective
 * call necessarily proceeds through the remaining advice chain; this cache
 * advice is bypassed for that one call so the stale entry cannot be served,
 * but a transaction advice outside it may run. A method whose body depends on
 * request-scoped state must therefore not be given a soft TTL. A refresh
 * failure reaches the engine's failure path (WARN plus a closed retry window),
 * so a failing source is not re-run on every stale read.</li>
 * <li>Configuration is validated when the bean is proxied: a cache name that
 * is still blank after the {@link ZetaConfig} fallback, a conflicting verb, two
 * views declaring the same read/write verb with different attributes, a
 * {@code void}/{@code Void} return, an unparsable expression, an unknown SpEL
 * variable (when the method's parameter names are discoverable), an
 * out-of-range {@code #pN}/{@code #aN}, a digits-only TTL literal outside the
 * {@code long} range, a constant TTL expression that evaluates negative, or a pre-body
 * {@code #result} fails startup, not the first
 * call. Every candidate method of the bean is checked on the first pointcut
 * question, including through a JDK proxy.</li>
 * <li>{@code #pN}/{@code #aN} always address parameter {@code N}: the
 * positional aliases are written after the discovered names, so a parameter
 * that happens to be named {@code p1} cannot move {@code #p1} to another slot.
 * {@code #root}, {@code #this} and {@code #result} are bound ahead of the
 * parameter table, so a parameter with one of those names is legal, is omitted
 * from the table, and the reserved name never resolves to it.</li>
 * </ul>
 *
 * <p>
 * <b>One read or write verb per method.</b> A method may carry one of
 * {@code @ZetaCached} and {@link ZetaPut}; declaring both is rejected at method
 * resolution. {@link ZetaInvalidate} may be repeated, but may not be combined
 * with a read or write verb on the same method. A composed annotation that
 * meta-annotates one of the verbs is discovered with its {@code @AliasFor}
 * overrides.
 *
 * <p>
 * <b>{@code Optional} contracts.</b> A method returning {@code Optional<T>}
 * stores the unwrapped {@code T}, so a raw and an optional read of the same
 * domain value share one entry; a hit is rewrapped as {@code Optional.of(value)}.
 * {@code Optional.empty()} is a null result and is never stored.
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
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ZetaCached {
  /**
   * The cache namespace, or empty to inherit {@link ZetaConfig#cache()} from the bean's user class
   * (or the business interfaces of a JDK proxy), falling back to the method's declaring class.
   * Every physical L1 key of this method is {@code cache + '\u0000' + key}, so two namespaces
   * cannot collide on the same logical key. Must not contain {@code U+0000}. Supports
   * {@code ${...}} placeholders, resolved once when the method's plan is created.
   *
   * @return the namespace, or empty to inherit the class configuration
   */
  String cache() default "";

  /**
   * SpEL expression resolving the logical key, evaluated once per call with the
   * method arguments bound as {@code #p0}/{@code #a0} and by parameter name
   * ({@code #id}) — discovered from the method that declares the verb
   * annotation, falling back to the most-specific method when the declaring view
   * has no parameter metadata, so an interface method without metadata still
   * binds by name.
   * {@code #root.methodName}, {@code #root.method}, {@code #root.target},
   * {@code #root.targetClass}, {@code #root.args} and
   * {@code @beanName.method(...)}
   * are available. {@code #result} is not: the key is resolved before the body
   * runs.
   *
   * <p>
   * Empty means the default key: a canonical encoding of the arguments (see
   * {@link io.github.hyshmily.zeta.annotation.support.DefaultKey}) — text
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
   * array still hits the same entry. Only the whole {@code #root.args} reference
   * (parentheses allowed) uses the multi-argument default encoding; a larger
   * expression that returns the argument array is encoded as one value.
   *
   * <p>
   * A context-free expression is folded to a constant physical key at
   * resolution; a constant {@code null} key fails startup instead of failing
   * every call.
   *
   * @return the key expression, or empty for the default key
   */
  String key() default "";

  /**
   * SpEL expression deciding whether this call enters the cache at all,
   * evaluated before the key and before the body. A {@code false} result runs
   * the body directly: no L1 lookup, no loader and no key resolution. This is
   * the supported way to skip a cache for a nullable key argument —
   * {@code condition = "#id != null"} — without making the default key accept a
   * null argument.
   *
   * <p>
   * {@code #result} is not available: the expression runs before the body. A
   * malformed expression fails startup. At evaluation time a {@code null} result is treated as
   * {@code false}, any other result is coerced to a Boolean, and a throwing expression propagates
   * to the caller: a broken admission guard never silently admits a call the caller meant to bypass.
   *
   * @return the entry condition, or empty for "always enter the cache"
   */
  String condition() default "";

  /**
   * SpEL expression deciding whether this result is stored, evaluated at most
   * once per load with {@code #result} bound to the unwrapped domain value.
   * {@code true} serves the value but writes no entry — the {@code unless}
   * contract of Spring's {@code @Cacheable}, without requiring Spring Cache.
   *
   * <p>
   * A malformed expression fails startup. At evaluation time a {@code null} result does not veto,
   * any other result is coerced to a Boolean, and a throwing expression propagates to the caller
   * instead of silently storing a value the veto was meant to reject.
   *
   * @return the store veto expression, or empty for "always store"
   */
  String unless() default "";

  /**
   * Hard TTL override: the absolute lifetime of the entry this method creates.
   * Either a literal millisecond count ({@code "60000"}) or a SpEL expression
   * ({@code "#vip ? 600000 : 60000"}).
   *
   * <p>The declaration vocabulary is the same on every attribute: empty, {@code "0"} and
   * {@code "-1"} all mean the configured default (a hard TTL cannot be switched off), every
   * positive value is the lifetime, and {@link Long#MAX_VALUE} means the entry never expires.
   *
   * <p>
   * A literal — optionally signed — is folded to a constant at method
   * resolution and surrounding whitespace is ignored; any other negative literal
   * fails startup, and so does a digits-only literal outside the {@code long}
   * range. Supports {@code ${...}} placeholders. An expression is compiled once; a
   * context-free expression is folded at resolution, and a negative fold other than {@code -1}
   * fails startup because SpEL's integer arithmetic overflows at 32 bits. A dynamic expression is
   * evaluated lazily — only when the engine actually needs it (entry creation, renewal, or a
   * soft-expire refresh), never on a plain hit. A malformed expression or
   * an unknown SpEL variable fails startup; at evaluation time a negative result other than
   * {@code -1} fails instead of falling back to the default, and a {@code BigInteger} or
   * {@code BigDecimal} result that is not exactly representable as a {@code long} fails instead of
   * wrapping into a positive lifetime or the {@code -1} sentinel.
   * {@code #result} is not available here (the same reason as {@link #key()}).
   *
   * @return the hard TTL literal or expression
   */
  String hardTtl() default "";

  /**
   * Soft TTL override — how long the entry is served without revalidation before a background
   * refresh is armed. Same literal-or-SpEL rule as {@link #hardTtl()}, evaluated independently:
   * {@code "-1"} disables the window outright (no warming path resurrects it), {@code ""}
   * or {@code "0"} inherits {@code zeta.local.soft-ttl-ms}, and {@link Long#MAX_VALUE} means the
   * entry never goes stale.
   *
   * @return the soft TTL literal or expression
   */
  String softTtl() default "";

  /**
   * Which heat signal may prewarm a missing entry for this method's namespace.
   * {@link HeatTrigger.Option#INHERIT} takes the enclosing {@code @ZetaConfig} value and then the
   * global {@code zeta.local.warm.prewarm.trigger}.
   *
   * @return the prewarm trigger declaration
   */
  HeatTrigger.Option prewarm() default HeatTrigger.Option.INHERIT;

  /**
   * Which heat signal may renew a resident entry before its deadline.
   * {@link HeatTrigger.Option#INHERIT} takes the enclosing {@code @ZetaConfig} value and then the
   * global {@code zeta.local.warm.keep-warm.trigger}. The trigger is independent of the soft TTL:
   * a hard-only namespace can renew, and a namespace with a soft window still needs this
   * declaration (or the global default) to opt in.
   *
   * @return the keep-warm trigger declaration
   */
  HeatTrigger.Option keepWarm() default HeatTrigger.Option.INHERIT;

  /**
   * Authorize the framework to bind this method itself as its namespace's background loader.
   *
   * <p>It is sugar for a programmatic loader, not a second resolution model: the method is
   * validated at startup and then registered as the same background-loader declaration a
   * {@code ZetaWarmConfigurer} would produce. The validation rejects a non-reversible key, a
   * dynamic or conflicting TTL for prewarm, a {@code condition}, and an {@code unless} that reads
   * anything but {@code #result}.
   *
   * <p><b>The load runs outside the caller's advice chain.</b> It invokes the method reflectively
   * on the unwrapped target, so a transaction or method-security advice on the method is not part
   * of that call; when only the proxy exposes the annotated view (a repository or a scoped proxy),
   * the call re-enters the whole chain with no caller state. The plan is built while the bean is
   * still being proxied, so the advisors are not visible there: startup logs one WARN naming every
   * advisor that applies to the method and cannot run as it does for the caller. A method whose
   * body depends on that advice — a transaction, method security, request-scoped state — must bind
   * its loader through {@link io.github.hyshmily.zeta.policy.ZetaWarmConfigurer} instead.
   *
   * <p>A namespace may have at most one canonical warmed read method. If another
   * {@code @ZetaCached} method in the same namespace produces a different key shape, or another
   * declaration already registered a loader, startup fails rather than letting a broadcast warm
   * the wrong entry.
   *
   * @return whether this method is its namespace's background loader
   */
  boolean selfWarmLoader() default false;
}
