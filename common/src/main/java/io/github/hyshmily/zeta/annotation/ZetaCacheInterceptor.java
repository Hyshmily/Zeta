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

import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.annotation.supporter.CompiledExpression;
import io.github.hyshmily.zeta.annotation.supporter.DefaultKey;
import io.github.hyshmily.zeta.annotation.supporter.ZetaEvaluationContext;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.InvalidatePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.PutPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.StalePolicy;
import io.github.hyshmily.zeta.util.Internal;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.function.LongSupplier;
import lombok.extern.slf4j.Slf4j;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.jspecify.annotations.Nullable;
import org.springframework.aop.TargetClassAware;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.expression.spel.ast.Literal;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * The one advice behind {@link ZetaCached}, {@link ZetaPut} and
 * {@link ZetaInvalidate}: {@link PlanResolver} resolves the method's plan once,
 * the advice resolves the key once, and then performs exactly one data verb
 * against the {@link Zeta} facade.
 *
 * <p><b>No cache abstraction in the middle.</b> The annotations are not Spring
 * Cache operations, so there is no {@code CacheManager}, no {@code Cache}, no
 * {@code CacheOperationSource} and no second interceptor to keep inert. The
 * only Spring contract this class honours is the method-interceptor one, and
 * the only Zeta contract is the facade's — which is why a read can hand the
 * method body to {@link Zeta#get} as a real loader: soft-expire
 * (stale-while-revalidate) and SingleFlight dedup come for free, and a new
 * attribute can never mean two different things in two places.
 *
 * <p><b>One method, one verb.</b> A method carrying two of the three
 * annotations is a configuration error and is rejected at resolution instead of
 * one annotation silently shadowing the other.
 *
 * <p><b>Resolution is the fail-fast boundary, and {@link PlanResolver} owns it.</b>
 * {@link ZetaCacheAdvisor}'s pointcut calls {@link #matches} while Spring is deciding
 * whether to proxy a bean, so a blank namespace, a void/Void return, a conflicting verb,
 * an unparsable expression, an unknown or out-of-range variable, a pre-body
 * {@code #result} or a permanent synced write fails bean creation instead of the first
 * request. The resolver scans every candidate method of the target class once: a JDK proxy
 * matches lazily per call, so without that scan a malformed second verb would surface only
 * in production. Its table is keyed by the target class, with a CGLIB proxy sharing the
 * user class's table, so a validated class answers every later question about one of its
 * views with one lookup and no annotation scan.
 *
 * <p><b>One resolution, every view.</b> The most-specific implementation method
 * is resolved once per candidate, and it is stored in the plan: SpEL parameter
 * names are discovered from it (an interface method reported by a JDK proxy has
 * no parameter metadata of its own, so the declaring method is the fallback), a
 * background refresh invokes it, and its reflective access is claimed once
 * instead of on every refresh. Every view of an annotated group maps to the
 * same plan instance, so an interface verb is never compiled twice.
 *
 * <p><b>Key.</b> Resolved once per call and reused by every consumer:
 * {@code cache + '\u0000' + key}, the namespace encoding that keeps two cache
 * names from colliding in the single L1 table. An absent {@code key} attribute
 * derives the canonical {@link DefaultKey} encoding of the arguments. A simple
 * single-argument expression ({@code #id}, {@code #p0}, {@code #a0}) resolves
 * straight from the argument array; a no-argument default key and a
 * whole-literal expression are precomputed at resolution. Anything else goes
 * through SpEL, whose parser runs in MIXED mode so a repeatedly evaluated
 * expression is compiled after its first few interpreted evaluations.
 *
 * <p><b>Body exceptions.</b> The read path returns a {@link BodyFailure}
 * sentinel from the loader instead of throwing, because a business exception
 * must reach the caller unchanged and must not be counted as a data-source
 * failure (that would drive the breaker open). The sentinel is never stored and
 * is rethrown by the caller. On the background-refresh leg the failure is
 * thrown to the engine instead: there is no caller to rethrow to, and the
 * engine's WARN plus lease-on-failure path must observe a real failure.
 *
 * <p><b>Thread contract.</b> {@code MethodInvocation.proceed()} is single-shot
 * and not thread-safe, so it serves only the first load, on the invocation
 * thread — the body therefore keeps its transaction, security context and MDC.
 * The loader owns that one-shot chain and claims it at most once; the
 * invocation chain is released as soon as the foreground read returns, because
 * a refresh task may outlive the caller and must not pin the proxy chain,
 * target and original advice state. The refresh goes through
 * {@link CacheLoader#reload}, which invokes the target method reflectively,
 * <b>outside</b> the advice chain; that refreshed value is only ever written to
 * the local L1. A method whose body depends on request-scoped state must not be
 * given a soft TTL.
 *
 * <p><b>One context per phase.</b> A context is built from the call's immutable
 * inputs when a phase needs one: the result-independent key before the body,
 * and one shared post-body context for {@code unless}, a result-dependent key
 * and the TTLs evaluated when the write happens. A read's TTL suppliers still
 * build their own context when the engine evaluates them, so an evaluation on
 * a refresh thread, after the caller returned, or inline on the caller's
 * thread cannot observe another phase's state.
 */
@Slf4j
@Internal
public final class ZetaCacheInterceptor implements MethodInterceptor {

  private static final BigDecimal MAX_LONG_DECIMAL = BigDecimal.valueOf(Long.MAX_VALUE);

  private static final Object[] NO_ARGS = new Object[0];

  private static final LongSupplier ZERO = () -> 0L;

  private final Zeta zeta;

  private final PlanResolver resolver;

  /**
   * Create the interceptor.
   *
   * @param zeta        the facade every data verb goes through
   * @param beanFactory the bean factory backing {@code @beanName} SpEL and
   *                    {@code ${...}} attribute placeholders, or {@code null}
   *                    to disable both
   */
  public ZetaCacheInterceptor(Zeta zeta, @Nullable BeanFactory beanFactory) {
    this.zeta = zeta;
    this.resolver = new PlanResolver(beanFactory);
  }

  /** The pointcut decision, delegated to the resolved plan table. */
  boolean matches(Method method, Class<?> targetClass) {
    return resolver.matches(method, targetClass);
  }

  @Override
  public Object invoke(MethodInvocation invocation) throws Throwable {
    Method method = invocation.getMethod();
    if (method.getDeclaringClass() == Object.class) {
      return invocation.proceed();
    }

    // Spring's MethodInvocation.getThis() is the TARGET object, not the proxy
    // (the proxy is ProxyMethodInvocation.getProxy()). A nested proxy — a
    // FactoryBean returning a proxy, a scoped target — is the only case that
    // needs unwrapping. Unwrap once here: every SpEL context and a background
    // refresh reuse the same receiver instead of walking the proxy chain again.
    // The unwrap stops at the first object that still implements the reported
    // method's declaring type: an interface proxy (a Spring Data repository, a
    // mapper) has a target implementation that does not implement the
    // interface, and reflectively invoking that target would fail.
    Object target = findUltimateTargetObject(invocation.getThis(), method);
    Class<?> targetClass = planClass(target, method);

    Plan plan = resolver.plan(method, targetClass);
    if (plan == null) {
      return invocation.proceed();
    }
    Object[] args = invocation.getArguments();
    if (plan instanceof Plan.Read read) {
      return read(invocation, method, read, target, targetClass, args);
    }
    if (plan instanceof Plan.Write write) {
      return write(invocation, write, target, targetClass, args);
    }
    if (plan instanceof Plan.Invalidate invalidation) {
      return invalidate(invocation, invalidation, target, targetClass, args);
    }
    throw new IllegalStateException("Unknown plan type: " + plan.getClass());
  }

  /**
   * Unwrap nested proxies down to the object the method body should run on.
   * {@code invocation.getThis()} is already the target object, so this is a
   * no-op for every ordinary proxy; only a target that is itself an
   * {@link Advised} proxy needs the singleton-target unwrap, and a
   * non-singleton {@code TargetSource} keeps the proxy it cannot unwrap. An
   * interface proxy stops at the proxy itself when the next target no longer
   * implements the reported method's declaring type: that object is the only
   * one both the plan lookup and a reflective reload can use.
   *
   * @param candidate the invocation target
   * @param method    the reported method the receiver must be able to invoke
   * @return the deepest target that still implements the method's declaring type
   */
  private static @Nullable Object findUltimateTargetObject(@Nullable Object candidate, Method method) {
    Object current = candidate;
    while (current instanceof Advised advised) {
      Object next = AopProxyUtils.getSingletonTarget(advised);
      if (next == null || next == current || !method.getDeclaringClass().isInstance(next)) {
        return current;
      }
      current = next;
    }
    return current;
  }

  /**
   * The class the plan table is keyed on: the target's user class when it still
   * implements the reported method, otherwise the method's declaring type. A
   * Spring Data repository proxy's ultimate target is {@code SimpleJpaRepository},
   * which does not implement the repository interface; keying on it would miss
   * every plan and silently bypass the cache.
   */
  private static Class<?> planClass(@Nullable Object target, Method method) {
    if (target == null) {
      return method.getDeclaringClass();
    }
    Class<?> ultimate =
      target instanceof TargetClassAware ? AopProxyUtils.ultimateTargetClass(target) : target.getClass();
    return method.getDeclaringClass().isAssignableFrom(ultimate) ? ultimate : method.getDeclaringClass();
  }

  /**
   * The read: the body is the loader, so the engine owns hit/miss/soft-expire,
   * the pre-read stamp and the dedup of concurrent same-key loads. The loader's
   * {@code load} may resume the advice chain once; its {@code reload} never
   * does.
   */
  private Object read(
    MethodInvocation invocation,
    Method method,
    Plan.Read plan,
    @Nullable Object target,
    Class<?> targetClass,
    Object[] args
  ) throws Throwable {
    // Resolve the key before touching the engine or allocating the loader, so a
    // null argument or a null key fails without a doomed load object.
    String key = plan.key().resolve(args, preContextIfNeeded(plan, target, targetClass, args));
    ReadLoader loader = new ReadLoader(invocation, method, plan, target, targetClass, args);
    try {
      Object value = zeta.get(key, loader, loader.readPolicy());
      if (value instanceof BodyFailure failure) {
        throw failure.cause();
      }
      return value;
    } finally {
      // A refresh task can retain this loader after the caller returns; the advice chain is
      // useless to it (reload never proceeds) and is released here. The loader keeps the target
      // and a defensive copy of the arguments for the reflective reload only. A method whose
      // body depends on request-scoped or caller-mutated argument state must not use a soft TTL:
      // the refresh cannot snapshot arbitrary mutable objects, only the argument array and its
      // array elements.
      loader.release();
    }
  }

  /**
   * The write: the result-independent key resolves before the body, so a null
   * argument or a null key fails before the transaction commits. A synced
   * dynamic hard TTL is the only TTL evaluated before the body: a permanent
   * value cannot be broadcast, and that rejection must precede the business
   * side effect. Every other TTL waits until the body returned a non-null,
   * non-vetoed value, and all post-body expressions share one context bound to
   * that value.
   */
  private Object write(
    MethodInvocation invocation,
    Plan.Write plan,
    @Nullable Object target,
    Class<?> targetClass,
    Object[] args
  ) throws Throwable {
    ZetaEvaluationContext preContext = plan.needsPreContext() ? newContext(plan, target, targetClass, args) : null;
    KeyPlan keyPlan = plan.key();
    String key = keyPlan.resultDependent() ? null : keyPlan.resolve(args, preContext);
    long preflightHardMs = preflightHardTtl(plan, preContext);

    Object value = invocation.proceed();
    if (value == null) {
      return null;
    }

    // The one post-body context: unless, a result-dependent key and every TTL
    // evaluated at write time share it, so they all see the same #result.
    ZetaEvaluationContext postContext = null;
    if (!plan.unless().absent()) {
      postContext = newPostContext(plan, target, targetClass, args, value);
      if (vetoed(plan.unless(), postContext)) {
        return value;
      }
    }
    if (postContext == null && plan.needsPostWriteContext()) {
      postContext = newPostContext(plan, target, targetClass, args, value);
    }
    if (key == null) {
      key = keyPlan.resolve(args, postContext);
    }
    zeta.put(key, value, writePolicy(plan, preflightHardMs, postContext));
    return value;
  }

  /** The synced dynamic hard TTL the pre-body phase validates, or {@code 0} when there is none. */
  private long preflightHardTtl(Plan.Write plan, @Nullable ZetaEvaluationContext context) {
    if (!plan.preflightHard()) {
      return 0L;
    }
    long hardMs = evaluateTtl(plan.ttls().hard(), context, plan.specific());
    PutPolicy.requireSyncable(hardMs, true);
    return hardMs;
  }

  /** The write policy: the prebuilt one, else the TTLs evaluated against the post-body context. */
  private PutPolicy writePolicy(Plan.Write plan, long preflightHardMs, @Nullable ZetaEvaluationContext context) {
    PutPolicy prebuilt = plan.prebuiltPolicy();
    if (prebuilt != null) {
      return prebuilt;
    }
    long hardMs = plan.preflightHard() ? preflightHardMs : evaluateTtl(plan.ttls().hard(), context, plan.specific());
    long softMs = evaluateTtl(plan.ttls().soft(), context, plan.specific());
    return PutPolicy.of(hardMs, softMs, plan.sync());
  }

  /** The invalidation: after the body by default, before it on request. */
  private Object invalidate(
    MethodInvocation invocation,
    Plan.Invalidate plan,
    @Nullable Object target,
    Class<?> targetClass,
    Object[] args
  ) throws Throwable {
    // `immediate` and `beforeInvocation` are the same fact: a removal that must
    // run before the body cannot wait for the transaction's after-commit hook.
    InvalidatePolicy policy = plan.policy();
    KeyPlan keyPlan = plan.key();
    // A before-invocation key cannot read #result (rejected at resolution), so
    // the immediate form always has a resolved key here; the after-the-body
    // form may defer a #result key until the body returned.
    String key = keyPlan.resultDependent()
      ? null
      : keyPlan.resolve(args, preContextIfNeeded(plan, target, targetClass, args));
    if (policy.immediate()) {
      zeta.invalidate(key, policy);
      return invocation.proceed();
    }

    Object value = invocation.proceed();
    if (key == null) {
      // A key that reads #result is resolved against the returned value; a null
      // result is a null dereference in the expression, so an expression that
      // accepts one writes #result?.id (null → no removal). A key that simply
      // resolves to null is a no-op removal.
      key = keyPlan.resolveOrNull(args, newPostContext(plan, target, targetClass, args, value));
    }
    if (key != null) {
      zeta.invalidate(key, policy);
    }
    return value;
  }

  /**
   * Logs one annotation-expression evaluation failure at DEBUG. A broken
   * expression sits on a hot path and must not become a per-call log storm.
   */
  private void logEvaluationFailure(String role, Method method, RuntimeException failure) {
    log.debug("[Zeta] {} evaluation failed for {}: {}", role, method, failure.toString());
  }

  /**
   * Evaluate one TTL against the context of the current phase. A constant
   * returns its folded value without a context; a negative or absent result
   * clamps to {@code 0} (use the configured default), and an evaluation failure
   * is logged at DEBUG and degrades the same way, because a runtime failure
   * must not fail the cache operation.
   */
  private long evaluateTtl(Ttl ttl, @Nullable ZetaEvaluationContext context, Method method) {
    if (ttl.isConstant()) {
      return ttl.literalMs();
    }
    try {
      Number value = ttl.expression().getValue(context, Number.class);
      return value == null ? 0L : clampTtl(value);
    } catch (RuntimeException e) {
      logEvaluationFailure(ttl.role(), method, e);
      return 0L;
    }
  }

  /**
   * Clamp a TTL number to {@code [0, Long.MAX_VALUE]}. {@link BigInteger} and
   * {@link BigDecimal} do not saturate in {@code longValue()}: they keep the low
   * 64 bits, so a huge positive value can become negative and silently degrade
   * to the default TTL. Compare first and use the permanent sentinel as the
   * upper bound.
   */
  private static long clampTtl(Number value) {
    if (value instanceof BigInteger integer) {
      if (integer.signum() <= 0) {
        return 0L;
      }
      return integer.bitLength() > 63 ? Long.MAX_VALUE : integer.longValue();
    }
    if (value instanceof BigDecimal decimal) {
      if (decimal.signum() <= 0) {
        return 0L;
      }
      return decimal.compareTo(MAX_LONG_DECIMAL) > 0 ? Long.MAX_VALUE : decimal.longValue();
    }
    long millis = value.longValue();
    return Math.max(millis, 0L);
  }

  /** The one read-policy shape every annotation read uses. */
  static GetPolicy readPolicy(LongSupplier hardTtlMs, LongSupplier softTtlMs) {
    return new GetPolicy(hardTtlMs, softTtlMs, StalePolicy.SOFT_REFRESH, true, true);
  }

  /**
   * @param unless  the expression to evaluate
   * @param context the context bound to the post-body value
   * @return whether the present {@code unless} expression rejects the bound
   *         result (fail-open)
   */
  private boolean vetoed(CompiledExpression unless, ZetaEvaluationContext context) {
    try {
      return Boolean.TRUE.equals(unless.getValue(context, Boolean.class));
    } catch (RuntimeException e) {
      logEvaluationFailure("unless", context.root().getMethod(), e);
      return false;
    }
  }

  /**
   * Build a fresh SpEL context for one evaluation phase. The root wraps the
   * immutable invocation inputs; the context is confined to the thread that
   * evaluates the expression.
   */
  private ZetaEvaluationContext newContext(Plan plan, @Nullable Object target, Class<?> targetClass, Object[] args) {
    ZetaEvaluationContext.Root root = ZetaEvaluationContext.Root.of(target, targetClass, plan.specific(), args);
    return ZetaEvaluationContext.of(root, plan.parameterNames(), resolver.beanResolver(), resolver.expressionSupport());
  }

  /** A fresh context bound to the body's result for the post-body evaluation phase. */
  private ZetaEvaluationContext newPostContext(
    Plan plan,
    @Nullable Object target,
    Class<?> targetClass,
    Object[] args,
    @Nullable Object result
  ) {
    return newContext(plan, target, targetClass, args).withResult(result);
  }

  /** The pre-body context when the key needs one, otherwise {@code null}. */
  private @Nullable ZetaEvaluationContext preContextIfNeeded(
    Plan plan,
    @Nullable Object target,
    Class<?> targetClass,
    Object[] args
  ) {
    return plan.key().needsContext() ? newContext(plan, target, targetClass, args) : null;
  }

  /**
   * What one annotated method does, resolved once per (method, user class).
   * A sealed hierarchy rather than one record with unused fields: an attribute
   * that cannot apply to a verb is not representable.
   */
  sealed interface Plan {
    /** The most-specific implementation method: SpEL parameter discovery, reflective refresh. */
    Method specific();

    /** The method's parameter names, discovered once at resolution ({@code null} when unavailable). */
    String@Nullable [] parameterNames();

    /** The key plan; absent expression means the default key. */
    @SuppressWarnings("all")
    KeyPlan key();

    /** {@code @ZetaCached}. */
    record Read(
      Method specific,
      String@Nullable [] parameterNames,
      KeyPlan key,
      CompiledExpression unless,
      Ttls ttls,
      @Nullable GetPolicy prebuiltPolicy
    ) implements Plan {}

    /** {@code @ZetaPut}. */
    record Write(
      Method specific,
      String@Nullable [] parameterNames,
      KeyPlan key,
      CompiledExpression unless,
      Ttls ttls,
      boolean sync,
      @Nullable PutPolicy prebuiltPolicy
    ) implements Plan {
      /** Whether the hard TTL is dynamic, result-independent and validated before the body. */
      boolean preflightHard() {
        return sync && !ttls.hard().isConstant() && !ttls.hard().resultDependent();
      }

      /** Whether the pre-body phase needs an evaluation context. */
      boolean needsPreContext() {
        return (!key.resultDependent() && key.needsContext()) || preflightHard();
      }

      /** Whether the post-veto write phase needs an evaluation context. */
      boolean needsPostWriteContext() {
        return key.resultDependent() || (!ttls.hard().isConstant() && !preflightHard()) || !ttls.soft().isConstant();
      }
    }

    /** {@code @ZetaInvalidate}. */
    record Invalidate(
      Method specific,
      String@Nullable [] parameterNames,
      KeyPlan key,
      InvalidatePolicy policy
    ) implements Plan {}
  }

  /**
   * A key expression plus the zero-SpEL fast path. {@code argumentIndex >= 0}
   * means the expression is exactly one argument reference; {@code rootArgs}
   * means it is exactly {@code #root.args}; {@code expression} is {@code null}
   * for those and the default-key/constant forms, so their parsed AST is not
   * retained. Otherwise the compiled expression is evaluated. {@code constantKey}
   * is the full physical key when the expression cannot depend on the invocation.
   *
   * <p>Every value an explicit expression selects is encoded as one default-key
   * argument, so {@code key = "#id"} and an absent {@code key} address the same
   * entry for a single-id method. A whole {@code #root.args} selects the argument
   * list itself and uses the multi-argument default encoding instead.
   */
  record KeyPlan(
    String namespace,
    Method method,
    int argumentIndex,
    boolean rootArgs,
    boolean resultDependent,
    @Nullable String constantKey,
    @Nullable CompiledExpression expression,
    @Nullable String rawExpression
  ) {
    static KeyPlan of(
      String namespace,
      Method method,
      CompiledExpression expression,
      String@Nullable [] parameterNames
    ) {
      String constantKey = constantKeyOf(namespace, method, expression);
      int index = constantKey == null ? argumentIndex(expression, parameterNames) : -1;
      // A varargs tail is one array parameter (or several expanded arguments).
      // Only the SpEL lookup path preserves both shapes; the single-argument fast
      // path would read just the first expanded element.
      if (index >= 0 && method.isVarArgs() && index == method.getParameterCount() - 1) {
        index = -1;
      }
      boolean rootArgs = constantKey == null && index < 0 && expression.wholeRootArgs();
      CompiledExpression retained =
        constantKey == null && index < 0 && !rootArgs && !expression.absent() ? expression : null;
      return new KeyPlan(
        namespace,
        method,
        index,
        rootArgs,
        expression.references("result"),
        constantKey,
        retained,
        expression.raw()
      );
    }

    /**
     * Precompute the full physical key when it cannot depend on the invocation:
     * the no-argument default key ({@code namespace + "[]"}) and an expression
     * whose whole AST is a non-null literal. A literal key is what it says, so
     * evaluating it through SpEL on every call buys nothing.
     */
    private static @Nullable String constantKeyOf(String namespace, Method method, CompiledExpression compiled) {
      if (compiled.absent()) {
        return method.getParameterCount() == 0 ? namespace + DefaultKey.encode(NO_ARGS) : null;
      }
      if (compiled.expression() instanceof SpelExpression spel && spel.getAST() instanceof Literal literal) {
        Object value = literal.getLiteralValue().getValue();
        if (value != null) {
          return DefaultKey.encodeSingle(namespace, value);
        }
      }
      return null;
    }

    /**
     * The physical key.
     *
     * @param args    the invocation arguments
     * @param context the context, or {@code null} when the key needs none
     * @return the physical key
     * @throws IllegalStateException when the key expression resolves to
     *                               {@code null}
     */
    String resolve(Object[] args, @Nullable ZetaEvaluationContext context) {
      String resolved = resolveOrNull(args, context);
      if (resolved == null) {
        throw new IllegalStateException("cache key '" + rawExpression + "' resolved to null for " + method);
      }
      return resolved;
    }

    /**
     * The physical key, or {@code null} when a present expression resolved to
     * {@code null}. {@code context} must be non-null whenever
     * {@link #needsContext()} is true.
     */
    @Nullable
    String resolveOrNull(Object[] args, @Nullable ZetaEvaluationContext context) {
      if (constantKey != null) {
        return constantKey;
      }
      if (argumentIndex >= 0) {
        Object argument = args[argumentIndex];
        return argument == null ? null : encodeValue(namespace, argument);
      }
      if (rootArgs || expression == null) {
        return defaultKey(args);
      }
      Object value = expression.getValue(context);
      return value == null ? null : encodeValue(namespace, value);
    }

    /**
     * The physical key for one explicitly selected value, encoded as one
     * argument of the default key. That is what makes {@code key = "#id"} and an
     * absent key address the same entry for a single-argument method, and it
     * keeps arrays encoded by content instead of the identity hash of
     * {@code Object.toString()}.
     */
    private static String encodeValue(String namespace, Object value) {
      return DefaultKey.encodeSingle(namespace, value);
    }

    /**
     * Whether resolution needs a SpEL context: a retained expression that is
     * not already folded to a constant key or a single argument reference.
     */
    boolean needsContext() {
      return expression != null;
    }

    /**
     * The canonical default key under this plan's namespace; a null top-level
     * argument is a configuration error, not a key.
     */
    private String defaultKey(Object[] args) {
      for (int i = 0; i < args.length; i++) {
        if (args[i] == null) {
          throw new IllegalStateException(
            "null argument " + i + " for " + method + ": pass a non-null argument or declare key = \"...\""
          );
        }
      }
      return DefaultKey.encode(namespace, args);
    }

    /**
     * Recognize a key that reads exactly one argument — {@code #p0}/{@code #a0}
     * or a bare parameter name — so the common single-id key resolves straight
     * from the argument array instead of through SpEL. Anything else returns
     * {@code -1} and keeps the full expression path.
     *
     * <p>The expression must be one variable reference, not a text form: a
     * literal {@code '#p1'} or a longer expression is never mistaken for a
     * reference. A bare parameter name is looked up in the discovered parameter
     * list; a canonical {@code #pN}/{@code #aN} is an index. {@code #root},
     * {@code #this} and {@code #result} are not parameter names, so they keep
     * the expression path. A non-canonical name such as {@code #p01},
     * {@code #accountId} or {@code #page} is only ever a variable name — never an
     * index reinterpretation. The range is validated by
     * {@link CompiledExpression#of}; this method only classifies the shape.
     */
    private static int argumentIndex(CompiledExpression expression, String@Nullable [] parameterNames) {
      String name = expression.singleVariable();
      return name == null ? -1 : CompiledExpression.argumentIndex(name, parameterNames);
    }
  }

  /** Both TTL attributes of one verb, classified at resolution. */
  record Ttls(Ttl hard, Ttl soft) {
    boolean bothConstant() {
      return hard.isConstant() && soft.isConstant();
    }
  }

  /**
   * One TTL attribute classified at resolution. An integral literal — optionally
   * signed, after stripping surrounding whitespace — and a context-free SpEL
   * expression such as {@code 60000L} or {@code 60 * 1000} are folded to a
   * constant (negative values clamp to {@code 0}, the configured default) and to
   * one prebuilt {@link LongSupplier}, so a static TTL costs no lambda and no
   * SpEL per call. A floating-point literal is rejected: milliseconds are
   * discrete, and accepting {@code 1e3} or {@code NaN} only buys precision
   * hazards. A literal outside the {@code long} range fails resolution with a
   * range error. Anything else is SpEL compiled once and evaluated lazily;
   * {@code resultDependent} marks an expression that must wait for the body. A
   * parse failure fails resolution; only a runtime evaluation failure degrades
   * to the configured default, so a broken attribute can never fail a read.
   */
  record Ttl(
    String role,
    long literalMs,
    @Nullable LongSupplier constantSupplier,
    CompiledExpression expression,
    boolean resultDependent
  ) {
    static Ttl of(
      String role,
      @Nullable String raw,
      SpelExpressionParser parser,
      Method method,
      String@Nullable [] parameterNames,
      boolean resultAllowed
    ) {
      String literal = raw == null ? null : raw.strip();
      if (literal == null || literal.isEmpty()) {
        return constant(role, 0L);
      }
      if (isInteger(literal)) {
        try {
          return constant(role, Math.max(0L, Long.parseLong(literal)));
        } catch (NumberFormatException overflow) {
          throw new IllegalStateException(
            role + " literal '" + literal + "' on " + method + " exceeds the long range",
            overflow
          );
        }
      }
      CompiledExpression compiled = CompiledExpression.of(literal, parser, role, method, parameterNames, resultAllowed);
      if (compiled.constant()) {
        Object value = compiled.getValue(new StandardEvaluationContext());
        if (!(value instanceof Number number) || number instanceof Double || number instanceof Float) {
          throw new IllegalStateException(role + " '" + literal + "' on " + method + " must be an integral number");
        }
        return constant(role, clampTtl(number));
      }
      return new Ttl(role, 0L, null, compiled, resultAllowed && compiled.references("result"));
    }

    private static Ttl constant(String role, long ms) {
      return new Ttl(role, ms, ms == 0L ? ZERO : () -> ms, CompiledExpression.ABSENT, false);
    }

    boolean isConstant() {
      return constantSupplier != null;
    }

    /**
     * Whether the hard TTL is the constant {@link Long#MAX_VALUE} (permanent),
     * which {@code sync = true} cannot broadcast safely.
     */
    boolean isPermanent() {
      return isConstant() && literalMs == Long.MAX_VALUE;
    }

    private static boolean isInteger(String raw) {
      int start = raw.charAt(0) == '+' || raw.charAt(0) == '-' ? 1 : 0;
      if (start == raw.length()) {
        return false;
      }
      for (int i = start; i < raw.length(); i++) {
        char c = raw.charAt(i);
        if (c < '0' || c > '9') {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * The engine loader for one annotated read: immutable invocation inputs plus
   * the one-shot advice chain, which only the foreground {@link #load} may
   * claim. A refresh never touches the chain, and a retained loader therefore
   * pins neither the proxy nor any evaluation state.
   *
   * <p>The loader is also the call's only wrapper object: a write or an
   * invalidation allocates none, and the evaluation contexts are local to the
   * phase that builds them, never retained past the foreground call.
   */
  private final class ReadLoader implements CacheLoader<String, Object> {

    private final Plan.Read plan;

    /** The method the invocation reported; the reflective fallback when the target does not implement the specific one. */
    private final Method method;

    private final @Nullable Object target;

    private final Class<?> targetClass;

    private final Object[] args;

    /**
     * The one-shot advice chain, claimed and cleared on the invocation thread.
     * A read sets {@code callerRunsLoader}, so the engine's foreground load is
     * this thread; the refresh path reads only the immutable fields.
     */
    private MethodInvocation chain;

    private ReadLoader(
      MethodInvocation chain,
      Method method,
      Plan.Read plan,
      @Nullable Object target,
      Class<?> targetClass,
      Object[] args
    ) {
      this.plan = plan;
      this.chain = chain;
      this.target = target;
      this.targetClass = targetClass;
      this.args = snapshotArguments(args);
      this.method = method;
    }

    /** The read options: one prebuilt instance when both TTLs are constants, otherwise lazy suppliers. */
    GetPolicy readPolicy() {
      GetPolicy prebuilt = plan.prebuiltPolicy();
      if (prebuilt != null) {
        return prebuilt;
      }
      Ttls ttls = plan.ttls();
      return ZetaCacheInterceptor.readPolicy(ttlSupplier(ttls.hard()), ttlSupplier(ttls.soft()));
    }

    /** A supplier that evaluates the TTL lazily, or the prebuilt constant one. */
    private LongSupplier ttlSupplier(Ttl ttl) {
      LongSupplier constant = ttl.constantSupplier();
      return constant != null
        ? constant
        : () -> evaluateTtl(ttl, newContext(plan, target, targetClass, args), plan.specific());
    }

    /** Foreground miss load: the invocation chain may be resumed, once, on the invocation thread. */
    @Override
    public Object load(String key) {
      MethodInvocation invocation = chain;
      chain = null;
      try {
        return invocation != null ? invocation.proceed() : invokeTarget();
      } catch (Error fatal) {
        throw fatal;
      } catch (Throwable failure) {
        return new BodyFailure(failure);
      }
    }

    /**
     * Engine background refresh: never resume the one-shot chain, even when the
     * executor runs the refresh inline on the invocation thread. A new
     * reflective invocation has none of the advice chain's context, which is
     * exactly the documented cost of stale-while-revalidate on an annotated
     * method.
     *
     * <p>A refresh failure is <b>thrown</b> to the engine instead of being
     * returned as the {@link BodyFailure} sentinel: there is no caller to
     * rethrow to, and the engine's failure path (WARN plus lease-on-failure)
     * must see a real failure — otherwise a failing source would hard-expire the
     * stale entry while re-running the body on every stale read. The foreground
     * load keeps the sentinel, which is what keeps a business exception off the
     * breaker.
     */
    @Override
    public Object reload(String key) {
      try {
        return invokeTarget();
      } catch (Error | RuntimeException failure) {
        throw failure;
      } catch (Throwable checked) {
        throw new IllegalStateException("background refresh of " + method + " failed", checked);
      }
    }

    @Override
    public boolean isCacheable(Object value) {
      if (value == null || value instanceof BodyFailure) {
        return false;
      }
      CompiledExpression unless = plan.unless();
      if (unless.absent()) {
        return true;
      }
      // The context is built only for a present expression, so the common
      // (no-unless) read pays no evaluation allocation.
      return !vetoed(unless, newPostContext(plan, target, targetClass, args, value));
    }

    /** Drop the one-shot advice chain as soon as the foreground read returns. */
    void release() {
      chain = null;
    }

    private Object invokeTarget() throws Throwable {
      if (target == null) {
        throw new IllegalStateException(
          "cannot refresh " + method + " reflectively: the invocation has no target object"
        );
      }
      // The plan's method is the implementation method; when the target is an unwrappable proxy
      // that does not implement it, fall back to the method the invocation reported. An interface
      // proxy whose target implements a super-interface (Spring Data: SimpleJpaRepository for
      // CrudRepository) implements neither, so the last resort is the most specific method on the
      // target's own class — otherwise every background refresh fails with
      // "object is not an instance of declaring class".
      Method invocable = plan.specific();
      if (!invocable.getDeclaringClass().isInstance(target)) {
        invocable = method.getDeclaringClass().isInstance(target)
          ? method
          : AopUtils.getMostSpecificMethod(method, target.getClass());
        if (!invocable.getDeclaringClass().isInstance(target)) {
          invocable = method;
        }
      }
      try {
        return invocable.invoke(target, ZetaEvaluationContext.packArguments(invocable, args));
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }
  }

  /**
   * Copy the invocation argument array and every array element it holds. The loader outlives
   * the invocation (a soft refresh keeps it until the background reload), and a caller that
   * mutates its own array after the call must not change the arguments that reload reuses. A
   * non-array mutable object is still shared; such a method must not declare a soft TTL.
   *
   * @param args the invocation arguments
   * @return a snapshot safe to retain, with array elements copied by content
   */
  @SuppressWarnings("all")
  private static Object[] snapshotArguments(Object[] args) {
    if (args.length == 0) {
      return NO_ARGS;
    }
    Object[] copy = args.clone();
    for (int i = 0; i < copy.length; i++) {
      Object argument = copy[i];
      if (argument != null && argument.getClass().isArray()) {
        int length = Array.getLength(argument);
        Object cloned = Array.newInstance(argument.getClass().getComponentType(), length);
        System.arraycopy(argument, 0, cloned, 0, length);
        copy[i] = cloned;
      }
    }
    return copy;
  }

  /** A completed load whose value is a failure; never stored. */
  private record BodyFailure(Throwable cause) {}
}
