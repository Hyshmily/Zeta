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
import io.github.hyshmily.zeta.annotation.CachePlan.ResolvedTarget;
import io.github.hyshmily.zeta.annotation.support.CompiledExpression;
import io.github.hyshmily.zeta.annotation.support.InvocationArguments;
import io.github.hyshmily.zeta.annotation.support.ZetaEvaluationContext;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.PutPolicy;
import io.github.hyshmily.zeta.util.Internal;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Optional;
import java.util.function.LongSupplier;
import lombok.extern.slf4j.Slf4j;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.springframework.aop.ProxyMethodInvocation;
import org.springframework.aop.TargetSource;
import org.springframework.aop.framework.Advised;
import org.springframework.core.BridgeMethodResolver;
import org.springframework.core.BridgeMethodResolver;

/**
 * The one advice behind {@link ZetaCached}, {@link ZetaPut} and
 * {@link ZetaInvalidate}: {@link PlanResolver} resolves the method's plan once,
 * the advice resolves the key once, and then performs the plan's data verbs
 * against the {@link Zeta} facade.
 *
 * <p><b>The method body is the loader.</b> A read hands the invocation to
 * {@link Zeta#get}, so soft-expire (stale-while-revalidate) and SingleFlight
 * dedup come for free. A business exception must reach the caller unchanged and
 * must not be counted as a data-source failure (that would drive the breaker
 * open), so the read path returns a {@link BodyFailure} sentinel from the loader
 * and rethrows it as the caller's cause; the refresh leg throws instead, because
 * the engine's failure path (WARN plus lease-on-failure) must observe a real
 * failure (design decisions: annotations).
 *
 * <p><b>Thread contract.</b> {@code MethodInvocation.proceed()} is single-shot
 * and not thread-safe, so it serves only the first load, on the invocation
 * thread — the body therefore keeps its transaction, security context and MDC.
 * The invocation chain is released as soon as the foreground read returns,
 * because a refresh task may outlive the caller and must not pin the proxy
 * chain. The refresh goes through {@link CacheLoader#reload}, which invokes the
 * target method reflectively. When only a proxy can invoke that method, the
 * reflective call is marked so this advice lets it reach the body instead of
 * re-entering the cache read; that refreshed value is only ever written to the
 * local L1. A method whose body depends on request-scoped state must not be
 * given a soft TTL.
 *
 * <p>Resolution, key encoding, the per-phase evaluation context and the
 * fail-fast validation rules are owned by {@link PlanResolver} and
 * {@link CachePlan} (design decisions: annotations, design decisions:
 * annotation attributes).
 */
@Slf4j
@Internal
public final class ZetaCacheInterceptor implements MethodInterceptor {

  private static final Object[] NO_ARGS = new Object[0];

  /**
   * The proxy a background refresh is invoking reflectively on this thread, if any. A refresh that
   * cannot reach a callable target invokes the proxy itself; without this marker that reflective
   * call re-enters this advice chain and serves the stale entry as the "refreshed" value (or joins
   * its own load). The marker is scoped to the reflective call and matched by proxy identity and
   * method signature, so a body's nested call to another cache method is not bypassed.
   */
  private static final ThreadLocal<RefreshBypass> REFRESH_BYPASS = new ThreadLocal<>();

  private final Zeta zeta;

  private final PlanResolver resolver;

  private final CacheFailurePolicy failurePolicy;

  /**
   * Create the interceptor.
   *
   * @param zeta          the facade every data verb goes through
   * @param resolver      the plan source shared with the advisor
   * @param failurePolicy the cache-side failure policy
   */
  public ZetaCacheInterceptor(Zeta zeta, PlanResolver resolver, CacheFailurePolicy failurePolicy) {
    this.zeta = zeta;
    this.resolver = resolver;
    this.failurePolicy = failurePolicy;
  }

  /**
   * Create the interceptor with the default failure policy.
   *
   * @param zeta     the facade every data verb goes through
   * @param resolver the plan source shared with the advisor
   */
  public ZetaCacheInterceptor(Zeta zeta, PlanResolver resolver) {
    this(zeta, resolver, CacheFailurePolicy.defaults());
  }

  @Override
  public Object invoke(@NonNull MethodInvocation invocation) throws Throwable {
    if (bypassesRefreshReentry(invocation)) {
      return invocation.proceed();
    }
    PlanResolver.InvocationPlan resolved = resolver.resolveInvocation(invocation);
    if (resolved == null) {
      return invocation.proceed();
    }

    CachePlan plan = resolved.plan();
    Object target = resolved.target();
    Class<?> targetClass = resolved.targetClass();
    Object[] args = invocation.getArguments();
    if (args == null) {
      args = NO_ARGS;
    }
    if (plan instanceof CachePlan.Read read) {
      return read(invocation, read, target, targetClass, args);
    }
    if (plan instanceof CachePlan.Write write) {
      return write(invocation, write, target, targetClass, args);
    }
    if (plan instanceof CachePlan.Invalidate invalidate) {
      return invalidate(invocation, invalidate, target, targetClass, args);
    }
    throw new AssertionError("unknown plan type: " + plan.getClass());
  }

  /**
   * The read: the body is the loader, so the engine owns hit/miss/soft-expire,
   * the pre-read stamp and the dedup of concurrent same-key loads. The loader's
   * {@code load} may resume the advice chain once; its {@code reload} never
   * does.
   */
  private Object read(
    MethodInvocation invocation,
    CachePlan.Read plan,
    @Nullable Object target,
    Class<?> targetClass,
    Object[] args
  ) throws Throwable {
    // One detached argument view for the key, the post-body unless and a reflective refresh: a
    // later caller or body mutation must not change what is evaluated after the body. A flat
    // argument list is returned as-is, so an array-free call copies nothing. The copy is deferred
    // until after a condition that needs no context, so a bypassed call pays none.
    boolean conditionContext = plan.condition().needsContext();
    Object[] snapshot = conditionContext ? InvocationArguments.snapshot(args) : args;
    ZetaEvaluationContext pre = conditionContext ? resolver.createContext(plan, target, targetClass, snapshot) : null;
    if (!conditionPasses(plan.condition(), pre, plan.specific())) {
      return invocation.proceed();
    }
    if (!conditionContext) {
      snapshot = InvocationArguments.snapshot(args);
      if (plan.key().needsContext()) {
        pre = resolver.createContext(plan, target, targetClass, snapshot);
      }
    }
    // Resolve the key before touching the engine or allocating the loader, so a
    // null argument or a null key fails without a doomed load object.
    String key = plan.key().resolve(snapshot, pre);
    ReadLoader loader = new ReadLoader(invocation, plan, target, targetClass, snapshot, pre);
    try {
      Object value = zeta.get(key, loader, loader.readPolicy());
      if (value instanceof BodyFailure failure) {
        throw failure.cause();
      }
      return plan.optional() ? wrapOptional(value) : value;
    } finally {
      // A refresh task can retain this loader after the caller returns; the advice chain is
      // useless to it (reload never proceeds) and is released here. The loader keeps the target
      // and its array-argument snapshot. A method whose body depends on request-scoped or
      // caller-mutated argument state must not use a soft TTL: the refresh cannot snapshot
      // arbitrary mutable objects, only the argument array and its array elements.
      loader.release();
    }
  }

  /**
   * The write: a false condition runs the body without resolving the key or
   * writing. A result-independent key resolves before the body, so a null
   * argument or a null key fails before the transaction commits. A synced
   * dynamic hard TTL is the only TTL evaluated before the body: a permanent
   * value cannot be broadcast, and that rejection must precede the business
   * side effect. Every other TTL waits until the body returned a non-null,
   * non-vetoed value, and all post-body expressions share one context bound to
   * that value. A result-dependent key that resolves to null skips the write:
   * the body has already produced its side effect, so a null key is a no-op
   * rather than a failure.
   */
  private Object write(
    MethodInvocation invocation,
    CachePlan.Write plan,
    @Nullable Object target,
    Class<?> targetClass,
    Object[] args
  ) throws Throwable {
    ZetaEvaluationContext pre = plan.needsPreContext() ? resolver.createContext(plan, target, targetClass, args) : null;
    if (!conditionPasses(plan.condition(), pre, plan.specific())) {
      return invocation.proceed();
    }
    CachePlan.Key keyPlan = plan.key();
    String key = keyPlan.resultDependent() ? null : keyPlan.resolve(args, pre);
    long preflightHardMs = plan.preflightHardTtl(pre);

    Object returned = invocation.proceed();
    Object value = plan.optional() ? unwrapOptional(returned) : returned;
    if (value == null) {
      return returned;
    }

    // One post-body context for unless, a result-dependent key and every TTL evaluated
    // at write time, so they all see the same #result. The pre-body context is reused
    // when it exists: the phases never overlap, so binding the result on it is safe.
    ZetaEvaluationContext post = plan.needsPostContext()
      ? postContextFrom(plan, pre, target, targetClass, args, value)
      : null;
    if (vetoed(plan.unless(), post, plan.specific())) {
      return returned;
    }
    if (keyPlan.resultDependent()) {
      key = keyPlan.resolveOrNull(args, post);
      if (key == null) {
        return returned;
      }
    }
    // Policy assembly is validation, not an engine failure: a result-dependent permanent synced
    // TTL or an unencodable duration must surface after the body (the only point it is knowable)
    // instead of being logged away as a failed cache write.
    PutPolicy policy = plan.policy(preflightHardMs, post);
    try {
      zeta.put(key, value, policy);
    } catch (RuntimeException e) {
      failurePolicy.onFailure(new CacheFailure(CacheVerb.PUT, plan.cache(), List.of(key), value, e, false));
    }
    return returned;
  }

  /**
   * Run every declared invalidation in its phase. A target that can resolve before the body is
   * resolved and validated first, so a later declaration's null argument or bad key cannot leave an
   * earlier removal applied while the body never runs. A conditional or result-dependent target
   * resolves after the body, against the one post-body context.
   */
  private Object invalidate(
    MethodInvocation invocation,
    CachePlan.Invalidate plan,
    @Nullable Object target,
    Class<?> targetClass,
    Object[] args
  ) throws Throwable {
    List<CachePlan.Invalidate.Operation> operations = plan.operations();
    int size = operations.size();
    ZetaEvaluationContext pre = plan.needsPreContext() ? resolver.createContext(plan, target, targetClass, args) : null;

    @Nullable
    ResolvedTarget[] beforeBody = null;
    for (int i = 0; i < size; i++) {
      CachePlan.Invalidate.Operation operation = operations.get(i);
      if (!operation.targetBeforeBody()) {
        continue;
      }
      // An immediate removal evaluates its condition in this phase; a present condition on a
      // deferred removal is evaluated after the body, so it must not run here.
      if (operation.immediate() && !conditionPasses(operation.condition(), pre, plan.specific())) {
        continue;
      }
      if (beforeBody == null) {
        beforeBody = new ResolvedTarget[size];
      }
      beforeBody[i] = operation.target().resolve(args, pre, false);
    }
    if (beforeBody != null) {
      for (int i = 0; i < size; i++) {
        CachePlan.Invalidate.Operation operation = operations.get(i);
        if (operation.immediate()) {
          apply(beforeBody[i], operation);
        }
      }
    }

    Object returned = invocation.proceed();

    ZetaEvaluationContext post = plan.needsPostContext()
      ? postContextFrom(plan, pre, target, targetClass, args, plan.optional() ? unwrapOptional(returned) : returned)
      : null;
    // A resolution failure on one declaration must not suppress the others. Collect the first
    // failure, run every remaining removal, then rethrow it: the body already committed, so one
    // malformed expression cannot leave every other namespace stale.
    RuntimeException failure = null;
    for (int i = 0; i < size; i++) {
      CachePlan.Invalidate.Operation operation = operations.get(i);
      try {
        if (operation.targetBeforeBody()) {
          // An immediate removal already ran; a deferred one applies its pre-body resolution now.
          if (beforeBody != null && !operation.immediate()) {
            apply(beforeBody[i], operation);
          }
          continue;
        }
        if (conditionPasses(operation.condition(), post, plan.specific())) {
          apply(operation.target().resolve(args, post, true), operation);
        }
      } catch (RuntimeException e) {
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
    return returned;
  }

  /**
   * Apply one resolved target through the engine, routing an engine failure to the policy. A
   * {@code null} target is no work: the declaration removed nothing in this phase.
   */
  private void apply(@Nullable ResolvedTarget target, CachePlan.Invalidate.Operation operation) {
    if (target == null) {
      return;
    }
    if (target instanceof ResolvedTarget.Single single) {
      try {
        zeta.invalidate(single.key(), operation.policy());
      } catch (RuntimeException e) {
        failurePolicy.onFailure(
          new CacheFailure(CacheVerb.INVALIDATE, single.cache(), List.of(single.key()), null, e, operation.immediate())
        );
      }
      return;
    }
    if (target instanceof ResolvedTarget.Many many) {
      try {
        zeta.invalidate(many.keys(), operation.policy());
      } catch (RuntimeException e) {
        failurePolicy.onFailure(
          new CacheFailure(CacheVerb.INVALIDATE, many.cache(), many.keys(), null, e, operation.immediate())
        );
      }
      return;
    }
    String cache = ((ResolvedTarget.All) target).cache();
    try {
      zeta.clearNamespace(cache, operation.policy());
    } catch (RuntimeException e) {
      failurePolicy.onFailure(new CacheFailure(CacheVerb.CLEAR, cache, List.of(), null, e, operation.immediate()));
    }
  }

  /**
   * Evaluate an admission condition. A false result bypasses the cache; a broken
   * guard also bypasses it, because admitting a call the caller meant to skip is
   * the unsafe direction.
   */
  private boolean conditionPasses(
    CompiledExpression condition,
    @Nullable ZetaEvaluationContext context,
    Method method
  ) {
    if (condition.absent()) {
      return true;
    }
    try {
      return Boolean.TRUE.equals(condition.getValue(context, Boolean.class));
    } catch (RuntimeException e) {
      log.debug("[Zeta] condition evaluation failed for {}: {}", method, e.toString());
      return false;
    }
  }

  /**
   * Evaluate the store veto. A broken guard keeps the value, the opposite of a broken admission
   * condition.
   *
   * @param unless  the expression to evaluate; an absent attribute vetoes nothing
   * @param context the context bound to the post-body value
   * @param method  the method the attribute belongs to
   * @return whether the expression rejects the bound result (fail-open)
   */
  private boolean vetoed(CompiledExpression unless, @Nullable ZetaEvaluationContext context, Method method) {
    if (unless.absent()) {
      return false;
    }
    try {
      return Boolean.TRUE.equals(unless.getValue(context, Boolean.class));
    } catch (RuntimeException e) {
      // A broken expression sits on a hot path and must not become a per-call log storm.
      log.debug("[Zeta] unless evaluation failed for {}: {}", method, e.toString());
      return false;
    }
  }

  /**
   * The post-body context: the pre-body context rebound to the result when one exists, else a
   * fresh one. Reusing it is safe because the phases never overlap.
   */
  private ZetaEvaluationContext postContextFrom(
    CachePlan plan,
    @Nullable ZetaEvaluationContext pre,
    @Nullable Object target,
    Class<?> targetClass,
    Object[] args,
    @Nullable Object result
  ) {
    ZetaEvaluationContext context = pre != null ? pre : resolver.createContext(plan, target, targetClass, args);
    return context.bindResult(result);
  }

  private static @Nullable Object unwrapOptional(@Nullable Object value) {
    return value instanceof Optional<?> optional ? optional.orElse(null) : value;
  }

  private static Object wrapOptional(@Nullable Object value) {
    return Optional.ofNullable(value);
  }

  /**
   * Whether this invocation is the reflective refresh call for which {@link #REFRESH_BYPASS} was
   * armed. The marker is not consumed here: it stays valid for the whole reflective call so a
   * nested proxy layer of the same refresh is bypassed too, and the caller restores it in a
   * {@code finally}. A nested call to a different method does not match the signature and keeps
   * its normal cache semantics.
   */
  private static boolean bypassesRefreshReentry(MethodInvocation invocation) {
    RefreshBypass bypass = REFRESH_BYPASS.get();
    return (
      bypass != null &&
      invocation instanceof ProxyMethodInvocation proxyInvocation &&
      bypass.matches(proxyInvocation.getProxy(), invocation.getMethod())
    );
  }

  /**
   * Whether {@code reported} is the same operation as {@code token} across a bridge or erased
   * view. Parameter assignability alone is too wide: it would match an unrelated overload such as
   * {@code load(String)} against {@code load(CharSequence)}.
   */
  private static boolean sameOperation(Method token, Method reported) {
    if (token.equals(reported)) {
      return true;
    }
    if (
      BridgeMethodResolver.findBridgedMethod(reported).equals(token) ||
      BridgeMethodResolver.findBridgedMethod(token).equals(reported)
    ) {
      return true;
    }
    // A CGLIB proxy reports the generated subclass method; it has the same signature as the plan
    // method but a different declaring class. Exact parameter types keep this from matching an
    // unrelated overload such as load(String) against load(CharSequence).
    if (!token.getName().equals(reported.getName())) {
      return false;
    }
    Class<?>[] tokenParameters = token.getParameterTypes();
    Class<?>[] reportedParameters = reported.getParameterTypes();
    if (tokenParameters.length != reportedParameters.length) {
      return false;
    }
    for (int i = 0; i < tokenParameters.length; i++) {
      if (tokenParameters[i] != reportedParameters[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * The engine loader for one annotated read: the invocation inputs plus the one-shot advice
   * chain, which only the foreground {@link #load} may claim. A refresh never touches the chain,
   * and the foreground context is cleared before the foreground call returns, so a retained loader
   * pins neither the proxy nor any evaluation state; a later inline refresh that reuses the
   * invocation thread sees {@code released} and builds a stack-local context instead of filling
   * the cleared field again. The read snapshots array-valued arguments before the key runs, and
   * before any condition that can read them; an argument list with no array is stored as-is.
   *
   * <p>The loader is also the call's only wrapper object: a write or an
   * invalidation allocates none, and the foreground read builds at most one evaluation context,
   * reused by the pre-body phase, the lazy TTL suppliers and the post-body {@code unless}.
   */
  private final class ReadLoader implements CacheLoader<String, Object> {

    private final CachePlan.Read plan;

    /** The method the invocation reported; the reflective fallback when the target does not implement the specific one. */
    private final Method method;

    private final @Nullable Object target;

    private final Class<?> targetClass;

    private final Object[] args;

    /** The invocation thread; only it may reuse {@link #foregroundContext}. */
    private final Thread foregroundThread;

    private static final VarHandle CHAIN;

    static {
      try {
        CHAIN = MethodHandles.lookup().findVarHandle(ReadLoader.class, "chain", MethodInvocation.class);
      } catch (ReflectiveOperationException e) {
        throw new ExceptionInInitializerError(e);
      }
    }

    /**
     * The foreground one-shot advice chain until {@link #load} claims it; {@code null} once
     * consumed or released. A refresh never reads or writes it.
     */
    private volatile @Nullable MethodInvocation chain;

    /**
     * The invocation thread's single evaluation context, built on first use and cleared by
     * {@link #release()}. A refresh thread never reads it; it builds its own context.
     */
    private @Nullable ZetaEvaluationContext foregroundContext;

    /**
     * Whether the foreground call has returned. Only the invocation thread writes it, and the
     * context cache is read only behind the same thread check, so no cross-thread publication is
     * needed: another thread already fails that check, and the invocation thread observes its own
     * write. Once released, a later inline refresh reusing this thread builds a fresh context
     * instead of pinning a new one on a loader the engine may retain.
     */
    private boolean released;

    private ReadLoader(
      MethodInvocation chain,
      CachePlan.Read plan,
      @Nullable Object target,
      Class<?> targetClass,
      Object[] snapshot,
      @Nullable ZetaEvaluationContext foregroundContext
    ) {
      this.plan = plan;
      this.chain = chain;
      this.target = target;
      this.targetClass = targetClass;
      this.args = snapshot;
      this.method = chain.getMethod();
      this.foregroundThread = Thread.currentThread();
      this.foregroundContext = foregroundContext;
    }

    private ZetaEvaluationContext evaluationContext() {
      if (Thread.currentThread() == foregroundThread && !released) {
        ZetaEvaluationContext current = foregroundContext;
        if (current == null) {
          current = resolver.createContext(plan, target, targetClass, args);
          foregroundContext = current;
        }
        return current;
      }
      return resolver.createContext(plan, target, targetClass, args);
    }

    /** The read options: one prebuilt instance when both TTLs are constants, otherwise lazy suppliers. */
    GetPolicy readPolicy() {
      GetPolicy prebuilt = plan.prebuiltPolicy();
      if (prebuilt != null) {
        return prebuilt;
      }
      CachePlan.Ttls ttls = plan.ttls();
      return CachePlan.readPolicy(ttlSupplier(ttls.hard()), ttlSupplier(ttls.soft()));
    }

    /** A supplier that evaluates the TTL lazily, or the prebuilt constant one. */
    private LongSupplier ttlSupplier(CachePlan.Ttl ttl) {
      LongSupplier constant = ttl.constantSupplier();
      return constant != null ? constant : () -> ttl.evaluate(evaluationContext());
    }

    /**
     * Foreground miss load: the invocation chain may be resumed, once, on the invocation thread. A
     * load reported from another thread runs the body reflectively instead, because
     * {@code proceed()} is single-shot and must keep the caller's transaction/security context.
     */
    @Override
    public Object load(String key) {
      // proceed() is single-shot and thread-confined. A load reported on another thread (an
      // engine change or a custom executor) must not resume it there; it runs the body
      // reflectively, like a refresh, rather than corrupting the invocation context.
      MethodInvocation invocation =
        Thread.currentThread() == foregroundThread ? (MethodInvocation) CHAIN.getAndSet(this, null) : null;
      try {
        Object value = invocation != null ? invocation.proceed() : invokeTarget();
        return plan.optional() ? unwrapOptional(value) : value;
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
        Object value = invokeTarget();
        return plan.optional() ? unwrapOptional(value) : value;
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
      if (!plan.hasUnless()) {
        return true;
      }
      // Only a dynamic expression needs a context; a constant guard is a stored field read. The
      // invocation thread rebinds its one context to this value; a refresh thread builds its own.
      ZetaEvaluationContext context = plan.unless().needsContext() ? evaluationContext().bindResult(value) : null;
      return !vetoed(plan.unless(), context, method);
    }

    /**
     * Drop the one-shot advice chain and the foreground context as soon as the foreground read
     * returns. The foreground {@link #load}, when it runs, completes on the invocation thread
     * before {@code get} returns, so clearing the chain here cannot race a first load.
     */
    void release() {
      released = true;
      chain = null;
      foregroundContext = null;
    }

    private Object invokeTarget() throws Throwable {
      Object receiver = target;
      if (receiver == null) {
        throw new IllegalStateException(
          "cannot refresh " + method + " reflectively: the invocation has no target object"
        );
      }
      // A scoped bean's target is resolved per call and must be released after the refresh. The
      // proxy itself only dispatches back into this advice chain, which would re-enter the cache
      // read instead of running the body.
      TargetSource source = null;
      Object scoped = receiver;
      Object scopedTarget = null;
      TargetSource candidate = receiver instanceof Advised advised ? advised.getTargetSource() : null;
      if (candidate != null && !candidate.isStatic()) {
        source = candidate;
        scopedTarget = source.getTarget();
        if (scopedTarget == null) {
          throw new IllegalStateException("TargetSource returned no target for refresh of " + method);
        }
      }
      try {
        if (scopedTarget != null) {
          // A scoped target can itself be a proxy (a factory product or a nested AOP proxy);
          // unwrap it again so the refresh reaches the object that runs the body.
          scoped = PlanResolver.unwrapTarget(scopedTarget, method);
        }
        Object invocationTarget =
          scoped != receiver && resolver.targetMethods().canInvoke(scoped, method) ? scoped : receiver;
        Method targetMethod = resolver
          .targetMethods()
          .invocable(plan.specific(), plan.erasedBridge(), invocationTarget, method);
        // The only refresh target left that re-enters this advice chain is a proxy (the repository
        // fallback or a scoped proxy that cannot invoke the method). Mark that one reflective call
        // so invoke() lets it reach the body instead of reading the entry being refreshed.
        Object proxy =
          invocationTarget instanceof Advised || Proxy.isProxyClass(invocationTarget.getClass())
            ? invocationTarget
            : null;
        RefreshBypass previous = proxy == null ? null : REFRESH_BYPASS.get();
        if (proxy != null) {
          REFRESH_BYPASS.set(new RefreshBypass(proxy, targetMethod, plan.specific()));
        }
        try {
          return TargetMethodResolver.invoke(targetMethod, invocationTarget, args);
        } finally {
          if (proxy != null) {
            if (previous == null) {
              REFRESH_BYPASS.remove();
            } else {
              REFRESH_BYPASS.set(previous);
            }
          }
        }
      } finally {
        if (source != null) {
          source.releaseTarget(scopedTarget);
        }
      }
    }
  }

  /** The proxy and the two method views one reflective refresh is invoking on this thread. */
  private record RefreshBypass(Object proxy, Method target, Method specific) {
    boolean matches(@Nullable Object candidate, Method reported) {
      return candidate == proxy && (sameOperation(target, reported) || sameOperation(specific, reported));
    }
  }

  /** A completed load whose value is a failure; never stored. */
  private record BodyFailure(Throwable cause) {}
}
