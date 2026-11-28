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
import io.github.hyshmily.zeta.annotation.support.ParameterBindings;
import io.github.hyshmily.zeta.annotation.support.ZetaEvaluationContext;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.PutPolicy;
import io.github.hyshmily.zeta.exception.ZetaModeException;
import io.github.hyshmily.zeta.util.Internal;
import java.io.Serial;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import lombok.extern.slf4j.Slf4j;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.springframework.aop.ProxyMethodInvocation;
import org.springframework.aop.SpringProxy;
import org.springframework.aop.TargetClassAware;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.framework.AopProxyUtils;

/**
 * The AOP {@link MethodInterceptor} backing {@link ZetaCached}, {@link ZetaPut}, and
 * {@link ZetaInvalidate}. {@link PlanResolver} compiles each method's plan once; on each
 * invocation this interceptor resolves the plan and key, then executes the corresponding operations
 * against the {@link Zeta} facade.
 *
 * <p><b>The method body is the loader.</b> Reads delegate to {@link Zeta#get}, gaining soft-expiry
 * (stale-while-revalidate) and SingleFlight deduplication automatically. Exceptions thrown by the
 * method body propagate through the engine's failure path and are rethrown unchanged to the caller
 * (checked exceptions are wrapped internally and unwrapped on exit).
 *
 * <p><b>Thread contract.</b> {@link MethodInvocation#proceed()} is single-shot and confined to the
 * invocation thread, preserving transactions, security context, and MDC on foreground misses. The
 * invocation chain is released before the foreground call returns so retained refresh tasks never
 * pin the proxy chain. Background refreshes execute via {@link CacheLoader#reload} using reflective
 * invocation; when only a proxy can reach the method body, a thread-local bypass marker prevents
 * the reflective call from re-entering cache reads.
 */
@Slf4j
@Internal
public final class ZetaCacheInterceptor implements MethodInterceptor {

  /**
   * Thread-local marker identifying the proxy and plan currently undergoing a reflective refresh.
   * Scoped to the reflective call and matched by proxy identity and plan so nested calls to other
   * cached methods remain intercepted normally.
   */
  private static final ThreadLocal<RefreshBypass> REFRESH_BYPASS = new ThreadLocal<>();

  /**
   * Number of reflective refresh calls currently in flight across all threads. The steady state is
   * {@code 0}, which lets every cache read answer {@link #isBypassed} without touching the
   * thread-local at all.
   */
  private static final AtomicInteger ACTIVE_BYPASSES = new AtomicInteger();

  private final Zeta zeta;
  private final PlanResolver resolver;

  /**
   * Creates a cache interceptor bound to the given facade and plan resolver.
   *
   * @param zeta     the cache facade executing all data verbs
   * @param resolver the plan resolver shared with the advisor
   */
  public ZetaCacheInterceptor(Zeta zeta, PlanResolver resolver) {
    this.zeta = Objects.requireNonNull(zeta, "zeta must not be null");
    this.resolver = Objects.requireNonNull(resolver, "resolver must not be null");
  }

  /**
   * Intercepts a matched method invocation, resolves its {@link CachePlan}, and dispatches the
   * corresponding read, write, or invalidation lifecycle.
   *
   * <p>Plan lookup is tiered by cost: a plain bean resolves in two map reads via its published
   * table without proxy traversal or an intermediate record. A proxy whose published table names a
   * business interface — one layer, or two when Spring Data's repository proxy is itself the target
   * of a Boot-created proxy — answers from that table when its receiver does not implement the
   * interface, again without unwrapping, and only an unfamiliar receiver shape falls back to
   * {@link #resolveProxyPlan}.
   *
   * @param invocation the intercepted method invocation
   * @return the cached or freshly computed method result
   * @throws Throwable any exception thrown by the target method or validation layer
   */
  @Override
  public @Nullable Object invoke(@NonNull MethodInvocation invocation) throws Throwable {
    Method method = invocation.getMethod();
    if (method.getDeclaringClass() == Object.class) {
      return invocation.proceed();
    }

    Object target = invocation.getThis();
    if (target != null) {
      if (isPlainReceiver(target)) {
        PlanResolver.PlanTable table = resolver.cachedTable(target.getClass());
        // A known-empty table is a negative cache for class-declared methods only: it is not proof
        // that an interface view has no plan, so a miss falls through to the proxy tier instead of
        // proceeding.
        CachePlan plan = table != null ? table.plans().get(method) : null;
        if (plan != null) {
          return dispatch(invocation, plan, target, table.userClass(), arguments(invocation));
        }

        // The receiver is the body, but only the proxy exposes the annotated view: a repository
        // proxy's target implements the parent interface, not the child that carries the verb.
        if (invocation instanceof ProxyMethodInvocation proxyInvocation) {
          Object proxy = proxyInvocation.getProxy();
          PlanResolver.PlanTable proxyTable = resolver.cachedTable(proxy.getClass());
          if (proxyTable != null && proxyTable.userClass().isInterface()) {
            CachePlan proxyPlan = proxyTable.plans().get(method);
            if (proxyPlan != null && !proxyTable.userClass().isInstance(target)) {
              return dispatch(invocation, proxyPlan, proxy, proxyTable.userClass(), arguments(invocation));
            }
          }
        }
      } else if (target instanceof Advised && Proxy.isProxyClass(target.getClass())) {
        // Two layers: this invocation's receiver is the inner interface proxy, whose own target is
        // the plain body. Dispatch on the inner proxy so a refresh keeps the query chain it applies.
        PlanResolver.PlanTable proxyTable = resolver.cachedTable(target.getClass());
        if (proxyTable != null && proxyTable.userClass().isInterface()) {
          CachePlan proxyPlan = proxyTable.plans().get(method);
          if (proxyPlan != null) {
            Object inner = AopProxyUtils.getSingletonTarget(target);
            if (inner != null && isPlainReceiver(inner) && !proxyTable.userClass().isInstance(inner)) {
              return dispatch(invocation, proxyPlan, target, proxyTable.userClass(), arguments(invocation));
            }
          }
        }
      }
    }
    ResolvedInvocation resolved = resolveProxyPlan(invocation, method, target);
    CachePlan plan = resolved.plan();
    if (plan == null) {
      return invocation.proceed();
    }
    return dispatch(invocation, plan, resolved.target(), resolved.targetClass(), arguments(invocation));
  }

  /**
   * Returns the invocation arguments, substituting the shared empty array when the invocation
   * reports none.
   *
   * @param invocation the intercepted invocation
   * @return the argument array, never {@code null}
   */
  @SuppressWarnings("all")
  private static Object[] arguments(MethodInvocation invocation) {
    Object[] args = invocation.getArguments();
    return args != null ? args : ParameterBindings.EMPTY_ARGS;
  }

  /**
   * Slow-path resolution for proxies or uncached receivers: unwraps nested proxies to the deepest
   * target that still exposes the method's declaring view, determines the authoritative target
   * class, and falls back to the proxy's business interface views when the target only implements a
   * generic parent interface.
   *
   * <p>{@link MethodInvocation#getThis()} reports the target, not the proxy, so a repository's base
   * implementation would lose the query interceptor the caller's chain applies. The refresh
   * therefore uses the proxy whenever the receiver does not implement the declaring view and the
   * invocation reports one that does; the reflective call then arms the cache bypass so that chain
   * runs without re-entering the read advice.
   *
   * <p>When the resolved target class is an interface and the receiver is a JDK proxy, the lookup
   * is keyed on the proxy's own class: that scan registers the view's table under the proxy class,
   * so from the next call on {@link #invoke} answers it from two map reads instead of unwrapping
   * again. A single business interface reuses that interface's table, so the plan instance is the
   * one the interface lookup returns and the refresh bypass keeps its identity check.
   *
   * @param invocation the intercepted invocation
   * @param method     the invoked method
   * @param target     the receiver reported by {@link MethodInvocation#getThis()}, or {@code null}
   * @return the resolved invocation metadata
   */
  private ResolvedInvocation resolveProxyPlan(MethodInvocation invocation, Method method, @Nullable Object target) {
    Object proxy = invocation instanceof ProxyMethodInvocation pm ? pm.getProxy() : null;

    Object unwrapped = PlanResolver.unwrapTarget(target, method);
    if (!method.getDeclaringClass().isInstance(unwrapped) && method.getDeclaringClass().isInstance(proxy)) {
      // Only the proxy exposes the declaring view; keep it as the reflective refresh target.
      unwrapped = proxy;
    }

    Class<?> targetClass = PlanResolver.resolveTargetClass(unwrapped, method.getDeclaringClass());
    CachePlan plan = resolver.lookupPlanForMethod(
      method,
      unwrapped != null && targetClass.isInterface() && Proxy.isProxyClass(unwrapped.getClass())
        ? unwrapped.getClass()
        : targetClass
    );
    if (plan == null && proxy != null) {
      plan = resolver.lookupPlanForMethod(method, proxy.getClass());
      if (plan != null && proxy instanceof Advised advised) {
        Class<?> view = PlanResolver.mostSpecificInterface(advised.getProxiedInterfaces(), method.getDeclaringClass());
        if (view != null) {
          targetClass = view;
          // The plan came from the proxy's table, so the view it describes is the receiver the
          // refresh must use: the raw target implements only a parent view, and its erased body is
          // not the body the caller ran (a repository's base implementation). Invoking it would
          // drop the query, entity graph and lock the proxy chain applies.
          if (!view.isInstance(unwrapped) && view.isInstance(proxy)) {
            unwrapped = proxy;
          }
        }
      }
    }
    return new ResolvedInvocation(plan, unwrapped, targetClass);
  }

  /**
   * Returns whether {@code receiver} is a plain bean eligible for fast-path table lookup.
   *
   * @param receiver the invocation receiver
   * @return {@code true} if {@code receiver} is neither a Spring proxy nor a JDK dynamic proxy
   */
  private static boolean isPlainReceiver(Object receiver) {
    return !(receiver instanceof SpringProxy || receiver instanceof TargetClassAware || receiver instanceof Proxy);
  }

  /**
   * Dispatches execution to the handler matching {@code plan}'s concrete verb type.
   *
   * @param invocation  the intercepted invocation
   * @param plan        the non-null resolved cache plan
   * @param target      the unwrapped invocation target, or {@code null}
   * @param targetClass the authoritative target class
   * @param args        the invocation arguments
   * @return the method return value
   * @throws Throwable any exception thrown during execution
   */
  private @Nullable Object dispatch(
    MethodInvocation invocation,
    CachePlan plan,
    @Nullable Object target,
    Class<?> targetClass,
    Object[] args
  ) throws Throwable {
    if (plan instanceof CachePlan.Read read) {
      if (isBypassed(invocation, plan)) {
        // One-shot: a recursive call to the same cached method from the body is a real business
        // read and must not inherit the refresh bypass.
        REFRESH_BYPASS.remove();
        return invocation.proceed();
      }
      return read(invocation, read, target, targetClass, args);
    }
    if (plan instanceof CachePlan.Write write) {
      return write(invocation, write, target, targetClass, args);
    }
    if (plan instanceof CachePlan.Invalidate invalidate) {
      return invalidate(invocation, invalidate, target, targetClass, args);
    }
    throw new AssertionError("Unsupported CachePlan type: " + plan.getClass().getName());
  }

  /**
   * Executes a {@link CachePlan.Read} by delegating to {@link Zeta#get} with the method body
   * wrapped in a {@link ReadLoader}.
   *
   * <p>Array arguments are snapshotted only when the signature can carry one and only after the
   * key resolved: a rejected call, a key failure or an all-scalar signature never pays the copy.
   * The condition and the key keep reading the invocation's own arguments — one view for both —
   * while the loader binds the snapshot, so a post-body {@code unless} or a lazy TTL cannot
   * evaluate the array the body rewrites in place.
   *
   * @param invocation  the intercepted invocation
   * @param plan        the read plan
   * @param target      the unwrapped target, or {@code null}
   * @param targetClass the authoritative target class
   * @param args        the invocation arguments
   * @return the cached or loaded value (re-wrapped in {@link Optional} if the method returns one)
   * @throws Throwable any exception thrown by the method body or expression evaluation
   */
  private @Nullable Object read(
    MethodInvocation invocation,
    CachePlan.Read plan,
    @Nullable Object target,
    Class<?> targetClass,
    Object[] args
  ) throws Throwable {
    ZetaEvaluationContext pre = plan.condition().needsContext() ? preContext(plan, target, targetClass, args) : null;
    if (!plan.condition().evalAdmission(pre)) {
      return invocation.proceed();
    }
    if (pre == null) {
      pre = preContext(plan, target, targetClass, args);
    }
    String key = plan.key().resolve(args, pre);
    Object[] view = plan.bindings().mayContainArray() ? ParameterBindings.snapshot(args) : args;
    ReadLoader loader = new ReadLoader(invocation, plan, target, targetClass, view, view == args ? pre : null);
    try {
      Object value = zeta.get(key, loader, loader.readPolicy());
      return plan.optional() ? Optional.ofNullable(value) : value;
    } catch (CheckedBodyFailure failure) {
      throw failure.original();
    } finally {
      loader.release();
    }
  }

  /**
   * Executes a {@link CachePlan.Write}: evaluates {@code condition}, pre-resolves result-independent
   * keys and synced hard TTLs before running the body, then evaluates {@code unless} and writes the
   * resulting value to {@link Zeta#put}.
   *
   * <p>Caller/configuration rejections ({@link IllegalArgumentException}, {@link ZetaModeException})
   * propagate to the caller; transient engine failures after the method body commits are logged so
   * the business result is preserved.
   *
   * @param invocation  the intercepted invocation
   * @param plan        the write plan
   * @param target      the unwrapped target, or {@code null}
   * @param targetClass the authoritative target class
   * @param args        the invocation arguments
   * @return the value returned by the target method
   * @throws Throwable any exception thrown by the method body or pre-body validation
   */
  private @Nullable Object write(
    MethodInvocation invocation,
    CachePlan.Write plan,
    @Nullable Object target,
    Class<?> targetClass,
    Object[] args
  ) throws Throwable {
    ZetaEvaluationContext pre = plan.condition().needsContext() ? preContext(plan, target, targetClass, args) : null;
    if (!plan.condition().evalAdmission(pre)) {
      return invocation.proceed();
    }
    if (pre == null) {
      pre = preContext(plan, target, targetClass, args);
    }

    CachePlan.Key keyPlan = plan.key();
    String key = keyPlan.resultDependent() ? null : keyPlan.resolve(args, pre);
    long preflightHardMs = plan.preflightHardTtl(pre);

    Object returned = invocation.proceed();
    Object value = plan.unwrapResult(returned);
    if (value == null) {
      return returned;
    }

    ZetaEvaluationContext post = plan.needsPostContext()
      ? postContext(plan, pre, target, targetClass, args, value)
      : null;
    if (plan.unless().evalVeto(post)) {
      return returned;
    }
    if (keyPlan.resultDependent()) {
      key = keyPlan.resolveOrNull(args, post);
    }
    if (key == null) {
      return returned;
    }

    PutPolicy policy = plan.policy(preflightHardMs, post);
    try {
      zeta.put(key, value, policy);
    } catch (RuntimeException e) {
      if (e instanceof IllegalArgumentException || e instanceof ZetaModeException) {
        throw e;
      }
      log.warn("[Zeta] cache put failed for namespace '{}'; the business result is returned", plan.cache(), e);
    }
    return returned;
  }

  /**
   * Executes every declared {@link CachePlan.Invalidate.Operation} in its resolved phase.
   *
   * <p>Targets that do not depend on {@code #result} are resolved and validated before running the
   * body so an invalid later declaration cannot leave an earlier pre-body removal applied without
   * the body running. Post-body operations run all remaining entries even if one fails, suppressing
   * secondary failures onto the first thrown exception.
   *
   * @param invocation  the intercepted invocation
   * @param plan        the invalidation plan
   * @param target      the unwrapped target, or {@code null}
   * @param targetClass the authoritative target class
   * @param args        the invocation arguments
   * @return the value returned by the target method
   * @throws Throwable any exception thrown by the method body or invalidation resolution
   */
  private @Nullable Object invalidate(
    MethodInvocation invocation,
    CachePlan.Invalidate plan,
    @Nullable Object target,
    Class<?> targetClass,
    Object[] args
  ) throws Throwable {
    List<CachePlan.Invalidate.Operation> operations = plan.operations();
    int size = operations.size();
    Method specific = plan.specific();
    ZetaEvaluationContext pre = preContext(plan, target, targetClass, args);
    @Nullable
    Object[] beforeBody = plan.hasPrePhase() ? resolveBeforeBody(operations, specific, args, pre) : null;

    if (beforeBody != null) {
      for (int i = 0; i < size; i++) {
        CachePlan.Invalidate.Operation operation = operations.get(i);
        if (operation.phase() == CachePlan.Invalidate.Phase.BEFORE_BODY) {
          operation.apply(zeta, beforeBody[i]);
        }
      }
    }

    Object returned = invocation.proceed();
    if (!plan.hasPostPhase()) {
      return returned;
    }

    Object result = plan.unwrapResult(returned);
    ZetaEvaluationContext post = plan.needsPostContext()
      ? postContext(plan, pre, target, targetClass, args, result)
      : null;

    RuntimeException failure = null;
    for (int i = 0; i < size; i++) {
      CachePlan.Invalidate.Operation operation = operations.get(i);
      try {
        switch (operation.phase()) {
          case BEFORE_BODY -> {
            // Already applied before invoking the method body.
          }
          case PRE_RESOLVE_POST_APPLY -> {
            if (beforeBody != null) {
              operation.applyAfterBody(zeta, beforeBody[i]);
            }
          }
          case AFTER_BODY -> {
            if (operation.condition().evalAdmission(post)) {
              operation.applyAfterBody(zeta, operation.resolve(specific, args, post, true));
            }
          }
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
   * Resolves all invalidation targets that can be evaluated prior to executing the method body.
   *
   * <p>A {@code null} entry indicates no pre-resolved work (either because the operation runs after
   * the body, its pre-body condition evaluated to {@code false}, or its key set was empty).
   *
   * @param operations the plan's invalidation operations
   * @param specific   the group's most-specific method
   * @param args       the invocation arguments
   * @param pre        the pre-body evaluation context, or {@code null}
   * @return an array aligned with {@code operations} holding each pre-resolved target or {@code null}
   */
  private @Nullable Object[] resolveBeforeBody(
    List<CachePlan.Invalidate.Operation> operations,
    Method specific,
    Object[] args,
    @Nullable ZetaEvaluationContext pre
  ) {
    int size = operations.size();
    @Nullable
    Object[] resolved = new Object[size];
    for (int i = 0; i < size; i++) {
      CachePlan.Invalidate.Operation operation = operations.get(i);
      if (operation.phase() == CachePlan.Invalidate.Phase.AFTER_BODY) {
        continue;
      }
      if (operation.phase() == CachePlan.Invalidate.Phase.BEFORE_BODY && !operation.condition().evalAdmission(pre)) {
        continue;
      }
      resolved[i] = operation.resolve(specific, args, pre, false);
    }
    return resolved;
  }

  /**
   * Creates a pre-body evaluation context if any pre-body expression in {@code plan} requires one.
   *
   * @param plan        the cache plan
   * @param target      the unwrapped target, or {@code null}
   * @param targetClass the authoritative target class
   * @param args        the invocation arguments
   * @return a fresh pre-body context, or {@code null} when unnecessary
   */
  private @Nullable ZetaEvaluationContext preContext(
    CachePlan plan,
    @Nullable Object target,
    Class<?> targetClass,
    Object[] args
  ) {
    return plan.needsPreContext() ? resolver.createContext(plan, target, targetClass, args) : null;
  }

  /**
   * Returns a post-body evaluation context with {@code #result} bound, reusing {@code pre} when
   * available or creating a fresh context otherwise.
   *
   * @param plan        the cache plan
   * @param pre         the existing pre-body context, or {@code null}
   * @param target      the unwrapped target, or {@code null}
   * @param targetClass the authoritative target class
   * @param args        the invocation arguments
   * @param result      the unwrapped method return value, or {@code null}
   * @return a post-body evaluation context bound to {@code result}
   */
  private ZetaEvaluationContext postContext(
    CachePlan plan,
    @Nullable ZetaEvaluationContext pre,
    @Nullable Object target,
    Class<?> targetClass,
    Object[] args,
    @Nullable Object result
  ) {
    ZetaEvaluationContext context = pre != null ? pre : resolver.createContext(plan, target, targetClass, args);
    return context.withResult(result);
  }

  /**
   * Invokes {@code invocable} reflectively on {@code target}, arming {@link #REFRESH_BYPASS} when
   * {@code target} is a proxy so cache read advice is bypassed for that single call.
   *
   * <p>The bypass marker is thread-confined, so the process-wide counter is incremented around it:
   * a read on any other thread then knows that no marker can exist for it and skips the
   * thread-local lookup. The token is read only when a bypass is already in flight, which keeps a
   * nested refresh restoring the outer marker.
   *
   * @param target    the target object (plain bean or proxy)
   * @param invocable the accessible reflective method to invoke
   * @param plan      the read plan being bypassed
   * @param args      the invocation arguments
   * @return the method invocation result
   * @throws Throwable any exception thrown by the underlying method body
   */
  static @Nullable Object invokeBypassingCache(Object target, Method invocable, CachePlan plan, Object[] args)
    throws Throwable {
    if (!PlanResolver.isProxy(target)) {
      return invokeReflective(invocable, target, args);
    }
    int active = ACTIVE_BYPASSES.getAndIncrement();
    RefreshBypass previous = null;
    try {
      if (active > 0) {
        previous = REFRESH_BYPASS.get();
      }
      REFRESH_BYPASS.set(new RefreshBypass(target, plan));
      return invokeReflective(invocable, target, args);
    } finally {
      if (previous == null) {
        REFRESH_BYPASS.remove();
      } else {
        REFRESH_BYPASS.set(previous);
      }
      ACTIVE_BYPASSES.decrementAndGet();
    }
  }

  /**
   * Invokes {@code invocable} with packed arguments and unwraps {@link InvocationTargetException}.
   *
   * @param invocable the accessible method
   * @param target    the invocation receiver
   * @param args      the raw invocation arguments
   * @return the invocation result
   * @throws Throwable the underlying target exception or reflective failure
   */
  private static @Nullable Object invokeReflective(Method invocable, Object target, Object[] args) throws Throwable {
    try {
      return invocable.invoke(target, ParameterBindings.pack(invocable, args));
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      throw cause != null ? cause : e;
    }
  }

  /**
   * Returns whether the current invocation matches an active {@link #REFRESH_BYPASS} marker on
   * this thread.
   *
   * <p>The counter answers the common case — no reflective refresh anywhere — without reading the
   * thread-local, so a steady-state read never builds a thread-local map for a marker it does not
   * have.
   *
   * @param invocation the intercepted invocation
   * @param plan       the resolved read plan
   * @return {@code true} if cache read advice should be bypassed for this call
   */
  private static boolean isBypassed(MethodInvocation invocation, CachePlan plan) {
    if (ACTIVE_BYPASSES.get() == 0) {
      return false;
    }
    RefreshBypass bypass = REFRESH_BYPASS.get();
    if (bypass == null) {
      // The lookup above created a null-valued entry on a thread that holds no marker; drop it so
      // the slot does not outlive this call.
      REFRESH_BYPASS.remove();
      return false;
    }
    return (
      invocation instanceof ProxyMethodInvocation proxyInvocation && bypass.matches(proxyInvocation.getProxy(), plan)
    );
  }

  /**
   * Resolved plan, unwrapped target, and authoritative target class for a single invocation.
   *
   * @param plan        the resolved {@link CachePlan}, or {@code null} if uncached
   * @param target      the unwrapped target receiver, or {@code null}
   * @param targetClass the authoritative class for {@code #root.targetClass}
   */
  private record ResolvedInvocation(@Nullable CachePlan plan, @Nullable Object target, Class<?> targetClass) {}

  /**
   * Thread-local bypass token pairing a proxied target with the read plan being refreshed.
   *
   * @param proxy the proxy instance being invoked reflectively
   * @param plan  the read plan whose cache advice should be skipped
   */
  private record RefreshBypass(Object proxy, CachePlan plan) {
    /**
     * Checks whether this bypass token matches the given proxy identity and cache plan.
     *
     * @param candidate the intercepted proxy instance
     * @param current   the plan resolved for the intercepted call
     * @return {@code true} if both proxy identity and plan match
     */
    boolean matches(@Nullable Object candidate, CachePlan current) {
      return (
        candidate == proxy &&
        (plan == current ||
          PlanResolver.sameMethod(plan.specific(), current.specific(), AopProxyUtils.ultimateTargetClass(proxy)))
      );
    }
  }

  /**
   * Stackless unchecked wrapper carrying a checked exception from a method body across the cache
   * engine's unchecked {@link CacheLoader#load} boundary so {@link #read} can rethrow it intact.
   */
  private static final class CheckedBodyFailure extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    private final Throwable original;

    /**
     * Wraps a checked exception without capturing a redundant stack trace.
     *
     * @param original the checked exception thrown by the target method
     */
    private CheckedBodyFailure(Throwable original) {
      super(null, original, false, false);
      this.original = original;
    }

    /**
     * Returns the original checked exception thrown by the target method.
     *
     * @return the wrapped checked exception
     */
    private Throwable original() {
      return original;
    }
  }

  /**
   * {@link CacheLoader} bridging one annotated read invocation to the {@link Zeta} engine.
   *
   * <p>The one-shot {@link MethodInvocation} chain may only be claimed once by the foreground
   * invocation thread during {@link #load} and is cleared in {@link #release()} before returning.
   * Background refreshes through {@link #reload} always invoke the target reflectively.
   */
  private final class ReadLoader implements CacheLoader<String, Object> {

    private final CachePlan.Read plan;
    private final @Nullable Object target;
    private final Class<?> targetClass;
    private final Object[] args;
    private @Nullable Thread invocationThread;
    private @Nullable MethodInvocation chain;
    private @Nullable ZetaEvaluationContext foregroundContext;

    /**
     * Creates a loader bound to one foreground read invocation and its snapshotted arguments.
     *
     * @param chain       the one-shot invocation chain
     * @param plan        the resolved read plan
     * @param target      the unwrapped target, or {@code null}
     * @param targetClass       the authoritative target class
     * @param args              the snapshotted invocation arguments
     * @param foregroundContext the pre-body context to share with the owner thread, or {@code null}
     *                          when the arguments were copied or no expression needs one
     */
    private ReadLoader(
      @Nullable MethodInvocation chain,
      CachePlan.Read plan,
      @Nullable Object target,
      Class<?> targetClass,
      Object[] args,
      @Nullable ZetaEvaluationContext foregroundContext
    ) {
      this.plan = plan;
      this.target = target;
      this.targetClass = targetClass;
      this.args = args;
      this.invocationThread = Thread.currentThread();
      this.chain = chain;
      this.foregroundContext = foregroundContext;
    }

    /**
     * Returns the {@link GetPolicy} for this read: the prebuilt constant policy when both TTLs are
     * constant, or a policy backed by lazy TTL suppliers otherwise.
     *
     * @return the effective {@link GetPolicy}
     */
    GetPolicy readPolicy() {
      GetPolicy prebuilt = plan.prebuiltPolicy();
      if (prebuilt != null) {
        return prebuilt;
      }
      return CachePlan.readPolicy(ttlSupplier(plan.hardTtl()), ttlSupplier(plan.softTtl()));
    }

    /**
     * Executes a foreground cache-miss load, resuming the invocation chain once on the calling
     * thread or falling back to reflective invocation on any other thread.
     *
     * @param key the physical cache key being loaded
     * @return the loaded value (unwrapped when the method returns {@link Optional})
     * @throws CheckedBodyFailure if the target method throws a checked exception
     */
    @Override
    public @Nullable Object load(String key) {
      MethodInvocation invocation = claimChain();
      try {
        Object value = invocation != null ? invocation.proceed() : invokeTarget();
        return plan.unwrapResult(value);
      } catch (Error | RuntimeException failure) {
        throw failure;
      } catch (Throwable checked) {
        throw new CheckedBodyFailure(checked);
      }
    }

    /**
     * Executes a background stale-while-revalidate refresh by invoking the target reflectively
     * without touching the one-shot foreground invocation chain.
     *
     * @param key the physical cache key being refreshed
     * @return the refreshed value (unwrapped when the method returns {@link Optional})
     * @throws IllegalStateException if the target method throws a checked exception
     */
    @Override
    public @Nullable Object reload(String key) {
      try {
        Object value = invokeTarget();
        return plan.unwrapResult(value);
      } catch (Error | RuntimeException failure) {
        throw failure;
      } catch (Throwable checked) {
        throw new IllegalStateException("background refresh failed for " + plan.specific(), checked);
      }
    }

    /**
     * Checks whether a loaded value may be stored in the cache by rejecting {@code null} and
     * evaluating any configured {@code unless} veto expression against {@code #result}.
     *
     * @param value the candidate loaded value
     * @return {@code true} if {@code value} is non-null and not vetoed by {@code unless}
     */
    @Override
    public boolean isCacheable(@Nullable Object value) {
      if (value == null) {
        return false;
      }
      if (!plan.hasUnless()) {
        return true;
      }
      ZetaEvaluationContext context = plan.unless().needsContext() ? evaluationContext().withResult(value) : null;
      return !plan.unless().evalVeto(context);
    }

    /**
     * Releases the foreground invocation chain, its shared evaluation context and the owner thread
     * before {@code get} returns. A background refresh keeps only the detached target, method and
     * arguments; clearing these fields prevents a retained task from pinning the AOP proxy, a bound
     * business value, or a caller thread that would otherwise stay reachable for the entry's TTL.
     */
    void release() {
      invocationThread = null;
      chain = null;
      foregroundContext = null;
    }

    /**
     * Returns the owner thread's evaluation context, creating it on first use. The thread identity
     * is the only guard: it is written once by the constructing thread and cleared by the same
     * thread in {@link #release()}, so another thread can only ever observe the owner or
     * {@code null} — never itself — and builds a stack-local context instead. A retained task
     * therefore cannot pin the invocation's bound result.
     */
    private ZetaEvaluationContext evaluationContext() {
      if (Thread.currentThread() == invocationThread) {
        ZetaEvaluationContext current = foregroundContext;
        if (current == null) {
          current = resolver.createContext(plan, target, targetClass, args);
          foregroundContext = current;
        }
        return current;
      }
      return resolver.createContext(plan, target, targetClass, args);
    }

    /**
     * Claims and clears the one-shot {@link MethodInvocation} if called on the foreground thread.
     *
     * @return the invocation chain if available on the foreground thread, otherwise {@code null}
     */
    private @Nullable MethodInvocation claimChain() {
      if (Thread.currentThread() != invocationThread) {
        return null;
      }
      MethodInvocation invocation = chain;
      chain = null;
      return invocation;
    }

    /**
     * Returns a {@link LongSupplier} evaluating {@code ttl} on demand. A context-free expression is
     * evaluated against the resolver's empty context, so no per-refresh root or context is built.
     *
     * @param ttl the TTL specification
     * @return a constant supplier when folded, or a dynamic evaluation supplier
     */
    private LongSupplier ttlSupplier(CachePlan.Ttl ttl) {
      LongSupplier constant = ttl.constantSupplier();
      if (constant != null) {
        return constant;
      }
      return ttl.needsContext() ? () -> ttl.evaluate(evaluationContext()) : () -> ttl.evaluate(null);
    }

    /**
     * Invokes the underlying target method reflectively while bypassing cache read advice.
     *
     * @return the raw method return value
     * @throws Throwable any exception thrown by the target method
     */
    private @Nullable Object invokeTarget() throws Throwable {
      if (target == null) {
        throw new IllegalStateException("cannot refresh " + plan.specific() + ": the invocation has no target object");
      }
      return invokeBypassingCache(target, resolver.targetMethod(plan.specific(), target), plan, args);
    }
  }
}
