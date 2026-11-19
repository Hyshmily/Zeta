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

import io.github.hyshmily.zeta.annotation.support.CompiledExpression;
import io.github.hyshmily.zeta.annotation.support.DefaultKey;
import io.github.hyshmily.zeta.annotation.support.InvocationArguments;
import io.github.hyshmily.zeta.annotation.support.ParameterBindings;
import io.github.hyshmily.zeta.annotation.support.ZetaEvaluationContext;
import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.InvalidatePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.PutPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.StalePolicy;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.function.LongSupplier;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * What one annotated method does, resolved once per (method, user class). A sealed
 * hierarchy rather than one record with unused fields: an attribute that cannot
 * apply to a verb is not representable.
 *
 * <p>The plan is the annotation layer's immutable data model. It owns the key
 * shape, the TTL classification and evaluation, and the write policy assembled
 * from the resolved TTLs; the interceptor owns the per-invocation orchestration
 * and the evaluation context, and {@link TargetMethodResolver} owns reflective
 * target resolution.
 */
sealed interface CachePlan {
  /** The group's most-specific method: {@code #root.method}, reflective refresh and the parameter-name fallback. */
  Method specific();

  /** The plan's parameter slot table, built once at resolution. */
  ParameterBindings bindings();

  /**
   * {@code @ZetaCached}. The trailing booleans are the per-call decisions resolved once at plan
   * time: whether the method returns {@link java.util.Optional} and whether an {@code unless} is
   * declared.
   */
  record Read(
    Method specific,
    ParameterBindings bindings,
    Key key,
    CompiledExpression condition,
    CompiledExpression unless,
    Ttls ttls,
    @Nullable GetPolicy prebuiltPolicy,
    @Nullable Method erasedBridge,
    boolean optional,
    boolean hasUnless
  ) implements CachePlan {
    static Read of(
      Method specific,
      ParameterBindings bindings,
      Key key,
      CompiledExpression condition,
      CompiledExpression unless,
      Ttls ttls,
      @Nullable GetPolicy prebuiltPolicy,
      @Nullable Method erasedBridge
    ) {
      return new Read(
        specific,
        bindings,
        key,
        condition,
        unless,
        ttls,
        prebuiltPolicy,
        erasedBridge,
        returnsOptional(specific),
        !unless.absent()
      );
    }
  }

  /**
   * {@code @ZetaPut}. {@code cache} is the physical namespace, carried so a failed write reports it
   * without re-deriving it from the key. The trailing booleans are the per-call decisions resolved
   * once at plan time: whether a synced dynamic hard TTL is preflighted, whether the pre-body or
   * post-body phase needs a context, and whether the method returns {@link java.util.Optional}.
   */
  record Write(
    Method specific,
    String cache,
    ParameterBindings bindings,
    Key key,
    CompiledExpression condition,
    CompiledExpression unless,
    Ttls ttls,
    boolean sync,
    @Nullable PutPolicy prebuiltPolicy,
    boolean preflightHard,
    boolean needsPreContext,
    boolean needsPostContext,
    boolean optional
  ) implements CachePlan {
    static Write of(
      Method specific,
      String cache,
      ParameterBindings bindings,
      Key key,
      CompiledExpression condition,
      CompiledExpression unless,
      Ttls ttls,
      boolean sync,
      @Nullable PutPolicy prebuiltPolicy
    ) {
      boolean preflight = sync && !ttls.hard().isConstant() && !ttls.hard().resultDependent();
      boolean preContext = condition.needsContext() || (!key.resultDependent() && key.needsContext()) || preflight;
      boolean postContext =
        unless.needsContext() ||
        key.resultDependent() ||
        (!ttls.hard().isConstant() && !preflight) ||
        !ttls.soft().isConstant();
      return new Write(
        specific,
        cache,
        bindings,
        key,
        condition,
        unless,
        ttls,
        sync,
        prebuiltPolicy,
        preflight,
        preContext,
        postContext,
        returnsOptional(specific)
      );
    }

    /**
     * Evaluate the hard TTL that must be validated before the body, or {@code 0} when there is
     * none. A synced permanent result cannot be broadcast, so the rejection precedes the business
     * side effect.
     *
     * @param context the pre-body context, or {@code null} when the evaluation reads no arguments
     * @return the evaluated hard TTL, or {@code 0}
     */
    long preflightHardTtl(@Nullable ZetaEvaluationContext context) {
      if (!preflightHard) {
        return 0L;
      }
      long hardMs = ttls.hard().evaluate(context);
      PutPolicy.requireSyncable(hardMs, sync);
      return hardMs;
    }

    /**
     * The write policy: the prebuilt one, else the TTLs evaluated against the post-body context.
     *
     * @param preflightHardMs the value already evaluated before the body, or {@code 0}
     * @param context         the post-body context, or {@code null} when no expression needs one
     * @return the write policy
     */
    PutPolicy policy(long preflightHardMs, @Nullable ZetaEvaluationContext context) {
      if (prebuiltPolicy != null) {
        return prebuiltPolicy;
      }
      long hardMs = preflightHard ? preflightHardMs : ttls.hard().evaluate(context);
      long softMs = ttls.soft().evaluate(context);
      return PutPolicy.of(hardMs, softMs, sync);
    }
  }

  /**
   * {@code @ZetaInvalidate}: every removal declared on one method. Each operation owns its target,
   * condition and policy, so one declaration's skipped condition or empty expansion can neither
   * suppress nor re-resolve another's removal. Whether a phase needs an evaluation context is
   * resolved once from those operations, never re-derived per call.
   */
  record Invalidate(
    Method specific,
    ParameterBindings bindings,
    List<Operation> operations,
    boolean needsPreContext,
    boolean needsPostContext,
    boolean optional
  ) implements CachePlan {
    public Invalidate {
      operations = List.copyOf(operations);
    }

    static Invalidate of(Method specific, ParameterBindings bindings, List<Operation> operations) {
      boolean preContext = false;
      boolean postContext = false;
      for (Operation operation : operations) {
        preContext |= operation.needsPreContext();
        postContext |= operation.needsPostContext();
      }
      return new Invalidate(specific, bindings, operations, preContext, postContext, returnsOptional(specific));
    }

    /**
     * One declared removal. Its condition and target phases follow from the policy's
     * {@code immediate} flag and the target's {@code #result} dependency; the predicates below
     * derive that phase from the immutable declaration.
     */
    record Operation(Target target, InvalidatePolicy policy, CompiledExpression condition) {
      boolean immediate() {
        return policy.immediate();
      }

      boolean unconditional() {
        return condition.absent();
      }

      /** Whether the target is resolved and validated before the body runs. */
      boolean targetBeforeBody() {
        return immediate() || (unconditional() && !target.resultDependent());
      }

      boolean needsPreContext() {
        return (immediate() && condition.needsContext()) || (targetBeforeBody() && target.needsContext());
      }

      boolean needsPostContext() {
        return (!immediate() && condition.needsContext()) || (!targetBeforeBody() && target.needsContext());
      }
    }
  }

  /**
   * What one invalidation removes: one key, one key per resolved element, or the whole namespace.
   * The three shapes are structurally exclusive, so a namespace clear cannot carry a stale key and
   * a single-key removal cannot accidentally be expanded.
   */
  sealed interface Target {
    /** Whether resolution needs the post-body {@code #result}. */
    boolean resultDependent();

    /** Whether resolution needs a SpEL context. */
    boolean needsContext();

    /**
     * Resolve this target for one phase.
     *
     * @param args      the invocation arguments
     * @param context   the phase context, or {@code null} when this target needs none
     * @param afterBody whether the body already returned; a target resolved in that phase treats a
     *                  {@code null} key, or a {@code null}/empty collection, as no removal
     * @return the executable target, or {@code null} when this phase removes nothing
     * @throws IllegalStateException when a target resolved before the body yields {@code null}
     */
    @Nullable
    ResolvedTarget resolve(Object[] args, @Nullable ZetaEvaluationContext context, boolean afterBody);

    /** One explicit or default-encoded key. */
    record Key(String cache, CachePlan.Key key) implements Target {
      @Override
      public boolean resultDependent() {
        return key.resultDependent();
      }

      @Override
      public boolean needsContext() {
        return key.needsContext();
      }

      @Override
      public @Nullable ResolvedTarget resolve(
        Object[] args,
        @Nullable ZetaEvaluationContext context,
        boolean afterBody
      ) {
        String resolved = afterBody ? key.resolveOrNull(args, context) : key.resolve(args, context);
        return resolved == null ? null : new ResolvedTarget.Single(cache, resolved);
      }
    }

    /** One physical key per non-null element of the resolved array/Iterable. */
    record Keys(String cache, String namespace, CompiledExpression expression, Method specific) implements Target {
      @Override
      public boolean resultDependent() {
        return expression.referencesResult();
      }

      @Override
      public boolean needsContext() {
        return expression.needsContext();
      }

      @Override
      public @Nullable ResolvedTarget resolve(
        Object[] args,
        @Nullable ZetaEvaluationContext context,
        boolean afterBody
      ) {
        Object value = expression.getValue(context);
        if (value == null) {
          if (!afterBody) {
            throw new IllegalStateException("cache key '" + expression.raw() + "' resolved to null for " + specific);
          }
          return null;
        }
        List<String> keys = expand(namespace, value);
        return keys.isEmpty() ? null : new ResolvedTarget.Many(cache, keys);
      }

      /** The bound on one expansion, matching the default-key collection cap. */
      private static final int MAX_KEYS = 1024;

      /** Expand one array or {@link Iterable} into distinct physical keys, skipping null elements. */
      private List<String> expand(String namespace, Object value) {
        if (value.getClass().isArray()) {
          int length = Array.getLength(value);
          if (length == 0) {
            return List.of();
          }
          requireBounded(length);
          Set<String> keys = new LinkedHashSet<>(capacity(length));
          for (int i = 0; i < length; i++) {
            Object element = Array.get(value, i);
            if (element != null) {
              keys.add(DefaultKey.encodeSingle(namespace, element));
            }
          }
          return List.copyOf(keys);
        }
        if (value instanceof Iterable<?> iterable) {
          if (iterable instanceof Collection<?> collection && collection.isEmpty()) {
            return List.of();
          }
          if (iterable instanceof Collection<?> collection) {
            requireBounded(collection.size());
          }
          Set<String> keys =
            iterable instanceof Collection<?> collection
              ? new LinkedHashSet<>(capacity(collection.size()))
              : new LinkedHashSet<>();
          int count = 0;
          for (Object element : iterable) {
            if (++count > MAX_KEYS) {
              requireBounded(count);
            }
            if (element != null) {
              keys.add(DefaultKey.encodeSingle(namespace, element));
            }
          }
          return List.copyOf(keys);
        }
        throw new IllegalStateException("eachKey requires an array or Iterable, got " + value.getClass().getName());
      }

      private void requireBounded(int size) {
        if (size > MAX_KEYS) {
          throw new IllegalStateException(
            "eachKey on " + specific + " expands more than " + MAX_KEYS + " keys; split the batch"
          );
        }
      }

      /** A set capacity that holds {@code entries} at the default 0.75 load factor without a rehash. */
      private static int capacity(int entries) {
        return (int) (entries / 0.75f) + 1;
      }
    }

    /** Every entry of one namespace. */
    record Namespace(String cache) implements Target {
      @Override
      public boolean resultDependent() {
        return false;
      }

      @Override
      public boolean needsContext() {
        return false;
      }

      @Override
      public ResolvedTarget resolve(Object[] args, @Nullable ZetaEvaluationContext context, boolean afterBody) {
        return new ResolvedTarget.All(cache);
      }
    }
  }

  /** One executable invalidation target: the namespace it addresses and the keys to remove. */
  sealed interface ResolvedTarget {
    record Single(String cache, String key) implements ResolvedTarget {}

    record Many(String cache, List<String> keys) implements ResolvedTarget {}

    record All(String cache) implements ResolvedTarget {}
  }

  /** The one read-policy shape every annotation read uses. */
  static GetPolicy readPolicy(LongSupplier hardTtlMs, LongSupplier softTtlMs) {
    return new GetPolicy(hardTtlMs, softTtlMs, StalePolicy.SOFT_REFRESH, true, true);
  }

  /** Whether the method returns {@link Optional}, the one wrapper this layer unwraps. */
  private static boolean returnsOptional(Method method) {
    return method.getReturnType() == Optional.class;
  }

  /**
   * The resolved shape of a method's key. Four mutually exclusive forms: a fixed physical
   * key, one argument value, the canonical argument-list key, or a SpEL expression. The shape
   * is chosen once at resolution, so a plain hit never reclassifies it.
   *
   * <p>An explicit expression encodes its value as one default-key argument, so
   * {@code key = "#id"} and an absent key address the same entry for a single-id method. A
   * whole {@code #root.args} selects the argument list itself and uses the multi-argument
   * default encoding instead. A context-free expression is folded to a constant physical key;
   * a constant {@code null} is a configuration error.
   */
  sealed interface Key {
    /** Whether the key reads the post-body {@code #result}; only the expression form can. */
    default boolean resultDependent() {
      return false;
    }

    /** Whether resolution needs a SpEL context; only the expression form does. */
    default boolean needsContext() {
      return false;
    }

    /** The method the key belongs to, named in a resolution failure. */
    Method method();

    /** The configured key text, or {@code null} for an absent attribute or a whole {@code #root.args}. */
    @Nullable
    String rawExpression();

    /**
     * The physical key, or {@code null} when the selected argument or expression value is
     * {@code null}; a whole-argument-list key throws instead, because a null argument cannot be
     * encoded.
     * {@code context} must be non-null whenever {@link #needsContext()} is true.
     */
    @Nullable
    String resolveOrNull(Object[] args, @Nullable ZetaEvaluationContext context);

    /**
     * The physical key.
     *
     * @param args    the invocation arguments
     * @param context the context, or {@code null} when the key needs none
     * @return the physical key
     * @throws IllegalStateException when the key resolves to {@code null}, or an
     *                               argument-list key sees a null argument
     */
    default String resolve(Object[] args, @Nullable ZetaEvaluationContext context) {
      String resolved = resolveOrNull(args, context);
      if (resolved == null) {
        throw new IllegalStateException("cache key '" + rawExpression() + "' resolved to null for " + method());
      }
      return resolved;
    }

    static Key of(String namespace, Method method, CompiledExpression compiled, String@Nullable [] parameterNames) {
      String constantKey = constantKeyOf(namespace, method, compiled);
      if (constantKey != null) {
        return new Constant(constantKey, method, compiled.raw());
      }
      int index = argumentIndex(compiled, parameterNames);
      if (index >= 0) {
        Class<?> parameterType = method.getParameterTypes()[index];
        // A varargs tail is one array parameter (or several expanded arguments). Only the SpEL
        // lookup path preserves both shapes; the single-argument fast path would read just the
        // first expanded element.
        if (method.isVarArgs() && index == method.getParameterCount() - 1) {
          index = -1;
        } else {
          return new SingleArgument(
            namespace,
            method,
            index,
            parameterType.isArray() ? parameterType : null,
            compiled.raw()
          );
        }
      }
      if (compiled.absent() || compiled.wholeRootArgs()) {
        return new DefaultArguments(namespace, method, compiled.raw());
      }
      return new Expression(namespace, method, compiled, compiled.referencesResult());
    }

    /**
     * Precompute the full physical key when it cannot depend on the invocation: the
     * no-argument default key, a whole {@code #root.args} over no arguments, and a
     * context-free expression. A constant {@code null} cannot address an entry.
     */
    private static @Nullable String constantKeyOf(String namespace, Method method, CompiledExpression compiled) {
      if (compiled.absent() || compiled.wholeRootArgs()) {
        return method.getParameterCount() == 0 ? DefaultKey.encode(namespace, new Object[0]) : null;
      }
      if (!compiled.constant()) {
        return null;
      }
      Object value = compiled.evaluateConstant();
      if (value == null) {
        throw new IllegalStateException("cache key '" + compiled.raw() + "' on " + method + " is a constant null");
      }
      return DefaultKey.encodeSingle(namespace, value);
    }

    /**
     * Recognize a key that reads exactly one argument — {@code #p0}/{@code #a0} or a bare
     * parameter name — so the common single-id key resolves straight from the argument
     * array instead of through SpEL. Anything else returns {@code -1} and keeps the full
     * expression path.
     */
    private static int argumentIndex(CompiledExpression expression, String@Nullable [] parameterNames) {
      String name = expression.singleVariable();
      return name == null ? -1 : CompiledExpression.argumentIndex(name, parameterNames);
    }

    /** A full physical key that cannot depend on the invocation. */
    record Constant(String physicalKey, Method method, @Nullable String rawExpression) implements Key {
      @Override
      public @Nullable String resolveOrNull(Object[] args, @Nullable ZetaEvaluationContext context) {
        return physicalKey;
      }
    }

    /**
     * One argument value, encoded as one default-key argument. An array-shaped parameter is
     * normalized to its declared component type first: a generic interface erases {@code T[]} to
     * {@code Object[]}, and the key must see the same shape the SpEL path sees.
     */
    record SingleArgument(
      String namespace,
      Method method,
      int argumentIndex,
      @Nullable Class<?> expectedArrayType,
      @Nullable String rawExpression
    ) implements Key {
      @Override
      public @Nullable String resolveOrNull(Object[] args, @Nullable ZetaEvaluationContext context) {
        if (argumentIndex >= args.length) {
          return null;
        }
        Object argument = args[argumentIndex];
        if (argument == null) {
          return null;
        }
        if (expectedArrayType != null) {
          argument = InvocationArguments.adaptArgument(expectedArrayType, argument);
        }
        return DefaultKey.encodeSingle(namespace, argument);
      }
    }

    /** The canonical argument-list key for an absent attribute or a whole {@code #root.args}. */
    record DefaultArguments(String namespace, Method method, @Nullable String rawExpression) implements Key {
      @Override
      public String resolveOrNull(Object[] args, @Nullable ZetaEvaluationContext context) {
        for (int i = 0; i < args.length; i++) {
          if (args[i] == null) {
            throw new IllegalStateException(nullArgumentMessage(i));
          }
        }
        return DefaultKey.encode(namespace, args);
      }

      private String nullArgumentMessage(int index) {
        String prefix = "null argument " + index + " for " + method + ": ";
        if (rawExpression == null) {
          return prefix + "pass a non-null argument or declare an explicit key";
        }
        return (
          prefix +
          "key = \"" +
          rawExpression +
          "\" requires every argument to be non-null; pass a non-null argument or declare an expression that tolerates null"
        );
      }
    }

    /** A SpEL expression evaluated once per call. */
    record Expression(
      String namespace,
      Method method,
      CompiledExpression expression,
      boolean resultDependent
    ) implements Key {
      @Override
      public boolean needsContext() {
        return true;
      }

      @Override
      public @Nullable String rawExpression() {
        return expression.raw();
      }

      @Override
      public @Nullable String resolveOrNull(Object[] args, @Nullable ZetaEvaluationContext context) {
        Object value = expression.getValue(context);
        return value == null ? null : DefaultKey.encodeSingle(namespace, value);
      }
    }
  }

  /** Both TTL attributes of one verb, classified at resolution. */
  record Ttls(Ttl hard, Ttl soft) {
    boolean bothConstant() {
      return hard.isConstant() && soft.isConstant();
    }
  }

  /**
   * One TTL attribute classified at resolution. The two forms are structurally exclusive: a
   * constant is a folded millisecond value, while a dynamic attribute is SpEL compiled once and
   * evaluated lazily and is the only form that can read {@code #result}.
   *
   * <p>An integral literal — optionally signed, after stripping surrounding whitespace — and a
   * context-free SpEL expression such as {@code 60000L} or {@code 60 * 1000} are folded to a
   * constant with one prebuilt {@link LongSupplier}, so a static TTL costs no lambda per call and
   * no SpEL. A negative literal clamps to {@code 0} (the configured default); a constant expression
   * that evaluates negative fails resolution, because SpEL's integer arithmetic overflows at 32
   * bits and a negative fold would otherwise silently degrade a 30-day TTL to the default. A
   * floating-point literal is rejected: milliseconds are discrete, and accepting
   * {@code 1e3} or {@code NaN} only buys precision hazards. A literal outside the {@code long}
   * range fails resolution with a range error. Anything else is SpEL compiled once and evaluated
   * lazily; {@code resultDependent} marks an expression that must wait for the body. A parse
   * failure or a constant expression that fails to evaluate fails resolution; only a runtime
   * evaluation of a dynamic expression degrades to the configured default, including a
   * floating-point result, so a broken attribute can never fail a read.
   */
  sealed interface Ttl permits Ttl.Constant, Ttl.Dynamic {
    BigDecimal MAX_LONG_DECIMAL = BigDecimal.valueOf(Long.MAX_VALUE);

    boolean isConstant();

    /**
     * Whether this is the constant {@link Long#MAX_VALUE} (permanent), which {@code sync = true}
     * cannot broadcast safely.
     */
    default boolean isPermanent() {
      return false;
    }

    default boolean resultDependent() {
      return false;
    }

    /** The prebuilt supplier when this TTL is constant, else {@code null}. */
    default @Nullable LongSupplier constantSupplier() {
      return null;
    }

    /**
     * Evaluate this TTL against the context of the current phase. A constant returns its folded
     * value without a context; a negative or absent dynamic result clamps to {@code 0} (use the
     * configured default), a floating-point result is rejected like a floating-point literal at
     * resolution and also resolves to {@code 0}, and an evaluation failure is logged at DEBUG and
     * degrades the same way, because a runtime failure must not fail the cache operation.
     */
    long evaluate(@Nullable ZetaEvaluationContext context);

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
        return Constant.of(0L);
      }
      if (isInteger(literal)) {
        try {
          return Constant.of(Math.max(0L, Long.parseLong(literal)));
        } catch (NumberFormatException overflow) {
          throw new IllegalStateException(
            role + " literal '" + literal + "' on " + method + " exceeds the long range",
            overflow
          );
        }
      }
      CompiledExpression compiled = CompiledExpression.of(literal, parser, role, method, parameterNames, resultAllowed);
      if (compiled.constant()) {
        Object value = compiled.evaluateConstant();
        if (!(value instanceof Number number) || isFloating(number)) {
          throw new IllegalStateException(role + " '" + literal + "' on " + method + " must be an integral number");
        }
        if (isNegative(number)) {
          throw new IllegalStateException(
            role +
              " '" +
              literal +
              "' on " +
              method +
              " evaluated to a negative value (" +
              number +
              "); check for 32-bit integer overflow and write a long literal such as 2592000000L"
          );
        }
        return Constant.of(clampTtl(number));
      }
      return new Dynamic(role, compiled, method, resultAllowed && compiled.referencesResult());
    }

    /** Whether a numeric TTL result is a floating-point value, which milliseconds cannot represent. */
    private static boolean isFloating(Number value) {
      return value instanceof Double || value instanceof Float;
    }

    /** Whether a constant TTL value is below zero, including a {@link BigInteger}/{@link BigDecimal} result. */
    private static boolean isNegative(Number value) {
      if (value instanceof BigInteger integer) {
        return integer.signum() < 0;
      }
      if (value instanceof BigDecimal decimal) {
        return decimal.signum() < 0;
      }
      return value.longValue() < 0;
    }

    /** A folded millisecond value, which is also the prebuilt supplier. */
    record Constant(long millis) implements Ttl, LongSupplier {
      private static final Constant ZERO = new Constant(0L);

      static Constant of(long millis) {
        return millis == 0L ? ZERO : new Constant(millis);
      }

      @Override
      public boolean isConstant() {
        return true;
      }

      @Override
      public boolean isPermanent() {
        return millis == Long.MAX_VALUE;
      }

      @Override
      public LongSupplier constantSupplier() {
        return this;
      }

      @Override
      public long getAsLong() {
        return millis;
      }

      @Override
      public long evaluate(@Nullable ZetaEvaluationContext context) {
        return millis;
      }
    }

    /** A compiled expression evaluated lazily on the phase that needs it. */
    @Slf4j
    record Dynamic(String role, CompiledExpression expression, Method method, boolean resultDependent) implements Ttl {
      @Override
      public boolean isConstant() {
        return false;
      }

      @Override
      public long evaluate(@Nullable ZetaEvaluationContext context) {
        try {
          Number value = expression.getValue(context, Number.class);
          if (value == null) {
            return 0L;
          }
          if (isFloating(value)) {
            log.debug("[Zeta] {} evaluated to a floating-point value for {}; using the default TTL", role, method);
            return 0L;
          }
          return clampTtl(value);
        } catch (RuntimeException e) {
          log.debug("[Zeta] {} evaluation failed for {}: {}", role, method, e.toString());
          return 0L;
        }
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
}
