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
import io.github.hyshmily.zeta.annotation.support.*;
import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.InvalidatePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.PutPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.StalePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.exception.ZetaModeException;
import io.github.hyshmily.zeta.policy.KeyFormat;
import io.github.hyshmily.zeta.util.CollectionSizing;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * What one annotated method does, resolved once per (method, user class). A sealed hierarchy
 * rather than one record with unused fields: an attribute that cannot apply to a verb is not
 * representable.
 *
 * <p>The plan is the annotation layer's immutable data model. It owns the key shape, the TTL
 * classification and evaluation, and the write policy assembled from the resolved TTLs; the
 * interceptor owns the per-invocation orchestration and the evaluation context.
 */
sealed interface CachePlan {
  /** The group's most-specific method: {@code #root.method} and the reflective refresh. */
  Method specific();

  /** The plan's parameter slot table, built once at resolution. */
  ParameterBindings bindings();

  /**
   * Whether any expression evaluated before the body needs an evaluation context. The interceptor
   * builds a pre-body context only when this is {@code true}, so a plan whose condition and key are
   * plain constants never pays for SpEL setup on the read path.
   *
   * @return {@code true} when a pre-body context must be created
   */
  boolean needsPreContext();

  /** Whether the annotated method returns {@link Optional}, so its result must be unwrapped. */
  boolean optional();

  /**
   * Unwraps a method result when the method returns {@link Optional}. One definition for the read,
   * write, invalidate and background-load paths, so they cannot disagree about an empty value.
   *
   * @param value the returned value, or {@code null}
   * @return the contained value, or {@code null} when absent or empty
   */
  default @Nullable Object unwrapResult(@Nullable Object value) {
    return optional() ? unwrap(value) : value;
  }

  /**
   * @param value the raw return value, which may be {@code null} or an {@link Optional}
   * @return the contained value, or {@code null}
   */
  static @Nullable Object unwrap(@Nullable Object value) {
    return value instanceof Optional<?> optional ? optional.orElse(null) : value;
  }

  /**
   * {@code @ZetaCached}. {@code optional} is the one per-call decision resolved at plan time;
   * whether an {@code unless} is declared is a direct field query on the guard itself.
   */
  record Read(
    Method specific,
    ParameterBindings bindings,
    Key key,
    CompiledExpression condition,
    CompiledExpression unless,
    Ttl hardTtl,
    Ttl softTtl,
    @Nullable GetPolicy prebuiltPolicy,
    boolean optional
  ) implements CachePlan {
    @Override
    public boolean needsPreContext() {
      return condition.needsContext() || key.needsContext();
    }

    boolean hasUnless() {
      return !unless.absent();
    }
  }

  /**
   * {@code @ZetaPut}. {@code cache} is the logical namespace, carried so a failed write reports it
   * without re-deriving it from the key.
   */
  record Write(
    Method specific,
    String cache,
    ParameterBindings bindings,
    Key key,
    CompiledExpression condition,
    CompiledExpression unless,
    Ttl hardTtl,
    Ttl softTtl,
    @Nullable PutPolicy prebuiltPolicy,
    boolean sync,
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
      Ttl hardTtl,
      Ttl softTtl,
      boolean sync,
      @Nullable PutPolicy prebuiltPolicy,
      boolean optional
    ) {
      boolean preflight = sync && !hardTtl.isConstant() && !hardTtl.resultDependent();
      boolean preContext =
        condition.needsContext() ||
        (!key.resultDependent() && key.needsContext()) ||
        (preflight && hardTtl.needsContext());
      boolean postContext =
        unless.needsContext() ||
        key.resultDependent() ||
        (!hardTtl.isConstant() && !preflight && hardTtl.needsContext()) ||
        (!softTtl.isConstant() && softTtl.needsContext());
      return new Write(
        specific,
        cache,
        bindings,
        key,
        condition,
        unless,
        hardTtl,
        softTtl,
        prebuiltPolicy,
        sync,
        preflight,
        preContext,
        postContext,
        optional
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
      long hardMs = hardTtl.evaluate(context);
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
      long hardMs = preflightHard ? preflightHardMs : hardTtl.evaluate(context);
      long softMs = softTtl.evaluate(context);
      return PutPolicy.of(hardMs, softMs, sync);
    }
  }

  /**
   * {@code @ZetaInvalidate}: every removal declared on one method. Each operation owns its target,
   * condition and policy, so one declaration's skipped condition or empty expansion can neither
   * suppress nor re-resolve another's removal.
   */
  @lombok.extern.slf4j.Slf4j
  record Invalidate(
    Method specific,
    ParameterBindings bindings,
    List<Operation> operations,
    boolean needsPreContext,
    boolean needsPostContext,
    boolean hasPrePhase,
    boolean hasPostPhase,
    boolean optional
  ) implements CachePlan {
    public Invalidate {
      operations = List.copyOf(operations);
    }

    static Invalidate of(Method specific, ParameterBindings bindings, List<Operation> operations, boolean optional) {
      boolean preContext = false;
      boolean postContext = false;
      boolean prePhase = false;
      boolean postPhase = false;
      for (Operation operation : operations) {
        preContext |= operation.needsPreContext();
        postContext |= operation.needsPostContext();
        prePhase |= operation.phase() != Phase.AFTER_BODY;
        postPhase |= operation.phase() != Phase.BEFORE_BODY;
      }
      return new Invalidate(specific, bindings, operations, preContext, postContext, prePhase, postPhase, optional);
    }

    /** The execution stage of one removal, decided once at resolution. */
    enum Phase {
      /** The condition and target run before the body; the removal is applied immediately. */
      BEFORE_BODY,
      /** The target resolves before the body and the removal is applied after it returns. */
      PRE_RESOLVE_POST_APPLY,
      /** The condition and target resolve after the body, against the one post-body context. */
      AFTER_BODY,
    }

    /**
     * One declared removal. The target is a sealed hierarchy rather than a set of mutually exclusive
     * nullable fields, so an operation always names exactly one removal shape.
     */
    record Operation(String cache, InvalidatePolicy policy, CompiledExpression condition, Phase phase, Target target) {
      static Operation single(String cache, Key key, InvalidatePolicy policy, CompiledExpression condition) {
        return create(cache, policy, condition, new Target.Single(key));
      }

      static Operation eachKey(
        String cache,
        KeyEncoding encoding,
        CompiledExpression expression,
        int slot,
        InvalidatePolicy policy,
        CompiledExpression condition
      ) {
        return create(cache, policy, condition, new Target.Each(encoding, expression, slot));
      }

      static Operation all(String cache, InvalidatePolicy policy, CompiledExpression condition) {
        return create(cache, policy, condition, Target.All.INSTANCE);
      }

      /**
       * Builds an operation and fixes its execution phase at plan time. Immediate invalidations run
       * before the body. A deferred invalidation runs after the body when its condition or target
       * reads invocation state; otherwise resolution happens before the body and only the apply is
       * deferred. Fixing the phase here keeps the interceptor free of per-call branching.
       *
       * @param cache     the logical cache namespace
       * @param policy    the invalidation policy
       * @param condition the guard expression
       * @param target    the removal shape
       * @return the operation with its phase settled
       */
      private static Operation create(
        String cache,
        InvalidatePolicy policy,
        CompiledExpression condition,
        Target target
      ) {
        Phase phase = policy.immediate()
          ? Phase.BEFORE_BODY
          : condition.absent() && !target.resultDependent()
            ? Phase.PRE_RESOLVE_POST_APPLY
            : Phase.AFTER_BODY;
        return new Operation(cache, policy, condition, phase, target);
      }

      boolean needsPreContext() {
        return switch (phase) {
          case BEFORE_BODY -> condition.needsContext() || target.needsContext();
          case PRE_RESOLVE_POST_APPLY -> target.needsContext();
          case AFTER_BODY -> false;
        };
      }

      boolean needsPostContext() {
        return phase == Phase.AFTER_BODY && (condition.needsContext() || target.needsContext());
      }

      /**
       * Resolves one phase.
       *
       * @return the target's resolved form, or {@code null} when nothing is to be removed
       */
      @Nullable
      Object resolve(Method specific, Object[] args, @Nullable ZetaEvaluationContext context, boolean afterBody) {
        return target.resolve(specific, args, context, afterBody);
      }

      /**
       * Apply one resolved removal. A {@code null} target is no work.
       *
       * @param zeta     the cache facade
       * @param resolved the resolved target, or {@code null} when the condition rejected it or the
       *                 target expanded to nothing
       */
      void apply(Zeta zeta, @Nullable Object resolved) {
        if (resolved != null) {
          target.apply(zeta, cache, policy, resolved);
        }
      }

      /**
       * Apply one resolved removal after the body. A {@code null} target is no work. A caller or
       * configuration rejection propagates like {@code @ZetaPut}'s; a transient engine failure is
       * logged and the committed business result stands.
       *
       * @param zeta     the cache facade
       * @param resolved the resolved target, or {@code null} when nothing is to be removed
       */
      void applyAfterBody(Zeta zeta, @Nullable Object resolved) {
        try {
          apply(zeta, resolved);
        } catch (ZetaModeException | IllegalArgumentException e) {
          throw e;
        } catch (RuntimeException e) {
          log.warn("[Zeta] cache invalidation failed for namespace '{}'; the business result is returned", cache, e);
        }
      }

      /** The mutually exclusive removal shapes one declaration can name. */
      sealed interface Target permits Target.All, Target.Single, Target.Each {
        /** Whether the target reads {@code #result}, so its phase cannot precede the body. */
        boolean resultDependent();

        /** Whether resolving the target reads invocation state. */
        boolean needsContext();

        /**
         * Resolves the removal at one phase.
         *
         * @return the resolved form this target applies, or {@code null} when nothing is to be removed
         */
        @Nullable
        Object resolve(Method specific, Object[] args, @Nullable ZetaEvaluationContext context, boolean afterBody);

        /**
         * Applies one non-null resolved form.
         *
         * @param zeta     the cache facade
         * @param cache    the logical cache namespace
         * @param policy   the invalidation policy
         * @param resolved the form this target's {@code resolve} produced
         */
        void apply(Zeta zeta, String cache, InvalidatePolicy policy, Object resolved);

        /** The table-wide clear: every entry of the namespace. */
        enum All implements Target {
          INSTANCE;

          @Override
          public boolean resultDependent() {
            return false;
          }

          @Override
          public boolean needsContext() {
            return false;
          }

          @Override
          public Object resolve(
            Method specific,
            Object[] args,
            @Nullable ZetaEvaluationContext context,
            boolean afterBody
          ) {
            return this;
          }

          @Override
          public void apply(Zeta zeta, String cache, InvalidatePolicy policy, Object resolved) {
            zeta.clearNamespace(cache, policy);
          }
        }

        /** One exact key resolved from the invocation. */
        record Single(Key key) implements Target {
          @Override
          public boolean resultDependent() {
            return key.resultDependent();
          }

          @Override
          public boolean needsContext() {
            return key.needsContext();
          }

          @Override
          public @Nullable Object resolve(
            Method specific,
            Object[] args,
            @Nullable ZetaEvaluationContext context,
            boolean afterBody
          ) {
            return afterBody ? key.resolveOrNull(args, context) : key.resolve(args, context);
          }

          @Override
          public void apply(Zeta zeta, String cache, InvalidatePolicy policy, Object resolved) {
            zeta.invalidate((String) resolved, policy);
          }
        }

        /** An argument or expression expanded into the keys it names. */
        record Each(KeyEncoding encoding, CompiledExpression expression, int slot) implements Target {
          private static final int MAX_EACH_KEYS = 1024;

          @Override
          public boolean resultDependent() {
            return slot < 0 && expression.referencesResult();
          }

          @Override
          public boolean needsContext() {
            return slot < 0 && expression.needsContext();
          }

          @Override
          public @Nullable Object resolve(
            Method specific,
            Object[] args,
            @Nullable ZetaEvaluationContext context,
            boolean afterBody
          ) {
            Object value = slot >= 0 ? (slot < args.length ? args[slot] : null) : expression.getValue(context);
            if (value == null) {
              if (!afterBody) {
                throw new IllegalStateException(
                  "cache key '" + expression.raw() + "' resolved to null for " + specific
                );
              }
              return null;
            }
            List<String> keys = expand(encoding, value, specific);
            return keys.isEmpty() ? null : keys;
          }

          @Override
          @SuppressWarnings("unchecked")
          public void apply(Zeta zeta, String cache, InvalidatePolicy policy, Object resolved) {
            zeta.invalidate((List<String>) resolved, policy);
          }

          /**
           * Expands an array or iterable target into distinct encoded keys, stopping at
           * {@link #MAX_EACH_KEYS}. Null elements are skipped and insertion order is preserved so the
           * resulting batch is stable. The cap turns an unbounded SpEL result into a fail-fast
           * declaration error instead of a large invalidation burst.
           *
           * @param encoding the namespace encoder
           * @param value    the array or iterable of raw keys
           * @param specific the method named in errors
           * @return the encoded, deduplicated keys
           * @throws IllegalStateException when the value is neither array nor iterable, or exceeds the cap
           */
          private static List<String> expand(KeyEncoding encoding, Object value, Method specific) {
            if (value instanceof Iterable<?> iterable) {
              Iterator<?> elements = iterable.iterator();
              if (!elements.hasNext()) {
                return List.of();
              }
              Object first = elements.next();
              if (!elements.hasNext()) {
                return first == null ? List.of() : List.of(encoding.encodeSingle(first));
              }
              Set<String> keys = new LinkedHashSet<>(
                iterable instanceof Collection<?> collection
                  ? CollectionSizing.capacity(Math.min(collection.size(), MAX_EACH_KEYS))
                  : 16
              );
              if (first != null) {
                keys.add(encoding.encodeSingle(first));
              }
              while (elements.hasNext()) {
                Object element = elements.next();
                if (element != null && keys.add(encoding.encodeSingle(element)) && keys.size() > MAX_EACH_KEYS) {
                  throw tooManyKeys(specific);
                }
              }
              return List.copyOf(keys);
            }
            if (!value.getClass().isArray()) {
              throw new IllegalStateException(
                "eachKey requires an array or Iterable, got " + value.getClass().getName()
              );
            }
            int length = Array.getLength(value);
            if (length == 0) {
              return List.of();
            }
            if (length == 1) {
              Object element = Array.get(value, 0);
              return element == null ? List.of() : List.of(encoding.encodeSingle(element));
            }
            Set<String> keys = new LinkedHashSet<>(CollectionSizing.capacity(Math.min(length, MAX_EACH_KEYS)));
            for (int i = 0; i < length; i++) {
              Object element = Array.get(value, i);
              if (element != null && keys.add(encoding.encodeSingle(element)) && keys.size() > MAX_EACH_KEYS) {
                throw tooManyKeys(specific);
              }
            }
            return List.copyOf(keys);
          }

          private static IllegalStateException tooManyKeys(Method specific) {
            return new IllegalStateException(
              "eachKey on " + specific + " expands more than " + MAX_EACH_KEYS + " keys; split the batch"
            );
          }
        }
      }
    }
  }

  /**
   * The one read-policy shape every annotation read uses. {@code callerRunsLoader} keeps the
   * elected first-miss leader on the invocation thread, so {@code MethodInvocation.proceed()}
   * preserves the caller's transaction, security context and MDC.
   */
  static GetPolicy readPolicy(LongSupplier hardTtlMs, LongSupplier softTtlMs) {
    return new GetPolicy(hardTtlMs, softTtlMs, StalePolicy.SOFT_REFRESH, true, true);
  }

  /**
   * The resolved shape of a method's key. Four mutually exclusive forms: a fixed physical key, one
   * argument value, the canonical argument-list key, or a SpEL expression. The shape is chosen once
   * at resolution, so a plain hit never reclassifies it.
   */
  sealed interface Key permits Key.Constant, Key.SingleArgument, Key.DefaultArguments, Key.Expression {
    /**
     * Whether the key reads the post-body {@code #result}. A result-dependent key cannot be resolved
     * before the body, so a deferred removal that uses one runs after the body and against the one
     * post-body context instead of a pre-body snapshot.
     */
    boolean resultDependent();

    /** Whether resolution needs a SpEL context. */
    boolean needsContext();

    /**
     * The physical key, or {@code null} when the selected value is {@code null}.
     *
     * <p>A {@code null} argument yields {@code null} in every shape, including the canonical
     * argument list: the caller decides whether that is fatal (a read) or a no-op (an after-body
     * removal).
     */
    @Nullable
    String resolveOrNull(Object[] args, @Nullable ZetaEvaluationContext context);

    /**
     * The physical key.
     *
     * @throws IllegalStateException when the key resolves to {@code null}
     */
    String resolve(Object[] args, @Nullable ZetaEvaluationContext context);

    static Key of(KeyEncoding encoding, Method method, CompiledExpression compiled, ParameterBindings bindings) {
      String constantKey = constantKeyOf(encoding, method, compiled);
      if (constantKey != null) {
        return new Constant(constantKey);
      }
      int index = compiled.directArgumentSlot(method, bindings, false);
      if (index >= 0) {
        return new SingleArgument(encoding, method, index, compiled);
      }
      if (compiled.absent() || compiled.wholeRootArgs()) {
        if (encoding.format() == KeyFormat.PLAIN) {
          String reason = method.isVarArgs()
            ? "cannot encode a varargs argument list"
            : "cannot use a multi-argument default key";
          throw new IllegalStateException(
            "PLAIN namespace '" +
              encoding.namespace() +
              "' " +
              reason +
              " on " +
              method +
              "; declare one explicit key or use TYPED"
          );
        }
        return new DefaultArguments(encoding, method, compiled);
      }
      return new Expression(encoding, method, compiled);
    }

    /**
     * Folds a startup-constant key into its physical form when it needs no invocation state. An
     * absent expression on a no-arg method folds to the namespace default; a pure expression is
     * evaluated once here so a non-encoding or null result fails at startup instead of on the
     * first invocation.
     *
     * @return the folded key, or {@code null} when the key must be resolved per invocation
     */
    private static @Nullable String constantKeyOf(KeyEncoding encoding, Method method, CompiledExpression compiled) {
      if ((compiled.absent() || compiled.wholeRootArgs()) && method.getParameterCount() == 0) {
        return encoding.encodeArgs(new Object[0]);
      }
      if (!compiled.isPureConstant()) {
        return null;
      }
      Object value;
      try {
        value = compiled.getValue(null);
      } catch (RuntimeException e) {
        throw new IllegalStateException(
          "cache key '" + compiled.raw() + "' on " + method + " cannot be evaluated at startup",
          e
        );
      }
      if (value == null) {
        throw new IllegalStateException("cache key '" + compiled.raw() + "' resolved to null for " + method);
      }
      return encoding.encodeSingle(value);
    }

    private static String nullArgumentMessage(int index, @Nullable String raw, Method method) {
      String prefix = "null argument " + index + " for " + method + ": ";
      return raw == null
        ? prefix + "pass a non-null argument or declare an explicit key"
        : prefix +
          "key = \"" +
          raw +
          "\" requires every argument to be non-null; pass a non-null argument or declare an expression that tolerates null";
    }

    /** A physical key folded once at resolution. */
    record Constant(String physicalKey) implements Key {
      @Override
      public boolean resultDependent() {
        return false;
      }

      @Override
      public boolean needsContext() {
        return false;
      }

      @Override
      public @Nullable String resolveOrNull(Object[] args, @Nullable ZetaEvaluationContext context) {
        return physicalKey;
      }

      @Override
      public String resolve(Object[] args, @Nullable ZetaEvaluationContext context) {
        return physicalKey;
      }
    }

    /** One selected argument, encoded as the namespace's single-value key. */
    record SingleArgument(
      KeyEncoding encoding,
      Method method,
      int argumentIndex,
      CompiledExpression expression
    ) implements Key {
      @Override
      public boolean resultDependent() {
        return false;
      }

      @Override
      public boolean needsContext() {
        return false;
      }

      @Override
      public @Nullable String resolveOrNull(Object[] args, @Nullable ZetaEvaluationContext context) {
        if (argumentIndex >= args.length) {
          return null;
        }
        Object argument = args[argumentIndex];
        return argument == null ? null : encoding.encodeSingle(argument);
      }

      @Override
      public String resolve(Object[] args, @Nullable ZetaEvaluationContext context) {
        String resolved = resolveOrNull(args, context);
        if (resolved != null) {
          return resolved;
        }
        if (expression.absent()) {
          throw new IllegalStateException(Key.nullArgumentMessage(argumentIndex, null, method));
        }
        throw new IllegalStateException("cache key '" + expression.raw() + "' resolved to null for " + method);
      }
    }

    /** The canonical argument-list key: every parameter contributes, in order. */
    record DefaultArguments(KeyEncoding encoding, Method method, CompiledExpression expression) implements Key {
      @Override
      public boolean resultDependent() {
        return false;
      }

      @Override
      public boolean needsContext() {
        return false;
      }

      @Override
      public @Nullable String resolveOrNull(Object[] args, @Nullable ZetaEvaluationContext context) {
        for (Object argument : args) {
          if (argument == null) {
            return null;
          }
        }
        return encoding.encodeArgs(args);
      }

      @Override
      public String resolve(Object[] args, @Nullable ZetaEvaluationContext context) {
        String resolved = resolveOrNull(args, context);
        if (resolved != null) {
          return resolved;
        }
        for (int i = 0; i < args.length; i++) {
          if (args[i] == null) {
            throw new IllegalStateException(Key.nullArgumentMessage(i, expression.raw(), method));
          }
        }
        throw new IllegalStateException("cache key '" + expression.raw() + "' resolved to null for " + method);
      }
    }

    /** A SpEL expression resolved against the invocation's context. */
    record Expression(KeyEncoding encoding, Method method, CompiledExpression expression) implements Key {
      @Override
      public boolean resultDependent() {
        return expression.referencesResult();
      }

      @Override
      public boolean needsContext() {
        return expression.needsContext();
      }

      @Override
      public @Nullable String resolveOrNull(Object[] args, @Nullable ZetaEvaluationContext context) {
        Object value = expression.getValue(context);
        return value == null ? null : encoding.encodeSingle(value);
      }

      @Override
      public String resolve(Object[] args, @Nullable ZetaEvaluationContext context) {
        String resolved = resolveOrNull(args, context);
        if (resolved != null) {
          return resolved;
        }
        throw new IllegalStateException("cache key '" + expression.raw() + "' resolved to null for " + method);
      }
    }
  }

  /**
   * One TTL attribute. A constant is a folded millisecond value with no expression; a dynamic
   * attribute is the compiled expression evaluated lazily.
   *
   * <p>The declaration vocabulary: {@code 0} inherits the configured default, {@code -1} disables
   * a soft window (and inherits a hard one, which has no "off" state), every positive value is used
   * exactly as written, and {@code Long.MAX_VALUE} never expires. Any other negative literal fails
   * resolution; a negative dynamic result fails evaluation. Both rules exist because a silent
   * fallback would turn a mistyped TTL into the configured default.
   */
  sealed interface Ttl permits Ttl.Constant, Ttl.Dynamic {
    /** The disable sentinel: a soft window is skipped where allowed, otherwise the default is used. */
    long DISABLE = -1L;

    boolean isConstant();

    /** Whether evaluating this TTL reads invocation state, so a phase context is required. */
    boolean needsContext();

    /** Whether this is the constant {@link Long#MAX_VALUE} (permanent). */
    boolean isPermanent();

    /** Whether this TTL reads {@code #result}. */
    boolean resultDependent();

    /** This instance when it is constant, else {@code null}. */
    @Nullable
    LongSupplier constantSupplier();

    /**
     * Evaluate this TTL against the context of the current phase. A constant returns its folded
     * value without a context; an absent dynamic result clamps to {@code 0} (use the configured
     * default). A negative, floating-point or out-of-range result is a configuration error and is
     * thrown, not silently degraded.
     *
     * <p>An integral result must be exactly representable as a {@code long}: a {@code BigInteger} or
     * {@code BigDecimal} outside the range fails instead of wrapping, because a wrapped negative
     * value would either disable a window or throw "must be &gt;= -1" for a value the caller never
     * wrote.
     */
    long evaluate(@Nullable ZetaEvaluationContext context);

    static Ttl of(
      String role,
      @Nullable String raw,
      SpelExpressionParser parser,
      EvaluationContext emptyContext,
      Method method,
      ParameterBindings bindings,
      boolean resultAllowed,
      boolean disableAllowed
    ) {
      String literal = raw == null ? null : raw.strip();
      if (literal == null || literal.isEmpty()) {
        return new Constant(0L);
      }
      String numeric =
        literal.endsWith("L") || literal.endsWith("l") ? literal.substring(0, literal.length() - 1) : literal;
      if (isDigits(numeric)) {
        try {
          return constant(role, Long.parseLong(numeric), literal, method, disableAllowed);
        } catch (NumberFormatException overflow) {
          throw new IllegalStateException(
            role + " literal '" + literal + "' on " + method + " exceeds the long range",
            overflow
          );
        }
      }
      CompiledExpression compiled = CompiledExpression.of(
        literal,
        parser,
        emptyContext,
        role,
        method,
        bindings,
        resultAllowed
      );
      if (!compiled.isPureConstant()) {
        return new Dynamic(compiled, role, method, disableAllowed);
      }
      return new Constant(fold(role, literal, method, compiled, disableAllowed));
    }

    /**
     * Evaluates a context-free expression once at resolution so the stored attribute is a constant.
     *
     * @param role           the attribute role for diagnostics
     * @param literal        the stripped attribute text
     * @param method         the annotated method
     * @param compiled       the context-free expression
     * @param disableAllowed whether {@code -1} keeps its disable meaning
     * @return the folded millisecond value
     * @throws IllegalStateException if the expression fails or produces an invalid TTL
     */
    private static long fold(
      String role,
      String literal,
      Method method,
      CompiledExpression compiled,
      boolean disableAllowed
    ) {
      try {
        return new Dynamic(compiled, role, method, disableAllowed).foldValue();
      } catch (IllegalStateException invalid) {
        throw invalid;
      } catch (RuntimeException failure) {
        throw new IllegalStateException(
          role + " '" + literal + "' on " + method + " cannot be evaluated at startup",
          failure
        );
      }
    }

    /**
     * Whether text is an optionally signed ASCII integer literal. This distinguishes an
     * out-of-range literal from a SpEL expression when parsing fails, so an oversized number is
     * reported as a range error instead of being compiled as code.
     */
    private static boolean isDigits(String text) {
      int length = text.length();
      if (length == 0) {
        return false;
      }
      int start = (text.charAt(0) == '+' || text.charAt(0) == '-') ? 1 : 0;
      if (start == length) {
        return false;
      }
      for (int i = start; i < length; i++) {
        char ch = text.charAt(i);
        if (ch < '0' || ch > '9') {
          return false;
        }
      }
      return true;
    }

    /**
     * Validates and folds a numeric TTL literal. {@code -1} becomes the inherited value unless the
     * attribute allows a disable sentinel; other negative values are rejected at declaration time.
     * A positive value must fit the packed entry fields, so an unencodable literal is reported
     * where it was written.
     *
     * @param role           the attribute name for errors
     * @param millis         the parsed literal
     * @param literal        the original text for errors
     * @param method         the declaring method for errors
     * @param disableAllowed whether {@code -1} is a valid disable value here
     * @return the constant TTL
     * @throws IllegalStateException for a negative literal below {@code -1} or an out-of-range value
     */
    private static Ttl constant(String role, long millis, String literal, Method method, boolean disableAllowed) {
      if (millis < DISABLE) {
        throw new IllegalStateException(
          role +
            " '" +
            literal +
            "' on " +
            method +
            " is negative; only -1 and >= 0 are accepted (use a long literal such as 2592000000L)"
        );
      }
      if (millis == DISABLE) {
        return new Constant(disableAllowed ? DISABLE : 0L);
      }
      try {
        CacheEntry.requireEncodableTtl(millis, role);
      } catch (IllegalArgumentException e) {
        throw new IllegalStateException(role + " '" + literal + "' on " + method + " is out of range: " + millis, e);
      }
      return new Constant(millis);
    }

    /** A folded millisecond value, resolved once at bean creation. */
    record Constant(long millis) implements Ttl, LongSupplier {
      @Override
      public boolean isConstant() {
        return true;
      }

      @Override
      public boolean needsContext() {
        return false;
      }

      @Override
      public boolean isPermanent() {
        return millis == Long.MAX_VALUE;
      }

      @Override
      public boolean resultDependent() {
        return false;
      }

      @Override
      public @Nullable LongSupplier constantSupplier() {
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

    /** A compiled expression evaluated lazily against the current phase's context. */
    record Dynamic(CompiledExpression expression, String role, Method method, boolean disableAllowed) implements Ttl {
      @Override
      public boolean isConstant() {
        return false;
      }

      @Override
      public boolean needsContext() {
        return expression.needsContext();
      }

      @Override
      public boolean isPermanent() {
        return false;
      }

      @Override
      public boolean resultDependent() {
        return expression.referencesResult();
      }

      @Override
      public @Nullable LongSupplier constantSupplier() {
        return null;
      }

      @Override
      public long evaluate(@Nullable ZetaEvaluationContext context) {
        Number value = expression.getValue(context, Number.class);
        return value == null ? 0L : toMillis(value);
      }

      /**
       * Folds a context-free expression at resolution; a {@code null} result is a declaration
       * error because only a dynamic expression may inherit the default TTL at evaluation time.
       *
       * @return the folded millisecond value
       */
      long foldValue() {
        Number value = expression.getValue(null, Number.class);
        if (value == null) {
          throw invalidValue("null", "must be an integral number of milliseconds");
        }
        return toMillis(value);
      }

      private long toMillis(Number value) {
        long evaluated;
        if (value instanceof Double || value instanceof Float) {
          throw invalidValue(value, "must be an integral number of milliseconds");
        } else if (value instanceof BigInteger integer) {
          try {
            evaluated = integer.longValueExact();
          } catch (ArithmeticException overflow) {
            throw invalidValue(value, "exceeds the long range");
          }
        } else if (value instanceof BigDecimal decimal) {
          try {
            evaluated = decimal.longValueExact();
          } catch (ArithmeticException invalid) {
            throw invalidValue(value, "must be an integral number of milliseconds within the long range");
          }
        } else {
          evaluated = value.longValue();
        }
        if (evaluated == DISABLE) {
          return disableAllowed ? DISABLE : 0L;
        }
        if (evaluated < 0L) {
          throw invalidValue(value, "must be >= -1");
        }
        CacheEntry.requireEncodableTtl(evaluated, role);
        return evaluated;
      }

      private IllegalStateException invalidValue(Object value, String requirement) {
        return new IllegalStateException(
          role + " evaluated to " + value + " for " + method + "; the value " + requirement
        );
      }
    }
  }
}
