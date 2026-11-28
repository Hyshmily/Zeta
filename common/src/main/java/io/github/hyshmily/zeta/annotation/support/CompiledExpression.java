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
package io.github.hyshmily.zeta.annotation.support;

import io.github.hyshmily.zeta.util.Internal;
import java.lang.reflect.Method;
import java.util.Objects;
import lombok.Getter;
import org.jspecify.annotations.Nullable;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.SpelNode;
import org.springframework.expression.spel.ast.*;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * A SpEL expression compiled once per method, or the shared {@link #ABSENT} marker.
 *
 * <p>The expression is parsed at resolution time so syntax errors fail bean creation early. Static
 * AST inspection also validates variable references: every {@code #name} must be {@code #result}
 * (when the phase binds a result), {@code #this}/{@code #root}, or a parameter declared by the
 * method. A misspelt variable is treated as a configuration error rather than a runtime {@code null}.
 *
 * <p>Evaluation is a pure function of the invocation. Assignment, increment/decrement and function
 * references are rejected while parsing — the contexts this layer evaluates with are read-only and
 * register no functions — so a state-mutating typo fails bean creation instead of the first call.
 * Runtime failures propagate to the caller instead of being swallowed into default answers.
 */
@Internal
public final class CompiledExpression {

  /** The shared absent expression marker. */
  public static final CompiledExpression ABSENT = new CompiledExpression(
    null,
    null,
    null,
    false,
    false,
    false,
    false,
    null
  );

  private final @Nullable String raw;
  private final @Nullable Expression expression;

  /** The read-only context of the owning resolver, used when a phase passes none. */
  private final @Nullable EvaluationContext emptyContext;

  private final boolean referencesResult;
  private final boolean referencesInvocationContext;

  /** Whether the AST is free of every node that can only be evaluated at runtime.
   * -- GETTER --
   *  Returns whether this expression is present and evaluates from literals and operators alone.
   * <p>A pure constant reads no variable, bean, type, constructor, function, property, method or
   * collection element of the invocation, so resolution can evaluate it once and store the result.
   *
   * @return {@code true} if the expression can be evaluated at resolution time
   */
  @Getter
  private final boolean pureConstant;

  private final boolean wholeRootArgs;
  private final @Nullable String singleVariable;

  /**
   * Creates a compiled expression instance with precomputed static analysis facts.
   *
   * @param raw                         the stripped expression text, or {@code null} when absent
   * @param expression                  the parsed SpEL expression, or {@code null} when absent
   * @param emptyContext                the owning resolver's context-free context, or {@code null}
   *                                    for {@link #ABSENT}
   * @param referencesResult            whether {@code #result} is referenced
   * @param referencesInvocationContext whether non-result invocation state or beans are referenced
   * @param pureConstant                whether the AST holds only literals and operators
   * @param wholeRootArgs               whether the entire AST is exactly {@code #root.args}
   * @param singleVariable              the variable name if the entire AST is a single variable
   *                                    reference, or {@code null} otherwise
   */
  private CompiledExpression(
    @Nullable String raw,
    @Nullable Expression expression,
    @Nullable EvaluationContext emptyContext,
    boolean referencesResult,
    boolean referencesInvocationContext,
    boolean pureConstant,
    boolean wholeRootArgs,
    @Nullable String singleVariable
  ) {
    this.raw = raw;
    this.expression = expression;
    this.emptyContext = emptyContext;
    this.referencesResult = referencesResult;
    this.referencesInvocationContext = referencesInvocationContext;
    this.pureConstant = pureConstant;
    this.wholeRootArgs = wholeRootArgs;
    this.singleVariable = singleVariable;
  }

  /**
   * Compiles and statically validates one annotation expression attribute.
   *
   * @param raw           the raw attribute text; {@code null} or blank yields {@link #ABSENT}
   * @param parser        the shared SpEL parser
   * @param emptyContext  the owning resolver's context-free context
   * @param role          the attribute name used in diagnostic messages
   * @param method        the annotated method
   * @param bindings      the method's parameter slot table
   * @param resultAllowed whether {@code #result} is allowed in this evaluation phase
   * @return the compiled expression, or {@link #ABSENT} when {@code raw} is {@code null} or blank
   * @throws IllegalStateException if the expression cannot be parsed or references an invalid variable
   */
  public static CompiledExpression of(
    @Nullable String raw,
    SpelExpressionParser parser,
    EvaluationContext emptyContext,
    String role,
    Method method,
    ParameterBindings bindings,
    boolean resultAllowed
  ) {
    Objects.requireNonNull(parser, "parser must not be null");
    Objects.requireNonNull(emptyContext, "emptyContext must not be null");
    Objects.requireNonNull(role, "role must not be null");
    Objects.requireNonNull(method, "method must not be null");
    Objects.requireNonNull(bindings, "bindings must not be null");

    String text = raw == null ? null : raw.strip();
    if (text == null || text.isEmpty()) {
      return ABSENT;
    }

    Expression expression = parse(text, parser, role, method);
    if (expression instanceof SpelExpression spel) {
      SpelNode root = spel.getAST();
      AstInspector inspector = new AstInspector(text, role, method, bindings, resultAllowed);
      inspector.inspect(root, false, false);
      String singleVariable = root instanceof VariableReference ref ? variableName(ref) : null;
      boolean pure =
        !inspector.referencesResult && !inspector.referencesInvocationContext && !inspector.referencesRuntimeCall;
      return new CompiledExpression(
        text,
        expression,
        emptyContext,
        inspector.referencesResult,
        inspector.referencesInvocationContext,
        pure,
        isWholeRootArgs(root),
        singleVariable
      );
    }

    // Non-SpelExpression fallback: conservatively assume invocation context is required.
    return new CompiledExpression(text, expression, emptyContext, false, true, false, false, null);
  }

  /**
   * Returns whether this expression is present (i.e., configured with non-blank text).
   *
   * @return {@code true} if an expression is present
   */
  public boolean present() {
    return expression != null;
  }

  /**
   * Returns whether this expression is absent so every query behaves as if unconfigured.
   *
   * @return {@code true} if no expression is configured
   */
  public boolean absent() {
    return expression == null;
  }

  /**
   * Returns the stripped attribute text.
   *
   * @return the expression text, or {@code null} when absent
   */
  public @Nullable String raw() {
    return raw;
  }

  /**
   * Returns whether evaluating this expression requires a per-invocation {@link EvaluationContext}.
   *
   * @return {@code true} if the expression is present and depends on {@code #result}, invocation
   *         state, or bean references
   */
  public boolean needsContext() {
    return expression != null && (referencesResult || referencesInvocationContext);
  }

  /**
   * Returns whether this expression references {@code #result}.
   *
   * @return {@code true} if {@code #result} appears in the AST
   */
  public boolean referencesResult() {
    return referencesResult;
  }

  /**
   * Returns whether this expression depends on invocation state other than {@code #result}, such as
   * a parameter variable, {@code #root}/{@code #this}, an implicit root property/method/indexer
   * access, or a {@code @beanName} reference.
   *
   * @return {@code true} if non-result invocation context is referenced
   */
  public boolean referencesInvocationContext() {
    return referencesInvocationContext;
  }

  /**
   * Returns whether the entire AST is exactly {@code #root.args}.
   *
   * @return {@code true} if the expression is precisely {@code #root.args}
   */
  public boolean wholeRootArgs() {
    return wholeRootArgs;
  }

  /**
   * Resolves the zero-based parameter slot selected directly as a single value by this key
   * expression, or {@code -1} if the expression does not map to a single parameter slot.
   *
   * <p>When the expression is absent, {@code eachKey} is {@code false}, and the method has a single
   * non-varargs parameter, slot {@code 0} is selected implicitly.
   *
   * @param method   the annotated method
   * @param bindings the method's parameter slot table
   * @param eachKey  whether the key is expanded to one key per element
   * @return the zero-based parameter slot, or {@code -1}
   */
  public int singleArgumentSlot(Method method, ParameterBindings bindings, boolean eachKey) {
    int slot = singleVariable == null ? -1 : bindings.slotOf(singleVariable);
    if (slot < 0 && !eachKey && absent() && method.getParameterCount() == 1 && !method.isVarArgs()) {
      return 0;
    }
    return slot;
  }

  /**
   * Resolves the zero-based parameter slot that may be read directly without SpEL evaluation,
   * excluding a varargs tail parameter which must go through SpEL so the expanded array is preserved.
   *
   * @param method   the annotated method
   * @param bindings the method's parameter slot table
   * @param eachKey  whether the key is expanded to one key per element
   * @return the zero-based parameter slot, or {@code -1}
   */
  public int directArgumentSlot(Method method, ParameterBindings bindings, boolean eachKey) {
    int slot = singleArgumentSlot(method, bindings, eachKey);
    return slot >= 0 && method.isVarArgs() && slot == method.getParameterCount() - 1 ? -1 : slot;
  }

  /**
   * Evaluates this expression against the given context.
   *
   * @param context the phase context, or {@code null} when the expression reads no invocation state
   * @return the evaluated value, or {@code null} if absent or the expression evaluates to {@code null}
   */
  public @Nullable Object getValue(@Nullable EvaluationContext context) {
    Expression expr = this.expression;
    return expr == null ? null : expr.getValue(resolveContext(context));
  }

  /**
   * Evaluates this expression and coerces the result to the required type.
   *
   * @param context the phase context, or {@code null} when the expression reads no invocation state
   * @param type    the required result type
   * @param <T>     the result type
   * @return the evaluated value, or {@code null} if absent or the expression evaluates to {@code null}
   */
  public <T> @Nullable T getValue(@Nullable EvaluationContext context, Class<T> type) {
    Expression expr = this.expression;
    return expr == null ? null : expr.getValue(resolveContext(context), type);
  }

  /**
   * Evaluates an admission guard: an absent guard admits, while a {@code null} or non-true answer
   * denies admission.
   *
   * @param context the phase context, or {@code null} when the guard reads no invocation state
   * @return {@code true} if the guarded operation may proceed
   */
  public boolean evalAdmission(@Nullable EvaluationContext context) {
    Expression expr = this.expression;
    return expr == null || evaluatesTrue(expr, context);
  }

  /**
   * Evaluates a veto guard: an absent guard does not veto, and a {@code null} or non-true answer
   * does not veto either.
   *
   * @param context the phase context, or {@code null} when the guard reads no invocation state
   * @return {@code true} if the guard vetoes the operation
   */
  public boolean evalVeto(@Nullable EvaluationContext context) {
    Expression expr = this.expression;
    return expr != null && evaluatesTrue(expr, context);
  }

  /**
   * Evaluates a non-null expression as a boolean condition.
   *
   * @param expression the parsed expression to evaluate
   * @param context    the evaluation context, or {@code null} when the expression reads no
   *                   invocation state
   * @return {@code true} only if the expression evaluates to {@link Boolean#TRUE}
   */
  private boolean evaluatesTrue(Expression expression, @Nullable EvaluationContext context) {
    return Boolean.TRUE.equals(expression.getValue(resolveContext(context), Boolean.class));
  }

  /**
   * Returns the provided context or falls back to the owning resolver's context-free context.
   *
   * @param context the nullable evaluation context
   * @return a non-null evaluation context
   */
  private EvaluationContext resolveContext(@Nullable EvaluationContext context) {
    return context != null ? context : Objects.requireNonNull(emptyContext, "emptyContext must not be null");
  }

  /**
   * Parses raw SpEL text and wraps parser failures in a descriptive {@link IllegalStateException}.
   *
   * @param text   the stripped SpEL text
   * @param parser the SpEL parser
   * @param role   the attribute name for diagnostics
   * @param method the annotated method
   * @return the parsed {@link Expression}
   * @throws IllegalStateException if parsing fails
   */
  private static Expression parse(String text, SpelExpressionParser parser, String role, Method method) {
    try {
      return parser.parseExpression(text);
    } catch (RuntimeException e) {
      throw new IllegalStateException(role + " '" + text + "' cannot be parsed for " + method, e);
    }
  }

  /**
   * Checks whether the root AST node represents the exact expression {@code #root.args}.
   *
   * @param root the AST root node
   * @return {@code true} if the AST is strictly {@code #root.args}
   */
  private static boolean isWholeRootArgs(SpelNode root) {
    return (
      root instanceof CompoundExpression compound &&
      compound.getChildCount() == 2 &&
      compound.getChild(0) instanceof VariableReference variable &&
      "root".equals(variableName(variable)) &&
      compound.getChild(1) instanceof PropertyOrFieldReference property &&
      "args".equals(property.getName())
    );
  }

  /**
   * Extracts the unprefixed variable name from a {@link VariableReference} AST node.
   *
   * @param reference the variable reference node
   * @return the variable name without the leading {@code '#'}
   */
  private static String variableName(VariableReference reference) {
    String ast = reference.toStringAST();
    return ast.startsWith("#") ? ast.substring(1) : ast;
  }

  /**
   * Parses the zero-based positional index of a canonical {@code pN}/{@code aN} variable name.
   *
   * @param variable the unprefixed variable name
   * @return the non-negative positional index, or {@code -1} if {@code variable} is not a valid
   *         canonical positional name (e.g., non-numeric, leading zeros, or integer overflow)
   */
  static int argumentIndex(String variable) {
    int length = variable.length();
    if (length < 2) {
      return -1;
    }
    char prefix = variable.charAt(0);
    if (prefix != 'p' && prefix != 'a') {
      return -1;
    }
    return CanonicalInt.parse(variable, 1, length);
  }

  /**
   * Traverses a SpEL AST to validate variable references and record context dependency facts.
   */
  private static final class AstInspector {

    private final String text;
    private final String role;
    private final Method method;
    private final ParameterBindings bindings;
    private final boolean resultAllowed;

    private boolean referencesResult;
    private boolean referencesInvocationContext;
    private boolean referencesRuntimeCall;

    /**
     * Creates an inspector bound to one method attribute.
     *
     * @param text          the stripped expression text
     * @param role          the attribute name used in diagnostics
     * @param method        the annotated method
     * @param bindings      the method's parameter slot table
     * @param resultAllowed whether {@code #result} is permitted in this phase
     */
    private AstInspector(String text, String role, Method method, ParameterBindings bindings, boolean resultAllowed) {
      this.text = text;
      this.role = role;
      this.method = method;
      this.bindings = bindings;
      this.resultAllowed = resultAllowed;
    }

    /**
     * Recursively inspects a SpEL AST node and its children.
     *
     * @param node       the current AST node
     * @param inElement  whether traversal is currently inside a collection selection or projection,
     *                   where {@code #this} and unqualified accesses target the current element
     * @param onReceiver whether this node is a chained member (property, method, indexer, etc.)
     *                   invoked on a preceding receiver within a {@link CompoundExpression}
     */
    private void inspect(SpelNode node, boolean inElement, boolean onReceiver) {
      if (node instanceof InlineMap) {
        // An unquoted key is a PropertyOrFieldReference the evaluator reads by name; it never
        // touches #root, so it must neither mark the map dynamic nor disqualify it from folding.
        for (int i = 0; i < node.getChildCount(); i++) {
          SpelNode child = node.getChild(i);
          if ((i & 1) == 0 && child instanceof PropertyOrFieldReference) {
            continue;
          }
          inspect(child, inElement, false);
        }
        return;
      }
      if (node instanceof FunctionReference) {
        throw new IllegalStateException(
          role + " '" + text + "' on " + method + " cannot call a SpEL function: no functions are registered"
        );
      }
      if (node instanceof Assign || node instanceof OpInc || node instanceof OpDec) {
        // Every context this layer evaluates with reports isAssignmentEnabled() == false, so these
        // nodes could only fail at the first evaluation.
        throw new IllegalStateException(
          role + " '" + text + "' on " + method + " cannot mutate state: cache expressions are read-only"
        );
      }
      if (node instanceof VariableReference reference) {
        inspectVariable(reference, inElement);
      } else if (node instanceof BeanReference) {
        referencesInvocationContext = true;
      } else if (isRuntimeCall(node)) {
        referencesRuntimeCall = true;
        if (!inElement && !onReceiver && appliesToRoot(node)) {
          referencesInvocationContext = true;
        }
      } else if (!inElement && !onReceiver && appliesToRoot(node)) {
        // An unqualified property/method/indexer/selection/projection targets #root.
        referencesInvocationContext = true;
      }

      boolean compound = node instanceof CompoundExpression;
      boolean childInElement = inElement || node instanceof Selection || node instanceof Projection;
      for (int i = 0; i < node.getChildCount(); i++) {
        inspect(node.getChild(i), childInElement, compound && i > 0);
      }
    }

    /**
     * Validates a variable reference and updates dependency flags.
     *
     * @param reference the variable reference AST node
     * @param inElement whether the reference occurs inside a selection or projection
     */
    private void inspectVariable(VariableReference reference, boolean inElement) {
      String variable = variableName(reference);
      if ("result".equals(variable)) {
        if (!resultAllowed) {
          throw new IllegalStateException(
            role + " '" + text + "' on " + method + " cannot use #result: this phase does not bind a method result"
          );
        }
        referencesResult = true;
      } else if ("this".equals(variable) && inElement) {
        // Inside a selection or projection, #this denotes the current element rather than #root.
      } else if ("root".equals(variable) || "this".equals(variable)) {
        referencesInvocationContext = true;
      } else {
        referencesInvocationContext = true;
        if (bindings.slotOf(variable) < 0) {
          throw unknownVariable(variable);
        }
      }
    }

    /**
     * Builds a diagnostic exception explaining why an unknown variable cannot be resolved.
     *
     * @param variable the unresolved variable name
     * @return an {@link IllegalStateException} with actionable guidance
     */
    private IllegalStateException unknownVariable(String variable) {
      int index = argumentIndex(variable);
      if (index >= bindings.parameterCount()) {
        return new IllegalStateException(
          role +
            " '" +
            text +
            "' on " +
            method +
            " addresses parameter " +
            index +
            ", but the method declares " +
            bindings.parameterCount()
        );
      }
      if (bindings.parameterCount() > 0 && !bindings.hasParameterNames()) {
        return new IllegalStateException(
          role +
            " '" +
            text +
            "' on " +
            method +
            " references parameter '#" +
            variable +
            "', but parameter names are not available; compile with -parameters or use #p0/#a0"
        );
      }
      return new IllegalStateException(
        role + " '" + text + "' on " + method + " references unknown variable '#" + variable + "'"
      );
    }

    /**
     * Returns whether the given AST node can only be evaluated at runtime, even when its children
     * are literals: it reads a type, a constructor, a member, a function, or a collection element of
     * a receiver rather than this expression's own operands.
     *
     * @param node the AST node to test
     * @return {@code true} if the node disqualifies the expression from constant folding
     */
    private static boolean isRuntimeCall(SpelNode node) {
      return (
        node instanceof TypeReference ||
        node instanceof ConstructorReference ||
        node instanceof MethodReference ||
        node instanceof PropertyOrFieldReference ||
        node instanceof Selection ||
        node instanceof Projection
      );
    }

    /**
     * Returns whether the given AST node implicitly evaluates against {@code #root} when not
     * attached to an explicit receiver or element scope.
     *
     * @param node the AST node to test
     * @return {@code true} if the node accesses members or elements of the current target
     */
    private static boolean appliesToRoot(SpelNode node) {
      return (
        node instanceof PropertyOrFieldReference ||
        node instanceof MethodReference ||
        node instanceof Indexer ||
        node instanceof Selection ||
        node instanceof Projection
      );
    }
  }
}
