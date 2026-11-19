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
import org.jspecify.annotations.Nullable;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.SpelNode;
import org.springframework.expression.spel.ast.Assign;
import org.springframework.expression.spel.ast.BeanReference;
import org.springframework.expression.spel.ast.CompoundExpression;
import org.springframework.expression.spel.ast.ConstructorReference;
import org.springframework.expression.spel.ast.FunctionReference;
import org.springframework.expression.spel.ast.Indexer;
import org.springframework.expression.spel.ast.MethodReference;
import org.springframework.expression.spel.ast.OpDec;
import org.springframework.expression.spel.ast.OpInc;
import org.springframework.expression.spel.ast.Projection;
import org.springframework.expression.spel.ast.PropertyOrFieldReference;
import org.springframework.expression.spel.ast.Selection;
import org.springframework.expression.spel.ast.TypeReference;
import org.springframework.expression.spel.ast.VariableReference;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * A SpEL expression compiled once per method, or the shared absent marker.
 *
 * <p><b>Read-only contract.</b> A cache expression is a pure function of the invocation:
 * key, TTL and {@code unless} may read arguments and the result, never write. An assignment
 * ({@code #x = ...}, {@code #user.name = ...}) or an increment/decrement ({@code #x++},
 * {@code #x--}) is rejected at resolution, so an expression can neither mutate the caller's
 * arguments nor change the evaluation context between phases.
 *
 * <p>Resolution is the fail-fast boundary: a configured expression that cannot be parsed
 * throws with the attribute role and the method, so a typo fails application startup
 * instead of becoming a per-call log storm. Absent (a null, empty or whitespace-only
 * attribute) is not a failure and skips the tier silently: {@code parseExpression("")}
 * throws, so blankness is decided before parsing and such an attribute is never compiled.
 *
 * <p><b>AST inspection.</b> One walk collects every variable reference, rejects a function call and
 * marks whether the expression is a compile-time constant: only literals and operators qualify,
 * while a variable, bean, property, method, constructor or type access keeps the expression
 * dynamic. An indexer, selection or projection without an explicit receiver applies to
 * {@code #root}, so it is dynamic as well; folding it against an empty context would fail at
 * resolution. A variable inside a string literal or a property name is therefore not mistaken
 * for a reference. Every positional reference ({@code #pN}/{@code #aN}) is validated
 * against the method's arity at resolution. A context-free expression is evaluated exactly once
 * while the plan is built and the resulting value is stored on the {@link Present}, so a constant
 * key, guard or TTL costs one field read at runtime and a constant failure names its role and
 * method instead of becoming a per-call exception.
 *
 * <p><b>No SpEL functions.</b> The evaluation context registers no function resolver, so
 * {@code #fn(...)} could only throw on every evaluation. It is rejected at resolution instead of
 * leaving an {@code unless} or TTL to swallow the exception and degrade.
 *
 * <p><b>Unknown variables fail resolution.</b> The legal variable set is statically known:
 * {@code #root}, {@code #this}, every in-range {@code #pN}/{@code #aN}, every parameter name,
 * and {@code #result} only where the caller declares it available. When the method carries no
 * parameter metadata, a named variable is rejected too: the runtime context is built from the
 * same {@code null} names and could only resolve it to {@code null}. A misspelled name such as
 * {@code #reslt} is rejected here instead of silently evaluating to {@code null}.
 *
 * <p><b>Canonical positional names belong to the slot table.</b> {@code pN}/{@code aN} are
 * generated bindings; {@link ParameterBindings} owns the table and rejects a parameter name that
 * would point at a different argument, while a parameter named for its own slot is harmless.
 * {@code root}, {@code this} and {@code result} are bound ahead of the table — SpEL's root/active
 * object and the context's result field — so a parameter with one of those names stays legal and
 * the reserved name never resolves to it.
 *
 * <p><b>{@code #result} is declared, not guessed.</b> The caller states whether the
 * expression is evaluated before the body (a read key/TTL, a pre-invocation invalidation)
 * or after it. A pre-body expression that references {@code #result} is rejected.
 */
@Internal
public sealed interface CompiledExpression permits CompiledExpression.Absent, CompiledExpression.Present {
  /** Shared carrier for a null/empty attribute — no expression, no failure. */
  CompiledExpression ABSENT = new Absent();

  /**
   * Compile one attribute, classifying blank as absent and a parse failure as a
   * configuration error. Surrounding whitespace is stripped at this entry point, so
   * {@code unless = "   "} is the same absent attribute as {@code ""}.
   *
   * @param raw            the raw attribute text (may be {@code null})
   * @param parser         the shared parser
   * @param role           the attribute role named in the failure message
   * @param method         the method the attribute belongs to
   * @param parameterNames the method's parameter names, or {@code null} when they were not discoverable
   * @param resultAllowed  whether {@code #result} is bound when this expression is evaluated
   * @return the compiled-or-absent carrier, never {@code null}
   * @throws IllegalStateException when the attribute was configured but cannot be parsed,
   *                               contains an assignment or increment/decrement, calls a SpEL
   *                               function, addresses an out-of-range parameter, references an
   *                               unknown or unbindable named variable, reads {@code #result}
   *                               before assignment, or is a constant expression that fails to
   *                               evaluate
   */
  static CompiledExpression of(
    @Nullable String raw,
    SpelExpressionParser parser,
    String role,
    Method method,
    String@Nullable [] parameterNames,
    boolean resultAllowed
  ) {
    String text = raw == null ? null : raw.strip();
    if (text == null || text.isEmpty()) {
      return ABSENT;
    }
    Expression expression;
    try {
      expression = parser.parseExpression(text);
    } catch (RuntimeException e) {
      throw new IllegalStateException(role + " '" + text + "' cannot be parsed for " + method, e);
    }
    boolean constant = true;
    boolean referencesResult = false;
    if (expression instanceof SpelExpression spel) {
      AstFacts facts = new AstFacts();
      inspectAst(spel.getAST(), text, role, method, parameterNames, resultAllowed, facts);
      constant = facts.constant();
      referencesResult = facts.referencesResult();
    }
    Object constantValue = constant ? evaluateConstant(expression, text, role, method) : null;
    return new Present(
      text,
      expression,
      constant,
      isWholeRootArgs(expression),
      referencesResult,
      constantValue
    );
  }

  /** No expression configured (null/empty attribute): skip the tier silently. */
  boolean absent();

  /** Whether the expression evaluates without an invocation context. */
  boolean constant();

  /**
   * Whether evaluation needs an invocation context: a configured expression that
   * is not a compile-time constant. An absent expression and a folded constant
   * answer {@code false}.
   *
   * @return {@code true} when a context must be built
   */
  default boolean needsContext() {
    return !absent() && !constant();
  }

  /** The stripped attribute text, or {@code null} when absent. */
  @Nullable
  String raw();

  /**
   * Evaluate the expression, or return {@code null} when the attribute was
   * absent.
   *
   * @param context the evaluation context; a constant expression ignores it and may receive
   *                {@code null}
   * @return the evaluation result
   */
  @Nullable
  Object getValue(EvaluationContext context);

  /**
   * Evaluate the expression with a required result type, or return {@code null}
   * when the attribute was absent.
   *
   * @param context the evaluation context; a constant expression ignores it and may receive
   *                {@code null}
   * @param type    the required result type
   * @param <T>     the result type
   * @return the evaluation result, or {@code null} when absent
   */
  <T> @Nullable T getValue(EvaluationContext context, Class<T> type);

  /**
   * The value of a context-free expression, computed once when the expression was compiled.
   * An absent or non-constant expression returns {@code null}; a constant expression that
   * evaluates to {@code null} also returns {@code null}, so the caller decides whether a null
   * value is legal for its role.
   *
   * @return the constant value, or {@code null}
   */
  @Nullable
  Object evaluateConstant();

  /**
   * Whether the expression reads the post-body {@code #result}; because the bit is
   * collected from the parsed tree, {@code key = "'#result'"} and a property named
   * {@code result} stay legal.
   *
   * @return {@code true} when {@code #result} is referenced
   */
  boolean referencesResult();

  /**
   * The one variable a whole expression names, or {@code null} when the
   * expression is absent or larger than a single variable reference.
   *
   * @return the variable name without the leading {@code #}, or {@code null}
   */
  @Nullable
  String singleVariable();

  /**
   * Whether the whole expression is exactly {@code #root.args}, the argument list
   * itself rather than one argument value. The key plan uses this to apply the
   * multi-argument default encoding, so an explicit {@code key = "#root.args"}
   * addresses the same entry as an absent {@code key}.
   *
   * @return {@code true} for a whole {@code #root.args} reference
   */
  boolean wholeRootArgs();

  /** The absent state: every query answers as if no expression were configured. */
  final class Absent implements CompiledExpression {

    private Absent() {}

    @Override
    public boolean absent() {
      return true;
    }

    @Override
    public boolean constant() {
      return true;
    }

    @Override
    public @Nullable String raw() {
      return null;
    }

    @Override
    public @Nullable Object getValue(EvaluationContext context) {
      return null;
    }

    @Override
    public <T> @Nullable T getValue(EvaluationContext context, Class<T> type) {
      return null;
    }

    @Override
    public @Nullable Object evaluateConstant() {
      return null;
    }

    @Override
    public boolean referencesResult() {
      return false;
    }

    @Override
    public @Nullable String singleVariable() {
      return null;
    }

    @Override
    public boolean wholeRootArgs() {
      return false;
    }
  }

  /**
   * A compiled expression.
   *
   * <p>The three AST facts the invocation path asks for are boolean fields: whether the expression
   * is a compile-time constant, whether it is exactly {@code #root.args}, and whether it reads
   * {@code #result}.
   *
   * @param raw               the stripped attribute text
   * @param expression        the compiled expression
   * @param constant          whether the expression evaluates without an invocation context
   * @param wholeRootArgs     whether the whole expression is {@code #root.args}
   * @param referencesResult  whether the expression reads the post-body {@code #result}
   * @param constantValue     the value of a context-free expression, evaluated once at resolution
   */
  record Present(
    String raw,
    Expression expression,
    boolean constant,
    boolean wholeRootArgs,
    boolean referencesResult,
    @Nullable Object constantValue
  ) implements CompiledExpression {
    public Present {
      Objects.requireNonNull(raw, "raw");
      Objects.requireNonNull(expression, "expression");
    }

    @Override
    public boolean absent() {
      return false;
    }

    @Override
    public @Nullable Object getValue(EvaluationContext context) {
      return constant ? constantValue : expression.getValue(context);
    }

    @Override
    public <T> @Nullable T getValue(EvaluationContext context, Class<T> type) {
      return constant ? type.cast(constantValue) : expression.getValue(context, type);
    }

    @Override
    public @Nullable Object evaluateConstant() {
      return constantValue;
    }

    @Override
    public @Nullable String singleVariable() {
      if (expression instanceof SpelExpression spel && spel.getAST() instanceof VariableReference reference) {
        return variableName(reference);
      }
      return null;
    }
  }

  /** The two AST facts one walk collects, before they are stored on the {@link Present}. */
  final class AstFacts {

    private boolean constant = true;

    private boolean referencesResult;

    void markDynamic() {
      constant = false;
    }

    void markResult() {
      referencesResult = true;
    }

    boolean constant() {
      return constant;
    }

    boolean referencesResult() {
      return referencesResult;
    }
  }

  /**
   * One ordered walk: reject a mutation or a function call, validate every variable as it is
   * found, and decide whether the tree is context-free. A property or method access is dynamic
   * even when its receiver is a literal, because the access itself is not part of the read-only
   * literal subset. An indexer, selection or projection without an explicit receiver applies to
   * {@code #root}, so it is dynamic too; folding it against an empty context would fail at
   * resolution.
   */
  private static void inspectAst(
    SpelNode node,
    String text,
    String role,
    Method method,
    String@Nullable [] parameterNames,
    boolean resultAllowed,
    AstFacts facts
  ) {
    if (node instanceof Assign || node instanceof OpInc || node instanceof OpDec) {
      throw new IllegalStateException(
        role +
          " '" +
          text +
          "' on " +
          method +
          " must not contain assignments ('=', '++', '--'): cache expressions are read-only"
      );
    }
    if (node instanceof FunctionReference function) {
      throw new IllegalStateException(
        role +
          " '" +
          text +
          "' on " +
          method +
          " cannot call SpEL function " +
          function.toStringAST() +
          ": no functions are registered"
      );
    }
    if (!isContextFree(node)) {
      facts.markDynamic();
    }
    if (node instanceof VariableReference reference) {
      String variable = variableName(reference);
      validateVariable(variable, text, role, method, parameterNames, resultAllowed);
      if ("result".equals(variable)) {
        facts.markResult();
      }
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      inspectAst(node.getChild(i), text, role, method, parameterNames, resultAllowed, facts);
    }
  }

  /** Whether the node can be evaluated without an invocation context. */
  private static boolean isContextFree(SpelNode node) {
    return !(
      node instanceof VariableReference ||
      node instanceof PropertyOrFieldReference ||
      node instanceof MethodReference ||
      node instanceof BeanReference ||
      node instanceof ConstructorReference ||
      node instanceof TypeReference ||
      node instanceof Indexer ||
      node instanceof Selection ||
      node instanceof Projection
    );
  }

  /** Whether the whole expression is exactly {@code #root.args}. */
  private static boolean isWholeRootArgs(Expression expression) {
    return (
      expression instanceof SpelExpression spel &&
      spel.getAST() instanceof CompoundExpression compound &&
      compound.getChildCount() == 2 &&
      compound.getChild(0) instanceof VariableReference root &&
      "root".equals(variableName(root)) &&
      compound.getChild(1) instanceof PropertyOrFieldReference property &&
      "args".equals(property.getName())
    );
  }

  /**
   * Reject a positional reference the method cannot satisfy, a named variable the evaluation
   * context cannot bind — including every named variable when no parameter metadata was
   * discovered — and {@code #result} where the phase does not bind it.
   */
  private static void validateVariable(
    String variable,
    String text,
    String role,
    Method method,
    String@Nullable [] names,
    boolean resultAllowed
  ) {
    if (!resultAllowed && "result".equals(variable)) {
      throw new IllegalStateException(
        role +
          " '" +
          text +
          "' on " +
          method +
          " cannot use #result: this evaluation phase does not bind a method result"
      );
    }
    int parameterCount = method.getParameterCount();
    int index = argumentIndex(variable);
    if (index >= parameterCount) {
      throw new IllegalStateException(
        role +
          " '" +
          text +
          "' on " +
          method +
          " addresses parameter " +
          index +
          ", but the method declares " +
          parameterCount
      );
    }
    if (index >= 0 || ParameterBindings.isContextVariable(variable)) {
      return;
    }
    if (names == null) {
      throw new IllegalStateException(
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
    if (!contains(names, variable)) {
      throw new IllegalStateException(
        role + " '" + text + "' on " + method + " references unknown variable '#" + variable + "'"
      );
    }
  }

  private static boolean contains(String[] names, String variable) {
    for (String name : names) {
      if (variable.equals(name)) {
        return true;
      }
    }
    return false;
  }

  /** Evaluate one context-free expression once, wrapping a failure as a configuration error. */
  private static @Nullable Object evaluateConstant(
    Expression expression,
    String text,
    String role,
    Method method
  ) {
    try {
      return expression.getValue();
    } catch (RuntimeException e) {
      throw new IllegalStateException(role + " '" + text + "' on " + method + " failed constant evaluation", e);
    }
  }

  /** {@link VariableReference} exposes no name accessor; {@code toStringAST()} is {@code "#" + name}. */
  private static String variableName(VariableReference reference) {
    return reference.toStringAST().substring(1);
  }

  /**
   * The argument index a positional variable names, or {@code -1} when the
   * variable is not a canonical {@code #pN}/{@code #aN}.
   *
   * <p>{@code #p01} is deliberately not index 1: SpEL binds it as the variable
   * named {@code p01}, and reinterpreting it here would change the caller's
   * expression. The same rule keeps the interceptor's zero-SpEL fast path and
   * this validation in agreement.
   *
   * @param variable the variable name without the leading {@code #}
   * @return the zero-based index, or {@code -1}
   */
  static int argumentIndex(String variable) {
    if (variable.length() < 2) {
      return -1;
    }
    char kind = variable.charAt(0);
    if (kind != 'p' && kind != 'a') {
      return -1;
    }
    int index = 0;
    for (int i = 1; i < variable.length(); i++) {
      char digit = variable.charAt(i);
      if (digit < '0' || digit > '9') {
        return -1;
      }
      if (i == 1 && digit == '0' && variable.length() > 2) {
        return -1;
      }
      if (index > (Integer.MAX_VALUE - (digit - '0')) / 10) {
        return -1;
      }
      index = index * 10 + (digit - '0');
    }
    return index;
  }

  /**
   * The argument index a variable names — a canonical {@code #pN}/{@code #aN}
   * or one of the method's discovered parameter names — or {@code -1}.
   *
   * <p>{@code root}, {@code this} and {@code result} are bound ahead of the slot
   * table (SpEL's root/active object and the context's result field), so they
   * never resolve to a parameter of the same name and return {@code -1} even
   * when one is declared.
   *
   * @param variable       the variable name without the leading {@code #}
   * @param parameterNames the method's discovered parameter names, or
   *                       {@code null} when they were not discoverable
   * @return the zero-based index, or {@code -1}
   */
  public static int argumentIndex(String variable, String@Nullable [] parameterNames) {
    int index = argumentIndex(variable);
    if (index >= 0 || parameterNames == null || ParameterBindings.isContextVariable(variable)) {
      return index;
    }
    for (int i = 0; i < parameterNames.length; i++) {
      if (variable.equals(parameterNames[i])) {
        return i;
      }
    }
    return -1;
  }
}
