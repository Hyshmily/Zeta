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
package io.github.hyshmily.zeta.annotation.supporter;

import io.github.hyshmily.zeta.util.Internal;
import java.lang.reflect.Method;
import java.util.LinkedHashSet;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.SpelNode;
import org.springframework.expression.spel.ast.Assign;
import org.springframework.expression.spel.ast.VariableReference;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * A SpEL expression compiled once per method, or the shared absent marker.
 *
 * <p>Resolution is the fail-fast boundary: a configured expression that cannot
 * be parsed throws with the attribute role and the method, so a typo fails
 * application startup instead of becoming a per-call log storm. Absent (a
 * null, empty or whitespace-only attribute) is not a failure and skips the tier
 * silently: {@code parseExpression("")} throws, so blankness is decided before
 * parsing and such an attribute is never compiled.
 *
 * <p><b>AST inspection.</b> One walk of the parsed syntax tree collects every
 * variable reference and detects a user assignment ({@code #x = ...}); a
 * variable inside a string literal or a property name is therefore not mistaken
 * for a reference. Every positional reference ({@code #pN}/{@code #aN}) is
 * validated against the method's arity at resolution.
 *
 * <p><b>Unknown variables fail resolution.</b> When the method's parameter names
 * are available, the legal variable set is statically known: {@code #root},
 * {@code #this}, every in-range {@code #pN}/{@code #aN}, every parameter name,
 * and {@code #result} only where the caller declares it available. A misspelled
 * name such as {@code #reslt} is rejected here instead of silently evaluating to
 * {@code null}. A direct variable assignment declares that variable for this
 * expression, so its own name is legal; every other unknown name — including the
 * right-hand side of an assignment and a property/index assignment such as
 * {@code #user.name = #typo} — is still rejected.
 *
 * <p><b>Reserved names cannot be parameter names.</b> {@code root},
 * {@code this}, {@code result} and canonical {@code pN}/{@code aN} belong to
 * the evaluation context. A method that declares a parameter with one of those
 * names is rejected as soon as any expression on it is compiled, so a parameter
 * can never shadow the binding an expression names.
 *
 * <p><b>{@code #result} is declared, not guessed.</b> The caller states whether
 * the expression is evaluated before the body (a read key/TTL, a
 * pre-invocation invalidation) or after it. A pre-body expression that
 * references {@code #result} is rejected.
 *
 * @param raw        the raw attribute text ({@code null} for the absent state)
 * @param expression the compiled expression ({@code null} when absent)
 * @param variables  every variable the expression references, collected from the AST once
 */
@Internal
public record CompiledExpression(String raw, Expression expression, Set<String> variables) {
  /** Shared carrier for a null/empty attribute — no expression, no failure. */
  public static final CompiledExpression ABSENT = new CompiledExpression(null, null, Set.of());

  public CompiledExpression {
    variables = Set.copyOf(variables);
  }

  /**
   * Compile one attribute, classifying blank as absent and a parse failure as a
   * configuration error. Surrounding whitespace is stripped at this entry point,
   * so {@code unless = "   "} is the same absent attribute as {@code ""}.
   *
   * @param raw            the raw attribute text (may be {@code null})
   * @param parser         the shared parser
   * @param role           the attribute role named in the failure message
   * @param method         the method the attribute belongs to
   * @param parameterNames the method's parameter names, or {@code null} when they were not discoverable
   * @param resultAllowed  whether {@code #result} is bound when this expression is evaluated
   * @return the compiled-or-absent carrier, never {@code null}
   * @throws IllegalStateException when the attribute was configured but cannot be parsed,
   *                               addresses an out-of-range parameter, declares a reserved
   *                               parameter name, references an unknown variable, or reads
   *                               {@code #result} before the body has run
   */
  public static CompiledExpression of(
    @Nullable String raw,
    SpelExpressionParser parser,
    String role,
    Method method,
    String@Nullable [] parameterNames,
    boolean resultAllowed
  ) {
    String expressionText = raw == null ? null : raw.strip();
    if (expressionText == null || expressionText.isEmpty()) {
      return ABSENT;
    }
    Expression expression;
    try {
      expression = parser.parseExpression(expressionText);
    } catch (RuntimeException e) {
      throw new IllegalStateException(role + " '" + expressionText + "' cannot be parsed for " + method, e);
    }
    Set<String> variables = new LinkedHashSet<>();
    Set<String> assigned = new LinkedHashSet<>();
    collectVariables(expression, variables, assigned);
    validateArguments(expressionText, role, method, variables, assigned, parameterNames, resultAllowed);
    return new CompiledExpression(expressionText, expression, variables);
  }

  /**
   * Reject a reserved parameter name, a positional reference the method cannot
   * satisfy and, when the parameter names are known, a variable name the
   * evaluation context cannot bind. A variable the expression assigns is
   * declared by that assignment, so it is legal to reference; other unknown
   * names are still rejected, including the right-hand side of an assignment and
   * a property assignment such as {@code #user.name = #typo}.
   */
  private static void validateArguments(
    String text,
    String role,
    Method method,
    Set<String> variables,
    Set<String> assigned,
    String@Nullable [] names,
    boolean resultAllowed
  ) {
    validateParameterNames(text, role, method, names);
    if (variables.isEmpty()) {
      return;
    }
    if (!resultAllowed && variables.contains("result")) {
      throw new IllegalStateException(
        role +
          " '" +
          text +
          "' on " +
          method +
          " cannot use #result: the expression is resolved before the method body runs"
      );
    }
    int parameterCount = method.getParameterCount();
    for (String variable : variables) {
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
      if (
        index < 0 &&
        !assigned.contains(variable) &&
        names != null &&
        !isKnownVariable(variable, names)
      ) {
        throw new IllegalStateException(
          role + " '" + text + "' on " + method + " references unknown variable '#" + variable + "'"
        );
      }
    }
  }

  /** Whether the context binds a variable: {@code root}, {@code this}, {@code result}, or a parameter name. */
  private static boolean isKnownVariable(String variable, String[] names) {
    if ("root".equals(variable) || "this".equals(variable) || "result".equals(variable)) {
      return true;
    }
    for (String name : names) {
      if (variable.equals(name)) {
        return true;
      }
    }
    return false;
  }

  /** Reject a parameter that would shadow a reserved SpEL binding. */
  private static void validateParameterNames(String text, String role, Method method, String@Nullable [] names) {
    if (names == null) {
      return;
    }
    for (String name : names) {
      if (name != null && reserved(name)) {
        throw new IllegalStateException(
          role +
            " '" +
            text +
            "' on " +
            method +
            " declares reserved parameter '" +
            name +
            "': #root, #this, #result, #pN and #aN are bound by the evaluation context; rename the parameter"
        );
      }
    }
  }

  /**
   * Whether a name is reserved for a SpEL binding: {@code root}, {@code this},
   * {@code result}, or a canonical {@code pN}/{@code aN}. A method parameter
   * with a reserved name is rejected at resolution, so it cannot shadow the
   * binding the expression names.
   *
   * @param variable the variable name without the leading {@code #}
   * @return {@code true} when the name is reserved
   */
  private static boolean reserved(String variable) {
    return (
      "root".equals(variable) || "this".equals(variable) || "result".equals(variable) || argumentIndex(variable) >= 0
    );
  }

  /**
   * Collect every referenced variable in one walk and record the variables a direct
   * {@code #x = ...} assignment declares. A property or index assignment
   * ({@code #user.name = ...}) is not a variable declaration and leaves the
   * unknown-name validation active for both sides.
   */
  private static void collectVariables(Expression expression, Set<String> names, Set<String> assigned) {
    if (expression instanceof SpelExpression spel) {
      collectVariables(spel.getAST(), names, assigned);
    }
  }

  private static void collectVariables(SpelNode node, Set<String> names, Set<String> assigned) {
    if (node instanceof Assign assign) {
      SpelNode left = assign.getChildCount() > 0 ? assign.getChild(0) : null;
      if (left instanceof VariableReference reference) {
        assigned.add(variableName(reference));
      }
    }
    if (node instanceof VariableReference reference) {
      names.add(variableName(reference));
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      collectVariables(node.getChild(i), names, assigned);
    }
  }

  /** No expression configured (null/empty attribute): skip the tier silently. */
  public boolean absent() {
    return expression == null;
  }

  /**
   * Evaluate the expression, or return {@code null} when the attribute was
   * absent.
   *
   * @param context the evaluation context
   * @return the evaluation result
   */
  public Object getValue(EvaluationContext context) {
    return expression == null ? null : expression.getValue(context);
  }

  /**
   * Evaluate the expression with a required result type, or return {@code null}
   * when the attribute was absent.
   *
   * @param context the evaluation context
   * @param type    the required result type
   * @param <T>     the result type
   * @return the evaluation result, or {@code null} when absent
   */
  public <T> T getValue(EvaluationContext context, Class<T> type) {
    return expression == null ? null : expression.getValue(context, type);
  }

  /**
   * Whether the expression references a SpEL variable; because the check walks
   * the parsed tree, {@code key = "'#result'"} and a property named
   * {@code result} stay legal.
   *
   * @param variable the variable name without the leading {@code #}
   * @return {@code true} when the variable is referenced
   */
  public boolean references(String variable) {
    return expression != null && variables.contains(variable);
  }

  /**
   * The one variable a whole expression names, or {@code null} when the
   * expression is absent or larger than a single variable reference.
   *
   * @return the variable name without the leading {@code #}, or {@code null}
   */
  public @Nullable String singleVariable() {
    if (expression instanceof SpelExpression spel && spel.getAST() instanceof VariableReference reference) {
      return variableName(reference);
    }
    return null;
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
  public static int argumentIndex(String variable) {
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
   * <p>{@code root}, {@code this} and {@code result} are never parameter names:
   * a method parameter with a reserved name is rejected as soon as one of its
   * expressions is compiled.
   *
   * @param variable       the variable name without the leading {@code #}
   * @param parameterNames the method's discovered parameter names, or
   *                       {@code null} when they were not discoverable
   * @return the zero-based index, or {@code -1}
   */
  public static int argumentIndex(String variable, String@Nullable [] parameterNames) {
    int index = argumentIndex(variable);
    if (index >= 0 || parameterNames == null) {
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
