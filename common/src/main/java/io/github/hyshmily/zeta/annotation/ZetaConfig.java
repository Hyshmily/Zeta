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
import io.github.hyshmily.zeta.policy.KeyFormat;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Class-level defaults for the annotation verbs. It supplies only the values a
 * whole service class usually shares — the namespace and the two TTLs — never a
 * key, condition or veto: those are properties of one method's contract.
 *
 * <p>
 * A method attribute wins when it is non-blank. An empty {@code hardTtl} or
 * {@code softTtl} inherits the class value; the literal {@code "0"} (like {@code "-1"}) is an
 * explicit "use the configured default" and does not inherit, so it can mask an inherited value.
 * A cache name that is still blank after the fallback fails startup.
 *
 * <p>
 * Class declarations merge attribute by attribute, nearest first: a subclass may override only
 * {@code hardTtl} and inherit the parent's {@code cache}. A blank attribute inherits the next
 * declaration outward. A class root merges its superclass chain first and then the interfaces that
 * declare the method: a sibling interface never lends its namespace or TTLs to another interface's
 * method, while a method only the class declares falls back to every interface the class
 * implements. Within that interface graph a sub-interface overrides its parents, and two interfaces
 * that disagree on an attribute fail startup instead of one winning by scan order. A JDK proxy
 * resolves against its business interface hierarchy the same way. A composed config annotation is
 * discovered through its meta-annotation and {@code @AliasFor} overrides.
 */
@Target({ ElementType.TYPE, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ZetaConfig {
  /**
   * The default namespace for verbs declared on the annotated type, or empty
   * for no default.
   *
   * @return the default namespace
   */
  String cache() default "";

  /**
   * The default hard TTL literal or expression, or empty for no default.
   *
   * @return the default hard TTL
   */
  String hardTtl() default "";

  /**
   * The default soft TTL literal or expression, or empty for no default.
   *
   * @return the default soft TTL
   */
  String softTtl() default "";

  /**
   * The type-level default prewarm trigger. {@link HeatTrigger.Option#INHERIT} means "no type-level
   * default"; a method attribute always wins over this value.
   *
   * @return the type-level prewarm trigger declaration
   */
  HeatTrigger.Option prewarm() default HeatTrigger.Option.INHERIT;

  /**
   * The type-level default keep-warm trigger. {@link HeatTrigger.Option#INHERIT} means "no
   * type-level default"; a method attribute always wins over this value.
   *
   * @return the type-level keep-warm trigger declaration
   */
  HeatTrigger.Option keepWarm() default HeatTrigger.Option.INHERIT;

  /**
   * The physical key format for the namespaces this type declares. It is inherited by a method
   * only when that method also inherits {@link #cache()}: a method that names its own cache must
   * not silently lend this type's format to another namespace.
   *
   * <p>{@link KeyFormat.Option#PLAIN} prints a single scalar key value directly after the
   * delimiter without type tags and forbids a default key, so every key-bearing declaration in a
   * PLAIN namespace must name one explicit single-value key.
   *
   * @return the key format declaration, or {@link KeyFormat.Option#INHERIT}
   */
  KeyFormat.Option keyFormat() default KeyFormat.Option.INHERIT;
}
