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
 * {@code softTtl} inherits the class value; the literal {@code "0"} is an
 * explicit "use the configured default" and does not inherit. A cache name that
 * is still blank after the fallback fails startup.
 *
 * <p>
 * Class declarations merge attribute by attribute, nearest first: a subclass may override only
 * {@code hardTtl} and inherit the parent's {@code cache}. A blank attribute inherits the next
 * declaration outward. The roots are the bean's user class, or the maximal business interfaces of
 * a JDK proxy; only a root the method's declaring type is assignable from contributes, so a
 * sibling interface never lends its namespace or TTLs to another view's method. Complementary
 * attributes from two applicable sibling roots merge; two roots that set the same attribute to
 * different values fail resolution instead of depending on interface order.
 * When no root exposes a configuration, the method's declaring class supplies the fallback. A
 * composed config annotation is discovered through its meta-annotation and {@code @AliasFor}
 * overrides.
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
}
