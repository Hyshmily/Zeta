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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares a fallback value or method for a {@link org.springframework.cache.annotation.Cacheable @Cacheable} READ operation.
 * <p>
 * The fallback is invoked in two scenarios:
 * <ol>
 *   <li>The key is a local hot key and {@link Intercept @Intercept} is present</li>
 *   <li>The cache loader throws a {@link RuntimeException}</li>
 * </ol>
 * <p>
 * A blacklist block ({@link io.github.hyshmily.zeta.exception.ZetaBlockedException})
 * NEVER routes to the fallback — policy rejection is not a method failure and
 * always propagates to the caller.
 * <p>
 * When {@link #value()} is non-empty, it is evaluated as a SpEL expression
 * (method parameters are available as {@code #paramName} variables).
 * When empty, the aspect looks for a method named {@code {originalName}Fallback}
 * with identical parameter types on the same bean.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Fallback {
  /**
   * SpEL expression for the fallback value.
   * <p>Defaults to empty, which triggers naming-convention resolution
   * ({@code {methodName}Fallback}) or returns {@code null} if no such method exists.
   */
  String value() default "";
}
