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

import jakarta.validation.constraints.NotNull;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Per-method override for hard and soft TTLs on a {@link org.springframework.cache.annotation.Cacheable @Cacheable} READ operation.
 *
 * <p>When present, the specified TTL values take precedence over the global defaults
 * from {@code zeta.local.*} configuration. A value of {@code 0} for the static fields
 * means "use the global default for this TTL type".
 *
 * <p>SpEL expressions ({@link #hardTtlSpEl}, {@link #softTtlSpEl}) are evaluated against
 * method parameters when the corresponding static value is {@code 0}. If the SpEL
 * evaluates to a value greater than {@code 0}, it overrides the global default.
 *
 * <pre>{@code
 * @Cacheable("users")
 * @CacheTTL(hardTtlMs = 60000, softTtlMs = 30000)
 * User findUser(String id) { ... }
 *
 * // Dynamic TTL based on method parameter:
 * @Cacheable("users")
 * @CacheTTL(hardTtlSpEl = "#id.startsWith('vip') ? 600000 : 60000")
 * User findUser(String id) { ... }
 * }</pre>
 */
@Target({ ElementType.METHOD, ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface CacheTTL {
  /** Hard TTL in milliseconds. 0 = use global default. */
  @NotNull
  long hardTtlMs() default 0;

  /** Soft TTL in milliseconds. 0 = use global default. */
  @NotNull
  long softTtlMs() default 0;

  /**
   * SpEL expression for dynamic hard TTL, evaluated against method parameters.
   * Takes effect only when {@link #hardTtlMs} is {@code 0}.
   */
  String hardTtlSpEl() default "";

  /**
   * SpEL expression for dynamic soft TTL, evaluated against method parameters.
   * Takes effect only when {@link #softTtlMs} is {@code 0}.
   */
  String softTtlSpEl() default "";
}
