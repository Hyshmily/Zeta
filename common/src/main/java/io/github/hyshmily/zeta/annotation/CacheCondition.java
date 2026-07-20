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
 * SpEL-based condition that controls whether the return value of a
 * {@link org.springframework.cache.annotation.Cacheable @Cacheable} method
 * is actually stored in the cache after execution.
 *
 * <p>If the {@link #unless} SpEL expression evaluates to {@code true}
 * (using {@code #result} and method parameters as context variables),
 * the value returned by the method is <em>not</em> cached — it is returned
 * to the caller as-is, but the cache entry is evicted so the next call
 * will re-invoke the method.
 *
 * <p>This is analogous to Spring's own {@code @Cacheable(unless = ...)},
 * but operates at the Zeta annotation level <em>after</em> Spring's
 * {@code CacheInterceptor} has already stored the value. The aspect
 * invalidates the cache entry when the condition is not met.
 *
 * <p>Typical use cases:
 * <ul>
 *   <li>Avoid caching error responses: {@code @CacheCondition(unless = "#result == null")}</li>
 *   <li>Conditional caching based on result state: {@code @CacheCondition(unless = "!#result.active")}</li>
 *   <li>Argument-based skip: {@code @CacheCondition(unless = "#id.startsWith('temp_')")}</li>
 * </ul>
 *
 * <p>Only applies to {@code @Cacheable} READ operations. Ignored on
 * {@code @CachePut} and {@code @CacheEvict}.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface CacheCondition {

  /**
   * SpEL expression evaluated after method execution. When the expression
   * evaluates to {@code true}, the result is <em>not</em> cached.
   *
   * <p>Context variables:
   * <ul>
   *   <li>{@code #result} — the return value of the method</li>
   *   <li>{@code #paramName} — method parameters by name</li>
   * </ul>
   *
   * <p>Example: {@code "#result == null || #result.code != 200"}
   */
  String unless() default "";
}
