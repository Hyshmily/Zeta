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

import static io.github.hyshmily.zeta.annotation.InterceptType.IS_LOCAL_HOT;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation that controls when a {@code @Cacheable} READ operation is
 * intercepted and the cached or fallback value is returned instead of executing
 * the original method body.
 *
 * <p>The interception type is controlled by {@link #type()}:
 * <ul>
 *   <li>{@link InterceptType#IS_LOCAL_HOT} — intercepts when the HeavyKeeper
 *       TopK detector recognises the key as hot (default)</li>
 *   <li>{@link InterceptType#FORCE} — always intercepts</li>
 *   <li>{@link InterceptType#QPS} — intercepts when the per-key request rate
 *       exceeds {@link #qps()}</li>
 *   <li>{@link InterceptType#CONCURRENT_THREADS} — intercepts when the per-key
 *       concurrent thread count exceeds {@link #concurrentThreads()}</li>
 * </ul>
 *
 * <p>When a method is intercepted, the fallback resolution order is:
 * <ol>
 *   <li>{@link #fallback()} — SpEL expression evaluated against method parameters</li>
 *   <li>{@link Fallback @Fallback} — naming-convention method or SpEL expression</li>
 *   <li>{@code peek()} — stale cached value if available, otherwise {@code null}</li>
 * </ol>
 *
 * <p>Only applies to {@link org.springframework.cache.annotation.Cacheable @Cacheable}
 * READ operations. Ignored on {@code @CachePut} and {@code @CacheEvict}.
 *
 * @see InterceptType
 * @see CacheExtensionAspect
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Intercept {
  /**
   * Trigger mode that determines when the method call is intercepted.
   * Defaults to {@link InterceptType#IS_LOCAL_HOT}.
   */
  InterceptType type() default IS_LOCAL_HOT;

  /**
   * qps threshold for {@link InterceptType#QPS} mode.
   * When the per-key request rate (1-second sliding window) exceeds this
   * value, subsequent calls are intercepted. A value of {@code 0} means
   * the qps check is disabled (equivalent to no-op).
   */
  int qps() default 0;

  /**
   * Concurrent thread threshold for {@link InterceptType#CONCURRENT_THREADS} mode.
   * When the number of in-flight threads for the given cache key exceeds this
   * value, subsequent calls are intercepted. A value of {@code 0} means
   * the check is disabled.
   */
  int concurrentThreads() default 0;

  /**
   * SpEL expression evaluated against method parameters to produce a
   * fallback value when interception triggers. Takes precedence over
   * the method-level {@link Fallback @Fallback} annotation.
   *
   * <p>Example: {@code "'fallback-' + #id"}
   */
  String fallback() default "";
}
