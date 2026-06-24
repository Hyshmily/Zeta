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
package io.github.hyshmily.hotkey.annotation;

import static io.github.hyshmily.hotkey.annotation.InterceptTrigger.IS_LOCAL_HOT;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation that controls when a {@code @Cacheable} READ operation is
 * intercepted and the cached or fallback value is returned instead of executing
 * the original method body.
 *
 * <p>The interception trigger is controlled by {@link #trigger()}:
 * <ul>
 *   <li>{@link InterceptTrigger#IS_LOCAL_HOT} — intercepts when the HeavyKeeper
 *       TopK detector recognises the key as hot (default)</li>
 *   <li>{@link InterceptTrigger#FORCE} — always intercepts</li>
 *   <li>{@link InterceptTrigger#QPS} — intercepts when the per-key request rate
 *       exceeds {@link #QPS()}</li>
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
 * @see InterceptTrigger
 * @see HotKeyCacheExtensionAspect
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Intercept {

  /**
   * Trigger mode that determines when the method call is intercepted.
   * Defaults to {@link InterceptTrigger#IS_LOCAL_HOT}.
   */
  InterceptTrigger trigger() default IS_LOCAL_HOT;

  /**
   * QPS threshold for {@link InterceptTrigger#QPS} mode.
   * When the per-key request rate (1-second sliding window) exceeds this
   * value, subsequent calls are intercepted. A value of {@code 0} means
   * the QPS check is disabled (equivalent to no-op).
   */
  int QPS() default 0;

  /**
   * SpEL expression evaluated against method parameters to produce a
   * fallback value when interception triggers. Takes precedence over
   * the method-level {@link Fallback @Fallback} annotation.
   *
   * <p>Example: {@code "'fallback-' + #id"}
   */
  String fallback() default "";
}
