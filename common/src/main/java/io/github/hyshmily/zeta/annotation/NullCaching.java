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
 * Marks a {@link org.springframework.cache.annotation.Cacheable @Cacheable} method to allow
 * caching of {@code null} return values.
 *
 * <p>By default, when a {@code @Cacheable} method returns {@code null}, the result is
 * <em>not</em> cached — a subsequent call with the same key will invoke the method again.
 * With {@code @NullCaching(true)}, the {@code null} value is stored via an internal sentinel
 * ({@link io.github.hyshmily.zeta.cache.annotationsupporter.NullValue}), and subsequent calls skip the method
 * and return {@code null} directly.
 *
 * <p>This annotation is only effective on {@code @Cacheable} methods. It has no effect on
 * {@code @CachePut} or {@code @CacheEvict}.
 *
 * <pre>{@code
 * @Cacheable("users")
 * @NullCaching(true)
 * User findUser(String id) { ... }
 * }</pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface NullCaching {
  /** Whether to cache {@code null} return values. Default is {@code true}. */
  boolean value() default true;
}
