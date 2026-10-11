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

import io.github.hyshmily.zeta.annotation.annotationsupporter.NullValue;
import jakarta.validation.constraints.NotNull;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Opt-out for {@code null} caching on a
 * {@link org.springframework.cache.annotation.Cacheable @Cacheable} method.
 *
 * <p>By default every {@code null} return value IS cached as an internal
 * sentinel ({@link NullValue}) with a short TTL
 * ({@code zeta.local.null-value-ttl-seconds}) for cache-penetration
 * protection — a subsequent call with the same key skips the method and
 * returns {@code null} directly until the sentinel expires.
 * With {@code @NullCaching(false)}, the {@code null} result is <em>not</em>
 * cached and the next call re-invokes the method.
 *
 * <p>This annotation is only effective on {@code @Cacheable} methods. It has no effect on
 * {@code @CacheEvict}.
 *
 * <pre>{@code
 * @Cacheable("users")
 * @NullCaching(false)
 * User findUser(String id) { ... }
 * }</pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface NullCaching {
  /** Whether to cache {@code null} return values. Default is {@code true}. */
  @NotNull
  boolean value() default true;
}
