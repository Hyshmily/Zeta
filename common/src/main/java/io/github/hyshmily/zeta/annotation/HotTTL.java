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
 * Per-method override for the TTL values used when a cache key is classified
 * as <em>hot</em> by the HeavyKeeper TopK detector.
 *
 * <p>When present, the specified hot TTL values take precedence over the global
 * defaults from {@code zeta.local.hot-hard-ttl-ms} and
 * {@code zeta.local.hot-soft-ttl-ms}. A value of {@code 0} means "use the global
 * default for this TTL type".
 *
 * <p>Unlike {@link CacheTTL @CacheTTL} (which overrides the <em>normal</em>
 * hard/soft TTL), this annotation only takes effect when the cache key is
 * recognised as hot. Normal (non-hot) keys continue to use the configured
 * normal TTLs or the {@code @CacheTTL} override.
 *
 * <p>Both annotations may coexist on the same method:
 * <pre>{@code
 * @Cacheable("users")
 * @CacheTTL(hardTtlMs = 60000, softTtlMs = 30000)
 * @HotTTL(hardTtlMs = 10000, softTtlMs = 5000)
 * User findUser(String id) { ... }
 * }</pre>
 *
 * <p>Only applies to {@link org.springframework.cache.annotation.Cacheable @Cacheable}
 * READ operations. Ignored on {@code @CachePut} and {@code @CacheEvict}.
 */
@Target({ ElementType.METHOD, ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface HotTTL {
  /** Hot-key hard TTL in milliseconds. 0 = use global default. */
  @NotNull
  long hardTtlMs() default 0;

  /** Hot-key soft TTL in milliseconds. 0 = use global default. */
  @NotNull
  long softTtlMs() default 0;
}
