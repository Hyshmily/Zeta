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

import io.github.hyshmily.zeta.Zeta;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares cache keys that should be pre-loaded as <em>hot</em> before they
 * naturally accumulate enough accesses in the HeavyKeeper TopK sketch.
 *
 * <p>Pre-loaded keys have their detection counts inflated via
 * {@link Zeta#notifyLocalDetectorDirect(String, int)}
 * so they immediately benefit from long TTLs, hot-key interception
 * ({@link Intercept @Intercept}), and priority treatment — without waiting
 * for the detection engine to recognise them organically.
 *
 * <p>Typical use cases:
 * <ul>
 *   <li>Flash-sale / promotional items known to the business team in advance</li>
 *   <li>VIP-user data that should always be served from cache</li>
 *   <li>Infrastructure keys that must never degrade to short TTLs</li>
 * </ul>
 *
 * <p><b>Static keys</b> ({@link #keys}) are registered on first method
 * invocation and deduplicated by a bounded Caffeine cache (100k max, 1-hour
 * TTL). <b>Dynamic keys</b> ({@link #keyExpr}) are resolved on every
 * invocation through a SpEL expression that may reference method parameters
 * ({@code #paramName}). Dynamic keys are best suited for low-to-moderate
 * frequency scenarios (e.g., user login); for high-frequency call-sites
 * prefer static {@link #keys}.
 *
 * <p>Duplicates are silently ignored — a key is only inflated once.
 *
 * <p>Only applies to {@link org.springframework.cache.annotation.Cacheable @Cacheable}
 * READ operations. Ignored on {@code @CachePut} and {@code @CacheEvict}.
 *
 * <pre>{@code
 * @Cacheable("products")
 * @HotKeyPreload(keys = {"flash-item-001", "flash-item-002"})
 * @Intercept
 * Product getProduct(String id) { ... }
 * }</pre>
 *
 * @see Intercept
 * @see HotKeyCacheExtensionAspect
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface HotKeyPreload {
  /**
   * Static cache keys to pre-load as hot. Registered on first method
   * invocation and deduplicated — each key is inflated exactly once.
   */
  String[] keys() default {};

  /**
   * SpEL expression for a dynamic cache key to pre-load. Evaluated on
   * every method invocation, so the pre-loaded key corresponds to the
   * current parameter values (e.g. {@code #id}).
   *
   * <p>Duplicates are silently ignored — a key is only inflated once.
   */
  String keyExpr() default "";

  /**
   * Inflated access count injected into the detection engine per key.
   * {@code 0} means {@link Integer#MAX_VALUE} — virtually guaranteed to
   * be classified as hot by the HeavyKeeper sketch.
   */
  int count() default 0;
}
