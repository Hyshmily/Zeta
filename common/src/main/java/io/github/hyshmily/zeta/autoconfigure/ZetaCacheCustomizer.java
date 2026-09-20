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
package io.github.hyshmily.zeta.autoconfigure;

import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Callback interface for customizing the L1 Caffeine cache builder before the
 * {@code hotLocalCache} bean is built — the Spring Boot {@code *Customizer}
 * convention (cf. {@code Jackson2ObjectMapperBuilderCustomizer}).
 *
 * <p>Declaring one or more {@code ZetaCacheCustomizer} beans lets applications
 * tune the L1 cache (add a {@code removalListener}, set a {@code scheduler} or
 * {@code executor}, adjust capacity) <b>without</b> replacing the whole
 * {@code Cache<String, Object>} bean — a replacement that would otherwise have
 * to re-implement Zeta's {@code hardExpireAtMs}-driven {@code Expiry}, stats
 * recording, and weigher wiring by hand.
 *
 * <h3>Contract</h3>
 * <ul>
 *   <li>Customizers are applied <b>after</b> the built-in configuration
 *       (capacity / weigher, {@code recordStats()}, variable {@code Expiry}) and
 *       in {@link org.springframework.core.annotation.Order} sequence, just
 *       before {@code build()}.</li>
 *   <li>Caffeine setters are single-use: calling {@code expireAfter(...)},
 *       {@code maximumSize(...)} or {@code maximumWeight(...)} here throws
 *       {@link IllegalStateException} (those knobs belong to Zeta's
 *       {@code zeta.local.cache.*} configuration). Intended additions are
 *       orthogonal ones: {@code scheduler}, {@code executor}, {@code ticker},
 *       {@code removalListener}, {@code evictionListener}.</li>
 *   <li>{@code recordStats()} is always on; do not disable it — {@code Zeta#stats()}
 *       and the {@code cache.*} Micrometer metrics depend on it.</li>
 * </ul>
 */
@FunctionalInterface
public interface ZetaCacheCustomizer {

  /**
   * Customize the L1 cache builder.
   *
   * @param builder the Caffeine builder pre-configured by Zeta, about to be built
   */
  void customize(Caffeine<Object, Object> builder);
}
