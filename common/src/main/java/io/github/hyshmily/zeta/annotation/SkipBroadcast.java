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
 * Suppresses cross-instance broadcast (RabbitMQ sync messages) for cache
 * write/evict operations on this method.
 *
 * <p>When present, the underlying {@link
 * io.github.hyshmily.zeta.Zeta Zeta} methods use local-only variants
 * ({@code putLocal()} / {@code invalidateLocal()}) instead of the default
 * {@code putThrough()} / {@code invalidate()} paths. Peer instances will
 * not receive invalidation or refresh messages for this operation.
 *
 * <p>Typical use cases:
 * <ul>
 *   <li>Ephemeral data that is local to this instance</li>
 *   <li>High-frequency writes where cross-instance consistency is not required</li>
 *   <li>Bulk operations where individual broadcasts would flood the AMQP bus</li>
 * </ul>
 *
 * <p>Applies to {@link org.springframework.cache.annotation.Cacheable @Cacheable},
 * {@link org.springframework.cache.annotation.CachePut @CachePut}, and
 * {@link org.springframework.cache.annotation.CacheEvict @CacheEvict} methods.
 *
 * <p>Replaces the earlier {@code @Broadcast(false)} convention.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface SkipBroadcast {}
