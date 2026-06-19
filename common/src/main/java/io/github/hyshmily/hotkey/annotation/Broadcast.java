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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Controls whether cache write/evict operations on this method broadcast
 * sync messages to peer instances via RabbitMQ.
 * <p>
 * When {@code @Broadcast(false)} is present, the underlying
 * {@link io.github.hyshmily.hotkey.HotKey} methods use local-only variants
 * ({@code putLocal()} / {@code evictLocal()}) instead of the default
 * {@code putThrough()} / {@code invalidate()} paths.
 * <p>
 * Applies to {@link org.springframework.cache.annotation.Cacheable @Cacheable},
 * {@link org.springframework.cache.annotation.CachePut @CachePut}, and
 * {@link org.springframework.cache.annotation.CacheEvict @CacheEvict} methods.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Broadcast {

  /** Whether to broadcast sync messages. Default is {@code true}. */
  boolean value() default true;
}
