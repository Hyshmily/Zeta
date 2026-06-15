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
 * Marker annotation that intercepts READ operations when the cache key is
 * classified as a local hot key (tracked by the TopK algorithm in L1).
 * <p>
 * When applied:
 * <ul>
 *   <li>If the key is currently hot, the original method is <b>not</b> executed.</li>
 *   <li>If a {@link Fallback @Fallback} is also present, the fallback value is returned.</li>
 *   <li>If no {@link Fallback @Fallback} is present, {@code peek()} is used
 *       (the stale entry if available, otherwise {@code null}).</li>
 *   <li>If the key is <b>not</b> hot, the cache lookup proceeds normally.</li>
 * </ul>
 * <p>
 * Only applies to {@link org.springframework.cache.annotation.Cacheable @Cacheable} READ operations.
 * Ignored on WRITE and INVALIDATE.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Intercept {}
