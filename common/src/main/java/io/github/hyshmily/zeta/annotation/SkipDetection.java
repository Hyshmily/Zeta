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
 * Marks a {@link org.springframework.cache.annotation.Cacheable @Cacheable} method
 * to skip hot-key detection and Worker reporting for its cache accesses.
 *
 * <p>When present, the cache read path bypasses {@link
 * io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector#add(String) add()} and
 * {@link io.github.hyshmily.zeta.reporting.KeyReporter#reportToWorker(String) reportToWorker()},
 * eliminating the CPU and memory overhead of frequency sketching for this method's
 * keys. The cached value is still returned on hit and loaded on miss — only the
 * detection and reporting machinery is elided.
 *
 * <p>Typical use cases:
 * <ul>
 *   <li>Static configuration that changes rarely</li>
 *   <li>Health-check or metadata endpoints with stable access patterns</li>
 *   <li>Low-value keys whose hotness classification is irrelevant</li>
 * </ul>
 *
 * <p>Only applies to {@code @Cacheable} READ operations. Ignored on
 * {@code @CachePut} and {@code @CacheEvict}.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface SkipDetection {}
