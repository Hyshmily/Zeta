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
 * Declares that the cache key resolved by the SpEL expression should be
 * tagged — fed into the local HeavyKeeper sketch and optionally reported
 * to the Worker for hot-key detection — without performing a cache lookup.
 *
 * <p>This annotation is independent of Spring's {@code @Cacheable} and can
 * be placed on any method. The resolved key is passed to {@link
 * io.github.hyshmily.zeta.Zeta#tag(String, boolean, boolean)}.
 *
 * <p>Tagging consists of two independent operations:
 * <ol>
 *   <li><b>Local count</b> — increment the local HeavyKeeper sketch</li>
 *   <li><b>Worker report</b> — enqueue the key for batch reporting to the
 *       Worker cluster</li>
 * </ol>
 *
 * <p>Each can be suppressed via the {@link #skipDetection} and
 * {@link #skipReport} attributes respectively.
 *
 * <p>By default the resolved key lives in the <b>raw key namespace</b>. When
 * {@link #cacheName} is set, the key is prefixed with
 * {@code cacheName + keySeparator} so it aligns with the namespace used by
 * {@code @Cacheable(cacheNames = ...)} entries.
 *
 * <p><b>Note:</b> placing {@code @Tag} and {@code @Cacheable} on the same
 * method double-counts the key (both paths feed the detector); the aspect
 * logs a WARN for that combination.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Tag {

  /** SpEL expression that resolves to the cache key to tag. */
  String value();

  /**
   * Optional cache name used to prefix the resolved key as
   * {@code cacheName + keySeparator + key}, aligning the tagged key with the
   * {@code @Cacheable} key namespace. Empty (default) keeps the raw key.
   */
  String cacheName() default "";

  /**
   * If {@code true}, skip the local HeavyKeeper increment.
   * Only the Worker report will be performed.
   */
  boolean skipDetection() default false;

  /**
   * If {@code true}, skip the Worker report.
   * Only the local HeavyKeeper increment will be performed.
   */
  boolean skipReport() default false;
}
