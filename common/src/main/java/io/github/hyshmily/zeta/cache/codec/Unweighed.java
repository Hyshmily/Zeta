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
package io.github.hyshmily.zeta.cache.codec;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Excludes a field or a class from weight estimation in {@code max-weight} mode
 * ({@link DefaultWeigher}).
 *
 * <p>On a <b>field</b>: the field's content is not measured — a shared context, a connection pool,
 * a class loader or any other object that is reachable from the value but not owned by it. The
 * reference slot itself is still counted by the holder's shallow size, matching the convention that
 * a holder owns its own layout.
 *
 * <p>On a <b>class</b>: instances of the class are priced at their reference slot only (their
 * content is skipped entirely). Use it for types that are shared across the whole application and
 * must not be attributed to whichever cache entry happens to reference them.
 *
 * <p>Same escape hatch as Ehcache's {@code @IgnoreSizeOf} and Jamm's {@code @Unmetered}. Checking
 * happens once per class when the walk builds its field cache, so annotated fields cost nothing on
 * the per-node path.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.TYPE})
public @interface Unweighed {

  /**
   * Documents why the field or class is excluded. Purely informational.
   *
   * @return the reason for exclusion
   */
  String value() default "";
}
