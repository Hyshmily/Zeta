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
 * Per-method override for hard and soft TTLs on a {@link HotKey @HotKey} READ operation.
 * <p>
 * When present, the specified TTL values take precedence over the global defaults
 * from {@code hotkey.local.*} configuration. A value of {@code 0} means "use the
 * global default for this TTL type".
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface HotKeyCacheTTL {

  /** Hard TTL in milliseconds. 0 = use global default. */
  long hardTtlMs() default 0;

  /** Soft TTL in milliseconds. 0 = use global default. */
  long softTtlMs() default 0;
}
