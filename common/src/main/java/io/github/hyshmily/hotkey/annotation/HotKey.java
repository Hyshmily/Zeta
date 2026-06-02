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
 * Method-level annotation for declarative hot-key caching on Spring beans.
 * <p>
 * Supports three modes via {@link OperationType}:
 * <ul>
 *   <li><b>READ</b> — reads from L1 / loads via method invocation and caches the result</li>
 *   <li><b>WRITE</b> — executes the method as the mutation inside
 *       {@code putBeforeInvalidate}. After success, L1 is invalidated and INVALIDATE
 *       broadcast is sent to peers, which will lazily reload on next read.</li>
 *   <li><b>INVALIDATE</b> — invalidates the key from L1, increments version,
 *       and broadcasts TYPE_REFRESH (versioned) to peers before executing the method</li>
 * </ul>
 * <p>
 * The {@code key} attribute supports Spring Expression Language (SpEL), enabling
 * parameter-driven cache keys such as {@code 'user:' + #id}. The <tt>-parameters</tt>
 * compiler flag is required (enabled by default in the parent POM).
 * <p>
 * Requires {@code hotkey.annotation.enabled=true} to activate.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface HotKey {
  /**
   * SpEL expression for the cache key. Method parameters are available as
   * {@code #paramName} variables.
   */
  String key();

  /**
   * Cache operation type. Defaults to {@link OperationType#READ}.
   */
  OperationType operation() default OperationType.READ;

  /**
   * Hard TTL override in milliseconds. Pass 0 to use the configured default
   * for the key's current state.
   */
  long hardTtlMs() default 0;

  /**
   * Soft TTL override in milliseconds. Pass 0 to use the configured default
   * for the key's current state. Only applies when {@link #softExpire} is {@code true}.
   */
  long softTtlMs() default 0;

  /**
   * Whether to use soft-expire (stale-while-revalidate) semantics on READ.
   * Defaults to {@code true}. When disabled, behaves as a plain {@code get()}.
   */
  boolean softExpire() default true;

  /**
   * The type of cache operation.
   *
   * <ul>
   *   <li>{@link #READ} — reads from L1 or loads via method invocation</li>
   *   <li>{@link #WRITE} — executes the mutation and invalidates the cache afterward</li>
   *   <li>{@link #INVALIDATE} — removes the entry from L1 and broadcasts to peers</li>
   * </ul>
   */
  enum OperationType {
    READ,
    WRITE,
    INVALIDATE,
  }
}
