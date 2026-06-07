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
 * parameter-driven cache keys such as {@code 'user:' + #id}. The {@code -parameters}
 * compiler flag is required (enabled by default in the parent POM).
 * <p>
 * Companion annotations:
 * <ul>
 *   <li>{@link Fallback @Fallback} — fallback value when blacklist blocks, hot key
 *       intercept triggers, or cache loader throws</li>
 *   <li>{@link Intercept @Intercept} — skip method execution for recognised hot keys</li>
 *   <li>{@link HotKeyCacheTTL @HotKeyCacheTTL} — per-entry hard/soft TTL overrides</li>
 * </ul>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface HotKey {

  /** SpEL expression for the cache key. Method parameters are available as {@code #paramName} variables. */
  String key();

  /** Cache operation type. Defaults to {@link OperationType#READ}. */
  OperationType operation() default OperationType.READ;

  /**
   * SpEL condition that gates cache participation.
   * <p>When the expression evaluates to {@code false} (or {@code null}), the cache is
   * bypassed entirely and the method executes directly. Parameters are available
   * as {@code #paramName} variables.
   * <p>Defaults to empty (no condition — always attempt caching).
   */
  String condition() default "";

  /**
   * SpEL exclusion expression evaluated after a cache load succeeds.
   * <p>When the expression evaluates to {@code true}, the loaded value is still
   * placed into the cache but the result is accepted as-is. The special variable
   * {@code #result} holds the loaded value.
   * <p>Defaults to empty (no exclusion).
   */
  String unless() default "";

  /**
   * Whether to use soft-expire (stale-while-revalidate) semantics on READ.
   * When enabled (default), stale entries are served immediately while a
   * background refresh is triggered. When disabled, behaves as a plain {@code get()}.
   */
  boolean softExpire() default true;

  /** The type of cache operation. */
  enum OperationType {
    /** Reads from L1 / loads via method invocation and caches the result. */
    READ,
    /** Executes the method as a mutation inside {@code putBeforeInvalidate}, then invalidates L1 and broadcasts INVALIDATE to peers. */
    WRITE,
    /** Invalidates the key from L1, increments the version, and broadcasts TYPE_REFRESH to peers before executing the method. */
    INVALIDATE,
  }
}
