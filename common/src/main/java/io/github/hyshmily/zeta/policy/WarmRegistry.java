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
package io.github.hyshmily.zeta.policy;

import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import java.util.function.Function;

/**
 * The startup-only namespace registry handed to {@link ZetaWarmConfigurer}.
 *
 * <p>One namespace has one identity: its cache name. The physical prefix is derived from the
 * resolved {@link KeyFormat}, so a caller never writes {@code '\u0000'} or {@code ':'}. A
 * namespace that is also declared by an annotation keeps the annotation's format and key type;
 * this registry cannot change either. Omitting {@link NamespaceRegistration#format(KeyFormat)}
 * therefore inherits the annotation's format, and the global default only applies to a namespace
 * no annotation declared.
 */
public interface WarmRegistry {
  /**
   * Register a typed namespace. The loader receives the decoded key, never the physical string.
   *
   * @param name    the logical cache name
   * @param keyType the single logical key type
   * @param <K>     the key type
   * @return the namespace builder
   */
  <K> NamespaceRegistration<K> namespace(String name, Class<K> keyType);

  /**
   * Register a namespace whose loader handles the physical key itself. The key type is not
   * inspected, so a raw namespace works with a PLAIN format without locking a scalar type.
   *
   * @param name the logical cache name
   * @return the namespace builder
   */
  NamespaceRegistration<String> namespaceRaw(String name);

  /**
   * Register a deployment-wide loader for keys that carry no registered namespace.
   *
   * @param loader the fallback loader
   */
  void fallbackLoader(CacheLoader<String, Object> loader);

  /** One namespace declaration. */
  interface NamespaceRegistration<K> {
    /**
     * Set the physical key format. It is rejected when the namespace is annotation-owned and
     * already resolved to another format.
     *
     * @param format the format
     * @return this builder
     */
    NamespaceRegistration<K> format(KeyFormat format);

    /**
     * Bind a stateless loader. The same loader serves prewarm, keep-warm and a peer REFRESH.
     *
     * <p>Optional: a namespace whose loader is bound by an annotation ({@code selfWarmLoader}) can
     * be registered with triggers only, so one configurer can manage the triggers of every
     * namespace without restating the value path.
     *
     * @param loader the loader
     * @return this builder
     */
    NamespaceRegistration<K> loader(Function<K, ?> loader);

    /**
     * Bind a loader that owns admission or reload behaviour. It is named separately because a
     * lambda is ambiguous between this SPI and {@link #loader(Function)}.
     *
     * @param loader the loader
     * @return this builder
     */
    NamespaceRegistration<K> cacheLoader(CacheLoader<K, ?> loader);

    /**
     * Set the code-side default prewarm trigger.
     *
     * @param trigger the trigger
     * @return this builder
     */
    NamespaceRegistration<K> prewarm(HeatTrigger trigger);

    /**
     * Set the code-side default keep-warm trigger.
     *
     * @param trigger the trigger
     * @return this builder
     */
    NamespaceRegistration<K> keepWarm(HeatTrigger trigger);

    /**
     * Publish the declaration. Every builder must be registered: a builder that is created and
     * never published fails startup with its namespace name instead of silently dropping the
     * declaration.
     *
     * @throws IllegalStateException when the builder declares no loader and no trigger
     */
    void register();
  }
}
