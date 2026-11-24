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

import io.github.hyshmily.zeta.annotation.support.KeyEncoding;
import io.github.hyshmily.zeta.annotation.support.NamespaceKeys;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.util.Internal;
import org.jspecify.annotations.Nullable;
import org.springframework.util.ClassUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * The default {@link WarmRegistry}. It derives the physical prefix from the namespace's resolved
 * format and adapts a typed loader to the physical-key SPI by decoding with the same
 * {@link KeyEncoding.Decoder} the background planes use. A namespace that has an annotation
 * declaration cannot change its format or key type here: {@link NamespaceKeys} owns those facts.
 */
@Internal
public final class DefaultWarmRegistry implements WarmRegistry {

  private final NamespaceKeys keys;
  private final NamespacePolicyRegistry policies;

  /** Builders created and not yet published; startup-only, single-threaded by contract. */
  private final List<Registration<?>> pending = new ArrayList<>();

  DefaultWarmRegistry(NamespaceKeys keys, NamespacePolicyRegistry policies) {
    this.keys = Objects.requireNonNull(keys, "keys");
    this.policies = Objects.requireNonNull(policies, "policies");
  }

  /**
   * Creates the default registry. Container wiring, not an extension point.
   *
   * @param keys     the plan-time key registry
   * @param policies the warm policy registry
   * @return the registry
   */
  public static DefaultWarmRegistry create(NamespaceKeys keys, NamespacePolicyRegistry policies) {
    return new DefaultWarmRegistry(keys, policies);
  }

  /**
   * Rejects every builder that was created and never published. Called once after all configurers
   * ran and before the policy registry freezes, so a forgotten {@link NamespaceRegistration#register()}
   * fails startup with the namespace name instead of silently dropping the declaration.
   *
   * @throws IllegalStateException when a builder was never registered
   */
  public void assertFullyRegistered() {
    if (pending.isEmpty()) {
      return;
    }
    StringBuilder names = new StringBuilder();
    for (Registration<?> registration : pending) {
      if (!names.isEmpty()) {
        names.append(", ");
      }
      names.append('\'').append(registration.name).append('\'');
    }
    throw new IllegalStateException(
      "warm namespace declarations were never registered: " + names + "; call register() on each builder"
    );
  }

  @Override
  public <K> NamespaceRegistration<K> namespace(String name, Class<K> keyType) {
    return track(new Registration<>(name, Objects.requireNonNull(keyType, "keyType"), false));
  }

  @Override
  public NamespaceRegistration<String> namespaceRaw(String name) {
    return track(new Registration<>(name, String.class, true));
  }

  private <K> NamespaceRegistration<K> track(Registration<K> registration) {
    pending.add(registration);
    return registration;
  }

  @Override
  public void fallbackLoader(CacheLoader<String, Object> loader) {
    policies.setFallbackLoader(Objects.requireNonNull(loader, "loader"));
  }

  /** One fluent declaration. */
  private final class Registration<K> implements NamespaceRegistration<K> {

    private final String name;
    private final Class<K> keyType;
    private final boolean raw;

    /** {@code null} inherits the annotation's format, else the global default. */
    private @Nullable KeyFormat format;
    private @Nullable CacheLoader<K, Object> loader;
    private @Nullable HeatTrigger prewarm;
    private @Nullable HeatTrigger keepWarm;

    private Registration(String name, Class<K> keyType, boolean raw) {
      this.name = name;
      this.keyType = keyType;
      this.raw = raw;
    }

    @Override
    public NamespaceRegistration<K> format(KeyFormat format) {
      this.format = Objects.requireNonNull(format, "format");
      return this;
    }

    @Override
    public NamespaceRegistration<K> loader(Function<K, ?> loader) {
      Objects.requireNonNull(loader, "loader");
      return cacheLoader(loader::apply);
    }

    @Override
    @SuppressWarnings("unchecked")
    public NamespaceRegistration<K> cacheLoader(CacheLoader<K, ?> loader) {
      if (this.loader != null) {
        throw new IllegalStateException("namespace '" + name + "' loader is already set");
      }
      this.loader = (CacheLoader<K, Object>) Objects.requireNonNull(loader, "loader");
      return this;
    }

    @Override
    public NamespaceRegistration<K> prewarm(HeatTrigger trigger) {
      this.prewarm = Objects.requireNonNull(trigger, "trigger");
      return this;
    }

    @Override
    public NamespaceRegistration<K> keepWarm(HeatTrigger trigger) {
      this.keepWarm = Objects.requireNonNull(trigger, "trigger");
      return this;
    }

    /**
     * Publish this declaration. A builder must declare a loader or a trigger; the format goes
     * through the plan-time registry, so a namespace an annotation already declared keeps the
     * annotation's facts and a conflicting request fails here rather than at freeze.
     *
     * @throws IllegalStateException when the builder declares neither a loader nor a trigger, or
     *                               contradicts an annotation-declared format
     */
    @Override
    @SuppressWarnings("unchecked")
    public void register() {
      if (loader == null && prewarm == null && keepWarm == null) {
        throw new IllegalStateException("namespace '" + name + "' declares neither a loader nor a trigger");
      }
      KeyEncoding encoding = keys.registerProgrammatic(name, format);
      CacheLoader<String, Object> adapted =
        loader == null
          ? null
          : raw
            ? (CacheLoader<String, Object>) loader
            : adaptTyped(encoding.singleDecoder(keyType), keyType, loader);
      policies.registerProgrammaticNamespace(encoding.physicalPrefix(), prewarm, keepWarm, adapted);
      pending.remove(this);
    }

    /**
     * Present a typed loader through the physical-key SPI every warming path uses. The path holds
     * a physical key and nothing else — no invocation, no caller — so the decoder is the inverse
     * of the namespace's encoder and a key of another shape is not this method's key.
     *
     * @param decoder  the namespace's single-argument decoder
     * @param keyType  the declared logical key type
     * @param delegate the typed loader
     * @return the physical-key loader
     */
    private static <K> CacheLoader<String, Object> adaptTyped(
      KeyEncoding.Decoder decoder,
      Class<K> keyType,
      CacheLoader<K, Object> delegate
    ) {
      Class<?> boxedKeyType = ClassUtils.resolvePrimitiveIfNecessary(keyType);
      return new CacheLoader<>() {
        @Override
        public @Nullable Object load(String physicalKey) {
          K logical = decodeKey(decoder, boxedKeyType, physicalKey);
          return logical == null ? null : delegate.load(logical);
        }

        @Override
        public @Nullable Object reload(String physicalKey) {
          K logical = decodeKey(decoder, boxedKeyType, physicalKey);
          return logical == null ? null : delegate.reload(logical);
        }

        @Override
        public boolean isCacheable(Object value) {
          return delegate.isCacheable(value);
        }
      };
    }

    /**
     * Decode one physical key back to the loader's key, or {@code null} when the key does not
     * belong to this namespace's single-argument shape. The type is checked rather than cast: a key
     * another namespace encoded can decode to an argument of the wrong type, and that is a miss for
     * this loader, not an exception on a warm thread.
     *
     * @param decoder      the namespace's decoder
     * @param boxedKeyType the boxed logical key type
     * @param physicalKey  the key the warming path holds
     * @return the logical key, or {@code null}
     */
    @SuppressWarnings("unchecked")
    private static <K> @Nullable K decodeKey(KeyEncoding.Decoder decoder, Class<?> boxedKeyType, String physicalKey) {
      Object[] args = decoder.decode(physicalKey);
      return args != null && args.length == 1 && boxedKeyType.isInstance(args[0]) ? (K) args[0] : null;
    }
  }
}
