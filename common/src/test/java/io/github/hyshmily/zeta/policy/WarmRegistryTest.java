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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hyshmily.zeta.annotation.support.NamespaceKeys;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import java.lang.reflect.Method;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("WarmRegistry")
class WarmRegistryTest {

  private static NamespacePolicyRegistry policies() {
    return new NamespacePolicyRegistry(new ZetaProperties(), null);
  }

  private static Method sampleMethod() throws NoSuchMethodException {
    return WarmRegistryTest.class.getDeclaredMethod("sample", Long.class);
  }

  @SuppressWarnings("unused")
  private Long sample(Long id) {
    return id;
  }

  @Test
  @DisplayName("a typed loader receives the decoded key and serves the namespace prefix")
  void typedLoader() {
    NamespacePolicyRegistry policies = policies();
    WarmRegistry registry = DefaultWarmRegistry.create(new NamespaceKeys(KeyFormat.TYPED), policies);

    registry
      .namespace("users", Long.class)
      .loader(id -> "v-" + id)
      .prewarm(HeatTrigger.CLUSTER)
      .keepWarm(HeatTrigger.ANY)
      .register();

    ResolvedNamespacePolicy policy = policies.resolve("users\u0000[J:42]");
    assertThat(policy.prewarm()).isEqualTo(HeatTrigger.CLUSTER);
    assertThat(policy.keepWarm()).isEqualTo(HeatTrigger.ANY);
    assertThat(policy.loader()).isNotNull();
    assertThat(policy.loader().load("users\u0000[J:42]")).isEqualTo("v-42");
    assertThat(policy.loader().load("other\u0000[J:42]")).isNull();
  }

  @Test
  @DisplayName("a primitive key type is boxed before the loader decodes a physical key")
  void primitiveKeyType() {
    NamespacePolicyRegistry policies = policies();
    WarmRegistry registry = DefaultWarmRegistry.create(new NamespaceKeys(KeyFormat.TYPED), policies);

    registry
      .namespace("users", long.class)
      .loader(id -> "v-" + id)
      .register();

    CacheLoader<String, Object> loader = policies.resolve("users\u0000[J:42]").loader();
    assertThat(loader).isNotNull();
    assertThat(loader.load("users\u0000[J:42]")).isEqualTo("v-42");
    assertThat(loader.reload("users\u0000[J:42]")).isEqualTo("v-42");
  }

  @Test
  @DisplayName("a PLAIN loader decodes namespace:value")
  void plainLoader() {
    NamespacePolicyRegistry policies = policies();
    WarmRegistry registry = DefaultWarmRegistry.create(new NamespaceKeys(KeyFormat.TYPED), policies);

    registry
      .namespace("users", Long.class)
      .format(KeyFormat.PLAIN)
      .loader(id -> "v-" + id)
      .register();

    CacheLoader<String, Object> loader = policies.resolve("users:42").loader();
    assertThat(loader).isNotNull();
    assertThat(loader.load("users:42")).isEqualTo("v-42");
    assertThat(loader.load("users:nope")).isNull();
  }

  @Test
  @DisplayName("an omitted format inherits the annotation's PLAIN format")
  void omittedFormatInheritsTheAnnotation() throws Exception {
    NamespacePolicyRegistry policies = policies();
    NamespaceKeys keys = new NamespaceKeys(KeyFormat.TYPED);
    NamespaceKeys.Staging staging = keys.staging();
    staging.stage("users", KeyFormat.Option.PLAIN, sampleMethod(), true);
    staging.commit();
    WarmRegistry registry = DefaultWarmRegistry.create(keys, policies);

    registry
      .namespace("users", Long.class)
      .loader(id -> "v-" + id)
      .register();

    assertThat(policies.resolve("users:42").loader().load("users:42")).isEqualTo("v-42");
  }

  @Test
  @DisplayName("a raw loader owns the physical key and works with a PLAIN format and no scalar type")
  void rawLoader() {
    NamespacePolicyRegistry policies = policies();
    WarmRegistry registry = DefaultWarmRegistry.create(new NamespaceKeys(KeyFormat.TYPED), policies);

    registry
      .namespaceRaw("raw")
      .loader((String key) -> "raw-" + key)
      .register();
    registry
      .namespaceRaw("orders")
      .format(KeyFormat.PLAIN)
      .loader((String key) -> "order-" + key)
      .register();

    assertThat(policies.resolve("raw\u0000anything").loader().load("raw\u0000anything")).isEqualTo(
      "raw-raw\u0000anything"
    );
    assertThat(policies.resolve("orders:1").loader().load("orders:1")).isEqualTo("order-orders:1");
  }

  @Test
  @DisplayName("a builder with neither a loader nor a trigger fails registration")
  void emptyDeclaration() {
    WarmRegistry registry = DefaultWarmRegistry.create(new NamespaceKeys(KeyFormat.TYPED), policies());

    assertThatThrownBy(() -> registry.namespace("users", Long.class).register())
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("declares neither a loader nor a trigger");
  }

  @Test
  @DisplayName("a triggers-only registration keeps the loader another declaration bound")
  void triggersOnlyRegistration() {
    NamespacePolicyRegistry policies = policies();
    WarmRegistry registry = DefaultWarmRegistry.create(new NamespaceKeys(KeyFormat.TYPED), policies);
    registry
      .namespace("users", Long.class)
      .loader(id -> "v-" + id)
      .register();

    registry.namespace("users", Long.class).keepWarm(HeatTrigger.ANY).register();

    ResolvedNamespacePolicy policy = policies.resolve("users\u0000[J:42]");
    assertThat(policy.keepWarm()).isEqualTo(HeatTrigger.ANY);
    assertThat(policy.loader()).isNotNull();
    assertThat(policy.loader().load("users\u0000[J:42]")).isEqualTo("v-42");
    policies.validateAndFreeze();
  }

  @Test
  @DisplayName("a builder that was never registered fails the startup check")
  void unregisteredBuilder() {
    DefaultWarmRegistry registry = DefaultWarmRegistry.create(new NamespaceKeys(KeyFormat.TYPED), policies());
    registry.namespace("users", Long.class).keepWarm(HeatTrigger.ANY);

    assertThatThrownBy(registry::assertFullyRegistered)
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("'users'");
  }

  @Test
  @DisplayName("a second loader for one namespace fails instead of overwriting")
  void duplicateLoader() {
    NamespacePolicyRegistry policies = policies();
    WarmRegistry registry = DefaultWarmRegistry.create(new NamespaceKeys(KeyFormat.TYPED), policies);
    registry
      .namespace("users", Long.class)
      .loader(id -> "first")
      .register();

    assertThatThrownBy(() ->
      registry
        .namespace("users", Long.class)
        .loader(id -> "second")
        .register()
    )
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("already has a background loader");
  }

  @Test
  @DisplayName("a fallback loader serves a key with no registered namespace")
  void fallbackLoader() {
    NamespacePolicyRegistry policies = policies();
    WarmRegistry registry = DefaultWarmRegistry.create(new NamespaceKeys(KeyFormat.TYPED), policies);
    registry.fallbackLoader(key -> "fallback");

    assertThat(policies.resolve("unregistered\u0000k").loader().load("unregistered\u0000k")).isEqualTo("fallback");
  }
}
