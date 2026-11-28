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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hyshmily.zeta.annotation.support.NamespaceKeys;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.policy.KeyFormat;
import io.github.hyshmily.zeta.policy.NamespacePolicyRegistry;
import java.lang.reflect.Method;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Pins the plan-time namespace checks: a declared PLAIN type must match the inferred key. */
class PlanResolverNamespaceTest {

  private PlanResolver resolver;
  private NamespaceKeys keys;

  @BeforeEach
  void setUp() {
    NamespacePolicyRegistry contracts = new NamespacePolicyRegistry(new ZetaProperties(), null);
    keys = new NamespaceKeys(KeyFormat.TYPED);
    resolver = new PlanResolver(null, () -> contracts, () -> keys);
  }

  @ZetaConfig(cache = "plainUsers", keyFormat = KeyFormat.Option.PLAIN)
  static final class PlainNamespace {

    @ZetaCached(cache = "plainUsers", key = "#id")
    public String load(Long id) {
      return String.valueOf(id);
    }
  }

  static final class ClearingService {

    @ZetaInvalidate(cache = "plainUsers", allEntries = true)
    public void clear() {}
  }

  @Test
  void aTableWideClearAfterAPlainDeclarationDoesNotConflict() throws Exception {
    Method load = PlainNamespace.class.getDeclaredMethod("load", Long.class);
    Method clear = ClearingService.class.getDeclaredMethod("clear");

    assertThat(resolver.matches(load, PlainNamespace.class)).isTrue();
    assertThat(resolver.matches(clear, ClearingService.class)).isTrue();
    assertThat(keys.formats()).containsEntry("plainUsers", KeyFormat.PLAIN);
  }

  @Test
  void aTableWideClearBeforeAPlainDeclarationDoesNotConflict() throws Exception {
    Method load = PlainNamespace.class.getDeclaredMethod("load", Long.class);
    Method clear = ClearingService.class.getDeclaredMethod("clear");

    assertThat(resolver.matches(clear, ClearingService.class)).isTrue();
    assertThat(resolver.matches(load, PlainNamespace.class)).isTrue();
    assertThat(keys.formats()).containsEntry("plainUsers", KeyFormat.PLAIN);
  }

  static final class BadKey {

    @ZetaCached(cache = "fresh", key = "'unclosed")
    public String load(String id) {
      return id;
    }
  }

  @Test
  void aScanThatFailsAfterRegisteringANamespacePublishesNoKeyFormat() throws Exception {
    Method load = BadKey.class.getDeclaredMethod("load", String.class);

    assertThatThrownBy(() -> resolver.matches(load, BadKey.class))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("cannot be parsed");
    assertThat(keys.formats()).doesNotContainKey("fresh");
  }

  @Test
  void aFailedSecondWarmLoader_shouldNotPublishEitherDeclaration() throws Exception {
    Method first = FirstLoader.class.getDeclaredMethod("load", String.class);
    Method second = SecondLoader.class.getDeclaredMethod("load", String.class);

    assertThat(resolver.matches(first, FirstLoader.class)).isTrue();
    assertThat(resolver.warmSpecs()).hasSize(1);
    assertThatThrownBy(() -> resolver.matches(second, SecondLoader.class))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("declares more than one selfWarmLoader method");

    assertThat(resolver.warmSpecs()).hasSize(1);
    assertThat(resolver.warmSpecs().iterator().next().beanClass()).isEqualTo(FirstLoader.class);
  }

  @Test
  void siblingSubclassesSharingANamespaceFailInsteadOfDroppingOne() throws Exception {
    Method load = ParentLoader.class.getDeclaredMethod("load", String.class);

    assertThat(resolver.matches(load, SiblingLoaderA.class)).isTrue();
    assertThatThrownBy(() -> resolver.matches(load, SiblingLoaderB.class))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("declares more than one selfWarmLoader method")
      .hasMessageContaining(SiblingLoaderA.class.getName())
      .hasMessageContaining(SiblingLoaderB.class.getName());
  }

  @Test
  void parentAndChildBeansSharingANamespaceFailInsteadOfOverwritingTheParent() throws Exception {
    Method load = ParentLoader.class.getDeclaredMethod("load", String.class);

    assertThat(resolver.matches(load, ParentLoader.class)).isTrue();
    assertThatThrownBy(() -> resolver.matches(load, ChildLoader.class))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("declares more than one selfWarmLoader method")
      .hasMessageContaining(ParentLoader.class.getName())
      .hasMessageContaining(ChildLoader.class.getName());
  }

  @ZetaConfig(cache = "users", keyFormat = KeyFormat.Option.PLAIN)
  static final class Mismatched {

    @ZetaCached(cache = "users", key = "#name")
    public String byName(String name) {
      return name;
    }
  }

  @ZetaConfig(cache = "dup")
  static final class FirstLoader {

    @ZetaCached(cache = "dup", key = "#id", selfWarmLoader = true)
    public String load(String id) {
      return id;
    }
  }

  @ZetaConfig(cache = "dup")
  static final class SecondLoader {

    @ZetaCached(cache = "dup", key = "#id", selfWarmLoader = true)
    public String load(String id) {
      return id;
    }
  }

  @ZetaConfig(cache = "shared")
  static class ParentLoader {

    @ZetaCached(cache = "shared", key = "#id", selfWarmLoader = true)
    public String load(String id) {
      return id;
    }
  }

  static final class SiblingLoaderA extends ParentLoader {}

  static final class SiblingLoaderB extends ParentLoader {}

  /** A distinct bean that inherits the parent's warmed method and namespace. */
  static final class ChildLoader extends ParentLoader {}
}
