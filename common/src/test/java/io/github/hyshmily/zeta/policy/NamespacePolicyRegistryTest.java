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

import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.TtlContract;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import org.junit.jupiter.api.Test;

/**
 * Pins the namespace policy contract: one resolution point for a namespace's TTL contract and its
 * two warming triggers, with a documented precedence and a safe answer for a key without a
 * namespace.
 */
class NamespacePolicyRegistryTest {

  private static final String KEY = "quotes\u0000q-1";

  private static ZetaProperties properties() {
    ZetaProperties props = new ZetaProperties();
    props.setHardTtlMs(300_000L);
    props.setSoftTtlMs(0L);
    return props;
  }

  @Test
  void aNameDeclaredInBothOverridesCachesAndPrefixes_failsInsteadOfSplitting() {
    ZetaProperties props = properties();
    ZetaProperties.Warm.TriggerOverride override = new ZetaProperties.Warm.TriggerOverride();
    override.setPrewarm(HeatTrigger.LOCAL);
    props.getWarm().getOverrides().getCaches().put("quotes", override);
    props.getWarm().getOverrides().getPrefixes().put("quotes", override);

    assertThatThrownBy(() -> new NamespacePolicyRegistry(props, null))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("both caches and prefixes");
  }

  @Test
  void resolve_shouldUseGlobalPolicyForKeyWithoutNamespace() {
    NamespacePolicyRegistry registry = new NamespacePolicyRegistry(properties(), null);

    ResolvedNamespacePolicy policy = registry.resolve("plain-key");

    assertThat(policy.ttl().hardTtlMs()).isEqualTo(300_000L);
    assertThat(policy.ttl().softTtlMs()).isZero();
    assertThat(policy.prewarm()).isEqualTo(HeatTrigger.OFF);
    assertThat(policy.keepWarm()).isEqualTo(HeatTrigger.OFF);
    assertThat(policy.prewarmSource()).isEqualTo(ResolvedNamespacePolicy.Source.GLOBAL);
  }

  @Test
  void resolve_shouldReadTheRegisteredAnnotationDeclaration() {
    NamespacePolicyRegistry registry = new NamespacePolicyRegistry(properties(), null);
    registry.registerAnnotatedNamespace("quotes\u0000", 60_000L, 30_000L, false, HeatTrigger.LOCAL, HeatTrigger.OFF);

    ResolvedNamespacePolicy policy = registry.resolve(KEY);

    assertThat(policy.ttl().hardTtlMs()).isEqualTo(60_000L);
    assertThat(policy.ttl().softTtlMs()).isEqualTo(30_000L);
    assertThat(policy.ttl().prewarmable()).isTrue();
    assertThat(policy.prewarm()).isEqualTo(HeatTrigger.LOCAL);
    assertThat(policy.prewarmSource()).isEqualTo(ResolvedNamespacePolicy.Source.ANNOTATION);
    assertThat(policy.keepWarm()).isEqualTo(HeatTrigger.OFF);
    assertThat(policy.keepWarmSource()).isEqualTo(ResolvedNamespacePolicy.Source.ANNOTATION);
  }

  @Test
  void resolve_shouldKeepWarmIndependentOfTheSoftWindow() {
    ZetaProperties props = properties();
    props.getWarm().getKeepWarm().setTrigger(HeatTrigger.ANY);
    NamespacePolicyRegistry registry = new NamespacePolicyRegistry(props, null);
    registry.registerAnnotatedNamespace("soft\u0000", 60_000L, 30_000L, false, null, null);
    registry.registerAnnotatedNamespace("hard-only\u0000", 60_000L, 0L, false, null, null);

    assertThat(registry.resolve("soft\u0000k").keepWarm()).isEqualTo(HeatTrigger.ANY);
    assertThat(registry.resolve("soft\u0000k").keepWarmSource()).isEqualTo(ResolvedNamespacePolicy.Source.GLOBAL);
    // A namespace without a soft window inherits the same trigger instead of being silently
    // switched off by the TTL shape.
    assertThat(registry.resolve("hard-only\u0000k").keepWarm()).isEqualTo(HeatTrigger.ANY);
  }

  @Test
  void resolve_shouldRefusePrewarmForADynamicNamespace() {
    NamespacePolicyRegistry registry = new NamespacePolicyRegistry(properties(), null);
    registry.registerAnnotatedNamespace("dyn\u0000", 60_000L, 30_000L, true, null, null);

    TtlContract contract = registry.resolveTtl("dyn\u0000k");

    assertThat(contract.dynamicTtl()).isTrue();
    assertThat(contract.prewarmable()).isFalse();
  }

  @Test
  void registerAnnotation_shouldFoldDisagreeingTtlsButRejectDisagreeingTriggers() {
    NamespacePolicyRegistry registry = new NamespacePolicyRegistry(properties(), null);
    registry.registerAnnotatedNamespace("mixed\u0000", 60_000L, 30_000L, false, null, null);
    registry.registerAnnotatedNamespace("mixed\u0000", 120_000L, 30_000L, false, null, null);
    assertThat(registry.resolveTtl("mixed\u0000k").dynamicTtl()).isTrue();

    NamespacePolicyRegistry conflicting = new NamespacePolicyRegistry(properties(), null);
    conflicting.registerAnnotatedNamespace("mixed\u0000", 60_000L, 0L, false, HeatTrigger.LOCAL, null);
    assertThatThrownBy(() ->
      conflicting.registerAnnotatedNamespace("mixed\u0000", 60_000L, 0L, false, HeatTrigger.CLUSTER, null)
    )
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("prewarm");
  }

  @Test
  void registerAnnotation_shouldFoldHardInheritSynonyms() {
    NamespacePolicyRegistry registry = new NamespacePolicyRegistry(properties(), null);
    registry.registerAnnotatedNamespace("quotes\u0000", 0L, 30_000L, false, null, null);
    registry.registerAnnotatedNamespace("quotes\u0000", -1L, 30_000L, false, null, null);

    TtlContract contract = registry.resolveTtl(KEY);

    assertThat(contract.dynamicTtl()).isFalse();
    assertThat(contract.hardTtlMs()).isEqualTo(300_000L);
  }

  @Test
  void yamlPrefixWithSeparator_shouldFailAtStartup() {
    ZetaProperties props = properties();
    props.getWarm().getOverrides().getPrefixes().put("user:", new ZetaProperties.Warm.TriggerOverride());

    assertThatThrownBy(() -> new NamespacePolicyRegistry(props, null))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("bare prefix");
  }

  @Test
  void yamlPrefixWithoutSeparator_shouldMatchAProgrammaticPrefix() {
    ZetaProperties props = properties();
    ZetaProperties.Warm.TriggerOverride override = new ZetaProperties.Warm.TriggerOverride();
    override.setKeepWarm(HeatTrigger.OFF);
    props.getWarm().getOverrides().getPrefixes().put("order", override);
    NamespacePolicyRegistry registry = new NamespacePolicyRegistry(props, null);
    registry.registerAnnotatedNamespace("order:", 60_000L, 0L, false, null, HeatTrigger.LOCAL);
    registry.validateAndFreeze();

    ResolvedNamespacePolicy policy = registry.resolve("order:1");

    assertThat(policy.keepWarm()).isEqualTo(HeatTrigger.OFF);
    assertThat(policy.keepWarmSource()).isEqualTo(ResolvedNamespacePolicy.Source.YAML_OVERRIDE);
  }

  @Test
  void yamlPrefixForATypedNamespace_shouldFailAtStartupInsteadOfSilentlyNeverMatching() {
    ZetaProperties props = properties();
    props.getWarm().getOverrides().getPrefixes().put("orders", new ZetaProperties.Warm.TriggerOverride());
    NamespacePolicyRegistry registry = new NamespacePolicyRegistry(props, null);
    registry.registerAnnotatedNamespace("orders\u0000", 60_000L, 0L, false, null, HeatTrigger.LOCAL);

    assertThatThrownBy(registry::validateAndFreeze)
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("unknown namespace 'orders:'");
  }

  @Test
  void resolve_shouldApplyTheDocumentedPrecedence() {
    ZetaProperties props = properties();
    ZetaProperties.Warm.TriggerOverride override = new ZetaProperties.Warm.TriggerOverride();
    override.setPrewarm(HeatTrigger.OFF);
    props.getWarm().getOverrides().getCaches().put("quotes", override);

    NamespacePolicyRegistry registry = new NamespacePolicyRegistry(props, null);
    registry.registerAnnotatedNamespace("quotes\u0000", 60_000L, 30_000L, false, HeatTrigger.LOCAL, HeatTrigger.LOCAL);
    registry.registerProgrammaticNamespace("quotes\u0000", HeatTrigger.ANY, HeatTrigger.ANY, null);

    ResolvedNamespacePolicy policy = registry.resolve(KEY);

    assertThat(policy.prewarm()).isEqualTo(HeatTrigger.OFF);
    assertThat(policy.prewarmSource()).isEqualTo(ResolvedNamespacePolicy.Source.YAML_OVERRIDE);
    assertThat(policy.keepWarm()).isEqualTo(HeatTrigger.ANY);
    assertThat(policy.keepWarmSource()).isEqualTo(ResolvedNamespacePolicy.Source.PROGRAMMATIC);
  }

  @Test
  void resolve_shouldPreferTheNamespaceLoaderOverTheGlobalOne() {
    CacheLoader<String, Object> global = key -> "global";
    CacheLoader<String, Object> namespaced = key -> "namespaced";
    NamespacePolicyRegistry registry = new NamespacePolicyRegistry(properties(), global);
    registry.registerAnnotatedNamespace("quotes\u0000", 60_000L, 30_000L, false, null, null);
    registry.registerProgrammaticNamespace("quotes\u0000", null, null, namespaced);

    assertThat(registry.resolve(KEY).loader()).isSameAs(namespaced);
    assertThat(registry.resolve("other\u0000k").loader()).isSameAs(global);
  }

  @Test
  void freeze_shouldPublishTheSnapshotAndIgnoreALateRegistration() {
    NamespacePolicyRegistry registry = new NamespacePolicyRegistry(properties(), null);
    registry.registerAnnotatedNamespace("quotes\u0000", 60_000L, 30_000L, false, null, null);
    registry.validateAndFreeze();

    assertThat(registry.resolve(KEY).ttl().hardTtlMs()).isEqualTo(60_000L);
    registry.registerAnnotatedNamespace("later\u0000", 1_000L, 0L, false, null, null);
    assertThat(registry.resolve("later\u0000k").ttl().hardTtlMs()).isNotEqualTo(1_000L);
  }

  /**
   * A programmatic declaration after the freeze cannot reach the published snapshot, so accepting
   * it would drop the declaration while its builder reports success and leaves the warm registry's
   * pending set. It fails instead — only an annotation, whose declaring bean may legitimately be
   * created after the freeze, is tolerated.
   */
  @Test
  void registerProgrammaticNamespace_afterFreeze_shouldFailInsteadOfDropping() {
    NamespacePolicyRegistry registry = new NamespacePolicyRegistry(properties(), null);
    registry.validateAndFreeze();

    assertThatThrownBy(() ->
      registry.registerProgrammaticNamespace("late\u0000", HeatTrigger.LOCAL, null, key -> "v")
    )
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("already frozen")
      .hasMessageContaining("late");
    assertThat(registry.resolve("late\u0000k").loader()).isNull();
  }

  @Test
  void setFallbackLoader_afterFreeze_shouldFailInsteadOfDropping() {
    NamespacePolicyRegistry registry = new NamespacePolicyRegistry(properties(), null);
    registry.validateAndFreeze();

    assertThatThrownBy(() -> registry.setFallbackLoader(key -> "v"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("already frozen");
    assertThat(registry.resolve("plain-key").loader()).isNull();
  }
}
