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

import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.CacheKeysPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.TtlContract;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.TtlPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import io.github.hyshmily.zeta.util.CollectionSizing;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.Merge;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.util.Assert;

/**
 * The one resolution point for a namespace's TTL and background-warming contract.
 *
 * <p><b>Why a registry.</b> A cold prewarm has no invocation to ask, a keep-warm task outlives the
 * call that created it, and several read entry points may address the same key. The policy is
 * therefore a namespace fact, not a per-call option. This registry is written at startup by the
 * annotation planner and by {@link ZetaWarmConfigurer}s, and read by the prewarm/keep-warm paths
 * that already hold the key.
 *
 * <p><b>One key space.</b> Every internal table is keyed by the physical prefix a key resolves to
 * ({@code "users:"} or {@code "users\u0000"}), never by a bare logical name. A YAML
 * {@code overrides.caches} entry is registered under both possible delimiters because the format is
 * not known when the registry is constructed; a {@code overrides.prefixes} entry is registered as
 * {@code prefix + ':'} so it matches a programmatic key's first segment. With that one rule,
 * validation never has to guess which form an operator meant.
 *
 * <p><b>Freeze.</b> {@link #validateAndFreeze()} resolves every known namespace once and publishes
 * one immutable {@link FrozenSnapshot} holding the prefix map, the default policy and the keep-warm
 * aggregate. A hot-path {@link #resolve(String)} is then one prefix read and one immutable-map
 * lookup, with no string splitting beyond the prefix and no temporary policy objects.
 *
 * <p><b>Precedence.</b> A YAML operator override wins over a programmatic registration, which
 * wins over an annotation, which wins over the global default. Overrides may only change a
 * trigger: an operator kill switch is safe, while a second TTL/loader declaration would be a
 * second owner.
 *
 * <p><b>Thread-safety.</b> Registration happens while beans are being proxied; reads happen on
 * decision, prewarm and scheduler threads. All maps are concurrent and their values immutable.
 */
@Internal
@Slf4j
public final class NamespacePolicyRegistry {

  /** One source of warmed methods that must be bound before the warm registry freezes. */
  @FunctionalInterface
  public interface WarmLoaderBinder {
    /** Bind every warmed method this source owns. */
    void bindAll();
  }

  private final ZetaProperties properties;
  private final Map<String, AnnotationPolicy> yamlOverrides;
  private final ConcurrentMap<String, AnnotationPolicy> annotations = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, ProgrammaticWarm> programmaticWarms = new ConcurrentHashMap<>();
  private final @Nullable CacheLoader<String, Object> globalLoader;

  /** Programmatic fallback registered by a {@link ZetaWarmConfigurer}; wins over the bean. */
  private volatile @Nullable CacheLoader<String, Object> fallbackLoader;

  /**
   * The frozen policies, the default policy and the keep-warm aggregate, published as one
   * immutable object so a reader cannot observe two generations at once.
   */
  private volatile @Nullable FrozenSnapshot frozen;

  /**
   * @param properties   the live configuration supplying global defaults
   * @param globalLoader the optional deployment-wide warmable loader
   */
  public NamespacePolicyRegistry(ZetaProperties properties, @Nullable CacheLoader<String, Object> globalLoader) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.globalLoader = globalLoader;
    this.yamlOverrides = buildOverrides(properties.getWarm().getOverrides());
  }

  /**
   * Registers one annotated method's declaration. Repeated identical declarations are idempotent;
   * a TTL disagreement marks the namespace dynamic, and two explicit warming triggers that
   * disagree fail startup because a wrong prewarm can hit a data source.
   *
   * @param namespace  the physical namespace prefix (ends with the delimiter)
   * @param hardTtlMs  declared hard TTL ({@code 0}/{@code -1} = inherit)
   * @param softTtlMs  declared soft TTL ({@code 0} = inherit, {@code -1} = disabled)
   * @param dynamicTtl whether a TTL attribute is evaluated per call
   * @param prewarm    declared prewarm trigger, or {@code null}
   * @param keepWarm   declared keep-warm trigger, or {@code null}
   */
  public void registerAnnotatedNamespace(
    String namespace,
    long hardTtlMs,
    long softTtlMs,
    boolean dynamicTtl,
    @Nullable HeatTrigger prewarm,
    @Nullable HeatTrigger keepWarm
  ) {
    Assert.hasText(namespace, "namespace must not be blank");
    if (frozen != null) {
      log.warn("Annotated namespace registered after freeze, ignored: namespace='{}'", displayNamespace(namespace));
      return;
    }
    TtlContract candidateTtl = dynamicTtl
      ? new TtlContract(properties.getHardTtlMs(), properties.getSoftTtlMs(), true)
      : new TtlContract(
          TtlPolicy.resolveHardTtl(hardTtlMs, properties.getHardTtlMs()),
          TtlPolicy.resolveSoftTtl(softTtlMs, properties.getSoftTtlMs()),
          false
        );
    String display = displayNamespace(namespace);
    // One compute keeps the triggers and the TTL baseline of a namespace consistent: a conflicting
    // declaration throws before any part of it is recorded.
    annotations.compute(namespace, (key, existing) -> {
      HeatTrigger mergedPrewarm = mergeTrigger(
        existing == null ? null : existing.prewarm(),
        prewarm,
        display,
        "prewarm"
      );
      HeatTrigger mergedKeepWarm = mergeTrigger(
        existing == null ? null : existing.keepWarm(),
        keepWarm,
        display,
        "keepWarm"
      );
      // Declarations are compared by their effective TTL values, so an empty attribute and an
      // explicit value equal to the global default agree; any real disagreement is dynamic.
      TtlContract mergedTtl =
        existing == null || existing.ttl().equals(candidateTtl)
          ? candidateTtl
          : new TtlContract(properties.getHardTtlMs(), properties.getSoftTtlMs(), true);
      return new AnnotationPolicy(mergedPrewarm, mergedKeepWarm, mergedTtl, false);
    });
  }

  /**
   * Registers one programmatic declaration under an already resolved physical prefix.
   *
   * @param namespace the physical prefix, including the format delimiter
   * @param prewarm   the trigger for loading a missing entry, or {@code null} to inherit
   * @param keepWarm  the trigger for renewing a resident entry, or {@code null} to inherit
   * @param loader    the namespace-scoped loader, or {@code null}
   */
  public void registerProgrammaticNamespace(
    String namespace,
    @Nullable HeatTrigger prewarm,
    @Nullable HeatTrigger keepWarm,
    @Nullable CacheLoader<String, ?> loader
  ) {
    Assert.hasText(namespace, "namespace must not be blank");
    if (frozen != null) {
      return;
    }
    Assert.isTrue(CacheKeysPolicy.hasTrailingDelimiter(namespace), "namespace must end with a key delimiter");
    programmaticWarms.compute(namespace, (key, existing) -> {
      if (existing == null) {
        return new ProgrammaticWarm(prewarm, keepWarm, loader);
      }
      if (existing.loader() != null && loader != null) {
        throw new IllegalStateException(
          "namespace '" + key + "' already has a background loader; keep one registration"
        );
      }
      return new ProgrammaticWarm(
        mergeTrigger(existing.prewarm(), prewarm, key, "prewarm"),
        mergeTrigger(existing.keepWarm(), keepWarm, key, "keepWarm"),
        loader != null ? loader : existing.loader()
      );
    });
    AnnotationPolicy override = yamlOverrides.get(namespace);
    if (
      override != null &&
      ((prewarm != null && override.prewarm() != null) || (keepWarm != null && override.keepWarm() != null))
    ) {
      log.warn(
        "YAML warm override shadows the programmatic registration for namespace '{}'; the override wins",
        displayNamespace(namespace)
      );
    }
  }

  /**
   * Registers the deployment-wide fallback used when a key carries no namespace loader.
   *
   * @param loader the fallback loader, or {@code null} to clear it
   */
  public void setFallbackLoader(@Nullable CacheLoader<String, Object> loader) {
    Assert.isTrue(
      fallbackLoader == null || loader == null,
      "a fallback loader is already registered; keep one registration"
    );
    if (frozen != null) {
      return;
    }
    this.fallbackLoader = loader;
  }

  /**
   * Validates every registered namespace and freezes the registry into an immutable snapshot.
   *
   * <p>An override naming no declaration is a typo that would silently disable an emergency stop,
   * so it fails startup. An active trigger without a loader, or a cold prewarm on a dynamic TTL,
   * would fail on the first key, so both fail here instead. Once frozen, the hot path is one lookup.
   *
   * @throws IllegalStateException when an override names an unknown namespace, an active namespace
   *                               has no loader, or a prewarm has no static TTL
   */
  public void validateAndFreeze() {
    if (frozen != null) {
      return;
    }
    Set<String> known = new LinkedHashSet<>();
    known.addAll(annotations.keySet());
    known.addAll(programmaticWarms.keySet());
    for (Map.Entry<String, AnnotationPolicy> entry : yamlOverrides.entrySet()) {
      String namespace = entry.getKey();
      if (known.contains(namespace)) {
        continue;
      }
      // Only a caches override is registered under both physical forms. A prefixes entry is
      // registered as prefix + ':' and must match exactly: accepting its alternate form let an
      // operator's emergency stop for a TYPED namespace pass startup validation and then never
      // match the key at runtime.
      String alternate = entry.getValue().dualRegistered() ? alternateDelimiter(namespace) : null;
      if (alternate != null && known.contains(alternate)) {
        continue;
      }
      throw new IllegalStateException(
        "zeta.local.warm.overrides names unknown namespace '" +
          displayNamespace(namespace) +
          "'; known namespaces: " +
          known
      );
    }

    Map<String, ResolvedNamespacePolicy> snapshot = new HashMap<>(CollectionSizing.capacity(known.size()));
    boolean anyKeepWarm = false;
    for (String namespace : known) {
      ResolvedNamespacePolicy policy = computePolicy(namespace);
      boolean active = policy.prewarm() != HeatTrigger.OFF || policy.keepWarm() != HeatTrigger.OFF;
      if (active && policy.loader() == null) {
        throw new IllegalStateException(
          "namespace '" +
            displayNamespace(namespace) +
            "' activates prewarm=" +
            policy.prewarm() +
            " (" +
            policy.prewarmSource() +
            "), keepWarm=" +
            policy.keepWarm() +
            " (" +
            policy.keepWarmSource() +
            ") but has no background loader"
        );
      }
      if (policy.prewarm() != HeatTrigger.OFF && !policy.ttl().prewarmable()) {
        throw new IllegalStateException(
          "namespace '" +
            displayNamespace(namespace) +
            "' activates prewarm=" +
            policy.prewarm() +
            " but its TTL is dynamic or conflicting and cannot be cold-prewarmed"
        );
      }
      anyKeepWarm |= policy.keepWarm() != HeatTrigger.OFF && policy.loader() != null;
      snapshot.put(namespace, policy);
    }
    // A key with no namespace resolves against this default, so its own keep-warm policy is part
    // of the aggregate the scheduler short-circuits on.
    ResolvedNamespacePolicy defaultPolicy = computePolicy(null);
    anyKeepWarm |= defaultPolicy.keepWarm() != HeatTrigger.OFF && defaultPolicy.loader() != null;
    this.frozen = new FrozenSnapshot(Map.copyOf(snapshot), defaultPolicy, anyKeepWarm);
  }

  /**
   * Whether any namespace in this application can ever schedule a renewal. The scheduler asks this
   * once per tide: a deployment whose namespaces all leave keep-warm off pays one boolean read
   * instead of a policy lookup and a heat probe per read key. Before the freeze the answer is
   * {@code true}, because the registry can still gain a declaration.
   *
   * @return {@code true} when at least one namespace has a keep-warm trigger and a loader
   */
  public boolean hasActiveKeepWarm() {
    FrozenSnapshot snapshot = frozen;
    return snapshot == null || snapshot.activeKeepWarm();
  }

  /**
   * Resolves a physical key's effective policy.
   *
   * <p>A frozen registry answers with one prefix read and one map lookup. A key that carries no
   * namespace resolves against the global default.
   *
   * @param cacheKey the physical key
   * @return the resolved TTL, triggers, sources and loader
   */
  public ResolvedNamespacePolicy resolve(String cacheKey) {
    String prefix = CacheKeysPolicy.physicalPrefixOf(cacheKey);
    FrozenSnapshot snapshot = frozen;
    if (snapshot == null) {
      return computePolicy(prefix);
    }
    ResolvedNamespacePolicy policy = prefix == null ? null : snapshot.policies().get(prefix);
    return policy != null ? policy : snapshot.defaultPolicy();
  }

  /**
   * The TTL contract of a physical key, independent of the warming policy.
   *
   * @param cacheKey the physical key
   * @return the resolved TTL contract
   */
  public TtlContract resolveTtl(String cacheKey) {
    return resolve(cacheKey).ttl();
  }

  /**
   * Resolve one namespace's policy from every source, in precedence order.
   *
   * <p>Triggers follow YAML override &gt; programmatic &gt; annotation &gt; global. The loader is a
   * separate decision: a namespace with any declaration owns it, so a namespace that deliberately
   * registered no loader never borrows the deployment-wide one — only a key with no namespace at
   * all reaches the fallback.
   *
   * @param namespace the physical prefix, or {@code null} for a key with no namespace
   * @return the resolved policy, never {@code null}
   */
  private ResolvedNamespacePolicy computePolicy(@Nullable String namespace) {
    AnnotationPolicy yaml = namespace == null ? null : yamlOverrides.get(namespace);
    ProgrammaticWarm programmatic = namespace == null ? null : programmaticWarms.get(namespace);
    AnnotationPolicy annotation = namespace == null ? null : annotations.get(namespace);
    // A dynamic declaration has no single baseline to publish, so it reports the global contract and
    // marks itself dynamic: cold prewarm is refused rather than guessed, while a renewal still uses
    // the resident's stored durations.
    TtlContract ttl =
      annotation != null
        ? annotation.ttl()
        : new TtlContract(properties.getHardTtlMs(), properties.getSoftTtlMs(), false);

    TriggerChoice prewarm = resolveTrigger(
      yaml == null ? null : yaml.prewarm(),
      programmatic == null ? null : programmatic.prewarm(),
      annotation == null ? null : annotation.prewarm(),
      properties.getWarm().getPrewarm().getTrigger()
    );
    // keepWarm is independent of softTtl. A soft window is a read-path stale policy; a renewal is
    // a background-load policy, and coupling them made a soft-TTL edit silently enable spending.
    TriggerChoice keepWarm = resolveTrigger(
      yaml == null ? null : yaml.keepWarm(),
      programmatic == null ? null : programmatic.keepWarm(),
      annotation == null ? null : annotation.keepWarm(),
      properties.getWarm().getKeepWarm().getTrigger()
    );

    // A registered namespace owns its loader decision. Falling back to a deployment-wide loader
    // for a namespace that deliberately has none would let a global bean warm another
    // namespace's key format. Only a key with no namespace at all reaches the fallback.
    boolean known = namespace != null && (annotation != null || programmatic != null || yaml != null);
    @SuppressWarnings("unchecked")
    CacheLoader<String, Object> loader =
      programmatic != null && programmatic.loader() != null
        ? (CacheLoader<String, Object>) programmatic.loader()
        : known
          ? null
          : fallbackLoader != null
            ? fallbackLoader
            : globalLoader;
    return new ResolvedNamespacePolicy(
      ttl,
      prewarm.trigger(),
      prewarm.source(),
      keepWarm.trigger(),
      keepWarm.source(),
      loader
    );
  }

  /**
   * The one precedence chain for a warming trigger: YAML override, programmatic registration,
   * annotation declaration, then the caller-supplied global default.
   */
  private static TriggerChoice resolveTrigger(
    @Nullable HeatTrigger yaml,
    @Nullable HeatTrigger programmatic,
    @Nullable HeatTrigger annotation,
    HeatTrigger globalDefault
  ) {
    if (yaml != null) {
      return new TriggerChoice(yaml, ResolvedNamespacePolicy.Source.YAML_OVERRIDE);
    }
    if (programmatic != null) {
      return new TriggerChoice(programmatic, ResolvedNamespacePolicy.Source.PROGRAMMATIC);
    }
    if (annotation != null) {
      return new TriggerChoice(annotation, ResolvedNamespacePolicy.Source.ANNOTATION);
    }
    return new TriggerChoice(globalDefault, ResolvedNamespacePolicy.Source.GLOBAL);
  }

  private static @Nullable HeatTrigger mergeTrigger(
    @Nullable HeatTrigger existing,
    @Nullable HeatTrigger candidate,
    String namespace,
    String attribute
  ) {
    return Merge.mergeOrThrow(
      existing,
      candidate,
      () ->
        "Conflicting " + attribute + " declarations in namespace " + namespace + ": " + existing + " vs " + candidate
    );
  }

  /**
   * Renders a physical prefix for a log line or an exception: the NUL namespace delimiter is
   * invisible in a terminal and would make two namespaces look identical.
   */
  private static String displayNamespace(String namespace) {
    return namespace.replace("\u0000", "\\u0000");
  }

  /** The same logical namespace under the other physical delimiter, or {@code null}. */
  private static @Nullable String alternateDelimiter(String namespace) {
    int end = namespace.length() - 1;
    if (end < 0) {
      return null;
    }
    char last = namespace.charAt(end);
    if (last == CacheKeysPolicy.PLAIN_DELIMITER) {
      return namespace.substring(0, end) + CacheKeysPolicy.NAMESPACE_DELIMITER;
    }
    if (last == CacheKeysPolicy.NAMESPACE_DELIMITER) {
      return namespace.substring(0, end) + CacheKeysPolicy.PLAIN_DELIMITER;
    }
    return null;
  }

  /**
   * Read the operator overrides into the physical prefixes a key resolves to.
   *
   * <p>An override is written against a logical name, but every internal table is keyed by the
   * physical prefix, and the format that chooses the delimiter is declared later by an annotation
   * or a programmatic registration. A {@code caches} entry is therefore registered under both
   * possible prefixes; exactly one of them can match a key. A {@code prefixes} entry names a bare
   * prefix and is extended the same way a programmatic key is split.
   *
   * @param overrides the configured overrides
   * @return an immutable prefix-to-declaration map
   */
  private static Map<String, AnnotationPolicy> buildOverrides(ZetaProperties.Warm.Overrides overrides) {
    Map<String, AnnotationPolicy> resolved = new LinkedHashMap<>();
    overrides
      .getCaches()
      .forEach((cache, override) -> {
        if (cache != null && !cache.isBlank() && override != null) {
          if (CacheKeysPolicy.containsDelimiter(cache)) {
            throw new IllegalStateException(
              "zeta.local.warm.overrides.caches key '" + cache + "' must be a bare cache name without ':' or U+0000"
            );
          }
          AnnotationPolicy value = AnnotationPolicy.yaml(override.getPrewarm(), override.getKeepWarm(), true);
          // The format is not known when the registry is constructed, so one logical cache name is
          // registered under both physical prefixes. A namespace resolves to exactly one of them.
          resolved.put(CacheKeysPolicy.namespacePrefix(cache), value);
          resolved.put(cache + CacheKeysPolicy.PLAIN_DELIMITER, value);
        }
      });
    overrides
      .getPrefixes()
      .forEach((prefix, override) -> {
        if (prefix == null || prefix.isBlank() || override == null) {
          return;
        }
        // A physical prefix is what resolve() looks up; a bare prefix must be extended exactly as
        // physicalPrefixOf("prefix:id") would, or the override can never match.
        if (CacheKeysPolicy.containsDelimiter(prefix)) {
          throw new IllegalStateException(
            "zeta.local.warm.overrides.prefixes key '" + prefix + "' must be a bare prefix without ':' or U+0000"
          );
        }
        String plainPrefix = prefix + CacheKeysPolicy.PLAIN_DELIMITER;
        if (resolved.containsKey(plainPrefix)) {
          throw new IllegalStateException(
            "zeta.local.warm.overrides defines '" + prefix + "' in both caches and prefixes"
          );
        }
        resolved.put(plainPrefix, AnnotationPolicy.yaml(override.getPrewarm(), override.getKeepWarm(), false));
      });
    return Map.copyOf(resolved);
  }

  /** The one immutable state a frozen registry publishes. */
  private record FrozenSnapshot(
    Map<String, ResolvedNamespacePolicy> policies,
    ResolvedNamespacePolicy defaultPolicy,
    boolean activeKeepWarm
  ) {}

  /** One resolved trigger and the layer that decided it. */
  private record TriggerChoice(HeatTrigger trigger, ResolvedNamespacePolicy.Source source) {}

  /**
   * One namespace declaration: its two triggers and, for an annotation, the TTL baseline its entries
   * are created with. A YAML override carries no TTL — an operator may only stop a trigger.
   *
   * @param prewarm        the declared prewarm trigger, or {@code null} to inherit
   * @param keepWarm       the declared keep-warm trigger, or {@code null} to inherit
   * @param ttl            the TTL contract for an annotation declaration, or {@code null} for YAML
   * @param dualRegistered whether a {@code caches} override registered this entry under both
   *                       physical delimiters, so the alternate form is not a typo
   */
  private record AnnotationPolicy(
    @Nullable HeatTrigger prewarm,
    @Nullable HeatTrigger keepWarm,
    @Nullable TtlContract ttl,
    boolean dualRegistered
  ) {
    /** One YAML override: triggers only, with the dual-registration flag of its source key. */
    private static AnnotationPolicy yaml(@Nullable HeatTrigger prewarm, @Nullable HeatTrigger keepWarm, boolean dual) {
      return new AnnotationPolicy(prewarm, keepWarm, null, dual);
    }
  }

  /**
   * One namespace's programmatic declaration.
   *
   * @param prewarm  the declared prewarm trigger, or {@code null} to inherit
   * @param keepWarm the declared keep-warm trigger, or {@code null} to inherit
   * @param loader   the namespace's background loader, or {@code null} when it declares none
   */
  private record ProgrammaticWarm(
    @Nullable HeatTrigger prewarm,
    @Nullable HeatTrigger keepWarm,
    @Nullable CacheLoader<String, ?> loader
  ) {}
}
