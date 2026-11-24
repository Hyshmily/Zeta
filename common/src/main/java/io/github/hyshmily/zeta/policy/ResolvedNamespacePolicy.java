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

import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.TtlContract;
import io.github.hyshmily.zeta.cache.cachesupport.loader.CacheLoader;
import org.jspecify.annotations.Nullable;

/**
 * The fully resolved policy for one physical key: the TTL contract plus the two warming triggers
 * and the loader a background task may use. Every field is concrete.
 *
 * <p>{@code prewarmSource}/{@code keepWarmSource} attribute each trigger to the layer that decided
 * it, so a customizer or a diagnostic can explain a resolved policy instead of guessing between
 * YAML, annotation, programmatic registration and global defaults.
 */
public record ResolvedNamespacePolicy(
  TtlContract ttl,
  HeatTrigger prewarm,
  Source prewarmSource,
  HeatTrigger keepWarm,
  Source keepWarmSource,
  @Nullable CacheLoader<String, Object> loader
) {
  /** Where one resolved warming dimension came from. */
  public enum Source {
    /** An operator override in {@code zeta.local.warm.overrides}. */
    YAML_OVERRIDE,
    /** A {@link ZetaWarmConfigurer} registration. */
    PROGRAMMATIC,
    /** A {@code @ZetaConfig}/{@code @ZetaCached} declaration. */
    ANNOTATION,
    /** The global {@code zeta.local.warm.*} default. */
    GLOBAL,
  }
}
