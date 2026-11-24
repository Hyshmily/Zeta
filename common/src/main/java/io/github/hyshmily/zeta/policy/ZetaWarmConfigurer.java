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

/**
 * The public startup hook for programmatic background warming.
 *
 * <p>Every bean is collected once, after all singletons exist and before the AMQP listeners
 * start. The declarations are merged with the annotation contracts and frozen; a namespace that
 * enables a trigger without a loader fails startup instead of silently skipping every load.
 *
 * <p>A declaration is for the lifetime of the context. It is not a runtime registry: a dynamic
 * multi-tenant namespace belongs behind a raw fallback loader, not behind a mutable map.
 */
@FunctionalInterface
public interface ZetaWarmConfigurer {
  /**
   * Register the application's background loaders and default triggers.
   *
   * @param registry the startup-only registry
   */
  void configure(WarmRegistry registry);
}
