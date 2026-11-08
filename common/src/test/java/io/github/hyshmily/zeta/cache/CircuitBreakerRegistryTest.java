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
package io.github.hyshmily.zeta.cache;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.circuitbreaker.CircuitBreaker;
import io.github.hyshmily.zeta.cache.cachesupport.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.Test;

/** Shard selection for annotated and programmatic keys. */
class CircuitBreakerRegistryTest {

  private final CircuitBreakerRegistry registry = new CircuitBreakerRegistry(new ZetaProperties.CircuitBreaker(), 16);

  @Test
  void annotatedKeysShardOnTheCacheNamespace() {
    CircuitBreaker users = registry.breakerFor("users" + CacheKeysPolicy.NAMESPACE_DELIMITER + "1001");

    assertThat(registry.breakerFor("users" + CacheKeysPolicy.NAMESPACE_DELIMITER + "1002")).isSameAs(users);
    assertThat(registry.breakerFor("users" + CacheKeysPolicy.NAMESPACE_DELIMITER + "tenantA:1001")).isSameAs(users);
    assertThat(registry.breakerFor("orders" + CacheKeysPolicy.NAMESPACE_DELIMITER + "1001")).isNotSameAs(users);
  }

  @Test
  void programmaticKeysKeepTheColonPrefix() {
    CircuitBreaker user = registry.breakerFor("user:1");

    assertThat(registry.breakerFor("user:2")).isSameAs(user);
    assertThat(registry.breakerFor("product:1")).isNotSameAs(user);
    assertThat(registry.breakerFor("no-namespace")).isSameAs(registry.breakerFor("also-none"));
  }
}
