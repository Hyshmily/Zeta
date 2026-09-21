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
package io.github.hyshmily.zeta.cache.loader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;

class PrefixRoutedLoaderTest {

  @Test
  void load_registryHit_usesSpecLoaderAndSkipsFallback() {
    ZetaLoaderRegistry registry = new ZetaLoaderRegistry();
    registry.register("user:", ZetaLoadingSpec.of(key -> "user:" + key));
    CacheLoader<Object> fallback = mock(CacheLoader.class);
    PrefixRoutedLoader composite = new PrefixRoutedLoader(registry, fallback);

    assertThat(composite.load("user:42")).isEqualTo("user:user:42");
    verifyNoInteractions(fallback);
  }

  @Test
  void load_registryMiss_delegatesToFallback() {
    ZetaLoaderRegistry registry = new ZetaLoaderRegistry();
    registry.register("user:", ZetaLoadingSpec.of(key -> "U"));
    CacheLoader<Object> fallback = mock(CacheLoader.class);
    when(fallback.load("vendor:1")).thenReturn("V");
    PrefixRoutedLoader composite = new PrefixRoutedLoader(registry, fallback);

    assertThat(composite.load("vendor:1")).isEqualTo("V");
    verify(fallback).load("vendor:1");
  }

  @Test
  void load_emptyRegistry_alwaysDelegates() {
    ZetaLoaderRegistry registry = new ZetaLoaderRegistry();
    CacheLoader<Object> fallback = mock(CacheLoader.class);
    when(fallback.load("any")).thenReturn("V");
    PrefixRoutedLoader composite = new PrefixRoutedLoader(registry, fallback);

    assertThat(composite.load("any")).isEqualTo("V");
  }

  @Test
  void load_specLoaderUncheckedException_propagatesToConsumer() {
    ZetaLoaderRegistry registry = new ZetaLoaderRegistry();
    registry.register(
      "user:",
      ZetaLoadingSpec.of(key -> {
        throw new IllegalStateException("db down");
      })
    );
    CacheLoader<Object> fallback = mock(CacheLoader.class);
    PrefixRoutedLoader composite = new PrefixRoutedLoader(registry, fallback);

    // The consumer's existing load-failure handling (rate-limited warn, treated
    // as value-not-found) catches this — the router must not swallow it.
    assertThatThrownBy(() -> composite.load("user:42")).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void load_fallbackException_propagates() {
    ZetaLoaderRegistry registry = new ZetaLoaderRegistry();
    CacheLoader<Object> fallback = mock(CacheLoader.class);
    when(fallback.load("vendor:1")).thenThrow(new RuntimeException("redis down"));
    PrefixRoutedLoader composite = new PrefixRoutedLoader(registry, fallback);

    assertThatThrownBy(() -> composite.load("vendor:1")).isInstanceOf(RuntimeException.class);
  }

  @Test
  void construction_nullArgumentsThrow() {
    ZetaLoaderRegistry registry = new ZetaLoaderRegistry();
    CacheLoader<Object> fallback = mock(CacheLoader.class);
    assertThatThrownBy(() -> new PrefixRoutedLoader(null, fallback)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new PrefixRoutedLoader(registry, null)).isInstanceOf(NullPointerException.class);
  }
}
