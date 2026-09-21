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

import org.junit.jupiter.api.Test;

class ZetaLoaderRegistryTest {

  private final ZetaLoaderRegistry registry = new ZetaLoaderRegistry();

  private static ZetaLoadingSpec<String> spec(String marker) {
    return ZetaLoadingSpec.of(key -> marker);
  }

  // ── Longest-prefix match ──

  @Test
  void match_moreSpecificPrefixWins() {
    registry.register("user:", spec("shallow"));
    registry.register("user:profile:", spec("deep"));
    assertThat(registry.match("user:profile:42").loader().load("x")).isEqualTo("deep");
    assertThat(registry.match("user:42").loader().load("x")).isEqualTo("shallow");
  }

  @Test
  void match_keyEqualToPrefixMatches() {
    registry.register("user:", spec("s"));
    assertThat(registry.match("user:")).isSameAs(registry.match("user:1"));
  }

  @Test
  void match_noMatchReturnsNull() {
    registry.register("user:", spec("s"));
    assertThat(registry.match("vendor:1")).isNull();
  }

  @Test
  void match_emptyRegistryReturnsNull() {
    assertThat(registry.match("user:1")).isNull();
    assertThat(registry.isEmpty()).isTrue();
  }

  /**
   * Fallback path: the floor entry ({@code "abZ"}) sorts between the matching
   * prefix ({@code "ab"}) and the key ({@code "aba"}) and is not a prefix of the
   * key — the descending prefix walk must still find {@code "ab"}.
   */
  @Test
  void match_findsPrefixWhenFloorEntryIsNotAPrefix() {
    registry.register("ab", spec("short"));
    registry.register("abZ", spec("other"));
    assertThat(registry.match("aba").loader().load("x")).isEqualTo("short");
  }

  @Test
  void match_emptyPrefixNeverMatchesSinceRegistrationRequiresText() {
    // register() rejects blank prefixes, so an all-blank registry cannot match.
    assertThatThrownBy(() -> registry.register("", spec("s"))).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> registry.register("  ", spec("s"))).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void match_nullKeyThrows() {
    assertThatThrownBy(() -> registry.match(null)).isInstanceOf(NullPointerException.class);
  }

  // ── Registration lifecycle ──

  @Test
  void register_replacesExistingPrefixSpec() {
    registry.register("user:", spec("first"));
    registry.register("user:", spec("second"));
    assertThat(registry.size()).isEqualTo(1);
    assertThat(registry.match("user:42").loader().load("x")).isEqualTo("second");
  }

  @Test
  void unregister_removesOnlyThatPrefix() {
    registry.register("user:", spec("s1"));
    registry.register("order:", spec("s2"));
    registry.unregister("user:");
    assertThat(registry.match("user:42")).isNull();
    assertThat(registry.match("order:1")).isNotNull();
  }

  @Test
  void unregister_unknownPrefixIsNoOp() {
    registry.unregister("never-registered");
  }

  @Test
  void clear_removesEverything() {
    registry.register("user:", spec("s1"));
    registry.register("order:", spec("s2"));
    registry.clear();
    assertThat(registry.isEmpty()).isTrue();
    assertThat(registry.match("user:42")).isNull();
  }

  @Test
  void register_nullSpecThrows() {
    assertThatThrownBy(() -> registry.register("user:", null)).isInstanceOf(NullPointerException.class);
  }
}
