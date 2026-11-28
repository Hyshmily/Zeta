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
package io.github.hyshmily.zeta.annotation.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hyshmily.zeta.policy.KeyFormat;
import java.lang.reflect.Method;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("NamespaceKeys")
class NamespaceKeysTest {

  private static Method method(String name, Class<?>... parameterTypes) throws NoSuchMethodException {
    return Samples.class.getDeclaredMethod(name, parameterTypes);
  }

  private static KeyEncoding register(
    NamespaceKeys keys,
    String namespace,
    KeyFormat.Option option,
    Method method
  ) {
    NamespaceKeys.Staging staging = keys.staging();
    KeyEncoding encoding = staging.stage(namespace, option, method, true);
    staging.commit();
    return encoding;
  }

  @Test
  @DisplayName("TYPED keeps the canonical self-delimiting key")
  void typedKey() throws Exception {
    NamespaceKeys keys = new NamespaceKeys(KeyFormat.TYPED);
    KeyEncoding encoding = register(keys, "users", KeyFormat.Option.INHERIT, method("text", String.class));

    assertThat(encoding.physicalPrefix()).isEqualTo("users\u0000");
    assertThat(encoding.encodeSingle("42")).isEqualTo("users\u0000[s2:42]");
  }

  @Test
  @DisplayName("PLAIN prints the scalar after the colon and does not lock a key type")
  void plainKey() throws Exception {
    NamespaceKeys keys = new NamespaceKeys(KeyFormat.TYPED);
    KeyEncoding encoding = register(keys, "users", KeyFormat.Option.PLAIN, method("id", Long.class));

    assertThat(encoding.physicalPrefix()).isEqualTo("users:");
    assertThat(encoding.encodeSingle(42L)).isEqualTo("users:42");
    assertThat(encoding.encodeSingle("42")).isEqualTo("users:42");
  }

  @Test
  @DisplayName("a namespace cannot switch from TYPED to PLAIN")
  void formatConflict() throws Exception {
    NamespaceKeys keys = new NamespaceKeys(KeyFormat.TYPED);
    register(keys, "users", KeyFormat.Option.TYPED, method("text", String.class));

    assertThatThrownBy(() -> register(keys, "users", KeyFormat.Option.PLAIN, method("id", Long.class)))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("conflicting key format");
  }

  @Test
  @DisplayName("PLAIN refuses a multi-argument default key")
  void plainRefusesMultiArgumentKey() throws Exception {
    NamespaceKeys keys = new NamespaceKeys(KeyFormat.TYPED);
    KeyEncoding encoding = register(keys, "users", KeyFormat.Option.PLAIN, method("two", Long.class, Long.class));

    assertThatThrownBy(() -> encoding.encodeArgs(new Object[] { 1L, 2L }))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("cannot use a default key");
  }

  @Test
  @DisplayName("PLAIN refuses the zero-argument default key too")
  void plainRefusesZeroArgumentDefaultKey() throws Exception {
    NamespaceKeys keys = new NamespaceKeys(KeyFormat.TYPED);
    KeyEncoding encoding = register(keys, "users", KeyFormat.Option.PLAIN, method("none"));

    assertThatThrownBy(() -> encoding.encodeArgs(new Object[0]))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("cannot use a default key");
  }

  @Test
  @DisplayName("the logical namespace name rejects both delimiters")
  void nameValidation() throws Exception {
    NamespaceKeys keys = new NamespaceKeys(KeyFormat.TYPED);

    assertThatThrownBy(() -> register(keys, "us:ers", KeyFormat.Option.TYPED, method("text", String.class)))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("must not contain");
    assertThatThrownBy(() -> register(keys, "us\u0000ers", KeyFormat.Option.TYPED, method("text", String.class)))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("must not contain");
  }

  @Test
  @DisplayName("a programmatic PLAIN namespace registers without a key type")
  void programmaticPlainNeedsNoType() {
    NamespaceKeys keys = new NamespaceKeys(KeyFormat.TYPED);
    KeyEncoding encoding = keys.registerProgrammatic("users", KeyFormat.PLAIN);

    assertThat(encoding.physicalPrefix()).isEqualTo("users:");
    assertThat(keys.formats()).containsEntry("users", KeyFormat.PLAIN);
  }

  @Test
  @DisplayName("a staged declaration is invisible until the scan commits")
  void stagedDeclarationIsInvisibleUntilCommit() throws Exception {
    NamespaceKeys keys = new NamespaceKeys(KeyFormat.TYPED);
    NamespaceKeys.Staging staging = keys.staging();

    staging.stage("users", KeyFormat.Option.PLAIN, method("id", Long.class), true);

    assertThatThrownBy(() -> keys.resolve("users"))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("unknown namespace");
    staging.commit();
    assertThat(keys.resolve("users").format()).isEqualTo(KeyFormat.PLAIN);
  }

  @Test
  @DisplayName("an inherited annotation follows a programmatically configured format")
  void inheritedDeclarationAdoptsConfiguredFormat() throws Exception {
    NamespaceKeys keys = new NamespaceKeys(KeyFormat.TYPED);
    keys.registerProgrammatic("users", KeyFormat.PLAIN);

    KeyEncoding encoding = register(keys, "users", KeyFormat.Option.INHERIT, method("id", Long.class));

    assertThat(encoding.format()).isEqualTo(KeyFormat.PLAIN);
  }

  @Test
  @DisplayName("a keyless clear leaves the namespace unpinned for a later declaration")
  void keylessClearLeavesTheNamespaceUnpinned() throws Exception {
    NamespaceKeys keys = new NamespaceKeys(KeyFormat.TYPED);
    NamespaceKeys.Staging staging = keys.staging();
    KeyEncoding unpinned = staging.stage("users", KeyFormat.Option.INHERIT, method("none"), false);
    staging.commit();

    assertThat(unpinned.format()).isEqualTo(KeyFormat.TYPED);
    assertThat(register(keys, "users", KeyFormat.Option.PLAIN, method("id", Long.class)).format())
      .isEqualTo(KeyFormat.PLAIN);
  }

  @Test
  @DisplayName("a pinned namespace rejects an inherited declaration in the other format")
  void pinnedNamespaceRejectsAnInheritedMismatch() throws Exception {
    NamespaceKeys keys = new NamespaceKeys(KeyFormat.TYPED);
    register(keys, "users", KeyFormat.Option.PLAIN, method("id", Long.class));

    assertThatThrownBy(() -> register(keys, "users", KeyFormat.Option.INHERIT, method("text", String.class)))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("conflicting key format");
  }

  @Test
  @DisplayName("a namespace reconfigured during the scan fails the commit instead of re-encoding")
  void reconfigurationDuringTheScanFailsTheCommit() throws Exception {
    NamespaceKeys keys = new NamespaceKeys(KeyFormat.TYPED);
    NamespaceKeys.Staging clear = keys.staging();
    clear.stage("users", KeyFormat.Option.INHERIT, method("none"), false);
    clear.commit();

    NamespaceKeys.Staging scan = keys.staging();
    scan.stage("users", KeyFormat.Option.INHERIT, method("id", Long.class), true);
    keys.registerProgrammatic("users", KeyFormat.PLAIN);

    assertThatThrownBy(scan::commit)
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("mid-scan");
  }

  static class Samples {

    public String text(String value) {
      return value;
    }

    public String id(Long value) {
      return String.valueOf(value);
    }

    public String two(Long first, Long second) {
      return first + ":" + second;
    }

    public String none() {
      return "none";
    }
  }
}
