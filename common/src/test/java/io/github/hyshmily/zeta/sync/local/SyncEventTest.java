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
package io.github.hyshmily.zeta.sync.local;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Pins the namespace a {@link SyncPolicy} sees: both encodings answer one name, prefixes range. */
@DisplayName("sync event namespace")
class SyncEventTest {

  private static SyncEvent keyEvent(String key) {
    return new SyncEvent(1L, key, SyncType.PUT, 1L);
  }

  private static SyncEvent prefixClear(String prefix) {
    return new SyncEvent(2L, prefix, SyncType.INVALIDATE_PREFIX, 2L);
  }

  @Test
  void namespace_shouldAnswerOneBareNameForBothEncodings() {
    assertThat(keyEvent("users\u0000u-1").namespace()).isEqualTo("users");
    assertThat(keyEvent("users:u-1").namespace()).isEqualTo("users");
    assertThat(keyEvent("plain").namespace()).isNull();
  }

  @Test
  void namespace_shouldNameAPrefixOnlyWhenItStartsWithACompleteName() {
    assertThat(prefixClear("users\u0000u-").namespace()).isEqualTo("users");
    assertThat(prefixClear("users\u0000").namespace()).isEqualTo("users");
    assertThat(prefixClear("use").namespace()).isNull();
    assertThat(new SyncEvent(3L, null, SyncType.INVALIDATE_ALL, 3L).namespace()).isNull();
  }

  @Test
  void isValue_shouldCoverPutAndRefreshOnly() {
    assertThat(keyEvent("users\u0000u-1").isValue()).isTrue();
    assertThat(new SyncEvent(1L, "users\u0000u-1", SyncType.REFRESH, 1L).isValue()).isTrue();
    assertThat(new SyncEvent(1L, "users\u0000u-1", SyncType.INVALIDATE, 1L).isValue()).isFalse();
  }

  @Test
  void affectsNamespace_shouldMatchAKeyOfThatNamespaceOnly() {
    assertThat(keyEvent("users\u0000u-1").affectsNamespace("users")).isTrue();
    assertThat(keyEvent("users:u-1").affectsNamespace("users")).isTrue();
    assertThat(keyEvent("orders\u0000o-1").affectsNamespace("users")).isFalse();
    assertThat(keyEvent("plain").affectsNamespace("users")).isFalse();
  }

  @Test
  void affectsNamespace_shouldAcceptATrailingDelimiter() {
    assertThat(keyEvent("users\u0000u-1").affectsNamespace("users\u0000")).isTrue();
    assertThat(keyEvent("users:u-1").affectsNamespace("users:")).isTrue();
  }

  @Test
  void affectsNamespace_shouldTreatATableWideClearAsAffectingEveryNamespace() {
    SyncEvent clear = new SyncEvent(3L, null, SyncType.INVALIDATE_ALL, 3L);

    assertThat(clear.affectsNamespace("users")).isTrue();
    assertThat(clear.affectsNamespace("orders")).isTrue();
  }

  @Test
  void affectsNamespace_shouldRangeOverAPrefixClear() {
    assertThat(prefixClear("users\u0000").affectsNamespace("users")).isTrue();
    assertThat(prefixClear("users\u0000u-").affectsNamespace("users")).isTrue();
    // A raw prefix shorter than the name matches every key of that namespace.
    assertThat(prefixClear("use").affectsNamespace("users")).isTrue();
    assertThat(prefixClear("user").affectsNamespace("users")).isTrue();
    assertThat(prefixClear("orders\u0000").affectsNamespace("users")).isFalse();
  }

  @Test
  void affectsNamespace_shouldNotLeakIntoASiblingWithTheSameStem() {
    // "users\u0000u-" lies inside "users"; it must not be read as reaching "user".
    assertThat(prefixClear("users\u0000u-").affectsNamespace("user")).isFalse();
    assertThat(prefixClear("users2:").affectsNamespace("users")).isFalse();
  }

  @Test
  void affectsNamespace_shouldRejectADelimiterOnlyNamespace() {
    SyncEvent clear = new SyncEvent(3L, null, SyncType.INVALIDATE_ALL, 3L);

    // Stripping the trailing delimiter leaves nothing to compare, and a table-wide clear would
    // otherwise answer "affects" for every input.
    assertThatThrownBy(() -> clear.affectsNamespace(":")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> clear.affectsNamespace("\u0000")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> clear.affectsNamespace(" :")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void affectsNamespace_shouldRejectABlankNamespace() {
    assertThatThrownBy(() -> keyEvent("users\u0000u-1").affectsNamespace(" ")).isInstanceOf(
      IllegalArgumentException.class
    );
    assertThatThrownBy(() -> keyEvent("users\u0000u-1").affectsNamespace(null)).isInstanceOf(
      NullPointerException.class
    );
  }
}
