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
package io.github.hyshmily.zeta.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hyshmily.zeta.cache.cachesupport.policy.*;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * The closed option records reject a state that cannot describe a real cache
 * operation <em>at construction</em>, rather than several frames deeper where the
 * failure is an NPE or a silently ignored flag.
 */
@DisplayName("Option record validation")
class OptionRecordsTest {

  @Nested
  @DisplayName("InvalidatePolicy")
  class Invalidate {

    @Test
    @DisplayName("a null scope fails at construction, not at first use")
    void nullScopeIsRejectedAtConstruction() {
      assertThatThrownBy(() -> new InvalidatePolicy(null, false))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("scope must not be null");
    }

    @Test
    @DisplayName("every factory keeps a non-null scope")
    void factoriesKeepScope() {
      assertThat(InvalidatePolicy.defaults().scope()).isEqualTo(InvalidatePolicy.Scope.BOTH);
      assertThat(InvalidatePolicy.localOnly().scope()).isEqualTo(InvalidatePolicy.Scope.LOCAL);
      assertThat(InvalidatePolicy.immediate(true).scope()).isEqualTo(InvalidatePolicy.Scope.BOTH);
      assertThatCode(() -> InvalidatePolicy.defaults().sync()).doesNotThrowAnyException();
    }
  }

  @Nested
  @DisplayName("PutPolicy")
  class Put {

    @Test
    @DisplayName("a permanent entry cannot be synced")
    void permanentEntryCannotBeSynced() {
      assertThatThrownBy(() -> PutPolicy.of(Long.MAX_VALUE, 0L, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("permanent entry cannot be synced");
    }

    @Test
    @DisplayName("a permanent local entry is still allowed")
    void permanentLocalEntryIsAllowed() {
      assertThatCode(() -> PutPolicy.of(Long.MAX_VALUE, 0L, false)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("a negative TTL override is rejected at construction")
    void negativeTtlIsRejected() {
      assertThatThrownBy(() -> PutPolicy.of(-1L, 0L, false)).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> PutPolicy.of(0L, -1L, false)).isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Nested
  @DisplayName("GetPolicy")
  class Get {

    @Test
    @DisplayName("a negative TTL override is rejected at construction")
    void negativeTtlIsRejected() {
      assertThatThrownBy(() -> GetPolicy.of(-1L, 0L, true, StalePolicy.SOFT_REFRESH))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must not be negative");
    }

    @Test
    @DisplayName("zero means 'use the configured default', not an error")
    void zeroIsAccepted() {
      assertThatCode(() -> GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH)).doesNotThrowAnyException();
      assertThat(GetPolicy.of(0L, 0L, true, StalePolicy.SOFT_REFRESH).hardTtlMs().getAsLong()).isZero();
    }
  }

  @Nested
  @DisplayName("EntryDraft")
  class Draft {

    @Test
    @DisplayName("a negative duration is rejected before it reaches L1")
    void negativeDurationIsRejected() {
      assertThatThrownBy(() -> EntryDraft.blank(null).value("v").version(1L).ttl(-1L, 0L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must not be negative");
      assertThatThrownBy(() -> EntryDraft.blank(null).value("v").version(1L).ttl(0L, -1L)).isInstanceOf(
        IllegalArgumentException.class
      );
    }

    @Test
    @DisplayName("a soft TTL beyond the hard one is inert, not rejected")
    void softBeyondHardIsAccepted() {
      // Caffeine expires on the earlier of the two, so the soft window simply
      // never opens. Rejecting it would forbid the single-field updates
      // (hardTtl/softTtl) that extend one side of a live entry.
      assertThatCode(() ->
        EntryDraft.blank(null).value("v").version(1L).ttl(5_000L, 30_000L)
      ).doesNotThrowAnyException();
    }
  }
}
