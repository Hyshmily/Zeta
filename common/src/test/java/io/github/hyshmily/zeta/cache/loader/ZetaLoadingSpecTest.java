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

import io.github.hyshmily.zeta.model.CachePolicy;
import io.github.hyshmily.zeta.model.StalePolicy;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ZetaLoadingSpecTest {

  @Test
  void of_allDefaults() {
    ZetaLoadingSpec<String> spec = ZetaLoadingSpec.of(key -> "v");
    assertThat(spec.hardTtlMs()).isZero();
    assertThat(spec.softTtlMs()).isZero();
    assertThat(spec.stalePolicy()).isEqualTo(StalePolicy.SOFT_REFRESH);
    assertThat(spec.nullCaching()).isTrue();
    assertThat(spec.reportEnabled()).isTrue();
    assertThat(spec.failOnError()).isFalse();
  }

  @Test
  void of_nullLoaderRejectedAtConstruction() {
    assertThatThrownBy(() -> ZetaLoadingSpec.of(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void syncPlaneCacheLoaderFitsDirectlyIntoASpec() {
    // CacheLoader<Object> is the sync-plane instantiation of the root SPI
    // (ADR-0070): the Redis value-channel loader can be reused as a spec
    // loader without adaptation.
    CacheLoader<Object> syncLoader = key -> key.startsWith("user:") ? "U" : null;
    ZetaLoadingSpec<Object> spec = ZetaLoadingSpec.of(syncLoader);
    assertThat(spec.loader().load("user:42")).isEqualTo("U");
    assertThat(spec.toPolicy("user:42").reader().get()).isEqualTo("U");
  }

  @Test
  void builder_withoutLoaderFails() {
    assertThatThrownBy(() -> ZetaLoadingSpec.builder().build()).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void builder_rejectsNegativeTtls() {
    assertThatThrownBy(() -> ZetaLoadingSpec.<String>builder().loader(k -> "v").hardTtl(-1).build())
      .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ZetaLoadingSpec.<String>builder().loader(k -> "v").softTtl(-1).build())
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void builder_carriesAllKnobs() {
    ZetaLoadingSpec<String> spec = ZetaLoadingSpec.<String>builder()
      .loader(key -> "v")
      .hardTtl(5_000)
      .softTtl(1_000)
      .stalePolicy(StalePolicy.RETURN)
      .nullCaching(false)
      .skipReport()
      .failOnError()
      .build();
    assertThat(spec.hardTtlMs()).isEqualTo(5_000);
    assertThat(spec.softTtlMs()).isEqualTo(1_000);
    assertThat(spec.stalePolicy()).isEqualTo(StalePolicy.RETURN);
    assertThat(spec.nullCaching()).isFalse();
    assertThat(spec.reportEnabled()).isFalse();
    assertThat(spec.failOnError()).isTrue();
  }

  @Test
  void toPolicy_carriesTtlsAndKnobsIntoPolicy() {
    ZetaLoadingSpec<String> spec = ZetaLoadingSpec.<String>builder()
      .loader(key -> "v")
      .hardTtl(5_000)
      .softTtl(1_000)
      .nullCaching(false)
      .failOnError()
      .build();
    CachePolicy policy = spec.toPolicy("k");
    assertThat(policy.hardTtlMs().getAsLong()).isEqualTo(5_000);
    assertThat(policy.softTtlMs().getAsLong()).isEqualTo(1_000);
    assertThat(policy.nullCaching()).isFalse();
    assertThat(policy.reportEnabled()).isTrue();
    assertThat(policy.stalePolicy()).isEqualTo(StalePolicy.SOFT_REFRESH);
    assertThat(policy.failOnError()).isTrue();
  }

  @Test
  void toPolicy_readerDelegatesToLoaderWithCapturedKey() {
    ZetaLoadingSpec<String> spec = ZetaLoadingSpec.of(key -> "loaded:" + key);
    CachePolicy policy = spec.toPolicy("user:42");
    assertThat(policy.reader().get()).isEqualTo("loaded:user:42");
  }

  @Test
  void toPolicy_stalePolicyOverrideReplacesSpecPolicy() {
    ZetaLoadingSpec<String> spec = ZetaLoadingSpec.<String>builder().loader(key -> "v").stalePolicy(StalePolicy.RETURN).build();
    assertThat(spec.toPolicy("k").stalePolicy()).isEqualTo(StalePolicy.RETURN);
    assertThat(spec.toPolicy("k", StalePolicy.REVALIDATE).stalePolicy()).isEqualTo(StalePolicy.REVALIDATE);
  }

  @Test
  void toPolicy_nullStaleOverrideThrows() {
    ZetaLoadingSpec<String> spec = ZetaLoadingSpec.of(key -> "v");
    assertThatThrownBy(() -> spec.toPolicy("k", null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void withLoader_swapsLoaderAndCarriesAllKnobs() {
    ZetaLoadingSpec<String> spec = ZetaLoadingSpec.<String>builder()
      .loader(key -> "orig")
      .hardTtl(5_000)
      .softTtl(1_000)
      .stalePolicy(StalePolicy.RETURN)
      .nullCaching(false)
      .skipReport()
      .failOnError()
      .build();

    CacheLoader<String> replacement = key -> "replaced";
    ZetaLoadingSpec<String> copy = spec.withLoader(replacement);

    assertThat(copy.loader()).isSameAs(replacement);
    assertThat(copy.hardTtlMs()).isEqualTo(5_000);
    assertThat(copy.softTtlMs()).isEqualTo(1_000);
    assertThat(copy.stalePolicy()).isEqualTo(StalePolicy.RETURN);
    assertThat(copy.nullCaching()).isFalse();
    assertThat(copy.reportEnabled()).isFalse();
    assertThat(copy.failOnError()).isTrue();
  }

  @Test
  void withLoader_defaultKnobsCarriedOver() {
    ZetaLoadingSpec<String> spec = ZetaLoadingSpec.of(key -> "orig");
    ZetaLoadingSpec<String> copy = spec.withLoader(key -> "replaced");
    assertThat(copy.hardTtlMs()).isZero();
    assertThat(copy.softTtlMs()).isZero();
    assertThat(copy.stalePolicy()).isEqualTo(StalePolicy.SOFT_REFRESH);
    assertThat(copy.nullCaching()).isTrue();
    assertThat(copy.reportEnabled()).isTrue();
    assertThat(copy.failOnError()).isFalse();
  }

  @Test
  void withLoader_sourceSpecUnchanged() {
    ZetaLoadingSpec<String> spec = ZetaLoadingSpec.of(key -> "orig");
    spec.withLoader(key -> "replaced");
    assertThat(spec.loader().load("k")).isEqualTo("orig");
  }

  @Test
  void withLoader_nullRejected() {
    ZetaLoadingSpec<String> spec = ZetaLoadingSpec.of(key -> "v");
    assertThatThrownBy(() -> spec.withLoader(null)).isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * Pins the intended "inherit spec policy, swap only the data source" flow:
   * a policy derived from the copy routes reads through the replacement
   * loader while keeping the spec's knobs — the explicit alternative to the
   * registry-bypassing reader overloads on the {@code Zeta} facade.
   */
  @Test
  void withLoader_copyPolicyRoutesThroughReplacementLoader() {
    ZetaLoadingSpec<String> spec = ZetaLoadingSpec.<String>builder()
      .loader(key -> "orig")
      .hardTtl(5_000)
      .build();
    CachePolicy policy = spec.withLoader(key -> "swapped:" + key).toPolicy("user:42");
    assertThat(policy.reader().get()).isEqualTo("swapped:user:42");
    assertThat(policy.hardTtlMs().getAsLong()).isEqualTo(5_000);
  }

  /**
   * Pins the reader contract of {@link ZetaLoadingSpec#toPolicy(String)}: the
   * loader is collapsed into a key-only {@link java.util.function.Supplier},
   * which is the sole channel from a registered loader into the cache. Misses
   * and soft-expire refreshes both travel through that same zero-argument
   * reader — one load per invocation, no other loader method involved.
   */
  @Test
  void toPolicy_reader_invokesLoadPerCall() {
    AtomicInteger loads = new AtomicInteger();
    CacheLoader<String> loader = cacheKey -> {
      loads.incrementAndGet();
      return "v:" + cacheKey;
    };

    CachePolicy policy = ZetaLoadingSpec.<String>builder().loader(loader).build().toPolicy("user:42");
    assertThat(policy.reader().get()).isEqualTo("v:user:42");
    assertThat(policy.reader().get()).isEqualTo("v:user:42");

    assertThat(loads).hasValue(2);
  }
}
