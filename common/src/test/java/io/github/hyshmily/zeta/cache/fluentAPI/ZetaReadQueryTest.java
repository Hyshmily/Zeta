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
package io.github.hyshmily.zeta.cache.fluentAPI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.annotation.annotationsupporter.NullValue;
import io.github.hyshmily.zeta.exception.ZetaBlockedException;
import io.github.hyshmily.zeta.model.CachePolicy;
import io.github.hyshmily.zeta.model.StalePolicy;
import java.util.Optional;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ZetaReadQueryTest {

  private Zeta zeta;
  private ZetaReadQuery<String> query;

  @BeforeEach
  void setUp() {
    zeta = mock(Zeta.class);
    query = new ZetaReadQuery<>(zeta, "test-key");
  }

  private static CachePolicy eqPolicy(long hardTtlMs, long softTtlMs, boolean nullCaching, boolean skipBroadcast) {
    return argThat(p ->
      p.hardTtlMs().getAsLong() == hardTtlMs
        && p.softTtlMs().getAsLong() == softTtlMs
        && p.nullCaching() == nullCaching
        && p.skipBroadcast() == skipBroadcast
    );
  }

  /**
   * Matcher for the fallback-null sentinel round trip: the null reader is
   * routed through the cache layer's read path with the short null-value TTL
   * (no TTL override), null caching enabled, reporting disabled (the primary
   * read already counted the access), and no stale-policy machinery.
   */
  private static CachePolicy eqNullSentinelPolicy() {
    return argThat(p ->
      p.hardTtlMs().getAsLong() == 0L
        && p.softTtlMs().getAsLong() == 0L
        && p.nullCaching()
        && p.stalePolicy() == StalePolicy.RETURN
        && !p.reportEnabled()
    );
  }

  // ── Block rule ──

  /**
   * BLOCK-rule enforcement lives in the cache layer (HotKeyCache.preGuard); the fluent
   * query must propagate the exception it throws on every read path.
   */
  @Test
  void execute_shouldPropagateBlockedExceptionFromCacheLayer() {
    when(zeta.get(anyString(), any(CachePolicy.class)))
      .thenThrow(new ZetaBlockedException("HotKeyCache", "test-key"));
    ZetaReadQuery<String> q = query.withPrimary(() -> "v");
    assertThatThrownBy(q::execute).isInstanceOf(ZetaBlockedException.class);
  }

  // ── Cache hit (GET mode) ──

  @Test
  void execute_shouldReturnCachedValueInGetMode() {
    when(zeta.get(anyString(), any(CachePolicy.class))).thenReturn(Optional.of("cached"));
    Optional<String> result = query.withPrimary(() -> "db").execute();
    assertThat(result).contains("cached");
    verify(zeta).get(eq("test-key"), eqPolicy(0L, 0L, true, false));
  }

  // ── Stale policy override ──

  @Test
  void execute_shouldCarryStalePolicyOverrideIntoPolicy() {
    when(zeta.get(anyString(), any(CachePolicy.class))).thenReturn(Optional.of("cached"));
    Optional<String> result = query.withPrimary(() -> "db").withStalePolicy(StalePolicy.RETURN).execute();
    assertThat(result).contains("cached");
    verify(zeta).get(eq("test-key"), argThat((CachePolicy p) -> p.stalePolicy() == StalePolicy.RETURN));
  }

  @Test
  void withStalePolicy_nullRejected() {
    assertThatThrownBy(() -> query.withStalePolicy(null)).isInstanceOf(IllegalArgumentException.class);
  }

  // ── Cache hit (GET_WITH_SOFT_EXPIRE mode) ──

  @Test
  void execute_shouldReturnCachedValueInSoftExpireMode() {
    when(zeta.getWithSoftExpire(anyString(), any(CachePolicy.class))).thenReturn(
      Optional.of("cached")
    );
    Optional<String> result = query.withPrimary(() -> "db", CacheMode.GET_WITH_SOFT_EXPIRE).execute();
    assertThat(result).contains("cached");
    verify(zeta).getWithSoftExpire(eq("test-key"), eqPolicy(0L, 0L, true, false));
  }

  // ── NullValue sentinel unwrapping ──

  @Test
  void execute_shouldUnwrapNullValueSentinel() {
    when(zeta.get(anyString(), any(CachePolicy.class))).thenReturn(Optional.empty());
    Optional<String> result = query.withPrimary(() -> null).execute();
    assertThat(result).isEmpty();
  }

  // ── Primary reader null, null caching disabled ──

  @Test
  void execute_shouldNotCacheNullWhenDisabled() {
    when(zeta.get(anyString(), any(CachePolicy.class))).thenReturn(Optional.empty());
    Optional<String> result = query
      .withPrimary(() -> null)
      .nullCaching(false)
      .execute();
    assertThat(result).isEmpty();
  }

  // ── Primary reader null, null caching enabled (default) ──

  @Test
  void execute_shouldReturnEmptyForNullCached() {
    when(zeta.get(anyString(), any(CachePolicy.class))).thenReturn(Optional.empty());
    Optional<String> result = query.withPrimary(() -> null).execute();
    assertThat(result).isEmpty();
  }

  // ── Cache miss, fallback returns value ──

  @Test
  void execute_shouldUseFallbackWhenCacheMiss() {
    when(zeta.get(anyString(), any(CachePolicy.class))).thenReturn(Optional.empty());
    Optional<String> result = query
      .withPrimary(() -> null)
      .thenExecute(() -> "fallback")
      .execute();
    assertThat(result).contains("fallback");
    verify(zeta).putLocal("test-key", "fallback", CachePolicy.of(0L, 0L));
  }

  // ── Cache miss, fallback with send ──

  @Test
  void execute_shouldUseFallbackWithBroadcast() {
    when(zeta.get(anyString(), any(CachePolicy.class))).thenReturn(Optional.empty());
    Optional<String> result = query
      .withPrimary(() -> null)
      .allowBroadcast()
      .thenExecute(() -> "fb")
      .execute();
    assertThat(result).contains("fb");
    verify(zeta).putThrough(eq("test-key"), eq("fb"), any(Runnable.class), eq(CachePolicy.of(0L, 0L)));
  }

  // ── Multiple fallbacks, first returns value ──

  @Test
  void execute_shouldUseFirstNonNullFallback() {
    when(zeta.get(anyString(), any(CachePolicy.class))).thenReturn(Optional.empty());
    Supplier<String> fb1 = mock(Supplier.class);
    when(fb1.get()).thenReturn(null);
    Supplier<String> fb2 = () -> "fb2";
    Optional<String> result = query
      .withPrimary(() -> null)
      .thenExecute(fb1)
      .thenExecute(fb2)
      .execute();
    assertThat(result).contains("fb2");
    verify(fb1).get();
    verify(zeta).putLocal("test-key", "fb2", CachePolicy.of(0L, 0L));
  }

  // ── Fallback null, null caching enabled → short-TTL null sentinel via the cache layer ──

  /**
   * A fallback-null sentinel is no longer stored via {@code putLocal} with the
   * caller's (possibly long) query TTL: it is routed through the cache layer's
   * read path, which stores the sentinel with the short null-value TTL the
   * {@code nullCaching(true)} contract promises.
   */
  @Test
  void execute_shouldCacheNullValueWhenFallbackNull() {
    when(zeta.get(anyString(), any(CachePolicy.class))).thenReturn(Optional.empty());
    Optional<String> result = query
      .withPrimary(() -> null)
      .thenExecute(() -> null)
      .execute();
    assertThat(result).isEmpty();
    // Primary read + one null-sentinel round trip; no putLocal for the null.
    verify(zeta, times(2)).get(eq("test-key"), any(CachePolicy.class));
    verify(zeta).get(eq("test-key"), eqNullSentinelPolicy());
    verify(zeta, never()).putLocal(anyString(), any(), any(CachePolicy.class));
  }

  // ── Fallback null with broadcast enabled → still no version INCR / broadcast ──

  /**
   * A fallback null must never bump the data version nor broadcast a REFRESH:
   * {@code allowBroadcast()} applies to real fallback values only, and the
   * null sentinel takes the same non-broadcast cache-layer route.
   */
  @Test
  void execute_shouldNotVersionIncrOrBroadcastFallbackNull() {
    when(zeta.get(anyString(), any(CachePolicy.class))).thenReturn(Optional.empty());
    Optional<String> result = query
      .withPrimary(() -> null)
      .allowBroadcast()
      .thenExecute(() -> null)
      .execute();
    assertThat(result).isEmpty();
    verify(zeta).get(eq("test-key"), eqNullSentinelPolicy());
    verify(zeta, never()).putThrough(anyString(), any(), any(Runnable.class), any(CachePolicy.class));
  }

  // ── Fallback null, null caching disabled → no cache call ──

  @Test
  void execute_shouldNotCacheNullFallbackWhenDisabled() {
    when(zeta.get(anyString(), any(CachePolicy.class))).thenReturn(Optional.empty());
    Optional<String> result = query
      .withPrimary(() -> null)
      .nullCaching(false)
      .thenExecute(() -> null)
      .execute();
    assertThat(result).isEmpty();
    // Only the primary read ran — no null-sentinel round trip, no cache write.
    verify(zeta, times(1)).get(anyString(), any(CachePolicy.class));
    verify(zeta, never()).putLocal(anyString(), any(), any(CachePolicy.class));
    verify(zeta, never()).putThrough(anyString(), any(), any(), any(CachePolicy.class));
  }

  // ── Default value ──

  @Test
  void execute_shouldReturnDefaultWhenAllNull() {
    when(zeta.get(anyString(), any(CachePolicy.class))).thenReturn(Optional.empty());
    String result = query
      .withPrimary(() -> null)
      .thenExecute(() -> null)
      .executeOrNull("default");
    assertThat(result).isEqualTo("default");
  }

  // ── All null, no default → empty ──

  @Test
  void execute_shouldReturnEmptyWhenAllReadersNull() {
    when(zeta.get(anyString(), any(CachePolicy.class))).thenReturn(Optional.empty());
    Optional<String> result = query.withPrimary(() -> null).execute();
    assertThat(result).isEmpty();
  }

  // ── executeOrNull ──

  @Test
  void executeOrNull_shouldReturnNullWhenEmpty() {
    when(zeta.get(anyString(), any(CachePolicy.class))).thenReturn(Optional.empty());
    String result = query.withPrimary(() -> null).executeOrNull();
    assertThat(result).isNull();
  }

  @Test
  void executeOrNull_shouldReturnValueWhenCached() {
    when(zeta.get(anyString(), any(CachePolicy.class))).thenReturn(Optional.of("cached"));
    String result = query.withPrimary(() -> "db").executeOrNull();
    assertThat(result).isEqualTo("cached");
  }

  // ── TTL overrides ──

  @Test
  void execute_shouldPassTtlOverrides() {
    when(zeta.get(anyString(), any(CachePolicy.class))).thenReturn(Optional.of("v"));
    query
      .withPrimary(() -> "db")
      .withHardTtl(5000L)
      .withSoftTtl(500L)
      .execute();
    verify(zeta).get(eq("test-key"), eqPolicy(5000L, 500L, true, false));
  }

  @Test
  void execute_shouldPassTtlOverridesViaWithTtl() {
    when(zeta.get(anyString(), any(CachePolicy.class))).thenReturn(Optional.of("v"));
    query
      .withPrimary(() -> "db")
      .withTtl(10000L, 1000L)
      .execute();
    verify(zeta).get(eq("test-key"), eqPolicy(10000L, 1000L, true, false));
  }

  @Test
  void execute_shouldInvokePrimaryReader_whenMockInvokesSupplier() {
    when(zeta.get(anyString(), any(CachePolicy.class))).thenAnswer(invocation -> {
      CachePolicy policy = invocation.getArgument(1);
      @SuppressWarnings("all")
      Supplier<Object> reader = (Supplier<Object>) policy.reader();
      Object val = reader.get();
      return val == NullValue.INSTANCE ? Optional.empty() : Optional.ofNullable(val);
    });
    Optional<String> result = query.withPrimary(() -> "from-reader").execute();
    assertThat(result).contains("from-reader");
  }

  // ── Builder chaining ──

  @Test
  void builder_shouldReturnSameInstance() {
    when(zeta.get(anyString(), any(CachePolicy.class))).thenReturn(Optional.of("v"));
    Supplier<String> fallback = () -> null;
    ZetaReadQuery<String> q = query
      .withPrimary(() -> "db")
      .notAllowBroadcast()
      .allowBroadcast()
      .nullCaching(false)
      .nullCaching(true)
      .thenExecute(fallback)
      .withHardTtl(1000L)
      .withSoftTtl(100L);
    assertThat(q.execute()).contains("v");
  }

  // ── Explicit hard/soft TTL in withPrimary + mode ──

  @Test
  void execute_primaryWithModeShouldRespectExplicitMode() {
    when(zeta.getWithSoftExpire(anyString(), any(CachePolicy.class))).thenReturn(Optional.of("v"));
    query.withPrimary(() -> "db", CacheMode.GET_WITH_SOFT_EXPIRE).execute();
    verify(zeta).getWithSoftExpire(eq("test-key"), eqPolicy(0L, 0L, true, false));
  }
}
