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
import io.github.hyshmily.zeta.rule.Rule;
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

  // ── Block rule ──

  @Test
  void execute_shouldThrowWhenKeyBlocked() {
    when(zeta.evaluateRule("test-key")).thenReturn(Rule.RuleAction.BLOCK);
    ZetaReadQuery<String> q = query.withPrimary(() -> "v");
    assertThatThrownBy(q::execute).isInstanceOf(ZetaBlockedException.class);
    verify(zeta).evaluateRule("test-key");
    verifyNoMoreInteractions(zeta);
  }

  // ── Cache hit (GET mode) ──

  @Test
  void execute_shouldReturnCachedValueInGetMode() {
    when(zeta.evaluateRule("test-key")).thenReturn(Rule.RuleAction.ALLOW);
    when(zeta.get(anyString(), any(), anyLong(), anyLong())).thenReturn(Optional.of("cached"));
    Optional<String> result = query.withPrimary(() -> "db").execute();
    assertThat(result).contains("cached");
    verify(zeta).get(eq("test-key"), any(), eq(0L), eq(0L));
  }

  // ── Cache hit (GET_WITH_SOFT_EXPIRE mode) ──

  @Test
  void execute_shouldReturnCachedValueInSoftExpireMode() {
    when(zeta.evaluateRule("test-key")).thenReturn(Rule.RuleAction.ALLOW);
    when(zeta.getWithSoftExpire(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(
      Optional.of("cached")
    );
    Optional<String> result = query.withPrimary(() -> "db", CacheMode.GET_WITH_SOFT_EXPIRE).execute();
    assertThat(result).contains("cached");
    verify(zeta).getWithSoftExpire(eq("test-key"), any(), eq(0L), eq(0L), anyBoolean());
  }

  // ── NullValue sentinel unwrapping ──

  @Test
  void execute_shouldUnwrapNullValueSentinel() {
    when(zeta.evaluateRule("test-key")).thenReturn(Rule.RuleAction.ALLOW);
    when(zeta.get(anyString(), any(), anyLong(), anyLong())).thenReturn(Optional.of(NullValue.INSTANCE));
    Optional<String> result = query.withPrimary(() -> null).execute();
    assertThat(result).isEmpty();
  }

  // ── Primary reader null, null caching disabled ──

  @Test
  void execute_shouldNotCacheNullWhenDisabled() {
    when(zeta.evaluateRule("test-key")).thenReturn(Rule.RuleAction.ALLOW);
    when(zeta.get(anyString(), any(), anyLong(), anyLong())).thenReturn(Optional.empty());
    Optional<String> result = query
      .withPrimary(() -> null)
      .notAllowNull()
      .execute();
    assertThat(result).isEmpty();
  }

  // ── Primary reader null, null caching enabled (default) ──

  @Test
  void execute_shouldReturnEmptyForNullCached() {
    when(zeta.evaluateRule("test-key")).thenReturn(Rule.RuleAction.ALLOW);
    when(zeta.get(anyString(), any(), anyLong(), anyLong())).thenReturn(Optional.of(NullValue.INSTANCE));
    Optional<String> result = query.withPrimary(() -> null).execute();
    assertThat(result).isEmpty();
  }

  // ── Cache miss, fallback returns value ──

  @Test
  void execute_shouldUseFallbackWhenCacheMiss() {
    when(zeta.evaluateRule("test-key")).thenReturn(Rule.RuleAction.ALLOW);
    when(zeta.get(anyString(), any(), anyLong(), anyLong())).thenReturn(Optional.empty());
    Optional<String> result = query
      .withPrimary(() -> null)
      .thenExecute(() -> "fallback")
      .execute();
    assertThat(result).contains("fallback");
    verify(zeta).putLocal("test-key", "fallback", 0L, 0L);
  }

  // ── Cache miss, fallback with send ──

  @Test
  void execute_shouldUseFallbackWithBroadcast() {
    when(zeta.evaluateRule("test-key")).thenReturn(Rule.RuleAction.ALLOW);
    when(zeta.get(anyString(), any(), anyLong(), anyLong())).thenReturn(Optional.empty());
    Optional<String> result = query
      .withPrimary(() -> null)
      .allowBroadcast()
      .thenExecute(() -> "fb")
      .execute();
    assertThat(result).contains("fb");
    verify(zeta).putThrough(eq("test-key"), eq("fb"), any(Runnable.class), eq(0L), eq(0L), anyBoolean());
  }

  // ── Multiple fallbacks, first returns value ──

  @Test
  void execute_shouldUseFirstNonNullFallback() {
    when(zeta.evaluateRule("test-key")).thenReturn(Rule.RuleAction.ALLOW);
    when(zeta.get(anyString(), any(), anyLong(), anyLong())).thenReturn(Optional.empty());
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
    verify(zeta).putLocal("test-key", "fb2", 0L, 0L);
  }

  // ── Fallback null, null caching enabled → stores NullValue ──

  @Test
  void execute_shouldCacheNullValueWhenFallbackNull() {
    when(zeta.evaluateRule("test-key")).thenReturn(Rule.RuleAction.ALLOW);
    when(zeta.get(anyString(), any(), anyLong(), anyLong())).thenReturn(Optional.empty());
    Optional<String> result = query
      .withPrimary(() -> null)
      .thenExecute(() -> null)
      .execute();
    assertThat(result).isEmpty();
    verify(zeta).putLocal("test-key", NullValue.INSTANCE, 0L, 0L);
  }

  // ── Fallback null, null caching + send → stores NullValue via putThrough ──

  @Test
  void execute_shouldCacheNullValueViaPutThroughWhenBroadcast() {
    when(zeta.evaluateRule("test-key")).thenReturn(Rule.RuleAction.ALLOW);
    when(zeta.get(anyString(), any(), anyLong(), anyLong())).thenReturn(Optional.empty());
    Optional<String> result = query
      .withPrimary(() -> null)
      .allowBroadcast()
      .thenExecute(() -> null)
      .execute();
    assertThat(result).isEmpty();
    verify(zeta).putThrough(eq("test-key"), eq(NullValue.INSTANCE), any(Runnable.class), eq(0L), eq(0L), anyBoolean());
  }

  // ── Fallback null, null caching disabled → no cache call ──

  @Test
  void execute_shouldNotCacheNullFallbackWhenDisabled() {
    when(zeta.evaluateRule("test-key")).thenReturn(Rule.RuleAction.ALLOW);
    when(zeta.get(anyString(), any(), anyLong(), anyLong())).thenReturn(Optional.empty());
    Optional<String> result = query
      .withPrimary(() -> null)
      .notAllowNull()
      .thenExecute(() -> null)
      .execute();
    assertThat(result).isEmpty();
    verify(zeta, never()).putLocal(anyString(), any(), anyLong(), anyLong());
    verify(zeta, never()).putThrough(anyString(), any(), any(), anyLong(), anyLong(), anyBoolean());
  }

  // ── Default value ──

  @Test
  void execute_shouldReturnDefaultWhenAllNull() {
    when(zeta.evaluateRule("test-key")).thenReturn(Rule.RuleAction.ALLOW);
    when(zeta.get(anyString(), any(), anyLong(), anyLong())).thenReturn(Optional.empty());
    String result = query
      .withPrimary(() -> null)
      .thenExecute(() -> null)
      .executeOrNull("default");
    assertThat(result).isEqualTo("default");
  }

  // ── All null, no default → empty ──

  @Test
  void execute_shouldReturnEmptyWhenAllReadersNull() {
    when(zeta.evaluateRule("test-key")).thenReturn(Rule.RuleAction.ALLOW);
    when(zeta.get(anyString(), any(), anyLong(), anyLong())).thenReturn(Optional.empty());
    Optional<String> result = query.withPrimary(() -> null).execute();
    assertThat(result).isEmpty();
  }

  // ── executeOrNull ──

  @Test
  void executeOrNull_shouldReturnNullWhenEmpty() {
    when(zeta.evaluateRule("test-key")).thenReturn(Rule.RuleAction.ALLOW);
    when(zeta.get(anyString(), any(), anyLong(), anyLong())).thenReturn(Optional.empty());
    String result = query.withPrimary(() -> null).executeOrNull();
    assertThat(result).isNull();
  }

  @Test
  void executeOrNull_shouldReturnValueWhenCached() {
    when(zeta.evaluateRule("test-key")).thenReturn(Rule.RuleAction.ALLOW);
    when(zeta.get(anyString(), any(), anyLong(), anyLong())).thenReturn(Optional.of("cached"));
    String result = query.withPrimary(() -> "db").executeOrNull();
    assertThat(result).isEqualTo("cached");
  }

  // ── TTL overrides ──

  @Test
  void execute_shouldPassTtlOverrides() {
    when(zeta.evaluateRule("test-key")).thenReturn(Rule.RuleAction.ALLOW);
    when(zeta.get(anyString(), any(), anyLong(), anyLong())).thenReturn(Optional.of("v"));
    query
      .withPrimary(() -> "db")
      .withHardTtl(5000L)
      .withSoftTtl(500L)
      .execute();
    verify(zeta).get(eq("test-key"), any(), eq(5000L), eq(500L));
  }

  @Test
  void execute_shouldPassTtlOverridesViaWithTtl() {
    when(zeta.evaluateRule("test-key")).thenReturn(Rule.RuleAction.ALLOW);
    when(zeta.get(anyString(), any(), anyLong(), anyLong())).thenReturn(Optional.of("v"));
    query
      .withPrimary(() -> "db")
      .withTtl(10000L, 1000L)
      .execute();
    verify(zeta).get(eq("test-key"), any(), eq(10000L), eq(1000L));
  }

  @Test
  void execute_shouldInvokeWrappedPrimaryReader_whenMockInvokesSupplier() {
    when(zeta.evaluateRule("test-key")).thenReturn(Rule.RuleAction.ALLOW);
    when(zeta.get(anyString(), any(), anyLong(), anyLong())).thenAnswer(invocation -> {
      Supplier<Object> reader = invocation.getArgument(1);
      Object val = reader.get();
      return val == NullValue.INSTANCE ? Optional.empty() : Optional.ofNullable(val);
    });
    Optional<String> result = query.withPrimary(() -> "from-reader").execute();
    assertThat(result).contains("from-reader");
  }

  // ── Builder chaining ──

  @Test
  void builder_shouldReturnSameInstance() {
    when(zeta.evaluateRule("test-key")).thenReturn(Rule.RuleAction.ALLOW);
    when(zeta.get(anyString(), any(), anyLong(), anyLong())).thenReturn(Optional.of("v"));
    Supplier<String> fallback = () -> null;
    ZetaReadQuery<String> q = query
      .withPrimary(() -> "db")
      .notAllowBroadcast()
      .allowBroadcast()
      .notAllowNull()
      .allowNull()
      .thenExecute(fallback)
      .withHardTtl(1000L)
      .withSoftTtl(100L);
    assertThat(q.execute()).contains("v");
  }

  // ── Explicit hard/soft TTL in withPrimary + mode ──

  @Test
  void execute_primaryWithModeShouldRespectExplicitMode() {
    when(zeta.evaluateRule("test-key")).thenReturn(Rule.RuleAction.ALLOW);
    when(zeta.getWithSoftExpire(anyString(), any(), anyLong(), anyLong(), anyBoolean())).thenReturn(Optional.of("v"));
    query.withPrimary(() -> "db", CacheMode.GET_WITH_SOFT_EXPIRE).execute();
    verify(zeta).getWithSoftExpire(eq("test-key"), any(), eq(0L), eq(0L), anyBoolean());
  }
}
