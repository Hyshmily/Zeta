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
package io.github.hyshmily.zeta.cache.annotationsupporter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.github.hyshmily.zeta.Zeta;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.cache.Cache.ValueRetrievalException;

@DisplayName("ZetaSpringCache tests")
class ZetaSpringCacheTest {

  private Zeta zeta;
  private ZetaProperties properties;
  private ZetaProperties.SpringCache springCache;
  private ZetaSpringCache cache;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    zeta = mock(Zeta.class);
    properties = mock(ZetaProperties.class);
    springCache = new ZetaProperties.SpringCache();
    springCache.setKeySeparator("::");
    when(properties.getSpringCache()).thenReturn(springCache);
    cache = new ZetaSpringCache("test", zeta, properties, true);
  }

  @AfterEach
  void tearDown() {
    ZetaCacheContext.get().restore(null);
  }

  @Test
  @DisplayName("getName returns the cache name")
  void getName_returnsCacheName() {
    assertThat(cache.getName()).isEqualTo("test");
  }

  @Test
  @DisplayName("getNativeCache returns the HotKey facade")
  void getNativeCache_returnsHotKey() {
    assertThat(cache.getNativeCache()).isSameAs(zeta);
  }

  @Test
  @DisplayName("lookup returns value from hotKey.peek")
  void lookup_returnsValueFromPeek() {
    when(zeta.peek("test::myKey")).thenReturn(Optional.of("stored-value"));
    Object result = cache.lookup("myKey");
    assertThat(result).isEqualTo("stored-value");
  }

  @Test
  @DisplayName("lookup returns null when hotKey.peek is empty")
  void lookup_returnsNullWhenPeekEmpty() {
    when(zeta.peek("test::myKey")).thenReturn(Optional.empty());
    Object result = cache.lookup("myKey");
    assertThat(result).isNull();
  }

  @Test
  @DisplayName("lookup returns null when key is null-cached")
  void lookup_returnsNullWhenNullCached() {
    ZetaCacheContext.get().apply(0, 0, true, false);
    when(zeta.computeIfAbsent(anyString(), any())).thenReturn(null);
    cache.get("myKey", (Callable<String>) () -> null);
    ZetaCacheContext.get().restore(null);

    assertThat(cache.lookup("myKey")).isNull();
  }

  @Test
  @DisplayName("get without TTL override calls hotKey.computeIfAbsent")
  void get_withoutTtl_callsHotKeyComputeIfAbsent() {
    when(zeta.computeIfAbsent(eq("test::myKey"), any())).thenReturn("value");
    String result = cache.get("myKey", (Callable<String>) () -> "loaded");
    assertThat(result).isEqualTo("value");
    verify(zeta).computeIfAbsent(eq("test::myKey"), any());
    verify(zeta, never()).computeIfAbsentWithSoftExpire(anyString(), any(), anyLong(), anyLong());
  }

  @Test
  @DisplayName("get with TTL override calls hotKey.computeIfAbsentWithSoftExpire")
  void get_withTtlOverride_callsComputeIfAbsentSoft() {
    ZetaCacheContext.get().apply(5000L, 1000L, false, false);
    when(zeta.computeIfAbsentWithSoftExpire(eq("test::myKey"), any(), eq(5000L), eq(1000L))).thenReturn("value");
    String result = cache.get("myKey", (Callable<String>) () -> "loaded");
    assertThat(result).isEqualTo("value");
    verify(zeta, never()).computeIfAbsent(anyString(), any());
    verify(zeta).computeIfAbsentWithSoftExpire(eq("test::myKey"), any(), eq(5000L), eq(1000L));
  }

  @Test
  @DisplayName("get with only softTtl override calls computeIfAbsentWithSoftExpire")
  void get_withSoftTtlOverride_only() {
    ZetaCacheContext.get().apply(0L, 500L, false, false);
    when(zeta.computeIfAbsentWithSoftExpire(eq("test::myKey"), any(), eq(0L), eq(500L))).thenReturn("value");
    cache.get("myKey", (Callable<String>) () -> "loaded");
    verify(zeta).computeIfAbsentWithSoftExpire(eq("test::myKey"), any(), eq(0L), eq(500L));
  }

  @Test
  @DisplayName("get returns fromStoreValue of the result")
  void get_whenResultPresent_returnsFromStoreValue() {
    when(zeta.computeIfAbsent(anyString(), any())).thenReturn("computed-value");
    String result = cache.get("myKey", (Callable<String>) () -> "loaded");
    assertThat(result).isEqualTo("computed-value");
  }

  @Test
  @DisplayName("get returns null when result is null and allowNull is false")
  void get_whenNullAndNotAllowNull_returnsNull() {
    when(zeta.computeIfAbsent(anyString(), any())).thenReturn(null);
    String result = cache.get("myKey", (Callable<String>) () -> null);
    assertThat(result).isNull();
    verify(zeta, never()).putThrough(anyString(), any(), any());
  }

  @Test
  @DisplayName("get returns null when result is null and allowNull is true")
  void get_whenNullAndAllowNull_returnsNull() {
    ZetaCacheContext.get().apply(0, 0, true, false);
    when(zeta.computeIfAbsent(anyString(), any())).thenReturn(null);
    String result = cache.get("myKey", (Callable<String>) () -> null);
    assertThat(result).isNull();
    verify(zeta, never()).putThrough(anyString(), any(), any());
    verify(zeta, never()).putLocal(anyString(), any());
  }

  @Test
  @DisplayName("get with allowNull and skipBroadcast does not call putLocal")
  void get_whenNullAllowNullAndSkipBroadcast_doesNotCallPutLocal() {
    ZetaCacheContext.get().apply(0, 0, true, true);
    when(zeta.computeIfAbsent(anyString(), any())).thenReturn(null);
    String result = cache.get("myKey", (Callable<String>) () -> null);
    assertThat(result).isNull();
    verify(zeta, never()).putLocal(anyString(), any());
    verify(zeta, never()).putThrough(anyString(), any(), any());
  }

  @Test
  @DisplayName("get with allowNull=true adds key to nullCachedKeys")
  void get_whenAllowNull_addsToNullCachedKeys() {
    ZetaCacheContext.get().apply(0, 0, true, false);
    when(zeta.computeIfAbsent(anyString(), any())).thenReturn(null);
    cache.get("myKey", (Callable<String>) () -> null);
    assertThat(cache.lookup("myKey")).isNull();
  }

  @Test
  @DisplayName("put stores value via hotKey.putThrough")
  void put_storesValue() {
    cache.put("myKey", "myValue");
    verify(zeta).putThrough(eq("test::myKey"), eq("myValue"), any());
  }

  @Test
  @DisplayName("put with skipBroadcast uses putLocal")
  void put_withSkipBroadcast_usesPutLocal() {
    ZetaCacheContext.get().apply(0, 0, false, true);
    cache.put("myKey", "myValue");
    verify(zeta).putLocal("test::myKey", "myValue");
    verify(zeta, never()).putThrough(anyString(), any(), any());
  }

  @Test
  @DisplayName("evict with skipBroadcast calls invalidateLocal")
  void evict_withSkipBroadcast_callsInvalidateLocal() {
    ZetaCacheContext.get().apply(0, 0, false, true);
    cache.evict("myKey");
    verify(zeta).invalidate("test::myKey", false);
    verify(zeta, never()).invalidate(anyString());
  }

  @Test
  @DisplayName("put removes key from nullCachedKeys")
  void put_removesFromNullCachedKeys() {
    ZetaCacheContext.get().apply(0, 0, true, false);
    when(zeta.computeIfAbsent(anyString(), any())).thenReturn(null);
    cache.get("myKey", (Callable<String>) () -> null);
    assertThat(cache.lookup("myKey")).isNull();
    ZetaCacheContext.get().restore(null);

    when(zeta.peek("test::myKey")).thenReturn(Optional.of("real-value"));
    cache.put("myKey", "real-value");
    assertThat(cache.lookup("myKey")).isEqualTo("real-value");
  }

  @Test
  @DisplayName("evict removes from nullCachedKeys and calls invalidate")
  void evict_removesFromNullCachedKeysAndInvalidates() {
    ZetaCacheContext.get().apply(0, 0, true, false);
    when(zeta.computeIfAbsent(anyString(), any())).thenReturn(null);
    cache.get("myKey", (Callable<String>) () -> null);
    ZetaCacheContext.get().restore(null);

    cache.evict("myKey");
    verify(zeta).invalidate("test::myKey");
    when(zeta.peek("test::myKey")).thenReturn(Optional.empty());
    assertThat(cache.lookup("myKey")).isNull();
  }

  @Test
  @DisplayName("clear clears nullCachedKeys and calls invalidateAllLocal")
  void clear_clearsNullCachedKeysAndInvalidatesAll() {
    ZetaCacheContext.get().apply(0, 0, true, false);
    when(zeta.computeIfAbsent(anyString(), any())).thenReturn(null);
    cache.get("key1", (Callable<String>) () -> null);
    cache.get("key2", (Callable<String>) () -> null);
    ZetaCacheContext.get().restore(null);

    cache.clear();
    verify(zeta).invalidateAllLocal();
    verify(zeta, never()).invalidate(anyString());
    when(zeta.peek("test::key1")).thenReturn(Optional.empty());
    when(zeta.peek("test::key2")).thenReturn(Optional.empty());
    assertThat(cache.lookup("key1")).isNull();
  }

  @Test
  @DisplayName("lookup with nullCachedKeys removed returns peek value")
  void lookup_afterNullCachedKeysRemoved_returnsPeekValue() {
    ZetaCacheContext.get().apply(0, 0, true, false);
    when(zeta.computeIfAbsent(anyString(), any())).thenReturn(null);
    cache.get("myKey", (Callable<String>) () -> null);
    ZetaCacheContext.get().restore(null);

    cache.evict("myKey");
    when(zeta.peek("test::myKey")).thenReturn(Optional.of("new-value"));
    assertThat(cache.lookup("myKey")).isEqualTo("new-value");
  }

  @Test
  @DisplayName("prefixedKey formats correctly")
  void prefixedKey_formatsCorrectly() {
    when(zeta.peek("test::myCustomKey")).thenReturn(Optional.of("v"));
    assertThat(cache.lookup("myCustomKey")).isEqualTo("v");
  }

  @Test
  @DisplayName("prefixedKey uses custom separator when configured")
  void prefixedKey_usesCustomSeparator() {
    springCache.setKeySeparator("-->");
    when(zeta.peek("test-->otherKey")).thenReturn(Optional.of("v2"));
    assertThat(cache.lookup("otherKey")).isEqualTo("v2");
  }

  @Test
  @DisplayName("get wraps loader exception in ValueRetrievalException")
  void get_whenLoaderThrows_wrapsInValueRetrievalException() {
    when(zeta.computeIfAbsent(anyString(), any())).thenAnswer(invocation -> {
      Supplier<Object> supplier = invocation.getArgument(1);
      supplier.get();
      return null;
    });
    assertThatThrownBy(() ->
      cache.get(
        "myKey",
        (Callable<String>) () -> {
          throw new RuntimeException("db error");
        }
      )
    ).isInstanceOf(ValueRetrievalException.class);
  }

  @Test
  @DisplayName("get invokes valueLoader successfully")
  void get_whenLoaderSucceeds_invokesLoader() {
    when(zeta.computeIfAbsent(anyString(), any())).thenAnswer(invocation -> {
      Supplier<Object> supplier = invocation.getArgument(1);
      return supplier.get();
    });
    String result = cache.get("myKey", (Callable<String>) () -> "loaded");
    assertThat(result).isEqualTo("loaded");
  }

  @Test
  @DisplayName("put with skipBroadcast=false calls putThrough")
  void put_withSkipBroadcastFalse_callsPutThrough() {
    ZetaCacheContext.get().apply(0, 0, false, false);
    cache.put("myKey", "myValue");
    verify(zeta).putThrough(eq("test::myKey"), eq("myValue"), any());
  }

  @Test
  @DisplayName("get with allowNull and skipBroadcast=false does not call putThrough")
  void get_whenNullAllowNullAndNoSkipBroadcast_doesNotCallPutThrough() {
    ZetaCacheContext.get().apply(0, 0, true, false);
    when(zeta.computeIfAbsent(anyString(), any())).thenReturn(null);
    String result = cache.get("myKey", (Callable<String>) () -> null);
    assertThat(result).isNull();
    verify(zeta, never()).putThrough(anyString(), any(), any());
    verify(zeta, never()).putLocal(anyString(), any());
  }
}
