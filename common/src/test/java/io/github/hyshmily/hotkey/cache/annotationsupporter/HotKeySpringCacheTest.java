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
package io.github.hyshmily.hotkey.cache.annotationsupporter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.github.hyshmily.hotkey.HotKey;
import io.github.hyshmily.hotkey.autoconfigure.HotKeyProperties;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.cache.Cache.ValueRetrievalException;

@DisplayName("HotKeySpringCache tests")
class HotKeySpringCacheTest {

  private HotKey hotKey;
  private HotKeyProperties properties;
  private HotKeyProperties.SpringCache springCache;
  private HotKeySpringCache cache;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    hotKey = mock(HotKey.class);
    properties = mock(HotKeyProperties.class);
    springCache = new HotKeyProperties.SpringCache();
    springCache.setKeySeparator("::");
    when(properties.getSpringCache()).thenReturn(springCache);
    cache = new HotKeySpringCache("test", hotKey, properties, true);
  }

  @AfterEach
  void tearDown() {
    HotKeyCacheContext.get().restore(null);
  }

  @Test
  @DisplayName("getName returns the cache name")
  void getName_returnsCacheName() {
    assertThat(cache.getName()).isEqualTo("test");
  }

  @Test
  @DisplayName("getNativeCache returns the HotKey facade")
  void getNativeCache_returnsHotKey() {
    assertThat(cache.getNativeCache()).isSameAs(hotKey);
  }

  @Test
  @DisplayName("lookup returns value from hotKey.peek")
  void lookup_returnsValueFromPeek() {
    when(hotKey.peek("test::myKey")).thenReturn(Optional.of("stored-value"));
    Object result = cache.lookup("myKey");
    assertThat(result).isEqualTo("stored-value");
  }

  @Test
  @DisplayName("lookup returns null when hotKey.peek is empty")
  void lookup_returnsNullWhenPeekEmpty() {
    when(hotKey.peek("test::myKey")).thenReturn(Optional.empty());
    Object result = cache.lookup("myKey");
    assertThat(result).isNull();
  }

  @Test
  @DisplayName("lookup returns null when key is null-cached")
  void lookup_returnsNullWhenNullCached() {
    HotKeyCacheContext.get().apply(0, 0, true, false);
    when(hotKey.computeIfAbsent(anyString(), any())).thenReturn(null);
    cache.get("myKey", (Callable<String>) () -> null);
    HotKeyCacheContext.get().restore(null);

    assertThat(cache.lookup("myKey")).isNull();
  }

  @Test
  @DisplayName("get without TTL override calls hotKey.computeIfAbsent")
  void get_withoutTtl_callsHotKeyComputeIfAbsent() {
    when(hotKey.computeIfAbsent(eq("test::myKey"), any())).thenReturn("value");
    String result = cache.get("myKey", (Callable<String>) () -> "loaded");
    assertThat(result).isEqualTo("value");
    verify(hotKey).computeIfAbsent(eq("test::myKey"), any());
    verify(hotKey, never()).computeIfAbsentWithSoftExpire(anyString(), any(), anyLong(), anyLong());
  }

  @Test
  @DisplayName("get with TTL override calls hotKey.computeIfAbsentWithSoftExpire")
  void get_withTtlOverride_callsComputeIfAbsentSoft() {
    HotKeyCacheContext.get().apply(5000L, 1000L, false, false);
    when(hotKey.computeIfAbsentWithSoftExpire(eq("test::myKey"), any(), eq(5000L), eq(1000L))).thenReturn("value");
    String result = cache.get("myKey", (Callable<String>) () -> "loaded");
    assertThat(result).isEqualTo("value");
    verify(hotKey, never()).computeIfAbsent(anyString(), any());
    verify(hotKey).computeIfAbsentWithSoftExpire(eq("test::myKey"), any(), eq(5000L), eq(1000L));
  }

  @Test
  @DisplayName("get with only softTtl override calls computeIfAbsentWithSoftExpire")
  void get_withSoftTtlOverride_only() {
    HotKeyCacheContext.get().apply(0L, 500L, false, false);
    when(hotKey.computeIfAbsentWithSoftExpire(eq("test::myKey"), any(), eq(0L), eq(500L))).thenReturn("value");
    cache.get("myKey", (Callable<String>) () -> "loaded");
    verify(hotKey).computeIfAbsentWithSoftExpire(eq("test::myKey"), any(), eq(0L), eq(500L));
  }

  @Test
  @DisplayName("get returns fromStoreValue of the result")
  void get_whenResultPresent_returnsFromStoreValue() {
    when(hotKey.computeIfAbsent(anyString(), any())).thenReturn("computed-value");
    String result = cache.get("myKey", (Callable<String>) () -> "loaded");
    assertThat(result).isEqualTo("computed-value");
  }

  @Test
  @DisplayName("get returns null when result is null and allowNull is false")
  void get_whenNullAndNotAllowNull_returnsNull() {
    when(hotKey.computeIfAbsent(anyString(), any())).thenReturn(null);
    String result = cache.get("myKey", (Callable<String>) () -> null);
    assertThat(result).isNull();
    verify(hotKey, never()).putThrough(anyString(), any(), any());
  }

  @Test
  @DisplayName("get returns null when result is null and allowNull is true")
  void get_whenNullAndAllowNull_returnsNull() {
    HotKeyCacheContext.get().apply(0, 0, true, false);
    when(hotKey.computeIfAbsent(anyString(), any())).thenReturn(null);
    String result = cache.get("myKey", (Callable<String>) () -> null);
    assertThat(result).isNull();
    verify(hotKey, never()).putThrough(anyString(), any(), any());
    verify(hotKey, never()).putLocal(anyString(), any());
  }

  @Test
  @DisplayName("get with allowNull and skipBroadcast does not call putLocal")
  void get_whenNullAllowNullAndSkipBroadcast_doesNotCallPutLocal() {
    HotKeyCacheContext.get().apply(0, 0, true, true);
    when(hotKey.computeIfAbsent(anyString(), any())).thenReturn(null);
    String result = cache.get("myKey", (Callable<String>) () -> null);
    assertThat(result).isNull();
    verify(hotKey, never()).putLocal(anyString(), any());
    verify(hotKey, never()).putThrough(anyString(), any(), any());
  }

  @Test
  @DisplayName("get with allowNull=true adds key to nullCachedKeys")
  void get_whenAllowNull_addsToNullCachedKeys() {
    HotKeyCacheContext.get().apply(0, 0, true, false);
    when(hotKey.computeIfAbsent(anyString(), any())).thenReturn(null);
    cache.get("myKey", (Callable<String>) () -> null);
    assertThat(cache.lookup("myKey")).isNull();
  }

  @Test
  @DisplayName("put stores value via hotKey.putThrough")
  void put_storesValue() {
    cache.put("myKey", "myValue");
    verify(hotKey).putThrough(eq("test::myKey"), eq("myValue"), any());
  }

  @Test
  @DisplayName("put with skipBroadcast uses putLocal")
  void put_withSkipBroadcast_usesPutLocal() {
    HotKeyCacheContext.get().apply(0, 0, false, true);
    cache.put("myKey", "myValue");
    verify(hotKey).putLocal("test::myKey", "myValue");
    verify(hotKey, never()).putThrough(anyString(), any(), any());
  }

  @Test
  @DisplayName("evict with skipBroadcast calls invalidateLocal")
  void evict_withSkipBroadcast_callsInvalidateLocal() {
    HotKeyCacheContext.get().apply(0, 0, false, true);
    cache.evict("myKey");
    verify(hotKey).invalidate("test::myKey", false);
    verify(hotKey, never()).invalidate(anyString());
  }

  @Test
  @DisplayName("put removes key from nullCachedKeys")
  void put_removesFromNullCachedKeys() {
    HotKeyCacheContext.get().apply(0, 0, true, false);
    when(hotKey.computeIfAbsent(anyString(), any())).thenReturn(null);
    cache.get("myKey", (Callable<String>) () -> null);
    assertThat(cache.lookup("myKey")).isNull();
    HotKeyCacheContext.get().restore(null);

    when(hotKey.peek("test::myKey")).thenReturn(Optional.of("real-value"));
    cache.put("myKey", "real-value");
    assertThat(cache.lookup("myKey")).isEqualTo("real-value");
  }

  @Test
  @DisplayName("evict removes from nullCachedKeys and calls invalidate")
  void evict_removesFromNullCachedKeysAndInvalidates() {
    HotKeyCacheContext.get().apply(0, 0, true, false);
    when(hotKey.computeIfAbsent(anyString(), any())).thenReturn(null);
    cache.get("myKey", (Callable<String>) () -> null);
    HotKeyCacheContext.get().restore(null);

    cache.evict("myKey");
    verify(hotKey).invalidate("test::myKey");
    when(hotKey.peek("test::myKey")).thenReturn(Optional.empty());
    assertThat(cache.lookup("myKey")).isNull();
  }

  @Test
  @DisplayName("clear clears nullCachedKeys and calls invalidateAllLocal")
  void clear_clearsNullCachedKeysAndInvalidatesAll() {
    HotKeyCacheContext.get().apply(0, 0, true, false);
    when(hotKey.computeIfAbsent(anyString(), any())).thenReturn(null);
    cache.get("key1", (Callable<String>) () -> null);
    cache.get("key2", (Callable<String>) () -> null);
    HotKeyCacheContext.get().restore(null);

    cache.clear();
    verify(hotKey).invalidateAllLocal();
    verify(hotKey, never()).invalidate(anyString());
    when(hotKey.peek("test::key1")).thenReturn(Optional.empty());
    when(hotKey.peek("test::key2")).thenReturn(Optional.empty());
    assertThat(cache.lookup("key1")).isNull();
  }

  @Test
  @DisplayName("lookup with nullCachedKeys removed returns peek value")
  void lookup_afterNullCachedKeysRemoved_returnsPeekValue() {
    HotKeyCacheContext.get().apply(0, 0, true, false);
    when(hotKey.computeIfAbsent(anyString(), any())).thenReturn(null);
    cache.get("myKey", (Callable<String>) () -> null);
    HotKeyCacheContext.get().restore(null);

    cache.evict("myKey");
    when(hotKey.peek("test::myKey")).thenReturn(Optional.of("new-value"));
    assertThat(cache.lookup("myKey")).isEqualTo("new-value");
  }

  @Test
  @DisplayName("prefixedKey formats correctly")
  void prefixedKey_formatsCorrectly() {
    when(hotKey.peek("test::myCustomKey")).thenReturn(Optional.of("v"));
    assertThat(cache.lookup("myCustomKey")).isEqualTo("v");
  }

  @Test
  @DisplayName("prefixedKey uses custom separator when configured")
  void prefixedKey_usesCustomSeparator() {
    springCache.setKeySeparator("-->");
    when(hotKey.peek("test-->otherKey")).thenReturn(Optional.of("v2"));
    assertThat(cache.lookup("otherKey")).isEqualTo("v2");
  }

  @Test
  @DisplayName("get wraps loader exception in ValueRetrievalException")
  void get_whenLoaderThrows_wrapsInValueRetrievalException() {
    when(hotKey.computeIfAbsent(anyString(), any())).thenAnswer(invocation -> {
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
    when(hotKey.computeIfAbsent(anyString(), any())).thenAnswer(invocation -> {
      Supplier<Object> supplier = invocation.getArgument(1);
      return supplier.get();
    });
    String result = cache.get("myKey", (Callable<String>) () -> "loaded");
    assertThat(result).isEqualTo("loaded");
  }

  @Test
  @DisplayName("put with skipBroadcast=false calls putThrough")
  void put_withSkipBroadcastFalse_callsPutThrough() {
    HotKeyCacheContext.get().apply(0, 0, false, false);
    cache.put("myKey", "myValue");
    verify(hotKey).putThrough(eq("test::myKey"), eq("myValue"), any());
  }

  @Test
  @DisplayName("get with allowNull and skipBroadcast=false does not call putThrough")
  void get_whenNullAllowNullAndNoSkipBroadcast_doesNotCallPutThrough() {
    HotKeyCacheContext.get().apply(0, 0, true, false);
    when(hotKey.computeIfAbsent(anyString(), any())).thenReturn(null);
    String result = cache.get("myKey", (Callable<String>) () -> null);
    assertThat(result).isNull();
    verify(hotKey, never()).putThrough(anyString(), any(), any());
    verify(hotKey, never()).putLocal(anyString(), any());
  }
}
