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
package io.github.hyshmily.hotkey.cache.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class Lz4CacheCompressorTest {

  private Lz4CacheCompressor compressor;

  @BeforeEach
  void setUp() {
    compressor = new Lz4CacheCompressor();
  }

  @Test
  void wrap_shortString_shouldStoreAsRaw() {
    byte[] result = (byte[]) compressor.wrap("hello");
    assertThat(result[0]).isZero(); // FLAG_RAW
    assertThat(new String(result, 1, result.length - 1, java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("hello");
  }

  @Test
  void wrap_longString_shouldCompress() {
    String input = "x".repeat(500);
    byte[] result = (byte[]) compressor.wrap(input);
    assertThat(result[0]).isEqualTo((byte) 1); // FLAG_LZ4
    assertThat(result.length).isLessThan(input.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
  }

  @Test
  void wrap_nonString_shouldPassThrough() {
    Object result = compressor.wrap(42);
    assertThat(result).isEqualTo(42);
  }

  @Test
  void wrap_null_shouldReturnNull() {
    Object result = compressor.wrap(null);
    assertThat(result).isNull();
  }

  @Test
  void unwrap_compressedString_shouldReturnOriginal() throws IOException {
    String input = "x".repeat(500);
    byte[] compressed = (byte[]) compressor.wrap(input);
    Object result = compressor.unwrap(compressed);
    assertThat(result).isEqualTo(input);
  }

  @Test
  void unwrap_rawString_shouldReturnOriginal() throws IOException {
    String input = "hello";
    byte[] raw = (byte[]) compressor.wrap(input);
    Object result = compressor.unwrap(raw);
    assertThat(result).isEqualTo(input);
  }

  @Test
  void unwrap_nonByteArray_shouldPassThrough() throws IOException {
    Object result = compressor.unwrap("notbytes");
    assertThat(result).isEqualTo("notbytes");
  }

  @Test
  void unwrap_truncatedData_shouldThrow() {
    assertThatThrownBy(() -> compressor.unwrap(new byte[] { 1, 0, 0 })).isInstanceOf(IOException.class);
  }

  @Test
  void wrapAndUnwrap_roundTrip_shouldPreserveContent() throws IOException {
    String[] inputs = { "a", "ab", "abc", "x".repeat(255), "x".repeat(256), "x".repeat(1000) };
    for (String input : inputs) {
      byte[] wrapped = (byte[]) compressor.wrap(input);
      Object result = compressor.unwrap(wrapped);
      assertThat(result).as("Round-trip failed for length=" + input.length()).isEqualTo(input);
    }
  }

  @Test
  void cacheCompressorNONE_shouldPassThrough() throws IOException {
    assertThat(CacheCompressor.NONE.wrap("hello")).isEqualTo("hello");
    assertThat(CacheCompressor.NONE.unwrap("hello")).isEqualTo("hello");
    assertThat(CacheCompressor.NONE.wrap(null)).isNull();
  }
}
