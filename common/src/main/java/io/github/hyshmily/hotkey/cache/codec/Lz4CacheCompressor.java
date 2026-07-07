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

import io.github.hyshmily.hotkey.Internal;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.IOException;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * LZ4-based {@link CacheCompressor}. Uses the fastest available LZ4 instance.
 *
 * <p>Format:
 * <ul>
 *   <li>Flag {@code 0x00} = uncompressed: {@code [0x00][raw bytes]}</li>
 *   <li>Flag {@code 0x01} = LZ4 compressed:
 *       {@code [0x01][4-byte original-length LE][LZ4 payload]}</li>
 * </ul>
 * Original length is bounds-checked during decompression to prevent resource
 * exhaustion attacks.
 */
@Internal
public class Lz4CacheCompressor implements CacheCompressor {

  private static final byte FLAG_RAW = 0;
  private static final byte FLAG_LZ4 = 1;

  private final LZ4Compressor compressor;
  private final LZ4FastDecompressor decompressor;

  public Lz4CacheCompressor() {
    LZ4Factory factory = LZ4Factory.fastestInstance();
    this.compressor = factory.fastCompressor();
    this.decompressor = factory.fastDecompressor();
  }

  @Override
  public Object wrap(Object value) {
    if (!(value instanceof String s)) return value;

    byte[] raw = s.getBytes(UTF_8);
    if (raw.length < MIN_COMPRESS_LENGTH) {
      byte[] buf = new byte[raw.length + 1];
      buf[0] = FLAG_RAW;
      System.arraycopy(raw, 0, buf, 1, raw.length);
      return buf;
    }

    int maxLen = compressor.maxCompressedLength(raw.length);
    byte[] compressed = new byte[maxLen + 5];
    compressed[0] = FLAG_LZ4;
    writeLen(compressed, raw.length);
    int len = compressor.compress(raw, 0, raw.length, compressed, 5, maxLen);
    return Arrays.copyOf(compressed, len + 5);
  }

  @Override
  public Object unwrap(Object stored) throws IOException {
    if (!(stored instanceof byte[] b)) return stored;
    if (b.length < 1) return stored;
    if (b[0] == FLAG_RAW) {
      return new String(b, 1, b.length - 1, UTF_8);
    }
    if (b[0] != FLAG_LZ4) return stored;
    if (b.length < 5) throw new IOException("Truncated LZ4 data");

    int originalLen = readLen(b);
    if (originalLen <= 0 || originalLen > 100_000_000) {
      throw new IOException("Invalid decompressed length: " + originalLen);
    }

    byte[] restored = new byte[originalLen];
    try {
      decompressor.decompress(b, 5, restored, 0, originalLen);
    } catch (LZ4Exception e) {
      throw new IOException("LZ4 decompression failed", e);
    }
    return new String(restored, UTF_8);
  }

  private static void writeLen(byte[] buf, int len) {
    buf[1] = (byte) len;
    buf[2] = (byte) (len >>> 8);
    buf[3] = (byte) (len >>> 16);
    buf[4] = (byte) (len >>> 24);
  }

  private static int readLen(byte[] buf) {
    return ((buf[1] & 0xFF) | ((buf[2] & 0xFF) << 8) | ((buf[3] & 0xFF) << 16) | ((buf[4] & 0xFF) << 24));
  }
}
