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
package io.github.hyshmily.zeta.cache.codec;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.hyshmily.zeta.Internal;
import java.io.IOException;
import java.util.Arrays;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * LZ4-based {@link CacheCompressor}. Uses the fastest available LZ4 instance.
 *
 * <p>Format:
 * <ul>
 *   <li>Flag {@code 0x00} = uncompressed String: {@code [0x00][UTF-8 bytes]}</li>
 *   <li>Flag {@code 0x01} = LZ4 compressed String:
 *       {@code [0x01][4-byte original-length LE][LZ4 payload]}</li>
 *   <li>Flag {@code 0x02} = LZ4 compressed byte[]:
 *       {@code [0x02][4-byte original-length LE][LZ4 payload]}</li>
 *   <li>Flag {@code 0x03} = uncompressed byte[]: {@code [0x03][raw bytes]}</li>
 * </ul>
 * Flags {@code 0x01}/{@code 0x02} are only written when the compressed form is strictly
 * smaller than the flag-prefixed raw form; incompressible values fall back to
 * {@code 0x00}/{@code 0x03} so hits never pay a decompression that bought nothing.
 * Original length is bounds-checked during decompression to prevent resource
 * exhaustion attacks. An unknown flag throws {@link IOException} — the format
 * exists only inside this JVM's L1 (ADR-0015), so an unrecognized flag means
 * corruption, and the caller's invalidate-and-reload path must treat it as
 * such rather than receive a wrongly-typed value.
 */
@Internal
public class Lz4CacheCompressor implements CacheCompressor {

  private static final byte FLAG_RAW = 0;
  private static final byte FLAG_LZ4 = 1;
  private static final byte FLAG_LZ4_BYTES = 2;
  private static final byte FLAG_RAW_BYTES = 3;

  /**
   * Upper bound on the decompressed size accepted from the 4-byte length header. The
   * {@code originalLen} allocation happens before LZ4 validates a single byte, so this
   * bound is the only guard against a corrupt header triggering a huge allocation. It is
   * deliberately generous: a legit stored value larger than the bound would fail
   * decompression on every read (invalidate → reload → fail again), so only a bound no
   * real value can exceed is safe. The compressed form never leaves this JVM's L1
   * (ADR-0015), so an over-large claim can only be corruption, never a foreign format.
   */
  private static final int MAX_DECOMPRESSED_BYTES = 100_000_000;

  /**
   * Reusable compression scratch buffer, one per thread. Compress allocates a
   * {@code maxCompressedLength + 5} array plus a final {@code copyOf} result
   * per call; reusing the intermediate array removes one allocation per
   * compression. The buffer grows with the largest value compressed on that
   * thread and stays resident, capped at {@link #SCRATCH_MAX_BYTES}: beyond
   * the cap the buffer is per-call, so one huge value cannot pin its
   * worst-case buffer in every executor thread for the process lifetime.
   * Package-private so tests can assert the cap.
   */
  static final ThreadLocal<byte[]> SCRATCH = ThreadLocal.withInitial(() -> new byte[0]);

  /** Scratch-buffer residency cap (1 MiB); larger buffers are per-call. Package-private for tests. */
  static final int SCRATCH_MAX_BYTES = 1 << 20;

  private final LZ4Compressor compressor;
  private final LZ4FastDecompressor decompressor;

  public Lz4CacheCompressor() {
    LZ4Factory factory = LZ4Factory.fastestInstance();
    this.compressor = factory.fastCompressor();
    this.decompressor = factory.fastDecompressor();
  }

  @Override
  public Object wrap(Object value) {
    if (value instanceof String s) {
      return wrapString(s);
    }
    if (value instanceof byte[] b) {
      return wrapBytes(b);
    }
    return value;
  }

  private Object wrapString(String s) {
    // Char-count fast path: the common small-value case must not pay a full
    // UTF-8 encode + byte[] allocation for a length check that then discards
    // it. Char count is an approximation of byte length for multi-byte
    // characters — a string just below the threshold may skip compression
    // even if its UTF-8 form would exceed it, which only costs a little
    // space, never correctness (unwrap returns non-byte[] values verbatim).
    if (s.length() < MIN_COMPRESS_LENGTH) {
      return s;
    }
    byte[] raw = s.getBytes(UTF_8);
    if (raw.length < MIN_COMPRESS_LENGTH) {
      // Small values pass through as the original String: unwrap returns
      // non-byte[] values verbatim, so hits are a zero-copy, zero-allocation
      // return of the stored instance. Compression only pays off at or above
      // MIN_COMPRESS_LENGTH — small values must not pay the per-hit byte[]
      // decode cost for a benefit they never receive.
      return s;
    }
    return process(raw, FLAG_RAW, FLAG_LZ4);
  }

  private byte[] wrapBytes(byte[] raw) {
    return process(raw, FLAG_RAW_BYTES, FLAG_LZ4_BYTES);
  }

  private byte[] process(byte[] raw, byte flagRaw, byte flagLz4) {
    if (raw.length < MIN_COMPRESS_LENGTH) {
      return prefixed(raw, flagRaw);
    }
    return compress(raw, flagRaw, flagLz4);
  }

  private byte[] compress(byte[] raw, byte flagRaw, byte flagLz4) {
    int maxLen = compressor.maxCompressedLength(raw.length);
    int need = maxLen + 5;
    byte[] scratch = SCRATCH.get();
    if (scratch.length < need) {
      scratch = new byte[need];
      if (need <= SCRATCH_MAX_BYTES) {
        SCRATCH.set(scratch);
      }
    }
    scratch[0] = flagLz4;
    writeLen(scratch, raw.length);
    int len = compressor.compress(raw, 0, raw.length, scratch, 5, maxLen);
    if (len + 5 >= raw.length + 1) {
      // Incompressible payload: the compressed form would be no smaller than
      // the flag-prefixed raw form, so store raw — the stored size is the same
      // or better and every hit skips a decompression that buys nothing.
      return prefixed(raw, flagRaw);
    }
    return Arrays.copyOf(scratch, len + 5);
  }

  private static byte[] prefixed(byte[] raw, byte flag) {
    byte[] buf = new byte[raw.length + 1];
    buf[0] = flag;
    System.arraycopy(raw, 0, buf, 1, raw.length);
    return buf;
  }

  @Override
  public Object unwrap(Object stored) throws IOException {
    if (!(stored instanceof byte[] b)) {
      return stored;
    }
    if (b.length < 1) {
      return stored;
    }

    return switch (b[0]) {
      case FLAG_RAW -> new String(b, 1, b.length - 1, UTF_8);
      case FLAG_LZ4 -> new String(decompress(b), UTF_8);
      case FLAG_RAW_BYTES -> Arrays.copyOfRange(b, 1, b.length);
      case FLAG_LZ4_BYTES -> decompress(b);
      default -> throw new IOException("Unknown codec flag: 0x" + Integer.toHexString(b[0] & 0xFF));
    };
  }

  private byte[] decompress(byte[] compressed) throws IOException {
    if (compressed.length < 5) {
      throw new IOException("Truncated LZ4 data");
    }
    int originalLen = readLen(compressed);
    if (originalLen <= 0 || originalLen > MAX_DECOMPRESSED_BYTES) {
      throw new IOException("Invalid decompressed length: " + originalLen);
    }
    byte[] restored = new byte[originalLen];
    try {
      decompressor.decompress(compressed, 5, restored, 0, originalLen);
    } catch (LZ4Exception e) {
      throw new IOException("LZ4 decompression failed", e);
    }
    return restored;
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
