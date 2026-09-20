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
package io.github.hyshmily.zeta.reporting;

import io.github.hyshmily.zeta.Internal;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Compact binary codec for {@link ReportMessage} wire bodies, adapted from
 * RocksDB {@code util/coding.h} (varint continuation encoding, single-byte
 * fast path, length-prefix framing).
 *
 * <h3>Wire layout</h3>
 * <pre>
 * [0]  magic   0x5A ('Z') — lets a receiver pick the decoder from the body alone
 * [1]  version 0x01
 * varint       id (Snowflake, unsigned 64-bit)
 * varint       timestamp (epoch millis, unsigned 64-bit)
 * varint32     appName length + UTF-8 bytes
 * varint32     entry count
 * per entry:   varint32 key length + UTF-8 key bytes + varint64 count
 * </pre>
 *
 * <p>Counts are small integers in practice, so they cost 1-2 bytes; the
 * win over JSON comes from dropping the per-entry quoting/structural
 * overhead. Every read is bounds-checked — a truncated or absurd length
 * field throws {@link IllegalArgumentException} instead of allocating, so a
 * corrupt body can never become an allocation bomb (deserialization-DoS
 * guard).
 *
 * <p><b>Nothing on the success path allocates state that is immediately
 * discarded.</b> Every guard uses {@code if (!ok) throw} instead of a
 * message-building helper call, because the message argument of a helper such
 * as {@code Assert.isTrue(cond, msg)} is evaluated eagerly for every key even
 * when nothing throws — 160 bytes per entry on a 2000-entry batch, measured
 * against this class (method and numbers: ADR-0074). Encode sizes the body
 * exactly in a first pass and writes it into that one array instead of guessing
 * a capacity: the previous {@code 64 + size * 16} hint assumed ~14-byte keys, so
 * a 40-byte-key batch overshot it 2.7x, the stream doubled its buffer twice and
 * {@code toByteArray()} copied the whole body — 53% of that method's allocation
 * (ADR-0074).
 *
 * <p>What is deliberately <em>not</em> optimised is the decode-side fixed cost:
 * the {@code int[]} cursor is one small array per decode, 0.1% of that method's
 * total allocation against 99.9% for the per-entry work, and a {@code ByteBuffer}
 * rewrite would add an abstraction without removing the hand-rolled varint loops
 * (ADR-0074).
 *
 * <p><b>Malformed input is rejected, not repaired.</b> Decode refuses trailing
 * bytes, a varint whose final byte overflows the 64-bit accumulator, and keys
 * or app names that are not well-formed UTF-8 — the JDK's replacement-character
 * decode would silently turn a forged body into a key that was never sent. The
 * encoder only ever emits well-formed UTF-8, so none of these can reject a body
 * this class produced.
 *
 * <p><b>The byte string is not canonical.</b> Two encodes of the same logical
 * message may differ: {@code counts} is a {@link HashMap} whose iteration order
 * depends on insertion history and capacity, and decode accepts non-minimal
 * varint encodings (an entry count padded with continuation bytes). Do not hash
 * or dedupe bodies on their wire representation.
 *
 * <p><b>Bumping {@link #VERSION} is an upgrade event, not a code detail.</b>
 * Decode-side format selection never depends on this class alone: receivers
 * sniff {@link #MAGIC} on the first body byte and otherwise fall back to JSON,
 * which is what makes the JSON→COMPACT rolling upgrade safe (ADR-0074). A body
 * carrying a *newer* version also matches the magic, so it fails as a malformed
 * body rather than falling back to JSON; a version bump therefore needs the same
 * Workers-first sequence as the original flip, and is only safe once every
 * receiver accepts both versions.
 */
@Internal
public final class ReportMessageCodec {

  /** First body byte of the compact encoding ('Z'); JSON bodies start with '{' (0x7B). */
  public static final byte MAGIC = 0x5A;

  /** Wire-format version — bumped only for incompatible layout changes. */
  public static final byte VERSION = 0x01;

  /**
   * Largest entry count used to pre-size the result map. {@code entryCount} is
   * attacker-controlled and bounded only by the body length, so it may drive a
   * large up-front table allocation; {@link HashMap} grows on demand, so capping
   * the hint costs nothing and takes that allocation off the attack surface.
   */
  private static final int MAX_PRESIZE = 4_096;

  private static final String MALFORMED_MESSAGE = "Malformed compact report message: ";

  /** Reads eight body bytes as one long for the ASCII fast path. */
  private static final VarHandle LONG_VIEW = MethodHandles.byteArrayViewVarHandle(
    long[].class,
    ByteOrder.LITTLE_ENDIAN
  );

  private ReportMessageCodec() {}

  /**
   * Whether the given body starts with the compact-encoding magic byte.
   *
   * @param body the raw message body, may be {@code null} or empty
   * @return {@code true} if the body claims the compact format
   */
  public static boolean isCompact(byte[] body) {
    return body != null && body.length >= 2 && body[0] == MAGIC;
  }

  /**
   * Encode a report into the compact binary body.
   *
   * <p>Sizes the body in a first pass and writes it into that single array, so
   * the method allocates only the result plus one {@code byte[]} per key. The
   * map is iterated twice — once to size, once to write — so {@code counts}
   * must not be mutated while the encode runs; the production report path
   * encodes a drained snapshot. Null keys or values fail with a named error.
   *
   * @param message the report to encode
   * @return the encoded body
   */
  public static byte[] encode(ReportMessage message) {
    // The compact format cannot express null fields (JSON can): reject them
    // at the boundary — the production report path never produces them.
    String appNameValue = Objects.requireNonNull(message.appName(), "appName must be non-null for compact encoding");
    Map<String, Long> counts = Objects.requireNonNull(message.counts(), "counts must be non-null for compact encoding");
    // Read once: the two passes below must size and write the same shape.
    int entryCount = counts.size();
    byte[] appName = appNameValue.getBytes(StandardCharsets.UTF_8);

    // First pass: exact size, no allocation. Sizing the key lengths is why
    // utf8Length exists — getBytes here would allocate every key twice.
    int size =
      2 +
      varlongLength(message.id()) +
      varlongLength(message.timestamp()) +
      varlongLength(appName.length) +
      appName.length +
      varlongLength(entryCount);
    for (Map.Entry<String, Long> entry : counts.entrySet()) {
      // Named failures before any arithmetic: a null here would otherwise
      // surface as a bare NPE from utf8Length or from Long unboxing.
      String key = Objects.requireNonNull(entry.getKey(), "count key must be non-null");
      Long count = Objects.requireNonNull(entry.getValue(), "count value must be non-null");
      int keyLength = utf8Length(key);
      size += varlongLength(keyLength) + keyLength + varlongLength(count);
    }
    // Reachable only with a map far beyond any real batch; a wrapped size
    // would otherwise die as NegativeArraySizeException at the allocation.
    UNSAFE.check(size < 0, "Compact report message is too large: encoded size overflows int");

    byte[] out = new byte[size];
    int p = 0;
    out[p++] = MAGIC;
    out[p++] = VERSION;
    p = writeVarlong(out, p, message.id());
    p = writeVarlong(out, p, message.timestamp());
    p = writeVarlong(out, p, appName.length);
    System.arraycopy(appName, 0, out, p, appName.length);
    p += appName.length;
    p = writeVarlong(out, p, entryCount);
    for (Map.Entry<String, Long> entry : counts.entrySet()) {
      byte[] key = entry.getKey().getBytes(StandardCharsets.UTF_8);
      p = writeVarlong(out, p, key.length);
      System.arraycopy(key, 0, out, p, key.length);
      p += key.length;
      p = writeVarlong(out, p, entry.getValue());
    }
    // The two passes must agree; if they ever do not, the first one is wrong
    // and the body would ship with a hole in it (silent wire corruption).
    UNSAFE.ENCODE.checkLength(p, size);
    return out;
  }

  /**
   * Decode a compact binary body back into a report. Every field is
   * bounds-checked against the body length before it is read. A duplicated
   * key keeps the last occurrence — the encoder never emits duplicates, so
   * the behavior only matters for forged bodies.
   *
   * @param body the raw body, must start with {@link #MAGIC}
   * @return the decoded report
   * @throws IllegalArgumentException when the body is truncated, malformed,
   *         carries an unsupported version, holds text that is not well-formed
   *         UTF-8, or has bytes left over after the last entry
   */
  public static ReportMessage decode(byte[] body) {
    UNSAFE.DECODE.checkTag(body);
    UNSAFE.DECODE.checkVersion(body);

    int[] pos = { 2 };
    int end = body.length;

    long id = readVarlong(body, pos, end);
    long timestamp = readVarlong(body, pos, end);
    int appNameLength = readVarint32(body, pos, end);
    UNSAFE.requireRemaining(pos, end, appNameLength, "appName");
    String appName = readUtf8(body, pos[0], appNameLength, "appName");
    pos[0] += appNameLength;

    int entryCount = readVarint32(body, pos, end);
    // Every entry costs at least 2 bytes (1-byte key length + 1-byte count),
    // so an entry count beyond the remaining bytes is malformed — reject it
    // before it can size the result map.
    UNSAFE.DECODE.checkRemaining(pos, end, entryCount, "entry count");

    Map<String, Long> counts = new HashMap<>(Math.max(4, Math.min(entryCount, MAX_PRESIZE)));
    for (int i = 0; i < entryCount; i++) {
      int keyLength = readVarint32(body, pos, end);
      UNSAFE.requireRemaining(pos, end, keyLength, "key");
      String key = readUtf8(body, pos[0], keyLength, "key");
      pos[0] += keyLength;
      long count = readVarlong(body, pos, end);
      counts.put(key, count);
    }

    // Leftover bytes mean the sender and the receiver disagree about the
    // layout — the one framing error a magic-sniffing receiver cannot see.
    UNSAFE.DECODE.checkEnd(pos, end);
    return new ReportMessage(id, appName, timestamp, counts);
  }

  /** Writes {@code value} as a varint64 into {@code buf} at {@code pos}; returns the position after it. */
  private static int writeVarlong(byte[] buf, int pos, long value) {
    long v = value;
    int p = pos;
    while ((v & ~0x7FL) != 0) {
      buf[p++] = (byte) ((v & 0x7F) | 0x80);
      v >>>= 7;
    }
    buf[p++] = (byte) v;
    return p;
  }

  /** Number of bytes {@link #writeVarlong} will write for {@code value}; the sizing pass of {@link #encode}. */
  private static int varlongLength(long value) {
    int length = 1;
    long v = value;
    while ((v & ~0x7FL) != 0) {
      v >>>= 7;
      length++;
    }
    return length;
  }

  /**
   * Exact UTF-8 byte length of {@code value}, equal to {@code value.getBytes(UTF_8).length} —
   * including the single-byte {@code '?'} the JDK encoder substitutes for an unpaired surrogate.
   *
   * <p>Package-private so the unit test can differential-test it against the JDK encoder
   * instead of trusting the arithmetic.
   */
  static int utf8Length(String value) {
    int length = 0;
    for (int i = 0, n = value.length(); i < n; i++) {
      char c = value.charAt(i);
      if (c < 0x80) {
        length += 1;
      } else if (c < 0x800) {
        length += 2;
      } else if (Character.isHighSurrogate(c)) {
        if (i + 1 < n && Character.isLowSurrogate(value.charAt(i + 1))) {
          length += 4;
          i++;
        } else {
          length += 1;
        }
      } else if (Character.isLowSurrogate(c)) {
        length += 1;
      } else {
        length += 3;
      }
    }
    return length;
  }

  /**
   * Reads an unsigned varint64; every continuation byte is bounds-checked.
   *
   * <p>The tenth byte may only carry the one bit that still fits in a long:
   * Java shifts are modulo 64, so a larger payload would be dropped silently and
   * a malformed body would decode to a <em>wrong</em> value instead of failing.
   */
  @SuppressWarnings("all")
  private static long readVarlong(byte[] body, int[] pos, int end) {
    long value = 0;
    int shift = 0;
    int p = pos[0];
    while (p < end) {
      byte b = body[p++];
      long payload = b & 0x7FL;
      UNSAFE.check((shift == 63 && payload > 1L), MALFORMED_MESSAGE + "varint exceeds 64 bits");

      value |= payload << shift;
      if ((b & 0x80) == 0) {
        pos[0] = p;
        return value;
      }

      shift += 7;
      UNSAFE.check((shift > 63), MALFORMED_MESSAGE + "varint exceeds 64 bits");
    }
    UNSAFE.check(true, MALFORMED_MESSAGE + "truncated varint");
    return -1;
  }

  /**
   * Reads an unsigned varint32, rejecting values beyond the int range.
   * <p>Delegating to {@link #readVarlong} is deliberate: it stops at the first
   * byte without a continuation bit, so a typical 1-2 byte length costs 1-2
   * iterations — a dedicated 32-bit loop measured no gain (ADR-0074).
   */
  private static int readVarint32(byte[] body, int[] pos, int end) {
    long value = readVarlong(body, pos, end);
    // A value in [2^63, 2^64) returns from readVarlong as a negative long and
    // would slip past an upper-bound-only check, truncating a forged length to
    // its low 32 bits — a wrong value accepted as success instead of failing.
    UNSAFE.check((value < 0 || value > Integer.MAX_VALUE), MALFORMED_MESSAGE + "varint32 exceeds 32 bits");
    return (int) value;
  }

  /**
   * Decodes a UTF-8 field, rejecting bytes that are not well-formed UTF-8.
   *
   * <p>{@code new String(bytes, charset)} substitutes U+FFFD for malformed
   * input, so a forged body would decode into a key that was never sent and
   * propagate it into the Worker's TopK state. Validating first keeps decode a
   * faithful inverse of encode, which only ever emits well-formed UTF-8.
   *
   * @throws IllegalArgumentException when the range is not well-formed UTF-8
   */
  private static String readUtf8(byte[] body, int offset, int length, String what) {
    // Inline throw, not a guard-helper call: `what` is a runtime value, so a
    // helper would concatenate the message on every successful key decode —
    // the eager-message cost the class javadoc forbids (ADR-0074, item 1).
    if (!isWellFormedUtf8(body, offset, length)) {
      throw new IllegalArgumentException(MALFORMED_MESSAGE + what + " is not well-formed UTF-8");
    }
    return new String(body, offset, length, StandardCharsets.UTF_8);
  }

  /**
   * Whether {@code body[offset, offset + length)} is well-formed UTF-8 per
   * RFC 3629: rejects overlong forms, UTF-16 surrogates, code points above
   * U+10FFFF, stray continuation bytes and truncated sequences.
   *
   * <p>Package-private so the unit test can differential-test it against the
   * JDK decoder's {@code REPORT} mode over an exhaustive two-byte corpus.
   */
  static boolean isWellFormedUtf8(byte[] body, int offset, int length) {
    int p = offset;
    int end = offset + length;
    // Fast path: cache keys are overwhelmingly ASCII, so while no byte in the
    // next whole word has its high bit set, eight bytes are already known good.
    // The mask is endianness-independent — it tests one bit per byte.
    while (end - p >= Long.BYTES) {
      if (((long) LONG_VIEW.get(body, p) & 0x8080808080808080L) != 0L) {
        break;
      }
      p += Long.BYTES;
    }

    while (p < end) {
      int first = body[p] & 0xFF;
      if (first < 0x80) {
        p++;
        continue;
      }

      int continuations;
      int codePoint;
      int smallest;
      if (first >= 0xC2 && first <= 0xDF) {
        continuations = 1;
        codePoint = first & 0x1F;
        smallest = 0x80;
      } else if (first >= 0xE0 && first <= 0xEF) {
        continuations = 2;
        codePoint = first & 0x0F;
        smallest = 0x800;
      } else if (first >= 0xF0 && first <= 0xF4) {
        continuations = 3;
        codePoint = first & 0x07;
        smallest = 0x10000;
      } else {
        // 0x80-0xC1: a continuation byte, or an overlong two-byte lead.
        // 0xF5-0xFF: beyond the Unicode range.
        return false;
      }

      if (end - p <= continuations) {
        return false;
      }

      for (int i = 1; i <= continuations; i++) {
        int follower = body[p + i] & 0xFF;
        if ((follower & 0xC0) != 0x80) {
          return false;
        }
        codePoint = (codePoint << 6) | (follower & 0x3F);
      }

      if (codePoint < smallest || codePoint > 0x10FFFF || (codePoint >= 0xD800 && codePoint <= 0xDFFF)) {
        return false;
      }
      p += continuations + 1;
    }
    return true;
  }

  /**
   * Shared validation guards. Despite the name, nothing here touches
   * {@code sun.misc.Unsafe}: the class only centralizes {@code throw} sites so
   * hot paths build no error message on the success path. That guarantee holds
   * for {@link #check} call sites whose message is a compile-time constant
   * (javac folds it into one interned string); guards with a dynamic message
   * part must inline their {@code throw} instead (see {@link #readUtf8}).
   */
  private static class UNSAFE {

    /**
     * Throws {@code IllegalArgumentException} carrying {@code what} when
     * {@code error} is true.
     */
    public static void check(boolean error, String what) {
      if (error) {
        throw new IllegalArgumentException(what);
      }
    }

    /** Ensures that {@code length} fits between the cursor position {@code pos[0]} and the body end {@code end}. */
    public static void requireRemaining(int[] pos, int end, int length, String what) {
      if (length < 0 || length > end - pos[0]) {
        throw new IllegalArgumentException(MALFORMED_MESSAGE + what + " length " + length + " exceeds remaining body");
      }
    }

    static class ENCODE {

      /** Post-write invariant: the cursor must land exactly on the pre-computed size. */
      public static void checkLength(int p, int size) {
        if (p != size) {
          throw new IllegalStateException("Compact encode size mismatch: wrote " + p + " of " + size + " bytes");
        }
      }
    }

    static class DECODE {

      public static void checkTag(byte[] body) {
        if (body == null || body.length < 2 || body[0] != MAGIC) {
          throw new IllegalArgumentException(MALFORMED_MESSAGE + "missing magic header");
        }
      }

      public static void checkVersion(byte[] body) {
        if (body[1] != VERSION) {
          throw new IllegalArgumentException(MALFORMED_MESSAGE + "unsupported version " + body[1]);
        }
      }

      public static void checkRemaining(int[] pos, int end, int length, String what) {
        if (length < 0 || length > (end - pos[0]) >> 1) {
          throw new IllegalArgumentException(
            MALFORMED_MESSAGE + what + " length " + length + " exceeds remaining body"
          );
        }
      }

      public static void checkEnd(int[] pos, int end) {
        if (pos[0] != end) {
          throw new IllegalArgumentException(MALFORMED_MESSAGE + (end - pos[0]) + " trailing bytes");
        }
      }
    }
  }
}
