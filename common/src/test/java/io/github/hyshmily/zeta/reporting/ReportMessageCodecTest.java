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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Round-trip, size, and malformed-input tests for the compact report wire
 * format ({@link ReportMessageCodec}, ADR-0074).
 */
class ReportMessageCodecTest {

  private static ReportMessage sample() {
    Map<String, Long> counts = new LinkedHashMap<>();
    counts.put("user:1001", 1L);
    counts.put("user:1002", 127L);
    counts.put("user:1003", 128L);
    counts.put("订单:2026:锁定", 300L);
    counts.put("product:sku:000000000042", 1L << 40);
    return new ReportMessage(123456789012345L, "order-service", 1_758_000_000_000L, counts);
  }

  @Test
  void roundTrip_shouldReproduceTheMessage() {
    ReportMessage original = sample();
    ReportMessage decoded = ReportMessageCodec.decode(ReportMessageCodec.encode(original));
    assertThat(decoded).isEqualTo(original);
  }

  @Test
  void roundTrip_emptyCounts() {
    ReportMessage original = new ReportMessage(42L, "app", 7L, Map.of());
    ReportMessage decoded = ReportMessageCodec.decode(ReportMessageCodec.encode(original));
    assertThat(decoded).isEqualTo(original);
  }

  @Test
  void roundTrip_boundaryCounts() {
    Map<String, Long> counts = new HashMap<>();
    counts.put("a", 0L);
    counts.put("b", 127L);
    counts.put("c", 128L);
    counts.put("d", Long.MAX_VALUE);
    ReportMessage original = new ReportMessage(-1L, "app", -1L, counts);
    // Negative id/timestamp are varint-safe (two's-complement shifts) even
    // though the production path never produces them.
    ReportMessage decoded = ReportMessageCodec.decode(ReportMessageCodec.encode(original));
    assertThat(decoded).isEqualTo(original);
  }

  @Test
  void roundTrip_longKeys() {
    String longKey = "k".repeat(10_000);
    ReportMessage original = new ReportMessage(1L, "app", 1L, Map.of(longKey, 3L));
    ReportMessage decoded = ReportMessageCodec.decode(ReportMessageCodec.encode(original));
    assertThat(decoded).isEqualTo(original);
  }

  @Test
  void encode_bodyStartsWithMagicAndVersion() {
    byte[] body = ReportMessageCodec.encode(sample());
    assertThat(body[0]).isEqualTo((byte) 'Z');
    assertThat(body[1]).isEqualTo((byte) 0x01);
    assertThat(ReportMessageCodec.isCompact(body)).isTrue();
  }

  @Test
  void isCompact_rejectsNonCompactBodies() {
    assertThat(ReportMessageCodec.isCompact(null)).isFalse();
    assertThat(ReportMessageCodec.isCompact(new byte[0])).isFalse();
    assertThat(ReportMessageCodec.isCompact("{\"id\":1".getBytes(StandardCharsets.UTF_8))).isFalse();
    assertThat(ReportMessageCodec.isCompact(new byte[] { 'Z' })).isFalse();
  }

  @Test
  void encode_isSmallerThanAnEquivalentJsonBody() {
    ReportMessage message = sample();
    byte[] compact = ReportMessageCodec.encode(message);
    String json = manualJson(message);
    assertThat(compact.length).isLessThan(json.length());
  }

  @Test
  void encode_nullAppNameOrNullCounts_rejected() {
    assertThatThrownBy(() -> ReportMessageCodec.encode(new ReportMessage(1L, null, 1L, Map.of("k", 1L)))).isInstanceOf(
      NullPointerException.class
    );
    assertThatThrownBy(() -> ReportMessageCodec.encode(new ReportMessage(1L, "app", 1L, null))).isInstanceOf(
      NullPointerException.class
    );
  }

  @Test
  void decode_nonCompactBody_rejected() {
    assertThatThrownBy(() -> ReportMessageCodec.decode("{\"id\":1".getBytes(StandardCharsets.UTF_8))).isInstanceOf(
      IllegalArgumentException.class
    );
    assertThatThrownBy(() -> ReportMessageCodec.decode(null)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void decode_unsupportedVersion_rejected() {
    byte[] body = ReportMessageCodec.encode(sample());
    body[1] = 0x02;
    assertThatThrownBy(() -> ReportMessageCodec.decode(body)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void decode_truncatedBody_rejected() {
    byte[] body = ReportMessageCodec.encode(sample());
    for (int cut = 2; cut < body.length; cut += 7) {
      byte[] truncated = new byte[cut];
      System.arraycopy(body, 0, truncated, 0, cut);
      assertThatThrownBy(() -> ReportMessageCodec.decode(truncated))
        .as("truncation at %s must be detected", cut)
        .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  void decode_absurdEntryCount_rejected() {
    byte[] body = ReportMessageCodec.encode(new ReportMessage(1L, "app", 1L, Map.of("k", 1L)));
    // Layout: magic(1) version(1) varint(id) varint(ts) varint(nameLen) "app" varint(entryCount)…
    int entryCountPos = 2 + varintLength(1L) + varintLength(1L) + varintLength("app".length()) + 3;
    body[entryCountPos] = 0x7F; // claims 127 entries, but only one follows
    assertThatThrownBy(() -> ReportMessageCodec.decode(body)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void decode_absurdKeyLength_rejected() {
    byte[] body = ReportMessageCodec.encode(new ReportMessage(1L, "app", 1L, Map.of("k", 1L)));
    int keyLenPos = 2 + varintLength(1L) + varintLength(1L) + varintLength("app".length()) + 3 + 1;
    body[keyLenPos] = 0x7F; // claims a 127-byte key, but only "k" follows
    assertThatThrownBy(() -> ReportMessageCodec.decode(body)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void encode_goldenBytes_minimalMessage() {
    ReportMessage message = new ReportMessage(1L, "app", 1L, Map.of("a", 1L));
    // Literal, hand-derived from the documented layout. A round-trip test
    // cannot catch a layout change that stays self-consistent — this can.
    byte[] expected = new byte[] {
      0x5A, // magic 'Z'
      0x01, // version
      0x01, // id = 1
      0x01, // timestamp = 1
      0x03, // appName length = 3
      0x61,
      0x70,
      0x70, // "app"
      0x01, // entry count = 1
      0x01, // key length = 1
      0x61, // "a"
      0x01, // count = 1
    };
    assertThat(ReportMessageCodec.encode(message)).isEqualTo(expected);
    assertThat(ReportMessageCodec.decode(expected)).isEqualTo(message);
  }

  @Test
  void encode_goldenBytes_unicodeAndMultiByteVarints() {
    // Same batch as sample(), laid out field by field from the documented
    // format rather than from the codec's own writer.
    byte[] expected = concat(
      new byte[] { ReportMessageCodec.MAGIC, ReportMessageCodec.VERSION },
      varint(123456789012345L),
      varint(1_758_000_000_000L),
      lengthPrefixed("order-service"),
      varint(5L),
      entry("user:1001", 1L),
      entry("user:1002", 127L),
      entry("user:1003", 128L),
      entry("订单:2026:锁定", 300L),
      entry("product:sku:000000000042", 1L << 40)
    );
    assertThat(ReportMessageCodec.encode(sample())).isEqualTo(expected);
    assertThat(ReportMessageCodec.decode(expected)).isEqualTo(sample());
  }

  @Test
  void decode_exhaustiveTruncation_rejected() {
    byte[] body = ReportMessageCodec.encode(sample());
    for (int cut = 0; cut < body.length; cut++) {
      byte[] truncated = new byte[cut];
      System.arraycopy(body, 0, truncated, 0, cut);
      assertThatThrownBy(() -> ReportMessageCodec.decode(truncated))
        .as("truncation to %s of %s bytes must be detected", cut, body.length)
        .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  void decode_trailingBytes_rejected() {
    byte[] body = ReportMessageCodec.encode(sample());
    byte[] padded = concat(body, new byte[] { 0x00 });
    assertThatThrownBy(() -> ReportMessageCodec.decode(padded))
      .as("a body the receiver decodes incompletely signals a layout disagreement")
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("trailing bytes");
  }

  @Test
  void decode_varintOverflowingItsTenthByte_rejected() {
    // 0x80 x9 then 0x7F: nine continuation bytes put the last byte at shift 63,
    // where only one bit still fits. Java shifts are modulo 64, so before this
    // was guarded the body decoded to Long.MIN_VALUE without any error.
    byte[] id = new byte[10];
    Arrays.fill(id, 0, 9, (byte) 0x80);
    id[9] = 0x7F;
    assertThatThrownBy(() -> ReportMessageCodec.decode(bodyWithRawId(id)))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("exceeds 64 bits");
  }

  @Test
  void decode_tenByteVarintForLongMinValue_accepted() {
    // The canonical encoding of Long.MIN_VALUE also uses ten bytes, but its
    // last byte carries the single bit that fits — it must still round-trip.
    byte[] id = new byte[10];
    Arrays.fill(id, 0, 9, (byte) 0x80);
    id[9] = 0x01;
    assertThat(ReportMessageCodec.decode(bodyWithRawId(id)).id()).isEqualTo(Long.MIN_VALUE);
  }

  @Test
  void decode_malformedUtf8InKey_rejected() {
    // 0xC3 0x28 is an ill-formed sequence: the JDK decoder would substitute
    // U+FFFD, letting a forged body invent a key that was never sent.
    byte[] body = concat(
      new byte[] { ReportMessageCodec.MAGIC, ReportMessageCodec.VERSION },
      varint(1L),
      varint(1L),
      lengthPrefixed("app"),
      varint(1L),
      varint(2L),
      new byte[] { (byte) 0xC3, 0x28 },
      varint(1L)
    );
    assertThatThrownBy(() -> ReportMessageCodec.decode(body))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("not well-formed UTF-8");
  }

  @Test
  void decode_malformedUtf8InAppName_rejected() {
    byte[] body = concat(
      new byte[] { ReportMessageCodec.MAGIC, ReportMessageCodec.VERSION },
      varint(1L),
      varint(1L),
      varint(1L),
      new byte[] { (byte) 0x80 },
      varint(0L)
    );
    assertThatThrownBy(() -> ReportMessageCodec.decode(body))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("not well-formed UTF-8");
  }

  @Test
  void decode_batchLargerThanPresizeHint_roundTrips() {
    // The result map is pre-sized with a capped hint, so it has to grow on its
    // own well past that cap; this pins that the clamp is not a capacity limit.
    Map<String, Long> counts = new LinkedHashMap<>();
    for (int i = 0; i < 5_000; i++) {
      counts.put("cache:key:" + i, (long) i);
    }
    ReportMessage original = new ReportMessage(7L, "app", 9L, counts);
    assertThat(ReportMessageCodec.decode(ReportMessageCodec.encode(original))).isEqualTo(original);
  }

  @Test
  void decode_randomMutations_failOnlyAsMalformedInput() {
    byte[] valid = ReportMessageCodec.encode(sample());
    Random random = new Random(20260919L);
    for (int i = 0; i < 20_000; i++) {
      byte[] mutated = valid.clone();
      for (int flip = 0, flips = 1 + random.nextInt(3); flip < flips; flip++) {
        mutated[random.nextInt(mutated.length)] = (byte) random.nextInt(256);
      }
      try {
        ReportMessage decoded = ReportMessageCodec.decode(mutated);
        // A mutation may still describe a well-formed message; it must then be
        // stable under a re-encode/re-decode cycle.
        assertThat(ReportMessageCodec.decode(ReportMessageCodec.encode(decoded))).isEqualTo(decoded);
      } catch (IllegalArgumentException expected) {
        // The only failure mode a corrupt body may produce.
      }
    }
  }

  @Test
  void isWellFormedUtf8_matchesTheJdkDecoderOnAllOneAndTwoByteInputs() {
    for (int first = 0; first < 256; first++) {
      assertSameUtf8Verdict(new byte[] { (byte) first });
      for (int second = 0; second < 256; second++) {
        assertSameUtf8Verdict(new byte[] { (byte) first, (byte) second });
      }
    }
  }

  @Test
  void isWellFormedUtf8_matchesTheJdkDecoderOnThreeAndFourByteBoundaries() {
    int[] followers = { 0x00, 0x7F, 0x80, 0x8F, 0x90, 0x9F, 0xA0, 0xBF, 0xC0, 0xC2, 0xE0, 0xF0, 0xF4, 0xFF };
    for (int lead = 0xE0; lead <= 0xFF; lead++) {
      for (int f1 : followers) {
        for (int f2 : followers) {
          assertSameUtf8Verdict(new byte[] { (byte) lead, (byte) f1, (byte) f2 });
          for (int f3 : followers) {
            assertSameUtf8Verdict(new byte[] { (byte) lead, (byte) f1, (byte) f2, (byte) f3 });
          }
        }
      }
    }
  }

  @Test
  void isWellFormedUtf8_matchesTheJdkDecoderOnRandomInput() {
    Random random = new Random(20260919L);
    for (int i = 0; i < 50_000; i++) {
      byte[] bytes = new byte[random.nextInt(9)];
      random.nextBytes(bytes);
      assertSameUtf8Verdict(bytes);
    }
    for (int i = 0; i < 20_000; i++) {
      StringBuilder text = new StringBuilder();
      for (int c = 0, codePoints = random.nextInt(6); c < codePoints; c++) {
        int codePoint;
        do {
          codePoint = random.nextInt(0x110000);
        } while (codePoint >= 0xD800 && codePoint <= 0xDFFF);
        text.appendCodePoint(codePoint);
      }
      assertSameUtf8Verdict(text.toString().getBytes(StandardCharsets.UTF_8));
    }
  }

  @Test
  void isWellFormedUtf8_respectsOffsetAndLength() {
    byte[] padded = concat(
      new byte[] { (byte) 0xFF, (byte) 0xFF },
      "héllo€".getBytes(StandardCharsets.UTF_8),
      new byte[] { (byte) 0xC3 }
    );
    assertThat(ReportMessageCodec.isWellFormedUtf8(padded, 2, 9)).isTrue();
    assertThat(ReportMessageCodec.isWellFormedUtf8(padded, 0, padded.length)).as("leading 0xFF").isFalse();
    assertThat(ReportMessageCodec.isWellFormedUtf8(padded, 2, 10)).as("truncated trailing 0xC3").isFalse();
    assertThat(ReportMessageCodec.isWellFormedUtf8(padded, 2, 0)).isTrue();
  }

  @Test
  void utf8Length_matchesTheJdkEncoderForEveryBmpCharAndSurrogatePairing() {
    for (int c = 0; c < 0x10000; c++) {
      String single = String.valueOf((char) c);
      assertThat(ReportMessageCodec.utf8Length(single)).as("utf8Length of U+%04X", c).isEqualTo(utf8Bytes(single));
    }
    for (int high = 0xD800; high < 0xDC00; high += 37) {
      for (int low = 0xDC00; low < 0xE000; low += 53) {
        String pair = new String(new char[] { (char) high, (char) low });
        assertThat(ReportMessageCodec.utf8Length(pair)).as("paired %04X %04X", high, low).isEqualTo(utf8Bytes(pair));
        String unpaired = new String(new char[] { (char) high, (char) low, (char) high });
        assertThat(ReportMessageCodec.utf8Length(unpaired))
          .as("trailing lone high surrogate")
          .isEqualTo(utf8Bytes(unpaired));
      }
    }
  }

  @Test
  void utf8Length_matchesTheJdkEncoderOnRandomStrings() {
    Random random = new Random(20260919L);
    for (int i = 0; i < 50_000; i++) {
      StringBuilder text = new StringBuilder();
      for (int c = 0, chars = random.nextInt(8); c < chars; c++) {
        text.append((char) random.nextInt(0x10000)); // includes lone surrogates
      }
      String value = text.toString();
      assertThat(ReportMessageCodec.utf8Length(value))
        .as("utf8Length of %s", charCodes(value))
        .isEqualTo(utf8Bytes(value));
    }
    for (int i = 0; i < 20_000; i++) {
      StringBuilder text = new StringBuilder();
      for (int c = 0, codePoints = random.nextInt(6); c < codePoints; c++) {
        int codePoint;
        do {
          codePoint = random.nextInt(0x110000);
        } while (codePoint >= 0xD800 && codePoint <= 0xDFFF);
        text.appendCodePoint(codePoint);
      }
      assertThat(ReportMessageCodec.utf8Length(text.toString())).isEqualTo(utf8Bytes(text.toString()));
    }
  }

  @Test
  void encode_unpairedSurrogate_doesNotCorruptTheBody() {
    Map<String, Long> counts = new LinkedHashMap<>();
    counts.put("k\uD800", 1L);
    counts.put("ok", 2L);
    ReportMessage original = new ReportMessage(1L, "app\uD83D\uDE00", 1L, counts);
    // The JDK encoder replaces the unpaired surrogate with '?', so the key is
    // not preserved — but the body must still be exactly sized and parseable,
    // which encode's own pass-agreement check enforces. A utf8Length that
    // counted 3 bytes for a lone surrogate would fail here instead of shipping
    // a body with a hole in it.
    ReportMessage decoded = ReportMessageCodec.decode(ReportMessageCodec.encode(original));
    assertThat(decoded.counts()).containsEntry("k?", 1L).containsEntry("ok", 2L);
    assertThat(decoded.appName()).as("a valid surrogate pair survives").isEqualTo("app\uD83D\uDE00");
  }

  /** Length in bytes of the unsigned-varint encoding of {@code value}. */
  private static int varintLength(long value) {
    int length = 1;
    while ((value & ~0x7FL) != 0) {
      value >>>= 7;
      length++;
    }
    return length;
  }

  /** A faithful JSON rendering of the same payload (what Jackson emits). */
  private static String manualJson(ReportMessage message) {
    StringBuilder sb = new StringBuilder(64 + message.counts().size() * 24);
    sb
      .append("{\"id\":")
      .append(message.id())
      .append(",\"appName\":\"")
      .append(message.appName())
      .append("\",\"timestamp\":")
      .append(message.timestamp())
      .append(",\"counts\":{");
    boolean first = true;
    for (Map.Entry<String, Long> entry : message.counts().entrySet()) {
      if (!first) {
        sb.append(',');
      }
      first = false;
      sb.append('"').append(entry.getKey()).append("\":").append(entry.getValue());
    }
    return sb.append("}}").toString();
  }

  /** The minimal (non-padded) unsigned-varint encoding of {@code value}, written from the format spec. */
  private static byte[] varint(long value) {
    ByteArrayOutputStream out = new ByteArrayOutputStream(10);
    long remaining = value;
    while ((remaining & ~0x7FL) != 0) {
      out.write((byte) ((remaining & 0x7F) | 0x80));
      remaining >>>= 7;
    }
    out.write((byte) remaining);
    return out.toByteArray();
  }

  /** {@code varint(utf8Length) + utf8Bytes}, the format's length-prefix framing. */
  private static byte[] lengthPrefixed(String value) {
    byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
    return concat(varint(utf8.length), utf8);
  }

  /** {@code lengthPrefixed(key) + varint(count)}, the per-entry layout. */
  private static byte[] entry(String key, long count) {
    return concat(lengthPrefixed(key), varint(count));
  }

  private static byte[] concat(byte[]... parts) {
    int total = 0;
    for (byte[] part : parts) {
      total += part.length;
    }
    byte[] joined = new byte[total];
    int offset = 0;
    for (byte[] part : parts) {
      System.arraycopy(part, 0, joined, offset, part.length);
      offset += part.length;
    }
    return joined;
  }

  /** A header-only body whose id field is the given raw varint, so malformed encodings can be forged. */
  private static byte[] bodyWithRawId(byte[] rawId) {
    return concat(
      new byte[] { ReportMessageCodec.MAGIC, ReportMessageCodec.VERSION },
      rawId,
      varint(0L),
      varint(0L),
      varint(0L)
    );
  }

  private static void assertSameUtf8Verdict(byte[] bytes) {
    assertThat(ReportMessageCodec.isWellFormedUtf8(bytes, 0, bytes.length))
      .as("UTF-8 verdict for %s differs from the JDK decoder", hex(bytes))
      .isEqualTo(jdkAcceptsUtf8(bytes));
  }

  /** The reference verdict: the JDK decoder in REPORT mode, which rejects instead of substituting U+FFFD. */
  private static boolean jdkAcceptsUtf8(byte[] bytes) {
    CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT);
    try {
      decoder.decode(ByteBuffer.wrap(bytes));
      return true;
    } catch (CharacterCodingException e) {
      return false;
    }
  }

  private static String hex(byte[] bytes) {
    StringBuilder sb = new StringBuilder(bytes.length * 3);
    for (byte b : bytes) {
      sb.append(String.format("%02X ", b));
    }
    return sb.toString().trim();
  }

  private static int utf8Bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8).length;
  }

  private static String charCodes(String value) {
    StringBuilder sb = new StringBuilder(value.length() * 5);
    for (int i = 0; i < value.length(); i++) {
      sb.append(String.format("U+%04X ", (int) value.charAt(i)));
    }
    return sb.toString().trim();
  }
}
