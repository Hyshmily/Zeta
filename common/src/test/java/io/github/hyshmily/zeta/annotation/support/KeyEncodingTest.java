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
package io.github.hyshmily.zeta.annotation.support;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.policy.KeyFormat;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("KeyEncoding")
class KeyEncodingTest {

  private static final UUID UUID_VALUE = UUID.fromString("6c84fb90-12c4-11e1-840d-7b25c5ee775a");

  @Test
  @DisplayName("every supported scalar round-trips through its default-key part")
  void supportedScalarsRoundTrip() {
    assertRoundTrip(String.class, "value");
    assertRoundTrip(String.class, "");
    assertRoundTrip(String.class, "a,b]c:d");
    assertRoundTrip(String.class, "😀");
    assertRoundTrip(Integer.class, 42);
    assertRoundTrip(int.class, 42);
    assertRoundTrip(Long.class, Long.MIN_VALUE);
    assertRoundTrip(long.class, Long.MAX_VALUE);
    assertRoundTrip(Short.class, (short) -7);
    assertRoundTrip(short.class, (short) 7);
    assertRoundTrip(Byte.class, (byte) -1);
    assertRoundTrip(byte.class, (byte) 1);
    assertRoundTrip(Boolean.class, true);
    assertRoundTrip(boolean.class, false);
    assertRoundTrip(Character.class, 'x');
    assertRoundTrip(char.class, ',');
    assertRoundTrip(char.class, ']');
    assertRoundTrip(BigInteger.class, new BigInteger("123456789012345678901234567890"));
    assertRoundTrip(BigDecimal.class, new BigDecimal("-1234567890.0123456789"));
    assertRoundTrip(UUID.class, UUID_VALUE);
  }

  @Test
  @DisplayName("a multi-argument key decodes every argument in order")
  void multipleArgumentsRoundTrip() {
    KeyEncoding.Decoder decoder = typed(String.class, int.class, UUID.class);
    Object[] args = { "user, [42]:x", 7, UUID_VALUE };

    assertThat(decoder.decode(DefaultKey.encode("cache\u0000", args))).containsExactly(args);
  }

  @Test
  @DisplayName("an empty argument list decodes only for a method with no parameters")
  void emptyArgumentList() {
    KeyEncoding.Decoder noArguments = typed();
    KeyEncoding.Decoder oneArgument = typed(String.class);

    assertThat(noArguments.decode("cache\u0000[]")).isEmpty();
    assertThat(new KeyEncoding("", KeyFormat.TYPED).argumentsDecoder(new Class<?>[0]).decode("\u0000[]")).isEmpty();
    assertThat(oneArgument.decode("cache\u0000[]")).isNull();
    assertThat(noArguments.decode("cache\u0000[s1:a]")).isNull();
  }

  @Test
  @DisplayName("of accepts exactly the supported scalar set")
  void ofAcceptsSupportedScalars() {
    assertThat(KeyEncoding.ScalarType.of(null)).isNull();
    for (Class<?> type : new Class<?>[] {
      String.class,
      Integer.class,
      int.class,
      Long.class,
      long.class,
      Short.class,
      short.class,
      Byte.class,
      byte.class,
      Boolean.class,
      boolean.class,
      Character.class,
      char.class,
      BigInteger.class,
      BigDecimal.class,
      UUID.class,
    }) {
      assertThat(KeyEncoding.ScalarType.of(type)).as(type.getName()).isNotNull();
    }
  }

  @Test
  @DisplayName("of rejects floating point, arrays, collections and objects")
  void ofRejectsUnsupportedTypes() {
    for (Class<?> type : new Class<?>[] {
      float.class,
      Float.class,
      double.class,
      Double.class,
      String[].class,
      int[].class,
      List.class,
      Set.class,
      Object.class,
      Dto.class,
    }) {
      assertThat(KeyEncoding.ScalarType.of(type)).as(type.getName()).isNull();
    }
  }

  @Test
  @DisplayName("a different or missing prefix returns null")
  void prefixMismatchReturnsNull() {
    KeyEncoding.Decoder decoder = typed(int.class);

    assertThat(decoder.decode("other\u0000[I:1]")).isNull();
    assertThat(decoder.decode("[I:1]")).isNull();
    assertThat(decoder.decode("")).isNull();
    assertThat(decoder.decode("cache")).isNull();
    assertThat(decoder.decode("cache\u0000")).isNull();
  }

  @Test
  @DisplayName("a tag that does not match the declared parameter type returns null")
  void wrongTagReturnsNull() {
    assertThat(typed(Long.class).decode("cache\u0000[I:1]")).isNull();
    assertThat(typed(long.class).decode("cache\u0000[S:1]")).isNull();
    assertThat(typed(Integer.class).decode("cache\u0000[J:1]")).isNull();
    assertThat(typed(String.class).decode("cache\u0000[I:1]")).isNull();
    assertThat(typed(String.class).decode("cache\u0000[U:" + UUID_VALUE + "]")).isNull();
    assertThat(typed(UUID.class).decode("cache\u0000[s36:" + UUID_VALUE + "]")).isNull();
    assertThat(typed(Character.class).decode("cache\u0000[s1:a]")).isNull();
  }

  @Test
  @DisplayName("malformed keys return null instead of throwing")
  void malformedKeysReturnNull() {
    KeyEncoding.Decoder decoder = typed(int.class, String.class, UUID.class);
    String uuid = UUID_VALUE.toString();

    assertThat(decoder.decode("cache\u0000[I:1,s1:a]")).isNull();
    assertThat(decoder.decode("cache\u0000[I:1,s1:a,U:" + uuid + ",I:2]")).isNull();
    assertThat(decoder.decode("cache\u0000[I:1,s1:a,U:" + uuid)).isNull();
    assertThat(decoder.decode("cache\u0000[I:1,s1:a,U:" + uuid + "]x")).isNull();
    assertThat(decoder.decode("cache\u0000[I:1,s1:a,U:" + uuid + ",]")).isNull();
    assertThat(decoder.decode("cache\u0000[I:,s1:a,U:" + uuid + "]")).isNull();
    assertThat(decoder.decode("cache\u0000[I:not-a-number,s1:a,U:" + uuid + "]")).isNull();
    assertThat(decoder.decode("cache\u0000[I:1,s5:abc,U:" + uuid + "]")).isNull();
    assertThat(decoder.decode("cache\u0000[I:1,s1:ab,U:" + uuid + "]")).isNull();
    assertThat(decoder.decode("cache\u0000[I:1,s1:a,U:not-a-uuid]")).isNull();
    assertThat(decoder.decode("cache\u0000[I:1,Z:true,U:" + uuid + "]")).isNull();
    assertThat(decoder.decode("cache\u0000[I:1;s1:a;U:" + uuid + "]")).isNull();
    assertThat(decoder.decode("cache\u0000[I:1, s1:a,U:" + uuid + "]")).isNull();
    assertThat(typed(Boolean.class).decode("cache\u0000[Z:TRUE]")).isNull();
    assertThat(typed(Byte.class).decode("cache\u0000[B:300]")).isNull();
  }

  @Test
  @DisplayName("a PLAIN key decodes the text after the delimiter and rejects an unparsable value")
  void plainKey() {
    KeyEncoding users = new KeyEncoding("users", KeyFormat.PLAIN);
    KeyEncoding.Decoder decoder = users.singleDecoder(Long.class);

    assertThat(decoder.decode("users:42")).containsExactly(42L);
    assertThat(decoder.decode("users:not-a-long")).isNull();
    assertThat(decoder.decode("other:42")).isNull();
  }

  /** A user-defined value a default key can only render through its {@code toString()}. */
  private record Dto(int id) {}

  private static KeyEncoding.Decoder typed(Class<?>... parameterTypes) {
    return new KeyEncoding("cache", KeyFormat.TYPED).argumentsDecoder(parameterTypes);
  }

  private static void assertRoundTrip(Class<?> parameterType, Object value) {
    assertThat(typed(parameterType).decode(DefaultKey.encode("cache\u0000", new Object[] { value }))).containsExactly(
      value
    );
  }
}
