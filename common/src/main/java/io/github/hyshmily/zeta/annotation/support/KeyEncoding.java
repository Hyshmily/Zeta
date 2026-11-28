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

import io.github.hyshmily.zeta.policy.KeyFormat;
import io.github.hyshmily.zeta.util.CollectionSizing;
import io.github.hyshmily.zeta.util.Internal;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;

/**
 * The key codec of one namespace: the physical prefix, the encoder used on the request path, and
 * the decoder used by background warming.
 *
 * <p>A {@link KeyFormat#TYPED} key delegates to {@link DefaultKey}; a {@link KeyFormat#PLAIN} key
 * appends the single scalar text directly after the delimiter without type locking, so two
 * declarations that produce the same text share one entry by design.
 *
 * <p>{@link ScalarType} defines the closed set of types a default key can round-trip. Any type
 * whose text representation is unambiguous within a part is reversible: {@code String}, primitive
 * and boxed integral types, {@code Boolean}, {@code Character}, {@code BigInteger},
 * {@code BigDecimal}, and {@code UUID}. Floating-point keys are excluded because {@code -0.0} and
 * {@code 0.0} share an encoding and {@code NaN} has no useful cache identity.
 */
@Internal
public final class KeyEncoding {

  private static final Object[] EMPTY_ARGS = new Object[0];

  private final String namespace;
  private final KeyFormat format;
  private final String physicalPrefix;

  /**
   * Creates a key codec for the given namespace and key format.
   *
   * @param namespace the logical cache namespace
   * @param format    the resolved key format
   */
  KeyEncoding(String namespace, KeyFormat format) {
    this.namespace = Objects.requireNonNull(namespace, "namespace must not be null");
    this.format = Objects.requireNonNull(format, "format must not be null");
    this.physicalPrefix = namespace + format.delimiter();
  }

  /**
   * Returns the logical cache namespace.
   *
   * @return the namespace name
   */
  public String namespace() {
    return namespace;
  }

  /**
   * Returns the resolved key format.
   *
   * @return the key format
   */
  public KeyFormat format() {
    return format;
  }

  /**
   * Returns the physical key prefix, including the format delimiter.
   *
   * @return the physical prefix
   */
  public String physicalPrefix() {
    return physicalPrefix;
  }

  /**
   * Encodes one explicitly selected key value under this namespace's physical prefix.
   *
   * @param value the non-null key value
   * @return the physical cache key
   */
  public String encodeSingle(Object value) {
    Objects.requireNonNull(value, "key value must not be null");
    if (format == KeyFormat.TYPED) {
      return DefaultKey.encodeSingle(physicalPrefix, value);
    }
    return value instanceof String string ? physicalPrefix.concat(string) : physicalPrefix + value;
  }

  /**
   * Encodes an invocation's full argument list as a default key under this namespace's prefix.
   *
   * @param args the non-null invocation arguments
   * @return the physical cache key
   * @throws IllegalStateException if this namespace uses {@link KeyFormat#PLAIN}
   */
  public String encodeArgs(Object[] args) {
    Objects.requireNonNull(args, "args must not be null");
    if (format == KeyFormat.PLAIN) {
      throw new IllegalStateException(
        "PLAIN namespace '" + namespace + "' cannot use a default key; declare one explicit key or use TYPED"
      );
    }
    return DefaultKey.encode(physicalPrefix, args);
  }

  /**
   * Returns a decoder for a constant-key, zero-argument method: the key must equal the folded
   * physical key exactly.
   *
   * @param physicalKey the single physical key the method can load
   * @return a decoder that yields an empty argument array on an exact match
   */
  public Decoder constantDecoder(String physicalKey) {
    Objects.requireNonNull(physicalKey, "physicalKey must not be null");
    return key -> physicalKey.equals(key) ? EMPTY_ARGS : null;
  }

  /**
   * Returns a decoder for a single-argument method.
   *
   * @param type the declared parameter type
   * @return a decoder that parses the physical key back into a one-element argument array
   * @throws IllegalStateException if {@code type} is not a reversible {@link ScalarType}
   */
  public Decoder singleDecoder(Class<?> type) {
    ScalarType scalar = ScalarType.require(type);
    if (format == KeyFormat.PLAIN) {
      return physicalKey -> decodePlainSingle(physicalKey, scalar);
    }
    return new TypedDecoder(physicalPrefix, new ScalarType[] { scalar });
  }

  /**
   * Returns a decoder for a multi-argument default-key method in declared parameter order.
   *
   * @param parameterTypes the method's declared parameter types
   * @return a decoder that reconstructs the invocation argument array
   * @throws IllegalStateException if this namespace is {@link KeyFormat#PLAIN} or any parameter
   *                               type is not a reversible {@link ScalarType}
   */
  public Decoder argumentsDecoder(Class<?>[] parameterTypes) {
    Objects.requireNonNull(parameterTypes, "parameterTypes must not be null");
    if (format == KeyFormat.PLAIN) {
      throw new IllegalStateException("PLAIN namespace '" + namespace + "' cannot decode a multi-argument key");
    }
    if (parameterTypes.length == 0) {
      return constantDecoder(physicalPrefix + "[]");
    }
    ScalarType[] scalars = new ScalarType[parameterTypes.length];
    for (int i = 0; i < parameterTypes.length; i++) {
      scalars[i] = ScalarType.require(parameterTypes[i]);
    }
    return new TypedDecoder(physicalPrefix, scalars);
  }

  /**
   * Decodes a {@link KeyFormat#PLAIN} single-value physical key using the expected scalar type.
   *
   * @param physicalKey the physical key to decode
   * @param scalar      the expected scalar type
   * @return a one-element argument array, or {@code null} if the prefix or value does not match
   */
  private @Nullable Object[] decodePlainSingle(String physicalKey, ScalarType scalar) {
    if (!physicalKey.startsWith(physicalPrefix)) {
      return null;
    }
    Object value = scalar.parse(physicalKey.substring(physicalPrefix.length()));
    return value == null ? null : new Object[] { value };
  }

  /**
   * Decodes a physical cache key back into the argument array of the method that produced it.
   */
  @FunctionalInterface
  public interface Decoder {
    /**
     * Decodes a physical cache key.
     *
     * @param physicalKey the physical key to decode
     * @return the invocation arguments, or {@code null} when the key does not belong to this method
     */
    @Nullable
    Object[] decode(String physicalKey);
  }

  /**
   * The closed set of scalar types that a default key can encode and reversibly decode.
   */
  public enum ScalarType {
    STRING('s', String.class, null, token -> token),
    CHAR('c', Character.class, char.class, token -> token.length() == 1 ? token.charAt(0) : null),
    BOOLEAN('Z', Boolean.class, boolean.class, token ->
      "true".equals(token) ? Boolean.TRUE : "false".equals(token) ? Boolean.FALSE : null
    ),
    INT('I', Integer.class, int.class, Integer::valueOf),
    LONG('J', Long.class, long.class, Long::valueOf),
    SHORT('S', Short.class, short.class, Short::valueOf),
    BYTE('B', Byte.class, byte.class, Byte::valueOf),
    BIG_INTEGER('N', BigInteger.class, null, BigInteger::new),
    BIG_DECIMAL('E', BigDecimal.class, null, BigDecimal::new),
    UUID('U', java.util.UUID.class, null, java.util.UUID::fromString);

    private static final Map<Class<?>, ScalarType> BY_TYPE;

    static {
      Map<Class<?>, ScalarType> map = new HashMap<>(CollectionSizing.capacity(values().length));
      for (ScalarType type : values()) {
        map.put(type.boxedType, type);
        if (type.primitiveType != null) {
          map.put(type.primitiveType, type);
        }
      }
      BY_TYPE = Map.copyOf(map);
    }

    private final char tag;
    private final Class<?> boxedType;
    private final @Nullable Class<?> primitiveType;
    private final Function<String, @Nullable Object> parser;

    /**
     * Creates a scalar type descriptor.
     *
     * @param tag           the single-character part prefix used by {@link DefaultKey}
     * @param boxedType     the wrapper or reference class representing values at runtime
     * @param primitiveType the corresponding primitive class, or {@code null} if none exists
     * @param parser        the parser converting raw token text to a value of {@code boxedType}
     */
    ScalarType(
      char tag,
      Class<?> boxedType,
      @Nullable Class<?> primitiveType,
      Function<String, @Nullable Object> parser
    ) {
      this.tag = tag;
      this.boxedType = boxedType;
      this.primitiveType = primitiveType;
      this.parser = parser;
    }

    /**
     * Returns the single-character part prefix used by {@link DefaultKey}.
     *
     * @return the type tag character
     */
    public char tag() {
      return tag;
    }

    /**
     * Parses one part's raw text into a value of this scalar type.
     *
     * @param token the text following the type tag and separator
     * @return the parsed value, or {@code null} if {@code token} is not a valid value of this type
     */
    public @Nullable Object parse(String token) {
      try {
        return parser.apply(token);
      } catch (IllegalArgumentException invalid) {
        return null;
      }
    }

    /**
     * Resolves the {@link ScalarType} for a declared Java type.
     *
     * @param type the declared parameter or key type, or {@code null}
     * @return the matching scalar type, or {@code null} if the type is not reversible
     */
    public static @Nullable ScalarType of(@Nullable Class<?> type) {
      return type == null ? null : BY_TYPE.get(type);
    }

    /**
     * Resolves a reversible {@link ScalarType} for the given Java type or fails fast.
     *
     * @param type the declared parameter or key type
     * @return the matching scalar type
     * @throws IllegalStateException if {@code type} is not a reversible scalar type
     */
    public static ScalarType require(Class<?> type) {
      Objects.requireNonNull(type, "type must not be null");
      ScalarType scalar = of(type);
      if (scalar == null) {
        throw new IllegalStateException("unsupported reversible scalar key type: " + type.getName());
      }
      return scalar;
    }
  }

  /**
   * Inverse of {@link DefaultKey}'s self-delimiting argument-list encoding for reversible scalar
   * parameters.
   */
  private static final class TypedDecoder implements Decoder {

    private final String openingPrefix;
    private final ScalarType[] types;

    /**
     * Creates a typed decoder for the given physical prefix and expected scalar parameter types.
     *
     * @param prefix the namespace physical prefix
     * @param types  the expected scalar types in parameter order
     */
    private TypedDecoder(String prefix, ScalarType[] types) {
      this.openingPrefix = prefix + '[';
      this.types = types;
    }

    /**
     * Decodes a {@link KeyFormat#TYPED} physical key into an argument array matching {@link #types}.
     *
     * @param physicalKey the physical key to decode
     * @return the decoded argument array, or {@code null} if the key does not match
     */
    @Override
    public @Nullable Object[] decode(String physicalKey) {
      if (!physicalKey.startsWith(openingPrefix)) {
        return null;
      }

      int cursor = openingPrefix.length();
      int length = physicalKey.length();
      Object[] values = new Object[types.length];

      for (int i = 0; i < types.length; i++) {
        if (i > 0) {
          if (cursor >= length || physicalKey.charAt(cursor) != ',') {
            return null;
          }
          cursor++;
        }
        ScalarType type = types[i];
        if (cursor >= length || physicalKey.charAt(cursor) != type.tag()) {
          return null;
        }
        int next = readPart(physicalKey, cursor, type, values, i);
        if (next < 0) {
          return null;
        }
        cursor = next;
      }

      return cursor + 1 == length && physicalKey.charAt(cursor) == ']' ? values : null;
    }

    /**
     * Reads one encoded part starting at its type tag and stores the decoded value into
     * {@code values[index]}.
     *
     * @param key    the full physical key
     * @param cursor the index of the part's type tag
     * @param type   the expected scalar type
     * @param values the destination argument array
     * @param index  the slot index to populate
     * @return the cursor immediately after the part, or {@code -1} if decoding fails
     */
    private static int readPart(String key, int cursor, ScalarType type, Object[] values, int index) {
      if (type == ScalarType.STRING) {
        return readString(key, cursor, values, index);
      }
      if (type == ScalarType.CHAR) {
        return readChar(key, cursor, values, index);
      }
      return readScalar(key, cursor, type, values, index);
    }

    /**
     * Reads a length-prefixed string part ({@code s<length>:<text>}).
     *
     * @param key    the full physical key
     * @param cursor the index of the {@code 's'} tag
     * @param values the destination argument array
     * @param index  the slot index to populate
     * @return the cursor immediately after the string text, or {@code -1} if malformed
     */
    private static int readString(String key, int cursor, Object[] values, int index) {
      int colon = key.indexOf(':', cursor + 1);
      if (colon < 0) {
        return -1;
      }
      int length = parseLength(key, cursor + 1, colon);
      int valueStart = colon + 1;
      if (length < 0 || length > key.length() - valueStart) {
        return -1;
      }
      int valueEnd = valueStart + length;
      values[index] = key.substring(valueStart, valueEnd);
      return valueEnd;
    }

    /**
     * Reads a character part ({@code c1:<char>}).
     *
     * @param key    the full physical key
     * @param cursor the index of the {@code 'c'} tag
     * @param values the destination argument array
     * @param index  the slot index to populate
     * @return the cursor immediately after the character value, or {@code -1} if malformed
     */
    private static int readChar(String key, int cursor, Object[] values, int index) {
      if (!key.startsWith("c1:", cursor) || cursor + 4 >= key.length()) {
        return -1;
      }
      values[index] = key.charAt(cursor + 3);
      return cursor + 4;
    }

    /**
     * Reads a tag-colon-delimited scalar part ({@code <tag>:<token>}).
     *
     * @param key    the full physical key
     * @param cursor the index of the type tag
     * @param type   the expected scalar type
     * @param values the destination argument array
     * @param index  the slot index to populate
     * @return the cursor at the following delimiter ({@code ','} or {@code ']'}), or {@code -1}
     *         if malformed or unparseable
     */
    private static int readScalar(String key, int cursor, ScalarType type, Object[] values, int index) {
      if (cursor + 1 >= key.length() || key.charAt(cursor + 1) != ':') {
        return -1;
      }
      int valueStart = cursor + 2;
      int delimiter = findDelimiter(key, valueStart);
      if (delimiter < 0) {
        return -1;
      }
      Object parsed = type.parse(key.substring(valueStart, delimiter));
      if (parsed == null) {
        return -1;
      }
      values[index] = parsed;
      return delimiter;
    }

    /**
     * Parses a canonical non-negative decimal length from {@code key[start..end)}.
     *
     * @param key   the source key string
     * @param start the inclusive start index of the digits
     * @param end   the exclusive end index of the digits
     * @return the non-negative length, or {@code -1} if empty, non-numeric, has leading zeros,
     *         or overflows {@code int}
     */
    private static int parseLength(String key, int start, int end) {
      return CanonicalInt.parse(key, start, end);
    }

    /**
     * Finds the index of the next part delimiter ({@code ','} or {@code ']'}) starting at
     * {@code fromIndex}.
     *
     * @param key       the full physical key
     * @param fromIndex the index at which to start searching
     * @return the delimiter index, or {@code -1} if none is found
     */
    private static int findDelimiter(String key, int fromIndex) {
      for (int i = fromIndex; i < key.length(); i++) {
        char ch = key.charAt(i);
        if (ch == ',' || ch == ']') {
          return i;
        }
      }
      return -1;
    }
  }
}
