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

import io.github.hyshmily.zeta.util.Internal;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Canonical string encoding of a default cache key used when an annotated method declares no
 * explicit {@code key} expression.
 *
 * <p><b>Format.</b>
 *
 * <pre>
 * key                ::= "[]" | "[" part ("," part)* "]"
 * part(null)         ::= "n"
 * part(String)       ::= "s" length ":" text
 * part(Character)    ::= "c1:" char
 * part(CharSequence) ::= "q" length ":" text
 * part(Byte)         ::= "B:" text
 * part(Short)        ::= "S:" text
 * part(Integer)      ::= "I:" text
 * part(Long)         ::= "J:" text
 * part(Float)        ::= "F:" text
 * part(Double)       ::= "D:" text
 * part(BigInteger)   ::= "N:" text
 * part(BigDecimal)   ::= "E:" text
 * part(UUID)         ::= "U:" text
 * part(Boolean)      ::= "Z:" text
 * part(Iterable)     ::= "l[" (part ("," part)*)? "]"
 * part(Set)          ::= "t[" (sorted-part ("," sorted-part)*)? "]"
 * part(array)        ::= "a" component "[" (part ("," part)*)? "]"
 * part(other)        ::= "o" length ":" text      (the value's String.valueOf)
 * </pre>
 *
 * <p>{@code length} is the UTF-16 code-unit count of {@code text}; {@code component} is {@code O}
 * for a reference array and the JVM primitive letter ({@code B S I J F D C Z}) for a primitive
 * array. Every part is self-delimiting so no delimiter inside a value can shift an argument
 * boundary, and type tags keep {@code Integer 1}, {@code Long 1}, and {@code String "1"} distinct.
 * A {@link Set} is canonicalized by sorting its encoded elements, whereas a {@link List} or any
 * other {@link Iterable} preserves iteration order.
 *
 * <p><b>Limits.</b> A container is rejected above {@value #MAX_ELEMENTS} total elements or
 * {@value #MAX_DEPTH} nesting levels so malformed or unbounded arguments fail fast instead of
 * allocating multi-megabyte keys. A self-referential container is rejected on the step that
 * re-enters it. Non-array, non-collection objects are rendered via
 * {@link String#valueOf(Object)}; declare an explicit key when two such values must not collide.
 */
@Internal
public final class DefaultKey {

  private static final int MAX_DEPTH = 16;
  private static final int MAX_ELEMENTS = 1024;
  private static final int SINGLE_BUFFER_SLACK = 24;

  /** Prevents instantiation of this utility class. */
  private DefaultKey() {
    throw new AssertionError("No DefaultKey instances");
  }

  /**
   * Encodes one invocation's arguments without a key prefix.
   *
   * @param args the invocation arguments; must not be {@code null} and must not contain
   *             {@code null} top-level elements
   * @return the canonical key string
   * @throws IllegalStateException if container depth exceeds {@value #MAX_DEPTH} or total elements
   *                               exceed {@value #MAX_ELEMENTS}
   */
  public static String encode(Object[] args) {
    return encode("", args);
  }

  /**
   * Encodes one invocation's arguments under an already resolved key prefix.
   *
   * @param prefix the physical namespace prefix ({@code cache + delimiter}); must not be {@code null}
   * @param args   the invocation arguments; must not be {@code null} and must not contain
   *               {@code null} top-level elements
   * @return the physical key string
   * @throws IllegalStateException if container depth exceeds {@value #MAX_DEPTH} or total elements
   *                               exceed {@value #MAX_ELEMENTS}
   */
  public static String encode(String prefix, Object[] args) {
    Objects.requireNonNull(prefix, "prefix must not be null");
    Objects.requireNonNull(args, "args must not be null");

    if (args.length == 0) {
      return prefix + "[]";
    }
    if (args.length == 1) {
      return encodeSingle(prefix, args[0]);
    }

    StringBuilder key = new StringBuilder(estimateMultiCapacity(prefix, args)).append(prefix).append('[');
    Encoder encoder = new Encoder();
    for (int i = 0; i < args.length; i++) {
      if (i > 0) {
        key.append(',');
      }
      encoder.appendPart(key, args[i], 0);
    }
    return key.append(']').toString();
  }

  /**
   * Estimates the buffer size for a multi-argument key: the prefix, the delimiters, and one
   * allowance per argument, with a text argument contributing its full length.
   *
   * <p>The estimate only has to avoid the growth copy on the common hit path; an under-estimate
   * costs one {@link StringBuilder} expansion, never correctness.
   *
   * @param prefix the physical namespace prefix
   * @param args   the invocation arguments
   * @return the initial buffer capacity
   */
  private static int estimateMultiCapacity(String prefix, Object[] args) {
    int capacity = prefix.length() + 2 + args.length * 8;
    for (Object arg : args) {
      if (arg instanceof CharSequence text) {
        capacity += text.length() + 8;
      } else if (arg instanceof UUID) {
        capacity += 40;
      } else {
        capacity += 24;
      }
    }
    return capacity;
  }

  /**
   * Encodes one explicitly selected value under an already resolved key prefix.
   *
   * @param prefix the physical namespace prefix ({@code cache + delimiter}); must not be {@code null}
   * @param value  the value to encode; never {@code null}
   * @return the physical key string
   * @throws IllegalStateException if container depth exceeds {@value #MAX_DEPTH} or total elements
   *                               exceed {@value #MAX_ELEMENTS}
   */
  public static String encodeSingle(String prefix, Object value) {
    Objects.requireNonNull(prefix, "prefix must not be null");

    if (value instanceof String string) {
      return prefix + "[s" + string.length() + ':' + string + ']';
    }
    if (value instanceof Long longValue) {
      return prefix + "[J:" + longValue + ']';
    }
    if (value instanceof Integer integer) {
      return prefix + "[I:" + integer + ']';
    }
    if (value instanceof UUID uuid) {
      return prefix + "[U:" + uuid + ']';
    }

    StringBuilder key = new StringBuilder(prefix.length() + SINGLE_BUFFER_SLACK).append(prefix).append('[');
    new Encoder().appendPart(key, value, 0);
    return key.append(']').toString();
  }

  /**
   * Appends a length-prefixed text token ({@code <tag><length>:<text>}) to the target buffer.
   *
   * @param target the destination buffer
   * @param tag    the single-character type tag
   * @param text   the character sequence to encode
   */
  private static void appendText(StringBuilder target, char tag, CharSequence text) {
    target.append(tag).append(text.length()).append(':').append(text);
  }

  /**
   * Stateful recursive encoder that enforces container depth and total element budgets for a single
   * key encoding pass.
   */
  private static final class Encoder {

    private int remainingElements = MAX_ELEMENTS;

    /** The depth-1 container, recorded without allocating an ancestor stack. */
    private Object rootContainer;

    /** Containers at depth {@code >= 2}, allocated only when the traversal actually nests. */
    private Object[] ancestors;

    /**
     * Appends the canonical encoding of a single value to {@code target}.
     *
     * @param target the destination buffer
     * @param value  the value to encode, or {@code null}
     * @param depth  the current container nesting depth
     */
    private void appendPart(StringBuilder target, @Nullable Object value, int depth) {
      if (value == null) {
        target.append('n');
      } else if (value instanceof String string) {
        appendText(target, 's', string);
      } else if (value instanceof Integer integer) {
        appendInt(target, integer);
      } else if (value instanceof Long longValue) {
        appendLong(target, longValue);
      } else if (value instanceof Boolean bool) {
        appendBoolean(target, bool);
      } else if (value instanceof Double doubleValue) {
        appendDouble(target, doubleValue);
      } else if (value instanceof Float floatValue) {
        appendFloat(target, floatValue);
      } else if (value instanceof Short shortValue) {
        appendShort(target, shortValue);
      } else if (value instanceof Byte byteValue) {
        appendByte(target, byteValue);
      } else if (value instanceof BigInteger integer) {
        target.append("N:").append(integer);
      } else if (value instanceof BigDecimal decimal) {
        target.append("E:").append(decimal);
      } else if (value instanceof UUID uuid) {
        target.append("U:").append(uuid);
      } else if (value instanceof Character character) {
        appendChar(target, character);
      } else if (value instanceof CharSequence sequence) {
        appendText(target, 'q', sequence);
      } else if (value instanceof Set<?> set) {
        appendSet(target, set, depth + 1);
      } else if (value instanceof Iterable<?> iterable) {
        appendIterable(target, iterable, depth + 1);
      } else if (value.getClass().isArray()) {
        appendArray(target, value, depth + 1);
      } else {
        appendText(target, 'o', String.valueOf(value));
      }
    }

    private static void appendInt(StringBuilder target, int value) {
      target.append("I:").append(value);
    }

    private static void appendLong(StringBuilder target, long value) {
      target.append("J:").append(value);
    }

    private static void appendBoolean(StringBuilder target, boolean value) {
      target.append(value ? "Z:true" : "Z:false");
    }

    private static void appendDouble(StringBuilder target, double value) {
      // Normalize -0.0d to 0.0d so signed zeros encode identically.
      target.append("D:").append(value == 0.0d ? 0.0d : value);
    }

    private static void appendFloat(StringBuilder target, float value) {
      // Normalize -0.0f to 0.0f so signed zeros encode identically.
      target.append("F:").append(value == 0.0f ? 0.0f : value);
    }

    private static void appendShort(StringBuilder target, short value) {
      target.append("S:").append(value);
    }

    private static void appendByte(StringBuilder target, byte value) {
      target.append("B:").append(value);
    }

    private static void appendChar(StringBuilder target, char value) {
      target.append("c1:").append(value);
    }

    /**
     * Appends an order-independent encoding of a {@link Set} by sorting its encoded element parts.
     *
     * <p>The size is checked before the first element is encoded: a set larger than the remaining
     * budget can never fit, whatever its elements encode to.
     *
     * @param target the destination buffer
     * @param set    the set to encode
     * @param depth  the nesting depth of this set
     */
    private void appendSet(StringBuilder target, Set<?> set, int depth) {
      checkDepth(depth);
      pushContainer(set, depth);
      int size = set.size();
      if (size > remainingElements) {
        throw tooManyElements();
      }
      if (size == 0) {
        target.append("t[]");
        return;
      }
      if (size == 1) {
        consumeElements(1);
        target.append("t[");
        appendPart(target, set.iterator().next(), depth);
        target.append(']');
        return;
      }
      List<String> encodedElements = new ArrayList<>(size);
      StringBuilder elementBuffer = new StringBuilder();
      for (Object element : set) {
        consumeElements(1);
        elementBuffer.setLength(0);
        appendPart(elementBuffer, element, depth);
        encodedElements.add(elementBuffer.toString());
      }
      encodedElements.sort(null);

      target.append("t[");
      for (int i = 0; i < encodedElements.size(); i++) {
        if (i > 0) {
          target.append(',');
        }
        target.append(encodedElements.get(i));
      }
      target.append(']');
    }

    /**
     * Appends an order-preserving encoding of an {@link Iterable}.
     *
     * @param target   the destination buffer
     * @param iterable the iterable to encode
     * @param depth    the nesting depth of this iterable
     */
    private void appendIterable(StringBuilder target, Iterable<?> iterable, int depth) {
      checkDepth(depth);
      pushContainer(iterable, depth);
      if (iterable instanceof Collection<?> collection && collection.size() > remainingElements) {
        // A collection that cannot fit the remaining budget fails before the first element is
        // encoded, whatever its elements would expand to.
        throw tooManyElements();
      }
      target.append("l[");
      boolean first = true;
      for (Object element : iterable) {
        consumeElements(1);
        if (!first) {
          target.append(',');
        }
        first = false;
        appendPart(target, element, depth);
      }
      target.append(']');
    }

    /**
     * Appends an order-preserving encoding of a primitive or reference array.
     *
     * @param target the destination buffer
     * @param array  the array object to encode
     * @param depth  the nesting depth of this array
     */
    private void appendArray(StringBuilder target, Object array, int depth) {
      checkDepth(depth);
      if (array instanceof Object[] values) {
        pushContainer(values, depth);
        consumeElements(values.length);
        target.append("aO[");
        for (int i = 0; i < values.length; i++) {
          if (i > 0) {
            target.append(',');
          }
          appendPart(target, values[i], depth);
        }
        target.append(']');
        return;
      }

      if (array instanceof int[] values) {
        consumeElements(values.length);
        target.append("aI[");
        appendInts(target, values);
      } else if (array instanceof long[] values) {
        consumeElements(values.length);
        target.append("aJ[");
        appendLongs(target, values);
      } else if (array instanceof byte[] values) {
        consumeElements(values.length);
        target.append("aB[");
        appendBytes(target, values);
      } else if (array instanceof short[] values) {
        consumeElements(values.length);
        target.append("aS[");
        appendShorts(target, values);
      } else if (array instanceof double[] values) {
        consumeElements(values.length);
        target.append("aD[");
        appendDoubles(target, values);
      } else if (array instanceof float[] values) {
        consumeElements(values.length);
        target.append("aF[");
        appendFloats(target, values);
      } else if (array instanceof char[] values) {
        consumeElements(values.length);
        target.append("aC[");
        appendChars(target, values);
      } else if (array instanceof boolean[] values) {
        consumeElements(values.length);
        target.append("aZ[");
        appendBooleans(target, values);
      } else {
        throw new AssertionError("Unexpected array type: " + array.getClass().getName());
      }
      target.append(']');
    }

    private static void appendInts(StringBuilder target, int[] values) {
      for (int i = 0; i < values.length; i++) {
        if (i > 0) {
          target.append(',');
        }
        appendInt(target, values[i]);
      }
    }

    private static void appendLongs(StringBuilder target, long[] values) {
      for (int i = 0; i < values.length; i++) {
        if (i > 0) {
          target.append(',');
        }
        appendLong(target, values[i]);
      }
    }

    private static void appendBytes(StringBuilder target, byte[] values) {
      for (int i = 0; i < values.length; i++) {
        if (i > 0) {
          target.append(',');
        }
        appendByte(target, values[i]);
      }
    }

    private static void appendShorts(StringBuilder target, short[] values) {
      for (int i = 0; i < values.length; i++) {
        if (i > 0) {
          target.append(',');
        }
        appendShort(target, values[i]);
      }
    }

    private static void appendDoubles(StringBuilder target, double[] values) {
      for (int i = 0; i < values.length; i++) {
        if (i > 0) {
          target.append(',');
        }
        appendDouble(target, values[i]);
      }
    }

    private static void appendFloats(StringBuilder target, float[] values) {
      for (int i = 0; i < values.length; i++) {
        if (i > 0) {
          target.append(',');
        }
        appendFloat(target, values[i]);
      }
    }

    private static void appendChars(StringBuilder target, char[] values) {
      for (int i = 0; i < values.length; i++) {
        if (i > 0) {
          target.append(',');
        }
        appendChar(target, values[i]);
      }
    }

    private static void appendBooleans(StringBuilder target, boolean[] values) {
      for (int i = 0; i < values.length; i++) {
        if (i > 0) {
          target.append(',');
        }
        appendBoolean(target, values[i]);
      }
    }

    /**
     * Records one container as an ancestor for cycle detection. The depth-1 container is kept in a
     * plain field, so a flat array, set or iterable allocates nothing; the ancestor array is created
     * on the first nested container and each depth slot is overwritten by the next sibling subtree.
     *
     * @param container the container being entered
     * @param depth     its nesting depth ({@code >= 1})
     * @throws IllegalStateException when the container is one of its own ancestors
     */
    private void pushContainer(Object container, int depth) {
      if (depth == 1) {
        rootContainer = container;
        return;
      }
      if (ancestors == null) {
        ancestors = new Object[MAX_DEPTH + 1];
      }
      if (container == rootContainer) {
        throw selfReference();
      }
      for (int i = 2; i < depth; i++) {
        if (ancestors[i] == container) {
          throw selfReference();
        }
      }
      ancestors[depth] = container;
    }

    private static IllegalStateException selfReference() {
      return new IllegalStateException(
        "default cache key contains a self-referential container; declare an explicit key"
      );
    }

    /**
     * Ensures the current container nesting depth does not exceed {@link #MAX_DEPTH}.
     *
     * @param depth the container nesting depth to validate
     * @throws IllegalStateException if {@code depth} exceeds {@link #MAX_DEPTH}
     */
    private static void checkDepth(int depth) {
      if (depth > MAX_DEPTH) {
        throw new IllegalStateException(
          "default cache key exceeds " + MAX_DEPTH + " nested levels; declare an explicit key"
        );
      }
    }

    /**
     * Deducts {@code count} elements from the remaining element budget.
     *
     * @param count the non-negative number of elements to consume
     * @throws IllegalStateException if deducting {@code count} would exceed {@link #MAX_ELEMENTS}
     */
    private void consumeElements(int count) {
      if (count > remainingElements) {
        throw tooManyElements();
      }
      remainingElements -= count;
    }

    /**
     * Builds the one budget-exceeded failure shared by the pre-check and the running deduction.
     *
     * @return the exception to throw
     */
    private static IllegalStateException tooManyElements() {
      return new IllegalStateException(
        "default cache key exceeds " + MAX_ELEMENTS + " elements; declare an explicit key"
      );
    }
  }
}
