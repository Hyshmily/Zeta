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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * Canonical string encoding of a default cache key: the key an annotated method
 * uses when it declares no {@code key} expression.
 *
 * <p><b>Format.</b>
 *
 * <pre>
 * key              ::= "[]" | "[" part ("," part)* "]"
 * part(null)       ::= "n"
 * part(String)     ::= "s" length ":" text
 * part(Character)  ::= "c" length ":" text
 * part(CharSequence) ::= "q" length ":" text
 * part(Byte)       ::= "B:" text
 * part(Short)      ::= "S:" text
 * part(Integer)    ::= "I:" text
 * part(Long)       ::= "J:" text
 * part(Float)      ::= "F:" text
 * part(Double)     ::= "D:" text
 * part(BigInteger) ::= "N:" text
 * part(BigDecimal) ::= "E:" text
 * part(Boolean)    ::= "Z:" text
 * part(Iterable)   ::= "l[" (part ("," part)*)? "]"
 * part(Set)        ::= "t[" (sorted-part ("," sorted-part)*)? "]"
 * part(array)      ::= "a" component "[" (part ("," part)*)? "]"
 * part(other)      ::= "o" length ":" text      (the value's String.valueOf)
 * </pre>
 *
 * <p>{@code length} is the UTF-16 code-unit count of {@code text};
 * {@code component} is {@code O} for a reference array and the JVM primitive
 * letter ({@code B S I J F D C Z}) for a primitive array. Array elements are
 * encoded by the same rules — a primitive element uses its scalar tag, so
 * {@code int[]{1}} renders as {@code aI[I:1]}. The numeric tags carry no length
 * prefix because a number's text never contains the {@code ,} or {@code ]}
 * delimiters, and they keep {@code Integer 1}, {@code Long 1},
 * {@code BigDecimal("1")} and {@code String "1"} distinct. {@code -0.0} and
 * {@code 0.0} render identically: Java's {@code ==} treats them as equal, and a
 * key that kept them apart would miss for a numerically equal argument.
 *
 * <p><b>A {@code List} preserves order; a {@code Set} is canonicalised.</b> A
 * {@code Set} is encoded as {@code t[...]} with its element encodings sorted, so
 * {@code Set.of("a", "b")} produces the same physical key on every JVM
 * regardless of the implementation's iteration order or hash salt. A
 * {@code List} and any other {@code Iterable} use {@code l[...]} in iteration
 * order; two lists with the same elements in different order are different
 * keys. A {@code Set} and a {@code List} with the same elements are different
 * keys because their runtime shapes differ.
 *
 * <p><b>Collision-free across argument boundaries, arity and the common value
 * shapes.</b> Every part is self-delimiting: its tag identifies the shape, an
 * {@code s}/{@code c}/{@code q}/{@code o} text carries its length before the
 * separator, and an array or collection is bracketed and recursed, so no
 * delimiter inside a value can move an argument boundary. Therefore
 * {@code ("foo, bar", "baz")} and {@code ("foo", "bar, baz")} differ;
 * {@code []} can never equal a single-argument key, because every part starts
 * with a tag; arrays and collections are encoded by content rather than identity
 * hash; and {@code "1"}, {@code '1'}, {@code new StringBuilder("1")},
 * {@code 1}, {@code 1L} and {@code new BigDecimal("1")} all carry different
 * tags.
 *
 * <p><b>Remaining limits.</b> A non-array, non-collection object is rendered by
 * {@link String#valueOf(Object)}, so two values (or types) with the same
 * {@code toString()} share a key — an untagged {@link Number} subclass such as
 * {@code AtomicInteger} falls into this group. Every reference array carries the
 * same {@code aO} component tag, so two different component classes whose
 * element encodings coincide (e.g. {@code String[]{"a"}} and
 * {@code Object[]{"a"}}) share a key. A {@link java.util.Map} is not an
 * {@code Iterable} and still uses {@code toString()}; an unordered {@code Map}
 * should declare an explicit key. A value type or array shape that must not
 * collide with another should declare an explicit {@code key} expression.
 *
 * <p><b>Large values.</b> An array or collection is encoded element by element,
 * so a multi-megabyte binary argument produces a key of comparable size. Declare
 * an explicit key — a hash or an identifier — for large buffers. A non-array,
 * non-collection object's {@code toString()} is its own contract; a
 * self-referential {@code Map} or custom object that recurses through
 * {@code toString()} is outside this encoder's cycle detection and will
 * overflow the stack in that method.
 *
 * <p><b>Cycles.</b> A reference array or collection that repeats a container already
 * on the current recursion path is rejected at that first repeat; a cycle whose
 * period is longer than the path is rejected by the {@value #MAX_DEPTH}-level
 * depth cap. A collection with more
 * than {@value #MAX_ELEMENTS} elements is rejected instead of producing a giant
 * key. All of these fail the call with a configuration error and are fixed by
 * declaring an explicit {@code key} expression.
 *
 * <p>The caller validates the arguments: a {@code null} top-level argument is
 * rejected before {@link #encode} is called, so {@code null} is only encodable
 * as an array/collection element, where it renders as {@code n}.
 */
@Internal
public final class DefaultKey {

  /**
   * Maximum array nesting depth. Deep enough for every realistic DTO shape and
   * shallow enough that a cycle longer than this cap, and any acyclic graph,
   * fails without exhausting the stack; a shorter cycle is rejected earlier by
   * the ancestor check in {@link #enterContainer}.
   */
  private static final int MAX_DEPTH = 32;

  /**
   * Maximum elements in one collection key. Deep enough for a realistic id
   * batch and small enough that a malformed or unbounded {@link Iterable} fails
   * instead of building a multi-megabyte key.
   */
  private static final int MAX_ELEMENTS = 1024;

  /**
   * Encode one invocation's arguments under an already resolved key prefix.
   * The prefix is written into the same builder, so the default-key path does not
   * allocate a logical-key string and copy it again for the namespace.
   *
   * @param prefix the physical namespace prefix ({@code cache + delimiter})
   * @param args   the invocation arguments, never {@code null} and never holding
   *               a {@code null} top-level element
   * @return the physical key
   */
  public static String encode(String prefix, Object[] args) {
    if (args.length == 0) {
      return prefix + "[]";
    }
    StringBuilder key = new StringBuilder(
      args.length == 1 ? singleValueCapacity(prefix, args[0]) : prefix.length() + 32
    );
    key.append(prefix).append('[');
    for (int i = 0; i < args.length; i++) {
      if (i > 0) {
        key.append(',');
      }
      part(key, args[i], 0, null);
    }
    return key.append(']').toString();
  }

  /**
   * Encode one invocation's arguments.
   *
   * @param args the invocation arguments, never {@code null} and never holding a
   *             {@code null} top-level element
   * @return the canonical key string
   */
  public static String encode(Object[] args) {
    return encode("", args);
  }

  /**
   * Encode one explicitly selected value under an already resolved key prefix,
   * without allocating the one-element argument array {@link #encode(String, Object[])}
   * would need.
   *
   * @param prefix the physical namespace prefix ({@code cache + delimiter})
   * @param value  the value to encode; never {@code null}
   * @return the physical key
   */
  public static String encodeSingle(String prefix, Object value) {
    StringBuilder key = new StringBuilder(singleValueCapacity(prefix, value));
    key.append(prefix).append('[');
    part(key, value, 0, null);
    return key.append(']').toString();
  }

  /**
   * The builder capacity for one encoded value: the two brackets, the value's tag, the colon and
   * the worst-case ten-digit decimal length of a String's length prefix, plus its text. Sizing from
   * the text avoids a grow-and-copy on every call; the other common scalars fit a 24-character
   * default (a Long is the widest).
   */
  private static int singleValueCapacity(String prefix, Object value) {
    int extra = value instanceof String string ? string.length() + 14 : 24;
    return prefix.length() + extra;
  }

  private static void part(StringBuilder key, @Nullable Object value, int depth, Object@Nullable [] ancestors) {
    if (value == null) {
      key.append('n');
    } else if (value instanceof String string) {
      text(key, 's', string);
    } else if (value instanceof Integer integer) {
      key.append("I:").append(integer.intValue());
    } else if (value instanceof Long longValue) {
      key.append("J:").append(longValue.longValue());
    } else if (value instanceof Boolean bool) {
      key.append("Z:").append(bool.booleanValue());
    } else if (value instanceof Double doubleValue) {
      double v = doubleValue;
      key.append("D:").append(v == 0.0d ? 0.0d : v);
    } else if (value instanceof Float floatValue) {
      float v = floatValue;
      key.append("F:").append(v == 0.0f ? 0.0f : v);
    } else if (value instanceof Short shortValue) {
      key.append("S:").append(shortValue.intValue());
    } else if (value instanceof Byte byteValue) {
      key.append("B:").append(byteValue.intValue());
    } else if (value instanceof BigInteger integer) {
      key.append("N:").append(integer);
    } else if (value instanceof BigDecimal decimal) {
      key.append("E:").append(decimal);
    } else if (value instanceof Character character) {
      // One UTF-16 code unit, so the length prefix is the literal 1; appending
      // the char directly matches the char[] branch and allocates no String.
      key.append("c1:").append(character.charValue());
    } else if (value instanceof CharSequence sequence) {
      // Final classes are tested first: their check is a Klass pointer
      // comparison, while an interface check reads the secondary supertype
      // table. The common cached argument shapes are String, the wrappers and
      // Boolean; Character/CharSequence are rarer and keep their tags.
      text(key, 'q', sequence.toString());
    } else if (value instanceof Set<?> set) {
      collection(key, set, depth + 1, ancestors, true);
    } else if (value instanceof Iterable<?> iterable) {
      collection(key, iterable, depth + 1, ancestors, false);
    } else if (value.getClass().isArray()) {
      array(key, value, depth + 1, ancestors);
    } else {
      text(key, 'o', String.valueOf(value));
    }
  }

  /**
   * Encode one collection. A {@code Set} is canonicalised by sorting its encoded
   * elements, so an unordered set produces the same key on every JVM; any other
   * {@code Iterable} keeps its iteration order.
   *
   * <p>The ancestor chain is allocated lazily, only when an element is itself a
   * collection or array. A flat collection and every primitive array therefore
   * allocate no chain. A nested container that repeats an ancestor on the current
   * path fails at the first repeat, so a branching cycle cannot expand
   * exponentially; the depth cap catches a longer cycle and acyclic nesting.
   */
  private static void collection(
    StringBuilder key,
    Iterable<?> values,
    int depth,
    Object@Nullable [] ancestors,
    boolean sorted
  ) {
    Object[] chain = ancestors;
    enterContainer(values, depth, chain, "collection");

    if (sorted) {
      key.append("t[");
      List<String> parts = new ArrayList<>();
      StringBuilder encoded = new StringBuilder();
      int count = 0;
      for (Object element : values) {
        if (++count > MAX_ELEMENTS) {
          throw tooManyElements();
        }
        chain = ensureAncestors(values, element, depth, chain);
        encoded.setLength(0);
        part(encoded, element, depth, chain);
        parts.add(encoded.toString());
      }
      parts.sort(null);
      for (int i = 0; i < parts.size(); i++) {
        if (i > 0) {
          key.append(',');
        }
        key.append(parts.get(i));
      }
    } else {
      key.append("l[");
      int count = 0;
      for (Object element : values) {
        if (++count > MAX_ELEMENTS) {
          throw tooManyElements();
        }
        if (count > 1) {
          key.append(',');
        }
        chain = ensureAncestors(values, element, depth, chain);
        part(key, element, depth, chain);
      }
    }
    key.append(']');
  }

  /**
   * Enter one container: enforce the depth cap, reject a container already on the current path, and
   * record it as the path's newest ancestor. The chain exists from the second nesting level onwards;
   * a top-level container allocates it lazily in its element loop.
   */
  private static void enterContainer(Object container, int depth, Object@Nullable [] ancestors, String kind) {
    if (depth > MAX_DEPTH) {
      throw new IllegalStateException(
        "default cache key exceeds " + MAX_DEPTH + " nested " + kind + " levels; declare an explicit key"
      );
    }
    if (depth > 1 && ancestors != null) {
      for (int i = 0; i < depth - 1; i++) {
        if (ancestors[i] == container) {
          throw new IllegalStateException(
            "default cache key cannot encode a cyclic " + kind + "; declare an explicit key"
          );
        }
      }
      ancestors[depth - 1] = container;
    }
  }

  /**
   * Allocate the ancestor chain when a top-level container is about to recurse into a nested one.
   * A flat collection or reference array allocates nothing.
   */
  private static Object@Nullable [] ensureAncestors(
    Object container,
    @Nullable Object element,
    int depth,
    Object@Nullable [] chain
  ) {
    if (chain == null && depth == 1 && isContainer(element)) {
      Object[] allocated = new Object[MAX_DEPTH];
      allocated[0] = container;
      return allocated;
    }
    return chain;
  }

  /** Whether an element can recurse into the container encoder. */
  private static boolean isContainer(@Nullable Object value) {
    return value != null && (value instanceof Iterable<?> || value.getClass().isArray());
  }

  private static IllegalStateException tooManyElements() {
    return new IllegalStateException(
      "default cache key collection exceeds " + MAX_ELEMENTS + " elements; declare an explicit key"
    );
  }

  /**
   * Encode one array. Each branch writes its own component prefix and loops
   * over the concrete type, so no element is boxed, no {@link java.lang.reflect.Array#get}
   * call is made and no component class or descriptor string is resolved.
   *
   * <p>A reference array records itself in a lazily allocated ancestor chain
   * only when it actually contains another container (array or collection). A
   * nested container that matches an ancestor is a cycle and fails at the first
   * repeat, so a branching cycle cannot expand exponentially; a flat reference
   * array and every primitive array allocate no chain.
   */
  private static void array(StringBuilder key, Object array, int depth, Object@Nullable [] ancestors) {
    if (depth > MAX_DEPTH) {
      throw new IllegalStateException(
        "default cache key exceeds " + MAX_DEPTH + " nested array levels; declare an explicit key"
      );
    }
    if (array instanceof Object[] values) {
      Object[] chain = ancestors;
      enterContainer(values, depth, chain, "array");
      key.append("aO[");
      for (int i = 0; i < values.length; i++) {
        if (i > 0) {
          key.append(',');
        }
        chain = ensureAncestors(values, values[i], depth, chain);
        part(key, values[i], depth, chain);
      }
    } else if (array instanceof byte[] values) {
      key.append("aB[");
      for (int i = 0; i < values.length; i++) {
        if (i > 0) {
          key.append(',');
        }
        key.append("B:").append(values[i]);
      }
    } else if (array instanceof short[] values) {
      key.append("aS[");
      for (int i = 0; i < values.length; i++) {
        if (i > 0) {
          key.append(',');
        }
        key.append("S:").append(values[i]);
      }
    } else if (array instanceof int[] values) {
      key.append("aI[");
      for (int i = 0; i < values.length; i++) {
        if (i > 0) {
          key.append(',');
        }
        key.append("I:").append(values[i]);
      }
    } else if (array instanceof long[] values) {
      key.append("aJ[");
      for (int i = 0; i < values.length; i++) {
        if (i > 0) {
          key.append(',');
        }
        key.append("J:").append(values[i]);
      }
    } else if (array instanceof float[] values) {
      key.append("aF[");
      for (int i = 0; i < values.length; i++) {
        if (i > 0) {
          key.append(',');
        }
        key.append("F:").append(values[i] == 0.0f ? 0.0f : values[i]);
      }
    } else if (array instanceof double[] values) {
      key.append("aD[");
      for (int i = 0; i < values.length; i++) {
        if (i > 0) {
          key.append(',');
        }
        key.append("D:").append(values[i] == 0.0d ? 0.0d : values[i]);
      }
    } else if (array instanceof char[] values) {
      key.append("aC[");
      for (int i = 0; i < values.length; i++) {
        if (i > 0) {
          key.append(',');
        }
        key.append("c1:").append(values[i]);
      }
    } else if (array instanceof boolean[] values) {
      key.append("aZ[");
      for (int i = 0; i < values.length; i++) {
        if (i > 0) {
          key.append(',');
        }
        key.append(values[i] ? "Z:true" : "Z:false");
      }
    } else {
      throw new AssertionError();
    }
    key.append(']');
  }

  private static void text(StringBuilder key, char tag, String text) {
    key.append(tag).append(text.length()).append(':').append(text);
  }

  private DefaultKey() {}
}
