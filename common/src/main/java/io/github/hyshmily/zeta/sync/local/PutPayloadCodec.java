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
package io.github.hyshmily.zeta.sync.local;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import io.github.hyshmily.zeta.util.Internal;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serial;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Value-carrying {@code PUT} payload codec using a single Jackson {@link ObjectMapper} for
 * outbound serialization and inbound deserialization, paired with a bounded write buffer so
 * oversized values degrade to REFRESH without allocating beyond {@code zeta.sync.put-max-bytes}.
 *
 * <p>Default typing ({@link ObjectMapper.DefaultTyping#NON_FINAL}) over an {@code Object} root type
 * preserves runtime types across value classes such as {@code Long}, {@code BigDecimal},
 * {@code Instant} and {@code byte[]}. Deserialization fails closed against a
 * {@link PolymorphicTypeValidator} that admits an explicit set of JDK value and container classes,
 * the {@code java.time}/{@code java.math} packages, {@code io.github.hyshmily.zeta.model.*}, arrays
 * of admitted types, and configured {@code zeta.sync.put-allowed-packages}; a container is admitted
 * only together with its generic arguments, so a type id cannot smuggle an unlisted element type
 * through an allowed raw class. {@code java.lang} and {@code java.util} are never admitted as whole
 * prefixes: those packages hold side-effecting types such as {@code Thread},
 * {@code ProcessBuilder}, {@code Timer} and {@code FileHandler} that a compromised broker writer
 * must not be able to name.
 *
 * <p>Root JDK immutable collections and views ({@code List.of}, {@code Set.of},
 * {@code Arrays.asList}, {@code Collections} wrappers) are normalized into standard mutable
 * containers prior to encoding so receivers can instantiate them; nested immutable collections
 * inside DTOs are not normalized and may degrade to INVALIDATE if non-instantiable.
 */
@Slf4j
@Internal
public final class PutPayloadCodec {

  /** Initial byte-buffer cap so small payloads do not pre-allocate {@link #maxBytes}. */
  private static final int INITIAL_BUFFER_SIZE = 1024;

  /**
   * Exact JDK value and container classes admitted without opening all of {@code java.lang} or
   * {@code java.util}. Anything else in those packages — a thread, a process builder, a logging
   * handler, a timer — stays unreachable from a payload type id.
   */
  private static final Set<String> DEFAULT_ALLOWED_CLASSES = Set.of(
    "java.lang.Object",
    "java.lang.String",
    "java.lang.Boolean",
    "java.lang.Byte",
    "java.lang.Short",
    "java.lang.Integer",
    "java.lang.Long",
    "java.lang.Float",
    "java.lang.Double",
    "java.lang.Character",
    "java.lang.StringBuilder",
    "java.lang.StringBuffer",
    "java.util.ArrayList",
    "java.util.LinkedList",
    "java.util.HashSet",
    "java.util.LinkedHashSet",
    "java.util.TreeSet",
    "java.util.HashMap",
    "java.util.LinkedHashMap",
    "java.util.TreeMap",
    "java.util.UUID",
    "java.util.Date",
    "java.util.Currency",
    "java.util.Locale",
    "java.util.BitSet",
    "java.util.Optional",
    // JDK collection views that a DTO field can carry on the wire; Jackson maps each back to a
    // standard mutable container through its collection fallbacks.
    "java.util.Arrays$ArrayList",
    "java.util.Collections$EmptyList",
    "java.util.Collections$EmptySet",
    "java.util.Collections$EmptyMap",
    "java.util.Collections$SingletonList",
    "java.util.Collections$SingletonSet",
    "java.util.Collections$SingletonMap",
    "java.util.Collections$UnmodifiableList",
    "java.util.Collections$UnmodifiableRandomAccessList",
    "java.util.Collections$UnmodifiableSet",
    "java.util.Collections$UnmodifiableMap",
    "java.util.ImmutableCollections$List12",
    "java.util.ImmutableCollections$ListN",
    "java.util.ImmutableCollections$SubList",
    "java.util.ImmutableCollections$Set12",
    "java.util.ImmutableCollections$SetN",
    "java.util.ImmutableCollections$Map1",
    "java.util.ImmutableCollections$MapN"
  );

  /** Package prefixes always admitted by the polymorphic type validator. */
  private static final List<String> DEFAULT_ALLOWED_PREFIXES = List.of(
    "java.time.",
    "java.math.",
    "io.github.hyshmily.zeta.model."
  );

  private final ObjectWriter rootWriter;
  private final ObjectReader rootReader;
  private final int maxBytes;

  /**
   * Creates a codec from sync configuration properties.
   *
   * @param properties sync properties supplying the payload size cap and package allowlist
   * @throws NullPointerException if {@code properties} is {@code null}
   */
  public PutPayloadCodec(CacheSyncProperties properties) {
    this(
      Objects.requireNonNull(properties, "properties must not be null").getPutMaxBytes(),
      properties.getPutAllowedPackages()
    );
  }

  /**
   * Creates a codec from explicit size-cap and allowlist settings.
   *
   * @param maxBytes        maximum serialized payload size in bytes; non-positive disables PUT payloads
   * @param allowedPackages extra package prefixes admitted during polymorphic deserialization
   *                        (a trailing {@code '.'} is appended when absent); may be {@code null}
   */
  public PutPayloadCodec(int maxBytes, @Nullable List<String> allowedPackages) {
    this.maxBytes = maxBytes;
    ObjectMapper mapper = buildMapper(allowedPackages);
    // Both ends declare Object.class so default typing applies to the root value: a final runtime
    // type (Long, Instant, byte[], a record) is otherwise written without its type id.
    this.rootWriter = mapper.writerFor(Object.class);
    this.rootReader = mapper.readerFor(Object.class);
  }

  /**
   * Serializes {@code value} into an immutable byte payload bounded by {@link #maxBytes}, or
   * returns {@code null} when the value cannot be serialized or exceeds the size cap (signaling
   * callers to degrade to a value-less REFRESH).
   *
   * @param value the freshly written cache value, or {@code null}
   * @return the serialized payload bytes, or {@code null} on skip/failure
   */
  public byte@Nullable [] encode(@Nullable Object value) {
    if (value == null || maxBytes <= 0) {
      return null;
    }
    BoundedBuffer buffer = new BoundedBuffer(maxBytes);
    try {
      rootWriter.writeValue(buffer, normalizeRoot(value));
      return buffer.toByteArray();
    } catch (RuntimeException | IOException e) {
      // Check buffer.exceeded rather than catching PayloadTooLargeException directly because
      // Jackson may wrap OutputStream failures in a JsonMappingException.
      if (buffer.exceeded) {
        log.debug("Put payload exceeds put-max-bytes {}, degrading to REFRESH", maxBytes);
      } else {
        log.debug("Put payload serialization failed, degrading to REFRESH: {}", e.toString());
      }
      return null;
    }
  }

  /**
   * Deserializes a received PUT payload back into its original runtime type.
   *
   * @param payload the raw AMQP body bytes; {@code null} or empty returns {@code null}
   * @return the decoded value, or {@code null} if deserialization or type validation fails
   */
  public @Nullable Object decode(byte@Nullable [] payload) {
    if (payload == null || payload.length == 0) {
      return null;
    }
    try {
      return rootReader.readValue(payload);
    } catch (RuntimeException | IOException e) {
      log.debug("Put payload value deserialization failed, degrading to INVALIDATE: {}", e.toString());
      return null;
    }
  }

  /**
   * Copies non-standard root collection/map views into standard mutable containers that receivers
   * can reconstruct; already-standard containers pass through unchanged.
   *
   * <p>The comparison is exact on purpose. A framework map subclass or a double-brace initializer
   * is neither admitted by the type validator nor instantiable by it, so encoding it would degrade
   * the peer to INVALIDATE; normalizing every non-standard root keeps the PUT.
   */
  private static Object normalizeRoot(Object value) {
    Class<?> type = value.getClass();
    if (value instanceof List<?> list && type != ArrayList.class && type != LinkedList.class) {
      return new ArrayList<>(list);
    }
    if (value instanceof Set<?> set && type != HashSet.class && type != LinkedHashSet.class && type != TreeSet.class) {
      return new LinkedHashSet<>(set);
    }
    if (
      value instanceof Map<?, ?> map && type != HashMap.class && type != LinkedHashMap.class && type != TreeMap.class
    ) {
      return new LinkedHashMap<>(map);
    }
    return value;
  }

  /**
   * Builds a Jackson {@link ObjectMapper} with fail-closed polymorphic typing enabled. The default
   * package set is always admitted; configured packages are normalized with a trailing dot so a
   * prefix cannot admit a sibling package by accident. Every type id is then checked by
   * {@link SafePolymorphicTypeValidator} before Jackson loads a class, so an untrusted PUT payload
   * cannot instantiate an arbitrary type on the receiving node.
   *
   * @param allowedPackages additional package prefixes from configuration, or {@code null}
   * @return the mapper shared by encoding and decoding
   */
  private static ObjectMapper buildMapper(@Nullable List<String> allowedPackages) {
    List<String> prefixes = new ArrayList<>(DEFAULT_ALLOWED_PREFIXES);
    if (allowedPackages != null) {
      for (String pkg : allowedPackages) {
        if (pkg != null && !pkg.isBlank()) {
          String prefix = pkg.strip();
          // Ensure a trailing dot so "com.foo" does not accidentally admit "com.foobar".
          prefixes.add(prefix.endsWith(".") ? prefix : prefix + ".");
        }
      }
    }
    return JsonMapper.builder()
      .findAndAddModules()
      .build()
      .activateDefaultTyping(
        new SafePolymorphicTypeValidator(List.copyOf(prefixes)),
        ObjectMapper.DefaultTyping.NON_FINAL,
        JsonTypeInfo.As.WRAPPER_ARRAY
      );
  }

  /**
   * Fail-closed validator that judges every resolved type — raw class, array component and generic
   * arguments — and admits {@code java.lang}/{@code java.util} only through the explicit
   * value/container set.
   *
   * <p>{@code validateSubClassName} receives the type id truncated at the first {@code '<'}, and
   * Jackson only consults {@code validateSubType} when that name check is not a plain {@code ALLOWED}.
   * An admitted raw name therefore answers {@link Validity#INDETERMINATE}, which makes the
   * constructed {@link JavaType} — type arguments included — pass through
   * {@code validateSubType} before any value is bound. Answering {@code ALLOWED} there would leave
   * {@code ArrayList<com.example.Gadget>} unjudged, because the truncated name alone cannot show its
   * arguments.
   */
  private static final class SafePolymorphicTypeValidator extends PolymorphicTypeValidator.Base {

    private static final long serialVersionUID = 1L;

    private final List<String> allowedPrefixes;

    private SafePolymorphicTypeValidator(List<String> allowedPrefixes) {
      this.allowedPrefixes = allowedPrefixes;
    }

    @Override
    public Validity validateSubClassName(MapperConfig<?> config, JavaType baseType, String subClassName) {
      return isAllowedTypeName(subClassName) ? Validity.INDETERMINATE : Validity.DENIED;
    }

    @Override
    public Validity validateSubType(MapperConfig<?> config, JavaType baseType, JavaType subType) {
      return isAllowedJavaType(subType) ? Validity.ALLOWED : Validity.DENIED;
    }

    /**
     * Whether one resolved type and everything it is parameterized by is admitted.
     *
     * @param type the type to judge
     * @return {@code true} when the raw class, every generic argument and every array component admit
     */
    private boolean isAllowedJavaType(JavaType type) {
      // Primitive components of int[]/byte[] and friends have no class name in the allowlist.
      if (type.isPrimitive()) {
        return true;
      }
      // An array's raw class name is its JVM descriptor ("[Ljava.lang.String;"), which the scalar
      // set never holds; the component type is what the name rule judges.
      if (type.isArrayType()) {
        JavaType component = type.getContentType();
        return component != null && isAllowedJavaType(component);
      }
      if (!isAllowedScalarName(type.getRawClass().getName())) {
        return false;
      }
      for (JavaType parameter : type.getBindings().getTypeParameters()) {
        if (!isAllowedJavaType(parameter)) {
          return false;
        }
      }
      return true;
    }

    /**
     * Returns whether one type id names an admitted class after stripping array dimensions.
     *
     * @param typeName the JVM type id, optionally in array descriptor form
     * @return {@code true} if the scalar type is admitted
     */
    private boolean isAllowedTypeName(String typeName) {
      if (!typeName.startsWith("[")) {
        return isAllowedScalarName(typeName);
      }
      int i = 0;
      while (i < typeName.length() && typeName.charAt(i) == '[') {
        i++;
      }
      if (i == typeName.length() - 1) {
        return switch (typeName.charAt(i)) {
          case 'Z', 'B', 'C', 'S', 'I', 'J', 'F', 'D' -> true;
          default -> false;
        };
      }
      if (i < typeName.length() - 2 && typeName.charAt(i) == 'L' && typeName.charAt(typeName.length() - 1) == ';') {
        return isAllowedScalarName(typeName.substring(i + 1, typeName.length() - 1));
      }
      return false;
    }

    /**
     * Returns whether one scalar class name is admitted by the explicit set or a configured prefix.
     *
     * @param className the binary class name
     * @return {@code true} if admitted
     */
    private boolean isAllowedScalarName(String className) {
      if (DEFAULT_ALLOWED_CLASSES.contains(className)) {
        return true;
      }
      for (String prefix : allowedPrefixes) {
        if (className.startsWith(prefix)) {
          return true;
        }
      }
      return false;
    }
  }

  /** Stackless control-flow exception aborting Jackson encoding once {@link #maxBytes} is exceeded. */
  private static final class PayloadTooLargeException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final PayloadTooLargeException INSTANCE = new PayloadTooLargeException();

    private PayloadTooLargeException() {
      super("put payload exceeds put-max-bytes", null, false, false);
    }
  }

  /**
   * Bounded {@link ByteArrayOutputStream} that aborts serialization via
   * {@link PayloadTooLargeException} before growing past {@code limit}.
   */
  private static final class BoundedBuffer extends ByteArrayOutputStream {

    private final int limit;
    private boolean exceeded;

    private BoundedBuffer(int limit) {
      super(Math.min(limit, INITIAL_BUFFER_SIZE));
      this.limit = limit;
    }

    @Override
    public void write(int b) {
      guard(1);
      super.write(b);
    }

    @Override
    public void write(byte@NonNull [] bytes, int offset, int length) {
      guard(length);
      super.write(bytes, offset, length);
    }

    /** Aborts before writing if {@code length} exceeds the remaining byte budget. */
    private void guard(int length) {
      // Subtraction form avoids integer overflow if length is large.
      if (length > limit - count) {
        exceeded = true;
        throw PayloadTooLargeException.INSTANCE;
      }
    }
  }
}
