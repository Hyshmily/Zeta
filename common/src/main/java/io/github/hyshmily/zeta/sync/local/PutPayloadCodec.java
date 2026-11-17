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
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator.Validity;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import io.github.hyshmily.zeta.util.Internal;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serial;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * The value-carrying {@code PUT} payload codec: one configured Jackson mapper for both
 * the outbound serialization and the inbound deserialization, with a bounded write buffer
 * so an oversized business value degrades to REFRESH without materializing beyond
 * {@code zeta.sync.put-max-bytes}.
 *
 * <p>Default typing is on for <b>every</b> type ({@code EVERYTHING} +
 * {@code WRAPPER_ARRAY}), so a final type such as {@code Long}, {@code BigDecimal} or
 * {@code byte[]} carries its type marker and round-trips as the exact runtime class the
 * writer stored. Deserialization enforces a polymorphic whitelist — JDK value types, JDK
 * {@link Collection}/{@link Map} implementations in {@code java.util} (other packages and the
 * legacy {@link Properties} and {@link Hashtable} containers are denied),
 * {@code io.github.hyshmily.zeta.model.*} plus the configured
 * {@code zeta.sync.put-allowed-packages}; a business DTO outside it fails closed and the
 * receiver degrades to a versioned INVALIDATE.
 *
 * <p>A JDK collection implementation that the receiver cannot instantiate (an immutable
 * {@code List.of} family, a {@code Collections} view, {@code Arrays.asList}) is written under
 * the type id of a standard {@code ArrayList}/{@code HashSet}/{@code HashMap}, so it
 * round-trips without widening the receiver's allowlist.
 */
@Slf4j
@Internal
public final class PutPayloadCodec {

  private final ObjectMapper mapper;
  private final int maxBytes;

  /**
   * Creates the codec from the sync configuration.
   *
   * @param properties the sync properties carrying the payload cap and allowlist
   */
  public PutPayloadCodec(CacheSyncProperties properties) {
    this(properties.getPutMaxBytes(), properties.getPutAllowedPackages());
  }

  /**
   * Creates the codec from explicit settings.
   *
   * @param maxBytes       maximum serialized payload size; non-positive disables PUT payloads
   * @param allowedPackages extra package prefixes admitted by the polymorphic validator; a
   *                        trailing {@code .} is added when absent
   */
  public PutPayloadCodec(int maxBytes, @Nullable List<String> allowedPackages) {
    this.maxBytes = maxBytes;
    this.mapper = buildMapper(allowedPackages);
  }

  /**
   * Serializes one value into an immutable payload, or returns {@code null} when it must
   * degrade to a value-less REFRESH (not serializable, or above the configured cap).
   *
   * @param value the freshly written value; {@code null} yields {@code null}
   * @return the payload bytes, or {@code null}
   */
  public byte@Nullable [] encode(@Nullable Object value) {
    if (value == null || maxBytes <= 0) {
      return null;
    }
    // byte[] travels Base64-encoded (4/3× expansion), so the exact pre-encoding
    // limit is 3/4 of the cap; a String's UTF-8 form is never shorter than its
    // char count, so that count is the conservative bound.
    if (value instanceof byte[] bytes && bytes.length > (maxBytes * 3L) / 4L) {
      log.debug("Put payload byte[] length {} exceeds put-max-bytes {}, degrading to REFRESH", bytes.length, maxBytes);
      return null;
    }
    if (value instanceof String text && text.length() > maxBytes) {
      log.debug("Put payload String length {} exceeds put-max-bytes {}, degrading to REFRESH", text.length(), maxBytes);
      return null;
    }

    BoundedByteArrayOutputStream buffer = new BoundedByteArrayOutputStream(maxBytes);
    try {
      mapper.writeValue(buffer, value);
      return buffer.toByteArray();
    } catch (RuntimeException | IOException e) {
      if (buffer.exceeded) {
        log.debug("Put payload exceeds put-max-bytes {}, degrading to REFRESH", maxBytes);
      } else {
        log.debug("Put payload serialization failed, degrading to REFRESH: {}", e.toString());
      }
      return null;
    }
  }

  /**
   * Deserializes a received PUT payload into its original runtime type.
   *
   * @param payload the raw AMQP body; {@code null}/empty yields {@code null}
   * @return the value, or {@code null} when it cannot be read
   */
  public @Nullable Object decode(byte@Nullable [] payload) {
    if (payload == null || payload.length == 0) {
      return null;
    }
    try {
      return mapper.readValue(payload, Object.class);
    } catch (RuntimeException | IOException e) {
      log.debug("Put payload value deserialization failed, degrading to INVALIDATE: {}", e.toString());
      return null;
    }
  }

  @SuppressWarnings("java:S5135")
  private static ObjectMapper buildMapper(@Nullable List<String> allowedPackages) {
    BasicPolymorphicTypeValidator.Builder ptv = BasicPolymorphicTypeValidator.builder()
      .allowIfSubType(String.class)
      .allowIfSubType(StringBuilder.class)
      .allowIfSubType(StringBuffer.class)
      .allowIfSubType(Byte.class)
      .allowIfSubType(Short.class)
      .allowIfSubType(Integer.class)
      .allowIfSubType(Long.class)
      .allowIfSubType(Float.class)
      .allowIfSubType(Double.class)
      .allowIfSubType(Boolean.class)
      .allowIfSubType(Character.class)
      .allowIfSubType(java.util.Optional.class)
      .allowIfSubType(java.util.UUID.class)
      .allowIfSubType(java.util.Date.class)
      .allowIfSubType(Collection.class)
      .allowIfSubType(Map.class)
      .allowIfSubType("java.time.")
      .allowIfSubType("java.math.")
      .allowIfSubType("io.github.hyshmily.zeta.model.")
      .allowIfSubTypeIsArray();
    if (allowedPackages != null) {
      for (String pkg : allowedPackages) {
        if (pkg != null && !pkg.isBlank()) {
          String prefix = pkg.trim();
          ptv.allowIfSubType(prefix.endsWith(".") ? prefix : prefix + ".");
        }
      }
    }
    ObjectMapper.DefaultTypeResolverBuilder typing = new NormalizingTypeResolverBuilder(
      ObjectMapper.DefaultTyping.EVERYTHING,
      new PayloadTypeValidator(ptv.build())
    );
    typing.init(JsonTypeInfo.Id.CLASS, null);
    typing.inclusion(JsonTypeInfo.As.WRAPPER_ARRAY);

    return JsonMapper.builder().findAndAddModules().setDefaultTyping(typing).build();
  }

  /**
   * Maps a {@code java.util} collection implementation the receiver cannot instantiate to a
   * standard mutable container. Other packages and the denied {@link Properties}/{@link Hashtable}
   * keep their own id so the existing type rules still apply.
   */
  private static @Nullable String normalizedContainerName(Object value) {
    Class<?> type = value.getClass();
    if (!"java.util".equals(type.getPackageName()) || value instanceof Properties || value instanceof Hashtable) {
      return null;
    }
    if (value instanceof List<?> && !(value instanceof ArrayList || value instanceof LinkedList)) {
      return ArrayList.class.getName();
    }
    if (
      value instanceof Set<?> &&
      !(value instanceof HashSet ||
        value instanceof LinkedHashSet ||
        value instanceof TreeSet ||
        value instanceof EnumSet)
    ) {
      return HashSet.class.getName();
    }
    if (
      value instanceof Map<?, ?> &&
      !(value instanceof HashMap ||
        value instanceof LinkedHashMap ||
        value instanceof TreeMap ||
        value instanceof EnumMap)
    ) {
      return HashMap.class.getName();
    }
    return null;
  }

  /**
   * Installs the normalizing resolver on the serialization side only; deserialization keeps
   * the standard class-name resolver, so a wire id is gated by the same polymorphic validator
   * as before.
   */
  private static final class NormalizingTypeResolverBuilder extends ObjectMapper.DefaultTypeResolverBuilder {

    private NormalizingTypeResolverBuilder(
      ObjectMapper.DefaultTyping applicability,
      PolymorphicTypeValidator subtypeValidator
    ) {
      super(applicability, subtypeValidator);
    }

    @Override
    protected TypeIdResolver idResolver(
      MapperConfig<?> config,
      JavaType baseType,
      PolymorphicTypeValidator subtypeValidator,
      Collection<NamedType> subtypes,
      boolean forSer,
      boolean forDeser
    ) {
      TypeIdResolver delegate = super.idResolver(config, baseType, subtypeValidator, subtypes, forSer, forDeser);
      return forSer ? new NormalizingIdResolver(delegate) : delegate;
    }
  }

  /** Delegates every resolver operation; only the two outbound id methods are normalized. */
  private static final class NormalizingIdResolver implements TypeIdResolver {

    private final TypeIdResolver delegate;

    private NormalizingIdResolver(TypeIdResolver delegate) {
      this.delegate = delegate;
    }

    @Override
    public void init(JavaType baseType) {
      delegate.init(baseType);
    }

    @Override
    public String idFromValue(Object value) {
      String normalized = value == null ? null : normalizedContainerName(value);
      return normalized != null ? normalized : delegate.idFromValue(value);
    }

    @Override
    public String idFromValueAndType(Object value, Class<?> suggestedType) {
      String normalized = value == null ? null : normalizedContainerName(value);
      return normalized != null ? normalized : delegate.idFromValueAndType(value, suggestedType);
    }

    @Override
    public String idFromBaseType() {
      return delegate.idFromBaseType();
    }

    @Override
    public JavaType typeFromId(DatabindContext context, String id) throws IOException {
      return delegate.typeFromId(context, id);
    }

    @Override
    public String getDescForKnownTypeIds() {
      return delegate.getDescForKnownTypeIds();
    }

    @Override
    public JsonTypeInfo.Id getMechanism() {
      return delegate.getMechanism();
    }
  }

  /**
   * Denies the legacy {@link Properties} and {@link Hashtable} containers by name, and keeps
   * every allowlisted name {@link Validity#INDETERMINATE} so the resolved class is always
   * checked. {@link #validateSubType} then denies those containers, any
   * {@link Collection}/{@link Map} implementation outside {@code java.util}, and every class
   * the allowlist does not admit; a plain {@code Map}/{@code Collection} rule would otherwise
   * admit every implementation on the classpath.
   */
  private static final class PayloadTypeValidator extends PolymorphicTypeValidator.Base {

    private final PolymorphicTypeValidator delegate;

    private PayloadTypeValidator(PolymorphicTypeValidator delegate) {
      this.delegate = delegate;
    }

    @Override
    public Validity validateSubClassName(MapperConfig<?> config, JavaType baseType, String subClassName)
      throws JsonMappingException {
      if (Properties.class.getName().equals(subClassName) || Hashtable.class.getName().equals(subClassName)) {
        return Validity.DENIED;
      }
      return delegate.validateSubClassName(config, baseType, subClassName) == Validity.DENIED
        ? Validity.DENIED
        : Validity.INDETERMINATE;
    }

    @Override
    public Validity validateSubType(MapperConfig<?> config, JavaType baseType, JavaType subType)
      throws JsonMappingException {
      Class<?> raw = subType.getRawClass();
      if (raw == Properties.class || raw == Hashtable.class || isNonJdkContainer(raw)) {
        return Validity.DENIED;
      }
      return delegate.validateSubClassName(config, baseType, raw.getName()) == Validity.ALLOWED
        ? Validity.ALLOWED
        : delegate.validateSubType(config, baseType, subType);
    }

    /** A Collection/Map implementation outside {@code java.util} is not a payload value type. */
    private static boolean isNonJdkContainer(Class<?> type) {
      return (
        (Collection.class.isAssignableFrom(type) || Map.class.isAssignableFrom(type)) &&
        !"java.util".equals(type.getPackageName())
      );
    }
  }

  /** Stackless control-flow exception: the caller discards it and degrades to REFRESH. */
  private static final class PayloadTooLargeException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final PayloadTooLargeException INSTANCE = new PayloadTooLargeException();

    private PayloadTooLargeException() {
      super("put payload exceeds put-max-bytes", null, false, false);
    }
  }

  /**
   * A byte-array output stream that refuses to grow past {@code limit}. Only
   * {@link #write(int)} and {@link #write(byte[], int, int)} need the guard:
   * {@link ByteArrayOutputStream#writeBytes(byte[])} delegates to the latter, so
   * overriding it would check every write twice.
   */
  private static final class BoundedByteArrayOutputStream extends ByteArrayOutputStream {

    private final int limit;
    private boolean exceeded;

    BoundedByteArrayOutputStream(int limit) {
      super(Math.min(limit, 1024));
      this.limit = limit;
    }

    @Override
    public void write(int b) {
      guard(1);
      super.write(b);
    }

    @Override
    public void write(byte@NonNull [] b, int off, int len) {
      guard(len);
      super.write(b, off, len);
    }

    private void guard(int len) {
      if (count + len > limit) {
        exceeded = true;
        throw PayloadTooLargeException.INSTANCE;
      }
    }
  }
}
