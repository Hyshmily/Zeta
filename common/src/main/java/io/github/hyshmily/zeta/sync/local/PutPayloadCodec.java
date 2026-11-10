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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import io.github.hyshmily.zeta.util.Internal;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serial;
import java.util.List;
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
 * writer stored. Deserialization enforces a polymorphic whitelist — JDK value types plus
 * {@code io.github.hyshmily.zeta.*} plus the configured
 * {@code zeta.sync.put-allowed-packages}; a business DTO outside it fails closed and the
 * receiver degrades to a versioned INVALIDATE.
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
   * @param allowedPackages extra class prefixes admitted by the polymorphic validator
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
        log.debug("Put payload serialization failed, degrading to REFRESH");
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

  private static ObjectMapper buildMapper(@Nullable List<String> allowedPackages) {
    BasicPolymorphicTypeValidator.Builder ptv = BasicPolymorphicTypeValidator.builder()
      .allowIfSubType(String.class)
      .allowIfSubType(Number.class)
      .allowIfSubType(Boolean.class)
      .allowIfSubType(Character.class)
      .allowIfSubType(CharSequence.class)
      .allowIfSubType("java.util.")
      .allowIfSubType("java.time.")
      .allowIfSubType("java.math.")
      .allowIfSubType("io.github.hyshmily.zeta.")
      .allowIfSubTypeIsArray();
    if (allowedPackages != null) {
      for (String pkg : allowedPackages) {
        if (pkg != null && !pkg.isBlank()) {
          ptv.allowIfSubType(pkg.trim());
        }
      }
    }
    return JsonMapper.builder()
      .activateDefaultTyping(ptv.build(), ObjectMapper.DefaultTyping.EVERYTHING, JsonTypeInfo.As.WRAPPER_ARRAY)
      .build();
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
      super(Math.min(limit, 256));
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
