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

import static io.github.hyshmily.zeta.cache.CacheKeysPolicy.invalidCacheKey;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.*;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Version.VERSION_DEFAULT;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.sync.worker.WorkerDecision;
import io.github.hyshmily.zeta.util.Internal;
import io.github.hyshmily.zeta.util.amqp.AmqpMessageReader;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * One cache-synchronization operation between peer application instances: an invalidation, a
 * refresh, a value-carrying put, or a table/namespace clear.
 *
 * <p>Unlike {@link WorkerDecision} (control-plane HOT/COOL decisions from Workers),
 * {@code SyncMessage} carries application-level data mutations to keep peer L1 caches coherent.
 * {@link SyncType#INVALIDATE_BATCH} is a transport wire frame rather than a {@code SyncMessage}
 * and is decoded via {@link #parseBatch(Message)} into per-key {@link SyncType#INVALIDATE}
 * messages before entering the dispatcher.
 *
 * <p>Every message carries a positive {@code dataVersion} compared via
 * {@link VersionGuard#shouldSkipForRefresh}; non-positive versions are unorderable and dropped at
 * the wire boundary.
 *
 * @param id       time-sortable trace identifier (wall-clock ms from publisher; {@code 0L} if absent)
 * @param cacheKey target cache key (body UTF-8 for single-key types, {@code HEADER_CACHE_KEY} for
 *                 PUT, prefix for {@link SyncType#INVALIDATE_PREFIX}, or {@link #CLEAR_ALL_BODY}
 *                 for {@link SyncType#INVALIDATE_ALL})
 * @param type     parsed operation from {@link ZetaConstants.Amqp#HEADER_TYPE}; never {@code null}
 *                 or {@link SyncType#INVALIDATE_BATCH}
 * @param version  positive node-local {@code dataVersion} (or {@code clearStamp} for clear types)
 * @param payload  raw serialized value bytes for {@link SyncType#PUT}, or {@code null} otherwise
 */
@Internal
public record SyncMessage(long id, String cacheKey, SyncType type, long version, byte@Nullable [] payload) {
  /** Fixed body marker for {@link SyncType#INVALIDATE_ALL} messages. */
  public static final String CLEAR_ALL_BODY = "*";

  /** Maximum decoded entries accepted in one {@link SyncType#INVALIDATE_BATCH} wire frame. */
  public static final int MAX_BATCH_ENTRIES = 1000;

  private static final int MIN_BATCH_BUFFER_BYTES = 32;
  private static final int ESTIMATED_BYTES_PER_ENTRY = 48;

  /** Shared Jackson factory for streaming {@link SyncType#INVALIDATE_BATCH} JSON frames. */
  private static final JsonFactory BATCH_JSON_FACTORY = new JsonFactory();

  /** Validates required fields and rejects the {@link SyncType#INVALIDATE_BATCH} frame type. */
  public SyncMessage {
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");
    Objects.requireNonNull(type, "type must not be null");
    if (type == SyncType.INVALIDATE_BATCH) {
      throw new IllegalArgumentException(
        "INVALIDATE_BATCH is a wire frame; decode it with SyncMessage.parseBatch(Message)"
      );
    }
  }

  /** Creates a sync message carrying no value payload. */
  public SyncMessage(long id, String cacheKey, SyncType type, long version) {
    this(id, cacheKey, type, version, null);
  }

  /**
   * Deserializes a single-operation {@link SyncMessage} from an AMQP message, parsing
   * {@link ZetaConstants.Amqp#HEADER_TYPE} automatically.
   *
   * @param msg the incoming AMQP message; must not be {@code null}
   * @return the decoded {@link SyncMessage}, or {@code null} if invalid, missing properties,
   *         unorderable, or a batch frame
   */
  public static @Nullable SyncMessage from(Message msg) {
    Objects.requireNonNull(msg, "msg must not be null");
    MessageProperties props = msg.getMessageProperties();
    return props == null ? null : from(msg, SyncType.parse(props.getHeader(HEADER_TYPE)));
  }

  /**
   * Deserializes a single-operation {@link SyncMessage} using a pre-parsed {@link SyncType}.
   *
   * @param msg  the incoming AMQP message; must not be {@code null}
   * @param type the pre-parsed type, or {@code null} if unknown/missing
   * @return the decoded {@link SyncMessage}, or {@code null} if malformed, empty, non-positive
   *         version, or a batch frame
   */
  public static @Nullable SyncMessage from(Message msg, @Nullable SyncType type) {
    Objects.requireNonNull(msg, "msg must not be null");
    if (type == null || type == SyncType.INVALIDATE_BATCH) {
      return null;
    }
    MessageProperties props = msg.getMessageProperties();
    byte[] body = msg.getBody();
    if (props == null || body == null || body.length == 0) {
      return null;
    }

    long version = AmqpMessageReader.readLongHeader(props, HEADER_VERSION, VERSION_DEFAULT);
    if (version <= 0L) {
      return null;
    }
    long id = AmqpMessageReader.readLongHeader(props, HEADER_MESSAGE_ID, 0L);
    return switch (type) {
      case PUT -> {
        // PUT carries the value in the body and its ordering key in HEADER_CACHE_KEY.
        String key = AmqpMessageReader.readStringHeader(props, HEADER_CACHE_KEY);
        yield (key == null || invalidCacheKey(key)) ? null : new SyncMessage(id, key, type, version, body);
      }
      case INVALIDATE_ALL ->
        // Reuse the shared constant rather than decoding the fixed "*" marker on every clear.
        new SyncMessage(id, CLEAR_ALL_BODY, type, version);
      case INVALIDATE, REFRESH, INVALIDATE_PREFIX -> {
        String key = new String(body, StandardCharsets.UTF_8);
        yield invalidCacheKey(key) ? null : new SyncMessage(id, key, type, version);
      }
      case INVALIDATE_BATCH -> null;
    };
  }

  /**
   * Decodes one {@link SyncType#INVALIDATE_BATCH} wire frame into per-key
   * {@link SyncType#INVALIDATE} messages plus the highest observed version stamp.
   *
   * @param msg the incoming AMQP batch-invalidation message; must not be {@code null}
   * @return the {@link ParsedBatch}, or {@code null} if empty, malformed, oversized, or containing
   *         no valid entries
   */
  public static @Nullable ParsedBatch parseBatch(Message msg) {
    Objects.requireNonNull(msg, "msg must not be null");
    byte[] body = msg.getBody();
    if (body == null || body.length == 0) {
      return null;
    }
    long id = AmqpMessageReader.readLongHeader(msg.getMessageProperties(), HEADER_MESSAGE_ID, 0L);
    try (JsonParser parser = BATCH_JSON_FACTORY.createParser(body)) {
      if (parser.nextToken() != JsonToken.START_ARRAY) {
        return null;
      }

      List<SyncMessage> entries = new ArrayList<>();
      int elementCount = 0;
      long maxVersion = 0L;
      JsonToken token;
      while ((token = parser.nextToken()) != JsonToken.END_ARRAY) {
        if (token != JsonToken.START_ARRAY || ++elementCount > MAX_BATCH_ENTRIES) {
          return null;
        }
        if (parser.nextToken() != JsonToken.VALUE_STRING) {
          return null;
        }
        String key = parser.getText();
        if (parser.nextToken() != JsonToken.VALUE_NUMBER_INT) {
          return null;
        }
        long version = parser.getLongValue();
        if (parser.nextToken() != JsonToken.END_ARRAY) {
          return null;
        }
        if (!invalidCacheKey(key) && version > 0L) {
          entries.add(new SyncMessage(id, key, SyncType.INVALIDATE, version));
          maxVersion = Math.max(maxVersion, version);
        }
      }

      // Reject trailing tokens after the closing array bracket.
      return (entries.isEmpty() || parser.nextToken() != null) ? null : new ParsedBatch(entries, maxVersion);
    } catch (IOException | RuntimeException e) {
      return null;
    }
  }

  /**
   * Serializes {@code entries} into a JSON array of two-element {@code [key, version]} arrays
   * matching {@link #parseBatch(Message)}.
   *
   * @param entries the non-null entries to encode
   * @return UTF-8 JSON bytes, or {@code null} if serialization fails (callers degrade to per-key)
   */
  public static byte@Nullable [] encodeBatchBody(List<Entry> entries) {
    Objects.requireNonNull(entries, "entries must not be null");
    // ~48 bytes per ["key",version] keeps a full chunk to at most one buffer growth.
    ByteArrayOutputStream out = new ByteArrayOutputStream(
      Math.max(MIN_BATCH_BUFFER_BYTES, entries.size() * ESTIMATED_BYTES_PER_ENTRY)
    );
    try (JsonGenerator generator = BATCH_JSON_FACTORY.createGenerator(out, JsonEncoding.UTF8)) {
      generator.writeStartArray();
      for (Entry entry : entries) {
        generator.writeStartArray();
        generator.writeString(entry.key());
        generator.writeNumber(entry.version());
        generator.writeEndArray();
      }
      generator.writeEndArray();
    } catch (IOException | RuntimeException e) {
      return null;
    }
    return out.toByteArray();
  }

  /**
   * Single key/version pair carried inside an {@link SyncType#INVALIDATE_BATCH} frame.
   *
   * @param key     the cache key to invalidate
   * @param version the positive invalidation data stamp
   */
  public record Entry(String key, long version) {}

  /**
   * Decoded per-key invalidation tasks and highest observed stamp from one batch frame.
   *
   * @param entries    non-empty immutable list of per-key {@link SyncType#INVALIDATE} messages
   * @param maxVersion maximum of the header stamp and all decoded entry versions
   */
  public record ParsedBatch(List<SyncMessage> entries, long maxVersion) {
    /** Defensively copies {@code entries} and rejects an empty batch. */
    public ParsedBatch {
      entries = List.copyOf(entries);
      if (entries.isEmpty()) {
        throw new IllegalArgumentException("a parsed batch must not be empty");
      }
    }
  }
}
