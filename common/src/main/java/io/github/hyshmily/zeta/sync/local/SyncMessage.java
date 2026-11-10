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
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hyshmily.zeta.constants.ZetaConstants;
import io.github.hyshmily.zeta.sync.worker.WorkerDecision;
import io.github.hyshmily.zeta.util.amqp.AmqpMessageReader;
import io.github.hyshmily.zeta.util.version.VersionGuard;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * One cache-synchronization operation between application instances: an invalidation, a
 * refresh, a value-carrying put, or a table/namespace clear.
 *
 * <p>This is the data-plane counterpart to {@link WorkerDecision}: while
 * {@code WorkerDecision} carries hot/cool lifecycle decisions from the Worker cluster,
 * {@code SyncMessage} carries application-level data mutation events (writes, evictions)
 * between peer instances to keep L1 caches coherent.
 *
 * <p><b>Message types:</b>
 * <ul>
 *   <li>{@link SyncType#INVALIDATE} — Remove a single key from the local cache</li>
 *   <li>{@link SyncType#REFRESH} — Reload a single key from the data source into the local cache</li>
 *   <li>{@link SyncType#PUT} — Deliver the writer's value directly: the body is the
 *       default-typed JSON value and the real cache key rides in the
 *       {@code HEADER_CACHE_KEY} header, so receivers deserialize the bytes in
 *       one step and never re-parse a key out of the value</li>
 *   <li>{@link SyncType#INVALIDATE_ALL} — Clear the whole cache: the body is the
 *       fixed {@link #CLEAR_ALL_BODY} marker, the version header carries the
 *       {@code clearStamp}; every entry at or below the stamp is removed
 *       (design decisions: invalidation, design decisions: cache table).</li>
 *   <li>{@link SyncType#INVALIDATE_PREFIX} — Clear one key namespace: the body is
 *       the physical key prefix (a namespace prefix plus its delimiter),
 *       the version header carries the scoped {@code clearStamp}; only entries
 *       whose key starts with the prefix are removed.</li>
 * </ul>
 *
 * <p>{@link SyncType#INVALIDATE_BATCH} is a wire frame, not a message: the listener
 * decodes it with {@link #parseBatch(Message)} into per-key {@link SyncType#INVALIDATE}
 * messages before the dispatcher sees anything. A {@code SyncMessage} therefore never
 * carries a batch type and never holds an untyped attachment.
 *
 * <p>Each message carries a {@code dataVersion} stamp; the sync receivers compare it
 * numerically via {@link VersionGuard#shouldSkipForRefresh}. A non-positive stamp is
 * unorderable and is dropped at the wire boundary.
 *
 * @param id       message trace identifier — cached wall-clock value from the
 *                 publishing instance; time-sortable, orders nothing, and may
 *                 coincide across messages in one millisecond; 0L if the header
 *                 was missing or non-numeric (a DEBUG line names a non-numeric value)
 * @param cacheKey the affected cache key (for single-key operations), the
 *                 {@link #CLEAR_ALL_BODY} marker (for {@link SyncType#INVALIDATE_ALL}),
 *                 the key prefix (for {@link SyncType#INVALIDATE_PREFIX}), or the real
 *                 cache key read from {@code HEADER_CACHE_KEY} (for {@link SyncType#PUT});
 *                 UTF-8 decoded from AMQP body bytes, except
 *                 {@link SyncType#INVALIDATE_ALL} which carries the shared
 *                 {@link #CLEAR_ALL_BODY} constant without decoding
 * @param type     the operation, parsed once from AMQP header
 *                 {@link ZetaConstants.Amqp#HEADER_TYPE}; never {@code null} and never
 *                 {@link SyncType#INVALIDATE_BATCH}
 * @param version  the application-level {@code dataVersion} at which the operation
 *                 occurred; a positive stamp allocated by the node-local
 *                 {@code StampClock} (design decisions: version stamps). For the two clear
 *                 types this is the {@code clearStamp}.
 * @param payload  the raw value payload of a {@link SyncType#PUT} message, or {@code null}
 *                 for every other type
 */
@Slf4j
public record SyncMessage(long id, String cacheKey, SyncType type, long version, byte@Nullable [] payload) {
  /** Body marker of a {@link SyncType#INVALIDATE_ALL} message. */
  public static final String CLEAR_ALL_BODY = "*";

  /**
   * Upper bound of decoded batch entries accepted per message — matches the
   * sender's chunk size, so an oversized (foreign or hostile) body is dropped
   * instead of exploding the dispatcher.
   */
  public static final int MAX_BATCH_ENTRIES = 1000;

  /** Plain mapper for the {@link SyncType#INVALIDATE_BATCH} body. */
  private static final ObjectMapper BATCH_MAPPER = new ObjectMapper();

  public SyncMessage {
    if (type == SyncType.INVALIDATE_BATCH) {
      throw new IllegalArgumentException(
        "INVALIDATE_BATCH is a wire frame; decode it with SyncMessage.parseBatch(Message)"
      );
    }
  }

  /**
   * One key/version pair of a {@link SyncType#INVALIDATE_BATCH} message.
   *
   * @param key     the cache key to invalidate
   * @param version the data stamp the invalidation was allocated at
   */
  public record Entry(String key, long version) {}

  /**
   * The per-key invalidations decoded from one {@link SyncType#INVALIDATE_BATCH} frame,
   * plus the highest stamp the frame carries. Each entry is already the
   * {@link SyncType#INVALIDATE} task the listener submits, so the receive path allocates
   * no second wrapper per key.
   *
   * @param entries    the decoded invalidations; never empty
   * @param maxVersion the highest of the header stamp and every entry version
   */
  public record ParsedBatch(List<SyncMessage> entries, long maxVersion) {
    public ParsedBatch {
      entries = List.copyOf(entries);
      if (entries.isEmpty()) {
        throw new IllegalArgumentException("a parsed batch must not be empty");
      }
    }
  }

  /** Convenience constructor for a message without a payload. */
  public SyncMessage(long id, String cacheKey, SyncType type, long version) {
    this(id, cacheKey, type, version, null);
  }

  /**
   * This message at another version, keeping its type and payload as they are.
   *
   * @param newVersion the replacement version
   * @return this message when the version already matches, otherwise a copy
   */
  public SyncMessage withVersion(long newVersion) {
    return newVersion == version ? this : new SyncMessage(id, cacheKey, type, newVersion, payload);
  }

  /**
   * This message as a plain INVALIDATE at the same version: a clear keeps its
   * type, an INVALIDATE is returned as is, and any other type drops its
   * payload (a PUT payload has no meaning on an invalidation).
   *
   * @return this message or its INVALIDATE form
   */
  public SyncMessage asInvalidate() {
    return type.isClear() || type == SyncType.INVALIDATE
      ? this
      : new SyncMessage(id, cacheKey, SyncType.INVALIDATE, version, null);
  }

  /**
   * Deserializes a {@code SyncMessage} from an AMQP message body and headers.
   *
   * <p>Deserialization rules:
   * <ul>
   *   <li>For key-carrying types the cache key is the message body (UTF-8
   *       decoded).</li>
   *   <li>For {@link SyncType#PUT} the body is the typed value payload, kept
   *       verbatim in {@link #payload()} — it is never decoded to a
   *       {@code String} — and the real cache key is read from the
   *       {@link ZetaConstants.Amqp#HEADER_CACHE_KEY} header, which also
   *       preserves per-key ordering on the shared dispatcher.</li>
   *   <li>The type is read from the {@link ZetaConstants.Amqp#HEADER_TYPE} header.</li>
   *   <li>The {@code dataVersion} is read from the {@code HEADER_VERSION} header. A
   *       non-positive version is unorderable, so the message is dropped here, at the
   *       boundary — every receiver below assumes a positive stamp.</li>
   *   <li>For {@link SyncType#INVALIDATE_ALL}, the body is the fixed
   *       {@link #CLEAR_ALL_BODY} marker and is not decoded: the record carries the
   *       shared constant.</li>
   *   <li>For {@link SyncType#INVALIDATE_PREFIX}, the body is the key prefix.</li>
   * </ul>
   *
   * @param msg the incoming AMQP message; must not be null
   * @return a parsed {@link SyncMessage}, or {@code null} if the body is empty,
   *         the data version is non-positive, the key/prefix is invalid, a PUT
   *         carries no cache-key header, or the type is a batch frame —
   *         callers must skip such messages rather than treat them as errors
   */
  public static SyncMessage from(Message msg) {
    MessageProperties props = msg.getMessageProperties();
    return from(msg, SyncType.parse(props.getHeader(HEADER_TYPE)));
  }

  /**
   * Deserializes a message whose type was already parsed at the listener boundary, so
   * the header string is converted once per delivery.
   *
   * @param msg  the incoming AMQP message; must not be null
   * @param type the parsed type; {@code null} for an unknown/missing header
   * @return the decoded message, or {@code null} when it is not applicable. The listener
   *         owns the unknown-type warning and the batch-frame branch, so this method stays
   *         a pure single-message decoder.
   */
  public static SyncMessage from(Message msg, @Nullable SyncType type) {
    if (type == null || type == SyncType.INVALIDATE_BATCH) {
      return null;
    }
    byte[] body = msg.getBody();
    if (body == null || body.length == 0) {
      return null;
    }

    MessageProperties props = msg.getMessageProperties();
    long version = AmqpMessageReader.readLongHeader(props, HEADER_VERSION, VERSION_DEFAULT);
    if (version <= 0L) {
      return null;
    }
    long id = AmqpMessageReader.readLongHeader(props, HEADER_MESSAGE_ID, 0L);
    return switch (type) {
      case PUT -> {
        // The body is the value payload, not a cache key: the key rides in its
        // own header. Without it the per-key ordering guarantee is lost and
        // there is no key to apply under, so the message is dropped — the next
        // write or the TTL self-heals.
        String key = AmqpMessageReader.readStringHeader(props, HEADER_CACHE_KEY);
        yield (key == null || invalidCacheKey(key)) ? null : new SyncMessage(id, key, type, version, body);
      }
      case INVALIDATE_ALL -> {
        // The body is the fixed marker, never a key: reuse the shared constant instead
        // of decoding the bytes into a second equal String on every clear delivery.
        yield new SyncMessage(id, CLEAR_ALL_BODY, type, version);
      }
      case INVALIDATE, REFRESH, INVALIDATE_PREFIX -> {
        String key = new String(body, StandardCharsets.UTF_8);
        yield invalidCacheKey(key) ? null : new SyncMessage(id, key, type, version);
      }
      case INVALIDATE_BATCH -> null;
    };
  }

  /**
   * Decode one {@link SyncType#INVALIDATE_BATCH} wire frame into its per-key
   * {@link SyncType#INVALIDATE} messages. The frame is a transport envelope: the
   * dispatcher never sees it.
   *
   * @param msg the incoming AMQP message; must not be null
   * @return the decoded entries and their highest stamp, or {@code null} when the body is
   *         empty, malformed, oversized, or carries no valid entry
   */
  public static @Nullable ParsedBatch parseBatch(Message msg) {
    byte[] body = msg.getBody();
    if (body == null || body.length == 0) {
      return null;
    }

    MessageProperties props = msg.getMessageProperties();
    long id = AmqpMessageReader.readLongHeader(props, HEADER_MESSAGE_ID, 0L);
    long headerVersion = AmqpMessageReader.readLongHeader(props, HEADER_VERSION, VERSION_DEFAULT);
    ParsedBatch batch = parseBatchBody(body, id);
    if (batch == null) {
      return null;
    }
    return headerVersion > batch.maxVersion() ? new ParsedBatch(batch.entries(), headerVersion) : batch;
  }

  /**
   * Parse a {@link SyncType#INVALIDATE_BATCH} body: a JSON array of
   * {@code [key, version]} two-element arrays. Entries with a null/blank key,
   * a non-numeric version or a non-positive version are skipped (unorderable);
   * an empty result, malformed JSON or more than
   * {@link #MAX_BATCH_ENTRIES} elements returns {@code null} (drop the message).
   *
   * <p>Streaming, not a DOM: the body can be tens of kilobytes of short
   * arrays, and a tree would allocate one node object per token just to be
   * discarded. The highest accepted entry version is collected in the same pass
   * so the caller does not rescan the list. Each accepted pair is materialised
   * directly as the {@code INVALIDATE} task the listener will submit, so the
   * receive path never allocates a second wrapper per key.
   *
   * @param body the raw UTF-8 body
   * @param id   the parent message's trace id, copied to every decoded entry
   * @return the decoded invalidations and their maximum version, or {@code null}
   *         when the body is unusable
   */
  private static ParsedBatch parseBatchBody(byte[] body, long id) {
    try (JsonParser parser = BATCH_MAPPER.getFactory().createParser(body)) {
      if (parser.nextToken() != JsonToken.START_ARRAY) {
        return null;
      }

      List<SyncMessage> entries = new ArrayList<>();
      int elementCount = 0;
      long maxVersion = 0L;
      JsonToken token;
      while ((token = parser.nextToken()) != JsonToken.END_ARRAY) {
        if (token == null) {
          return null;
        }
        if (++elementCount > MAX_BATCH_ENTRIES) {
          log.debug("Dropping INVALIDATE_BATCH over the {}-entry bound", MAX_BATCH_ENTRIES);
          return null;
        }
        if (token != JsonToken.START_ARRAY) {
          parser.skipChildren();
          continue;
        }
        if (parser.nextToken() != JsonToken.VALUE_STRING) {
          skipEntry(parser);
          continue;
        }
        String key = parser.getText();
        if (parser.nextToken() != JsonToken.VALUE_NUMBER_INT) {
          skipEntry(parser);
          continue;
        }
        long version = parser.getLongValue();
        if (parser.nextToken() != JsonToken.END_ARRAY) {
          skipEntry(parser);
          continue;
        }
        if (!invalidCacheKey(key) && version > 0L) {
          entries.add(new SyncMessage(id, key, SyncType.INVALIDATE, version));
          maxVersion = Math.max(maxVersion, version);
        }
      }

      return (entries.isEmpty() || parser.nextToken() != null) ? null : new ParsedBatch(entries, maxVersion);
    } catch (IOException | RuntimeException e) {
      log.debug("Dropping malformed INVALIDATE_BATCH body", e);
      return null;
    }
  }

  /**
   * Skip the rest of one malformed batch entry, stopping after its closing
   * {@code ]}. The outer loop expects the next token to be the next entry's
   * {@code [} or the body's closing {@code ]}.
   *
   * <p>The current token is part of the state: an element that ended early
   * (for example {@code []} or {@code ["key"]}) already has its entry
   * {@code END_ARRAY} as the current token, and consuming the next token would
   * swallow the following entry. A nested structure is skipped as a whole
   * before looking for the entry's closing bracket.
   */
  private static void skipEntry(JsonParser parser) throws IOException {
    JsonToken current = parser.currentToken();
    if (current == JsonToken.END_ARRAY) {
      return;
    }
    if (current != null && current.isStructStart()) {
      parser.skipChildren();
    }
    JsonToken token;
    while ((token = parser.nextToken()) != null) {
      if (token == JsonToken.END_ARRAY) {
        return;
      }
      if (token.isStructStart()) {
        parser.skipChildren();
      }
    }
  }

  /**
   * Serialize the {@link SyncType#INVALIDATE_BATCH} body for the given entries —
   * the exact counterpart of {@link #parseBatchBody}. Entries are emitted as
   * two-element {@code [key, version]} arrays.
   *
   * <p>Written through one {@link JsonGenerator} straight into the body buffer, so no
   * per-entry intermediate String is built and the text is copied once.
   *
   * @param entries the entries (never {@code null}/empty)
   * @return the JSON body bytes, or {@code null} when serialization failed
   *         (callers degrade to per-key sends)
   */
  public static byte[] encodeBatchBody(List<Entry> entries) {
    ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(32, entries.size() * 16));
    try (JsonGenerator generator = BATCH_MAPPER.createGenerator(out, JsonEncoding.UTF8)) {
      generator.writeStartArray();
      for (Entry entry : entries) {
        generator.writeStartArray();
        generator.writeString(entry.key());
        generator.writeNumber(entry.version());
        generator.writeEndArray();
      }
      generator.writeEndArray();
    } catch (IOException | RuntimeException e) {
      log.debug("INVALIDATE_BATCH body encoding failed for {} entries", entries.size(), e);
      return null;
    }
    return out.toByteArray();
  }
}
