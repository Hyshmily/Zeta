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
import io.github.hyshmily.zeta.sync.worker.WorkerMessage;
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
 * Message between application instances for cache synchronization, carrying an
 * invalidation, refresh, or value-carrying put operation. Travels via the
 * {@code zeta.sync.exchange} FanoutExchange.
 *
 * <p>This is the data-plane counterpart to {@link WorkerMessage}: while
 * {@code WorkerMessage} carries hot/cool lifecycle decisions from the Worker cluster,
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
 *   <li>{@link SyncType#INVALIDATE_BATCH} — Remove up to
 *       {@link #MAX_BATCH_ENTRIES} keys in one message: the body is a compact
 *       JSON array of {@code [key, version]} pairs (design decisions: invalidation). The
 *       {@code batch} field carries the decoded {@link SyncType#INVALIDATE} sub-messages.</li>
 * </ul>
 *
 * <p>Each single-key message carries a {@code dataVersion} stamp; the sync receivers
 * compare it numerically via {@link VersionGuard#shouldSkipForRefresh}. A
 * non-positive stamp is unorderable and is dropped.
 *
 * @param id                message trace identifier — cached wall-clock value from the
 *                          publishing instance; time-sortable, orders nothing, and may
 *                          coincide across messages in one millisecond; 0L if the header
 *                          was missing or non-numeric (a DEBUG line names a
 *                          non-numeric value)
 * @param cacheKey          the affected cache key (for single-key operations), the
 *                          {@link #CLEAR_ALL_BODY} marker (for
 *                          {@link SyncType#INVALIDATE_ALL}), the key prefix (for
 *                          {@link SyncType#INVALIDATE_PREFIX}), the real cache key
 *                          read from {@code HEADER_CACHE_KEY} (for {@link SyncType#PUT}),
 *                          the empty string for {@link SyncType#INVALIDATE_BATCH}
 *                          (the payload lives in {@link #batch()});
 *                          UTF-8 decoded from AMQP body bytes, except
 *                          {@link SyncType#INVALIDATE_ALL} which carries the shared
 *                          {@link #CLEAR_ALL_BODY} constant without decoding
 * @param type              the operation, parsed once from AMQP header
 *                          {@link ZetaConstants.Amqp#HEADER_TYPE}; never {@code null}
 * @param version           the application-level {@code dataVersion} at which the operation
 *                          occurred; a positive stamp allocated by the node-local
 *                          {@code StampClock} (design decisions: version stamps); VERSION_DEFAULT (0) when the
 *                          AMQP header is missing or non-numeric. For the two clear types
 *                          this is the {@code clearStamp}; for
 *                          {@link SyncType#INVALIDATE_BATCH} it is the higher of the header
 *                          stamp and the highest entry version.
 * @param attachment        the decoded {@link SyncType#INVALIDATE} sub-messages for a
 *                          {@link SyncType#INVALIDATE_BATCH} message, or the raw AMQP body of a
 *                          {@link SyncType#PUT} message, or
 *                          {@code null} for every other type. The two views are mutually
 *                          exclusive, so one reference carries both; {@link #batch()} and
 *                          {@link #payload()} expose the typed accessors.
 */
@Slf4j
public record SyncMessage(long id, String cacheKey, SyncType type, long version, @Nullable Object attachment) {
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

  /**
   * One key/version pair of a {@link SyncType#INVALIDATE_BATCH} message.
   *
   * @param key     the cache key to invalidate
   * @param version the data stamp the invalidation was allocated at
   */
  public record Entry(String key, long version) {}

  /**
   * The decoded per-key invalidations of an {@link SyncType#INVALIDATE_BATCH} message.
   * Each entry is already a submittable {@link SyncType#INVALIDATE} message, so the
   * receiver does not build a second wrapper per key.
   *
   * @return the decoded invalidations, or {@code null} for every other type
   */
  @SuppressWarnings("unchecked")
  public @Nullable List<SyncMessage> batch() {
    return attachment instanceof List<?> list ? (List<SyncMessage>) list : null;
  }

  /**
   * The raw value payload of a {@link SyncType#PUT} message.
   *
   * @return the payload bytes, or {@code null} for every other type
   */
  public byte@Nullable [] payload() {
    return attachment instanceof byte[] bytes ? bytes : null;
  }

  /** Convenience constructor for a message without an attachment. */
  public SyncMessage(long id, String cacheKey, SyncType type, long version) {
    this(id, cacheKey, type, version, null);
  }

  /**
   * This message at another version, keeping its type and attachment as they are.
   *
   * @param newVersion the replacement version
   * @return this message when the version already matches, otherwise a copy
   */
  public SyncMessage withVersion(long newVersion) {
    return newVersion == version ? this : new SyncMessage(id, cacheKey, type, newVersion, attachment);
  }

  /**
   * This message as a plain INVALIDATE at the same version: a clear keeps its
   * type, an INVALIDATE is returned as is, and any other type drops its
   * attachment (a PUT payload has no meaning on an invalidation).
   *
   * @return this message or its INVALIDATE form
   */
  public SyncMessage asInvalidate() {
    return type.isClear() || type == SyncType.INVALIDATE
      ? this
      : new SyncMessage(id, cacheKey, SyncType.INVALIDATE, version, null);
  }

  /**
   * The accepted per-key invalidations of a batch body and their highest version.
   *
   * @param entries    the decoded invalidations, never empty
   * @param maxVersion the highest entry version
   */
  private record ParsedBatch(List<SyncMessage> entries, long maxVersion) {}

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
   *       boundary — every receiver below assumes a positive stamp. A
   *       {@link SyncType#INVALIDATE_BATCH} message derives its version from its entries
   *       instead (the header is absent on that wire shape).</li>
   *   <li>For {@link SyncType#INVALIDATE_ALL}, the body is the fixed
   *       {@link #CLEAR_ALL_BODY} marker and is not decoded: the record carries the
   *       shared constant.</li>
   *   <li>For {@link SyncType#INVALIDATE_PREFIX}, the body is the key prefix.</li>
   *   <li>For {@link SyncType#INVALIDATE_BATCH}, the body is parsed into
   *       {@link #batch()} — a malformed or oversized body yields {@code null}
   *       (the message is dropped, one DEBUG line) — and the record's
   *       version is the highest stamp the message carries.</li>
   * </ul>
   *
   * @param msg the incoming AMQP message; must not be null
   * @return a parsed {@link SyncMessage}, or {@code null} if the body is empty,
   *         the data version is non-positive, the key/prefix is invalid, a PUT
   *         carries no cache-key header, or the batch body cannot be parsed —
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
   *         owns the unknown-type warning, so this method stays a pure data decoder.
   */
  public static SyncMessage from(Message msg, @Nullable SyncType type) {
    byte[] body = msg.getBody();
    if (body == null || body.length == 0) {
      return null;
    }

    MessageProperties props = msg.getMessageProperties();
    if (type == null) {
      return null;
    }

    long version = AmqpMessageReader.readLongHeader(props, HEADER_VERSION, VERSION_DEFAULT);
    // A batch derives its stamp from its entries, so its header is legitimately absent.
    if (version <= 0L && type != SyncType.INVALIDATE_BATCH) {
      return null;
    }
    long id = AmqpMessageReader.readLongHeader(props, HEADER_MESSAGE_ID, 0L);
    switch (type) {
      case PUT -> {
        // The body is the value payload, not a cache key: the key rides in its
        // own header. Without it the per-key ordering guarantee is lost and
        // there is no key to apply under, so the message is dropped — the next
        // write or the TTL self-heals.
        String key = AmqpMessageReader.readStringHeader(props, HEADER_CACHE_KEY);
        return (key == null || invalidCacheKey(key)) ? null : new SyncMessage(id, key, type, version, body);
      }
      case INVALIDATE_BATCH -> {
        // The body is a JSON array, not a cache key: parse it directly from the
        // bytes so the receiver never materialises the whole body as a String
        // or builds a JsonNode tree for up to MAX_BATCH_ENTRIES entries.
        ParsedBatch batch = parseBatchBody(body, id);
        if (batch == null) {
          return null;
        }
        // The message's data version is the highest stamp it carries: the batch
        // has no single stamp of its own, and the receiver merges exactly this
        // maximum into the local clock.
        long batchVersion = Math.max(version, batch.maxVersion());
        return new SyncMessage(id, "", type, batchVersion, batch.entries());
      }
      case INVALIDATE_ALL -> {
        // The body is the fixed marker, never a key: reuse the shared constant instead
        // of decoding the bytes into a second equal String on every clear delivery.
        return new SyncMessage(id, CLEAR_ALL_BODY, type, version);
      }
      case INVALIDATE, REFRESH, INVALIDATE_PREFIX -> {
        String key = new String(body, StandardCharsets.UTF_8);
        return invalidCacheKey(key) ? null : new SyncMessage(id, key, type, version);
      }
    }
    return null;
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
