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
package io.github.hyshmily.zeta.sync.worker;

import static io.github.hyshmily.zeta.cache.CacheKeysPolicy.invalidCacheKey;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.*;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Version.VERSION_DEFAULT;

import io.github.hyshmily.zeta.model.DecisionKind;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.util.amqp.AmqpMessageReader;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.jspecify.annotations.Nullable;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * One Worker HOT/COOL decision for a single cache key.
 *
 * <p>Each decision carries the Worker's monotonically increasing {@link DecisionStamp}: the
 * decision version orders decisions idempotently, while {@code nodeId} and {@code epoch}
 * partition the version space per Worker incarnation. This version space is orthogonal to the
 * application-level {@code dataVersion} (design decisions: version stamps).
 *
 * <p>The decision carries no data stamp: the Worker-path guard orders on the decision stamp
 * only, and {@code dataVersion} is allocated independently by the receiving node's stamp
 * clock (design decisions: version stamps).
 *
 * @param id       message trace identifier — cached wall-clock value from the originating
 *                 Worker; time-sortable but orders nothing and may coincide across messages
 *                 in one millisecond; 0L if the header was missing or non-numeric
 * @param cacheKey the affected cache key; must not be null
 * @param type     the decision type; never {@code null}
 * @param stamp    the Worker decision stamp (version, node id and epoch)
 */
public record WorkerDecision(long id, String cacheKey, WorkerType type, DecisionStamp stamp) {
  /** The type and the lease kind are one contract, not two independently writable facts. */
  public WorkerDecision {
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(stamp, "stamp must not be null");
    if ((type == WorkerType.HOT) != stamp.isHot()) {
      throw new IllegalArgumentException("WorkerDecision type and DecisionStamp kind disagree");
    }
  }

  /**
   * Deserializes a {@code WorkerDecision} from an AMQP message body and headers.
   *
   * @param msg the incoming AMQP message; must not be null
   * @return a parsed decision, or {@code null} if the body is empty, the cache key is
   *         invalid or the type header is unknown
   */
  public static @Nullable WorkerDecision from(Message msg) {
    return from(msg, WorkerType.parse(msg.getMessageProperties().getHeader(HEADER_TYPE)));
  }

  /**
   * Deserializes a message whose type was already parsed at the listener boundary, so the
   * header string is converted once per delivery.
   *
   * <p>This is the decision plane's validation boundary: a message that survives it is
   * orderable. A non-positive {@code decisionVersion} is unorderable and is dropped here,
   * before the body is decoded or the message enters the per-key dispatcher — the context
   * below assumes a positive stamp. A missing node id is accepted on arrival against a
   * named node: epoch and version counters are node-local and not comparable across nodes.
   *
   * @param msg  the incoming AMQP message; must not be null
   * @param type the parsed type; {@code null} for an unknown/missing header
   * @return the decoded decision, or {@code null} when the type is null, the decision
   *         version is non-positive, or the body or cache key is invalid
   */
  public static @Nullable WorkerDecision from(Message msg, @Nullable WorkerType type) {
    if (type == null) {
      return null;
    }
    byte[] body = msg.getBody();
    if (body == null || body.length == 0) {
      return null;
    }

    MessageProperties props = msg.getMessageProperties();
    // Header first: a non-positive version is unorderable, so decoding the body for it
    // would only allocate a key that no guard could ever apply.
    long decisionVersion = AmqpMessageReader.readLongHeader(props, HEADER_VERSION, VERSION_DEFAULT);
    if (decisionVersion <= 0L) {
      return null;
    }
    String cacheKey = new String(body, StandardCharsets.UTF_8);
    if (invalidCacheKey(cacheKey)) {
      return null;
    }
    String nodeId = AmqpMessageReader.readStringHeader(props, HEADER_NODE_ID);
    long epoch = AmqpMessageReader.readLongHeader(props, HEADER_EPOCH, 0L);
    long id = AmqpMessageReader.readLongHeader(props, HEADER_MESSAGE_ID, 0L);

    DecisionKind kind = type == WorkerType.HOT ? DecisionKind.HOT : DecisionKind.COOL;
    return new WorkerDecision(id, cacheKey, type, new DecisionStamp(decisionVersion, nodeId, epoch, kind));
  }
}
