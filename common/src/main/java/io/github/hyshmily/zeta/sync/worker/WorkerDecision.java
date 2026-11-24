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
import static io.github.hyshmily.zeta.cache.CacheKeysPolicy.logicalNamespaceOf;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Amqp.*;
import static io.github.hyshmily.zeta.constants.ZetaConstants.Version.VERSION_DEFAULT;

import io.github.hyshmily.zeta.model.DecisionKind;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.util.amqp.AmqpMessageReader;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

/**
 * One Worker HOT/COOL lifecycle decision for a single cache key.
 *
 * <p>Each decision carries the originating Worker's monotonic {@link DecisionStamp}: the decision
 * version orders decisions idempotently while {@code nodeId} and {@code epoch} partition the
 * version space per Worker incarnation. This control-plane version space is orthogonal to the
 * application-level {@code dataVersion} allocated locally by the receiving node's stamp clock.
 *
 * @param id       time-sortable message trace identifier (originating Worker wall-clock ms;
 *                 {@code 0L} when missing or non-numeric)
 * @param cacheKey the affected physical cache key; never {@code null}
 * @param stamp    the Worker decision stamp (version, node id, epoch, and {@link DecisionKind})
 */
@Slf4j
public record WorkerDecision(long id, String cacheKey, DecisionStamp stamp) {
  /** Validates that {@code cacheKey} and {@code stamp} are non-null. */
  public WorkerDecision {
    Objects.requireNonNull(cacheKey, "cacheKey must not be null");
    Objects.requireNonNull(stamp, "stamp must not be null");
  }

  /** Returns the decision operation from {@link #stamp()}. */
  public DecisionKind kind() {
    return stamp.kind();
  }

  /** Returns {@code true} if this decision grants a HOT lease. */
  public boolean isHot() {
    return stamp.isHot();
  }

  /**
   * The logical name of the affected key's namespace, with its encoding delimiter removed:
   * {@code users} for both {@code users\u0000u-1} (TYPED) and {@code users:u-1} (PLAIN). A
   * listener can tag or partition by namespace without parsing the physical key.
   *
   * @return the namespace name, or {@code null} when the key carries none
   */
  public @Nullable String namespace() {
    return logicalNamespaceOf(cacheKey);
  }

  /**
   * Deserializes a {@link WorkerDecision} from an AMQP message.
   *
   * <p>Serves as the decision plane's validation boundary: the type header is parsed once here, and
   * a non-positive {@code decisionVersion} is dropped before the body is decoded into a key, so
   * downstream handlers only observe positive, orderable stamps.
   *
   * @param msg the incoming AMQP message; must not be {@code null}
   * @return the decoded decision, or {@code null} if properties are missing, the type header is
   *         unknown, the version is non-positive, or the body/key is invalid
   */
  public static @Nullable WorkerDecision from(Message msg) {
    Objects.requireNonNull(msg, "msg must not be null");
    MessageProperties props = msg.getMessageProperties();
    if (props == null) {
      return null;
    }
    Object typeHeader = props.getHeader(HEADER_TYPE);
    DecisionKind kind = DecisionKind.parse(typeHeader);
    if (kind == null) {
      AmqpMessageReader.logUnknownType(log, "worker decision", typeHeader, msg);
      return null;
    }
    byte[] body = msg.getBody();
    if (body == null || body.length == 0) {
      return null;
    }

    // Validate the version header first so an unorderable message avoids UTF-8 key allocation.
    long decisionVersion = AmqpMessageReader.readLongHeader(props, HEADER_VERSION, VERSION_DEFAULT);
    if (decisionVersion <= 0L) {
      return null;
    }
    String cacheKey = new String(body, StandardCharsets.UTF_8);
    if (invalidCacheKey(cacheKey)) {
      return null;
    }
    return new WorkerDecision(
      AmqpMessageReader.readLongHeader(props, HEADER_MESSAGE_ID, 0L),
      cacheKey,
      new DecisionStamp(
        decisionVersion,
        AmqpMessageReader.readStringHeader(props, HEADER_NODE_ID),
        AmqpMessageReader.readLongHeader(props, HEADER_EPOCH, 0L),
        kind
      )
    );
  }
}
