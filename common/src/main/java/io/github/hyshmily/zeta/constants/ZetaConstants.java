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
package io.github.hyshmily.zeta.constants;

import io.github.hyshmily.zeta.util.Internal;

/**
 * Shared constants used across the Zeta library, organized by category.
 *
 * <p>Each nested interface groups related constants for improved discoverability.
 */
@Internal
public interface ZetaConstants {
  /** Logged at DEBUG when a sync operation is skipped because no publisher is available. */
  String NO_SYNC_PUBLISHER_MESSAGE = "No sync publisher found, please enable zeta.sync";

  /** Increment value applied to the local TopK frequency counter on each access. */
  int TOPK_INCR = 1;

  /** AMQP message header keys. */
  @Internal
  interface Amqp {
    /** Message type discriminator header (sync operation, HOT/COOL decision, or heartbeat type). */
    String HEADER_TYPE = "type";
    /** Data version header. */
    String HEADER_VERSION = "version";
    /** Origin node identifier. */
    String HEADER_NODE_ID = "nodeId";
    /**
     * Sending application's appName on decision, sync and heartbeat broadcasts (design
     * decisions: sync messages). Delivery isolation comes from the binding key
     * {@code send.<appName>} on the decision exchange; this header is the receiver's own
     * boundary check, so a queue bound to the wrong key still drops another application's
     * decisions instead of importing them. Absent on messages from older senders (rolling
     * upgrade); an absent appName is always processed.
     */
    String HEADER_APP_NAME = "appName";
    /** Worker epoch (restart generation counter). */
    String HEADER_EPOCH = "epoch";
    /** Heartbeat epoch (state machine generation). */
    String HEADER_HEARTBEAT_EPOCH = "hbEpoch";
    /** Whether the sender is ready to serve. */
    String HEADER_HEARTBEAT_READY = "hbReady";
    /** Time-sortable wall-clock value used as the message trace ID. */
    String HEADER_MESSAGE_ID = "messageId";
    /**
     * Publishing app instance's {@code NodeIdentity#getInstanceId()} instance ID on
     * instance-to-instance sync messages (design decisions: dedup lifecycle). Receivers drop
     * their own REFRESH, PUT and clear/batch INVALIDATE broadcasts — the sender already
     * applied them — while a plain self-INVALIDATE is still processed (it heals
     * invalidate-vs-reload repopulations stamped with older versions). Absent on messages
     * from older senders (rolling upgrade); an absent origin is always processed.
     */
    String HEADER_ORIGIN_INSTANCE = "originInstance";
    /**
     * Sender's 10-bit {@code dataVersion} node id on instance-to-instance sync
     * messages. Receivers compare it with their own id to detect the
     * misconfiguration that equal stamps cannot converge (design decisions:
     * version stamps).
     */
    String HEADER_ORIGIN_NODE_ID = "originNodeId";
    /**
     * Real cache key carried in the AMQP header of {@code PUT} sync messages.
     * A PUT's body is the JSON value payload (not a bare key), so receivers
     * take the dispatch key from this header to keep per-key ordering on the
     * shared ordered dispatcher — and drop the sender's own PUTs the same way
     * self-REFRESH is dropped.
     */
    String HEADER_CACHE_KEY = "cacheKey";
  }

  /** Thread name prefixes for zeta thread pools. */
  @Internal
  interface Threads {
    /** General zeta operations. */
    String PREFIX_KEY = "zeta-";
    /** Shared scheduler pool. */
    String PREFIX_SCHEDULER = "zeta-scheduler";
  }

  /** Routing keys and queue name prefixes. */
  @Internal
  interface Routing {
    /**
     * Prefix for HOT/COOL decision routing keys broadcast from Worker to apps
     * (the key is the receiving app's name, appended after this prefix).
     */
    String PREFIX_DECISION = "send.";
    /** Prefix for reportToWorker messages from apps to worker. */
    String KEY_REPORT = "reportToWorker.";
    /** Prefix for heartbeat messages from workers to apps. */
    String KEY_HEARTBEAT = "heartbeat.";
    /** Queue name prefix for reportToWorker queues ({@code <appName>.<nodeId>} appended). */
    String QUEUE_PREFIX_REPORT = "zeta.reportToWorker.";
  }

  /** Exchange names for AMQP routing. */
  @Internal
  interface Exchange {
    /** App-to-Worker reportToWorker routing (direct). */
    String REPORT = "zeta.reportToWorker.exchange";
    /** Worker heartbeat broadcasts (topic). */
    String HEARTBEAT = "zeta.heartbeat.exchange";
    /** Worker HOT/COOL decision broadcasts (direct, bound per {@code send.<appName>}). */
    String BROADCAST = "zeta.send.exchange";
    /** Cross-instance cache sync broadcasts (fanout). */
    String SYNC = "zeta.sync.exchange";
  }

  /** Version tracking defaults. */
  @Internal
  interface Version {
    /**
     * The "unknown" data stamp: an entry with this stamp makes the weakest
     * claim and accepts any real (positive) stamp (design decisions: version stamps).
     */
    long VERSION_DEFAULT = 0L;
  }
}
