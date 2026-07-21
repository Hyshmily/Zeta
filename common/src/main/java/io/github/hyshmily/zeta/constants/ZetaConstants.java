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

import io.github.hyshmily.zeta.Internal;

/**
 * Shared constants used across the HotKey library, organized by category.
 *
 * <p>Each nested interface groups related constants for improved discoverability.
 */
@Internal
@SuppressWarnings("all")
public interface ZetaConstants {
  /** AMQP message header keys. */
  @Internal
  interface Amqp {
    /** Message type header (SYNC/REPORT/WORKER). */
    String HEADER_TYPE = "type";
    /** Data version header. */
    String HEADER_VERSION = "version";
    /** Indicates whether the version is degraded. */
    String HEADER_IS_VERSION_DEGRADED = "isVersionDegraded";
    /** Origin node identifier. */
    String HEADER_NODE_ID = "nodeId";
    /** Message creation timestamp. */
    String HEADER_TIMESTAMP = "timestamp";
    /** Worker epoch (restart generation counter). */
    String HEADER_EPOCH = "epoch";
    /** Heartbeat epoch (state machine generation). */
    String HEADER_HEARTBEAT_EPOCH = "hbEpoch";
    /** Sender's current load metric. */
    String HEADER_HEARTBEAT_LOAD = "hbLoad";
    /** Whether the sender is ready to serve. */
    String HEADER_HEARTBEAT_READY = "hbReady";
    /** Sender's config fingerprint for change detection. */
    String HEADER_HEARTBEAT_CONFIG_FP = "hbConfigFp";
    /** Sender's data version high-water mark. */
    String HEADER_HEARTBEAT_DV_HWM = "hbDvHwm";
    /** Sender's config confirm count. */
    String HEADER_HEARTBEAT_CONFIG_CONFIRM = "hbConfigConfirm";
    /** Sender's config cool count. */
    String HEADER_HEARTBEAT_CONFIG_COOL = "hbConfigCool";
    /** Sender's config grace count. */
    String HEADER_HEARTBEAT_CONFIG_GRACE = "hbConfigGrace";
    /** Sender's config timestamp. */
    String HEADER_HEARTBEAT_CONFIG_TIMESTAMP = "hbConfigTs";
    /** Verification message type (PING or PONG). */
    String HEADER_VERIFY_TYPE = "verifyType";
    /** Originating app instance identifier. */
    String HEADER_VERIFY_APP_INSTANCE = "verifyAppInstance";
    /** Responding worker identifier. */
    String HEADER_VERIFY_WORKER_ID = "verifyWorkerId";
    /** Verification request type. */
    String HEADER_VERIFY_PING = "PING";
    /** Verification response type. */
    String HEADER_VERIFY_PONG = "PONG";
    /** Rule set version (rulesVersion). */
    String HEADER_RULES_VERSION = "rulesVersion";
    /** Snowflake message ID for end-to-end tracing. */
    String HEADER_MESSAGE_ID = "messageId";
  }

  /** Thread name prefixes for zeta thread pools. */
  @Internal
  interface Thread {
    /** General zeta operations. */
    String PREFIX_HOTKEY = "zeta-";
    /** Cross-instance sync tasks. */
    String PREFIX_SYNC = "zeta-sync";
    /** Worker-mode tasks. */
    String PREFIX_WORKER = "zeta-worker";
    /** Report submission tasks. */
    String PREFIX_REPORT = "zeta-reportToWorker";
    /** Shared scheduler pool. */
    String PREFIX_SCHEDULER = "zeta-scheduler";
  }

  /** Routing keys and queue name prefixes. */
  @Internal
  interface Routing {
    /** Prefix for send decisions (HOT/COOL) from worker to apps. */
    String KEY_BROADCAST = "send.";
    /** Prefix for reportToWorker messages from apps to worker. */
    String KEY_REPORT = "reportToWorker.";
    /** Prefix for heartbeat messages from workers to apps. */
    String KEY_HEARTBEAT = "heartbeat.";
    /** Queue name prefix for reportToWorker queues (appended with app name). */
    String QUEUE_PREFIX_REPORT = "zeta.reportToWorker.";
  }

  /** Source identifiers for decision origins. */
  @Internal
  interface Source {
    /** Sliding-window-based decisions. */
    String SLIDING_WINDOW = "sliding_window";
    /** TopK pre-warm validation based decisions. */
    String TOPK_PRE_WARM = "topk_pre_warm";
  }

  /** Redis key prefixes and keys. */
  @Internal
  interface Redis {
    /** Prefix for version tracking entries. */
    String VERSION_KEY_PREFIX = "zeta:ver:";
    /** Key for the dynamic rules hash (blacklist / whitelist entries). */
    String KEY_RULES = "zeta:rules";
    /** Prefix for distributed lock entries. */
    String LOCK_KEY_PREFIX = "zeta:lock:";
  }

  /** Exchange names for AMQP routing. */
  @Internal
  interface Exchange {
    /** App-to-Worker reportToWorker routing (topic). */
    String REPORT = "zeta.reportToWorker.exchange";
    /** Worker heartbeat broadcasts (topic). */
    String HEARTBEAT = "zeta.heartbeat.exchange";
    /** Worker HOT/COOL decision broadcasts (fanout). */
    String BROADCAST = "zeta.send.exchange";
    /** Cross-instance cache sync broadcasts (fanout). */
    String SYNC = "zeta.sync.exchange";
  }

  /** Version tracking defaults. */
  @Internal
  interface Version {
    /** Default initial version value when no prior version exists. */
    long VERSION_DEFAULT = 0L;
  }

  /** Increment value applied to the local TopK frequency counter on each access. */
  int TOPK_INCR = 1;

  /** Warning message logged when sync publisher is not available. */
  String NO_SYNC_PUBLISHER = "No sync publisher found, please enable zeta.sync";
}
