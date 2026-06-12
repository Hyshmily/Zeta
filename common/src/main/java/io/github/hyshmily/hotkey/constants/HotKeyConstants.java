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
package io.github.hyshmily.hotkey.constants;

/**
 * Shared constants used across the HotKey library.
 *
 * <p>Groups include AMQP message header names, thread name prefixes,
 * routing keys, queue prefixes, and version tracking defaults.
 */
public final class HotKeyConstants {

  /** Utility class — do not instantiate. */
  private HotKeyConstants() {}

  /** AMQP message header key for the message type (SYNC/REPORT/WORKER). */
  public static final String AMQP_HEADER_TYPE = "type";
  /** AMQP message header key for the data version. */
  public static final String AMQP_HEADER_VERSION = "version";
  /** AMQP message header key indicating whether the version is degraded. */
  public static final String AMQP_HEADER_IS_VERSION_DEGRADED = "isVersionDegraded";
  /** AMQP message header key for the origin node identifier. */
  public static final String AMQP_HEADER_NODE_ID = "nodeId";
  /** AMQP message header key for the message creation timestamp. */
  public static final String AMQP_HEADER_TIMESTAMP = "timestamp";

  /** AMQP message header key for the heartbeat epoch (state machine generation). */
  public static final String AMQP_HEADER_HEARTBEAT_EPOCH = "hbEpoch";
  /** AMQP message header key for the sender's current load metric. */
  public static final String AMQP_HEADER_HEARTBEAT_LOAD = "hbLoad";
  /** AMQP message header key indicating whether the sender is ready to serve. */
  public static final String AMQP_HEADER_HEARTBEAT_READY = "hbReady";
  /** AMQP message header key for the sender's config fingerprint for change detection. */
  public static final String AMQP_HEADER_HEARTBEAT_CONFIG_FP = "hbConfigFp";
  /** AMQP message header key for the sender's data version high-water mark. */
  public static final String AMQP_HEADER_HEARTBEAT_DV_HWM = "hbDvHwm";

  /** AMQP message header key for the verification message type (PING or PONG). */
  public static final String AMQP_HEADER_VERIFY_TYPE = "verifyType";
  /** AMQP message header key for the originating app instance identifier. */
  public static final String AMQP_HEADER_VERIFY_APP_INSTANCE = "verifyAppInstance";
  /** AMQP message header key for the responding worker identifier. */
  public static final String AMQP_HEADER_VERIFY_WORKER_ID = "verifyWorkerId";
  /** Verification request type constant. */
  public static final String AMQP_HEADER_VERIFY_PING = "PING";
  /** Verification response type constant. */
  public static final String AMQP_HEADER_VERIFY_PONG = "PONG";


  /** Thread name prefix for general hotkey operations. */
  public static final String THREAD_PREFIX_HOTKEY = "hotkey-";
  /** Thread name prefix for cross-instance sync tasks. */
  public static final String THREAD_PREFIX_SYNC = "hotkey-sync";
  /** Thread name prefix for worker-mode tasks. */
  public static final String THREAD_PREFIX_WORKER = "hotkey-worker";
  /** Thread name prefix for report submission tasks. */
  public static final String THREAD_PREFIX_REPORT = "hotkey-report";

  /** Routing key prefix for broadcast decisions (HOT/COOL) sent from worker to apps. */
  public static final String ROUTING_KEY_BROADCAST = "broadcast.";
  /** Routing key prefix for report messages from apps to worker. */
  public static final String ROUTING_KEY_REPORT = "report.";
  /** Queue name prefix for report queues (appended with app name). */
  public static final String QUEUE_PREFIX_REPORT = "hotkey.report.";

  /** Source identifier for sliding-window-based decisions. */
  public static final String SOURCE_SLIDING_WINDOW = "sliding_window";
  /** Source identifier for TopK pre-warm validation based decisions. */
  public static final String SOURCE_TOPK_PRE_WARM = "topk_pre_warm";

  /** Redis key prefix for version tracking entries. */
  public static final String REDIS_VERSION_KEY_PREFIX = "hotkey:ver:";

  /** Default initial version value when no prior version exists. */
  public static final long VERSION_DEFAULT = 0L;
  /** Increment value applied to the local TopK frequency counter on each access. */
  public static final int TOPK_INCR = 1;

  /** Warning message logged when sync publisher is not available. */
  public static final String NO_SYNC_PUBLISHER = "No sync publisher found, please enable hotkey.sync";

  /** Redis key for the dynamic rules hash (blacklist / whitelist entries). */
  public static final String REDIS_KEY_RULES = "hotkey:rules";
}
