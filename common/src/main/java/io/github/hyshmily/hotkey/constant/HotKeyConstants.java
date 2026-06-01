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
package io.github.hyshmily.hotkey.constant;

/**
 * Shared constants used across the HotKey library.
 *
 * <p>Groups include AMQP message header names, thread name prefixes,
 * routing keys, queue prefixes, and version tracking defaults.
 */
public final class HotKeyConstants {

  private HotKeyConstants() {}

  public static final String AMQP_HEADER_TYPE = "type";
  public static final String AMQP_HEADER_VERSION = "version";
  public static final String AMQP_HEADER_IS_VERSION_DEGRADED = "isVersionDegraded";

  public static final String THREAD_PREFIX_HOTKEY = "hotkey-";
  public static final String THREAD_PREFIX_SYNC = "hotkey-sync";
  public static final String THREAD_PREFIX_WORKER = "hotkey-worker";
  public static final String THREAD_PREFIX_REPORT = "hotkey-report";

  public static final String ROUTING_KEY_HOT = "hot.";
  public static final String ROUTING_KEY_COOL = "cool.";
  public static final String ROUTING_KEY_REPORT = "report.";
  public static final String QUEUE_PREFIX_REPORT = "hotkey.report.";

  public static final String SOURCE_SLIDING_WINDOW = "sliding_window";
  public static final String SOURCE_TOPK_PRE_WARM = "topk_pre_warm";

  public static final String REDIS_VERSION_KEY_PREFIX = "hotkey:ver:";

  public static final long VERSION_DEFAULT = 0L;
  public static final int TOPK_INCR = 1;

  public static final String NO_SYNC_PUBLISHER =
    "No sync publisher found, please enable hotkey.sync";
}
