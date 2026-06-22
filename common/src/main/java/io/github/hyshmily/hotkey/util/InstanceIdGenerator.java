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
package io.github.hyshmily.hotkey.util;

import io.github.hyshmily.hotkey.util.version.VersionGuard;
import java.util.UUID;

/**
 * Generates a unique instance identifier used for per-instance RabbitMQ queue names.
 * <p>
 * Resolution order:
 * <ol>
 *   <li>Explicit override from {@link #setOverride} (typically from {@code hotkey.instance-id})</li>
 *   <li>{@code HOSTNAME} environment variable + {@code server.port} system property</li>
 *   <li>Random {@link UUID} + {@code server.port} system property</li>
 * </ol>
 */
public final class InstanceIdGenerator {

  /** Private constructor to prevent instantiation of this utility class. */
  private InstanceIdGenerator() {}

  /**
   * JVM-local node identifier derived from the upper 31 bits of a random UUID.
   * Initialized once at class load, stable for the JVM lifetime.
   *
   * <p>Used by {@link VersionGuard} as the upper 32 bits of the
   * fallback version when Redis INCR is unavailable, ensuring that degraded
   * versions from different JVMs occupy non-overlapping ranges.
   */
  private static final int NODE_ID;

  static {
    NODE_ID = (int) (UUID.randomUUID().getMostSignificantBits() & Integer.MAX_VALUE);
  }

  /**
   * Return the JVM-local node identifier.
   *
   * @return a non-negative integer unique with high probability per JVM process
   */
  public static int getNodeId() {
    return NODE_ID;
  }

  /** Explicit instance-ID override set via {@link #setOverride}. */
  private static volatile String override;
  /** Cached computed instance-ID to avoid recomputation on every call. */
  private static volatile String cached;

  /**
   * Set an explicit instance ID that takes precedence over all auto-detection.
   * Typically called in a {@code @PostConstruct} from the first {@code @AutoConfiguration}.
   */
  public static void setOverride(String id) {
    override = id;
    cached = null;
  }

  /**
   * Return the unique instance identifier, computing and caching it on first access.
   *
   * @return the instance identifier, never {@code null}
   */
  public static String get() {
    if (cached == null) {
      synchronized (InstanceIdGenerator.class) {
        if (cached == null) {
          if (override != null && !override.isBlank()) {
            cached = override;
          } else {
            String port = System.getProperty("server.port", "instance");
            String hostname = System.getenv("HOSTNAME");
            String uniquePart = (hostname != null && !hostname.isBlank()) ? hostname : UUID.randomUUID().toString();
            cached = port + "-" + uniquePart;
          }
        }
      }
    }
    return cached;
  }
}
