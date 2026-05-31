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
package io.github.hyshmily.hotkey.hotkeycache;

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

  private InstanceIdGenerator() {}

  private static volatile String override;
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
