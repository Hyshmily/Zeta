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
package io.github.hyshmily.zeta.endpoint;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sharding.RingManager;
import io.github.hyshmily.zeta.sharding.impl.HealthViewImpl;
import java.util.LinkedHashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.util.Assert;

/**
 * Actuator {@code /actuator/hotkeyring} endpoint for consistent-hash ring inspection.
 *
 * <p>Operations:
 * <ul>
 *   <li>{@code GET /actuator/hotkeyring} — ring topology
 *   <li>{@code GET /actuator/hotkeyring/{key}} — query which node handles a key
 * </ul>
 *
 * <p>Runs on the management plane (port, exposure, and roles honored via the
 * standard {@code management.*} configuration), unlike a plain MVC controller.
 *
 * <p><b>Security:</b> This endpoint exposes cluster topology (live node addresses)
 * and per-key routing information. Protect it via Spring Security
 * (e.g. {@code management.endpoint.hotkeyring.roles=ADMIN}) to prevent
 * internal infrastructure discovery in production environments.
 */
@Internal
@Endpoint(id = "hotkeyring")
public class RingEndpoint {

  @Nullable
  private final RingManager ringManager;

  private final ObjectProvider<HealthView> healthViewProvider;

  public RingEndpoint(@Nullable RingManager ringManager, ObjectProvider<HealthView> healthViewProvider) {
    this.ringManager = ringManager;
    this.healthViewProvider = healthViewProvider;
  }

  /**
   * Return the current ring topology: node count, virtual node count,
   * and the sorted list of live nodes.
   *
   * @return a map containing {@code nodeCount}, {@code virtualNodes},
   *         and {@code nodes} entries
   */
  @ReadOperation
  public Map<String, Object> ringInfo() {
    if (ringManager == null) {
      return Map.of("error", "RingManager not available (RabbitMQ absent)");
    }
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("nodeCount", ringManager.nodeCount());
    result.put("virtualNodes", ringManager.getVirtualNodeCount());
    result.put("nodes", ringManager.getCurrentNodes().stream().sorted().toList());
    return result;
  }

  /**
   * Determine which node is responsible for the given key.
   * <p>When the cluster health view is unavailable (e.g. no Workers have
   * ever connected), a fallback view with zero expected nodes is used,
   * which may route the key to a local placeholder.</p>
   *
   * @param key the cache key to look up; must not be empty
   * @return a map containing the key and its assigned node ID
   * @throws IllegalArgumentException if {@code key} is empty
   */
  @ReadOperation
  public Map<String, Object> keyMapping(@Selector String key) {
    Assert.hasText(key, "key must not be empty");
    if (ringManager == null) {
      return Map.of("error", "RingManager not available (RabbitMQ absent)");
    }
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("key", key);
    HealthView view = healthViewProvider.getIfAvailable();
    if (view == null) {
      view = new HealthViewImpl(0, 0);
    }
    result.put("nodeId", ringManager.routeNode(key, view));
    return result;
  }
}
