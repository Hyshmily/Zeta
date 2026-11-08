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

import io.github.hyshmily.zeta.sharding.ConsistentHashRing;
import io.github.hyshmily.zeta.sharding.HealthView;
import io.github.hyshmily.zeta.sharding.HealthViewImpl;
import io.github.hyshmily.zeta.util.Internal;
import java.util.LinkedHashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.http.HttpStatus;
import org.springframework.util.Assert;
import org.springframework.web.server.ResponseStatusException;

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
 * <p>Both operations reconcile the ring from the health view before answering
 * (design decisions: topology).
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
  private final ConsistentHashRing ring;

  /** The live view, or a zero-timeout view so an unwired endpoint never allocates per request. */
  private final HealthView healthView;

  /**
   * Creates a new ring endpoint.
   *
   * @param ring       the ring to inspect, or {@code null} when the report
   *                   plane (which creates it) is not wired
   * @param healthView the cluster health view used for key routing, or {@code null}
   *                   when no view is wired
   */
  public RingEndpoint(@Nullable ConsistentHashRing ring, @Nullable HealthView healthView) {
    this.ring = ring;
    this.healthView = healthView != null ? healthView : new HealthViewImpl(0);
  }

  /**
   * Return the live ring topology as the sorted list of participating nodes.
   *
   * @return a map containing the {@code nodes} entry
   */
  @ReadOperation
  public Map<String, Object> ringInfo() {
    ConsistentHashRing activeRing = reconciledRing();
    return Map.of("nodes", activeRing.getNodes().stream().sorted().toList());
  }

  /**
   * Determine which node is responsible for the given key. With no Worker alive the
   * mapping reports {@code null} for {@code nodeId}.
   *
   * @param keySegments the path segments of the key; a key containing {@code /} arrives as
   *                    several segments and is re-joined verbatim
   * @return a map containing the key and its assigned node ID
   * @throws IllegalArgumentException if the joined key is blank
   */
  @ReadOperation
  public Map<String, Object> keyMapping(@Selector(match = Selector.Match.ALL_REMAINING) String[] keySegments) {
    String key = String.join("/", keySegments);
    Assert.hasText(key, "key must not be empty");
    ConsistentHashRing activeRing = reconciledRing();
    Map<String, Object> result = new LinkedHashMap<>(2);
    result.put("key", key);
    result.put("nodeId", activeRing.locateNode(key, healthView));
    return result;
  }

  /**
   * Reconcile the ring against the live health view and return it.
   *
   * <p>An absent ring is a disabled feature rather than a caller error, so the status line carries
   * the signal — a probe that checks only the status must not mark an unwired ring plane healthy.
   *
   * @return the wired ring, after a refresh from the health view
   * @throws ResponseStatusException with {@code 503 Service Unavailable} when no
   *                                {@link ConsistentHashRing} is wired
   */
  private ConsistentHashRing reconciledRing() {
    if (ring == null) {
      throw new ResponseStatusException(
        HttpStatus.SERVICE_UNAVAILABLE,
        "ConsistentHashRing not available: the report plane is disabled or RabbitMQ is absent"
      );
    }
    ring.rebuildIfPossible(healthView);
    return ring;
  }
}
