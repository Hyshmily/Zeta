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
package io.github.hyshmily.hotkey.endpoint;

import io.github.hyshmily.hotkey.Internal;
import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.web.bind.annotation.*;

/**
 * Actuator endpoint for reading and updating the Worker's state-machine
 * configuration at runtime.
 *
 * <p>Changes are propagated to peer Workers via heartbeat broadcast
 * (see {@code AMQP_HEADER_HEARTBEAT_CONFIG_FP} / {@code hbConfigFp} in
 * {@code WorkerHeartbeatProducer}).
 *
 * <p>Endpoint path: {@code /actuator/hotkey/worker/state}.
 *
 * <p><b>Security:</b> The {@code POST} endpoint allows callers to modify
 * detection thresholds ({@code confirmCount}, {@code coolCount},
 * {@code preCoolGraceCount}) at runtime. Protect it via Spring Security
 * (e.g. {@code management.endpoint.hotkeyworkerstate.roles=ADMIN}) to
 * prevent unauthorised configuration changes in production environments.
 */
@Internal
@RestController
@RequestMapping("${management.endpoints.web.base-path:/actuator}/hotkey/worker/state")
public class StateMachineEndpoint {

  /** Hot-key state machine whose config is being exposed/modified. */
  private final HotKeyStateMachine stateMachine;
  /** Shared atomic counter bumped on each config change; broadcast via heartbeat. */
  private final ObjectProvider<AtomicLong> configTimestampCounter;

  /**
   * Creates a new endpoint for the given state machine instance.
   *
   * @param stateMachine           the hot-key state machine whose config is exposed
   * @param configTimestampCounter shared atomic counter bumped on each config change;
   *                               propagated to peer Workers via heartbeat broadcast
   */
  public StateMachineEndpoint(HotKeyStateMachine stateMachine, ObjectProvider<AtomicLong> configTimestampCounter) {
    this.stateMachine = stateMachine;
    this.configTimestampCounter = configTimestampCounter;
  }

  /**
   * Returns the current state-machine configuration values.
   *
   * @return a map containing {@code confirmCount}, {@code coolCount},
   *         {@code preCoolGraceCount}, and {@code trackedKeys}
   */
  @GetMapping
  public Map<String, Object> get() {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("confirmCount", stateMachine.getConfirmCount());
    result.put("coolCount", stateMachine.getCoolCount());
    result.put("preCoolGraceCount", stateMachine.getPreCoolGraceCount());
    result.put("trackedKeys", stateMachine.getTrackedKeys());
    return result;
  }

  /**
   * Updates the state-machine configuration parameters at runtime.
   * <p>Changes are propagated to peer Workers via the next heartbeat
   * broadcast (the {@code configTimestampCounter} is bumped, causing
   * {@code WorkerHeartbeatProducer} to include the updated config
   * fingerprint in the next heartbeat message).</p>
   *
   * <p>Accepts a JSON body with optional integer fields:
   * <ul>
   *   <li>{@code confirmCount}
   *   <li>{@code coolCount}
   *   <li>{@code preCoolGraceCount}
   * </ul>
   *
   * @param body a map of parameter names to string values
   * @return a status map confirming the applied changes
   */
  @PostMapping
  public Map<String, Object> set(@RequestBody Map<String, String> body) {
    try {
      if (body.containsKey("confirmCount")) {
        int v = Integer.parseInt(body.get("confirmCount"));
        if (v < 0) return Map.of("status", "error", "message", "confirmCount must be >= 0");
        stateMachine.setConfirmCount(v);
      }
      if (body.containsKey("coolCount")) {
        int v = Integer.parseInt(body.get("coolCount"));
        if (v < 0) return Map.of("status", "error", "message", "coolCount must be >= 0");
        stateMachine.setCoolCount(v);
      }
      if (body.containsKey("preCoolGraceCount")) {
        int v = Integer.parseInt(body.get("preCoolGraceCount"));
        if (v < 0) return Map.of("status", "error", "message", "preCoolGraceCount must be >= 0");
        stateMachine.setPreCoolGraceCount(v);
      }
    } catch (NumberFormatException e) {
      return Map.of("status", "error", "message", "Invalid number format: " + e.getMessage());
    }
    var counter = configTimestampCounter.getIfAvailable();
    if (counter != null) {
      counter.incrementAndGet();
    }
    return Map.of("status", "ok");
  }
}
