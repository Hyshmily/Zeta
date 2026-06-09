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

import io.github.hyshmily.hotkey.detection.HotKeyStateMachine;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Actuator endpoint for reading and updating the Worker's state-machine
 * configuration at runtime.
 *
 * <p>Changes are propagated to peer Workers via the heartbeat broadcast
 * (see {@code AMQP_HEADER_CONFIG_*} headers in
 * {@code WorkerBroadcaster#broadcastHeartbeat}).
 *
 * <p>Endpoint path: {@code /actuator/hotkey/worker/state}.
 */
@RestController
@RequestMapping("${management.endpoints.web.base-path:/actuator}/hotkey/worker/state")
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class StateMachineEndpoint {

  private final HotKeyStateMachine stateMachine;
  private final ObjectProvider<AtomicLong> configTimestampCounter;

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
    if (body.containsKey("confirmCount")) {
      stateMachine.setConfirmCount(Integer.parseInt(body.get("confirmCount")));
    }
    if (body.containsKey("coolCount")) {
      stateMachine.setCoolCount(Integer.parseInt(body.get("coolCount")));
    }
    if (body.containsKey("preCoolGraceCount")) {
      stateMachine.setPreCoolGraceCount(Integer.parseInt(body.get("preCoolGraceCount")));
    }
    var counter = configTimestampCounter.getIfAvailable();
    if (counter != null) {
      counter.incrementAndGet();
    }
    return Map.of("status", "ok");
  }
}
