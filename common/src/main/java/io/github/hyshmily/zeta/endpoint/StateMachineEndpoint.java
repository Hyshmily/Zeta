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
import io.github.hyshmily.zeta.detection.ZetaBayesianSM;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Actuator endpoint for reading and updating the Worker's state-machine
 * configuration at runtime.
 *
 * <p>Changes are propagated to peer Workers via heartbeat send
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
  private final ZetaBayesianSM stateMachine;
  /** Shared atomic counter bumped on each config change; send via heartbeat. */
  private final ObjectProvider<AtomicLong> configTimestampCounter;

  /**
   * Creates a new endpoint for the given state machine instance.
   *
   * @param stateMachine           the hot-key state machine whose config is exposed
   * @param configTimestampCounter shared atomic counter bumped on each config change;
   *                               propagated to peer Workers via heartbeat send
   */
  public StateMachineEndpoint(ZetaBayesianSM stateMachine, ObjectProvider<AtomicLong> configTimestampCounter) {
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
   * send (the {@code configTimestampCounter} is bumped, causing
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
   * <p>The POST-applied combination must satisfy the same invariant the
   * config-negotiation layer enforces on heartbeat gossip
   * ({@code confirmCount >= 1, preCoolGraceCount >= 1, coolCount > preCoolGraceCount}):
   * without the check, a malformed POST would be applied locally but rejected
   * by every peer's gossip validation, leaving the originating Worker on a
   * permanently divergent config with no reconciliation path.</p>
   *
   * @param body a map of parameter names to string values
   * @return a status map confirming the applied changes
   */
  @PostMapping
  public Map<String, Object> set(@RequestBody Map<String, String> body) {
    int confirmCount = stateMachine.getConfirmCount();
    int coolCount = stateMachine.getCoolCount();
    int preCoolGraceCount = stateMachine.getPreCoolGraceCount();
    boolean anyProvided = false;
    try {
      if (body.containsKey("confirmCount")) {
        confirmCount = Integer.parseInt(body.get("confirmCount"));
        anyProvided = true;
      }
      if (body.containsKey("coolCount")) {
        coolCount = Integer.parseInt(body.get("coolCount"));
        anyProvided = true;
      }
      if (body.containsKey("preCoolGraceCount")) {
        preCoolGraceCount = Integer.parseInt(body.get("preCoolGraceCount"));
        anyProvided = true;
      }
    } catch (NumberFormatException e) {
      return Map.of("status", "error", "message", "Invalid number format: " + e.getMessage());
    }

    // Nothing requested: pure no-op — no rewrite, no timestamp bump.
    if (!anyProvided) {
      return Map.of("status", "ok");
    }

    // Mirror of the WorkerConfigNegotiator gossip predicate — validate the
    // POST-APPLIED combination (provided fields override, others keep their
    // current values) before mutating anything, so the endpoint can never
    // mint a config the cluster would refuse to adopt. The predicate itself
    // is defined once on {@link ZetaBayesianSM#isValidConfig} so the two
    // call sites cannot drift.
    if (!ZetaBayesianSM.isValidConfig(confirmCount, preCoolGraceCount, coolCount)) {
      return Map.of(
        "status",
        "error",
        "message",
        "Config rejected: confirmCount >= 1, preCoolGraceCount >= 1 and " +
          "coolCount > preCoolGraceCount are required (confirmCount=" +
          confirmCount +
          ", coolCount=" +
          coolCount +
          ", preCoolGraceCount=" +
          preCoolGraceCount +
          ")"
      );
    }

    if (body.containsKey("confirmCount")) {
      stateMachine.setConfirmCount(confirmCount);
    }
    if (body.containsKey("coolCount")) {
      stateMachine.setCoolCount(coolCount);
    }
    if (body.containsKey("preCoolGraceCount")) {
      stateMachine.setPreCoolGraceCount(preCoolGraceCount);
    }
    var counter = configTimestampCounter.getIfAvailable();
    if (counter != null) {
      counter.incrementAndGet();
    }
    return Map.of("status", "ok");
  }
}
