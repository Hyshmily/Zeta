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
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;

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
 * <p>Endpoint path: {@code /actuator/hotkey-worker-state} (an endpoint id
 * cannot nest like the former {@code /actuator/hotkey/worker/state} MVC path;
 * the read shape is unchanged, the write takes typed fields instead of a
 * string map — see {@link #set}).
 *
 * <p>Runs on the management plane (port, exposure, and roles honored via the
 * standard {@code management.*} configuration): the runtime config mutation
 * no longer sits on the application port.
 *
 * <p><b>Security:</b> The write operation allows callers to modify
 * detection thresholds ({@code confirmCount}, {@code coolCount},
 * {@code preCoolGraceCount}) at runtime. Protect it via Spring Security
 * (e.g. {@code management.endpoint.hotkey-worker-state.roles=ADMIN}) to
 * prevent unauthorised configuration changes in production environments.
 */
@Internal
@Endpoint(id = "hotkey-worker-state")
public class StateMachineEndpoint {

  /** Hot-key state machine whose config is being exposed/modified. */
  private final ZetaBayesianSM stateMachine;
  /** Shared atomic counter bumped on each config change; send via heartbeat. */
  private final ObjectProvider<AtomicLong> configTimestampCounter;

  /**
   * Serializes config POSTs: the validate step reads a snapshot and the apply
   * step writes three fields, so two unsynchronized requests can interleave
   * and mint an invalid combined config. Holds the fresh-snapshot read,
   * validation, and the setters together (see {@link #set}).
   */
  private final Object configWriteLock = new Object();

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
  @ReadOperation
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
   * <p>Each field is optional ({@code null} keeps the current value); the
   * applied combination must satisfy the same invariant the
   * config-negotiation layer enforces on heartbeat gossip
   * ({@code confirmCount >= 1, preCoolGraceCount >= 1, coolCount > preCoolGraceCount}):
   * without the check, a malformed write would be applied locally but rejected
   * by every peer's gossip validation, leaving the originating Worker on a
   * permanently divergent config with no reconciliation path. Malformed
   * (non-numeric) input never reaches this method — the actuator framework
   * rejects it with a 400 before dispatch.</p>
   *
   * @param confirmCount      the new confirm count, or {@code null} to keep it
   * @param coolCount         the new cool count, or {@code null} to keep it
   * @param preCoolGraceCount the new pre-cool grace count, or {@code null} to keep it
   * @return a status map confirming the applied changes
   */
  @WriteOperation
  public Map<String, Object> set(Integer confirmCount, Integer coolCount, Integer preCoolGraceCount) {
    // Nothing requested: pure no-op — no rewrite, no timestamp bump.
    if (confirmCount == null && coolCount == null && preCoolGraceCount == null) {
      return Map.of("status", "ok");
    }

    // Serialize snapshot-validate-apply: two concurrent writes that each
    // validate against their own starting snapshot can interleave their
    // setters and land a combination that is individually-invalid (e.g.
    // coolCount <= preCoolGraceCount) — the divergent-config state this
    // validation exists to prevent. The lock holds from the FRESH snapshot
    // (taken inside) through the last setter, so the validated combination
    // and the applied combination are always the same.
    synchronized (configWriteLock) {
      int effConfirm = confirmCount != null ? confirmCount : stateMachine.getConfirmCount();
      int effCool = coolCount != null ? coolCount : stateMachine.getCoolCount();
      int effGrace = preCoolGraceCount != null ? preCoolGraceCount : stateMachine.getPreCoolGraceCount();

      // Mirror of the WorkerConfigNegotiator gossip predicate — validate the
      // applied combination (provided fields override, others keep their
      // current values) before mutating anything, so the endpoint can never
      // mint a config the cluster would refuse to adopt. The predicate itself
      // is defined once on {@link ZetaBayesianSM#isValidConfig} so the two
      // call sites cannot drift.
      if (!ZetaBayesianSM.isValidConfig(effConfirm, effGrace, effCool)) {
        return Map.of(
          "status",
          "error",
          "message",
          "Config rejected: confirmCount >= 1, preCoolGraceCount >= 1 and " +
          "coolCount > preCoolGraceCount are required (confirmCount=" +
          effConfirm +
          ", coolCount=" +
          effCool +
          ", preCoolGraceCount=" +
          effGrace +
          ")"
        );
      }

      if (confirmCount != null) {
        stateMachine.setConfirmCount(effConfirm);
      }
      if (coolCount != null) {
        stateMachine.setCoolCount(effCool);
      }
      if (preCoolGraceCount != null) {
        stateMachine.setPreCoolGraceCount(effGrace);
      }
      var counter = configTimestampCounter.getIfAvailable();
      if (counter != null) {
        counter.incrementAndGet();
      }
    }
    return Map.of("status", "ok");
  }
}
