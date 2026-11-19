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
package io.github.hyshmily.zeta.util;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;
import java.util.zip.CRC32;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * One application instance's identity: the instance id used in queue names and message
 * origins, and the 10-bit node id embedded in every {@code dataVersion} stamp.
 *
 * <p>The identity is an immutable instance value, supplied as a Bean by the auto-configuration.
 * Two contexts in one JVM therefore get distinct instance ids and queue names; there is no
 * process-global mutable override.
 *
 * <p>Resolution order for the instance id:
 * <ol>
 *   <li>{@code zeta.local.instance-id}, when non-blank</li>
 *   <li>{@code <server.port|SERVER_PORT|instance>-<HOSTNAME|local>-<random 8 hex>}: the host and
 *       port make the id readable, the random suffix makes it unique per context and per process.
 *       Neither a hostname nor a process-wide UUID can distinguish two instances on one host or
 *       two contexts in one JVM.</li>
 * </ol>
 *
 * <p>Resolution order for the node id:
 * <ol>
 *   <li>{@code zeta.local.node-id}, when set (0..1023)</li>
 *   <li>a stable CRC32 hash of the instance id</li>
 * </ol>
 *
 * <p>Two nodes that share a node id can mint equal stamps in the same millisecond: both
 * start a fresh window at sequence zero, and the equal-version guard then skips both
 * directions. Set a unique {@code node-id} per instance when instance ids are not unique.
 *
 * @param instanceId the unique instance id; never blank
 * @param nodeId     the 10-bit data-version node id
 */
@Internal
@Slf4j
public record NodeIdentity(String instanceId, long nodeId) {
  /** Width of the node identity carried by the low bits of a stamp. */
  public static final int NODE_BITS = 10;

  /** Largest valid node id. */
  public static final long MAX_NODE_ID = (1L << NODE_BITS) - 1;

  /**
   * One UUID per JVM: its most significant bits are the {@code jvmId} identity, and its string
   * form is the instance-id fallback when no hostname exists. The string is materialized lazily,
   * because a hostname-bearing deployment never needs it.
   */
  private static final UUID JVM_UUID = UUID.randomUUID();

  /** JVM-local identifier exposed by the actuator endpoint; stable for the process. */
  private static final long JVM_ID = JVM_UUID.getMostSignificantBits() & Long.MAX_VALUE;

  public NodeIdentity {
    Objects.requireNonNull(instanceId, "instanceId must not be null");
    if (instanceId.isBlank()) {
      throw new IllegalArgumentException("instanceId must not be blank");
    }
    if (nodeId < 0L || nodeId > MAX_NODE_ID) {
      throw new IllegalArgumentException("nodeId must be 0.." + MAX_NODE_ID + ", got " + nodeId);
    }
  }

  /**
   * Resolves one instance identity from the configured overrides, falling back to
   * auto-detection for the instance id and to the CRC32 derivation for the node id.
   *
   * @param configuredInstanceId the configured {@code zeta.local.instance-id}, or blank
   * @param configuredNodeId     the configured {@code zeta.local.node-id}, or {@code null}
   * @return the resolved identity; never {@code null}
   */
  public static NodeIdentity resolve(@Nullable String configuredInstanceId, @Nullable Integer configuredNodeId) {
    String resolvedInstance =
      configuredInstanceId != null && !configuredInstanceId.isBlank() ? configuredInstanceId : autoInstanceId();
    long resolvedNode = configuredNodeId != null ? configuredNodeId : deriveNodeId(resolvedInstance);
    NodeIdentity identity = new NodeIdentity(resolvedInstance, resolvedNode);
    log.info(
      "Node identity: instanceId='{}', dataVersion node id {} (10 bits){}",
      resolvedInstance,
      resolvedNode,
      configuredNodeId != null
        ? " from zeta.local.node-id"
        : " derived from the instance id; set zeta.local.node-id explicitly for clusters above ~30 instances (10-bit collision risk)"
    );
    return identity;
  }

  /**
   * The process-wide JVM identifier exposed by the actuator endpoint; not part of any data
   * stamp or queue name.
   *
   * @return a non-negative identifier unique with high probability per JVM
   */
  public static long getJvmId() {
    return JVM_ID;
  }

  /**
   * The auto-detected instance id: {@code <port>-<hostname|local>-<random>}. The random suffix is
   * generated per call on purpose: a hostname alone is shared by every instance on the host, and a
   * process-wide UUID is shared by every context in the JVM. Both would give two instances the same
   * queue name and node id.
   */
  private static String autoInstanceId() {
    String port = System.getProperty("server.port");
    if (port == null || port.isBlank()) {
      port = System.getenv("SERVER_PORT");
    }
    if (port == null || port.isBlank()) {
      port = "instance";
    }
    String hostname = System.getenv("HOSTNAME");
    String hostPart = hostname != null && !hostname.isBlank() ? hostname : "local";
    // The first eight characters of a UUID's string form are hex digits (the first group), so the
    // suffix is short and safe in a queue name.
    return port + "-" + hostPart + "-" + UUID.randomUUID().toString().substring(0, 8);
  }

  /** Stable mapping from an instance id to the 10-bit node space. */
  private static long deriveNodeId(String instanceId) {
    CRC32 crc = new CRC32();
    crc.update(instanceId.getBytes(StandardCharsets.UTF_8));
    return crc.getValue() & MAX_NODE_ID;
  }
}
