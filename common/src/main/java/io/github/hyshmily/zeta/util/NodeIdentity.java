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
 * Immutable identity of one application instance: the {@code instanceId} used in queue names and
 * message origin headers, and the 10-bit {@code nodeId} embedded in {@code dataVersion} stamps.
 *
 * <p>Supplied as a Spring bean per context rather than a process-global mutable singleton, so
 * multiple contexts in the same JVM receive distinct instance IDs and queue names.
 *
 * <p><b>Resolution order:</b>
 * <ul>
 *   <li>{@code instanceId}: {@code zeta.local.instance-id} when non-blank; otherwise
 *       {@code <server.port|SERVER_PORT|instance>-<HOSTNAME|local>-<8-char hex>}.</li>
 *   <li>{@code nodeId}: {@code zeta.local.node-id} ({@code 0..1023}) when set; otherwise a
 *       stable CRC32 hash of {@code instanceId} masked to 10 bits.</li>
 * </ul>
 *
 * <p>Two instances sharing a {@code nodeId} can mint identical stamps within the same millisecond,
 * causing equal-version guards to skip updates in both directions. Configure unique explicit node
 * IDs for larger deployments.
 *
 * @param instanceId unique non-blank instance identifier
 * @param nodeId     10-bit {@code dataVersion} node identifier in {@code [0, MAX_NODE_ID]}
 */
@Internal
@Slf4j
public record NodeIdentity(String instanceId, long nodeId) {
  /** Bit width of the node identity carried in the low bits of a {@code dataVersion} stamp. */
  public static final int NODE_BITS = 10;

  /** Largest valid 10-bit node identifier ({@code 1023}). */
  public static final long MAX_NODE_ID = (1L << NODE_BITS) - 1L;

  private static final String DEFAULT_PORT_SEGMENT = "instance";
  private static final String DEFAULT_HOST_SEGMENT = "local";
  private static final int AUTO_ID_SUFFIX_LENGTH = 8;

  /**
   * Non-negative process-wide JVM identifier exposed via Actuator. Never used in instance IDs
   * because a process-wide value cannot distinguish multiple Spring contexts in one JVM.
   */
  private static final long JVM_ID = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;

  /** Validates that {@code instanceId} is non-blank and {@code nodeId} fits in 10 bits. */
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
   * Resolves a {@link NodeIdentity} from optional configuration overrides, falling back to
   * auto-detection for {@code instanceId} and CRC32 derivation for {@code nodeId}.
   *
   * @param configuredInstanceId configured {@code zeta.local.instance-id}, or {@code null}/blank
   * @param configuredNodeId     configured {@code zeta.local.node-id}, or {@code null} to derive
   * @return the resolved {@link NodeIdentity}
   * @throws IllegalArgumentException if {@code configuredNodeId} is outside {@code [0, MAX_NODE_ID]}
   */
  public static NodeIdentity resolve(@Nullable String configuredInstanceId, @Nullable Integer configuredNodeId) {
    String resolvedInstance = (configuredInstanceId != null && !configuredInstanceId.isBlank())
      ? configuredInstanceId
      : autoInstanceId();
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

  /** Returns the stable non-negative process-wide JVM identifier for Actuator diagnostics. */
  public static long getJvmId() {
    return JVM_ID;
  }

  /**
   * Synthesizes a readable, per-context unique instance ID ({@code <port>-<host>-<hex>}).
   */
  private static String autoInstanceId() {
    String port = System.getProperty("server.port");
    if (port == null || port.isBlank()) {
      port = System.getenv("SERVER_PORT");
    }
    String portPart = (port != null && !port.isBlank()) ? port : DEFAULT_PORT_SEGMENT;

    String hostname = System.getenv("HOSTNAME");
    String hostPart = (hostname != null && !hostname.isBlank()) ? hostname : DEFAULT_HOST_SEGMENT;

    // Fresh per-context random suffix: the first 8 UUID chars are hex digits, short and safe in queue names.
    String suffix = UUID.randomUUID().toString().substring(0, AUTO_ID_SUFFIX_LENGTH);
    return portPart + "-" + hostPart + "-" + suffix;
  }

  /** Deterministically hashes {@code instanceId} into the 10-bit node ID range via CRC32. */
  private static long deriveNodeId(String instanceId) {
    CRC32 crc = new CRC32();
    crc.update(instanceId.getBytes(StandardCharsets.UTF_8));
    return crc.getValue() & MAX_NODE_ID;
  }
}
