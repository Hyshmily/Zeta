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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;
import java.util.zip.CRC32;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * Process identity in the three forms Zeta needs: the instance id used in queue
 * names and message origins, the JVM-local id exposed as {@code jvmId}, and the
 * 10-bit node id embedded in every {@code dataVersion} stamp.
 *
 * <p>
 * Instance id resolution order (auto-detected forms render as
 * {@code <server.port|instance>-<hostname|uuid>}):
 * <ol>
 * <li>{@link #setInstanceOverride(String)} override
 * ({@code zeta.local.instance-id})</li>
 * <li>{@code HOSTNAME} environment variable, combined with the
 * {@code server.port} system property</li>
 * <li>Random {@link UUID}, combined with the {@code server.port} system
 * property</li>
 * </ol>
 *
 * <p>
 * Node id resolution order:
 * <ol>
 * <li>{@link #setNodeOverride(Long)} override ({@code zeta.local.node-id},
 * 0..1023)</li>
 * <li>A stable CRC32 hash of the instance id</li>
 * </ol>
 *
 * <p>
 * Two nodes that share a node id can mint equal stamps in the same millisecond:
 * both start a fresh window at sequence zero, and the equal-version guard then
 * skips
 * both directions, leaving the peers unable to order each other. The hash makes
 * accidental collisions rarer; only an explicitly unique {@code node-id} (or a
 * unique
 * {@code instance-id}) makes them impossible. The chosen id is logged once at
 * startup
 * (design decisions: version stamps).
 */
@Internal
@Slf4j
public final class NodeIdentity {

  /** Width of the node identity carried by the low 22 bits of a stamp. */
  public static final int NODE_BITS = 10;

  /** Largest valid node id. */
  public static final long MAX_NODE_ID = (1L << NODE_BITS) - 1;

  /**
   * One UUID per JVM: its upper half is {@link #JVM_ID}, and its string form is
   * the instance-id fallback when no hostname exists. The string is materialised
   * lazily, because a hostname-bearing deployment never needs it.
   */
  private static final UUID JVM_UUID = UUID.randomUUID();

  /**
   * JVM-local identifier derived from the upper 64 bits of {@link #JVM_UUID}.
   * Initialized once at class load, stable for the JVM lifetime.
   *
   * <p>
   * Exposed via the actuator endpoint as {@code jvmId} for process
   * identification. It is not part of any ID or version value: data stamps take
   * their node identity from {@link #getNodeId()}, and message trace IDs carry none.
   */
  private static final long JVM_ID = JVM_UUID.getMostSignificantBits() & Long.MAX_VALUE;

  /**
   * The effective identity as one immutable, fully resolved snapshot, replaced as a
   * unit under the class monitor. Both forms are resolved together, so a reader can
   * never pair a new instance id with an old node-id derivation, and the hot path is
   * one volatile read.
   *
   * <p>{@code boxedNodeId} is the single numeric representation: the pre-boxed value
   * keeps the AMQP header path from allocating a {@link Long} per message (the JDK
   * cache only covers -128..127), and {@link #getNodeId()} unboxes the same instance.
   */
  private record Identity(String instanceId, Long boxedNodeId) {}

  /** Lazy string form of {@link #JVM_UUID} for the no-hostname fallback. */
  private static final class FallbackUuid {

    private static final String VALUE = JVM_UUID.toString();

    private FallbackUuid() {}
  }

  /** The current identity snapshot; {@code null} until the first resolution. */
  @Nullable
  private static volatile Identity identity;

  /** An explicit instance id, or {@code null} when auto-detection is in force; guarded by the class monitor. */
  @Nullable
  @GuardedBy("NodeIdentity.class")
  private static String customInstanceId;

  /** An explicit node id, or {@code null} when it is derived; guarded by the class monitor. */
  @Nullable
  @GuardedBy("NodeIdentity.class")
  private static Long customNodeId;

  private NodeIdentity() {}

  /**
   * Set an explicit instance ID that takes precedence over all auto-detection.
   * A {@code null} or blank {@code id} clears the override. Typically called in
   * a {@code @PostConstruct} from the first {@code @AutoConfiguration}.
   *
   * @param id the instance id, or {@code null}/blank to clear
   */
  public static void setInstanceOverride(@Nullable String id) {
    String normalized = (id != null && !id.isBlank()) ? id : null;
    synchronized (NodeIdentity.class) {
      // Covers both idempotent shapes — setting the same value again and
      // clearing when nothing is set — before any derivation runs.
      if (Objects.equals(normalized, customInstanceId)) {
        return;
      }
      customInstanceId = normalized;
      if (normalized != null) {
        log.info("Instance ID override set: {}", normalized);
      } else {
        log.info("Instance ID override cleared");
      }
      String instanceId = normalized != null ? normalized : autoInstanceId();
      // A node-id override owns the effective value across instance-id changes;
      // otherwise the derivation moves with the instance id.
      Long boxedNodeId = customNodeId != null ? customNodeId : derivedNodeId(instanceId);
      identity = new Identity(instanceId, boxedNodeId);
    }
  }

  /**
   * Return the unique instance identifier, computing and caching it on first
   * access.
   *
   * @return the instance identifier, never {@code null}
   */
  public static String getInstanceId() {
    return current().instanceId();
  }

  /**
   * Return the JVM-local identifier.
   *
   * @return a non-negative integer unique with high probability per JVM process
   */
  public static long getJvmId() {
    return JVM_ID;
  }

  /**
   * Return the 10-bit node identity pre-boxed for the AMQP header API.
   *
   * <p>The publisher stamps {@code HEADER_ORIGIN_NODE_ID} on every sync message;
   * {@code Long.valueOf(long)} only caches -128..127, so 87.5% of the node-id
   * range would allocate a fresh {@link Long} per message. The snapshot holds
   * the single boxed instance instead.
   *
   * @return the node id as the snapshot's {@link Long}, never {@code null}
   */
  public static Long getBoxedNodeId() {
    return current().boxedNodeId();
  }

  /**
   * Set an explicit node id. {@code null} clears the override and falls back
   * to deriving from the instance id.
   *
   * @param id the node id in {@code [0, 1023]}, or {@code null}
   * @throws IllegalArgumentException if {@code id} is outside the 10-bit range
   */
  public static void setNodeOverride(@Nullable Long id) {
    if (id != null && (id < 0L || id > MAX_NODE_ID)) {
      throw new IllegalArgumentException("nodeId must be 0.." + MAX_NODE_ID + ", got " + id);
    }

    synchronized (NodeIdentity.class) {
      // One check covers a redundant clear and setting the same value again.
      if (Objects.equals(id, customNodeId)) {
        return;
      }
      customNodeId = id;
      Identity snapshot = identity;
      String instanceId = snapshot != null ? snapshot.instanceId() : autoInstanceId();
      if (id != null) {
        log.info("dataVersion node id {} (10 bits) from zeta.local.node-id", id);
        identity = new Identity(instanceId, id);
      } else {
        // Clearing the override re-derives immediately, so the snapshot stays fully
        // resolved; a late read never sees a stale node id.
        log.info("dataVersion node-id override cleared; deriving from the instance id");
        identity = new Identity(instanceId, derivedNodeId(instanceId));
      }
    }
  }

  /**
   * Return the 10-bit node identity for this process.
   *
   * <p>
   * The derived value is cached and invalidated by
   * {@link #setInstanceOverride(String)}
   * and {@link #setNodeOverride(Long)}, so the fast path is one volatile read —
   * no per-call override boxing or instance-id string comparison.
   *
   * @return the node id in {@code [0, 1023]}
   */
  public static long getNodeId() {
    return current().boxedNodeId();
  }

  /**
   * The one read path: resolves both forms together on first access and publishes
   * them as one immutable snapshot.
   *
   * @return the fully resolved identity
   */
  private static Identity current() {
    Identity snapshot = identity;
    if (snapshot != null) {
      return snapshot;
    }
    synchronized (NodeIdentity.class) {
      snapshot = identity;
      if (snapshot != null) {
        return snapshot;
      }
      String instanceId = autoInstanceId();
      Identity resolved = new Identity(instanceId, derivedNodeId(instanceId));
      identity = resolved;
      return resolved;
    }
  }

  /**
   * The auto-detected instance id and its one DEBUG line: the single owner of
   * that resolution, shared by both override setters and the lazy read path.
   *
   * @return the resolved instance id
   */
  @GuardedBy("NodeIdentity.class")
  private static String autoInstanceId() {
    String instanceId = resolveInstanceId();
    log.debug("Instance ID resolved (auto): {}", instanceId);
    return instanceId;
  }

  /** Boxes and logs one derived node id — the single owner of the derivation line. */
  private static Long derivedNodeId(String instanceId) {
    long derived = deriveNodeId(instanceId);
    log.info(
      "dataVersion node id {} (10 bits) derived from instance id '{}'; set a unique zeta.local.node-id (0..1023) " +
        "per instance when instance ids are not unique",
      derived,
      instanceId
    );
    return derived;
  }

  /** The auto-detected instance id: {@code <server.port|instance>-<hostname|uuid>}. */
  private static String resolveInstanceId() {
    String port = System.getProperty("server.port", "instance");
    String hostname = System.getenv("HOSTNAME");
    String uniquePart = (hostname != null && !hostname.isBlank()) ? hostname : FallbackUuid.VALUE;
    return port + "-" + uniquePart;
  }

  /** Stable mapping from an instance id to the 10-bit node space. */
  private static long deriveNodeId(String instanceId) {
    CRC32 crc = new CRC32();
    crc.update(instanceId.getBytes(StandardCharsets.UTF_8));
    return crc.getValue() & MAX_NODE_ID;
  }
}
