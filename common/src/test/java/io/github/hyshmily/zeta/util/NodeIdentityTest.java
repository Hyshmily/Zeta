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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link NodeIdentity}: explicit instance/node overrides, the per-resolution
 * auto-detected fallback, the stable CRC32 derivation and the 10-bit node range.
 */
class NodeIdentityTest {

  @Test
  void jvmIdIsNonNegativeAndStable() {
    assertThat(NodeIdentity.getJvmId()).isNotNegative().isEqualTo(NodeIdentity.getJvmId());
  }

  @Test
  void autoDetectedInstanceIdsAreUniquePerResolution() {
    NodeIdentity first = NodeIdentity.resolve(null, null);
    NodeIdentity second = NodeIdentity.resolve(null, null);

    assertThat(second.instanceId())
      .as("two contexts in one JVM must not share a queue name")
      .isNotEqualTo(first.instanceId());
  }

  @Test
  void anExplicitInstanceIdTakesPrecedence() {
    assertThat(NodeIdentity.resolve("custom-instance", null).instanceId()).isEqualTo("custom-instance");
  }

  @Test
  void aBlankInstanceIdFallsBackToAutoDetection() {
    NodeIdentity identity = NodeIdentity.resolve("  ", null);

    assertThat(identity.instanceId()).isNotBlank().isNotEqualTo("  ");
  }

  @Test
  void explicitNodeIdWinsOverTheInstanceDerivation() {
    assertThat(NodeIdentity.resolve("instance-a", 1000).nodeId()).isEqualTo(1000L);
  }

  @Test
  void nodeDerivationIsStablePerInstanceId() {
    long first = NodeIdentity.resolve("instance-a", null).nodeId();
    long second = NodeIdentity.resolve("instance-a", null).nodeId();

    assertThat(second).isEqualTo(first).isEqualTo(expected("instance-a"));
  }

  @Test
  void changingTheInstanceIdReDerivesTheNodeId() {
    long first = NodeIdentity.resolve("instance-a", null).nodeId();
    long second = NodeIdentity.resolve("instance-b", null).nodeId();

    assertThat(second).isEqualTo(expected("instance-b"));
    if (expected("instance-a") != expected("instance-b")) {
      assertThat(second).isNotEqualTo(first);
    }
  }

  @Test
  void derivedNodeIdsStayInsideTheTenBitSpace() {
    for (int i = 0; i < 200; i++) {
      assertThat(NodeIdentity.resolve("instance-" + i, null).nodeId()).isBetween(0L, NodeIdentity.MAX_NODE_ID);
    }
  }

  @Test
  void rejectsNodeIdsOutsideTheTenBitSpace() {
    assertThatThrownBy(() -> NodeIdentity.resolve("instance-a", -1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> NodeIdentity.resolve("instance-a", (int) NodeIdentity.MAX_NODE_ID + 1)).isInstanceOf(
      IllegalArgumentException.class
    );
  }

  @Test
  void rejectsABlankInstanceId() {
    assertThatThrownBy(() -> new NodeIdentity(" ", 1L)).isInstanceOf(IllegalArgumentException.class);
  }

  private static long expected(String instanceId) {
    CRC32 crc = new CRC32();
    crc.update(instanceId.getBytes(StandardCharsets.UTF_8));
    return crc.getValue() & NodeIdentity.MAX_NODE_ID;
  }
}
