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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.CRC32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link NodeIdentity}: instance id resolution and caching, the
 * JVM-local id, and the 10-bit node identity (explicit when configured,
 * otherwise a stable hash of the instance id, never a stale derivation).
 */
class NodeIdentityTest {

  @AfterEach
  void tearDown() {
    NodeIdentity.setNodeOverride(null);
    NodeIdentity.setInstanceOverride(null);
  }

  @Test
  void getJvmId_shouldReturnNonNegative() {
    assertThat(NodeIdentity.getJvmId()).isNotNegative();
  }

  @Test
  void getJvmId_shouldBeStable() {
    assertThat(NodeIdentity.getJvmId()).isEqualTo(NodeIdentity.getJvmId());
  }

  @Test
  void getInstanceId_shouldReturnCachedValueOnSecondCall() {
    String first = NodeIdentity.getInstanceId();
    String second = NodeIdentity.getInstanceId();
    assertThat(second).isEqualTo(first);
  }

  @Test
  void setInstanceOverride_shouldTakePrecedence() {
    NodeIdentity.setInstanceOverride("custom-instance");
    assertThat(NodeIdentity.getInstanceId()).isEqualTo("custom-instance");
  }

  @Test
  void setInstanceOverride_blankString_shouldNotOverride() {
    NodeIdentity.setInstanceOverride("  ");
    var result = NodeIdentity.getInstanceId();
    assertThat(result).isNotNull();
    assertThat(result).isNotEqualTo("  ");
  }

  @Test
  void setInstanceOverride_null_shouldClearAndFallback() {
    NodeIdentity.setInstanceOverride("temp-override");
    assertThat(NodeIdentity.getInstanceId()).isEqualTo("temp-override");
    NodeIdentity.setInstanceOverride(null);
    var result = NodeIdentity.getInstanceId();
    assertThat(result).isNotNull();
    assertThat(result).isNotEqualTo("temp-override");
  }

  @Test
  void getInstanceId_shouldBeThreadSafe() throws Exception {
    NodeIdentity.setInstanceOverride(null);
    var first = NodeIdentity.getInstanceId();
    ExecutorService exec = Executors.newFixedThreadPool(4);
    var futures = new java.util.ArrayList<Future<String>>();
    for (int i = 0; i < 10; i++) {
      futures.add(exec.submit(NodeIdentity::getInstanceId));
    }
    for (var f : futures) {
      assertThat(f.get()).isEqualTo(first);
    }
    exec.shutdown();
  }

  @Test
  void explicitNodeOverrideWins() {
    NodeIdentity.setInstanceOverride("instance-a");
    NodeIdentity.setNodeOverride(1000L);

    assertThat(NodeIdentity.getNodeId()).isEqualTo(1000L);
  }

  @Test
  void anInstanceIdChangeKeepsAnExplicitNodeOverride() {
    NodeIdentity.setNodeOverride(7L);
    NodeIdentity.setInstanceOverride("instance-b");

    assertThat(NodeIdentity.getNodeId()).isEqualTo(7L);
  }

  @Test
  void nullNodeOverrideFallsBackToTheInstanceDerivation() {
    NodeIdentity.setInstanceOverride("instance-a");
    NodeIdentity.setNodeOverride(7L);
    assertThat(NodeIdentity.getNodeId()).isEqualTo(7L);

    NodeIdentity.setNodeOverride(null);

    assertThat(NodeIdentity.getNodeId()).isEqualTo(expected("instance-a"));
  }

  @Test
  void nodeDerivationIsStablePerInstanceId() {
    NodeIdentity.setInstanceOverride("instance-a");
    long first = NodeIdentity.getNodeId();
    long second = NodeIdentity.getNodeId();

    assertThat(second).isEqualTo(first).isEqualTo(expected("instance-a"));
  }

  @Test
  void changingTheInstanceIdReDerivesInsteadOfReturningAStaleCache() {
    NodeIdentity.setInstanceOverride("instance-a");
    long first = NodeIdentity.getNodeId();

    NodeIdentity.setInstanceOverride("instance-b");
    long second = NodeIdentity.getNodeId();

    assertThat(second).isEqualTo(expected("instance-b"));
    if (expected("instance-a") != expected("instance-b")) {
      assertThat(second).isNotEqualTo(first);
    }
  }

  @Test
  void boxedNodeIdIsTheSnapshotInstanceAndTracksOverrides() {
    NodeIdentity.setInstanceOverride("instance-a");
    Long first = NodeIdentity.getBoxedNodeId();
    assertThat(first).isSameAs(NodeIdentity.getBoxedNodeId());

    NodeIdentity.setNodeOverride(900L);
    assertThat(NodeIdentity.getBoxedNodeId()).isEqualTo(900L);
  }

  @Test
  void derivedNodeIdsStayInsideTheTenBitSpace() {
    for (int i = 0; i < 200; i++) {
      NodeIdentity.setInstanceOverride("instance-" + i);
      assertThat(NodeIdentity.getNodeId()).isBetween(0L, NodeIdentity.MAX_NODE_ID);
    }
  }

  @Test
  void rejectsNodeIdsOutsideTheTenBitSpace() {
    assertThatThrownBy(() -> NodeIdentity.setNodeOverride(-1L)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> NodeIdentity.setNodeOverride(NodeIdentity.MAX_NODE_ID + 1)).isInstanceOf(
      IllegalArgumentException.class
    );
  }

  private static long expected(String instanceId) {
    CRC32 crc = new CRC32();
    crc.update(instanceId.getBytes(StandardCharsets.UTF_8));
    return crc.getValue() & NodeIdentity.MAX_NODE_ID;
  }
}
