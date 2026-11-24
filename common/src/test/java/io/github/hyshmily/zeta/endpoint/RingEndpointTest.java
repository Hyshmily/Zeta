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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.sharding.ConsistentHashRing;
import io.github.hyshmily.zeta.sharding.HealthView;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.endpoint.InvalidEndpointRequestException;
import org.springframework.boot.actuate.endpoint.web.WebEndpointResponse;

class RingEndpointTest {

  private ConsistentHashRing ring;
  private HealthView healthView;
  private RingEndpoint endpoint;

  @BeforeEach
  void setUp() {
    ring = mock(ConsistentHashRing.class);
    healthView = mock(HealthView.class);
    endpoint = new RingEndpoint(ring, healthView);
  }

  @Test
  void ringInfo_shouldReturnLiveNodesOnly() {
    when(ring.getNodes()).thenReturn(Set.of("node1", "node2", "node3"));

    WebEndpointResponse<Map<String, Object>> response = endpoint.ringInfo();

    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getBody()).containsOnlyKeys("nodes");
    assertThat(response.getBody().get("nodes")).isEqualTo(List.of("node1", "node2", "node3"));
  }

  @Test
  void keyMapping_shouldUseTheInjectedHealthView() {
    when(ring.locateNode("k", healthView)).thenReturn("node1");

    assertThat(endpoint.keyMapping(new String[] { "k" }).getBody()).containsEntry("nodeId", "node1");
  }

  /**
   * With no health view wired the endpoint must not rebuild the ring from an empty liveness set;
   * it reports the current snapshot and routes on that snapshot.
   */
  @Test
  void keyMapping_shouldUseTheSnapshotWhenNoHealthViewIsWired() {
    RingEndpoint unwired = new RingEndpoint(ring, null);
    when(ring.locateNode("k")).thenReturn("node1");

    Map<String, Object> result = unwired.keyMapping(new String[] { "k" }).getBody();

    verify(ring).locateNode("k");
    verify(ring, never()).rebuildIfPossible(any());
    assertThat(result.get("nodeId")).isEqualTo("node1");
  }

  @Test
  void ringInfo_withoutHealthView_doesNotRebuildTheRing() {
    RingEndpoint unwired = new RingEndpoint(ring, null);
    when(ring.getNodes()).thenReturn(Set.of("node1"));

    assertThat(unwired.ringInfo().getBody()).containsEntry("nodes", List.of("node1"));

    verify(ring, never()).rebuildIfPossible(any());
  }

  @Test
  void keyMapping_shouldJoinRemainingPathSegmentsVerbatim() {
    when(ring.locateNode("a/b", healthView)).thenReturn("node2");

    assertThat(endpoint.keyMapping(new String[] { "a", "b" }).getBody())
      .containsEntry("key", "a/b")
      .containsEntry("nodeId", "node2");
  }

  @Test
  void keyMapping_shouldRejectAnEmptyKeyAsAClientError() {
    assertThatThrownBy(() -> endpoint.keyMapping(new String[] { "" })).isInstanceOf(
      InvalidEndpointRequestException.class
    );
  }

  @Test
  void keyMapping_shouldRejectABlankKeyAsAClientError() {
    assertThatThrownBy(() -> endpoint.keyMapping(new String[] { "   " })).isInstanceOf(
      InvalidEndpointRequestException.class
    );
  }

  @Test
  void keyMapping_shouldRejectAnAbsentKeyAsAClientError() {
    assertThatThrownBy(() -> endpoint.keyMapping(null)).isInstanceOf(InvalidEndpointRequestException.class);
  }

  @Test
  void ringInfo_shouldBeServiceUnavailableWhenRingIsAbsent() {
    RingEndpoint unwired = new RingEndpoint(null, healthView);

    assertThat(unwired.ringInfo().getStatus()).isEqualTo(503);
  }

  @Test
  void keyMapping_shouldBeServiceUnavailableWhenRingIsAbsent() {
    RingEndpoint unwired = new RingEndpoint(null, healthView);

    assertThat(unwired.keyMapping(new String[] { "myKey" }).getStatus()).isEqualTo(503);
  }

  @Test
  void keyMapping_shouldRejectBlankKeyBeforeCheckingTheRing() {
    RingEndpoint unwired = new RingEndpoint(null, healthView);

    assertThatThrownBy(() -> unwired.keyMapping(new String[] { " " })).isInstanceOf(
      InvalidEndpointRequestException.class
    );
  }
}
