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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.hyshmily.zeta.sharding.ConsistentHashRing;
import io.github.hyshmily.zeta.sharding.HealthView;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

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

    Map<String, Object> result = endpoint.ringInfo();

    assertThat(result).containsOnlyKeys("nodes");
    assertThat(result.get("nodes")).isEqualTo(List.of("node1", "node2", "node3"));
  }

  @Test
  void keyMapping_shouldUseTheInjectedHealthView() {
    when(ring.locateNode("k", healthView)).thenReturn("node1");

    assertThat(endpoint.keyMapping(new String[] { "k" })).containsEntry("nodeId", "node1");
  }

  /**
   * With no health view wired the constructor stores a zero-timeout fallback, so the ring
   * always gets a non-null view and — no Worker ever being alive — resolves no node.
   */
  @Test
  void keyMapping_shouldPassAZeroTimeoutFallbackWhenNoHealthViewIsWired() {
    RingEndpoint unwired = new RingEndpoint(ring, null);

    Map<String, Object> result = unwired.keyMapping(new String[] { "k" });

    ArgumentCaptor<HealthView> view = ArgumentCaptor.forClass(HealthView.class);
    verify(ring).locateNode(eq("k"), view.capture());
    assertThat(view.getValue()).isNotNull();
    assertThat(view.getValue().isClusterHealthy()).isFalse();
    assertThat(result.get("nodeId")).isNull();
  }

  @Test
  void keyMapping_shouldJoinRemainingPathSegmentsVerbatim() {
    when(ring.locateNode("a/b", healthView)).thenReturn("node2");

    assertThat(endpoint.keyMapping(new String[] { "a", "b" }))
      .containsEntry("key", "a/b")
      .containsEntry("nodeId", "node2");
  }

  @Test
  void keyMapping_shouldThrowWhenKeyIsEmpty() {
    assertThatThrownBy(() -> endpoint.keyMapping(new String[] { "" })).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void keyMapping_shouldThrowWhenKeyIsBlank() {
    assertThatThrownBy(() -> endpoint.keyMapping(new String[] { "   " })).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void ringInfo_shouldBeServiceUnavailableWhenRingIsAbsent() {
    RingEndpoint unwired = new RingEndpoint(null, healthView);

    assertThatThrownBy(unwired::ringInfo)
      .isInstanceOf(ResponseStatusException.class)
      .satisfies(ex ->
        assertThat(((ResponseStatusException) ex).getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
      );
  }

  @Test
  void keyMapping_shouldBeServiceUnavailableWhenRingIsAbsent() {
    RingEndpoint unwired = new RingEndpoint(null, healthView);

    assertThatThrownBy(() -> unwired.keyMapping(new String[] { "myKey" }))
      .isInstanceOf(ResponseStatusException.class)
      .satisfies(ex ->
        assertThat(((ResponseStatusException) ex).getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
      );
  }

  @Test
  void keyMapping_shouldRejectBlankKeyBeforeCheckingTheRing() {
    RingEndpoint unwired = new RingEndpoint(null, healthView);

    assertThatThrownBy(() -> unwired.keyMapping(new String[] { " " })).isInstanceOf(IllegalArgumentException.class);
  }
}
