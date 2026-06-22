package io.github.hyshmily.hotkey.endpoint;

import io.github.hyshmily.hotkey.sharding.RingManager;
import io.github.hyshmily.hotkey.sharding.ClusterHealthView;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RingEndpointTest {

  private RingManager ringManager;
  private ObjectProvider<ClusterHealthView> healthViewProvider;
  private RingEndpoint endpoint;

  @BeforeEach
  void setUp() {
    ringManager = mock(RingManager.class);
    healthViewProvider = mock(ObjectProvider.class);
    endpoint = new RingEndpoint(ringManager, healthViewProvider);
  }

  @Test
  void ringInfo_shouldReturnAutoModeFields() {
    when(ringManager.isManualMode()).thenReturn(false);
    when(ringManager.nodeCount()).thenReturn(3);
    when(ringManager.getVirtualNodeCount()).thenReturn(150);
    when(ringManager.getCurrentNodes()).thenReturn(Set.of("node1", "node2", "node3"));

    Map<String, Object> result = endpoint.ringInfo();

    assertThat(result)
      .containsEntry("mode", "auto")
      .containsEntry("nodeCount", 3)
      .containsEntry("virtualNodes", 150);
    assertThat(result.get("nodes")).isEqualTo(List.of("node1", "node2", "node3"));
  }

  @Test
  void ringInfo_shouldReturnManualMode() {
    when(ringManager.isManualMode()).thenReturn(true);
    when(ringManager.nodeCount()).thenReturn(1);
    when(ringManager.getVirtualNodeCount()).thenReturn(50);
    when(ringManager.getCurrentNodes()).thenReturn(Set.of("manual-node"));

    Map<String, Object> result = endpoint.ringInfo();

    assertThat(result).containsEntry("mode", "manual");
    assertThat(result.get("nodes")).isEqualTo(List.of("manual-node"));
  }

  @Test
  void keyMapping_shouldReturnKeyAndNodeId() {
    ClusterHealthView healthView = mock(ClusterHealthView.class);
    when(healthViewProvider.getIfAvailable()).thenReturn(healthView);
    when(ringManager.routeNode(eq("myKey"), any(ClusterHealthView.class))).thenReturn("worker-1");

    Map<String, Object> result = endpoint.keyMapping("myKey");

    assertThat(result)
      .containsEntry("key", "myKey")
      .containsEntry("nodeId", "worker-1");
  }

  @Test
  void keyMapping_shouldFallbackToDefaultHealthViewWhenProviderReturnsNull() {
    when(healthViewProvider.getIfAvailable()).thenReturn(null);
    when(ringManager.routeNode(eq("fallbackKey"), any(ClusterHealthView.class))).thenReturn("placeholder");

    Map<String, Object> result = endpoint.keyMapping("fallbackKey");

    assertThat(result)
      .containsEntry("key", "fallbackKey")
      .containsEntry("nodeId", "placeholder");
    verify(ringManager).routeNode(eq("fallbackKey"), any(ClusterHealthView.class));
  }

  @Test
  void keyMapping_shouldThrowWhenKeyIsEmpty() {
    assertThatThrownBy(() -> endpoint.keyMapping(""))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void keyMapping_shouldThrowWhenKeyIsBlank() {
    assertThatThrownBy(() -> endpoint.keyMapping("   "))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void keyMapping_shouldHandleSingleCharacterKey() {
    ClusterHealthView healthView = mock(ClusterHealthView.class);
    when(healthViewProvider.getIfAvailable()).thenReturn(healthView);
    when(ringManager.routeNode(eq("x"), any(ClusterHealthView.class))).thenReturn("worker-1");

    Map<String, Object> result = endpoint.keyMapping("x");

    assertThat(result)
      .containsEntry("key", "x")
      .containsEntry("nodeId", "worker-1");
  }

  @Test
  void addNode_shouldDelegateToRingManager() {
    Map<String, Object> result = endpoint.addNode(Map.of("nodeId", "new-node"));

    assertThat(result)
      .containsEntry("status", "ok")
      .containsEntry("action", "addNode")
      .containsEntry("nodeId", "new-node");
    verify(ringManager).addNode("new-node");
  }

  @Test
  void addNode_shouldThrowWhenNodeIdMissing() {
    assertThatThrownBy(() -> endpoint.addNode(Map.of()))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void removeNode_shouldDelegateToRingManager() {
    Map<String, Object> result = endpoint.removeNode("old-node");

    assertThat(result)
      .containsEntry("status", "ok")
      .containsEntry("action", "removeNode")
      .containsEntry("nodeId", "old-node");
    verify(ringManager).removeNode("old-node");
  }

  @Test
  void removeNode_shouldThrowWhenNodeIdEmpty() {
    assertThatThrownBy(() -> endpoint.removeNode(""))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rebuild_shouldResetToAutoMode() {
    Map<String, Object> result = endpoint.rebuild();

    assertThat(result)
      .containsEntry("status", "ok")
      .containsEntry("action", "rebuild")
      .containsEntry("mode", "auto");
    verify(ringManager).resetToAuto();
  }
}
