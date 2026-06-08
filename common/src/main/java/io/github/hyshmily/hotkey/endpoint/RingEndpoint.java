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
package io.github.hyshmily.hotkey.endpoint;

import io.github.hyshmily.hotkey.sharding.RingManager;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Actuator {@code /actuator/hotkeyring} endpoint for consistent-hash ring CRUD.
 *
 * <p>Operations:
 * <ul>
 *   <li>{@code GET /actuator/hotkeyring} — ring topology and mode
 *   <li>{@code GET /actuator/hotkeyring/{key}} — query which node handles a key
 *   <li>{@code POST /actuator/hotkeyring} — add a node (body: {@code {"nodeId":"..."}})
 *   <li>{@code DELETE /actuator/hotkeyring/{nodeId}} — remove a node
 *   <li>{@code POST /actuator/hotkeyring/rebuild} — switch back to auto mode
 * </ul>
 */
@RestController
@RequestMapping("${management.endpoints.web.base-path:/actuator}/hotkeyring")
@RequiredArgsConstructor
public class RingEndpoint {

  private final RingManager ringManager;

  @GetMapping
  public Map<String, Object> ringInfo() {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("mode", ringManager.isManualMode() ? "manual" : "auto");
    result.put("nodeCount", ringManager.nodeCount());
    result.put("virtualNodes", ringManager.getVirtualNodeCount());
    result.put("nodes", ringManager.getCurrentNodes().stream().sorted().toList());
    return result;
  }

  @GetMapping("/{key}")
  public Map<String, Object> keyMapping(@PathVariable String key) {
    Assert.hasText(key, "key must not be empty");
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("key", key);
    result.put("nodeId", ringManager.getNode(key));
    return result;
  }

  @PostMapping
  public Map<String, Object> addNode(@RequestBody Map<String, String> body) {
    String nodeId = body.get("nodeId");
    Assert.hasText(nodeId, "nodeId must not be empty");
    ringManager.addNode(nodeId);
    return Map.of("status", "ok", "action", "addNode", "nodeId", nodeId);
  }

  @DeleteMapping("/{nodeId}")
  public Map<String, Object> removeNode(@PathVariable String nodeId) {
    Assert.hasText(nodeId, "nodeId must not be empty");
    ringManager.removeNode(nodeId);
    return Map.of("status", "ok", "action", "removeNode", "nodeId", nodeId);
  }

  @PostMapping("/rebuild")
  public Map<String, Object> rebuild() {
    ringManager.resetToAuto();
    return Map.of("status", "ok", "action", "rebuild", "mode", "auto");
  }
}
