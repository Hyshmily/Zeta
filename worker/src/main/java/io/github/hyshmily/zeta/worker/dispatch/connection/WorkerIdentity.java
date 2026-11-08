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
package io.github.hyshmily.zeta.worker.dispatch.connection;

import io.github.hyshmily.zeta.util.Internal;

/**
 * This Worker process incarnation's identity: its stable node id and the epoch minted once at
 * startup.
 *
 * <p>Both values are fixed for the lifetime of the process: the node id is the deployment's
 * instance id, and {@link WorkerEpochStore} persists and increments the epoch on each start. One
 * immutable value carries them together, so every sender stamps the same pair and no component can
 * observe a half-updated identity.
 *
 * @param nodeId unique identity of this Worker node
 * @param epoch  node-local restart counter, fixed for this process
 */
@Internal
public record WorkerIdentity(String nodeId, long epoch) {
  public WorkerIdentity {
    if (nodeId == null || nodeId.isBlank()) {
      throw new IllegalArgumentException("a worker identity needs a non-blank node id");
    }
  }
}
