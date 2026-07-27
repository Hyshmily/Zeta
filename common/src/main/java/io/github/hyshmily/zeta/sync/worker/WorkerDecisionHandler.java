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
package io.github.hyshmily.zeta.sync.worker;

import io.github.hyshmily.zeta.Internal;

/**
 * Strategy interface for processing Worker HOT/COOL decisions.
 *
 * <p>Implement this interface and expose it as a Spring {@code @Bean} to fully
 * replace the default HOT/COOL processing logic. The default implementation
 * ({@link DefaultWorkerDecisionHandler}) performs Redis-backed promotion and
 * downgrade with SRE rate limiting, version guarding, and hook dispatch.
 *
 * <p>Use {@code @ConditionalOnMissingBean(WorkerDecisionHandler.class)} on your
 * custom bean to replace the default.
 */
@Internal
public interface WorkerDecisionHandler {

  /**
   * Process a HOT decision from the Worker.
   *
   * @param wm the Worker message containing the HOT decision; must not be null
   */
  void handleHot(WorkerMessage wm);

  /**
   * Process a COOL decision from the Worker.
   *
   * @param wm the Worker message containing the COOL decision; must not be null
   */
  void handleCool(WorkerMessage wm);
}
