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
package io.github.hyshmily.zeta.sync.local;

import io.github.hyshmily.zeta.Internal;

/**
 * Strategy interface for processing cache-sync messages (REFRESH, INVALIDATE,
 * INVALIDATE_ALL, RULES_SYNC) from peer application instances.
 *
 * <p>Implement this interface and expose it as a Spring {@code @Bean} to fully
 * replace the default cache-sync processing logic. The default implementation
 * ({@link DefaultSyncDecisionHandler}) performs Redis-backed value loading,
 * version-guarded invalidation, batch processing, and rule syncing.
 *
 * <p>Use {@code @ConditionalOnMissingBean(SyncDecisionHandler.class)} on your
 * custom bean to replace the default.
 */
@Internal
public interface SyncDecisionHandler {

  /**
   * Process a REFRESH sync message from a peer instance.
   *
   * @param sm the sync message containing the key and version to refresh;
   *           must not be null
   */
  void handleRefresh(SyncMessage sm);

  /**
   * Process an INVALIDATE sync message from a peer instance.
   *
   * @param sm the sync message containing the key to invalidate; must not be null
   */
  void handleLocalInvalidate(SyncMessage sm);

  /**
   * Process an INVALIDATE_ALL sync message from a peer instance.
   *
   * @param sm the sync message containing the JSON-array of keys to invalidate;
   *           must not be null
   */
  void handleLocalInvalidateAll(SyncMessage sm);

  /**
   * Process a RULES_SYNC sync message from a peer instance.
   *
   * @param sm the sync message containing the serialized rule-set JSON;
   *           must not be null
   */
  void handleRulesSync(SyncMessage sm);
}
