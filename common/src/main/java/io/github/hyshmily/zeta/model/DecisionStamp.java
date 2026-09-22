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
package io.github.hyshmily.zeta.model;

import jakarta.annotation.Nullable;

/**
 * The Worker decision stamp carried by a {@link CacheEntry}. Only present when
 * the entry has a Worker origin (HOT/COOL broadcasts); local entries carry no
 * decision metadata.
 *
 * <p>This is the single value type for decision metadata across the entry
 * pipeline: {@code ExpireManager.decisionOf} extracts it from a raw cache
 * value, {@link EntryDraft#decision(DecisionStamp)} applies it to a draft, and
 * the sync/worker handlers pass it between read and write sites. A {@code null}
 * stamp is equivalent to the all-zero local origin
 * ({@code decisionVersion = 0, decisionNodeId = null, decisionEpoch = 0}).
 *
 * @param decisionVersion the Worker decision version
 * @param decisionNodeId  the Worker node ID that produced the decision
 * @param decisionEpoch   the epoch (restart counter) of the decision Worker
 */
public record DecisionStamp(long decisionVersion, @Nullable String decisionNodeId, long decisionEpoch) {}
