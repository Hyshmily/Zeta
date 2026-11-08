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

/**
 * The operation carried by a Worker decision stamp.
 *
 * <p>The kind is part of the lease, not of the cached data: a {@code COOL}
 * stamp can remain on an entry whose effective state is {@code NORMAL} (the
 * cool-down completed) or {@code HOT} (a local promotion overrode it while
 * the cluster was degraded). Keeping the kind with the stamp lets the read
 * path tell those cases apart without a second state field.
 */
public enum DecisionKind {
  HOT,
  COOL,
}
