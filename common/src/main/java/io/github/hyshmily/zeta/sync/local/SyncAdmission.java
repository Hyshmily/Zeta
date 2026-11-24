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

/**
 * The admission decision for one effective sync event, taken before a PUT payload is
 * decoded or a REFRESH loader read runs.
 *
 * <p>{@link #INVALIDATE_LOCAL} drops a value path and applies the framework's versioned local
 * invalidation. On a PUT/REFRESH that is a real degradation; on an INVALIDATE or a clear it is
 * treated as {@link #PROCEED}, because those operations already remove the local copy — refusing
 * the event there would leave a stale entry behind.
 */
public enum SyncAdmission {
  /** Continue with the normal pipeline. */
  PROCEED,

  /** Drop the value path and invalidate locally: no payload decode, no loader read. */
  INVALIDATE_LOCAL,

  /** Leave the local entry untouched. */
  SKIP,
}
