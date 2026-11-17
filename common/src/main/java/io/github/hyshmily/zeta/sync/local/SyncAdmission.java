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
 * <p>{@link InvalidateLocal} is meaningful only for a PUT/REFRESH: it drops the value path and
 * applies the framework's versioned local invalidation. For an INVALIDATE it is redundant and is
 * settled as {@link SyncSkipReason#POLICY_FAILED}; for a clear it is treated as
 * {@link Proceed} because the clear already sweeps the local table.
 */
public sealed interface SyncAdmission permits SyncAdmission.Proceed, SyncAdmission.InvalidateLocal, SyncAdmission.Skip {
  /** Continue with the normal pipeline. */
  record Proceed() implements SyncAdmission {}

  /**
   * Drop the value path and invalidate locally: no payload decode, no loader read. Valid
   * only for PUT/REFRESH.
   */
  record InvalidateLocal() implements SyncAdmission {}

  /** Leave the local entry untouched. */
  record Skip() implements SyncAdmission {}

  SyncAdmission PROCEED = new Proceed();
  SyncAdmission INVALIDATE_LOCAL = new InvalidateLocal();
  SyncAdmission SKIP = new Skip();
}
