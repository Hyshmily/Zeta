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

import java.util.Objects;

/**
 * The post-resolution decision for a PUT/REFRESH: write a value, invalidate locally, or
 * leave the entry untouched. The framework still owns the version guard, the loader
 * admission re-check and the terminal hook.
 */
public sealed interface SyncValueAction permits SyncValueAction.Apply, SyncValueAction.Invalidate, SyncValueAction.Skip {
  /**
   * Write the given value at the event's version.
   *
   * @param value the value to write; never {@code null}
   */
  record Apply(Object value) implements SyncValueAction {
    public Apply {
      Objects.requireNonNull(value, "value must not be null");
    }
  }

  /** Drop the resolved value and invalidate locally at the event's version. */
  record Invalidate() implements SyncValueAction {}

  /** Leave the local entry untouched. */
  record Skip() implements SyncValueAction {}

  SyncValueAction INVALIDATE = new Invalidate();
  SyncValueAction SKIP = new Skip();
}
