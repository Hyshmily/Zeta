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
 * Defines what to do when a cache entry is soft-expired (stale) but not
 * yet hard-expired.
 *
 * <ul>
 *   <li>{@link #RETURN} — serve the stale value immediately, no background
 *       refresh.  Useful when the caller tolerates staleness and wants to
 *       avoid any upstream load.</li>
 *   <li>{@link #REVALIDATE} — block the caller and load fresh data from the
 *       upstream, exactly as if the entry were hard-expired.  The stale
 *       value is never returned.</li>
 *   <li>{@link #SOFT_REFRESH} — return the stale value immediately and
 *       trigger an asynchronous background refresh (default).  The next
 *       caller finds the fresh value.</li>
 * </ul>
 */
public enum StalePolicy {
  RETURN,
  REVALIDATE,
  SOFT_REFRESH
}
