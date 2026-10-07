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
package io.github.hyshmily.zeta.sync;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.util.LogThrottle;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Shared-broker appName isolation (ADR-0068): the single home for the
 * foreign-app check previously duplicated in {@code WorkerListener} and
 * {@code CacheSyncListener} (the two copies had already diverged — one used
 * {@code equals}, the other {@code contentEquals} — for a predicate whose
 * semantics must be identical on both planes).
 *
 * <p>Both the decision fanout and the sync fanout ignore the routing key, so
 * on a shared broker every bound queue receives every application's
 * messages. A message is foreign when — and only when — the sender declared
 * an {@code appName} header that differs from this instance's appName:
 * <ul>
 *   <li>sender declared an appName that differs from ours → foreign (drop)</li>
 *   <li>sender declared no appName (pre-0068 sender) → not foreign (process)</li>
 *   <li>this instance declares no appName → not foreign (process everything)</li>
 * </ul>
 *
 * <p>The filter owns the drop counter, the last-observed foreign sender, and
 * the rate-limited log gate. Each listener keeps its own log wording (a
 * dropped decision vs a dropped sync message are different operational
 * signals) and its own delegating {@code foreignAppDrops()} /
 * {@code lastForeignApp()} accessors so the Actuator surface is unchanged.
 *
 * <p>Thread safety: all state is an immutable field, an {@link AtomicLong},
 * a volatile reference, or a thread-safe {@link LogThrottle} — safe for
 * concurrent AMQP consumer threads with no locking.
 */
@Internal
public final class AppIsolationFilter {

  /** This instance's appName; {@code null}/blank disables the filter. */
  private final String appName;

  /** Cumulative foreign-app drops (a non-zero value with a stable sender almost always means a misconfigured app-name). */
  private final AtomicLong foreignDropCounter = new AtomicLong();

  /** The most recent foreign sender appName observed (for pairing with the counter). */
  private volatile String lastForeignApp;

  /** Rate-limits the foreign-app-mismatch WARN to one per 10s window (ADR-0037). */
  private final LogThrottle foreignAppLogThrottle = LogThrottle.perDefaultWindow();

  /**
   * Creates the filter. {@code appName} may be {@code null}/blank to disable
   * foreign-app filtering (the pre-0068 behavior, which legacy wiring and
   * rolling upgrades still need).
   *
   * @param appName this application's appName
   */
  public AppIsolationFilter(String appName) {
    this.appName = appName;
  }

  /** This instance's appName (for log lines). */
  public String appName() {
    return appName;
  }

  /**
   * Whether the sender's {@code appName} header value marks the message as
   * belonging to a <em>different</em> application.
   *
   * @param senderAppHeader the raw header value ({@code null} when absent)
   * @return {@code true} if the message must be dropped as foreign
   */
  public boolean isForeign(Object senderAppHeader) {
    return senderAppHeader != null
      && appName != null
      && !appName.isBlank()
      && !appName.equals(senderAppHeader.toString());
  }

  /**
   * Records one foreign drop: remembers the sender and bumps the cumulative
   * counter.
   *
   * @param senderApp the sender appName (already known non-null via {@link #isForeign})
   * @return the new cumulative drop total (for the rate-limited log line)
   */
  public long noteDrop(String senderApp) {
    if (senderApp != null) {
      lastForeignApp = senderApp;
    }
    return foreignDropCounter.incrementAndGet();
  }

  /** Claims the one-per-window WARN slot for a foreign-drop log line. */
  public boolean tryLog() {
    return foreignAppLogThrottle.tryAcquire();
  }

  /** Total foreign-app drops since startup. */
  public long drops() {
    return foreignDropCounter.get();
  }

  /** The most recently observed foreign sender appName, or {@code null}. */
  public String lastForeignApp() {
    return lastForeignApp;
  }
}
