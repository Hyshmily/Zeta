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
package io.github.hyshmily.zeta.policy;

import org.jspecify.annotations.Nullable;

/**
 * Which heat signal may trigger a background warming action.
 *
 * <p>The enum describes the source of the permission, not the action itself: prewarm and keepWarm
 * share it so one namespace declaration can reason about both without two vocabularies. The two
 * signals form a two-bit mask ({@code 1} local, {@code 2} cluster), so a match is one bitwise test
 * and {@link OFF} is the empty mask rather than a special case.
 */
public enum HeatTrigger {
  /** Never trigger. */
  OFF(0),
  /** Trigger only on this instance's local TopK membership. */
  LOCAL(1),
  /** Trigger only on a valid Worker cluster lease. */
  CLUSTER(2),
  /** Trigger on either signal. */
  ANY(3);

  private static final int LOCAL_BIT = 1;
  private static final int CLUSTER_BIT = 2;

  private final int mask;

  HeatTrigger(int mask) {
    this.mask = mask;
  }

  /** Whether this trigger accepts the local TopK signal. */
  public boolean allowsLocal() {
    return (mask & LOCAL_BIT) != 0;
  }

  /** Whether this trigger accepts a Worker cluster lease. */
  public boolean allowsCluster() {
    return (mask & CLUSTER_BIT) != 0;
  }

  /**
   * Whether this trigger accepts two already-computed heat signals.
   *
   * @param localHot   whether the local TopK holds the key
   * @param clusterHot whether a valid cluster lease holds the key
   * @return {@code true} when the trigger fires
   */
  public boolean matches(boolean localHot, boolean clusterHot) {
    return (mask & ((localHot ? LOCAL_BIT : 0) | (clusterHot ? CLUSTER_BIT : 0))) != 0;
  }

  /** Annotation-facing form: {@link #INHERIT} keeps the enclosing declaration or global default. */
  public enum Option {
    /** No declaration here; inherit the class-level or global value. */
    INHERIT(null),
    /** Never trigger. */
    OFF(HeatTrigger.OFF),
    /** Trigger only on local heat. */
    LOCAL(HeatTrigger.LOCAL),
    /** Trigger only on a cluster lease. */
    CLUSTER(HeatTrigger.CLUSTER),
    /** Trigger on either signal. */
    ANY(HeatTrigger.ANY);

    private final @Nullable HeatTrigger trigger;

    Option(@Nullable HeatTrigger trigger) {
      this.trigger = trigger;
    }

    /**
     * The resolved trigger, or {@code null} for {@link #INHERIT}.
     *
     * @return the trigger, or {@code null}
     */
    public @Nullable HeatTrigger toTrigger() {
      return trigger;
    }
  }
}
