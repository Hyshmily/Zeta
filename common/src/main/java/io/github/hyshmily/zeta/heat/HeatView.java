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
package io.github.hyshmily.zeta.heat;

import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.policy.HeatTrigger;
import io.github.hyshmily.zeta.util.Internal;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * The read-only composition of the two heat sources: a valid Worker lease and the local TopK.
 *
 * <p>Keep-warm asks {@link #isLocalHot}; cluster-scoped actions ask {@link #isClusterHot}; a
 * business query may ask either or both.
 */
@Internal
public final class HeatView {

  private final @Nullable HeatDirectory cluster;
  private final HotKeyDetector local;

  /**
   * @param cluster the cluster lease table, or {@code null} when there is no Worker plane
   * @param local   the local TopK detector
   */
  public HeatView(@Nullable HeatDirectory cluster, HotKeyDetector local) {
    this.cluster = cluster;
    this.local = Objects.requireNonNull(local, "local");
  }

  /**
   * @param key the physical key
   * @return whether a valid cluster lease holds the key
   */
  public boolean isClusterHot(String key) {
    return cluster != null && cluster.isClusterHot(key);
  }

  /**
   * @param key the physical key
   * @return whether the local TopK holds the key
   */
  public boolean isLocalHot(String key) {
    return local.contains(key);
  }

  /**
   * @param key the physical key
   * @return whether either source considers the key hot
   */
  public boolean isHot(String key) {
    return isLocalHot(key) || isClusterHot(key);
  }

  /**
   * Whether {@code trigger} accepts the key, querying only the sources it allows: {@code OFF}
   * reads nothing, {@code LOCAL} never touches the lease table, {@code CLUSTER} never touches the
   * local sketch, and {@code ANY} stops at the first hit.
   *
   * @param trigger the trigger mask
   * @param key     the physical key
   * @return whether the trigger accepts the key
   */
  public boolean matches(HeatTrigger trigger, String key) {
    return (trigger.allowsLocal() && isLocalHot(key)) || (trigger.allowsCluster() && isClusterHot(key));
  }
}
