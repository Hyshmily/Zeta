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
package io.github.hyshmily.zeta.annotation;

import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * The default {@link CacheFailurePolicy}: protect a business write that already
 * committed, but never let a failed pre-invocation removal continue silently.
 */
@Slf4j
final class DefaultCacheFailurePolicy implements CacheFailurePolicy {

  static final DefaultCacheFailurePolicy INSTANCE = new DefaultCacheFailurePolicy();

  private static final int MAX_LOGGED_KEYS = 8;

  private DefaultCacheFailurePolicy() {}

  @Override
  public void onFailure(CacheFailure failure) {
    if (failure.beforeInvocation()) {
      throw failure.error();
    }
    log.warn(
      "[Zeta] cache {} failed for {}; the business result is returned",
      failure.verb(),
      describeTarget(failure),
      failure.error()
    );
  }

  private static String describeTarget(CacheFailure failure) {
    List<String> keys = failure.keys();
    if (keys.isEmpty()) {
      return "namespace '" + failure.cache() + "'";
    }
    if (keys.size() == 1) {
      return "key '" + keys.get(0) + "'";
    }
    StringBuilder text = new StringBuilder(keys.size() * 16).append(keys.size()).append(" keys [");
    int limit = Math.min(keys.size(), MAX_LOGGED_KEYS);
    for (int i = 0; i < limit; i++) {
      if (i > 0) {
        text.append(", ");
      }
      text.append(keys.get(i));
    }
    if (keys.size() > limit) {
      text.append(", ...");
    }
    return text.append(']').toString();
  }
}
