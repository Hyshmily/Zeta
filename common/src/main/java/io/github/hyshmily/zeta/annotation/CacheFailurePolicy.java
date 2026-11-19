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

/**
 * Decides what a cache-side failure does to the caller. The policy runs only
 * for a write, an invalidation or a namespace clear; a read failure always
 * propagates, because the loader may already have run and a fallback cannot
 * safely repeat the body.
 *
 * <p>
 * Returning normally authorizes the caller to continue. For a pre-invocation
 * removal that means the body runs with a stale entry; the default policy
 * rethrows instead. Throwing propagates the policy's exception to the caller.
 */
@FunctionalInterface
public interface CacheFailurePolicy {
  /**
   * Handle one failed cache operation.
   *
   * @param failure the failed operation; never {@code null}
   */
  void onFailure(CacheFailure failure);

  /**
   * The default: a committed write and an after-the-body removal log at WARN
   * and return, a pre-invocation removal/clear rethrows so the body does not
   * run against a cache that could not be invalidated.
   *
   * @return the default policy
   */
  static CacheFailurePolicy defaults() {
    return DefaultCacheFailurePolicy.INSTANCE;
  }
}
