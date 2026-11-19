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
 * The cache verb that failed. A read failure is not routed through
 * {@link CacheFailurePolicy}: a loader may already have run, so a fallback
 * cannot safely repeat the body.
 */
public enum CacheVerb {
  /** A write-through {@code zeta.put}. */
  PUT,
  /** A single-key or multi-key {@code zeta.invalidate}. */
  INVALIDATE,
  /** A namespace clear through {@code zeta.clearNamespace}. */
  CLEAR,
}
