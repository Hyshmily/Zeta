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
package io.github.hyshmily.zeta.util.version;

/**
 * Manages per-key monotonically increasing version numbers for the application-level
 * {@code dataVersion} space.
 */
public interface VersionController {
  /**
   * Result of a version allocation.
   *
   * @param dataVersion the allocated version number (positive for Redis, negative for local fallback)
   * @param degraded    whether the allocation used the local fallback (Redis was unavailable)
   */
  record VersionResult(long dataVersion, boolean degraded) {}

  /**
   * Atomically increment the version for the given cache key.
   *
   * @return a VersionResult containing the new version and a degraded flag
   */
  VersionResult nextVersion(String cacheKey);

  /**
   * Whether the Redis template is available for version tracking.
   *
   * @return {@code true} if Redis is configured
   */
  boolean isRedisConfigured();

  /**
   * Return the total number of degraded (local fallback) version allocations since startup.
   *
   * @return degraded version count
   */
  long getDegradedVersionCount();

  /**
   * Allocate a version in the negative long space for degraded operation.
   *
   * @return a VersionResult with a negative version and degraded=true
   */
  VersionResult fallbackVersion();

  /**
   * Read-only current version for the given cache key.  Performs a plain
   * {@code GET} on the Redis version key ({@code zeta:ver:{cacheKey}}) and
   * returns the value if it exists.
   *
   * <p>Returns an empty {@code Optional} when:
   * <ul>
   *   <li>The version key does not exist in Redis (never incremented)</li>
   *   <li>Redis is not configured</li>
   *   <li>A Redis error occurs (logged at WARN)</li>
   * </ul>
   *
   * @param cacheKey the cache key whose version to look up; must not be null
   * @return the current version, or empty if absent/unavailable
   */
  java.util.Optional<Long> currentVersion(String cacheKey);
}
