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
package io.github.hyshmily.zeta.cache;

import static io.github.hyshmily.zeta.sync.local.SyncMessage.*;

import io.github.hyshmily.zeta.Internal;
import io.github.hyshmily.zeta.cache.cachesupport.BroadcastBuffer;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.reporting.KeyReporter;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import java.util.Collection;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Central dispatch for all external communication from the HotKey cache layer.
 * <p>
 * Aggregates three responsibilities that were previously spread across
 * {@link HotKeyCache}:
 * <ul>
 *   <li>Reporting key accesses to the Worker via {@link KeyReporter}</li>
 *   <li>Broadcasting INVALIDATE/REFRESH operations to peer instances
 *       via {@link CacheSyncPublisher}</li>
 *   <li>Deferred buffering of REFRESH messages via {@link BroadcastBuffer}</li>
 * </ul>
 * <p>
 * This reduces {@link HotKeyCache}'s constructor dependencies and centralizes
 * all "where to send this message" decisions in one place.
 */
@RequiredArgsConstructor
@Slf4j
@Internal
public class CentralDispatcher {

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private final Optional<KeyReporter> hotKeyReporter;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private final Optional<CacheSyncPublisher> cacheSyncPublisher;

  private final BroadcastBuffer broadcastBuffer;

  private final HotKeyDetector hotKeyDetector;

  private static final String NO_SYNC_PUBLISHER = "No sync publisher found, please enable zeta.sync";

  /**
   * Increment the local hot-key detector counter and optionally report the
   * access to the Worker via the {@link KeyReporter}.
   *
   * @param cacheKey       the accessed cache key
   * @param skipBroadcast if {@code true}, skip reporting to Worker
   */
  @SuppressWarnings("java:S6213")
  public void recordAccess(String cacheKey, boolean skipBroadcast) {
    hotKeyDetector.add(cacheKey);
    if (!skipBroadcast) {
      hotKeyReporter.ifPresent(r -> r.recordReport(cacheKey));
    }
  }

  /**
   * Broadcast a sync operation to all peer instances.
   * <p>
   * The actual send method invoked on {@link CacheSyncPublisher} depends
   * on the operation type:
   * <ul>
   *   <li>{@link SyncMessage#TYPE_INVALIDATE} — calls
   *       {@link CacheSyncPublisher#broadcastLocalInvalidate}</li>
   *   <li>{@link SyncMessage#TYPE_REFRESH} — buffers via
   *       {@link BroadcastBuffer#record} for deferred flush</li>
   * </ul>
   *
   * @param cacheKey the affected cache key
   * @param type     the operation type ({@link SyncMessage#TYPE_INVALIDATE}
   *                 or {@link SyncMessage#TYPE_REFRESH})
   * @param version  the data version at which the operation occurred
   * @param degraded whether the version was obtained in degraded mode
   * @throws IllegalArgumentException if the type is unknown
   */
  public void send(String cacheKey, String type, long version, boolean degraded) {
    switch (type) {
      case TYPE_INVALIDATE -> cacheSyncPublisher.ifPresentOrElse(
        p -> p.broadcastLocalInvalidate(cacheKey, version, degraded),
        () -> log.debug("send INVALIDATE: {}", NO_SYNC_PUBLISHER)
      );
      case TYPE_REFRESH -> cacheSyncPublisher.ifPresent(p -> broadcastBuffer.record(cacheKey, version, degraded));
      default -> throw new IllegalArgumentException("Unknown send type: " + type);
    }
  }

  /**
   * Batch-send a sync operation for multiple keys.
   * <p>
   * Currently only supports {@link SyncMessage#TYPE_INVALIDATE_ALL}:
   * calls {@link CacheSyncPublisher#broadcastLocalInvalidateAll}.
   *
   * @param cacheKeys the keys to send; null or empty is silently ignored
   * @param type      the operation type ({@link SyncMessage#TYPE_INVALIDATE_ALL})
   */
  public void send(Collection<String> cacheKeys, String type) {
    if (cacheKeys == null || cacheKeys.isEmpty()) {
      return;
    }
    if (type.equals(TYPE_INVALIDATE_ALL)) {
      cacheSyncPublisher.ifPresent(p -> p.broadcastLocalInvalidateAll(cacheKeys));
    }
  }
}
