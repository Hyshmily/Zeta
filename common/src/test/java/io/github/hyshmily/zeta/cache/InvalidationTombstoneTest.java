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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hyshmily.zeta.cache.cachesupport.policy.GetPolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.InvalidatePolicy;
import io.github.hyshmily.zeta.cache.cachesupport.policy.StalePolicy;
import io.github.hyshmily.zeta.sync.local.DefaultSyncDecisionHandler;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import io.github.hyshmily.zeta.sync.local.SyncType;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Anti-resurrection guards: an in-flight read that started before an
 * {@code INVALIDATE}/{@code clearAll} must not write its (now removed) value
 * back into L1.
 *
 * <p>The window is real: the load path stamps its entry with the stamp it
 * sampled <em>before</em> the reader ran, and on an absent entry the
 * resident-version guard accepts any stamp — the invalidation tombstone is the
 * only defence. These tests pin that the mutation paths actually <em>record</em>
 * the tombstone the load path consults (a guard reading state nobody writes was
 * exactly the defect).
 */
@DisplayName("Invalidation tombstones stop in-flight fills from resurrecting")
class InvalidationTombstoneTest {

  /** Options with reporting off — keeps the harness free of Worker wiring. */
  private static GetPolicy options() {
    return GetPolicy.of(0L, 0L, false, StalePolicy.SOFT_REFRESH);
  }

  @Test
  @DisplayName("clearAll during an in-flight read: the value is served but not cached")
  void clearAllDuringLoad_refusesTheFill() {
    try (StampTestSupport s = new StampTestSupport()) {
      String served = s.cache.get(
        "k",
        () -> {
          // Lands after the load sampled its pre-read stamp: the clear stamp is
          // strictly higher, so the fill is pre-clear by definition.
          s.cache.clear(new InvalidatePolicy(InvalidatePolicy.Scope.BOTH, true));
          return "pre-clear-value";
        },
        options()
      );

      assertThat(served).as("the caller still gets its value").isEqualTo("pre-clear-value");
      assertThat(s.l1.getIfPresent("k")).as("the pre-clear value must not be re-cached").isNull();
    }
  }

  @Test
  @DisplayName("a read that starts after clearAll caches normally")
  void readAfterClearAll_stores() {
    try (StampTestSupport s = new StampTestSupport()) {
      s.cache.clear(new InvalidatePolicy(InvalidatePolicy.Scope.BOTH, true));

      String served = s.cache.get("k", () -> "fresh-value", options());

      assertThat(served).isEqualTo("fresh-value");
      assertThat(s.l1.getIfPresent("k")).as("a post-clear read is not stale").isNotNull();
    }
  }

  @Test
  @DisplayName("invalidate during an in-flight read: the value is served but not cached")
  void invalidateDuringLoad_refusesTheFill() {
    try (StampTestSupport s = new StampTestSupport()) {
      String served = s.cache.get(
        "k",
        () -> {
          s.cache.invalidate("k", new InvalidatePolicy(InvalidatePolicy.Scope.BOTH, false));
          return "pre-invalidate-value";
        },
        options()
      );

      assertThat(served).isEqualTo("pre-invalidate-value");
      assertThat(s.l1.getIfPresent("k")).as("the invalidated value must not be re-cached").isNull();
    }
  }

  @Test
  @DisplayName("a load that started before clearAll honours the watermark")
  void loadStartedBeforeClearAll_isRefused() {
    try (StampTestSupport s = new StampTestSupport()) {
      String served = s.cache.get(
        "k",
        () -> {
          s.cache.clear(new InvalidatePolicy(InvalidatePolicy.Scope.BOTH, true));
          return "pre-clear-value";
        },
        options()
      );

      assertThat(served).isEqualTo("pre-clear-value");
      assertThat(s.l1.getIfPresent("k")).isNull();
    }
  }

  @Test
  @DisplayName("a peer INVALIDATE refuses a local load that started before it")
  void peerInvalidate_refusesLocalLoad() {
    try (StampTestSupport s = new StampTestSupport()) {
      TestSyncHandler handler = new TestSyncHandler(s.l1, null, s.lifecycle, List.of(), null);
      String served = s.cache.get(
        "k",
        () -> {
          handler.handleLocalInvalidate(new SyncMessage(1L, "k", SyncType.INVALIDATE, s.stamps.incrementAndGet()));
          return "pre-invalidate-value";
        },
        options()
      );

      assertThat(served).isEqualTo("pre-invalidate-value");
      assertThat(s.l1.getIfPresent("k")).as("the peer's removal must survive a slow local load").isNull();
    }
  }

  @Test
  @DisplayName("a peer INVALIDATE_ALL refuses a local load that started before it")
  void peerClearAll_refusesLocalLoad() {
    try (StampTestSupport s = new StampTestSupport()) {
      TestSyncHandler handler = new TestSyncHandler(s.l1, null, s.lifecycle, List.of(), null);
      String served = s.cache.get(
        "k",
        () -> {
          handler.handleClearAll(s.stamps.incrementAndGet());
          return "pre-clear-value";
        },
        options()
      );

      assertThat(served).isEqualTo("pre-clear-value");
      assertThat(s.l1.getIfPresent("k")).isNull();
    }
  }

  @Test
  @DisplayName("clearPrefix during an in-flight read refuses the fill in that namespace")
  void clearPrefixDuringLoad_refusesTheFill() {
    try (StampTestSupport s = new StampTestSupport()) {
      String served = s.cache.get(
        "users\u0000k",
        () -> {
          // Lands after the load sampled its pre-read stamp: the scoped clear
          // stamp is strictly higher, so the fill is pre-clear by definition.
          s.cache.clearPrefix("users\u0000", new InvalidatePolicy(InvalidatePolicy.Scope.BOTH, true));
          return "pre-clear-value";
        },
        options()
      );

      assertThat(served).as("the caller still gets its value").isEqualTo("pre-clear-value");
      assertThat(s.l1.getIfPresent("users\u0000k")).as("but it is not cached").isNull();
    }
  }

  @Test
  @DisplayName("a scoped clear does not block in-flight fills of another namespace")
  void clearPrefix_leavesOtherNamespacesAlone() {
    try (StampTestSupport s = new StampTestSupport()) {
      // The load samples its pre-read stamp before the clear, so an unscoped
      // guard would refuse this fill too — exactly the over-eviction a scoped
      // clear exists to avoid.
      String served = s.cache.get(
        "orders\u0000k",
        () -> {
          s.cache.clearPrefix("users\u0000", new InvalidatePolicy(InvalidatePolicy.Scope.BOTH, true));
          return "order-value";
        },
        options()
      );

      assertThat(served).isEqualTo("order-value");
      assertThat(s.l1.getIfPresent("orders\u0000k")).as("other namespaces keep filling").isNotNull();
    }
  }

  @Test
  @DisplayName("a write at the tombstone's own version is refused")
  void writeAtTombstoneVersion_isRefused() {
    try (StampTestSupport s = new StampTestSupport()) {
      s.lifecycle.recordInvalidation("k", 7L);

      assertThat(s.lifecycle.staleAgainstInvalidation("k", 7L))
        .as("the invalidation applies at equality")
        .isTrue();
      assertThat(s.lifecycle.staleAgainstInvalidation("k", 8L)).isFalse();
    }
  }

  @Test
  @DisplayName("a scoped clear sweeps only the entries under its prefix")
  void clearPrefix_sweepsOnlyThePrefix() {
    try (StampTestSupport s = new StampTestSupport()) {
      s.stamps.set(10L);
      s.l1.put("users\u0000a", StampTestSupport.entry("a", 1L));
      s.l1.put("users\u0000b", StampTestSupport.entry("b", 2L));
      s.l1.put("orders\u0000a", StampTestSupport.entry("c", 3L));

      s.cache.clearPrefix("users\u0000", new InvalidatePolicy(InvalidatePolicy.Scope.BOTH, true));

      assertThat(s.l1.getIfPresent("users\u0000a")).isNull();
      assertThat(s.l1.getIfPresent("users\u0000b")).isNull();
      assertThat(s.l1.getIfPresent("orders\u0000a")).isNotNull();
    }
  }
}
