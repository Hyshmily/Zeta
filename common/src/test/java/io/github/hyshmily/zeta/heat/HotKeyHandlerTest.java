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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Pins the default {@link HotKeyHandler#onHot} split between a fresh grant and a rebroadcast. */
@DisplayName("default hot key handler")
class HotKeyHandlerTest {

  private static final HotKeyHandler HANDLER = new HotKeyHandler() {};

  @Test
  @DisplayName("a new grant on a live resident arms the renewal and loads nothing")
  void aNewGrantOnALiveResidentArmsTheRenewal() {
    HotKeyContext context = context(true, true);

    HANDLER.onHot(context);

    verify(context).armKeepWarm();
    verify(context, never()).prewarmIfAbsent();
  }

  @Test
  @DisplayName("a new grant without a resident prewarms")
  void aNewGrantWithoutAResidentPrewarms() {
    HotKeyContext context = context(true, false);

    HANDLER.onHot(context);

    verify(context).prewarmIfAbsent();
    verify(context, never()).armKeepWarm();
  }

  @Test
  @DisplayName("a rebroadcast over a live lease does nothing")
  void aRebroadcastOverALiveLeaseDoesNothing() {
    HotKeyContext context = context(false, true);

    HANDLER.onHot(context);

    verify(context, never()).armKeepWarm();
    verify(context, never()).prewarmIfAbsent();
  }

  @Test
  @DisplayName("a rebroadcast with no resident must not cold-load the key")
  void aRebroadcastWithoutAResidentDoesNotColdLoad() {
    // The key expired under a live lease: a repeat broadcast is not a new heat event.
    HotKeyContext context = context(false, false);

    HANDLER.onHot(context);

    verify(context, never()).armKeepWarm();
    verify(context, never()).prewarmIfAbsent();
  }

  private static HotKeyContext context(boolean newlyHot, boolean liveEntry) {
    HotKeyContext context = mock(HotKeyContext.class);
    when(context.isNewlyHot()).thenReturn(newlyHot);
    when(context.hasLiveEntry()).thenReturn(liveEntry);
    return context;
  }
}
