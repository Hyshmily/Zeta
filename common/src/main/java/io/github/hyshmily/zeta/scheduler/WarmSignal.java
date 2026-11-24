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
package io.github.hyshmily.zeta.scheduler;

import lombok.Getter;

/**
 * Why a warm task was scheduled. Only {@link #READ} and {@link #LOCAL_ADMISSION} prove that this
 * instance is actually serving the key; a write, a cluster grant or a previous renewal does not.
 */
@Getter
public enum WarmSignal {
  /** A caller read loaded or served the key. */
  READ(true),
  /** The local TopK admitted the key; the detector's own counts are the read evidence. */
  LOCAL_ADMISSION(true),
  /** A put/local write/peer value write; not evidence of local demand. */
  WRITE(false),
  /** A cluster grant or prewarm; not evidence of local demand. */
  HEAT(false),
  /** A previous renewal completed; the next cycle needs fresh read evidence. */
  RENEW(false);

  private final boolean readEvidence;

  WarmSignal(boolean readEvidence) {
    this.readEvidence = readEvidence;
  }
}
