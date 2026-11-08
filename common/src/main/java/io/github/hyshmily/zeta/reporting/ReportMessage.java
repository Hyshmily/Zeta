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
package io.github.hyshmily.zeta.reporting;

import io.github.hyshmily.zeta.util.Internal;
import java.util.Map;

/**
 * A batched report of access counts sent from an app instance to the Worker.
 *
 * @param id          time-sortable message trace ID; orders nothing and may coincide
 *                    across reports in one millisecond
 * @param appName     the reporting application name
 * @param timestamp   wall-clock ms when the batch was assembled; the Worker's optional
 *                    staleness filter measures the report's age against it
 * @param targetEpoch the epoch of the Worker incarnation this report was routed to;
 *                    the receiving Worker drops the report unless it matches its own
 *                    epoch (design decisions: reporting)
 * @param counts      per-key access counts accumulated since the previous flush
 */
@Internal
public record ReportMessage(long id, String appName, long timestamp, long targetEpoch, Map<String, Long> counts) {}
