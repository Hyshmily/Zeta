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

import io.github.hyshmily.zeta.Internal;
import java.util.Map;

/**
 * A batched report of access counts sent from an app instance to the Worker.
 *
 * @param appName   the reporting application name
 * @param timestamp the time at which the report was generated
 * @param counts    a map of key → cumulative access count
 */
@Internal
public record ReportMessage(String appName, long timestamp, Map<String, Long> counts) {}
