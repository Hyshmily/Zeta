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
package io.github.hyshmily.zeta.autoconfigure;

import jakarta.validation.constraints.Pattern;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Deployment-identity properties bound to the {@code zeta} prefix.
 *
 * <p>The application name is the tenant discriminator for every cross-process
 * channel: report routing keys ({@code reportToWorker.<appName>.<nodeId>}),
 * the {@code appName} header on sync broadcasts, and the Worker-side report
 * bindings plus decision headers. It lives here — one key
 * ({@code zeta.app-name}) bound by a single class consumed by <b>both</b> the
 * App side and the Worker side — so the two ends cannot disagree by
 * construction. The previous split ({@code zeta.local.app-name} vs
 * {@code zeta.worker.routing.app-name}) failed doubly and silently when the
 * two values diverged: decisions dropped on the App side <i>and</i> reports
 * dropped by the broker on the Worker side.
 *
 * <p>A shared default keeps single-application deployments working untouched;
 * deployments sharing a broker must set a unique value per application (a
 * startup WARN flags the default).
 */
@Data
@Validated
@ConfigurationProperties(prefix = "zeta")
public class ZetaAppProperties {

  /**
   * Default application name. Kept so single-application deployments need no
   * configuration, but note that a <em>shared</em> default silently disables
   * ADR-0068 application isolation: two applications that both keep it consider
   * each other "the same app" and keep processing one another's broadcasts.
   */
  public static final String DEFAULT_APP_NAME = "default";

  /** Application name used for queue naming and routing keys. Must match {@code [A-Za-z0-9_-]+}. */
  @Pattern(regexp = "^[A-Za-z0-9_-]+$", message = "appName must match ^[A-Za-z0-9_-]+$")
  private String appName = DEFAULT_APP_NAME;
}
