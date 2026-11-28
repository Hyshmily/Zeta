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

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hyshmily.zeta.annotation.support.NamespaceKeys;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.policy.HeatTrigger;
import io.github.hyshmily.zeta.policy.KeyFormat;
import io.github.hyshmily.zeta.policy.NamespacePolicyRegistry;
import java.lang.reflect.Method;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("class-level warming defaults")
class TriggerScopeTest {

  @ZetaConfig(cache = "users", prewarm = HeatTrigger.Option.CLUSTER, keepWarm = HeatTrigger.Option.ANY)
  static class MixedService {

    @ZetaCached(key = "#p0", prewarm = HeatTrigger.Option.OFF)
    public String read(Long id) {
      return "v";
    }

    @ZetaPut(key = "#p0")
    public String write(Long id) {
      return "v";
    }

    @ZetaCached(cache = "orders", key = "#p0")
    public String other(Long id) {
      return "v";
    }
  }

  @Test
  @DisplayName("a write and a method opt-out do not contradict the class default")
  void writeDeclaresNoTriggerAndAMethodDefaultIsScoped() throws Exception {
    NamespacePolicyRegistry policies = new NamespacePolicyRegistry(new ZetaProperties(), null);
    policies.registerProgrammaticNamespace("users\u0000", null, null, key -> "v");
    PlanResolver resolver = new PlanResolver(null, () -> policies, () -> new NamespaceKeys(KeyFormat.TYPED));

    Method read = MixedService.class.getDeclaredMethod("read", Long.class);
    assertThat(resolver.matches(read, MixedService.class)).isTrue();
    policies.validateAndFreeze();

    // The read method opted out; the @ZetaPut beside it declares no trigger of its own.
    assertThat(policies.resolve("users\u0000x").prewarm()).isEqualTo(HeatTrigger.OFF);
    assertThat(policies.resolve("users\u0000x").keepWarm()).isEqualTo(HeatTrigger.ANY);
    // A method that names another namespace lends it neither the format nor the class trigger.
    assertThat(policies.resolve("orders\u0000x").prewarm()).isEqualTo(HeatTrigger.OFF);
    assertThat(policies.resolve("orders\u0000x").keepWarm()).isEqualTo(HeatTrigger.OFF);
  }
}
