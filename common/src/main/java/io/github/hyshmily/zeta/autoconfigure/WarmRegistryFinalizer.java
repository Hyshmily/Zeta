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

import io.github.hyshmily.zeta.annotation.support.NamespaceKeys;
import io.github.hyshmily.zeta.cache.AccessCounter;
import io.github.hyshmily.zeta.cache.HotKeyCache;
import io.github.hyshmily.zeta.hotkeydetector.HotKeyDetector;
import io.github.hyshmily.zeta.policy.DefaultWarmRegistry;
import io.github.hyshmily.zeta.policy.NamespacePolicyRegistry;
import io.github.hyshmily.zeta.policy.NamespacePolicyRegistry.WarmLoaderBinder;
import io.github.hyshmily.zeta.policy.ZetaWarmConfigurer;
import io.github.hyshmily.zeta.scheduler.KeepWarm;
import io.github.hyshmily.zeta.util.Internal;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;

/**
 * The one warm startup sequence, after every singleton exists:
 * configurers → binders → validate → freeze → attach the renewal scheduler.
 *
 * <p>The order is the contract. A configurer registers through {@link WarmRegistry}, a warm-loader
 * binder resolves annotated methods against those registrations, validation rejects an active
 * trigger without a loader, freeze publishes the immutable snapshot, and only then may
 * {@link KeepWarm} receive a signal. Attaching earlier would let a scheduler read a registry that is
 * still being written or that is about to fail startup.
 *
 * <p>It lives in the app-side auto-configuration, not in the annotation layer, so a purely
 * programmatic deployment runs the same validation and freeze as one that uses annotations.
 */
@Internal
public final class WarmRegistryFinalizer implements SmartInitializingSingleton {

  private final NamespacePolicyRegistry policies;
  private final NamespaceKeys keys;
  private final ObjectProvider<ZetaWarmConfigurer> configurers;
  private final ObjectProvider<WarmLoaderBinder> binders;
  private final HotKeyCache cache;
  private final AccessCounter accessCounter;
  private final HotKeyDetector detector;
  private final ObjectProvider<KeepWarm> keepWarm;

  public WarmRegistryFinalizer(
    NamespacePolicyRegistry policies,
    NamespaceKeys keys,
    ObjectProvider<ZetaWarmConfigurer> configurers,
    ObjectProvider<WarmLoaderBinder> binders,
    HotKeyCache cache,
    AccessCounter accessCounter,
    HotKeyDetector detector,
    ObjectProvider<KeepWarm> keepWarm
  ) {
    this.policies = policies;
    this.keys = keys;
    this.configurers = configurers;
    this.binders = binders;
    this.cache = cache;
    this.accessCounter = accessCounter;
    this.detector = detector;
    this.keepWarm = keepWarm;
  }

  @Override
  public void afterSingletonsInstantiated() {
    DefaultWarmRegistry registry = DefaultWarmRegistry.create(keys, policies);
    configurers.orderedStream().forEach(configurer -> configurer.configure(registry));
    registry.assertFullyRegistered();
    binders.orderedStream().forEach(WarmLoaderBinder::bindAll);
    policies.validateAndFreeze();

    KeepWarm scheduler = keepWarm.getIfAvailable();
    if (scheduler != null) {
      cache.setWarmScheduler(scheduler);
      accessCounter.setReadObserver(scheduler);
      detector.setAdmittedSink(scheduler::onLocalAdmitted);
    }
  }
}
