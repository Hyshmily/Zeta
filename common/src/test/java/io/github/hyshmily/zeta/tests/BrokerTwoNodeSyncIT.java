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
package io.github.hyshmily.zeta.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.ConnectionFactory;
import io.github.hyshmily.zeta.autoconfigure.ZetaProperties;
import io.github.hyshmily.zeta.cache.cachesupport.lifecycle.EntryLifecycleImpl;
import io.github.hyshmily.zeta.cache.cachesupport.raw.CacheEntry;
import io.github.hyshmily.zeta.cache.cachesupport.raw.EntryDraft;
import io.github.hyshmily.zeta.model.DecisionStamp;
import io.github.hyshmily.zeta.model.KeyState;
import io.github.hyshmily.zeta.sync.local.CacheSyncListener;
import io.github.hyshmily.zeta.sync.local.CacheSyncProperties;
import io.github.hyshmily.zeta.sync.local.CacheSyncPublisher;
import io.github.hyshmily.zeta.sync.local.DefaultSyncContext;
import io.github.hyshmily.zeta.sync.local.PutPayloadCodec;
import io.github.hyshmily.zeta.sync.local.SyncPolicy;
import io.github.hyshmily.zeta.sync.local.SyncMessage;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Two-node end-to-end sync over a REAL broker (P2-7 / design decisions: invalidation evidence):
 * INVALIDATE, value-carrying PUT, the clear-all INVALIDATE_ALL and the
 * batched INVALIDATE_BATCH travel through a RabbitMQ fanout and are applied on
 * the peer's L1 — no mocks on the wire.
 *
 * <p>Skipped unless Docker is available AND {@code -Dzeta.it.broker=true} is
 * set (the class name ends in {@code IT}, so plain {@code mvn test} never
 * picks it up; run with {@code mvn test -Dtest=BrokerTwoNodeSyncIT
 * -Dzeta.it.broker=true}).
 */
@Testcontainers(disabledWithoutDocker = true)
@EnabledIfSystemProperty(named = "zeta.it.broker", matches = "true")
class BrokerTwoNodeSyncIT {

  @Container
  static final RabbitMQContainer BROKER = new RabbitMQContainer("rabbitmq:3.13-management");

  private static final CacheSyncProperties PROPS = new CacheSyncProperties();

  static CachingConnectionFactory connectionFactory;
  static RabbitAdmin admin;
  static FanoutExchange exchange;
  static Node nodeA;
  static Node nodeB;

  /** One sync-plane participant: its own queue, publisher, listener, L1. */
  static final class Node {

    final com.github.benmanes.caffeine.cache.Cache<String, CacheEntry> l1 =
      com.github.benmanes.caffeine.cache.Caffeine.newBuilder().maximumSize(1_000).build();
    final AnonymousQueue queue = new AnonymousQueue();
    final CacheSyncPublisher publisher;
    final SimpleMessageListenerContainer container;

    Node(String appName) {
      RabbitTemplate template = new RabbitTemplate(connectionFactory);
      PutPayloadCodec codec = new PutPayloadCodec(PROPS);
      publisher = new CacheSyncPublisher(template, PROPS, codec, appName);
      var context = new DefaultSyncContext(
        l1,
        null,
        new EntryLifecycleImpl(l1, new ZetaProperties(), null),
        List.of(),
        null,
        codec
      );
      var handler = new SyncPolicy() {};
      var listener = new CacheSyncListener(PROPS, Executors.newScheduledThreadPool(2), handler, context, appName, null);
      // The listener acks through the AMQP channel; the container auto-acks,
      // so the channel calls only need to be safe no-ops under test.
      com.rabbitmq.client.Channel noopChannel = (com.rabbitmq.client.Channel) java.lang.reflect.Proxy.newProxyInstance(
        Node.class.getClassLoader(),
        new Class<?>[] { com.rabbitmq.client.Channel.class },
        (proxy, method, args) -> {
          if ("isOpen".equals(method.getName())) {
            return Boolean.TRUE;
          }
          return null; // basicAck / basicNack / everything else: safe no-ops
        }
      );
      container = new SimpleMessageListenerContainer(connectionFactory);
      container.setQueueNames(queue.getName());
      container.setMessageListener(m -> {
        try {
          listener.onMessage(noopChannel, m);
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      });
      container.start();
    }

    void declare(RabbitAdmin rabbitAdmin, FanoutExchange ex) {
      rabbitAdmin.declareQueue(queue);
      rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(ex));
    }
  }

  @BeforeAll
  static void setUp() {
    var rabbit = new ConnectionFactory();
    rabbit.setHost(BROKER.getHost());
    rabbit.setPort(BROKER.getMappedPort(5672));
    connectionFactory = new CachingConnectionFactory(rabbit);
    admin = new RabbitAdmin(connectionFactory);
    PROPS.setEnabled(true);
    exchange = new FanoutExchange(PROPS.getExchangeName(), true, false);
    admin.declareExchange(exchange);
    nodeA = new Node("app-a");
    nodeB = new Node("app-b");
    nodeA.declare(admin, exchange);
    nodeB.declare(admin, exchange);
  }

  @AfterAll
  static void tearDown() {
    if (nodeA != null) {
      nodeA.container.stop();
    }
    if (nodeB != null) {
      nodeB.container.stop();
    }
    if (connectionFactory != null) {
      connectionFactory.destroy();
    }
  }

  private static CacheEntry entry(String value, long version) {
    return EntryDraft.blank(null)
      .value(value)
      .version(version)
      .decision(DecisionStamp.hot(0, null, 0L))
      .ttl(300_000, 30_000, 300_000, 30_000)
      .expiryAt(Long.MAX_VALUE, Long.MAX_VALUE)
      .keyState(KeyState.NORMAL)
      .build();
  }

  /** Polls until the condition holds or the 10 s budget is exhausted. */
  private static void await(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
    while (System.currentTimeMillis() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(50);
    }
    assertThat(condition.getAsBoolean()).as("condition met within 10s").isTrue();
  }

  @Test
  @DisplayName("INVALIDATE published on A removes the entry on B through the real broker")
  void invalidateCrossesTheBroker() throws Exception {
    nodeB.l1.put("it::k1", entry("v", 1L));
    nodeA.publisher.broadcastInvalidate("it::k1", 5L);

    await(() -> nodeB.l1.getIfPresent("it::k1") == null);
  }

  @Test
  @DisplayName("value-carrying PUT published on A applies the value on B")
  void putCrossesTheBroker() throws Exception {
    byte[] payload = nodeA.publisher.serializePutPayload("carried");
    assertThat(payload).isNotNull();
    nodeA.publisher.broadcastPut("it::k2", 7L, payload);

    await(
      () -> nodeB.l1.getIfPresent("it::k2") != null && "carried".equals(nodeB.l1.getIfPresent("it::k2").getValue())
    );
  }

  @Test
  @DisplayName("INVALIDATE_ALL published on A clears the cache on B")
  void clearAllCrossesTheBroker() throws Exception {
    nodeB.l1.put("a", entry("v1", 1L));
    nodeB.l1.put("b", entry("v2", 2L));
    nodeB.l1.put("fresh", entry("v3", 11L));

    nodeA.publisher.broadcastInvalidateAll(10L);

    await(
      () ->
        nodeB.l1.getIfPresent("a") == null &&
        nodeB.l1.getIfPresent("b") == null &&
        nodeB.l1.getIfPresent("fresh") != null
    );
  }

  @Test
  @DisplayName("batched INVALIDATE_BATCH published on A removes every entry on B")
  void batchInvalidateCrossesTheBroker() throws Exception {
    nodeB.l1.put("bat::a", entry("v", 1L));
    nodeB.l1.put("bat::b", entry("v", 1L));

    nodeA.publisher.broadcastInvalidateBatch(
      List.of(new SyncMessage.Entry("bat::a", 5L), new SyncMessage.Entry("bat::b", 5L))
    );

    await(() -> nodeB.l1.getIfPresent("bat::a") == null && nodeB.l1.getIfPresent("bat::b") == null);
  }
}
