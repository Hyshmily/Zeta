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
package io.github.hyshmily.hotkey.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Base class for all Docker-dependent integration tests.
 *
 * <p>Manages RabbitMQ listener container lifecycle (stop before each test, start after)
 * and connection factory resets to ensure a clean state between test cases.
 *
 * <p>Includes the heartbeat container ({@code hotkeyHeartbeatContainer})
 * in lifecycle management to prevent stale listener threads from surviving
 * across {@code @DirtiesContext} boundaries. Combined with a 1-second
 * {@code spring.rabbitmq.connection-timeout}, old reconnect attempts fail
 * quickly instead of leaking into the next test class.
 */
@SpringBootTest(classes = IntegrationTestApplication.class)
public abstract class AbstractIntegrationIT {

  /** Logger for integration test subclasses. */
  protected static final Logger log = LoggerFactory.getLogger(AbstractIntegrationIT.class);

  @Autowired(required = false)
  @Qualifier("syncListenerContainer")
  private SimpleMessageListenerContainer syncContainer;

  @Autowired(required = false)
  @Qualifier("workerListenerContainer")
  private SimpleMessageListenerContainer workerContainer;

  @Autowired(required = false)
  @Qualifier("hotkeyHeartbeatContainer")
  private SimpleMessageListenerContainer heartbeatContainer;

  @Autowired
  private CachingConnectionFactory connectionFactory;

  /** Resets the connection factory and restarts all listener containers before each test. */
  @BeforeEach
  void resetAndStartContainers() throws Exception {
    stopContainer(syncContainer);
    stopContainer(workerContainer);
    stopContainer(heartbeatContainer);
    connectionFactory.resetConnection();
    Thread.sleep(200); // wait for reset to propagate
    startContainer(syncContainer);
    startContainer(workerContainer);
    startContainer(heartbeatContainer);
    Thread.sleep(500); // wait for containers to connect
  }

  /** Stops all listener containers after each test to prevent thread leakage. */
  @AfterEach
  void stopContainers() {
    stopContainer(syncContainer);
    stopContainer(workerContainer);
    stopContainer(heartbeatContainer);
  }

  /**
   * Stops a listener container if it is non-null and currently running.
   * Waits up to 5 seconds for the container to fully shut down, preventing
   * stale listener threads from surviving across {@code @DirtiesContext}
   * boundaries.
   *
   * @param container the container to stop, may be {@code null}
   */
  private void stopContainer(SimpleMessageListenerContainer container) {
    if (container != null && container.isRunning()) {
      container.stop();
    }
  }

  /**
   * Starts a listener container if it is non-null and not already running.
   *
   * @param container the container to start, may be {@code null}
   */
  private void startContainer(SimpleMessageListenerContainer container) {
    if (container != null && !container.isRunning()) {
      container.start();
    }
  }
}
