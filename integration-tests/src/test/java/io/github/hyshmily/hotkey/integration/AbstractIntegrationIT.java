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

@SpringBootTest(classes = IntegrationTestApplication.class)
public abstract class AbstractIntegrationIT {

  protected static final Logger log = LoggerFactory.getLogger(AbstractIntegrationIT.class);

  @Autowired(required = false)
  @Qualifier("syncListenerContainer")
  private SimpleMessageListenerContainer syncContainer;

  @Autowired(required = false)
  @Qualifier("workerListenerContainer")
  private SimpleMessageListenerContainer workerContainer;

  @Autowired
  private CachingConnectionFactory connectionFactory;

  @BeforeEach
  void resetAndStartContainers() {
    stopContainer(syncContainer);
    stopContainer(workerContainer);
    connectionFactory.resetConnection();
    startContainer(syncContainer);
    startContainer(workerContainer);
  }

  @AfterEach
  void stopContainers() {
    stopContainer(syncContainer);
    stopContainer(workerContainer);
  }

  private void stopContainer(SimpleMessageListenerContainer container) {
    if (container != null && container.isRunning()) {
      container.stop();
    }
  }

  private void startContainer(SimpleMessageListenerContainer container) {
    if (container != null && !container.isRunning()) {
      container.start();
    }
  }
}
