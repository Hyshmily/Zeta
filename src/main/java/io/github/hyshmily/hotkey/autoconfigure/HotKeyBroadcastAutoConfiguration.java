package io.github.hyshmily.hotkey.autoconfigure;

import com.github.benmanes.caffeine.cache.Cache;
import io.github.hyshmily.hotkey.broadcast.BroadcastListener;
import io.github.hyshmily.hotkey.broadcast.BroadcastProperties;
import io.github.hyshmily.hotkey.broadcast.BroadcastPublisher;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.RedisTemplate;

@AutoConfiguration
@ConditionalOnClass(
  name = { "org.springframework.amqp.rabbit.core.RabbitTemplate", "org.springframework.data.redis.core.RedisTemplate" }
)
@ConditionalOnProperty(prefix = "hotkey.broadcast", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(BroadcastProperties.class)
public class HotKeyBroadcastAutoConfiguration {

  @Bean
  public FanoutExchange hotkeyBroadcastExchange(BroadcastProperties properties) {
    return new FanoutExchange(properties.getExchangeName(), true, false);
  }

  @Bean
  public Queue hotkeyBroadcastQueue(BroadcastProperties properties) {
    return QueueBuilder.durable(properties.getQueueName()).withArgument("x-message-ttl", 60_000).build();
  }

  @Bean
  public Binding hotkeyBroadcastBinding(Queue hotkeyBroadcastQueue, FanoutExchange hotkeyBroadcastExchange) {
    return BindingBuilder.bind(hotkeyBroadcastQueue).to(hotkeyBroadcastExchange);
  }

  @Bean
  @ConditionalOnMissingBean
  public BroadcastPublisher broadcastPublisher(RabbitTemplate rabbitTemplate, BroadcastProperties properties) {
    return new BroadcastPublisher(rabbitTemplate, properties);
  }

  @Bean
  @ConditionalOnMissingBean(name = "hotKeyRedisLoader")
  public Function<String, Object> hotKeyRedisLoader(RedisTemplate<String, Object> redisTemplate) {
    return key -> redisTemplate.opsForValue().get(key);
  }

  @Bean
  @ConditionalOnMissingBean
  public BroadcastListener broadcastListener(
    Cache<String, Object> hotLocalCache,
    Function<String, Object> hotKeyRedisLoader,
    BroadcastProperties properties,
    ScheduledExecutorService hotKeyBroadcastScheduler
  ) {
    return new BroadcastListener(hotLocalCache, hotKeyRedisLoader, properties, hotKeyBroadcastScheduler);
  }

  @Bean
  public SimpleMessageListenerContainer broadcastListenerContainer(
    ConnectionFactory connectionFactory,
    BroadcastListener broadcastListener,
    BroadcastProperties properties
  ) {
    SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
    container.setQueueNames(properties.getQueueName());
    container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
    container.setConcurrentConsumers(properties.getConcurrentConsumers());
    container.setMessageListener(
      (ChannelAwareMessageListener) (msg, channel) -> broadcastListener.handleHotKeyMessage(channel, msg)
    );
    return container;
  }

  @Bean(destroyMethod = "shutdown")
  public ScheduledExecutorService hotKeyBroadcastScheduler(BroadcastProperties properties) {
    return Executors.newScheduledThreadPool(properties.getSchedulerPoolSize(), r -> {
      Thread t = new Thread(r, "hotkey-broadcast");
      t.setDaemon(true);
      return t;
    });
  }
}
