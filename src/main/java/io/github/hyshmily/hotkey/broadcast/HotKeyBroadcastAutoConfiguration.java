package io.github.hyshmily.hotkey.broadcast;

import com.github.benmanes.caffeine.cache.Cache;
import java.util.function.Function;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.RedisTemplate;

@AutoConfiguration
@ConditionalOnClass({ RabbitTemplate.class, RedisTemplate.class })
@ConditionalOnProperty(prefix = "hotkey.broadcast", name = "enabled", havingValue = "true")
public class HotKeyBroadcastAutoConfiguration {

  private final BroadcastProperties properties;

  public HotKeyBroadcastAutoConfiguration(BroadcastProperties properties) {
    this.properties = properties;
  }

  @Bean
  @ConfigurationProperties(prefix = "hotkey.broadcast")
  public BroadcastProperties broadcastProperties() {
    return new BroadcastProperties();
  }

  @Bean
  public FanoutExchange hotkeyBroadcastExchange() {
    return new FanoutExchange(properties.getExchangeName(), true, false);
  }

  @Bean
  public Queue hotkeyBroadcastQueue() {
    return QueueBuilder.durable(properties.getQueueName()).build();
  }

  @Bean
  public Binding hotkeyBroadcastBinding(
      Queue hotkeyBroadcastQueue,
      FanoutExchange hotkeyBroadcastExchange) {
    return BindingBuilder.bind(hotkeyBroadcastQueue).to(hotkeyBroadcastExchange);
  }

  @Bean
  @ConditionalOnMissingBean
  public BroadcastPublisher broadcastPublisher(
      RabbitTemplate rabbitTemplate) {
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
      Function<String, Object> hotKeyRedisLoader) {
    return new BroadcastListener(hotLocalCache, hotKeyRedisLoader, properties);
  }
}
