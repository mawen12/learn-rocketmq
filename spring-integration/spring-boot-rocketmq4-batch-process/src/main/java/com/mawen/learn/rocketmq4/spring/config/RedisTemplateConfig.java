package com.mawen.learn.rocketmq4.spring.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/27
 */
@Configuration
public class RedisTemplateConfig {

	@Bean
	public RedisTemplate<String, Long> stringLongRedisTemplate(RedisConnectionFactory connectionFactory) {
		RedisTemplate<String, Long> redisTemplate = new RedisTemplate<>();
		redisTemplate.setConnectionFactory(connectionFactory);
		redisTemplate.setKeySerializer(new StringRedisSerializer());
		redisTemplate.setValueSerializer(new LongRedisSerializer(new StringRedisSerializer()));
		return redisTemplate;
	}

	public static class LongRedisSerializer implements RedisSerializer<Long> {

		private final RedisSerializer<String> delegate;

		public LongRedisSerializer() {
			this.delegate = new StringRedisSerializer();
		}

		public LongRedisSerializer(RedisSerializer<String> delegate) {
			this.delegate = delegate != null ? delegate : new StringRedisSerializer();
		}

		@Override
		public byte[] serialize(Long value) throws SerializationException {
			return delegate.serialize(String.valueOf(value));
		}

		@Override
		public Long deserialize(byte[] bytes) throws SerializationException {
			String value = delegate.deserialize(bytes);
			return value == null ? null : Long.valueOf(value);
		}
	}
}
