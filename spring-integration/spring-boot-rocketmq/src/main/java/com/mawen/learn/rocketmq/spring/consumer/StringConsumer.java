package com.mawen.learn.rocketmq.spring.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;

import org.springframework.stereotype.Service;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/17
 */
@Slf4j
@Service
@RocketMQMessageListener(topic = "${demo.rocketmq.topic}", consumerGroup = "string_consumer", tlsEnable = "${demo.rocketmq.tlsEnable}")
public class StringConsumer implements RocketMQListener<String> {

	@Override
	public void onMessage(String message) {
		log.info("------ StringConsumer received: {}", message);
	}
}
