package com.mawen.learn.rocketmq.spring.consumer;

import com.mawen.learn.rocketmq.spring.domain.ProductWithPayload;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQReplyListener;

import org.springframework.stereotype.Service;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/17
 */
@Slf4j
@Service
@RocketMQMessageListener(topic = "${demo.rocketmq.genericRequestTopic}",
	consumerGroup = "${demo.rocketmq.genericRequestConsumer}",
	selectorExpression = "${demo.rocketmq.tag}")
public class ConsumerWithReplyGeneric implements RocketMQReplyListener<String, ProductWithPayload<String>> {

	@Override
	public ProductWithPayload<String> onMessage(String message) {
		log.info("------- ConsumerWithReplyGeneric received: {}", message);
		return new ProductWithPayload<String>("replyProductName", "product payload");
	}
}
