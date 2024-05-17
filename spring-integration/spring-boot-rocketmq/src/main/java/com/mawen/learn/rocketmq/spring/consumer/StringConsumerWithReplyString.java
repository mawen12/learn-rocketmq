package com.mawen.learn.rocketmq.spring.consumer;

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
@RocketMQMessageListener(topic = "${demo.rocketmq.stringRequestTopic}", consumerGroup = "${demo.rocketmq.stringRequestConsumer}", selectorExpression = "${demo.rocketmq.tag}", replyTimeout = 10000)
public class StringConsumerWithReplyString implements RocketMQReplyListener<String, String> {

	@Override
	public String onMessage(String message) {
		log.info("------ StringConsumerWithReplyString received: {}", message);
		return "reply string";
	}
}
