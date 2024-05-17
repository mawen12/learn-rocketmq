package com.mawen.learn.rocketmq.spring.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQReplyListener;

import org.springframework.stereotype.Service;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/17
 */
@Slf4j
@Service
@RocketMQMessageListener(topic = "${demo.rocketmq.bytesRequestTopic}", consumerGroup = "${demo.rocketmq.bytesRequestConsumer}",
		selectorExpression = "${demo.rocketmq.tag}")
public class ConsumerWithReplyBytes implements RocketMQReplyListener<MessageExt, byte[]> {

	@Override
	public byte[] onMessage(MessageExt message) {
		log.info("------ ConsumerWithReplyBytes received: {}", message);
		return "reply message content".getBytes();
	}
}
