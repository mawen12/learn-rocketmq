package com.mawen.learn.rocketmq5.spring.listener;

import java.nio.charset.StandardCharsets;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.annotation.RocketMQMessageListener;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.core.RocketMQListener;

import org.springframework.stereotype.Service;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/24
 */
@Slf4j
@Service
@RocketMQMessageListener(endpoints = "localhost:8081", consumerGroup = "normalGroup", topic = "normalTopic", tag = "*")
public class NormalMessageListener implements RocketMQListener {

	@Override
	public ConsumeResult consume(MessageView message) {

		log.info("receive message, topic: {}, messageId: {}, content: {}, bornTimestamp: {}",
				message.getTopic(),
				message.getMessageId(),
				StandardCharsets.UTF_8.decode(message.getBody()),
				message.getBornTimestamp());

		return ConsumeResult.SUCCESS;
	}
}
