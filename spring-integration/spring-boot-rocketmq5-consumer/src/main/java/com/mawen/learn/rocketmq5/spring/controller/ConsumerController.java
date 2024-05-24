package com.mawen.learn.rocketmq5.spring.controller;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageId;
	import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.core.RocketMQClientTemplate;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/24
 */
@Slf4j
@RestController
@RequestMapping("/consumer")
public class ConsumerController {

	@Autowired
	private RocketMQClientTemplate rocketMQClientTemplate;

	@GetMapping("/normal")
	public String getNormal() throws ClientException {
		List<MessageView> messages = rocketMQClientTemplate.receive(1, Duration.ofSeconds(10));
		log.info("Received {} messages", messages.size());

		for (MessageView message : messages) {
			log.info("receive message, topic: {}, messageId: {}, content: {}, bornTimestamp: {}, deliveryTimestamp: {}",
					message.getTopic(), message.getMessageId(), StandardCharsets.UTF_8.decode(message.getBody()), message.getBornTimestamp(), message.getDeliveryTimestamp());
//			rocketMQClientTemplate.ack(message);
		}

		return "OK";
	}
}
