package com.mawen.learn.rocketmq5.spring.controller;

import com.mawen.learn.rocketmq5.spring.config.DemoProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.core.RocketMQClientTemplate;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/24
 */
@Slf4j
@RestController
@RequestMapping("/producer")
public class ProducerController {

	@Autowired
	private DemoProperties properties;

	@Autowired
	private RocketMQClientTemplate rocketMQClientTemplate;

	@PostMapping("/send/normal")
	public String sendNormal() {
		String payload = "Normal Message";

		Message<byte[]> message = MessageBuilder.withPayload(payload.getBytes()).build();
		SendReceipt sendReceipt = rocketMQClientTemplate.syncSendNormalMessage(properties.getNormalTopic(), message);
		log.info("normal send to topic: {}, sendReceipt: {}", properties.getNormalTopic(), sendReceipt);

		return "OK";
	}
}
