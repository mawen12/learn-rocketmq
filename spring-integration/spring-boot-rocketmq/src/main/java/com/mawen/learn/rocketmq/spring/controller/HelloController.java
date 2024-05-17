package com.mawen.learn.rocketmq.spring.controller;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.TypeReference;
import com.mawen.learn.rocketmq.spring.config.RocketMQDemoProperties;
import com.mawen.learn.rocketmq.spring.domain.OrderPaidEvent;
import com.mawen.learn.rocketmq.spring.domain.ProductWithPayload;
import com.mawen.learn.rocketmq.spring.domain.User;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.spring.core.RocketMQLocalRequestCallback;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeTypeUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @see <a href="https://github.com/apache/rocketmq-spring/blob/master/rocketmq-spring-boot-samples/rocketmq-produce-demo/src/main/java/org/apache/rocketmq/samples/springboot/ProducerApplication.java">ProducerApplication</a>
 * @since 2024/5/16
 */
@Slf4j
@RestController()
@RequestMapping("/hello")
@RequiredArgsConstructor
public class HelloController {

	private final RocketMQDemoProperties properties;
	private final RocketMQTemplate rocketMQTemplate;

	@PostMapping("/string")
	public String sendString() {
		SendResult sendResult = rocketMQTemplate.syncSend(properties.getTopic(), "Hello World");
		log.info("syncSend to topic {} sendResult {}", properties.getTopic(), sendResult);

		return "OK";
	}

	@PostMapping("/user")
	private String sendUser() {
		SendResult sendResult = rocketMQTemplate.syncSend(properties.getUserTopic(), new User("Kitty", (byte) 18));
		log.info("syncSend to topic {} sendResult {}", properties.getUserTopic(), sendResult);

		return "OK";
	}

	@PostMapping("/payload")
	public String sendPayload() {
		SendResult sendResult = rocketMQTemplate.syncSend(properties.getUserTopic(), MessageBuilder
				.withPayload(new User("Lester", (byte) 21))
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON_VALUE)
				.build());
		log.info("syncSend to topic {} sendResult {}", properties.getUserTopic(), sendResult);

		return "OK";
	}

	@PostMapping("/order")
	public String sendOrder() {
		rocketMQTemplate.asyncSend(properties.getOrderTopic(), new OrderPaidEvent("T_001", new BigDecimal("88.00")), new SendCallback() {
			@Override
			public void onSuccess(SendResult sendResult) {
				log.info("async onSuccess SendResult = {}", sendResult);
			}

			@Override
			public void onException(Throwable throwable) {
				log.info("async onException Throwable", throwable);
			}
		});

		return "OK";
	}

	@PostMapping("/tag0")
	public String sendTag0() {
		rocketMQTemplate.convertAndSend(properties.getMsgExtTopic() + ":tag0", "I'm from tag0");
		log.info("syncSend topic {} tag {}", properties.getMsgExtTopic(), "tag0");

		return "OK";
	}

	@PostMapping("/tag1")
	public String sendTag1() {
		rocketMQTemplate.convertAndSend(properties.getMsgExtTopic() + ":tag1", "I'm from tag1");
		log.info("syncSend topic {} tag {}", properties.getMsgExtTopic(), "tag1");

		return "OK";
	}

	@PostMapping("/batch")
	public String sendBatch() {

		List<Message> msgs = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			msgs.add(MessageBuilder.withPayload("Hello RocketMQ Batch Msg#" + i)
					.setHeader(RocketMQHeaders.KEYS, "KEY_" + i)
					.build());
		}
		SendResult sendResult = rocketMQTemplate.syncSend(properties.getTopic(), msgs, 60000);
		log.info("Batch messages send result: {}", sendResult);

		return "OK";
	}

	@PostMapping("/batch/orderly")
	public String sendBatchOrderly() {

		List<Message<String>> msgs = new ArrayList<>();
		for (int i = 0; i < 4; i++) {
			// send to 4 queues
			for (int j = 0; j < 10; j++) {
				int msgIndex = i * 10 + j;
				String msg = String.format("Hello RocketMQ Batch msg#%d to queue: %d", msgIndex, i);
				msgs.add(MessageBuilder.withPayload(msg)
						.setHeader(RocketMQHeaders.KEYS, "KEY_" + msgIndex)
						.build());
			}

			SendResult sendResult = rocketMQTemplate.syncSendOrderly(properties.getTopic(), msgs, i + "", 60000);
			log.info("Batch messages orderly to queue: {} send result {}", sendResult.getMessageQueue().getQueueId(), sendResult);
		}

		return "OK";
	}

	@PostMapping("/transaction")
	public String sendTransaction() {
		String[] tags = new String[]{"TagA", "TagB", "TagC", "TagD", "TagE"};
		try {
			for (int i = 0; i < 10; i++) {
				Message<String> message = MessageBuilder.withPayload("rocketMQTemplate transactional message " + i)
						.setHeader(RocketMQHeaders.TRANSACTION_ID, "KEY_" + i)
						.build();
				TransactionSendResult sendResult = rocketMQTemplate.sendMessageInTransaction(properties.getTransTopic() + ":" + tags[i % tags.length], message, null);
				log.info("RocketMQTemplate send Transactional msg body = {}, sendResult = {}", message.getPayload(), sendResult.getSendStatus());
			}

			Thread.sleep(10);
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}

		return "OK";
	}

	@PostMapping("/receive/string")
	public String sendAndReceiveString() {
		// sync call
		String replyString = rocketMQTemplate.sendAndReceive(properties.getStringRequestTopic() + ":" + properties.getTag(), "request string 1", String.class);
		log.info("send {} and receive {}", "request string 1", replyString);

		return "OK";
	}

	@PostMapping("/receive/bytes")
	public String sendAndReceiveBytes() {

		byte[] replyBytes = rocketMQTemplate.sendAndReceive(properties.getBytesRequestTopic() + ":" + properties.getTag(), MessageBuilder.withPayload("request bytes[]").build(), byte[].class, 3000);
		log.info("send {} and receive {}", "request byte[]", new String(replyBytes));

		return "OK";
	}

	@PostMapping("/receive/object")
	public String sendAndReceiveObject() {

		User requestUser = new User("requestUserName", (byte) 9);
		User replyUser = rocketMQTemplate.sendAndReceive(properties.getObjectRequestTopic() + ":" + properties.getTag(), requestUser, User.class, "order-id");
		log.info("send {} and receive {}", requestUser, replyUser);

		return "OK";
	}

	@PostMapping("/receive/generic")
	public String sendAndReceiveGeneric() {
		ProductWithPayload<String> replyGenericObject = rocketMQTemplate.sendAndReceive(properties.getGenericRequestTopic() + ":" + properties.getTag(), "request generic",
				new TypeReference<ProductWithPayload<String>>() {}.getType(), 30000, 0);
		log.info("send {} and receive {}", "request generic", replyGenericObject);

		return "OK";
	}

	@PostMapping("/receive/async")
	public String sendAndReceiveAsync() {
		rocketMQTemplate.sendAndReceive(properties.getStringRequestTopic() + ":" + properties.getTag(), "request string", new RocketMQLocalRequestCallback<String>() {
			@Override
			public void onSuccess(String message) {
				log.info("send {} and receive {}", "request string", message);
			}

			@Override
			public void onException(Throwable throwable) {
				throwable.printStackTrace();
			}
		}, 5000);

		return "OK";
	}
}
