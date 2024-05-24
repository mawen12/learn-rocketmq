package com.mawen.learn.rocketmq4.common;

import java.nio.charset.StandardCharsets;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @see <a href="https://rocketmq.apache.org/docs/4.x/producer/04message3">delayed message</a>
 * @since 2024/5/24
 */
@Slf4j
public class DelayedMessageProducerExample {

	@SneakyThrows
	public static void main(String[] args) {
		DefaultMQProducer producer = new DefaultMQProducer("producer_group");
		producer.setNamesrvAddr("localhost:9876");
		producer.start();

		Message message = new Message("delayedTopic", "TagA", "Hello RocketMQ".getBytes(StandardCharsets.UTF_8));
		message.setDelayTimeLevel(3);
		SendResult sendResult = producer.send(message);

		log.info("{}", sendResult);

		producer.shutdown();
	}
}
