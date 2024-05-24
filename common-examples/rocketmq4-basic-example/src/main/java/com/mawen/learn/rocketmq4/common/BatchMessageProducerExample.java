package com.mawen.learn.rocketmq4.common;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @see <a href="https://rocketmq.apache.org/docs/4.x/producer/05message4">batch message</a>
 * @since 2024/5/24
 */
@Slf4j
public class BatchMessageProducerExample {

	public static void main(String[] args) throws Exception {
		DefaultMQProducer producer = new DefaultMQProducer("producer_group");
		producer.setNamesrvAddr("localhost:9876");
		producer.start();

		List<Message> messages = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			String content = "Hello " + i;
			Message message = new Message("batchTopic", "TagA", content.getBytes(StandardCharsets.UTF_8));

			messages.add(message);
		}

		SendResult sendResult = producer.send(messages);

		log.info("{}", sendResult);

		producer.shutdown();
	}
}
