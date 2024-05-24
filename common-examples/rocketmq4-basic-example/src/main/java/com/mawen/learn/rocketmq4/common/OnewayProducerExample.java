package com.mawen.learn.rocketmq4.common;

import java.nio.charset.StandardCharsets;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @see <a href="https://rocketmq.apache.org/docs/4.x/producer/02message1">simple message</a>
 * @since 2024/5/24
 */
public class OnewayProducerExample {

	public static void main(String[] args) throws Exception {
		DefaultMQProducer producer = new DefaultMQProducer("producer_group");
		producer.setNamesrvAddr("localhost:9876");
		producer.start();

		Message message = new Message("simpleTopic", "TagA", "Hello RocketMQ".getBytes(StandardCharsets.UTF_8));
		producer.sendOneway(message);
		producer.shutdown();
	}
}
