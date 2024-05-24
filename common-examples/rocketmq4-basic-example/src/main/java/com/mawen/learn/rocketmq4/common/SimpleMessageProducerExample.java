package com.mawen.learn.rocketmq4.common;

import java.nio.charset.StandardCharsets;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @see <a href="https://rocketmq.apache.org/docs/4.x/producer/02message1">simple message</a>
 * @since 2024/5/24
 */
@Slf4j
public class SimpleMessageProducerExample {

	public static void main(String[] args) throws Exception {
		// Create producer
		DefaultMQProducer producer = new DefaultMQProducer("producer_group");
		// Set the address of NameServer
		producer.setNamesrvAddr("localhost:9876");
		// Start producer
		producer.start();
		// Create a message
		Message message = new Message("simpleTopic", "TagA", "Hello RocketMQ".getBytes(StandardCharsets.UTF_8));
		// Send message
		SendResult sendResult = producer.send(message);
		// print result
		log.info("{}", sendResult);

		producer.shutdown();
	}

}
