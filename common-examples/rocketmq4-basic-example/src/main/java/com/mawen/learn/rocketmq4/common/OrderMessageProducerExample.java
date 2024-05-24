package com.mawen.learn.rocketmq4.common;

import java.nio.charset.StandardCharsets;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @see <a href="https://rocketmq.apache.org/docs/4.x/producer/03message2">ordered message</a>
 * @since 2024/5/24
 */
@Slf4j
public class OrderMessageProducerExample {

	public static void main(String[] args) throws Exception {
		DefaultMQProducer producer = new DefaultMQProducer("producer_group");
		producer.setNamesrvAddr("localhost:9876");
		producer.start();

		Message message = new Message("orderedTopic", "TagA", "Hello RocketMQ".getBytes(StandardCharsets.UTF_8));
		int id = 10;
		SendResult sendResult = producer.send(message, (mqs, msg, arg) -> {
			Integer id1 = (Integer) arg;
			int index = id1 % mqs.size();
			return mqs.get(index);
		}, id);

		log.info("{}", sendResult);

		producer.shutdown();
	}
}
