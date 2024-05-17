package com.mawen.learn.rocketmq.common.delay;

import java.time.Duration;
import java.time.LocalDateTime;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.java.example.ProducerDelayMessageExample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @see ProducerDelayMessageExample
 * @since 2024/5/16
 */
public class DelayProducerExample {

	private static final Logger logger = LoggerFactory.getLogger(DelayProducerExample.class);

	public static void main(String[] args) throws ClientException {
		ClientServiceProvider provider = ClientServiceProvider.loadService();

		String endpoint = "localhost:8080";
		ClientConfiguration configuration = ClientConfiguration.newBuilder()
				.setEndpoints(endpoint)
				.build();

		String topic = "DelayTopic";
		Producer producer = provider.newProducerBuilder()
				.setTopics(topic)
				.setClientConfiguration(configuration)
				.build();

		byte[] body = "This is a delay message for Apache RocketMQ".getBytes();
		String tag = "yourMessageTagA";

		Duration messageDelayTime = Duration.ofSeconds(10);
		Message message = provider.newMessageBuilder()
				.setTopic(topic)
				.setTag(tag)
				.setKeys("yourMessageKey-0e094a5f9d85")
				// set expected delivery timestamp of message.
				.setDeliveryTimestamp(System.currentTimeMillis() + messageDelayTime.toMillis())
				.setBody(body)
				.build();


		try {
			SendReceipt sendReceipt = producer.send(message);
			logger.info("{} Send message successfully, messageId = {}", LocalDateTime.now(), sendReceipt.getMessageId());
		}
		catch (ClientException e) {
			logger.error("Failed to send message", e);
		}

	}
}
