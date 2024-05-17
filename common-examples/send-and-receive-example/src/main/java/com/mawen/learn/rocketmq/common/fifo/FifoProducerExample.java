package com.mawen.learn.rocketmq.common.fifo;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/16
 */
public class FifoProducerExample {

	private static final Logger logger = LoggerFactory.getLogger(FifoProducerExample.class);

	public static void main(String[] args) throws ClientException {
		ClientServiceProvider provider = ClientServiceProvider.loadService();

		String endpoint = "localhost:8080";
		ClientConfiguration configuration = ClientConfiguration.newBuilder()
				.setEndpoints(endpoint)
				.build();

		String topic = "FifoTopic";
		Producer producer = provider.newProducerBuilder()
				.setTopics(topic)
				.setClientConfiguration(configuration)
				.build();

		String producerGroup = "yourProducerGroup";
		String body = "This is a FIFO message for Apache RocketMQ";
		String tag = "yourMessageTagA";
		Message message = provider.newMessageBuilder()
				.setTopic(topic)
				.setTag(tag)
				.setKeys("yourMessageKey-1ff69ada8e0e")
				.setMessageGroup(producerGroup)
				.setBody(body.getBytes())
				.build();

		try {
			SendReceipt sendReceipt = producer.send(message);
			logger.info("Send message successfully, messageId = {}", sendReceipt.getMessageId());
		}
		catch (ClientException e) {
			logger.error("Failed to send message", e);
		}
	}

}
