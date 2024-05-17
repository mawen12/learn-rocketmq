package com.mawen.learn.rocketmq.common.simple;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Send message by {@link Producer}
 *
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @see <a href="https://rocketmq.apache.org/docs/quickStart/01quickstart/">quickstart</a>
 * @since 2024/5/16
 */
public class ProducerExample {

	private static final Logger logger = LoggerFactory.getLogger(ProducerExample.class);

	public static void main(String[] args) throws ClientException {
		String endpoint = "localhost:8081";
		String topic = "TestTopic";

		ClientServiceProvider provider = ClientServiceProvider.loadService();
		ClientConfigurationBuilder builder = ClientConfiguration.newBuilder().setEndpoints(endpoint);
		ClientConfiguration configuration = builder.build();

		Producer producer = provider.newProducerBuilder()
				.setTopics(topic)
				.setClientConfiguration(configuration)
				.build();

		Message message = provider.newMessageBuilder()
				.setTopic(topic)
				.setKeys("messageKey")
				.setTag("messageTag")
				.setBody("messageBody".getBytes())
				.build();

		try {
			SendReceipt sendReceipt = producer.send(message);
			logger.info("Send message successfully, messageId={}", sendReceipt.getMessageId());
		}
		catch (ClientException e) {
			logger.error("Failed to send message", e);
		}
	}

}
