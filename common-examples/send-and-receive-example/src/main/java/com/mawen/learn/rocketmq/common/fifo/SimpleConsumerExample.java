package com.mawen.learn.rocketmq.common.fifo;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumer message by {@link PushConsumer}
 *
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @see <a href="https://rocketmq.apache.org/docs/quickStart/01quickstart/">quickstart</a>
 * @since 2024/5/16
 */
public class SimpleConsumerExample {

	private static final Logger logger = LoggerFactory.getLogger(SimpleConsumerExample.class);

	public static void main(String[] args) throws ClientException, InterruptedException, IOException {
		String endpoint = "localhost:8081";

		ClientServiceProvider provider = ClientServiceProvider.loadService();
		ClientConfiguration configuration = ClientConfiguration.newBuilder()
				.setEndpoints(endpoint)
				.build();

		String tag = "*";
		FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
		String consumerGroup = "YourConsumerGroup";
		String topic = "FifoTopic";

		SimpleConsumer consumer = provider.newSimpleConsumerBuilder()
				.setClientConfiguration(configuration)
				.setConsumerGroup(consumerGroup)
				.setAwaitDuration(Duration.ofSeconds(30))
				.setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
				.build();

		int maxMessageNum = 16;
		Duration invisibleDuration = Duration.ofSeconds(15);

		do {
			List<MessageView> messages = consumer.receive(maxMessageNum, invisibleDuration);
			logger.info("Received {} messages", messages.size());
			for (MessageView message : messages) {
				MessageId messageId = message.getMessageId();
				try {
					consumer.ack(message);
					logger.info("Message is acknowledged successfully, messageId = {}", messageId);
				}
				catch (ClientException e) {
					logger.error("Message is failed to be acknowledged, messageId = {}", messageId, e);
				}
			}

			Thread.sleep(1000L);

		} while (true);

	}
}
