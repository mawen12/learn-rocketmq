package com.mawen.learn.rocketmq.common.simple;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumer message by {@link PushConsumer}
 *
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @see <a href="https://rocketmq.apache.org/docs/quickStart/01quickstart/">quickstart</a>
 * @since 2024/5/16
 */
public class ConsumerExample {

	private static final Logger logger = LoggerFactory.getLogger(ConsumerExample.class);

	public static void main(String[] args) throws ClientException, InterruptedException, IOException {
		String endpoint = "localhost:8081";

		ClientServiceProvider provider = ClientServiceProvider.loadService();
		ClientConfiguration configuration = ClientConfiguration.newBuilder()
				.setEndpoints(endpoint)
				.build();

		String tag = "*";
		FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
		String consumerGroup = "YourConsumerGroup";
		String topic = "TestTopic";

		PushConsumer pushConsumer = provider.newPushConsumerBuilder()
				.setClientConfiguration(configuration)
				.setConsumerGroup(consumerGroup)
				.setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
				.setMessageListener(messageView -> {
					ByteBuffer body = messageView.getBody();
					byte[] content = new byte[body.limit()];
					body.get(content);
					logger.info("Consume message successfully, messageId = {}, body = {}", messageView.getMessageId(), new String(content));
					return ConsumeResult.SUCCESS;
				})
				.build();

		Thread.sleep(Long.MAX_VALUE);

		pushConsumer.close();
	}
}
