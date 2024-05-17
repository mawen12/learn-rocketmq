package com.mawen.learn.rocketmq.common.delay;

import java.io.IOException;
import java.time.LocalDateTime;
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
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/16
 */
public class PushConsumerExample {

	private static final Logger logger = LoggerFactory.getLogger(PushConsumerExample.class);

	public static void main(String[] args) throws ClientException, InterruptedException, IOException {
		String endpoint = "localhost:8081";

		ClientServiceProvider provider = ClientServiceProvider.loadService();
		ClientConfiguration configuration = ClientConfiguration.newBuilder()
				.setEndpoints(endpoint)
				.build();

		String tag = "*";
		String topic = "DelayTopic";
		String consumerGroup = "yourConsumerGroup";
		FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);

		PushConsumer pushConsumer = provider.newPushConsumerBuilder()
				.setClientConfiguration(configuration)
				.setConsumerGroup(consumerGroup)
				.setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
				.setMessageListener(messageView -> {
					logger.info("{} Consume message = {}", LocalDateTime.now(), messageView);
					return ConsumeResult.SUCCESS;
				})
				.build();

		Thread.sleep(Long.MAX_VALUE);

		pushConsumer.close();
	}
}
