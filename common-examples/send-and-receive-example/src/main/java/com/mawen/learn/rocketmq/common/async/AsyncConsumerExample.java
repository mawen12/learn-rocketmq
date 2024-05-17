package com.mawen.learn.rocketmq.common.async;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/16
 */
public class AsyncConsumerExample {

	private static final Logger logger = LoggerFactory.getLogger(AsyncConsumerExample.class);

	public static void main(String[] args) throws ClientException, InterruptedException {
		ClientServiceProvider provider = ClientServiceProvider.loadService();

		String accessKey = "yourAccessKey";
		String secretKey = "yourSecretKey";
		StaticSessionCredentialsProvider sessionCredentialsProvider = new StaticSessionCredentialsProvider(accessKey, secretKey);

		String endpoint = "localhost:8080";
		ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
				.setEndpoints(endpoint)
				.setCredentialProvider(sessionCredentialsProvider)
				.build();

		String consumerGroup = "yourConsumerGroup";
		Duration duration = Duration.ofSeconds(30);
		String tag = "yourMessageTagA";
		String topic = "yourTopic";

		FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);

		SimpleConsumer consumer = provider.newSimpleConsumerBuilder()
				.setClientConfiguration(clientConfiguration)
				.setConsumerGroup(consumerGroup)
				.setAwaitDuration(duration)
				.setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
				.build();

		int maxMessageNum = 16;
		Duration invisibleDuration = Duration.ofSeconds(15);
		ExecutorService receiveCallbackExecutor = Executors.newCachedThreadPool();
		ExecutorService ackCallbackExecutor = Executors.newCachedThreadPool();

		do {

			CompletableFuture<List<MessageView>> future0 = consumer.receiveAsync(maxMessageNum, invisibleDuration);
			future0.whenCompleteAsync(((messages, throwable) -> {
				if (null != throwable) {
					logger.error(" Failed to receive message from remote", throwable);
					return;
				}

				logger.info("Received {} message(s)", messages.size());

				Map<MessageView, CompletableFuture<Void>> map = messages.stream().collect(Collectors.toMap(message -> message, consumer::ackAsync));
				for (Map.Entry<MessageView, CompletableFuture<Void>> entry : map.entrySet()) {
					MessageId messageId = entry.getKey().getMessageId();
					ByteBuffer body = entry.getKey().getBody();

					byte[] bytes = new byte[body.limit()];
					body.get(bytes);

					CompletableFuture<Void> future = entry.getValue();
					future.whenCompleteAsync((v, t) -> {
						if (null != t) {
							logger.error("Message is failed to acknowledged, messageId = {}", messageId, t);
							return;
						}

						logger.info("Message is acknowledged, messageId = {}, body = {}", messageId, new String(bytes));
					}, ackCallbackExecutor);
				}

			}), receiveCallbackExecutor);

			Thread.sleep(2000L);
		} while (true);
	}
}
