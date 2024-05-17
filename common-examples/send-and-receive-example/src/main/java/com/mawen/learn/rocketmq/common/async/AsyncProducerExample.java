package com.mawen.learn.rocketmq.common.async;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
 * @see org.apache.rocketmq.client.java.example.AsyncProducerExample
 * @since 2024/5/16
 */
public class AsyncProducerExample {

	private static final Logger logger = LoggerFactory.getLogger(AsyncProducerExample.class);

	public static void main(String[] args) throws ClientException, InterruptedException {
		ClientServiceProvider provider = ClientServiceProvider.loadService();

		String endpoint = "localhost:8080";
		String topic = "yourTopic";
		ClientConfiguration configuration = ClientConfiguration.newBuilder().setEndpoints(endpoint)
				.build();

		Producer producer = provider.newProducerBuilder()
				.setTopics(topic)
				.setClientConfiguration(configuration)
				.build();

		byte[] body = "This is a async message for Apache RocketMQ".getBytes();
		String tag = "yourMessageTagA";

		Message message = provider.newMessageBuilder()
				.setTopic(topic)
				.setTag(tag)
				.setKeys("yourMessageKey-0e094a5f9d85")
				.setBody(body)
				.build();

		CompletableFuture<SendReceipt> future = producer.sendAsync(message);
		ExecutorService sendCallbackExecutor = Executors.newCachedThreadPool();
		future.whenCompleteAsync((sendReceipt, throwable) -> {
			if (null != throwable) {
				logger.error("Failed to send message", throwable);
				return;
			}
			logger.info("Send message successfully, messageId = {}", sendReceipt.getMessageId());
		}, sendCallbackExecutor);

		Thread.sleep(Long.MAX_VALUE);
	}
}
