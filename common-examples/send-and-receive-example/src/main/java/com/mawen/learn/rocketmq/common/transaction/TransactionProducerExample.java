package com.mawen.learn.rocketmq.common.transaction;


import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.apis.producer.Transaction;
import org.apache.rocketmq.client.apis.producer.TransactionChecker;
import org.apache.rocketmq.client.apis.producer.TransactionResolution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/16
 */
public class TransactionProducerExample {

	private static final Logger logger = LoggerFactory.getLogger(TransactionProducerExample.class.getName());

	public static void main(String[] args) throws ClientException {
		ClientServiceProvider provider = ClientServiceProvider.loadService();

		String endpoint = "localhost:8081";
		ClientConfiguration configuration = ClientConfiguration.newBuilder()
				.setEndpoints(endpoint)
				.build();

		String topic = "TransactionTopic";
		TransactionChecker checker = messageView -> {
			logger.info("Receive transactional message check, message = {}", messageView);
			return TransactionResolution.COMMIT;
		};

		Producer producer = provider.newProducerBuilder()
				.setTopics(topic)
				.setTransactionChecker(checker)
				.setClientConfiguration(configuration)
				.build();

		Transaction transaction = producer.beginTransaction();

		String tag = "yourMessageTagA";
		String body = "This is a transaction message for Apache RocketMQ";
		Message message = provider.newMessageBuilder()
				.setTopic(topic)
				.setTag(tag)
				.setKeys("yourMessageKey-565ef26f5727")
				.setBody(body.getBytes())
				.build();

		try {
			SendReceipt sendReceipt = producer.send(message, transaction);
			logger.info("Send transaction message successfully, messageId = {}", sendReceipt.getMessageId());
		}
		catch (ClientException e) {
			logger.error("Failed to send message", e);
			return;
		}

		transaction.commit();
	}
}
