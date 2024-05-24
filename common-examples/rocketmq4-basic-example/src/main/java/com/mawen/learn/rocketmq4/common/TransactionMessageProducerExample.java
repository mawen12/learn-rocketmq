package com.mawen.learn.rocketmq4.common;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/24
 */
@Slf4j
public class TransactionMessageProducerExample {

	private static AtomicInteger integer = new AtomicInteger(0);

	@SneakyThrows
	public static void main(String[] args) {
		TransactionMQProducer producer = new TransactionMQProducer("producer_group");
		producer.setNamesrvAddr("localhost:9876");
		producer.setExecutorService(new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<>(200), new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread thread = new Thread(r);
				thread.setName("client-transaction-msg-check-thread");
				return thread;
			}
		}));
		producer.setTransactionListener(new TransactionListener() {
			@Override
			public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
				integer.incrementAndGet();
				return LocalTransactionState.COMMIT_MESSAGE;
			}

			@Override
			public LocalTransactionState checkLocalTransaction(MessageExt msg) {
				if (integer.get() == 1) {
					log.info("Transaction execute successful");
				}
				return LocalTransactionState.COMMIT_MESSAGE;
			}
		});
		producer.start();


		Message message = new Message("transTopic", "TagA", "Hello RocketMQ".getBytes(StandardCharsets.UTF_8));
		TransactionSendResult sendResult = producer.sendMessageInTransaction(message, null);

		log.info("{}", sendResult);

		Thread.sleep(1000L);

		producer.shutdown();
	}
}
