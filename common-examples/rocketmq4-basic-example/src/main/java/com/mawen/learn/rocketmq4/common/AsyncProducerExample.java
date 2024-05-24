package com.mawen.learn.rocketmq4.common;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/24
 */
@Slf4j
public class AsyncProducerExample {

	public static void main(String[] args) throws Exception{
		DefaultMQProducer producer = new DefaultMQProducer("producer_group");
		producer.setNamesrvAddr("127.0.0.1:9876");
		producer.start();

		Message message = new Message("simpleTopic", "TagA", "Hello RocketMQ".getBytes(StandardCharsets.UTF_8));

		CountDownLatch latch = new CountDownLatch(1);
		producer.send(message, new SendCallback() {
			@Override
			public void onSuccess(SendResult sendResult) {
				log.info("OK {}", sendResult);
				latch.countDown();
			}

			@Override
			public void onException(Throwable e) {
				log.error("Exception", e);
				latch.countDown();
			}
		});

		latch.await(5, TimeUnit.SECONDS);

		producer.shutdown();
	}
}
