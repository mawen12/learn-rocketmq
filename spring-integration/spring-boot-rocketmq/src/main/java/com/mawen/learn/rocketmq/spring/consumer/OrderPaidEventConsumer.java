package com.mawen.learn.rocketmq.spring.consumer;

import com.mawen.learn.rocketmq.spring.domain.OrderPaidEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;

import org.springframework.stereotype.Service;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/17
 */
@Slf4j
@Service
@RocketMQMessageListener(topic = "${demo.rocketmq.orderTopic}", consumerGroup = "order-paid-consumer")
public class OrderPaidEventConsumer implements RocketMQListener<OrderPaidEvent> {

	@Override
	public void onMessage(OrderPaidEvent message) {
		log.info("------ OrderPaidEventConsumer received: {}, [orderId: {}]", message, message.orderId());
	}
}
