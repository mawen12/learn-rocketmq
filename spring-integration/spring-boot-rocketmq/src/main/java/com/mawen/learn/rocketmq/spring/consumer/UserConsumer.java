package com.mawen.learn.rocketmq.spring.consumer;

import com.mawen.learn.rocketmq.spring.domain.User;
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
@RocketMQMessageListener(topic = "${demo.rocketmq.userTopic}", consumerGroup = "user_consumer")
public class UserConsumer implements RocketMQListener<User> {

	@Override
	public void onMessage(User message) {
		log.info("###### user_consumer received: {}; age: {}; name: {}",
				message,
				message.userAge(),
				message.username());
	}
}
