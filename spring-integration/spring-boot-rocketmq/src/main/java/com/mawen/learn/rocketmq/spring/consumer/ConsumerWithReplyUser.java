package com.mawen.learn.rocketmq.spring.consumer;

import com.mawen.learn.rocketmq.spring.domain.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQReplyListener;

import org.springframework.stereotype.Service;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/5/17
 */
@Slf4j
@Service
@RocketMQMessageListener(topic = "${demo.rocketmq.objectRequestTopic}",
		consumerGroup = "${demo.rocketmq.objectRequestConsumer}",
		selectorExpression = "${demo.rocketmq.tag}")
public class ConsumerWithReplyUser implements RocketMQReplyListener<User, User> {

	@Override
	public User onMessage(User user) {
		log.info("------ ConsumerWithReplyUser received: {}", user);

		return new User("replyUserName", (byte) 10);
	}
}
