package com.mawen.learn.rocketmq.remoting.protocol;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public interface ForbiddenType {

	int BROKER_FORBIDDEN = 1;

	int GROUP_FORBIDDEN = 2;

	int TOPIC_FORBIDDEN = 3;

	int BROADCASTING_DISABLE_FORBIDDEN = 4;

	int SUBSCRIPTION_FORBIDDEN = 5;
}
